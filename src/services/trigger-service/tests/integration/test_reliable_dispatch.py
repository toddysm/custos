"""Authenticated admission and durable delivery contracts on memory and production PostgreSQL."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from asyncpg.pool import Pool
from custos_pg import PgMetadataAdapter
from custos_spl.ids import RunId as SplRunId
from custos_spl.ids import StepId, WorkspaceId
from custos_spl.interfaces.dead_letters import DeadLetterEntry, encode_dead_letter
from custos_spl.interfaces.metadata_store import ResumeSubscription

from custos_trigger.clients.workflow import (
    FakeWorkflowServiceClient,
    RunRef,
    StartRunRequest,
    WorkflowClientTransportError,
)
from custos_trigger.dedup import compute_dedup_key
from custos_trigger.errors import TriggerError, TriggerErrorKind
from custos_trigger.events import NormalizedEvent
from custos_trigger.models import ResumeRegistration, SourceType, Subscription
from custos_trigger.pipeline.dispatch import NoopAuditSink
from custos_trigger.pipeline.match_resume import ResumeMatch
from custos_trigger.pipeline.match_start import StartMatch
from custos_trigger.pipeline.reliable import (
    FINALIZATION_MARGIN_SECONDS,
    MAX_DISPATCH_SECONDS,
    ReliableDispatcher,
    ReliableStorage,
)
from custos_trigger.providers import InMemoryTriggerMetadataStore

WS = WorkspaceId("reliable-test")
NOW = datetime(2026, 10, 29, tzinfo=UTC)


@dataclass
class Harness:
    """Provide deterministic memory time and real database failure injection."""

    store: ReliableStorage
    clock: list[datetime]
    pool: Pool | None

    async def expire_receipt(self, key: str) -> None:
        """Recover abandoned producer ownership without deleting its original identity."""
        receipt = await self.store.get_delivery(WS, "trigger", key)
        assert receipt is not None and receipt.lease_until is not None
        if self.pool is None:
            self.clock[0] = receipt.lease_until
        else:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE custos_state.delivery_receipt SET lease_until=clock_timestamp() "
                    "WHERE workspace_id=$1 AND namespace='trigger' AND key=$2",
                    WS,
                    key,
                )

    async def expire_replay(self, entry: DeadLetterEntry) -> None:
        """Reach the claim boundary while keeping database payload and index projections aligned."""
        assert entry.claim is not None
        if self.pool is None:
            self.clock[0] = entry.claim.expires_at
        else:
            now = await self.store.delivery_now()
            expired = replace(entry, claim=replace(entry.claim, expires_at=now))
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE custos_state.dead_letter SET payload=$3::json,claim_expires_at=$4 "
                    "WHERE workspace_id=$1 AND entry_id=$2",
                    WS,
                    entry.data.entry_id,
                    encode_dead_letter(expired),
                    now,
                )


@pytest.fixture(params=["memory", pytest.param("postgres", marks=pytest.mark.integration)])
def reliable_dsn(request: pytest.FixtureRequest) -> str | None:
    """Reset the database before creating the per-test async connection pool."""
    return None if request.param == "memory" else cast(str, request.getfixturevalue("pg_dsn"))


@pytest.fixture
async def harness(reliable_dsn: str | None) -> AsyncIterator[Harness]:
    """Exercise the actual production capability rather than a SQL-hydrated stand-in."""
    clock = [NOW]
    if reliable_dsn is None:
        yield Harness(InMemoryTriggerMetadataStore(now=lambda: clock[0]), clock, None)
        return
    import asyncpg

    pool = await asyncpg.create_pool(dsn=reliable_dsn, min_size=1, max_size=4)
    assert pool is not None
    try:
        yield Harness(PgMetadataAdapter(pool=pool), clock, pool)
    finally:
        await pool.close()


def event() -> NormalizedEvent:
    """Build a bounded normalized event with private raw transport that must not be retained."""
    return NormalizedEvent.model_validate(
        {
            "eventId": "event",
            "kind": "custom.test",
            "source": {"type": "manual", "occurredAt": NOW.isoformat()},
            "data": {"value": 1},
            "raw": {"headers": {"authorization": "do-not-store"}, "body": "do-not-store"},
        }
    )


def match() -> StartMatch:
    """Supply the same resolved target shape that the manual and external receivers dispatch."""
    return StartMatch(
        Subscription(
            workspace_id=str(WS),
            subscription_id="sub",
            workflow_id="workflow",
            target_workflow_version_id="version",
            source_type=SourceType.MANUAL,
            input_mapping={"value": 1},
            created_at=NOW,
            updated_at=NOW,
        )
    )


async def no_sleep(_seconds: float) -> None:
    """Run bounded retry branches deterministically without delaying unit tests."""


def dispatcher(
    harness: Harness, client: FakeWorkflowServiceClient, retries: int = 0
) -> ReliableDispatcher:
    """Bind the real reliable implementation over the selected provider."""
    return ReliableDispatcher(
        client, harness.store, NoopAuditSink(), max_retries=retries, sleep=no_sleep
    )


async def test_production_mesh_authentication_precedes_tenant_storage(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production composition denies forged callers before reads on either metadata backend."""
    from unittest.mock import AsyncMock

    import httpx

    from custos_trigger.api.routes.rpc import compute_resume_id
    from custos_trigger.app import create_app
    from custos_trigger.dedup import Deduplicator
    from custos_trigger.pipeline.dispatch import Dispatcher
    from custos_trigger.providers import Providers

    store = harness.store
    assert isinstance(store, (InMemoryTriggerMetadataStore, PgMetadataAdapter))
    read = store.get_resume_subscription
    tracked = AsyncMock(wraps=read)
    monkeypatch.setattr(store, "get_resume_subscription", tracked)
    monkeypatch.setenv("TRIGGER_WF_ENDPOINT", "workflow-service")
    monkeypatch.setenv("TRIGGER_WORKFLOW_APP_ID", "workflow-service")
    monkeypatch.setenv("TRIGGER_WORKFLOW_NAMESPACE", "custos")
    token = "fixture-only-sidecar-token"
    app = create_app(
        authz_endpoint="http://auth.invalid",
        environment="production",
        app_api_token=token,
        providers=Providers(metadata_store=store),
        dispatcher=Dispatcher(FakeWorkflowServiceClient(), Deduplicator(store)),
        receiver_configuration_enabled=True,
    )
    body = {
        "workspaceId": str(WS),
        "runId": "run",
        "stepId": "wait",
        "eventKey": "ready",
        "deliveryProtocol": 2,
    }
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://trigger",
        ) as client,
    ):
        for headers, status in (
            ({"dapr-caller-app-id": "workflow-service"}, 401),
            ({"dapr-api-token": token, "dapr-caller-app-id": "untrusted-service"}, 403),
        ):
            denied = await client.post("/RegisterResumeSubscription", json=body, headers=headers)
            assert denied.status_code == status
            tracked.assert_not_awaited()
        registered = await client.post(
            "/RegisterResumeSubscription",
            json=body,
            headers={
                "dapr-api-token": token,
                "dapr-caller-app-id": "workflow-service",
                "dapr-caller-namespace": "custos",
            },
        )
        assert registered.status_code == 200 and tracked.await_count > 0
        persisted = await read(WS, compute_resume_id("run", "wait", "ready"))
        assert persisted is not None and persisted.payload["deliveryProtocol"] == 2


async def test_failure_is_captured_before_ack_and_replays_original_target(harness: Harness) -> None:
    """Failures are durable, private data is excluded and operator replay never rematches config."""
    client = FakeWorkflowServiceClient(error=WorkflowClientTransportError("do-not-store"))
    reliable = dispatcher(harness, client, retries=2)
    original = match()
    result = await reliable.dispatch_start(event(), original)
    assert result.is_dead_lettered and len(client.start_run_calls) == 3
    page = await harness.store.query_dead_letters(WS)
    assert len(page.items) == 1
    entry = page.items[0]
    assert len(entry.data.history) == 3
    assert "do-not-store" not in encode_dead_letter(entry)
    key = compute_dedup_key("sub", "event")
    receipt = await harness.store.get_delivery(WS, "trigger", key)
    assert receipt is not None and receipt.completed
    assert json.loads(receipt.result_json or "{}") == {"deadLetterId": entry.data.entry_id}
    assert (await reliable.dispatch_start(event(), original)).is_duplicate
    assert len(client.start_run_calls) == 3
    original.subscription.target_workflow_version_id = "changed"
    original.subscription.input_mapping = {"changed": True}
    client.error = None
    replayed = await reliable.replay(str(WS), entry.data.entry_id, "operator")
    assert replayed.is_dispatched
    assert client.start_run_calls[-1].workflow_version_id == "version"
    assert client.start_run_calls[-1].inputs == {"value": 1}
    assert {call.idempotency_key for call in client.start_run_calls} == {key}
    assert client.start_run_calls[-1].delivery_protocol == 2
    final = await harness.store.get_dead_letter(WS, entry.data.entry_id)
    assert final is not None and final.status == "replayed"


async def test_replay_claim_recovers_worker_death(harness: Harness) -> None:
    """Abandoned attempts remain visible; only the new claim can commit the replay result."""
    client = FakeWorkflowServiceClient(error=WorkflowClientTransportError("unavailable"))
    reliable = dispatcher(harness, client)
    await reliable.dispatch_start(event(), match())
    entry = (await harness.store.query_dead_letters(WS)).items[0]
    abandoned = await harness.store.claim_dead_letter(WS, entry.data.entry_id, "dead-worker")
    await harness.expire_replay(abandoned)
    client.error = None
    assert (await reliable.replay(str(WS), entry.data.entry_id, "new-worker")).is_dispatched
    recovered = await harness.store.get_dead_letter(WS, entry.data.entry_id)
    assert recovered is not None
    assert [attempt.outcome for attempt in recovered.replay_history] == ["interrupted", "replayed"]


async def test_abandoned_final_replay_persists_interruption_without_another_attempt(
    harness: Harness,
) -> None:
    """Exhausted recovery records the interrupted attempt instead of staying stuck replaying."""
    client = FakeWorkflowServiceClient(error=WorkflowClientTransportError("unavailable"))
    reliable = ReliableDispatcher(
        client,
        harness.store,
        NoopAuditSink(),
        max_retries=0,
        max_replay_attempts=1,
    )
    await reliable.dispatch_start(event(), match())
    entry = (await harness.store.query_dead_letters(WS)).items[0]
    abandoned = await harness.store.claim_dead_letter(WS, entry.data.entry_id, "dead-worker")
    await harness.expire_replay(abandoned)
    client.error = None
    for _ in range(2):
        with pytest.raises(TriggerError) as exhausted:
            await reliable.replay(str(WS), entry.data.entry_id, "new-worker")
        assert exhausted.value.kind is TriggerErrorKind.REPLAY_UNSAFE
        recovered = await harness.store.get_dead_letter(WS, entry.data.entry_id)
        assert recovered is not None and recovered.status == "discarded"
        assert recovered.claim is None
        assert [attempt.outcome for attempt in recovered.replay_history] == ["interrupted"]
        assert recovered.replay_history[0].finished_at is not None
        assert len(client.start_run_calls) == 1


async def test_configuration_and_legacy_targets_fail_closed(harness: Harness) -> None:
    """Keep unsafe entries inspectable without issuing a new key or attempting a blind replay."""
    client = FakeWorkflowServiceClient()
    reliable = dispatcher(harness, client)
    missing = match()
    missing.subscription.target_workflow_version_id = None
    assert (await reliable.dispatch_start(event(), missing)).is_dead_lettered
    entry = (await harness.store.query_dead_letters(WS)).items[0]
    assert entry.data.history[-1].code == "configuration" and not client.start_run_calls
    with pytest.raises(TriggerError) as unsafe:
        await reliable.replay(str(WS), entry.data.entry_id, "operator")
    assert unsafe.value.kind is TriggerErrorKind.REPLAY_UNSAFE
    legacy = ResumeMatch(
        resume_id="resume",
        registration=ResumeRegistration(run_id="run", step_id="step", event_key="ready"),
    )
    assert (await reliable.dispatch_resume(event(), legacy, workspace_id=str(WS))).is_dead_lettered
    assert not client.raise_event_calls
    entries = (await harness.store.query_dead_letters(WS)).items
    assert any(row.data.history[-1].code == "unsafe" for row in entries)


async def test_expired_resume_cannot_rebind_delivery_protocol(harness: Harness) -> None:
    """Both providers preserve the protocol even when an expired registration is refreshed."""
    store = harness.store
    assert isinstance(store, (InMemoryTriggerMetadataStore, PgMetadataAdapter))
    now = await store.delivery_now()
    original = ResumeSubscription(
        workspace_id=WS,
        resume_id="resume",
        run_id=SplRunId("run"),
        step_id=StepId("step"),
        expires_at=now - timedelta(seconds=1),
        payload={"eventKey": "ready", "deliveryProtocol": 2},
    )
    await store.register_resume_subscription(WS, original, as_of=now)
    changed = replace(
        original,
        expires_at=now + timedelta(hours=1),
        payload={"eventKey": "ready", "deliveryProtocol": 1},
    )
    winner = await store.register_resume_subscription(WS, changed, as_of=now)
    assert winner.payload["deliveryProtocol"] == 2
    assert winner.expires_at == original.expires_at


class PausedFailure(FakeWorkflowServiceClient):
    """Let an old producer fail only after a newer owner has already committed success."""

    def __init__(self) -> None:
        """Expose barriers for an actual overlapping coroutine schedule."""
        super().__init__()
        self.started, self.finish = asyncio.Event(), asyncio.Event()
        self.count = 0

    async def start_run(self, request: StartRunRequest) -> RunRef:
        """Block the first attempt, then fail it; subsequent attempts succeed normally."""
        self.count += 1
        if self.count == 1:
            self.started.set()
            await self.finish.wait()
            raise WorkflowClientTransportError("old attempt failed")
        return await super().start_run(request)


async def test_stale_failure_cannot_create_phantom_dead_letter(harness: Harness) -> None:
    """Capture and receipt completion share the fence, not merely a pre-insert ownership check."""
    client = PausedFailure()
    reliable = dispatcher(harness, client)
    first = asyncio.create_task(reliable.dispatch_start(event(), match()))
    try:
        await asyncio.wait_for(client.started.wait(), timeout=5)
        await harness.expire_receipt(compute_dedup_key("sub", "event"))
        assert (await reliable.dispatch_start(event(), match())).is_dispatched
        client.finish.set()
        with pytest.raises(TriggerError) as stale:
            await first
        assert stale.value.kind is TriggerErrorKind.DISPATCH_UNAVAILABLE
        assert not (await harness.store.query_dead_letters(WS)).items
    finally:
        client.finish.set()
        if not first.done():
            first.cancel()
            await asyncio.gather(first, return_exceptions=True)


async def test_pending_delivery_recovers_original_target_after_configuration_change(
    harness: Harness,
) -> None:
    """A crash before capture cannot force redelivery to use a newly edited subscription."""
    client = PausedFailure()
    reliable = dispatcher(harness, client)
    first = asyncio.create_task(reliable.dispatch_start(event(), match()))
    await asyncio.wait_for(client.started.wait(), timeout=5)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    key = compute_dedup_key("sub", "event")
    pending = await harness.store.get_delivery(WS, "trigger", key)
    assert pending is not None and pending.request_json is not None
    assert "do-not-store" not in pending.request_json
    assert "history" not in json.loads(pending.request_json)
    await harness.expire_receipt(key)
    changed = match()
    changed.subscription.target_workflow_version_id = "edited-version"
    changed.subscription.input_mapping = {"authorization": "must-not-replace-original"}
    recovered = dispatcher(harness, client)
    recovered.retention = 1
    assert (await recovered.dispatch_start(event(), changed)).is_dispatched
    assert client.start_run_calls[-1].workflow_version_id == "version"
    assert client.start_run_calls[-1].inputs == {"value": 1}
    done = await harness.store.get_delivery(WS, "trigger", key)
    assert done is not None and done.completed and done.request_json is None


async def test_retry_budget_reserves_time_for_capture_and_replay_finalization() -> None:
    """Measure finalization headroom with enough retries to exhaust the former lease budget."""
    clock = [NOW]
    store = InMemoryTriggerMetadataStore(now=lambda: clock[0])

    async def advance(seconds: float) -> None:
        """Advance provider time rather than making a long wall-clock test."""
        clock[0] += timedelta(seconds=seconds)

    client = FakeWorkflowServiceClient(error=WorkflowClientTransportError("outage"))
    reliable = ReliableDispatcher(
        client,
        store,
        NoopAuditSink(),
        max_retries=63,
        sleep=advance,
    )
    assert (await reliable.dispatch_start(event(), match())).is_dead_lettered
    assert (clock[0] - NOW).total_seconds() == MAX_DISPATCH_SECONDS - FINALIZATION_MARGIN_SECONDS
    entry = (await store.query_dead_letters(WS)).items[0]
    assert 5 < len(entry.data.history) <= 64
    assert (await reliable.replay(str(WS), entry.data.entry_id, "operator")).is_dead_lettered
    updated = await store.get_dead_letter(WS, entry.data.entry_id)
    assert updated is not None and updated.replay_history[-1].outcome == "failed"
    assert (clock[0] - NOW).total_seconds() == 2 * (
        MAX_DISPATCH_SECONDS - FINALIZATION_MARGIN_SECONDS
    )


async def test_expired_orphan_snapshot_is_erased_without_rebinding_identity(
    harness: Harness,
) -> None:
    """Payload retention is bounded even when a producer dies before capture or completion."""
    client = PausedFailure()
    reliable = dispatcher(harness, client)
    task = asyncio.create_task(reliable.dispatch_start(event(), match()))
    await asyncio.wait_for(client.started.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    key = compute_dedup_key("sub", "event")
    pending = await harness.store.get_delivery(WS, "trigger", key)
    assert pending is not None and pending.request_json is not None
    assert await harness.store.clear_expired_delivery_requests(WS) == 0
    if harness.pool is None:
        harness.clock[0] = pending.retain_until
    else:
        async with harness.pool.acquire() as conn:
            await conn.execute(
                "UPDATE custos_state.delivery_receipt SET retain_until=clock_timestamp(), "
                "token=NULL,lease_until=NULL "
                "WHERE workspace_id=$1 AND namespace='trigger' AND key=$2",
                WS,
                key,
            )
    assert await harness.store.clear_expired_delivery_requests(WorkspaceId("other")) == 0
    assert await harness.store.clear_expired_delivery_requests(WS, limit=1) == 1
    assert await harness.store.clear_expired_delivery_requests(WS) == 0
    expired = await harness.store.get_delivery(WS, "trigger", key)
    assert expired is not None and expired.request_json is None
    assert expired.request_hash == pending.request_hash
    with pytest.raises(TriggerError) as unavailable:
        await reliable.dispatch_start(event(), match())
    assert unavailable.value.kind is TriggerErrorKind.REPLAY_UNSAFE


@pytest.mark.integration
async def test_postgres_capture_and_ack_roll_back_together(pg_dsn: str) -> None:
    """A failure after inserting the capture rolls it back; no success-shaped receipt survives."""
    import asyncpg

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    assert pool is not None
    try:
        store = PgMetadataAdapter(pool=pool)
        async with pool.acquire() as conn:
            await conn.execute(
                "ALTER TABLE custos_state.delivery_receipt ADD CONSTRAINT reject_completion "
                "CHECK (result_json IS NULL)"
            )
        client = FakeWorkflowServiceClient(error=WorkflowClientTransportError("unavailable"))
        reliable = ReliableDispatcher(client, store, NoopAuditSink(), max_retries=0)
        with pytest.raises(TriggerError) as failure:
            await reliable.dispatch_start(event(), match())
        assert failure.value.kind is TriggerErrorKind.DISPATCH_UNAVAILABLE
        assert not (await store.query_dead_letters(WS)).items
        row = await store.get_delivery(WS, "trigger", compute_dedup_key("sub", "event"))
        assert row is not None and not row.completed
    finally:
        await pool.close()

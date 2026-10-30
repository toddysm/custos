"""Memory/PostgreSQL parity through protected administration routes and audited retention."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock

import httpx
import pytest
from custos_spl.errors import BackendUnavailable
from custos_spl.interfaces.dead_letters import DeadLetterFilter
from fastapi import Depends, FastAPI, Request

from custos_trigger.app import create_app
from custos_trigger.clients import FakeWorkflowServiceClient
from custos_trigger.clients.workflow import (
    RunRef,
    StartRunRequest,
    WorkflowClientReplayUnsafeError,
    WorkflowClientTransportError,
)
from custos_trigger.dead_letters import DeadLetterAdmin, DeadLetterAdminError
from custos_trigger.dedup import Deduplicator, compute_dedup_key
from custos_trigger.errors import TriggerError, TriggerErrorKind
from custos_trigger.pipeline.dispatch import Dispatcher, NoopAuditSink
from custos_trigger.pipeline.reliable import ReliableDispatcher, ReliableStorage
from custos_trigger.providers import InMemoryTriggerMetadataStore, Providers
from custos_trigger.stores.base import TriggerMetadataStore
from tests.integration import test_dead_letter_state as state_tests
from tests.integration.test_dead_letter_state import (
    WS,
    Harness,
    capture,
)

harness = state_tests.harness
dead_letter_dsn = state_tests.dead_letter_dsn

BASE = f"/v1/workspaces/{WS}/triggers/deadletter"
HEADERS = {
    "x-custos-callctx": json.dumps(
        {
            "workspace_id": WS,
            "principal_id": "operator",
            "permissions": ["trigger:admin"],
        }
    )
}


def administration(harness: Harness, client: FakeWorkflowServiceClient) -> DeadLetterAdmin:
    """Assemble production dispatch/recovery against either authoritative storage backend."""
    assert isinstance(harness.store, ReliableStorage)
    return DeadLetterAdmin(
        ReliableDispatcher(client, harness.store, NoopAuditSink(), max_retries=0)
    )


async def audits(harness: Harness) -> list[str]:
    """Read actual durable audit rows or the memory backend's equivalent transition evidence."""
    if harness.pool is None:
        assert isinstance(harness.store, InMemoryTriggerMetadataStore)
        return [event.event_type for event in harness.store.dead_letter_audits]
    async with harness.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT event_id,event_type FROM custos_state.audit_event "
            "WHERE workspace_id=$1 ORDER BY event_id",
            WS,
        )
        outbox = await conn.fetch(
            "SELECT event_id,event_type FROM custos_state.audit_outbox "
            "WHERE workspace_id=$1 ORDER BY event_id",
            WS,
        )
        assert rows == outbox
        return [row["event_type"] for row in rows]


async def test_protected_routes_replay_original_target_and_idempotent_outcomes(
    harness: Harness,
) -> None:
    """Real handlers preserve targets, reject unsafe mutations and isolate tenant identities."""
    client = FakeWorkflowServiceClient()
    admin = administration(harness, client)
    data = capture()
    await harness.store.put_dead_letter(
        replace(data, target={**data.target, "deliveryProtocol": 2})
    )
    await harness.store.put_dead_letter(
        replace(capture("discard"), target={**data.target, "deliveryProtocol": 2})
    )
    store = cast(TriggerMetadataStore, harness.store)
    app = create_app(
        authz_endpoint="",
        providers=Providers(store),
        dispatcher=Dispatcher(client, Deduplicator(store), reliable=admin.dispatcher),
    )
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://trigger",
        ) as http,
    ):
        assert (await http.get(BASE)).status_code == 401
        denied = {
            "x-custos-callctx": json.dumps(
                {
                    "workspace_id": WS,
                    "principal_id": "reader",
                    "permissions": ["trigger:subscriptions:read"],
                }
            )
        }
        for method, suffix in (
            ("GET", ""),
            ("GET", "/entry"),
            ("POST", "/entry:replay"),
            ("POST", "/entry:discard"),
            ("POST", ":replay"),
        ):
            assert (await http.request(method, BASE + suffix, json={})).status_code == 401
            assert (
                await http.request(method, BASE + suffix, headers=denied, json={})
            ).status_code == 403
        assert (await http.get(BASE, headers=denied)).status_code == 403
        assert (await http.get(BASE.replace(str(WS), "other"), headers=HEADERS)).status_code == 403
        response = await http.get(BASE + "/entry", headers=HEADERS)
        assert response.status_code == 200
        entry = response.json()
        assert entry["schemaVersion"] == 1 and entry["target"]["inputs"] == {"v": 1}
        assert "private" not in response.text and "token" not in response.text
        for _ in range(2):
            replay = await http.post(BASE + "/entry:replay", headers=HEADERS)
            assert replay.status_code == 200 and replay.json() == {"status": "replayed"}
        assert len(client.start_run_calls) == 1
        assert client.start_run_calls[0].inputs == {"v": 1}
        assert client.start_run_calls[0].idempotency_key == "original-key"
        assert (await http.post(BASE + "/entry:discard", headers=HEADERS)).status_code == 409
        for _ in range(2):
            assert (
                await http.post(
                    BASE + "/discard:discard", headers=HEADERS, json={"reason": "operator cleanup"}
                )
            ).status_code == 204
        assert (await http.post(BASE + "/discard:replay", headers=HEADERS)).status_code == 409
        assert (await http.get(BASE + "/unknown", headers=HEADERS)).status_code == 404
        for query in ("limit=101", "status=expired", "since=2026-01-01", "cursor=invalid"):
            bad = await http.get(BASE + "?" + query, headers=HEADERS)
            assert bad.status_code == 422 and bad.json()["code"] == "trigger.deadletter.invalid"
        assert (
            await http.post(BASE + ":replay", headers=HEADERS, json={"limit": 101})
        ).status_code == 422
        assert (
            await http.post(BASE + ":replay", headers=HEADERS, json={"unexpected": True})
        ).status_code == 422
        bulk = await http.post(BASE + ":replay", headers=HEADERS, json={})
        assert bulk.status_code == 200 and bulk.json() == {"accepted": 1, "rejected": 1}
    events = await audits(harness)
    assert events.count("trigger.deadletter.stored") == 2
    assert events.count("trigger.deadletter.replayed") == 1
    assert events.count("trigger.deadletter.discarded") == 1


async def test_pages_bind_filters_and_workspace(harness: Harness) -> None:
    """Pagination is stable across inserts and cannot be reused under different filters."""
    admin = administration(harness, FakeWorkflowServiceClient())
    for identifier in ("a", "c", "e"):
        await harness.store.put_dead_letter(capture(identifier))
    first = await admin.list(str(WS), DeadLetterFilter(), limit=2)
    assert [row.data.entry_id for row in first.items] == ["a", "c"]
    assert first.next_cursor is not None
    await harness.store.put_dead_letter(capture("b"))
    second = await admin.list(str(WS), DeadLetterFilter(), cursor=first.next_cursor.token, limit=2)
    assert [row.data.entry_id for row in second.items] == ["e"] and second.next_cursor is None
    for workspace, filters in (
        (str(WS), DeadLetterFilter(status="pending")),
        ("other", DeadLetterFilter()),
    ):
        with pytest.raises(DeadLetterAdminError, match="pagination"):
            await admin.list(workspace, filters, cursor=first.next_cursor.token)
    with pytest.raises(DeadLetterAdminError, match="not found"):
        await admin.inspect("other", "a")


@pytest.mark.parametrize("audit_failure", [False, True])
async def test_failure_capture_audit_and_receipt_acknowledgement_are_atomic(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    audit_failure: bool,
) -> None:
    """Actual dispatch cannot acknowledge a failed batch unless capture and audit both commit."""
    from unittest.mock import Mock

    from tests.integration.test_reliable_dispatch import event, match

    client = FakeWorkflowServiceClient(error=WorkflowClientTransportError("upstream unavailable"))
    admin = administration(harness, client)
    original = match()
    original.subscription.workspace_id = str(WS)
    if audit_failure:
        if harness.pool is None:
            monkeypatch.setattr(
                "custos_spl.delivery_memory.dead_letter_transition_audits",
                Mock(side_effect=BackendUnavailable("audit unavailable")),
            )
        else:
            monkeypatch.setattr(
                harness.store,
                "append_audit",
                AsyncMock(side_effect=BackendUnavailable("audit unavailable")),
            )
        with pytest.raises(TriggerError) as failure:
            await admin.dispatcher.dispatch_start(event(), original)
        assert failure.value.kind == TriggerErrorKind.DISPATCH_UNAVAILABLE
    else:
        assert (await admin.dispatcher.dispatch_start(event(), original)).is_dead_lettered
    receipt = await admin.store.get_delivery(WS, "trigger", compute_dedup_key("sub", "event"))
    assert receipt is not None and receipt.completed is not audit_failure
    assert len((await harness.store.query_dead_letters(WS)).items) == int(not audit_failure)
    assert (await audits(harness)).count("trigger.deadletter.stored") == int(not audit_failure)


async def test_gateway_forwards_scoped_recovery_to_real_trigger_handlers(harness: Harness) -> None:
    """Exercise Gateway forwarding and Trigger recovery against both storage backends."""
    from custos_gateway.middleware.auth import AUTH_STATE_ATTR, AuthorizedCaller
    from custos_gateway.middleware.callctx_mint import OUTBOUND_METADATA_STATE_ATTR
    from custos_gateway.router import DownstreamRouter
    from custos_gateway.routes.registry import M1_ROUTE_REGISTRY, _make_forwarder

    client = FakeWorkflowServiceClient()
    admin = administration(harness, client)
    data = capture()
    await harness.store.put_dead_letter(
        replace(data, target={**data.target, "deliveryProtocol": 2})
    )
    store = cast(TriggerMetadataStore, harness.store)
    trigger = create_app(
        authz_endpoint="",
        providers=Providers(store),
        dispatcher=Dispatcher(client, Deduplicator(store), reliable=admin.dispatcher),
    )
    gateway = FastAPI()

    async def authorized(request: Request) -> None:
        """Bind the same already-authorized context used by Gateway's tested forwarding seam."""
        setattr(
            request.state,
            AUTH_STATE_ATTR,
            AuthorizedCaller(
                principal_id="operator",
                audit_event_id="test",
                permission="trigger:admin",
                workspace_id=str(WS),
            ),
        )
        setattr(request.state, OUTBOUND_METADATA_STATE_ATTR, HEADERS)

    for spec in M1_ROUTE_REGISTRY:
        if spec.required_permission == "trigger:admin":
            gateway.add_api_route(
                spec.path,
                _make_forwarder(spec),
                methods=[spec.method],
                dependencies=[Depends(authorized)],
            )
    async with (
        trigger.router.lifespan_context(trigger),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=trigger),
            base_url="http://trigger",
        ) as downstream,
    ):

        async def sidecar(request: httpx.Request) -> httpx.Response:
            """Pass actual serialized bytes and scope to the owning Trigger HTTP endpoint."""
            path = request.url.path.removeprefix("/v1.0/invoke/trigger-service/method")
            return await downstream.request(
                request.method,
                path,
                headers=request.headers,
                content=request.content,
                params=request.url.params,
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(sidecar)) as transport:
            gateway.state.downstream_router = DownstreamRouter(transport)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=gateway),
                base_url="http://gateway",
            ) as http:
                listing = await http.get(BASE + "?limit=1")
                assert (
                    listing.status_code == 200 and listing.json()["items"][0]["entryId"] == "entry"
                )
                for _ in range(2):
                    response = await http.post(
                        BASE + "/entry:replay", headers={"content-type": "application/json"}
                    )
                    assert response.status_code == 200 and response.json() == {"status": "replayed"}
    assert len(client.start_run_calls) == 1
    assert (await audits(harness)).count("trigger.deadletter.replayed") == 1


def test_administration_openapi_documents_versioned_shapes_and_bounds() -> None:
    """Generated schemas use the shared wire aliases and exact response/filter shapes."""
    schema = create_app(authz_endpoint="").openapi()
    base = "/v1/workspaces/{workspace_id}/triggers/deadletter"
    assert set(schema["paths"][base]) == {"get"}
    entry = schema["components"]["schemas"]["EntryView"]["properties"]
    assert entry["schemaVersion"]["const"] == 1
    assert {"entryId", "workspaceId", "replayHistory", "claimExpiresAt"} <= set(entry)
    assert "claim" not in entry and "token" not in entry
    selector = schema["components"]["schemas"]["BulkSelector"]
    assert selector["additionalProperties"] is False
    assert selector["properties"]["limit"]["maximum"] == 100
    assert schema["components"]["schemas"]["PageView"]["properties"]["nextCursor"]


@pytest.mark.parametrize("enabled", [False, True])
async def test_disabled_dispatch_and_storage_outage_fail_explicitly(
    monkeypatch: pytest.MonkeyPatch,
    enabled: bool,
) -> None:
    """Unavailable recovery never succeeds using an empty list or a volatile substitute."""
    store = InMemoryTriggerMetadataStore()
    workflow = FakeWorkflowServiceClient()
    reliable = ReliableDispatcher(workflow, store, NoopAuditSink()) if enabled else None
    app = create_app(
        authz_endpoint="",
        providers=Providers(store),
        dispatcher=Dispatcher(workflow, Deduplicator(store), reliable=reliable),
    )
    query = AsyncMock(side_effect=BackendUnavailable("private database connection detail"))
    monkeypatch.setattr(store, "query_dead_letters", query)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://trigger",
        ) as client,
    ):
        response = await client.get(BASE, headers=HEADERS)
    assert response.status_code == 503
    assert response.json()["code"] == "trigger.deadletter.unavailable"
    assert "private" not in response.text
    assert query.await_count == int(enabled)


async def test_concurrent_replay_and_discard_conflict(harness: Harness) -> None:
    """One real held downstream call permits neither another effect nor concurrent discard."""
    started, release = asyncio.Event(), asyncio.Event()

    class HeldClient(FakeWorkflowServiceClient):
        """Hold only the outbound start; use actual provider claim fencing."""

        async def start_run(self, request: StartRunRequest) -> RunRef:
            """Release the first call only after conflicting mutations are checked."""
            started.set()
            await release.wait()
            return await super().start_run(request)

    client = HeldClient()
    admin = administration(harness, client)
    data = capture()
    await harness.store.put_dead_letter(
        replace(data, target={**data.target, "deliveryProtocol": 2})
    )
    task = asyncio.create_task(admin.replay(str(WS), "entry", "operator"))
    try:
        async with asyncio.timeout(5):
            await started.wait()
            with pytest.raises(DeadLetterAdminError) as conflict:
                await admin.replay(str(WS), "entry", "another")
            assert conflict.value.code == "trigger.deadletter.conflict"
            with pytest.raises(DeadLetterAdminError) as conflict:
                await admin.discard(str(WS), "entry", "another")
            assert conflict.value.code == "trigger.deadletter.conflict"
            assert await admin.bulk_replay(str(WS), DeadLetterFilter(), "another") == (0, 1)
            release.set()
            assert await task == "replayed"
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert len(client.start_run_calls) == 1


async def test_replay_failure_and_terminal_attempt_are_durably_audited(harness: Harness) -> None:
    """Failed or unsafe upstream outcomes never masquerade as successful delivery."""
    client = FakeWorkflowServiceClient(error=WorkflowClientTransportError("private vendor error"))
    admin = administration(harness, client)
    data = capture()
    await harness.store.put_dead_letter(
        replace(data, max_replay_attempts=1, target={**data.target, "deliveryProtocol": 2})
    )
    assert await admin.replay(str(WS), "entry", "operator") == "discarded"
    events = await audits(harness)
    assert events.count("trigger.deadletter.replay_failed") == 1
    assert events.count("trigger.deadletter.discarded") == 1
    client.error = WorkflowClientReplayUnsafeError()
    await harness.store.put_dead_letter(
        replace(capture("unsafe"), target={**data.target, "deliveryProtocol": 2})
    )
    with pytest.raises(DeadLetterAdminError) as conflict:
        await admin.replay(str(WS), "unsafe", "operator")
    assert conflict.value.code == "trigger.deadletter.conflict"
    assert (await audits(harness)).count("trigger.deadletter.replay_failed") == 2


@pytest.mark.parametrize("status", ["pending", "replayed", "discarded", "replaying"])
async def test_retention_covers_all_statuses_without_deleting_live_replay(
    harness: Harness,
    status: str,
) -> None:
    """Expiry audit and deletion are atomic; live claims remain protected past retention."""
    admin = administration(harness, FakeWorkflowServiceClient())
    entry = await harness.store.put_dead_letter(capture())
    if status in ("replayed", "replaying"):
        entry = await harness.store.claim_dead_letter(WS, "entry", "operator", ttl_seconds=300)
        assert entry.claim is not None
        if status == "replayed":
            entry = await harness.store.finish_dead_letter(entry.claim, result_id="run")
    elif status == "discarded":
        entry = await harness.store.discard_dead_letter(WS, "entry")
    if status == "replaying" and harness.pool is None:
        harness.clock[0] = entry.created_at + timedelta(seconds=1)
        assert isinstance(harness.store, InMemoryTriggerMetadataStore)
        entry = replace(entry, expires_at=harness.clock[0])
        harness.store._dead_letters[(str(WS), "entry")] = entry
    else:
        await harness.expire(entry, retention=True)
    if status == "replaying":
        assert (await admin.sweep(str(WS))).expired == 0
        await harness.expire(entry)
    assert (await admin.sweep(str(WS))).expired == 1
    assert await harness.store.get_dead_letter(WS, "entry") is None
    assert (await audits(harness)).count("trigger.deadletter.expired") == 1
    assert (await admin.sweep(str(WS))).expired == 0


@pytest.mark.parametrize("operation", ["expire", "discard", "finish", "store"])
async def test_audit_failure_rolls_back_state(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    """Audit failure cannot commit a lifecycle mutation or acknowledge successful replay."""
    admin = administration(harness, FakeWorkflowServiceClient())
    entry = await harness.store.put_dead_letter(capture())
    if operation == "expire":
        await harness.expire(entry, retention=True)
    elif operation == "finish":
        entry = await harness.store.claim_dead_letter(WS, "entry", "operator")
    before = await harness.store.get_dead_letter(WS, "entry")
    events_before = await audits(harness)
    if harness.pool is None:
        from unittest.mock import Mock

        monkeypatch.setattr(
            "custos_trigger.providers.dead_letter_transition_audits",
            Mock(side_effect=BackendUnavailable("audit unavailable")),
        )
    else:
        append = AsyncMock(side_effect=BackendUnavailable("audit unavailable"))
        monkeypatch.setattr(harness.store, "append_audit", append)
    if operation == "expire":
        with pytest.raises(DeadLetterAdminError) as failure:
            await admin.sweep(str(WS))
        assert failure.value.code == "trigger.deadletter.unavailable"
    elif operation == "discard":
        with pytest.raises(DeadLetterAdminError):
            await admin.discard(str(WS), "entry", "operator")
    elif operation == "finish":
        assert entry.claim is not None
        with pytest.raises(BackendUnavailable):
            await harness.store.finish_dead_letter(entry.claim, result_id="run")
    else:
        with pytest.raises(BackendUnavailable):
            await harness.store.put_dead_letter(capture("new"))
        assert await harness.store.get_dead_letter(WS, "new") is None
    assert await harness.store.get_dead_letter(WS, "entry") == before
    assert await audits(harness) == events_before


async def test_bulk_selection_and_concurrency_are_bounded(harness: Harness) -> None:
    """The maximum page issues exactly 100 attempts, never more than eight simultaneously."""
    active, peak = 0, 0
    barrier = asyncio.Event()

    class ConcurrentClient(FakeWorkflowServiceClient):
        """Track real dispatcher concurrency while avoiding time-based sleeps."""

        async def start_run(self, request: StartRunRequest) -> RunRef:
            """Release the first group when the documented concurrency ceiling is reached."""
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            if active == 8:
                barrier.set()
            try:
                await barrier.wait()
                return await super().start_run(request)
            finally:
                active -= 1

    client = ConcurrentClient()
    admin = administration(harness, client)
    data = capture()
    for index in range(101):
        await harness.store.put_dead_letter(
            replace(
                data,
                entry_id=f"entry-{index:03}",
                idempotency_key=f"key-{index:03}",
                target={**data.target, "deliveryProtocol": 2},
            )
        )
    async with asyncio.timeout(15):
        assert await admin.bulk_replay(str(WS), DeadLetterFilter(), "operator", limit=100) == (
            100,
            0,
        )
    assert len(client.start_run_calls) == 100 and peak == 8 and active == 0
    assert (await admin.inspect(str(WS), "entry-100")).status == "pending"
    with pytest.raises(DeadLetterAdminError):
        await admin.bulk_replay(str(WS), DeadLetterFilter(), "operator", limit=101)


async def test_request_deadline_retains_recoverable_claims_and_awaits_children(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real expired request timeout returns unavailable without leaking replay tasks."""
    stopped = asyncio.Event()
    task: asyncio.Task[object] | None = None
    deadline: asyncio.Timeout | None = None
    actual_timeout = asyncio.timeout

    def observe_timeout(delay: float | None) -> asyncio.Timeout:
        """Capture the real command deadline rather than waiting on a timing heuristic."""
        nonlocal deadline
        timeout = actual_timeout(delay)
        if delay == 60:
            deadline = timeout
        return timeout

    class TimedClient(FakeWorkflowServiceClient):
        """Block downstream execution until the actual administration deadline cancels it."""

        async def start_run(self, request: StartRunRequest) -> RunRef:
            """Record cleanup so the caller can prove all owned work was awaited."""
            nonlocal task
            task = asyncio.current_task()
            assert deadline is not None
            deadline.reschedule(asyncio.get_running_loop().time())
            try:
                await asyncio.Future[None]()
            finally:
                stopped.set()
            raise AssertionError("unreachable")

    client = TimedClient()
    admin = administration(harness, client)
    data = capture()
    await harness.store.put_dead_letter(
        replace(data, target={**data.target, "deliveryProtocol": 2})
    )
    monkeypatch.setattr(asyncio, "timeout", observe_timeout)
    with pytest.raises(DeadLetterAdminError) as expired:
        await admin.bulk_replay(str(WS), DeadLetterFilter(), "operator")
    assert expired.value.code == "trigger.deadletter.unavailable"
    assert stopped.is_set() and task is not None and task.done()
    entry = await harness.store.get_dead_letter(WS, "entry")
    assert entry is not None and entry.status == "replaying"
    await harness.expire(entry)
    recovered = await harness.store.recover_dead_letter(WS, "entry")
    assert recovered.status == "pending"
    assert "trigger.deadletter.replay_failed" in await audits(harness)

"""Memory/PostgreSQL delivery contracts and real durable-wait generator execution.

The workflow runtime is the repository fake, not a deployed Dapr certification.
The same registered worker-thread activity bridge uses actual PostgreSQL in the
integration parametrization; restarted adapters never hydrate through test SQL.
"""

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
from custos_spl.errors import BackendUnavailable, ImmutableViolation, LeaseBusy, LeaseExpired
from custos_spl.ids import RunId as SplRunId
from custos_spl.ids import WorkflowId, WorkspaceId
from custos_spl.interfaces.delivery import (
    DeliveryReceipt,
    DeliveryStateProvider,
    WorkflowMirror,
    WorkflowStart,
    delivery_fingerprint,
    resume_channel,
    resume_identity,
    resume_result,
)
from custos_spl.interfaces.metadata_store import MetadataStoreProvider, Run

from custos_workflow.bindings.registry import InMemoryActivityTypeRegistry
from custos_workflow.clients.trigger import FakeTriggerServiceClient
from custos_workflow.document import WorkflowDocument
from custos_workflow.providers import _InProcessMetadataStoreProvider
from custos_workflow.runs.controller import InMemoryLifecycleEventPublisher, WorkflowVersion
from custos_workflow.runs.ids import derive_run_id
from custos_workflow.runs.orchestrator import WORKFLOW_NAME, make_run_orchestrator
from custos_workflow.runs.step_handler import NoopStepHandler
from custos_workflow.runs.store import InProcessRunStore
from custos_workflow.runs.trigger_delivery import TriggerDeliveryController
from custos_workflow.runtime._common import (
    GetRunStateRequest,
    RaiseRunEventRequest,
    ScheduleWorkflowRequest,
)
from custos_workflow.runtime.fake import FakeWorkflowClient, FakeWorkflowRuntime
from custos_workflow.steps.resume import (
    InMemoryResumeSubscriptionMirrorRepository,
    WaitForStepHandler,
)
from custos_workflow.steps.resume.durable import (
    RESUME_EFFECT_ACTIVITY,
    DurableResumeMirrorRepository,
    ResumeActivityHandler,
    build_resume_activity,
)

WS = WorkspaceId("delivery-test")


@dataclass
class Harness:
    """Keep the real metadata and optional delivery capabilities on the same backend."""

    metadata: MetadataStoreProvider
    delivery: DeliveryStateProvider
    clock: list[datetime]
    pool: Pool | None

    async def expire(self, receipt: DeliveryReceipt) -> None:
        """Reach the exact memory boundary or expire only the database claim, not its retention."""
        assert receipt.lease_until is not None
        if self.pool is None:
            self.clock[0] = receipt.lease_until
        else:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE custos_state.delivery_receipt SET lease_until=clock_timestamp() "
                    "WHERE workspace_id=$1 AND namespace=$2 AND key=$3",
                    receipt.workspace_id,
                    receipt.namespace,
                    receipt.key,
                )

    def restart(self) -> tuple[MetadataStoreProvider, DeliveryStateProvider]:
        """Construct an independent production adapter over the same durable state."""
        if self.pool is None:
            return self.metadata, self.delivery
        adapter = PgMetadataAdapter(pool=self.pool)
        return cast(MetadataStoreProvider, adapter), adapter


@pytest.fixture(params=["memory", pytest.param("postgres", marks=pytest.mark.integration)])
def delivery_dsn(request: pytest.FixtureRequest) -> str | None:
    """Use the existing per-test PostgreSQL migration/reset fixture when selected."""
    return None if request.param == "memory" else cast(str, request.getfixturevalue("pg_dsn"))


@pytest.fixture
async def harness(delivery_dsn: str | None) -> AsyncIterator[Harness]:
    """Yield the same public provider contract for memory and real PostgreSQL."""
    clock = [datetime(2026, 10, 29, tzinfo=UTC)]
    if delivery_dsn is None:
        provider = _InProcessMetadataStoreProvider(now=lambda: clock[0])
        yield Harness(cast(MetadataStoreProvider, provider), provider, clock, None)
        return
    import asyncpg

    pool = await asyncpg.create_pool(dsn=delivery_dsn, min_size=1, max_size=4)
    assert pool is not None
    try:
        adapter = PgMetadataAdapter(pool=pool)
        yield Harness(cast(MetadataStoreProvider, adapter), adapter, clock, pool)
    finally:
        await pool.close()


async def test_receipt_recovery_is_fenced_not_duplicate_success(harness: Harness) -> None:
    """Concurrent reservations have one owner; a pending row remains recoverable, not successful."""
    store = harness.delivery
    await store.check_delivery_ready()
    now = await store.delivery_now()
    fingerprint = delivery_fingerprint({"request": 1})
    until = now + timedelta(days=30)
    outcomes = await asyncio.gather(
        *(store.reserve_delivery(WS, "trigger", "key", fingerprint, until) for _ in range(2)),
        return_exceptions=True,
    )
    claims = [value for value in outcomes if isinstance(value, DeliveryReceipt)]
    assert len(claims) == 1 and sum(isinstance(value, LeaseBusy) for value in outcomes) == 1
    first = claims[0]
    assert not first.completed
    await harness.expire(first)
    second = await store.reserve_delivery(WS, "trigger", "key", fingerprint, until)
    assert second.epoch == first.epoch + 1 and second.token != first.token
    with pytest.raises(LeaseExpired):
        await store.release_delivery(first)
    with pytest.raises(LeaseExpired):
        await store.complete_delivery(first, '{"dispatched":true}')
    await store.release_delivery(second)
    third = await store.reserve_delivery(WS, "trigger", "key", fingerprint, until)
    done = await store.complete_delivery(third, '{"dispatched":true}')
    assert done.completed
    replay = await store.reserve_delivery(WS, "trigger", "key", fingerprint, until)
    assert replay.result_json == done.result_json
    with pytest.raises(ImmutableViolation):
        await store.reserve_delivery(
            WS, "trigger", "key", delivery_fingerprint({"request": 2}), until
        )


async def test_consumer_acknowledgement_survives_expired_producer(harness: Harness) -> None:
    """Persist consumption after producer death without granting stale write rights."""
    store = harness.delivery
    until = await store.delivery_now() + timedelta(days=30)
    key, fingerprint = resume_identity(str(WS), "run", "step", "ready", "event", {"value": 1})
    claim = await store.reserve_delivery(WS, "workflow-resume", key, fingerprint, until)
    await harness.expire(claim)
    done = await store.acknowledge_delivery(WS, key, fingerprint, resume_result(key))
    assert done.completed
    assert await store.complete_delivery(claim, resume_result(key)) == done
    with pytest.raises(ImmutableViolation):
        await store.complete_delivery(claim, '{"different":true}')
    with pytest.raises(ImmutableViolation):
        await store.acknowledge_delivery(WorkspaceId("other"), key, fingerprint, resume_result(key))
    with pytest.raises(LeaseExpired):
        await store.release_delivery(claim)


async def test_busy_retry_extends_retention_without_granting_ownership(harness: Harness) -> None:
    """A later recovery horizon survives even when the caller must retry an in-flight operation."""
    store = harness.delivery
    now = await store.delivery_now()
    fingerprint = delivery_fingerprint({"request": "same"})
    claim = await store.reserve_delivery(
        WS, "workflow-resume", "key", fingerprint, now + timedelta(days=1)
    )
    with pytest.raises(LeaseBusy):
        await store.reserve_delivery(
            WS, "workflow-resume", "key", fingerprint, now + timedelta(days=30)
        )
    current = await store.get_delivery(WS, "workflow-resume", "key")
    assert current is not None and current.retain_until == now + timedelta(days=30)
    assert current.epoch == claim.epoch and current.token == claim.token
    assert current.lease_until == claim.lease_until


async def test_atomic_start_and_legacy_collision(harness: Harness) -> None:
    """Run/input pairs survive adapter replacement and reject unversioned run collisions."""
    now = await harness.delivery.delivery_now()
    row = Run(WS, SplRunId("run"), WorkflowId("wf"), "v", "queued", None, now, now)
    body = {
        "workspace_id": str(WS),
        "workflow_version_id": "v",
        "inputs": {},
        "delivery_protocol": 2,
        "compiled_graph_json": "{}",
        "idempotency_key": "key",
    }
    fingerprint = delivery_fingerprint({"workflowVersionId": "v", "inputs": {}})
    snapshot = WorkflowStart(WS, "run", fingerprint, json.dumps(body))
    assert await harness.delivery.put_workflow_start(row, snapshot) == snapshot
    metadata, store = harness.restart()
    assert await metadata.get_run(WS, SplRunId("run")) == row
    assert await store.get_workflow_start(WS, "run") == snapshot
    with pytest.raises(ImmutableViolation):
        await store.put_workflow_start(
            row,
            WorkflowStart(
                WS,
                "run",
                delivery_fingerprint({"workflowVersionId": "v", "inputs": {"changed": True}}),
                json.dumps({**body, "inputs": {"changed": True}}),
            ),
        )
    legacy = replace(row, run_id=SplRunId("legacy"))
    await metadata.put_run(WS, legacy)
    with pytest.raises(ImmutableViolation, match="replay_unsafe"):
        await store.put_workflow_start(legacy, replace(snapshot, run_id="legacy"))
    assert await store.get_workflow_start(WS, "legacy") is None
    assert await store.get_workflow_start(WorkspaceId("other"), "run") is None


async def test_scoped_mirror_pages_and_association_immutability(harness: Harness) -> None:
    """Mirror metadata can refresh but workspace/run/step/source association cannot be rebound."""
    now = await harness.delivery.delivery_now()
    fingerprint = delivery_fingerprint({"run": "run"})
    for identifier in ("a", "b", "c"):
        await harness.delivery.put_workflow_mirror(
            WorkflowMirror(
                WS, identifier, "run", "step", fingerprint, now + timedelta(hours=1), '{"value":1}'
            )
        )
    await harness.delivery.put_workflow_mirror(
        WorkflowMirror(WorkspaceId("other"), "a", "run", "step", fingerprint, now, '{"value":2}')
    )
    page = await harness.delivery.query_workflow_mirrors(WS, run_id="run", step_id="step", limit=2)
    assert [row.mirror_id for row in page.items] == ["a", "b"]
    assert page.next_cursor is not None
    tail = await harness.delivery.query_workflow_mirrors(
        WS, run_id="run", step_id="step", limit=2, cursor=page.next_cursor
    )
    assert [row.mirror_id for row in tail.items] == ["c"] and tail.next_cursor is None
    with pytest.raises(ValueError):
        await harness.delivery.query_workflow_mirrors(WorkspaceId("other"), cursor=page.next_cursor)
    with pytest.raises(ImmutableViolation):
        await harness.delivery.put_workflow_mirror(
            replace(page.items[0], identity_hash=delivery_fingerprint({"new": 1}))
        )
    await harness.delivery.delete_workflow_mirror(WS, "a")
    assert len((await harness.delivery.query_workflow_mirrors(WorkspaceId("other"))).items) == 1


def version() -> WorkflowVersion:
    """Publish two successive same-name waits to expose unsafe channel reuse."""
    document = WorkflowDocument.model_validate(
        {
            "apiVersion": "custos.dev/v1",
            "kind": "Workflow",
            "metadata": {"name": "delivery", "workspace": str(WS)},
            "spec": {
                "steps": [
                    {"id": "first", "waitFor": {"eventKey": "${{ 'ready' }}", "ttl": "PT1H"}},
                    {
                        "id": "second",
                        "needs": ["first"],
                        "waitFor": {"eventKey": "${{ 'ready' }}", "ttl": "PT1H"},
                    },
                ]
            },
        }
    )
    return WorkflowVersion("version", "workflow", "delivery", "v1", document)


class AmbiguousClient(FakeWorkflowClient):
    """Simulate accepted runtime requests whose transport acknowledgement is lost."""

    def __init__(self, runtime: FakeWorkflowRuntime) -> None:
        """Retain the actual runtime while controlling one uncertainty window at a time."""
        super().__init__(runtime=runtime)
        self.fail_start = False
        self.defer_event = False
        self.buffered: list[RaiseRunEventRequest] = []

    async def schedule_new_workflow(self, request: ScheduleWorkflowRequest) -> str:
        """Lose the first acknowledgement after actual scheduling, not before the effect."""
        result = await super().schedule_new_workflow(request)
        if self.fail_start:
            self.fail_start = False
            raise OSError("simulated acknowledgement loss")
        return result

    async def raise_workflow_event(self, request: RaiseRunEventRequest) -> None:
        """Model runtime inbox acceptance before asynchronous consumer execution."""
        if self.defer_event:
            self.defer_event = False
            self.buffered.append(request)
            raise OSError("simulated inbox acknowledgement loss")
        await super().raise_workflow_event(request)

    async def flush(self) -> None:
        """Deliver an older accepted copy after a retry has already reached the consumer."""
        for request in self.buffered:
            await super().raise_workflow_event(request)
        self.buffered.clear()


async def setup_runtime(
    harness: Harness,
) -> tuple[FakeWorkflowRuntime, AmbiguousClient, FakeTriggerServiceClient]:
    """Register the actual durable activity bridge on a worker-thread-driven repository runtime."""
    runtime = FakeWorkflowRuntime(now=await harness.delivery.delivery_now())
    trigger = FakeTriggerServiceClient()
    handler = WaitForStepHandler(InMemoryResumeSubscriptionMirrorRepository())
    runtime.register_workflow(
        make_run_orchestrator(NoopStepHandler(), resume_handler=handler), name=WORKFLOW_NAME
    )
    runtime.register_activity(
        build_resume_activity(
            ResumeActivityHandler(harness.delivery, trigger), asyncio.get_running_loop()
        ),
        name=RESUME_EFFECT_ACTIVITY,
    )
    await runtime.start()
    return runtime, AmbiguousClient(runtime), trigger


async def test_start_ack_loss_recovers_original_runtime_and_snapshot(harness: Harness) -> None:
    """Restarted controllers recover persisted inputs and never recreate the already-started run."""
    runtime, client, trigger = await setup_runtime(harness)
    try:
        controller = TriggerDeliveryController(
            harness.delivery,
            InProcessRunStore(harness.metadata),
            client,
            InMemoryActivityTypeRegistry({}),
            InMemoryLifecycleEventPublisher(),
        )
        until = await harness.delivery.delivery_now() + timedelta(days=30)
        client.fail_start = True
        from custos_workflow.runs.errors import WorkflowRuntimeUnavailableError

        with pytest.raises(WorkflowRuntimeUnavailableError):
            await controller.start(str(WS), version(), {}, "start-key", until)
        metadata, provider = harness.restart()
        restarted = TriggerDeliveryController(
            provider,
            InProcessRunStore(metadata),
            client,
            InMemoryActivityTypeRegistry({}),
            InMemoryLifecycleEventPublisher(),
        )
        result = await restarted.lookup_start(str(WS), "version", {}, "start-key", until)
        assert result is not None and result.run_id == derive_run_id(str(WS), "start-key")
        assert len(trigger.register_calls) == 1
        assert trigger.register_calls[0].delivery_protocol == 2
        mirrors = await DurableResumeMirrorRepository(provider, str(WS)).list_open(
            str(result.run_id)
        )
        assert len(mirrors) == 1 and mirrors[0].step_id == "first"
        assert mirrors[0].delivery_protocol == 2
    finally:
        await runtime.shutdown()


async def test_uncertain_resume_cannot_consume_later_same_name_wait(harness: Harness) -> None:
    """Receipts and distinct channels close the send/commit window across controller restart."""
    runtime, client, trigger = await setup_runtime(harness)
    try:
        controller = TriggerDeliveryController(
            harness.delivery,
            InProcessRunStore(harness.metadata),
            client,
            InMemoryActivityTypeRegistry({}),
            InMemoryLifecycleEventPublisher(),
        )
        until = await harness.delivery.delivery_now() + timedelta(days=30)
        ref = await controller.start(str(WS), version(), {}, "start-key", until)
        client.defer_event = True
        from custos_workflow.runs.errors import WorkflowRuntimeUnavailableError

        with pytest.raises(WorkflowRuntimeUnavailableError):
            await controller.resume(
                str(WS), str(ref.run_id), "first", "ready", {"value": 1}, "event-1", until
            )
        metadata, provider = harness.restart()
        restarted = TriggerDeliveryController(
            provider,
            InProcessRunStore(metadata),
            client,
            InMemoryActivityTypeRegistry({}),
            InMemoryLifecycleEventPublisher(),
        )
        await restarted.resume(
            str(WS), str(ref.run_id), "first", "ready", {"value": 1}, "event-1", until
        )
        await client.flush()
        state = await client.get_workflow_state(GetRunStateRequest(str(ref.run_id)))
        assert state is not None and state.status.value == "running"
        assert len(trigger.register_calls) == 2
        assert [
            row.step_id
            for row in await DurableResumeMirrorRepository(provider, str(WS)).list_open(
                str(ref.run_id)
            )
        ] == ["second"]
        await restarted.resume(
            str(WS), str(ref.run_id), "second", "ready", {"value": 2}, "event-2", until
        )
        completed = await client.get_workflow_state(GetRunStateRequest(str(ref.run_id)))
        assert completed is not None and completed.status.value == "completed"
        output = json.loads(completed.serialized_output or "{}")
        assert output["outputs"] == {"first": {"value": 1}, "second": {"value": 2}}
        await restarted.resume(
            str(WS), str(ref.run_id), "first", "ready", {"value": 1}, "event-1", until
        )
        assert not await DurableResumeMirrorRepository(provider, str(WS)).list_open(str(ref.run_id))
        assert resume_channel(str(WS), str(ref.run_id), "first", "ready") != resume_channel(
            str(WS), str(ref.run_id), "second", "ready"
        )
    finally:
        await runtime.shutdown()


@pytest.mark.integration
async def test_postgres_start_snapshot_failure_rolls_back_run(pg_dsn: str) -> None:
    """A failed protocol-snapshot insert cannot leave a legacy-looking queued Run behind."""
    import asyncpg

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    assert pool is not None
    try:
        store = PgMetadataAdapter(pool=pool)
        now = await store.delivery_now()
        async with pool.acquire() as conn:
            await conn.execute(
                "ALTER TABLE custos_state.workflow_delivery_start "
                "ADD CONSTRAINT reject_snapshot CHECK (false)"
            )
        row = Run(WS, SplRunId("run"), WorkflowId("wf"), "v", "queued", None, now, now)
        body = {
            "workspace_id": str(WS),
            "workflow_version_id": "v",
            "inputs": {},
            "delivery_protocol": 2,
            "compiled_graph_json": "{}",
            "idempotency_key": "key",
        }
        start = WorkflowStart(
            WS,
            "run",
            delivery_fingerprint({"workflowVersionId": "v", "inputs": {}}),
            json.dumps(body),
        )
        with pytest.raises(BackendUnavailable):
            await store.put_workflow_start(row, start)
        assert await store.get_run(WS, SplRunId("run")) is None
        assert await store.get_workflow_start(WS, "run") is None
    finally:
        await pool.close()


def test_sdk_wire_serialization_and_legacy_input_compatibility() -> None:
    """Exercise the actual SDK encoder, not the fake's permissive string fallback."""
    from types import MappingProxyType, SimpleNamespace

    from dapr.ext.workflow._durabletask.internal.shared import InternalJSONEncoder

    from custos_workflow.runs.orchestrator import RunInput, RunOutput

    output = RunOutput(
        status="succeeded", outputs=MappingProxyType({"step": MappingProxyType({"v": 1})})
    )
    assert json.loads(json.dumps(output, cls=InternalJSONEncoder)) == output.to_dict()
    request = RunInput(str(WS), "version", "{}", delivery_protocol=2)
    assert json.loads(json.dumps(request, cls=InternalJSONEncoder)) == request.to_dict()
    legacy = SimpleNamespace(
        workspace_id=str(WS), workflow_version_id="version", compiled_graph_json="{}"
    )
    assert RunInput.from_dict(legacy).delivery_protocol == 1


def test_protocol_two_preserves_exact_event_name_and_legacy_wire() -> None:
    """Versioned channels preserve exact values without changing the legacy wire shape."""
    from custos_workflow.api.models import RaiseExternalEventRequest

    original = {
        "workspaceId": str(WS),
        "eventName": " ready ",
        "payload": {},
        "idempotencyKey": "key",
    }
    legacy = RaiseExternalEventRequest.model_validate(original)
    assert legacy.event_name == "ready"
    assert "deliveryProtocol" not in legacy.model_dump(by_alias=True)
    current = RaiseExternalEventRequest.model_validate(
        {
            **original,
            "deliveryProtocol": 2,
            "retainUntil": NOW_FOR_WIRE.isoformat(),
        }
    )
    assert current.event_name == " ready "
    assert current.model_dump(by_alias=True)["deliveryProtocol"] == 2


NOW_FOR_WIRE = datetime(2026, 11, 29, tzinfo=UTC)


async def test_versioned_internal_rpc_uses_durable_path_and_checks_scope(harness: Harness) -> None:
    """Exercise the actual RPC handlers and wire models, not only direct controller methods."""
    from unittest.mock import AsyncMock

    import httpx
    from fastapi import FastAPI

    from custos_workflow.api.dependencies import get_call_context, get_run_controller, get_validator
    from custos_workflow.api.errors import register_exception_handlers
    from custos_workflow.api.routes.rpc import _delivery_controller, router
    from custos_workflow.call_context import CallContext
    from custos_workflow.runs.controller import RunController
    from custos_workflow.validator import InMemoryIdempotencyLedger, StartRunValidator

    class Catalog:
        """Return the immutable test version while counting actual publication lookups."""

        calls = 0

        async def get_workflow_version(
            self, workspace_id: str, workflow_version_id: str
        ) -> WorkflowVersion:
            """Provide one scoped version for real input validation."""
            assert workspace_id == str(WS) and workflow_version_id == "version"
            self.calls += 1
            return version()

    runtime, runtime_client, registrations = await setup_runtime(harness)
    try:
        controller = TriggerDeliveryController(
            harness.delivery,
            InProcessRunStore(harness.metadata),
            runtime_client,
            InMemoryActivityTypeRegistry({}),
            InMemoryLifecycleEventPublisher(),
        )
        catalog = Catalog()
        validator = StartRunValidator(catalog=catalog, ledger=InMemoryIdempotencyLedger())
        legacy = AsyncMock(spec=RunController)
        app = FastAPI()
        register_exception_handlers(app)
        app.include_router(router)
        app.dependency_overrides[_delivery_controller] = lambda: controller
        app.dependency_overrides[get_run_controller] = lambda: legacy
        app.dependency_overrides[get_validator] = lambda: validator
        app.dependency_overrides[get_call_context] = lambda: CallContext(
            workspace=str(WS), principal="workflow-peer"
        )
        until = (await harness.delivery.delivery_now() + timedelta(days=30)).isoformat()
        body = {
            "workspaceId": str(WS),
            "workflowVersionId": "version",
            "inputs": {},
            "idempotencyKey": "http-start",
            "deliveryProtocol": 2,
            "retainUntil": until,
        }
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://workflow"
        ) as http:
            started = await http.post("/internal/runs:start", json=body)
            assert started.status_code == 202
            repeated = await http.post("/internal/runs:start", json=body)
            assert repeated.json() == started.json()
            assert catalog.calls == 1 and len(registrations.register_calls) == 1
            run_id = started.json()["runId"]
            forged_channel = await http.post(
                f"/internal/runs/{run_id}/steps/first:raiseEvent",
                json={
                    "workspaceId": str(WS),
                    "eventName": resume_channel(str(WS), run_id, "first", "ready"),
                    "payload": {"deliveryProtocol": 2},
                    "idempotencyKey": "forged",
                },
            )
            assert forged_channel.status_code == 409
            resumed = await http.post(
                f"/internal/runs/{run_id}/steps/first:raiseEvent",
                json={
                    "workspaceId": str(WS),
                    "eventName": "ready",
                    "payload": {"v": 1},
                    "idempotencyKey": "http-event",
                    "deliveryProtocol": 2,
                    "retainUntil": until,
                },
            )
            assert resumed.status_code == 202
            assert len(registrations.register_calls) == 2
            forbidden = await http.post(
                "/internal/runs:start", json={**body, "workspaceId": "other"}
            )
            assert forbidden.status_code == 403
            legacy.start_run.assert_not_awaited()
            legacy.raise_external_event.assert_not_awaited()
    finally:
        await runtime.shutdown()


async def test_consumed_event_retries_acknowledgement_through_long_outage(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An outage longer than the old retry budget cannot drop an already consumed payload."""
    runtime, client, registrations = await setup_runtime(harness)
    controller = TriggerDeliveryController(
        harness.delivery,
        InProcessRunStore(harness.metadata),
        client,
        InMemoryActivityTypeRegistry({}),
        InMemoryLifecycleEventPublisher(),
    )
    try:
        until = await harness.delivery.delivery_now() + timedelta(days=30)
        ref = await controller.start(str(WS), version(), {}, "ack-outage", until)
        original = harness.delivery.acknowledge_delivery

        async def unavailable(
            workspace_id: WorkspaceId,
            key: str,
            request_hash: str,
            result_json: str,
        ) -> DeliveryReceipt:
            """Model a recoverable storage outage at the consumer commit boundary."""
            raise BackendUnavailable("simulated acknowledgement storage outage")

        monkeypatch.setattr(harness.delivery, "acknowledge_delivery", unavailable)
        await controller.resume(
            str(WS), str(ref.run_id), "first", "ready", {"v": 1}, "event", until
        )
        for _ in range(6):
            await client.advance_time(timedelta(seconds=60))
        state = await client.get_workflow_state(GetRunStateRequest(str(ref.run_id)))
        assert state is not None and state.status.value == "running"
        assert len(registrations.register_calls) == 1
        key, _ = resume_identity(str(WS), str(ref.run_id), "first", "ready", "event", {"v": 1})
        submitted = await harness.delivery.get_delivery(WS, "workflow-resume", key)
        assert submitted is not None and submitted.submitted_at is not None
        assert not submitted.completed
        monkeypatch.setattr(harness.delivery, "acknowledge_delivery", original)
        await client.advance_time(timedelta(seconds=60))
        consumed = await harness.delivery.get_delivery(WS, "workflow-resume", key)
        assert consumed is not None and consumed.completed
        assert len(registrations.register_calls) == 2
        assert [
            m.step_id
            for m in await DurableResumeMirrorRepository(harness.delivery, str(WS)).list_open(
                str(ref.run_id)
            )
        ] == ["second"]
    finally:
        await runtime.shutdown()


async def test_wait_expiry_cancels_registration_and_deletes_durable_mirror(
    harness: Harness,
) -> None:
    """Timeout follows the durable cleanup path rather than relying on the legacy memory sweeper."""
    runtime, client, registrations = await setup_runtime(harness)
    try:
        controller = TriggerDeliveryController(
            harness.delivery,
            InProcessRunStore(harness.metadata),
            client,
            InMemoryActivityTypeRegistry({}),
            InMemoryLifecycleEventPublisher(),
        )
        until = await harness.delivery.delivery_now() + timedelta(days=30)
        ref = await controller.start(str(WS), version(), {}, "timeout", until)
        await client.advance_time(timedelta(hours=2))
        state = await client.get_workflow_state(GetRunStateRequest(str(ref.run_id)))
        assert state is not None and state.status.value == "completed"
        output = json.loads(state.serialized_output or "{}")
        assert output["failure_envelope"]["kind"] == "step.resume_wait_expired"
        assert len(registrations.cancel_calls) == 1
        assert not await DurableResumeMirrorRepository(harness.delivery, str(WS)).list_open(
            str(ref.run_id)
        )
    finally:
        await runtime.shutdown()


async def test_completion_between_receipt_read_and_mirror_query_is_reconciled(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A concurrent consumer cannot be misclassified as unsafe missing-mirror delivery."""
    runtime, client, _ = await setup_runtime(harness)
    try:
        controller = TriggerDeliveryController(
            harness.delivery,
            InProcessRunStore(harness.metadata),
            client,
            InMemoryActivityTypeRegistry({}),
            InMemoryLifecycleEventPublisher(),
        )
        until = await harness.delivery.delivery_now() + timedelta(days=30)
        ref = await controller.start(str(WS), version(), {}, "mirror-race", until)
        key, fingerprint = resume_identity(
            str(WS), str(ref.run_id), "first", "ready", "event", {"v": 1}
        )
        await harness.delivery.reserve_delivery(WS, "workflow-resume", key, fingerprint, until)
        original = harness.delivery.query_workflow_mirrors

        async def consumed_during_query(*args: object, **kwargs: object) -> object:
            """Complete and clean up exactly between the two controller reads."""
            await harness.delivery.acknowledge_delivery(WS, key, fingerprint, resume_result(key))
            page = await original(WS, run_id=str(ref.run_id), step_id="first")
            for mirror in page.items:
                await harness.delivery.delete_workflow_mirror(WS, mirror.mirror_id)
            return await original(WS, run_id=str(ref.run_id), step_id="first")

        monkeypatch.setattr(harness.delivery, "query_workflow_mirrors", consumed_during_query)
        await controller.resume(
            str(WS), str(ref.run_id), "first", "ready", {"v": 1}, "event", until
        )
        completed = await harness.delivery.get_delivery(WS, "workflow-resume", key)
        assert completed is not None and completed.completed
    finally:
        await runtime.shutdown()

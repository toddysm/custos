"""Identical fencing/outbox contracts against memory and real PostgreSQL."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast

import pytest
from asyncpg.pool import Pool
from custos_pg import PgMetadataAdapter
from custos_spl.errors import (
    BackendUnavailable,
    ImmutableViolation,
    LeaseExpired,
    MigrationRequired,
)
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.scheduler import (
    PendingScheduleFire,
    SchedulerLease,
    SchedulerStateProvider,
)
from custos_spl.interfaces.trigger_queries import TriggerQueryProvider

from custos_trigger.events import NormalizedEvent
from custos_trigger.models import SourceType, Subscription, SubscriptionState
from custos_trigger.pipeline.dispatch import NoopAuditSink
from custos_trigger.providers import InMemoryTriggerMetadataStore
from custos_trigger.receivers.scheduled import ScheduledReceiver
from custos_trigger.scheduler import SchedulerLeadership
from custos_trigger.source_config import SourceConfig
from custos_trigger.stores.base import TriggerMetadataStore
from custos_trigger.stores.subscriptions import SubscriptionStore

_WS = WorkspaceId("scheduler-test")


class Backend(TriggerMetadataStore, TriggerQueryProvider, SchedulerStateProvider, Protocol):
    """The production storage surfaces exercised by this contract."""


@dataclass
class Harness:
    """Provide deterministic memory time and real database fault injection."""

    store: Backend
    clock: list[datetime]
    pool: Pool | None

    async def expire(self, deployment: str) -> None:
        """Cross the exact memory boundary or make the database lease eligible without sleeps."""
        if self.pool is None:
            self.clock[0] += timedelta(seconds=30)
        else:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE custos_state.scheduler_lease SET expires_at=clock_timestamp() "
                    "WHERE deployment_id=$1",
                    deployment,
                )


@pytest.fixture(params=["memory", pytest.param("postgres", marks=pytest.mark.integration)])
def scheduler_dsn(request: pytest.FixtureRequest) -> str | None:
    """Reset the database before entering the test's event loop."""
    return None if request.param == "memory" else cast(str, request.getfixturevalue("pg_dsn"))


@pytest.fixture
async def harness(scheduler_dsn: str | None) -> AsyncIterator[Harness]:
    """Yield the same interface backed by either actual production provider."""
    clock = [datetime(2026, 10, 28, tzinfo=UTC)]
    if scheduler_dsn is None:
        yield Harness(InMemoryTriggerMetadataStore(now=lambda: clock[0]), clock, None)
        return
    import asyncpg

    pool = await asyncpg.create_pool(dsn=scheduler_dsn, min_size=1, max_size=4)
    assert pool is not None
    try:
        adapter = PgMetadataAdapter(pool=pool)
        await adapter.apply_pending()
        yield Harness(adapter, clock, pool)
    finally:
        await pool.close()


async def _due(
    harness: Harness,
    identifier: str = "schedule",
    workspace: WorkspaceId = _WS,
    *,
    interval_seconds: int = 30,
    input_mapping: dict[str, Any] | None = None,
    selector: str | None = None,
) -> Subscription:
    """Create real managed state and set an explicitly due instant through the maintenance API."""
    now = await harness.store.scheduler_now()
    store = SubscriptionStore(harness.store, now=lambda: now)
    subscription = await store.create(
        Subscription(
            workspace_id=workspace,
            subscription_id=identifier,
            workflow_id="workflow",
            source_type=SourceType.SCHEDULED,
            source_config=SourceConfig(interval_seconds=interval_seconds),
            target_workflow_version_id="version",
            input_mapping=input_mapping or {},
            selector=selector,
            created_at=now - timedelta(seconds=2 * interval_seconds),
            updated_at=now,
        )
    )
    await harness.store.update_schedule_next_fire(
        workspace, identifier, now - timedelta(seconds=interval_seconds)
    )
    return subscription


async def _advance(
    harness: Harness,
    subscription: Subscription,
    lease: SchedulerLease,
    *,
    next_at: datetime | None = None,
) -> PendingScheduleFire:
    """Advance the captured due instant via the fenced transaction."""
    schedule = await harness.store.get_schedule(
        WorkspaceId(subscription.workspace_id), subscription.subscription_id
    )
    assert schedule is not None
    now = await harness.store.scheduler_now()
    return await harness.store.advance_schedule(
        WorkspaceId(subscription.workspace_id),
        subscription.subscription_id,
        lease,
        expected_next_fire_at=schedule.next_fire_at,
        expected_revision=subscription.store_revision,
        planned_fire_at=schedule.next_fire_at,
        next_fire_at=next_at if next_at is not None else now + timedelta(minutes=1),
    )


async def test_receiver_recovers_uncertain_publication_with_stable_identity(
    harness: Harness,
) -> None:
    """Replica takeover republishes a stored occurrence without advancing or inventing a new ID."""
    await _due(harness, interval_seconds=3600)
    events: list[NormalizedEvent] = []
    audits: list[tuple[str, Mapping[str, Any]]] = []
    uncertain = True

    class Audit:
        """Retain coalescing evidence with stable occurrence identity for retry correlation."""

        async def emit(
            self,
            event_name: str,
            *,
            workspace_id: str,
            attributes: Mapping[str, Any],
        ) -> None:
            """Record only this test workspace's structured audit attributes."""
            assert workspace_id == str(_WS)
            audits.append((event_name, dict(attributes)))

    async def publish(event: NormalizedEvent) -> None:
        """Accept once but lose its acknowledgement to model producer death at publication."""
        nonlocal uncertain
        events.append(event)
        if uncertain:
            uncertain = False
            raise OSError("publication acknowledgement lost")

    a = ScheduledReceiver(
        harness.store,
        workspace_id=str(_WS),
        leadership=SchedulerLeadership(harness.store, deployment_id="receiver", holder_id="a"),
        publish=publish,
        audit=Audit(),
    )
    recovered_store = (
        PgMetadataAdapter(pool=harness.pool) if harness.pool is not None else harness.store
    )
    b = ScheduledReceiver(
        recovered_store,
        workspace_id=str(_WS),
        leadership=SchedulerLeadership(recovered_store, deployment_id="receiver", holder_id="b"),
        publish=publish,
        audit=Audit(),
    )
    with pytest.raises(OSError):
        await a.tick()
    before = await harness.store.get_schedule(_WS, "schedule")
    pending = await harness.store.query_pending_schedule_fires(_WS, "receiver")
    assert len(pending.items) == 1 and pending.items[0].skipped_count == 1
    assert not (await b.tick()).leader
    await harness.expire("receiver")
    recovered = await b.tick()
    assert recovered.published == 1 and recovered.planned == 0
    assert events[0].model_dump() == events[1].model_dump()
    assert events[0].data["skippedCount"] == 1
    assert len(audits) == 2
    assert all(
        name == "trigger.schedule.coalesced"
        and attributes["eventId"] == events[0].event_id
        and attributes["skippedCount"] == 1
        for name, attributes in audits
    )
    assert await harness.store.get_schedule(_WS, "schedule") == before
    assert not (await harness.store.query_pending_schedule_fires(_WS, "receiver")).items
    assert not (await a.tick()).leader


async def test_stale_publisher_cannot_acknowledge_after_takeover(harness: Harness) -> None:
    """A delayed old holder republishes one identity but cannot settle under its expired epoch."""
    await _due(harness, interval_seconds=3600)
    entered, release = asyncio.Event(), asyncio.Event()
    events: list[NormalizedEvent] = []

    async def publish(event: NormalizedEvent) -> None:
        """Hold the first send across lease takeover while allowing recovery to proceed."""
        events.append(event)
        if len(events) == 1:
            entered.set()
            await release.wait()

    a = ScheduledReceiver(
        harness.store,
        workspace_id=str(_WS),
        leadership=SchedulerLeadership(harness.store, deployment_id="overlap", holder_id="a"),
        publish=publish,
        audit=NoopAuditSink(),
    )
    b = ScheduledReceiver(
        harness.store,
        workspace_id=str(_WS),
        leadership=SchedulerLeadership(harness.store, deployment_id="overlap", holder_id="b"),
        publish=publish,
        audit=NoopAuditSink(),
    )
    task = asyncio.create_task(a.tick())
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        await harness.expire("overlap")
        assert (await b.tick()).published == 1
        release.set()
        with pytest.raises(LeaseExpired):
            await task
        assert len(events) == 2 and events[0].model_dump() == events[1].model_dump()
        assert not (await harness.store.query_pending_schedule_fires(_WS, "overlap")).items
    finally:
        release.set()
        if not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


@pytest.mark.parametrize("state", [SubscriptionState.PAUSED, SubscriptionState.EXPIRED])
async def test_receiver_suppresses_inactive_pending_occurrences(
    harness: Harness,
    state: SubscriptionState,
) -> None:
    """Suppression is terminal and distinct from a successful publication."""
    subscription = await _due(harness)
    lease = await harness.store.acquire_scheduler_lease("suppress", "a", ttl_seconds=30)
    assert lease is not None
    fire = await _advance(harness, subscription, lease)
    await SubscriptionStore(harness.store).set_state(str(_WS), "schedule", state)
    events: list[NormalizedEvent] = []

    async def publish(event: NormalizedEvent) -> None:
        """Record any unintended effect after pause/deletion."""
        events.append(event)

    receiver = ScheduledReceiver(
        harness.store,
        workspace_id=str(_WS),
        leadership=SchedulerLeadership(harness.store, deployment_id="suppress", holder_id="a"),
        publish=publish,
        audit=NoopAuditSink(),
    )
    result = await receiver.tick()
    assert result.suppressed == 1 and not result.published and not events
    assert not (await harness.store.query_pending_schedule_fires(_WS, "suppress")).items
    if harness.pool is None:
        assert isinstance(harness.store, InMemoryTriggerMetadataStore)
        stored = harness.store._pending_fires[(str(_WS), fire.fire_id)]
        assert stored.discarded_at is not None and stored.delivered_at is None
        assert stored.discard_reason == "inactive"
    else:
        async with harness.pool.acquire() as conn:
            stored_row = await conn.fetchrow(
                "SELECT delivered_at,discarded_at,discard_reason "
                "FROM custos_state.pending_schedule_fire WHERE workspace_id=$1 AND fire_id=$2",
                _WS,
                fire.fire_id,
            )
        assert stored_row is not None and stored_row["delivered_at"] is None
        assert stored_row["discarded_at"] is not None and stored_row["discard_reason"] == "inactive"


async def test_receiver_bounds_one_hundred_due_schedules(harness: Harness) -> None:
    """The documented due-batch workload publishes exactly its bounded number of unique events."""
    for index in range(101):
        await _due(harness, f"schedule-{index:03d}", interval_seconds=3600)
    events: list[NormalizedEvent] = []

    async def publish(event: NormalizedEvent) -> None:
        """Acknowledge every valid event while preserving exact workload accounting."""
        events.append(event)

    receiver = ScheduledReceiver(
        harness.store,
        workspace_id=str(_WS),
        page_size=100,
        leadership=SchedulerLeadership(harness.store, deployment_id="batch", holder_id="a"),
        publish=publish,
        audit=NoopAuditSink(),
    )
    result = await receiver.tick()
    assert result.planned == result.published == 100
    assert len({event.event_id for event in events}) == 100
    second = await receiver.tick()
    assert second.planned == second.published == 1
    assert len({event.event_id for event in events}) == 101


async def test_continuously_due_prefix_cannot_starve_later_pages(harness: Harness) -> None:
    """A fixed-cutoff cursor reaches the tail even when earlier IDs are already due again."""
    for identifier in ("a", "b", "c"):
        await _due(harness, identifier, interval_seconds=1)
    events: list[NormalizedEvent] = []

    async def publish(event: NormalizedEvent) -> None:
        """Acknowledge each occurrence for exact per-page accounting."""
        events.append(event)

    receiver = ScheduledReceiver(
        harness.store,
        workspace_id=str(_WS),
        page_size=2,
        leadership=SchedulerLeadership(harness.store, deployment_id="fair", holder_id="a"),
        publish=publish,
        audit=NoopAuditSink(),
    )
    assert (await receiver.tick()).published == 2
    prefix = await harness.store.get_schedule(_WS, "b")
    assert prefix is not None
    if harness.pool is None:
        harness.clock[0] = prefix.next_fire_at
    else:
        async with asyncio.timeout(5):
            while await harness.store.scheduler_now() < prefix.next_fire_at:
                await asyncio.sleep(0.01)
    second = await receiver.tick()
    assert second.published == 1
    assert [event.source.subscription_id for event in events] == ["a", "b", "c"]
    assert (await receiver.tick()).published == 2
    assert [event.source.subscription_id for event in events[-2:]] == ["a", "b"]


async def test_expired_plan_defers_without_aborting_later_schedules(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An elapsed one-second plan remains due without preventing tail processing."""
    from custos_trigger.receivers import scheduled

    await _due(harness, "a", interval_seconds=1)
    await _due(harness, "b", interval_seconds=3600)
    before = await harness.store.get_schedule(_WS, "a")
    events: list[NormalizedEvent] = []
    original = scheduled.plan_schedule
    delayed = False

    async def delayed_plan(*args: Any, **kwargs: Any) -> scheduled.SchedulePlan:
        """Advance provider time once after computing, before the fenced write."""
        nonlocal delayed
        plan = await original(*args, **kwargs)
        if not delayed:
            delayed = True
            if harness.pool is None:
                harness.clock[0] = plan.next_at
            else:
                async with asyncio.timeout(5):
                    while await harness.store.scheduler_now() < plan.next_at:
                        await asyncio.sleep(0.01)
        return plan

    async def publish(event: NormalizedEvent) -> None:
        """Record acknowledged publications for both bounded scan cycles."""
        events.append(event)

    monkeypatch.setattr(scheduled, "plan_schedule", delayed_plan)
    receiver = ScheduledReceiver(
        harness.store,
        workspace_id=str(_WS),
        leadership=SchedulerLeadership(harness.store, deployment_id="elapsed", holder_id="a"),
        publish=publish,
        audit=NoopAuditSink(),
        page_size=2,
    )
    first = await receiver.tick()
    assert first.planned == first.published == 1
    assert [event.source.subscription_id for event in events] == ["b"]
    assert await harness.store.get_schedule(_WS, "a") == before
    assert not (await harness.store.query_pending_schedule_fires(_WS, "elapsed")).items
    assert (await receiver.tick()).published == 1
    assert [event.source.subscription_id for event in events] == ["b", "a"]


async def test_reconfigured_pending_fire_is_discarded_not_rebound(harness: Harness) -> None:
    """A pending occurrence cannot adopt a new target or input mapping after producer death."""
    subscription = await _due(harness, interval_seconds=3600)
    lease = await harness.store.acquire_scheduler_lease("changed", "a", ttl_seconds=30)
    assert lease is not None
    fire = await _advance(harness, subscription, lease)
    await SubscriptionStore(harness.store).update(
        subscription.model_copy(update={"target_workflow_version_id": "new-version"}),
        expected_revision=subscription.store_revision,
    )
    published: list[NormalizedEvent] = []

    async def publish(event: NormalizedEvent) -> None:
        """Record any accidental rebind of the earlier planned occurrence."""
        published.append(event)

    receiver = ScheduledReceiver(
        harness.store,
        workspace_id=str(_WS),
        leadership=SchedulerLeadership(harness.store, deployment_id="changed", holder_id="a"),
        publish=publish,
        audit=NoopAuditSink(),
    )
    assert (await receiver.tick()).suppressed == 1
    assert not published
    with pytest.raises(ImmutableViolation):
        await harness.store.acknowledge_schedule_fire(_WS, fire.fire_id, lease)


async def test_suppression_is_fenced_and_cannot_undo_publication(harness: Harness) -> None:
    """Both terminal dispositions require current ownership and remain mutually exclusive."""
    subscription = await _due(harness)
    first = await harness.store.acquire_scheduler_lease("settlement", "a", ttl_seconds=30)
    assert first is not None
    fire = await _advance(harness, subscription, first)
    await harness.expire("settlement")
    second = await harness.store.acquire_scheduler_lease("settlement", "b", ttl_seconds=30)
    assert second is not None
    with pytest.raises(LeaseExpired):
        await harness.store.discard_schedule_fire(_WS, fire.fire_id, first, reason="inactive")
    with pytest.raises(ImmutableViolation):
        await harness.store.discard_schedule_fire(
            WorkspaceId("foreign"),
            fire.fire_id,
            second,
            reason="inactive",
        )
    with pytest.raises(ValueError):
        await harness.store.discard_schedule_fire(_WS, fire.fire_id, second, reason="private-data")
    await harness.store.acknowledge_schedule_fire(_WS, fire.fire_id, second)
    with pytest.raises(ImmutableViolation):
        await harness.store.discard_schedule_fire(_WS, fire.fire_id, second, reason="inactive")


async def test_recovered_publication_does_not_duplicate_workflow_effect(harness: Harness) -> None:
    """Clock-to-pipeline recovery uses real receipts and schedules exactly one runtime."""
    import json

    from custos_spl.interfaces.metadata_store import MetadataStoreProvider
    from custos_workflow.bindings.registry import InMemoryActivityTypeRegistry
    from custos_workflow.document import WorkflowDocument
    from custos_workflow.providers import _InProcessMetadataStoreProvider
    from custos_workflow.runs.controller import InMemoryLifecycleEventPublisher, WorkflowVersion
    from custos_workflow.runs.ids import derive_run_id
    from custos_workflow.runs.orchestrator import WORKFLOW_NAME, make_run_orchestrator
    from custos_workflow.runs.step_handler import NoopStepHandler
    from custos_workflow.runs.store import InProcessRunStore
    from custos_workflow.runs.trigger_delivery import TriggerDeliveryController
    from custos_workflow.runtime._common import GetRunStateRequest, ScheduleWorkflowRequest
    from custos_workflow.runtime.fake import FakeWorkflowClient, FakeWorkflowRuntime

    from custos_trigger.clients.workflow import RaiseExternalEventRequest, RunRef, StartRunRequest
    from custos_trigger.dedup import Deduplicator, compute_dedup_key
    from custos_trigger.pipeline.dispatch import Dispatcher
    from custos_trigger.pipeline.reliable import ReliableDispatcher, ReliableStorage
    from custos_trigger.receivers.scheduled import process_scheduled_event
    from custos_trigger.selector import SelectorEvaluator

    runtime = FakeWorkflowRuntime(now=await harness.store.scheduler_now())
    runtime.register_workflow(make_run_orchestrator(NoopStepHandler()), name=WORKFLOW_NAME)

    class CountingRuntime(FakeWorkflowClient):
        """Count calls into the real fake-runtime execution path, not synthetic run references."""

        def __init__(self) -> None:
            """Bind the registered runtime and start an independent effect counter."""
            super().__init__(runtime=runtime)
            self.schedule_calls = 0

        async def schedule_new_workflow(self, request: ScheduleWorkflowRequest) -> str:
            """Execute normally while measuring actual runtime schedule calls."""
            self.schedule_calls += 1
            return await super().schedule_new_workflow(request)

    workflow_metadata: PgMetadataAdapter | _InProcessMetadataStoreProvider = (
        harness.store
        if isinstance(harness.store, PgMetadataAdapter)
        else _InProcessMetadataStoreProvider(now=lambda: harness.clock[0])
    )
    client = CountingRuntime()
    # Match Workflow composition: its development provider implements the Run subset.
    controller = TriggerDeliveryController(
        workflow_metadata,
        InProcessRunStore(cast(MetadataStoreProvider, workflow_metadata)),
        client,
        InMemoryActivityTypeRegistry({}),
        InMemoryLifecycleEventPublisher(),
    )
    version = WorkflowVersion(
        "version",
        "workflow",
        "scheduled",
        "v1",
        WorkflowDocument.model_validate(
            {
                "apiVersion": "custos.dev/v1",
                "kind": "Workflow",
                "metadata": {"name": "scheduled", "workspace": str(_WS)},
                "spec": {"steps": [{"id": "done", "let": {"value": 1}}]},
            }
        ),
    )

    class WorkflowClient:
        """Bridge the real Trigger request into the actual versioned Workflow controller."""

        async def start_run(self, request: StartRunRequest) -> RunRef:
            """Preserve the original idempotency key and retention boundary across services."""
            assert request.idempotency_key is not None and request.retain_until is not None
            ref = await controller.start(
                request.workspace_id,
                version,
                request.inputs,
                request.idempotency_key,
                request.retain_until,
            )
            return RunRef(
                run_id=str(ref.run_id),
                status=ref.status.value,
                workspace_id=request.workspace_id,
                workflow_version_id=request.workflow_version_id,
            )

        async def raise_external_event(
            self,
            run_id: str,
            step_id: str,
            request: RaiseExternalEventRequest,
        ) -> None:
            """Scheduled start subscriptions must never invent a resume target."""
            raise AssertionError("scheduled occurrence unexpectedly routed to resume")

    assert isinstance(harness.store, ReliableStorage)
    audit = NoopAuditSink()
    workflow = WorkflowClient()
    dispatcher = Dispatcher(
        workflow,
        Deduplicator(harness.store),
        reliable=ReliableDispatcher(workflow, harness.store, audit),
    )
    subscriptions = SubscriptionStore(harness.store)
    mapping = {
        "planned": "${{ event.data.plannedFireAt }}",
        "skipped": "${{ event.data.skippedCount }}",
        "occurrences": "${{ event.data.skippedCount + 1 }}",
        "label": "schedule-${{ event.subject }}",
    }
    await _due(
        harness,
        interval_seconds=3600,
        input_mapping=mapping,
        selector="event.data.skippedCount + 1 > 0",
    )
    await _due(harness, "other", interval_seconds=3600)
    await harness.store.update_schedule_next_fire(
        _WS,
        "other",
        await harness.store.scheduler_now() + timedelta(days=1),
    )
    events: list[NormalizedEvent] = []
    uncertain = True

    async def publish(event: NormalizedEvent) -> None:
        """Run the actual matching/durable dispatch path, then lose the first acknowledgement."""
        nonlocal uncertain
        outcome = await process_scheduled_event(
            event,
            subscription_store=subscriptions,
            provider=harness.store,
            evaluator=SelectorEvaluator(),
            dispatcher=dispatcher,
            audit=audit,
        )
        assert outcome is not None and (outcome.is_dispatched or outcome.is_duplicate)
        events.append(event)
        if uncertain:
            uncertain = False
            raise OSError("accepted publication response lost")

    a = ScheduledReceiver(
        harness.store,
        workspace_id=str(_WS),
        leadership=SchedulerLeadership(harness.store, deployment_id="effects", holder_id="a"),
        publish=publish,
        audit=audit,
    )
    b = ScheduledReceiver(
        harness.store,
        workspace_id=str(_WS),
        leadership=SchedulerLeadership(harness.store, deployment_id="effects", holder_id="b"),
        publish=publish,
        audit=audit,
    )
    await runtime.start()
    try:
        with pytest.raises(OSError):
            await a.tick()
        await harness.expire("effects")
        assert (await b.tick()).published == 1
        assert client.schedule_calls == 1
        assert len(events) == 2 and events[0].event_id == events[1].event_id
        run_id = derive_run_id(str(_WS), compute_dedup_key("schedule", events[0].event_id))
        state = await client.get_workflow_state(GetRunStateRequest(str(run_id)))
        assert state is not None and state.status.value == "completed"
        assert json.loads(state.serialized_output or "{}")["status"] == "succeeded"
        assert json.loads(state.serialized_input or "{}")["inputs"] == {
            "planned": events[0].data["plannedFireAt"],
            "skipped": 1,
            "occurrences": 2,
            "label": "schedule-schedule",
        }
        stored = await subscriptions.get(str(_WS), "schedule")
        assert stored is not None and stored.input_mapping == mapping
    finally:
        await runtime.shutdown()


@pytest.mark.parametrize(
    "field",
    ["event_id", "plannedFireAt", "occurredAt", "workspace", "revision", "skippedCount"],
)
async def test_consumer_rejects_unproven_or_mutated_occurrences(
    harness: Harness,
    field: str,
) -> None:
    """Fresh dedup IDs or changed timestamps cannot manufacture an extra scheduled effect."""
    from custos_trigger.clients import FakeWorkflowServiceClient
    from custos_trigger.dedup import Deduplicator
    from custos_trigger.pipeline.dispatch import Dispatcher
    from custos_trigger.receivers.scheduled import process_scheduled_event, scheduled_event
    from custos_trigger.selector import SelectorEvaluator
    from custos_trigger.source_config import SourceConfigurationError

    subscription = await _due(harness)
    lease = await harness.store.acquire_scheduler_lease("proof", "a", ttl_seconds=30)
    assert lease is not None
    fire = await _advance(harness, subscription, lease)
    event = scheduled_event(fire)
    if field == "event_id":
        event.event_id = "invented-id"
    elif field == "occurredAt":
        event.source.occurred_at = "2099-01-01T00:00:00+00:00"
    elif field == "plannedFireAt":
        event.data["plannedFireAt"] = "2099-01-01T00:00:00+00:00"
    elif field == "workspace":
        event.data["workspace"] = "foreign"
    elif field == "revision":
        event.data["subscriptionRevision"] = fire.subscription_revision + 1
    else:
        event.data["skippedCount"] = 999
    client = FakeWorkflowServiceClient()
    with pytest.raises(SourceConfigurationError, match="canonical occurrence"):
        await process_scheduled_event(
            event,
            subscription_store=SubscriptionStore(harness.store),
            provider=harness.store,
            evaluator=SelectorEvaluator(),
            dispatcher=Dispatcher(client, Deduplicator(harness.store)),
            audit=NoopAuditSink(),
        )
    assert not client.start_run_calls and not client.raise_event_calls


@pytest.mark.parametrize("suppressed", [False, True])
async def test_consumer_retains_proof_after_producer_settlement(
    harness: Harness,
    suppressed: bool,
) -> None:
    """Broker acknowledgement must not erase proof needed by a later consumer or redelivery."""
    from custos_trigger.clients import FakeWorkflowServiceClient
    from custos_trigger.dedup import Deduplicator
    from custos_trigger.pipeline.dispatch import Dispatcher
    from custos_trigger.receivers.scheduled import process_scheduled_event, scheduled_event
    from custos_trigger.selector import SelectorEvaluator

    subscription = await _due(harness)
    lease = await harness.store.acquire_scheduler_lease("settled-proof", "a", ttl_seconds=30)
    assert lease is not None
    fire = await _advance(harness, subscription, lease)
    if suppressed:
        await harness.store.discard_schedule_fire(_WS, fire.fire_id, lease, reason="changed")
    else:
        await harness.store.acknowledge_schedule_fire(_WS, fire.fire_id, lease)
    assert not (await harness.store.query_pending_schedule_fires(_WS, "settled-proof")).items
    assert await harness.store.get_schedule_fire(_WS, fire.fire_id) is not None
    assert await harness.store.get_schedule_fire(WorkspaceId("foreign"), fire.fire_id) is None
    client = FakeWorkflowServiceClient()
    outcome = await process_scheduled_event(
        scheduled_event(fire),
        subscription_store=SubscriptionStore(harness.store),
        provider=harness.store,
        evaluator=SelectorEvaluator(),
        dispatcher=Dispatcher(client, Deduplicator(harness.store)),
        audit=NoopAuditSink(),
    )
    if suppressed:
        assert outcome is None and not client.start_run_calls
    else:
        assert outcome is not None and outcome.is_dispatched
        assert len(client.start_run_calls) == 1


async def test_resolved_mapping_is_captured_and_replayed_without_rematching(
    harness: Harness,
) -> None:
    """Durable replay stores concrete Workflow inputs while authored configuration stays intact."""
    from custos_spl.interfaces.dead_letters import json_value

    from custos_trigger.clients.workflow import (
        FakeWorkflowServiceClient,
        WorkflowClientTransportError,
    )
    from custos_trigger.dedup import Deduplicator
    from custos_trigger.pipeline.dispatch import Dispatcher
    from custos_trigger.pipeline.reliable import ReliableDispatcher, ReliableStorage
    from custos_trigger.receivers.scheduled import process_scheduled_event, scheduled_event
    from custos_trigger.selector import SelectorEvaluator

    mapping = {
        "planned": "${{ event.data.plannedFireAt }}",
        "nested": ["${{ event.subject }}", 3],
        "occurrences": "${{ event.data.skippedCount + 1 }}",
    }
    subscription = await _due(harness, input_mapping=mapping)
    lease = await harness.store.acquire_scheduler_lease("mapped", "a", ttl_seconds=30)
    assert lease is not None
    fire = await _advance(harness, subscription, lease)
    event = scheduled_event(fire)
    assert isinstance(harness.store, ReliableStorage)
    client = FakeWorkflowServiceClient(error=WorkflowClientTransportError("unavailable"))
    audit = NoopAuditSink()
    reliable = ReliableDispatcher(client, harness.store, audit, max_retries=0)
    store = SubscriptionStore(harness.store)
    outcome = await process_scheduled_event(
        event,
        subscription_store=store,
        provider=harness.store,
        evaluator=SelectorEvaluator(),
        dispatcher=Dispatcher(client, Deduplicator(harness.store), reliable=reliable),
        audit=audit,
    )
    assert outcome is not None and outcome.is_dead_lettered
    expected = {
        "planned": event.data["plannedFireAt"],
        "nested": ["schedule", 3],
        "occurrences": fire.skipped_count + 1,
    }
    capture = (await harness.store.query_dead_letters(_WS)).items[0]
    assert json_value(capture.data.target["inputs"]) == expected
    stored = await store.get(str(_WS), "schedule")
    assert stored is not None and stored.input_mapping == mapping
    await store.update(
        stored.model_copy(update={"input_mapping": {"different": 4}}),
        expected_revision=stored.store_revision,
    )
    client.error = None
    assert (await reliable.replay(str(_WS), capture.data.entry_id, "operator")).is_dispatched
    assert client.start_run_calls[-1].inputs == expected


async def test_competing_acquisition_and_stable_renewal(harness: Harness) -> None:
    """Only one replica acquires; renewal and same-holder acquire retries retain the epoch."""
    await harness.store.check_scheduler_ready()
    results = await asyncio.gather(
        *(
            harness.store.acquire_scheduler_lease("deployment", holder, ttl_seconds=30)
            for holder in ("a", "b")
        )
    )
    winners = [lease for lease in results if lease is not None]
    assert len(winners) == 1
    first = winners[0]
    assert first.expires_at - first.observed_at == timedelta(seconds=30)
    retry = await harness.store.acquire_scheduler_lease(
        "deployment", first.holder_id, ttl_seconds=30
    )
    assert retry is not None and retry.epoch == first.epoch and retry.expires_at == first.expires_at
    renewed = await harness.store.renew_scheduler_lease(first, ttl_seconds=30)
    assert renewed.epoch == first.epoch and renewed.expires_at >= first.expires_at
    independent = await harness.store.acquire_scheduler_lease(
        "other-deployment", "b", ttl_seconds=30
    )
    assert independent is not None and independent.epoch == 1


async def test_expiry_handover_and_stale_mutations(harness: Harness) -> None:
    """The first eligible scan acquires a higher epoch and every old mutation is fenced out."""
    subscription = await _due(harness)
    first = await harness.store.acquire_scheduler_lease("d", "a", ttl_seconds=30)
    assert first is not None
    await harness.expire("d")
    second = await harness.store.acquire_scheduler_lease("d", "b", ttl_seconds=30)
    assert second is not None and second.epoch == first.epoch + 1
    for token in (first, replace(second, holder_id="a"), replace(second, epoch=first.epoch)):
        with pytest.raises(LeaseExpired):
            await harness.store.renew_scheduler_lease(token, ttl_seconds=30)
        with pytest.raises(LeaseExpired):
            await harness.store.release_scheduler_lease(token)
        with pytest.raises(LeaseExpired):
            await _advance(harness, subscription, token)
    await harness.store.release_scheduler_lease(second)
    third = await harness.store.acquire_scheduler_lease("d", "b", ttl_seconds=30)
    assert third is not None and third.epoch == second.epoch + 1


async def test_competing_advancement_creates_one_durable_fire(harness: Harness) -> None:
    """Overlapping attempts cannot advance the same occurrence twice."""
    subscription = await _due(harness)
    lease = await harness.store.acquire_scheduler_lease("d", "a", ttl_seconds=30)
    assert lease is not None
    schedule = await harness.store.get_schedule(_WS, "schedule")
    assert schedule is not None
    now = await harness.store.scheduler_now()
    following = now + timedelta(minutes=1)

    async def advance() -> PendingScheduleFire:
        """Race identical captured input, not a second refreshed due read."""
        return await harness.store.advance_schedule(
            _WS,
            "schedule",
            lease,
            expected_next_fire_at=schedule.next_fire_at,
            expected_revision=subscription.store_revision,
            planned_fire_at=schedule.next_fire_at,
            next_fire_at=following,
        )

    results = await asyncio.gather(
        advance(),
        advance(),
        return_exceptions=True,
    )
    assert sum(isinstance(result, ImmutableViolation) for result in results) == 1
    page = await harness.store.query_pending_schedule_fires(_WS, "d")
    assert len(page.items) == 1 and page.items[0] in results
    assert page.items[0].planned_fire_at == schedule.next_fire_at
    updated = await harness.store.get_schedule(_WS, "schedule")
    assert updated is not None and updated.next_fire_at == following
    if harness.pool is not None:
        restarted = PgMetadataAdapter(pool=harness.pool)
        await restarted.check_scheduler_ready()
        assert (await restarted.query_pending_schedule_fires(_WS, "d")).items == page.items
    await harness.expire("d")
    replacement = await harness.store.acquire_scheduler_lease("d", "b", ttl_seconds=30)
    assert replacement is not None
    assert (await harness.store.query_pending_schedule_fires(_WS, "d")).items == page.items
    with pytest.raises(LeaseExpired):
        await harness.store.acknowledge_schedule_fire(_WS, page.items[0].fire_id, lease)
    await harness.store.acknowledge_schedule_fire(_WS, page.items[0].fire_id, replacement)
    await harness.store.acknowledge_schedule_fire(_WS, page.items[0].fire_id, replacement)
    assert (await harness.store.query_pending_schedule_fires(_WS, "d")).items == ()


@pytest.mark.parametrize("state", [SubscriptionState.PAUSED, SubscriptionState.EXPIRED])
async def test_changed_eligibility_cannot_advance(
    harness: Harness, state: SubscriptionState
) -> None:
    """Eligibility and configuration revisions are checked under transaction locks."""
    original = await _due(harness)
    lease = await harness.store.acquire_scheduler_lease("d", "a", ttl_seconds=30)
    assert lease is not None
    await SubscriptionStore(harness.store).set_state(_WS, "schedule", state)
    with pytest.raises(ImmutableViolation):
        await _advance(harness, original, lease)
    assert (await harness.store.query_pending_schedule_fires(_WS, "d")).items == ()


async def test_active_configuration_revision_is_fenced(harness: Harness) -> None:
    """A still-due active schedule cannot fire with a superseded configuration snapshot."""
    original = await _due(harness)
    lease = await harness.store.acquire_scheduler_lease("d", "a", ttl_seconds=30)
    assert lease is not None
    before = await harness.store.get_schedule(_WS, "schedule")
    await SubscriptionStore(harness.store).update(
        original.model_copy(update={"selector": "event.data.key == 'new'"}),
        expected_revision=original.store_revision,
        preserve_state=True,
    )
    with pytest.raises(ImmutableViolation):
        await _advance(harness, original, lease)
    assert await harness.store.get_schedule(_WS, "schedule") == before
    assert (await harness.store.query_pending_schedule_fires(_WS, "d")).items == ()


async def test_pending_pages_and_workspace_isolation(harness: Harness) -> None:
    """Recovery cursors and acknowledgment cannot cross a workspace or scheduler deployment."""
    lease = await harness.store.acquire_scheduler_lease("d", "a", ttl_seconds=30)
    assert lease is not None
    for identifier in ("a", "b"):
        await _advance(harness, await _due(harness, identifier), lease)
    foreign = await _advance(harness, await _due(harness, "a", WorkspaceId("other")), lease)
    page = await harness.store.query_pending_schedule_fires(_WS, "d", limit=1)
    assert len(page.items) == 1 and page.next_cursor is not None
    last = await harness.store.query_pending_schedule_fires(
        _WS, "d", limit=1, cursor=page.next_cursor
    )
    assert len(last.items) == 1 and last.next_cursor is None
    for workspace, deployment in ((WorkspaceId("other"), "d"), (_WS, "other")):
        with pytest.raises(ValueError):
            await harness.store.query_pending_schedule_fires(
                workspace, deployment, cursor=page.next_cursor
            )
    with pytest.raises(ImmutableViolation):
        await harness.store.acknowledge_schedule_fire(
            WorkspaceId("missing"), foreign.fire_id, lease
        )
    assert (
        len((await harness.store.query_pending_schedule_fires(WorkspaceId("other"), "d")).items)
        == 1
    )


@pytest.mark.integration
async def test_scheduler_schema_is_additive_and_required(pg_pool: Pool) -> None:
    """Older consumers keep their minimum; scheduler settlement requires revision nine."""
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.check_scheduler_ready()
    assert adapter.SCHEMA_REVISION == 4
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM custos_meta.adapter_revisions "
            "WHERE interface_name='MetadataStoreProvider' AND revision=9"
        )
    with pytest.raises(MigrationRequired):
        await adapter.check_scheduler_ready()
    await adapter.apply_pending()
    await adapter.check_scheduler_ready()


@pytest.mark.integration
async def test_pending_fire_rolls_back_if_schedule_write_fails(pg_pool: Pool) -> None:
    """Failure after outbox insertion cannot lose a fire or advance the schedule."""
    store = PgMetadataAdapter(pool=pg_pool)
    harness = Harness(store, [], pg_pool)
    subscription = await _due(harness)
    lease = await store.acquire_scheduler_lease("rollback", "a", ttl_seconds=30)
    assert lease is not None
    before = await store.get_schedule(_WS, "schedule")
    async with pg_pool.acquire() as conn:
        await conn.execute(
            "ALTER TABLE custos_state.schedule ADD CONSTRAINT test_reject_advance "
            "CHECK (next_fire_at <= clock_timestamp())"
        )
    try:
        with pytest.raises(BackendUnavailable):
            await _advance(harness, subscription, lease)
        assert await store.get_schedule(_WS, "schedule") == before
        assert (await store.query_pending_schedule_fires(_WS, "rollback")).items == ()
    finally:
        async with pg_pool.acquire() as conn:
            await conn.execute(
                "ALTER TABLE custos_state.schedule DROP CONSTRAINT test_reject_advance"
            )


@pytest.mark.integration
@pytest.mark.parametrize("expires", ["lease", "plan"])
async def test_clock_is_rechecked_after_waiting_for_schedule_locks(
    pg_pool: Pool, expires: str
) -> None:
    """Lock waits recheck lease and plan expiry with distinct failures and no partial writes."""
    from asyncpg import LockNotAvailableError

    store = PgMetadataAdapter(pool=pg_pool)
    harness = Harness(store, [], pg_pool)
    subscription = await _due(harness)
    lease = await store.acquire_scheduler_lease(
        "blocked", "a", ttl_seconds=2 if expires == "lease" else 30
    )
    assert lease is not None
    next_at = await store.scheduler_now() + timedelta(seconds=2 if expires == "plan" else 60)
    deadline = lease.expires_at if expires == "lease" else next_at
    failure = LeaseExpired if expires == "lease" else ImmutableViolation
    before = await store.get_schedule(_WS, "schedule")
    async with pg_pool.acquire() as blocker:
        transaction = blocker.transaction()
        await transaction.start()
        await blocker.fetchrow(
            "SELECT subscription_id FROM custos_state.subscription "
            "WHERE workspace_id=$1 AND subscription_id='schedule' FOR UPDATE",
            _WS,
        )
        task = asyncio.create_task(_advance(harness, subscription, lease, next_at=next_at))
        try:

            async def waiting_with_lease_lock() -> None:
                """Observe the operation's acquired lease lock, rather than assuming it started."""
                while True:
                    try:
                        async with pg_pool.acquire() as probe, probe.transaction():
                            await probe.fetchrow(
                                "SELECT * FROM custos_state.scheduler_lease "
                                "WHERE deployment_id='blocked' FOR UPDATE NOWAIT"
                            )
                    except LockNotAvailableError:
                        return
                    await asyncio.sleep(0)

            await asyncio.wait_for(waiting_with_lease_lock(), timeout=1)
            async with pg_pool.acquire() as clock:
                assert await clock.fetchval(
                    "SELECT expires_at > clock_timestamp() FROM custos_state.scheduler_lease "
                    "WHERE deployment_id='blocked'"
                )
                await clock.execute(
                    "SELECT pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM "
                    "$1::timestamptz-clock_timestamp())) + 0.02)",
                    deadline,
                )
                if expires == "plan":
                    assert await clock.fetchval(
                        "SELECT expires_at > clock_timestamp() FROM custos_state.scheduler_lease "
                        "WHERE deployment_id='blocked'"
                    )
        finally:
            await transaction.rollback()
            if not task.done():
                with suppress(failure):
                    await asyncio.wait_for(task, timeout=5)
        with pytest.raises(failure):
            await task
    assert await store.get_schedule(_WS, "schedule") == before
    assert (await store.query_pending_schedule_fires(_WS, "blocked")).items == ()

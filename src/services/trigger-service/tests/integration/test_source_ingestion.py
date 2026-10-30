"""Exercise source fan-out, stored resume routing and durable acknowledgement on both backends."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from dataclasses import replace
from datetime import timedelta
from typing import Any, Literal, cast
from unittest.mock import AsyncMock

import pytest
from custos_pg import PgMetadataAdapter
from custos_spl.errors import BackendUnavailable, ImmutableViolation
from custos_spl.ids import SubscriptionId, WorkspaceId
from custos_spl.interfaces.dead_letters import (
    MAX_PAYLOAD_BYTES,
    DeadLetterData,
    DispatchAttempt,
    json_value,
)
from custos_spl.interfaces.delivery import (
    decode_delivery_request,
    delivery_fingerprint,
    delivery_identity,
)
from custos_spl.interfaces.source_delivery import SourceDeliverySelection
from custos_spl.interfaces.trigger_queries import SubscriptionSnapshot
from custos_spl.pagination import Cursor, Page

from custos_trigger.clients import FakeWorkflowServiceClient
from custos_trigger.clients.workflow import (
    RaiseExternalEventRequest,
    WorkflowClientAcknowledgementPending,
    WorkflowClientTransportError,
)
from custos_trigger.dead_letters import DeadLetterAdmin
from custos_trigger.dedup import compute_dispatch_key
from custos_trigger.errors import TriggerError
from custos_trigger.models import ResumeRegistration, SourceType, Subscription
from custos_trigger.normalize import normalize_workflow_event
from custos_trigger.pipeline.dispatch import NoopAuditSink
from custos_trigger.pipeline.reliable import build_start_target
from custos_trigger.providers import InMemoryTriggerMetadataStore
from custos_trigger.receivers.internal import process_workflow_event
from custos_trigger.receivers.sources import (
    SOURCE_PAGE_SIZE,
    adapt_connector_event,
    ingest_source_event,
)
from custos_trigger.selector import SelectorEvaluator
from custos_trigger.source_config import SourceConfig
from custos_trigger.stores import ResumeSubscriptionStore, SubscriptionStore
from tests._source_helpers import NOW, WS, SourceStorage, connector_event, dispatcher, resume, start


@pytest.fixture(params=["memory", pytest.param("postgres", marks=pytest.mark.integration)])
def source_dsn(request: pytest.FixtureRequest) -> str | None:
    """Reset the real database before entering the test's event loop."""
    return None if request.param == "memory" else cast(str, request.getfixturevalue("pg_dsn"))


@pytest.fixture
async def source_store(source_dsn: str | None) -> AsyncIterator[SourceStorage]:
    """Use actual providers, not test-only query hydration."""
    if source_dsn is None:
        memory = InMemoryTriggerMetadataStore(now=lambda: NOW)
        assert isinstance(memory, SourceStorage)
        yield memory
        return
    import asyncpg

    pool = await asyncpg.create_pool(dsn=source_dsn, min_size=1, max_size=4)
    assert pool is not None
    try:
        postgres = PgMetadataAdapter(pool=pool)
        await postgres.apply_pending()
        assert isinstance(postgres, SourceStorage)
        yield postgres
    finally:
        await pool.close()


async def consume(
    store: SourceStorage,
    client: FakeWorkflowServiceClient,
    mode: Literal["push", "pull"] = "push",
    payload: dict[str, Any] | None = None,
) -> None:
    """Run the same scoped ingestion collaborator used by HTTP callbacks."""
    await ingest_source_event(
        adapt_connector_event(payload if payload is not None else connector_event(mode=mode)),
        dispatcher=dispatcher(store, client),
        evaluator=SelectorEvaluator(),
        subscriptions=SubscriptionStore(store),
        resumes=ResumeSubscriptionStore(store),
        audit=NoopAuditSink(),
        scheduler=store,
    )


class PendingResumeClient(FakeWorkflowServiceClient):
    """Record uncertain resume sends until the test simulates consumer recovery."""

    pending = True

    async def raise_external_event(
        self, run_id: str, step_id: str, request: RaiseExternalEventRequest
    ) -> None:
        """Persist the test observation while withholding durable consumption acknowledgement."""
        await super().raise_external_event(run_id, step_id, request)
        if self.pending:
            raise WorkflowClientAcknowledgementPending()


@pytest.mark.parametrize("mode", ["push", "pull"])
async def test_external_start_and_resume_use_only_stored_scope(
    source_store: SourceStorage, mode: Literal["push", "pull"]
) -> None:
    """No payload run/step/workspace value can redirect source-associated delivery."""
    source = SourceType.PULL if mode == "pull" else SourceType.VENDOR_PUSH
    await start(source_store, "start", source=source)
    await start(source_store, "other-connector", source=source, connector="other")
    await start(source_store, "start", source=source, workspace="source-b")
    await resume(source_store, "stored-run-1", mode=mode)
    await resume(source_store, "stored-run-2/child", mode=mode)
    await resume(source_store, "stored-run-1", mode=mode, workspace="source-b")
    await resume(source_store, "expired", mode=mode, expired=True)
    await resume(source_store, "wrong-mode", mode="pull" if mode == "push" else "push")
    await resume(source_store, "wrong-connector", mode=mode, connector="other")
    client = FakeWorkflowServiceClient()
    await consume(source_store, client, mode)
    await consume(source_store, client, mode)
    assert len(client.start_run_calls) == 1
    assert client.start_run_calls[0].inputs == {"amount": 7, "label": "ref-same"}
    assert {run for run, _, _ in client.raise_event_calls} == {"stored-run-1", "stored-run-2/child"}
    assert len(client.raise_event_calls) == 2
    assert all(step == "stored-step" for _, step, _ in client.raise_event_calls)
    assert all(
        request.workspace_id == WS and request.delivery_protocol == 2
        for _, _, request in client.raise_event_calls
    )
    assert all(
        request.payload["runId"] == "forged-run" for _, _, request in client.raise_event_calls
    )


async def test_paged_fanout_does_not_stop_at_first_candidate_page(
    source_store: SourceStorage,
) -> None:
    """Every candidate beyond the exact page boundary is delivered once."""
    for index in range(SOURCE_PAGE_SIZE + 1):
        await start(source_store, f"start-{index:03}")
        await resume(source_store, f"run-{index:03}")
    client = FakeWorkflowServiceClient()
    await consume(source_store, client)
    assert len(client.start_run_calls) == SOURCE_PAGE_SIZE + 1
    assert len(client.raise_event_calls) == SOURCE_PAGE_SIZE + 1
    assert len({call.idempotency_key for call in client.start_run_calls}) == SOURCE_PAGE_SIZE + 1


@pytest.mark.parametrize("mode", ["push", "pull"])
async def test_resume_only_source_needs_no_payload_routing_ids(
    source_store: SourceStorage, mode: Literal["push", "pull"]
) -> None:
    """Source waits work without start subscriptions or supplied run/step fields."""
    await resume(source_store, "stored-only", mode=mode, event_key="image-ready")
    body = connector_event(mode=mode)
    body["payload"] = {"route": "match", "amount": 7}
    client = FakeWorkflowServiceClient()
    await consume(source_store, client, mode, body)
    assert not client.start_run_calls
    assert len(client.raise_event_calls) == 1
    run, step, request = client.raise_event_calls[0]
    assert (run, step, request.workspace_id) == ("stored-only", "stored-step", WS)
    assert request.payload == body["payload"]
    assert request.event_name == "image-ready"


async def test_redelivery_recovers_original_selection_after_waits_change(
    source_store: SourceStorage,
) -> None:
    """Pending acknowledgement cannot lose other selected waits or deliver to a newly added wait."""
    identifiers = [
        await resume(source_store, "original-one"),
        await resume(source_store, "original-two"),
    ]

    client = PendingResumeClient()
    with pytest.raises(TriggerError, match="acknowledgement is pending"):
        await consume(source_store, client)
    assert len(client.raise_event_calls) == 1
    for identifier in identifiers:
        await ResumeSubscriptionStore(source_store).cancel(WS, identifier)
    await resume(source_store, "new-wait")
    client.pending = False
    await consume(source_store, client)
    assert {run for run, _, _ in client.raise_event_calls} == {"original-one", "original-two"}
    assert len(client.raise_event_calls) == 3
    await consume(source_store, client)
    assert len(client.raise_event_calls) == 3


async def test_source_selection_retention_requires_all_receipts_completed(
    source_store: SourceStorage,
) -> None:
    """Even expired plans retain pending targets; completed tombstones prevent future rematching."""
    await resume(source_store, "original")
    client = PendingResumeClient()
    with pytest.raises(TriggerError):
        await consume(source_store, client)
    event = adapt_connector_event(connector_event()).event
    key = delivery_identity(
        event.source.type.value, compute_dispatch_key("source-selection-v1", event)
    )
    selection = await source_store.get_source_delivery(WorkspaceId(WS), key)
    assert selection is not None and selection.requests is not None
    receipt_key = decode_delivery_request(
        selection.requests[0], selection.expires_at
    ).idempotency_key
    if isinstance(source_store, InMemoryTriggerMetadataStore):
        source_store._source_deliveries[(WS, key)] = replace(selection, expires_at=NOW)
        row_key = (WS, "trigger", receipt_key)
        source_store._delivery_rows[row_key] = replace(
            source_store._delivery_rows[row_key], retain_until=NOW
        )
    else:
        assert isinstance(source_store, PgMetadataAdapter)
        pool = await source_store._pool_ref()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE custos_state.source_delivery_selection SET expires_at=clock_timestamp() "
                "WHERE workspace_id=$1 AND key=$2",
                WS,
                key,
            )
            await conn.execute(
                "UPDATE custos_state.delivery_receipt SET retain_until=clock_timestamp() "
                "WHERE workspace_id=$1 AND namespace='trigger' AND key=$2",
                WS,
                receipt_key,
            )
    assert await source_store.clear_expired_delivery_requests(WorkspaceId(WS)) == 0
    assert await source_store.purge_source_delivery_selections(WorkspaceId(WS)) == 0
    client.pending = False
    await consume(source_store, client)
    reliable = dispatcher(source_store, client).reliable
    assert reliable is not None
    swept = await DeadLetterAdmin(reliable).sweep(WS)
    assert swept.expired == 0 and swept.source_selections_expired == 1
    tombstone = await source_store.get_source_delivery(WorkspaceId(WS), key)
    assert tombstone is not None and tombstone.requests is None
    assert tombstone.request_hash == selection.request_hash
    await resume(source_store, "late")
    await consume(source_store, client)
    assert len(client.raise_event_calls) == 2
    changed = connector_event()
    changed["payload"]["amount"] = 99
    with pytest.raises(TriggerError, match="selection is not acknowledged"):
        await consume(source_store, client, payload=changed)
    assert len(client.raise_event_calls) == 2


async def test_source_selection_failure_precedes_every_effect(
    source_store: SourceStorage,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed plan commit cannot dispatch even the first selected target."""
    await start(source_store, "first")
    await resume(source_store, "second")
    monkeypatch.setattr(
        source_store, "put_source_deliveries", AsyncMock(side_effect=BackendUnavailable("offline"))
    )
    client = FakeWorkflowServiceClient()
    with pytest.raises(TriggerError, match="selection is not acknowledged"):
        await consume(source_store, client)
    assert not client.start_run_calls and not client.raise_event_calls


async def test_internal_redelivery_uses_the_same_accepted_target_selection(
    source_store: SourceStorage,
) -> None:
    """Durable internal callbacks cannot lose a removed wait or select a late start target."""
    from custos_trigger.api.routes.rpc import compute_resume_id

    now = await source_store.delivery_now()
    event = normalize_workflow_event(
        {
            "workflowVersionId": "source-version",
            "workspace": WS,
            "runId": "original",
            "stepId": "step",
            "occurredAt": now.isoformat(),
            "status": "succeeded",
        }
    )
    registration = ResumeRegistration(
        workspace_id=WS,
        run_id="original",
        step_id="step",
        event_key=event.kind,
        delivery_protocol=2,
    )
    identifier = compute_resume_id("original", "step", event.kind)
    resumes = ResumeSubscriptionStore(source_store)
    subscriptions = SubscriptionStore(source_store)
    await resumes.register(
        registration, workspace_id=WS, resume_id=identifier, expires_at=now + timedelta(hours=1)
    )
    first = Subscription(
        workspace_id=WS,
        subscription_id="first",
        workflow_id="workflow",
        target_workflow_version_id="target",
        source_type=SourceType.INTERNAL,
        selector="true",
        input_mapping={},
        created_at=now,
        updated_at=now,
    )
    await subscriptions.create(first)
    client = PendingResumeClient()

    async def internal() -> None:
        """Create a fresh dispatcher for each delivery, using only persisted accepted intent."""
        await process_workflow_event(
            event,
            dispatcher=dispatcher(source_store, client),
            evaluator=SelectorEvaluator(),
            subscription_store=subscriptions,
            resume_store=resumes,
            now=lambda: now,
        )

    with pytest.raises(TriggerError, match="acknowledgement is pending"):
        await internal()
    await resumes.cancel(WS, identifier)
    await subscriptions.create(first.model_copy(update={"subscription_id": "late"}))
    client.pending = False
    await internal()
    assert len(client.start_run_calls) == 1
    assert len(client.raise_event_calls) == 2
    assert {run for run, _, _ in client.raise_event_calls} == {"original"}


async def test_empty_source_selection_does_not_match_late_registrations(
    source_store: SourceStorage,
) -> None:
    """An acknowledged no-match event stays a no-match after source intent changes."""
    client = FakeWorkflowServiceClient()
    await consume(source_store, client)
    await start(source_store, "late")
    await resume(source_store, "late")
    await consume(source_store, client)
    assert not client.start_run_calls and not client.raise_event_calls


@pytest.mark.parametrize("mode", ["push", "pull"])
async def test_publication_time_changes_recover_the_original_snapshot(
    source_store: SourceStorage,
    mode: Literal["push", "pull"],
) -> None:
    """A new receivedAt cannot replace the original event snapshot or selected target."""
    await resume(source_store, "original", mode=mode)
    client = PendingResumeClient()
    original = connector_event(mode=mode)
    with pytest.raises(TriggerError):
        await consume(source_store, client, mode, original)
    repeated = connector_event(mode=mode)
    repeated["source"]["receivedAt"] = (NOW + timedelta(minutes=5)).isoformat()
    client.pending = False
    await consume(source_store, client, mode, repeated)
    assert len(client.raise_event_calls) == 2
    event = adapt_connector_event(original).event
    key = delivery_identity(
        event.source.type.value, compute_dispatch_key("source-selection-v1", event)
    )
    selection = await source_store.get_source_delivery(WorkspaceId(WS), key)
    assert selection is not None and selection.requests
    saved = decode_delivery_request(selection.requests[0], selection.expires_at)
    saved_source = saved.event["source"]
    assert isinstance(saved_source, Mapping)
    assert saved_source["occurredAt"] == event.source.occurred_at


async def test_source_batch_binding_is_atomic(source_store: SourceStorage) -> None:
    """A conflicting shared arm rolls back a new mode-specific arm before either can dispatch."""
    now = await source_store.delivery_now()
    prior = SourceDeliverySelection(WorkspaceId(WS), "shared", "a" * 64, now, ())
    await source_store.put_source_delivery(prior)
    first = replace(prior, key="new-mode")
    with pytest.raises(ImmutableViolation):
        await source_store.put_source_deliveries((first, replace(prior, request_hash="b" * 64)))
    assert await source_store.get_source_delivery(WorkspaceId(WS), "new-mode") is None
    assert await source_store.get_source_delivery(WorkspaceId(WS), "shared") == prior


async def test_pending_selections_are_discoverable_and_reconciled_without_broker(
    source_store: SourceStorage,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Discover unstarted work, retain failures, and recover targets after wait removal."""
    identifiers = [await resume(source_store, name) for name in ("first", "second")]
    client = PendingResumeClient()
    with monkeypatch.context() as patch:
        patch.setattr(
            source_store, "reserve_delivery", AsyncMock(side_effect=BackendUnavailable("offline"))
        )
        for event_id in ("one", "two"):
            with pytest.raises(TriggerError):
                await consume(source_store, client, payload=connector_event(event_id=event_id))
    assert not client.raise_event_calls
    for identifier in identifiers:
        await ResumeSubscriptionStore(source_store).cancel(WS, identifier)
    await resume(source_store, "late")
    page = await source_store.query_pending_source_deliveries(WorkspaceId(WS), limit=1)
    assert len(page.items) == 1 and page.next_cursor is not None
    with pytest.raises(ValueError):
        await source_store.query_pending_source_deliveries(
            WorkspaceId("other"), cursor=page.next_cursor, limit=1
        )
    assert not (await source_store.query_pending_source_deliveries(WorkspaceId("other"))).items
    reliable = dispatcher(source_store, client).reliable
    assert reliable is not None
    pending = await reliable.reconcile_pending(WS, limit=1)
    assert pending.selections == 1 and pending.pending_targets == 2 and pending.settled_targets == 0
    client.pending = False
    cursor = None
    settled = 0
    while True:
        result = await reliable.reconcile_pending(WS, cursor=cursor, limit=1)
        assert result.pending_targets == 0
        settled += result.settled_targets
        cursor = result.next_cursor
        if cursor is None:
            break
    assert settled == 4 and len(client.raise_event_calls) == 6
    assert {run for run, _, _ in client.raise_event_calls} == {"first", "second"}
    assert not (await source_store.query_pending_source_deliveries(WorkspaceId(WS))).items


@pytest.mark.parametrize("failure", ["backend", "timeout"])
async def test_reconciliation_failure_is_not_an_empty_success(
    source_store: SourceStorage,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    """Discovery failures and actual deadline cancellation remain explicit retryable failures."""
    cancelled = asyncio.Event()

    async def stalled(
        workspace_id: WorkspaceId,
        *,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[SourceDeliverySelection]:
        """Block until the reconciler's real timeout cancels its pending query."""
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
        raise AssertionError("query should have been cancelled")

    if failure == "backend":
        monkeypatch.setattr(
            source_store,
            "query_pending_source_deliveries",
            AsyncMock(side_effect=BackendUnavailable("private backend details")),
        )
    else:
        monkeypatch.setattr("custos_trigger.pipeline.reliable.MAX_DISPATCH_SECONDS", 0.01)
        monkeypatch.setattr(source_store, "query_pending_source_deliveries", stalled)
    client = FakeWorkflowServiceClient()
    reliable = dispatcher(source_store, client).reliable
    assert reliable is not None
    with pytest.raises(TriggerError, match="reconciliation did not complete"):
        await reliable.reconcile_pending(WS)
    assert not client.start_run_calls and not client.raise_event_calls
    assert cancelled.is_set() == (failure == "timeout")


async def test_downstream_failure_is_durably_captured_before_ack(
    source_store: SourceStorage,
) -> None:
    """Broker redelivery does not duplicate a failed original attempt once capture is committed."""
    await start(source_store, "start")
    failed = FakeWorkflowServiceClient(error=WorkflowClientTransportError("offline"))
    await consume(source_store, failed)
    captured = (await source_store.query_dead_letters(WorkspaceId(WS))).items
    assert len(captured) == 1
    assert captured[0].data.event["eventId"] == "event"
    assert json_value(captured[0].data.target["inputs"]) == {"amount": 7, "label": "ref-same"}
    await consume(source_store, failed)
    assert len(failed.start_run_calls) == 1


async def test_connector_rebinding_does_not_alias_prior_event_ids(
    source_store: SourceStorage,
) -> None:
    """Different connector instances may reuse producer IDs without false duplicate suppression."""
    subscription = await start(source_store, "start")
    client = FakeWorkflowServiceClient()
    await consume(source_store, client)
    await SubscriptionStore(source_store).update(
        subscription.model_copy(
            update={"source_config": SourceConfig(connector_instance_id="other")}
        ),
        expected_revision=subscription.store_revision,
    )
    incoming = connector_event()
    incoming["source"]["instanceId"] = "other"
    await consume(source_store, client, payload=incoming)
    assert len(client.start_run_calls) == 2
    assert client.start_run_calls[0].idempotency_key != client.start_run_calls[1].idempotency_key
    assert client.start_run_calls[1].inputs["label"] == "ref-other"


@pytest.mark.parametrize("completed", [False, True])
@pytest.mark.parametrize("mapping_failure", ["unretainable", "unresolvable"])
async def test_existing_receipts_precede_changed_mapping_preparation(
    source_store: SourceStorage, completed: bool, mapping_failure: str
) -> None:
    """Accepted targets recover or deduplicate without evaluating a newly broken mapping."""
    sub = await start(source_store, "recover")
    client = FakeWorkflowServiceClient()
    expected = {"amount": 7, "label": "ref-same"}
    if completed:
        await consume(source_store, client)
    else:
        event = adapt_connector_event(connector_event()).event
        key = compute_dispatch_key(sub.subscription_id, event)
        now = await source_store.delivery_now()
        data = DeadLetterData(
            workspace_id=WorkspaceId(WS),
            entry_id="dl_" + delivery_identity(WS, key),
            subscription_id=sub.subscription_id,
            idempotency_key=key,
            event=event.model_dump(mode="json", by_alias=True, exclude={"raw"}),
            target=build_start_target(sub.model_copy(update={"input_mapping": expected})),
            history=(DispatchAttempt(now, "configuration"),),
        )
        claim = await source_store.reserve_delivery(
            WorkspaceId(WS),
            "trigger",
            key,
            delivery_fingerprint({"target": json_value(data.target)}),
            now + timedelta(hours=1),
            request=data,
        )
        await source_store.release_delivery(claim)
    invalid_mapping = (
        {"oversized": "x" * MAX_PAYLOAD_BYTES}
        if mapping_failure == "unretainable"
        else {"missing": "${{ event.data.absent }}"}
    )
    await SubscriptionStore(source_store).update(
        sub.model_copy(update={"input_mapping": invalid_mapping}),
        expected_revision=sub.store_revision,
    )
    await consume(source_store, client)
    await consume(source_store, client)
    assert len(client.start_run_calls) == 1
    assert client.start_run_calls[0].inputs == expected


async def test_active_revision_conflict_retries_instead_of_losing_delivery(
    source_store: SourceStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A mapping change after candidate selection cannot become a successful skipped delivery."""
    subscription = await start(source_store, "changed")
    original = source_store.get_subscription
    changed = False

    async def racing_get(
        workspace_id: WorkspaceId, subscription_id: SubscriptionId
    ) -> SubscriptionSnapshot | None:
        """Commit a real metadata edit immediately before the final candidate read."""
        nonlocal changed
        if not changed and subscription_id == "changed":
            changed = True
            await SubscriptionStore(source_store).update(
                subscription.model_copy(update={"input_mapping": {"amount": 99}}),
                expected_revision=subscription.store_revision,
            )
        return await original(workspace_id, subscription_id)

    monkeypatch.setattr(source_store, "get_subscription", racing_get)
    client = FakeWorkflowServiceClient()
    with pytest.raises(TriggerError, match="changed before delivery"):
        await consume(source_store, client)
    assert not client.start_run_calls
    await consume(source_store, client)
    assert len(client.start_run_calls) == 1
    assert client.start_run_calls[0].inputs == {"amount": 99}


async def test_same_connector_event_across_modes_resumes_once(source_store: SourceStorage) -> None:
    """A wait allowing both modes still sees one logical producer event, not two effects."""
    registration = ResumeRegistration.model_validate(
        {
            "runId": "both-modes",
            "stepId": "stored-step",
            "eventKey": "registry.push",
            "workspaceId": WS,
            "deliveryProtocol": 2,
            "selector": "event.data.route == 'match'",
            "source": {
                "connectorInstanceId": "same",
                "deliveryModes": ["push", "pull"],
                "pollIntervalSeconds": 10,
            },
        }
    )
    from datetime import timedelta

    await ResumeSubscriptionStore(source_store).register(
        registration,
        workspace_id=WS,
        resume_id="both-modes",
        expires_at=await source_store.delivery_now() + timedelta(hours=1),
    )
    client = FakeWorkflowServiceClient()
    await consume(source_store, client, "push")
    await consume(source_store, client, "pull")
    assert len(client.raise_event_calls) == 1


@pytest.mark.parametrize("first_mode", ["push", "pull"])
async def test_dual_mode_wait_selection_is_shared_without_merging_mode_specific_intent(
    source_store: SourceStorage,
    first_mode: Literal["push", "pull"],
) -> None:
    """Late dual-mode waits cannot receive an old event; mode-specific starts/waits still work."""

    async def dual(identifier: str) -> None:
        """Register one wait that explicitly accepts either delivery mode."""
        registration = ResumeRegistration.model_validate(
            {
                "workspaceId": WS,
                "runId": identifier,
                "stepId": "stored-step",
                "eventKey": "registry.push",
                "deliveryProtocol": 2,
                "selector": "event.data.route == 'match'",
                "source": {
                    "connectorInstanceId": "same",
                    "deliveryModes": ["push", "pull"],
                    "pollIntervalSeconds": 10,
                },
            }
        )
        await ResumeSubscriptionStore(source_store).register(
            registration,
            workspace_id=WS,
            resume_id=identifier,
            expires_at=await source_store.delivery_now() + timedelta(hours=1),
        )

    await dual("original-dual")
    await resume(source_store, "push-only", mode="push")
    await resume(source_store, "pull-only", mode="pull")
    await start(source_store, "push-start", source=SourceType.VENDOR_PUSH)
    await start(source_store, "pull-start", source=SourceType.PULL)
    client = FakeWorkflowServiceClient()
    await consume(source_store, client, first_mode)
    await dual("late-dual")
    second_mode: Literal["push", "pull"] = "pull" if first_mode == "push" else "push"
    await consume(source_store, client, second_mode)
    assert len(client.start_run_calls) == 2
    assert len(client.raise_event_calls) == 3
    assert {run for run, _, _ in client.raise_event_calls} == {
        "original-dual",
        "push-only",
        "pull-only",
    }


async def test_failed_capture_remains_unacknowledged(
    source_store: SourceStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A successful-looking dead-letter outcome cannot hide failed durable persistence."""
    await start(source_store, "start")
    monkeypatch.setattr(
        source_store,
        "capture_failed_delivery",
        AsyncMock(side_effect=BackendUnavailable("offline")),
    )
    with pytest.raises(TriggerError, match="not acknowledged"):
        await consume(
            source_store,
            FakeWorkflowServiceClient(error=WorkflowClientTransportError("downstream")),
        )

"""Internal workflow-event receiver tests (TS-IMPL-017).

Covers the candidate-enumeration store surface
(:meth:`SubscriptionStore.list_in_workspace`), the
:func:`process_workflow_event` pipeline glue (start / resume / dual-match /
duplicate-absorption / lapsed-resume), and the Dapr Pub/Sub HTTP surface
(``GET /dapr/subscribe`` discovery + the delivery route's ack semantics).
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from custos_trigger.api.routes.rpc import RESUME_WORKSPACE, compute_resume_id
from custos_trigger.clients import FakeWorkflowServiceClient
from custos_trigger.dedup import Deduplicator
from custos_trigger.middleware.callctx import _BYPASS_PATHS
from custos_trigger.models import (
    ResumeRegistration,
    SourceType,
    Subscription,
    SubscriptionKind,
    SubscriptionState,
)
from custos_trigger.normalize import normalize_workflow_event
from custos_trigger.pipeline.dispatch import Dispatcher
from custos_trigger.providers import InMemoryTriggerMetadataStore, Providers
from custos_trigger.receivers import (
    DAPR_SUBSCRIBE_PATH,
    INTERNAL_EVENTS_PATH,
    DeliveryStatus,
    InternalEventOutcome,
    build_internal_event_router,
    process_workflow_event,
)
from custos_trigger.receivers import internal as receiver
from custos_trigger.selector import SelectorEvaluator
from custos_trigger.stores import (
    ResumeSubscriptionStore,
    SubscriptionListUnsupportedError,
    SubscriptionStore,
)
from custos_trigger.stores.base import TriggerMetadataStore

_NOW = datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC)
_OCCURRED_AT = "2026-06-04T12:00:00Z"
_WORKSPACE = "ws-1"
_RUN_ID = "run-child"
_STEP_ID = "step-parent"
_COMPLETED_KIND = "workflow.completed"


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _envelope(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "workflowVersionId": "wfv-1",
        "runId": _RUN_ID,
        "workspace": _WORKSPACE,
        "occurredAt": _OCCURRED_AT,
        "status": "succeeded",
    }
    base.update(overrides)
    return base


def _start_sub(
    *,
    subscription_id: str = "sub-start",
    selector: str | None = None,
    state: SubscriptionState = SubscriptionState.ACTIVE,
) -> Subscription:
    return Subscription(
        workspace_id=_WORKSPACE,
        subscription_id=subscription_id,
        kind=SubscriptionKind.START,
        source_type=SourceType.INTERNAL,
        workflow_id="wf-downstream",
        target_workflow_version_id="wfv-downstream",
        selector=selector,
        state=state,
        created_at=_NOW,
        updated_at=_NOW,
    )


async def _register_resume(
    resume_store: ResumeSubscriptionStore,
    *,
    run_id: str = _RUN_ID,
    step_id: str = _STEP_ID,
    event_key: str = _COMPLETED_KIND,
    selector: str | None = None,
    expires_at: datetime | None = None,
) -> str:
    resume_id = compute_resume_id(run_id, step_id, event_key)
    await resume_store.register(
        ResumeRegistration(
            run_id=run_id,
            step_id=step_id,
            event_key=event_key,
            selector=selector,
        ),
        workspace_id=RESUME_WORKSPACE,
        resume_id=resume_id,
        expires_at=expires_at if expires_at is not None else _NOW + timedelta(hours=1),
    )
    return resume_id


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_client() -> FakeWorkflowServiceClient:
    return FakeWorkflowServiceClient()


@pytest.fixture
def dispatcher(
    fake_client: FakeWorkflowServiceClient,
    metadata_store: InMemoryTriggerMetadataStore,
) -> Dispatcher:
    return Dispatcher(fake_client, Deduplicator(metadata_store))


@pytest.fixture
def evaluator() -> SelectorEvaluator:
    return SelectorEvaluator()


@pytest.fixture
def subscription_store(metadata_store: InMemoryTriggerMetadataStore) -> SubscriptionStore:
    return SubscriptionStore(metadata_store)


@pytest.fixture
def resume_store(metadata_store: InMemoryTriggerMetadataStore) -> ResumeSubscriptionStore:
    return ResumeSubscriptionStore(metadata_store)


@pytest.fixture(autouse=True)
def frozen_now(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the receiver's resume-expiry clock for deterministic lapse tests."""
    monkeypatch.setattr(receiver, "_now", lambda: _NOW)


# ---------------------------------------------------------------------------
# SubscriptionStore.list_in_workspace
# ---------------------------------------------------------------------------


async def test_list_in_workspace_returns_workspace_rows(
    subscription_store: SubscriptionStore,
) -> None:
    await subscription_store.create(_start_sub(subscription_id="a", selector="true"))
    await subscription_store.create(_start_sub(subscription_id="b"))
    rows = await subscription_store.list_in_workspace(_WORKSPACE)
    by_id = {sub.subscription_id: sub for sub in rows}
    assert set(by_id) == {"a", "b"}
    # The latest selector blob is rehydrated onto the row.
    assert by_id["a"].selector == "true"


async def test_list_in_workspace_excludes_other_workspaces(
    subscription_store: SubscriptionStore,
    metadata_store: InMemoryTriggerMetadataStore,
) -> None:
    await subscription_store.create(_start_sub(subscription_id="here"))
    other = SubscriptionStore(metadata_store)
    elsewhere = _start_sub(subscription_id="there")
    elsewhere = elsewhere.model_copy(update={"workspace_id": "ws-other"})
    await other.create(elsewhere)
    rows = await subscription_store.list_in_workspace(_WORKSPACE)
    assert [sub.subscription_id for sub in rows] == ["here"]


def test_list_in_workspace_requires_listable_backend() -> None:
    import asyncio

    write_only = cast(TriggerMetadataStore, object())
    store = SubscriptionStore(write_only)
    with pytest.raises(SubscriptionListUnsupportedError):
        asyncio.run(store.list_in_workspace(_WORKSPACE))


# ---------------------------------------------------------------------------
# process_workflow_event — start arm
# ---------------------------------------------------------------------------


async def test_start_match_dispatches_run(
    dispatcher: Dispatcher,
    evaluator: SelectorEvaluator,
    subscription_store: SubscriptionStore,
    resume_store: ResumeSubscriptionStore,
    fake_client: FakeWorkflowServiceClient,
) -> None:
    await subscription_store.create(_start_sub(selector='event.data.status == "succeeded"'))
    event = normalize_workflow_event(_envelope())
    outcome = await process_workflow_event(
        event,
        dispatcher=dispatcher,
        evaluator=evaluator,
        subscription_store=subscription_store,
        resume_store=resume_store,
    )
    assert [o.is_dispatched for o in outcome.start] == [True]
    assert len(fake_client.start_run_calls) == 1
    started = fake_client.start_run_calls[0]
    assert started.workspace_id == _WORKSPACE
    assert started.workflow_version_id == "wfv-downstream"


async def test_start_selector_miss_does_not_dispatch(
    dispatcher: Dispatcher,
    evaluator: SelectorEvaluator,
    subscription_store: SubscriptionStore,
    resume_store: ResumeSubscriptionStore,
    fake_client: FakeWorkflowServiceClient,
) -> None:
    await subscription_store.create(_start_sub(selector='event.data.status == "failed"'))
    event = normalize_workflow_event(_envelope())
    outcome = await process_workflow_event(
        event,
        dispatcher=dispatcher,
        evaluator=evaluator,
        subscription_store=subscription_store,
        resume_store=resume_store,
    )
    assert outcome.start == ()
    assert fake_client.start_run_calls == []


# ---------------------------------------------------------------------------
# process_workflow_event — resume arm
# ---------------------------------------------------------------------------


async def test_resume_match_raises_external_event(
    dispatcher: Dispatcher,
    evaluator: SelectorEvaluator,
    subscription_store: SubscriptionStore,
    resume_store: ResumeSubscriptionStore,
    fake_client: FakeWorkflowServiceClient,
) -> None:
    await _register_resume(resume_store)
    event = normalize_workflow_event(_envelope(stepId=_STEP_ID))
    outcome = await process_workflow_event(
        event,
        dispatcher=dispatcher,
        evaluator=evaluator,
        subscription_store=subscription_store,
        resume_store=resume_store,
    )
    assert [o.is_dispatched for o in outcome.resume] == [True]
    assert len(fake_client.raise_event_calls) == 1
    run_id, step_id, request = fake_client.raise_event_calls[0]
    assert (run_id, step_id) == (_RUN_ID, _STEP_ID)
    assert request.workspace_id == _WORKSPACE
    assert request.event_name == _COMPLETED_KIND


async def test_resume_without_step_id_is_skipped(
    dispatcher: Dispatcher,
    evaluator: SelectorEvaluator,
    subscription_store: SubscriptionStore,
    resume_store: ResumeSubscriptionStore,
    fake_client: FakeWorkflowServiceClient,
) -> None:
    await _register_resume(resume_store)
    # No stepId in the envelope -> resume key cannot be extracted.
    event = normalize_workflow_event(_envelope())
    outcome = await process_workflow_event(
        event,
        dispatcher=dispatcher,
        evaluator=evaluator,
        subscription_store=subscription_store,
        resume_store=resume_store,
    )
    assert outcome.resume == ()
    assert fake_client.raise_event_calls == []


async def test_resume_lapsed_registration_is_skipped(
    dispatcher: Dispatcher,
    evaluator: SelectorEvaluator,
    subscription_store: SubscriptionStore,
    resume_store: ResumeSubscriptionStore,
    fake_client: FakeWorkflowServiceClient,
) -> None:
    await _register_resume(resume_store, expires_at=_NOW - timedelta(seconds=1))
    event = normalize_workflow_event(_envelope(stepId=_STEP_ID))
    outcome = await process_workflow_event(
        event,
        dispatcher=dispatcher,
        evaluator=evaluator,
        subscription_store=subscription_store,
        resume_store=resume_store,
    )
    assert outcome.resume == ()
    assert fake_client.raise_event_calls == []


async def test_resume_unregistered_triple_is_noop(
    dispatcher: Dispatcher,
    evaluator: SelectorEvaluator,
    subscription_store: SubscriptionStore,
    resume_store: ResumeSubscriptionStore,
    fake_client: FakeWorkflowServiceClient,
) -> None:
    event = normalize_workflow_event(_envelope(stepId=_STEP_ID))
    outcome = await process_workflow_event(
        event,
        dispatcher=dispatcher,
        evaluator=evaluator,
        subscription_store=subscription_store,
        resume_store=resume_store,
    )
    assert outcome.resume == ()
    assert fake_client.raise_event_calls == []


# ---------------------------------------------------------------------------
# process_workflow_event — dual-match + dedup + no-workspace
# ---------------------------------------------------------------------------


async def test_dual_match_starts_and_resumes(
    dispatcher: Dispatcher,
    evaluator: SelectorEvaluator,
    subscription_store: SubscriptionStore,
    resume_store: ResumeSubscriptionStore,
    fake_client: FakeWorkflowServiceClient,
) -> None:
    await subscription_store.create(_start_sub())  # unconditional start
    await _register_resume(resume_store)
    event = normalize_workflow_event(_envelope(stepId=_STEP_ID))
    outcome = await process_workflow_event(
        event,
        dispatcher=dispatcher,
        evaluator=evaluator,
        subscription_store=subscription_store,
        resume_store=resume_store,
    )
    assert [o.is_dispatched for o in outcome.start] == [True]
    assert [o.is_dispatched for o in outcome.resume] == [True]
    assert len(fake_client.start_run_calls) == 1
    assert len(fake_client.raise_event_calls) == 1


async def test_duplicate_delivery_is_absorbed_by_dedup(
    dispatcher: Dispatcher,
    evaluator: SelectorEvaluator,
    subscription_store: SubscriptionStore,
    resume_store: ResumeSubscriptionStore,
    fake_client: FakeWorkflowServiceClient,
) -> None:
    await subscription_store.create(_start_sub())
    await _register_resume(resume_store)
    event = normalize_workflow_event(_envelope(stepId=_STEP_ID))
    first = await process_workflow_event(
        event,
        dispatcher=dispatcher,
        evaluator=evaluator,
        subscription_store=subscription_store,
        resume_store=resume_store,
    )
    second = await process_workflow_event(
        event,
        dispatcher=dispatcher,
        evaluator=evaluator,
        subscription_store=subscription_store,
        resume_store=resume_store,
    )
    assert [o.is_dispatched for o in first.start] == [True]
    assert [o.is_dispatched for o in first.resume] == [True]
    assert [o.is_duplicate for o in second.start] == [True]
    assert [o.is_duplicate for o in second.resume] == [True]
    # The redelivery issued no new RPCs.
    assert len(fake_client.start_run_calls) == 1
    assert len(fake_client.raise_event_calls) == 1


async def test_event_without_workspace_is_unroutable(
    dispatcher: Dispatcher,
    evaluator: SelectorEvaluator,
    subscription_store: SubscriptionStore,
    resume_store: ResumeSubscriptionStore,
    fake_client: FakeWorkflowServiceClient,
) -> None:
    await subscription_store.create(_start_sub())
    event = normalize_workflow_event(_envelope(workspace=""))
    outcome = await process_workflow_event(
        event,
        dispatcher=dispatcher,
        evaluator=evaluator,
        subscription_store=subscription_store,
        resume_store=resume_store,
    )
    assert outcome == InternalEventOutcome()
    assert fake_client.start_run_calls == []


# ---------------------------------------------------------------------------
# HTTP surface
# ---------------------------------------------------------------------------


@pytest.fixture
def client(
    dispatcher: Dispatcher,
    evaluator: SelectorEvaluator,
    subscription_store: SubscriptionStore,
    resume_store: ResumeSubscriptionStore,
) -> Iterator[TestClient]:
    app = FastAPI()
    app.state.dispatcher = dispatcher
    app.state.selector_evaluator = evaluator
    app.state.subscription_store = subscription_store
    app.state.resume_subscription_store = resume_store
    app.include_router(
        build_internal_event_router(
            pubsub_component="custos-pubsub",
            workflow_events_topic="custos.workflow.events",
        )
    )
    with TestClient(app) as test_client:
        yield test_client


def _cloud_event(envelope: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": "ce-1",
        "source": "workflow-service",
        "type": "com.dapr.event.sent",
        "datacontenttype": "application/json",
        "data": envelope,
    }


def test_dapr_subscribe_declares_workflow_topic(client: TestClient) -> None:
    response = client.get(DAPR_SUBSCRIBE_PATH)
    assert response.status_code == 200
    subscriptions = response.json()
    assert subscriptions == [
        {
            "pubsubname": "custos-pubsub",
            "topic": "custos.workflow.events",
            "route": INTERNAL_EVENTS_PATH,
            "metadata": {},
        }
    ]


def test_delivery_routes_and_acks_success(
    client: TestClient,
    subscription_store: SubscriptionStore,
    fake_client: FakeWorkflowServiceClient,
) -> None:
    import asyncio

    asyncio.run(subscription_store.create(_start_sub()))
    response = client.post(INTERNAL_EVENTS_PATH, json=_cloud_event(_envelope()))
    assert response.status_code == 200
    assert response.json() == {"status": DeliveryStatus.SUCCESS.value}
    assert len(fake_client.start_run_calls) == 1


def test_delivery_drops_malformed_envelope(
    client: TestClient,
    fake_client: FakeWorkflowServiceClient,
) -> None:
    # Missing runId -> normalization fails -> permanent drop, no redelivery.
    bad = _envelope()
    del bad["runId"]
    response = client.post(INTERNAL_EVENTS_PATH, json=_cloud_event(bad))
    assert response.status_code == 200
    assert response.json() == {"status": DeliveryStatus.DROP.value}
    assert fake_client.start_run_calls == []


def test_delivery_drops_non_canonical_kind(
    client: TestClient,
    fake_client: FakeWorkflowServiceClient,
) -> None:
    response = client.post(
        INTERNAL_EVENTS_PATH,
        json=_cloud_event(_envelope(kind="workflow.bogus", status=None)),
    )
    assert response.status_code == 200
    assert response.json() == {"status": DeliveryStatus.DROP.value}
    assert fake_client.start_run_calls == []


def test_receiver_paths_bypass_call_context() -> None:
    assert DAPR_SUBSCRIBE_PATH in _BYPASS_PATHS
    assert INTERNAL_EVENTS_PATH in _BYPASS_PATHS


def test_in_memory_store_satisfies_listable(providers: Providers) -> None:
    # The in-memory provider bundle satisfies the listable capability the
    # receiver's candidate enumeration probes structurally.
    from custos_trigger.stores.base import SubscriptionListable

    assert isinstance(providers.metadata_store, SubscriptionListable)

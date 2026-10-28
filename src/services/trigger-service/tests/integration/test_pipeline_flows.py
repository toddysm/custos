"""Postgres-backed end-to-end pipeline flows (TS-IMPL-020).

Two durable flows proven against a *real* Postgres (see ``conftest.py`` for the
DSN / testcontainers resolution):

* **manual-fire -> ``StartRun``** — a start subscription (with a CEL selector)
  is persisted through the real :class:`SubscriptionStore`, read back +
  rehydrated from the SPL rows, matched by the real :class:`StartMatcher`, and
  dispatched through the real :class:`Dispatcher` to a Workflow Service
  ``Fake``. The dedup reserve-or-read is exercised against the live
  ``custos_state.dedup_key`` ledger: a redelivered fire collapses to
  ``DUPLICATE``.
* **resume-register -> internal event -> ``RaiseExternalEvent``** — a resume
  token is persisted through the real :class:`ResumeSubscriptionStore`, read
  back from the SPL row, matched on the ``(runId, stepId, eventKey)`` triple by
  the real :class:`ResumeMatcher`, and dispatched to the ``Fake``; the
  redelivery likewise collapses to ``DUPLICATE``.

Read-back uses the production store/query-provider path. Raw SQL is used only
to assert the resulting dedup row count, never to substitute for missing
production subscription or resume reads.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from custos_trigger.clients.workflow import FakeWorkflowServiceClient
from custos_trigger.dedup import Deduplicator
from custos_trigger.models import (
    ResumeRegistration,
    SourceType,
    Subscription,
    SubscriptionKind,
    SubscriptionState,
)
from custos_trigger.normalize import normalize_manual_fire, normalize_workflow_event
from custos_trigger.pipeline.dispatch import Dispatcher, DispatchStatus
from custos_trigger.pipeline.match_resume import ResumeCandidate, ResumeMatcher
from custos_trigger.pipeline.match_start import StartMatcher
from custos_trigger.selector import SelectorEvaluator
from custos_trigger.stores.base import TriggerMetadataStore
from custos_trigger.stores.resume import ResumeSubscriptionStore
from custos_trigger.stores.subscriptions import SubscriptionStore

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]

_WS = "ws_integration"
_NOW = datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC)


async def _dedup_count(pool: Any, workspace_id: str) -> int:
    """Inspect persisted dedup effects without bypassing the store read path."""
    async with pool.acquire() as conn:
        value = await conn.fetchval(
            "SELECT count(*) FROM custos_state.dedup_key WHERE workspace_id = $1",
            workspace_id,
        )
    return int(value)


# ---------------------------------------------------------------------------
# Flow A — manual fire -> StartRun
# ---------------------------------------------------------------------------


async def test_manual_fire_dispatches_start_run_against_postgres(
    metadata_store: TriggerMetadataStore, pg_pool: Any
) -> None:
    store = SubscriptionStore(metadata_store, now=lambda: _NOW)
    subscription = Subscription(
        workspace_id=_WS,
        subscription_id="sub_manual_1",
        kind=SubscriptionKind.START,
        source_type=SourceType.MANUAL,
        workflow_id="wf-1",
        target_workflow_version_id="wfv-1",
        selector='event.data.tier == "gold"',
        input_mapping={"tier": "gold"},
        state=SubscriptionState.ACTIVE,
        created_at=_NOW,
        updated_at=_NOW,
    )

    # Persist through the real store (base row + first selector revision).
    await store.create(subscription)

    # Read it back + rehydrate from the live SPL rows (durable round-trip).
    rehydrated = await store.get(_WS, "sub_manual_1")
    assert rehydrated is not None
    assert rehydrated.selector == 'event.data.tier == "gold"'
    assert rehydrated.target_workflow_version_id == "wfv-1"
    assert rehydrated.input_mapping == {"tier": "gold"}

    evaluator = SelectorEvaluator()
    event = normalize_manual_fire(
        occurred_at=_NOW.isoformat(),
        subscription_id="sub_manual_1",
        inputs={"tier": "gold"},
    )
    matches = StartMatcher(evaluator).match(event, [rehydrated])
    assert len(matches) == 1

    fake = FakeWorkflowServiceClient()
    dispatcher = Dispatcher(fake, Deduplicator(metadata_store))
    outcome = await dispatcher.dispatch_start(event, matches[0])

    assert outcome.status is DispatchStatus.DISPATCHED
    assert len(fake.start_run_calls) == 1
    request = fake.start_run_calls[0]
    assert request.workspace_id == _WS
    assert request.workflow_version_id == "wfv-1"
    assert request.inputs == {"tier": "gold"}

    # The dedup key landed in the live ledger.
    assert await _dedup_count(pg_pool, _WS) == 1

    # A redelivered fire collapses to DUPLICATE against the live dedup ledger.
    replay = await dispatcher.dispatch_start(event, matches[0])
    assert replay.status is DispatchStatus.DUPLICATE
    assert len(fake.start_run_calls) == 1
    assert await _dedup_count(pg_pool, _WS) == 1


async def test_manual_fire_selector_miss_does_not_dispatch(
    metadata_store: TriggerMetadataStore, pg_pool: Any
) -> None:
    store = SubscriptionStore(metadata_store, now=lambda: _NOW)
    subscription = Subscription(
        workspace_id=_WS,
        subscription_id="sub_manual_2",
        kind=SubscriptionKind.START,
        source_type=SourceType.MANUAL,
        workflow_id="wf-2",
        target_workflow_version_id="wfv-2",
        selector='event.data.tier == "gold"',
        input_mapping={},
        state=SubscriptionState.ACTIVE,
        created_at=_NOW,
        updated_at=_NOW,
    )
    await store.create(subscription)
    rehydrated = await store.get(_WS, "sub_manual_2")
    assert rehydrated is not None

    evaluator = SelectorEvaluator()
    event = normalize_manual_fire(
        occurred_at=_NOW.isoformat(),
        subscription_id="sub_manual_2",
        inputs={"tier": "silver"},
    )
    matches = StartMatcher(evaluator).match(event, [rehydrated])
    assert matches == []
    assert await _dedup_count(pg_pool, _WS) == 0


# ---------------------------------------------------------------------------
# Flow B — resume register -> internal event -> RaiseExternalEvent
# ---------------------------------------------------------------------------


async def test_resume_register_then_internal_event_raises_against_postgres(
    metadata_store: TriggerMetadataStore, pg_pool: Any
) -> None:
    resume_store = ResumeSubscriptionStore(metadata_store)
    registration = ResumeRegistration(
        run_id="run-9",
        step_id="step-3",
        event_key="workflow.completed",
    )
    await resume_store.register(
        registration,
        workspace_id=_WS,
        resume_id="res_abc",
        expires_at=_NOW + timedelta(hours=24),
    )

    # Read back the persisted resume token from the live row.
    stored = await resume_store.get(_WS, "res_abc")
    assert stored is not None
    persisted = stored.registration
    assert persisted.run_id == "run-9"
    assert persisted.step_id == "step-3"
    assert persisted.event_key == "workflow.completed"

    # An internal workflow-completion event carrying the run/step context.
    event = normalize_workflow_event(
        {
            "workflowVersionId": "wfv-9",
            "runId": "run-9",
            "stepId": "step-3",
            "workspace": _WS,
            "status": "succeeded",
            "occurredAt": _NOW.isoformat(),
            "outputs": {"ok": True},
        }
    )
    assert event.kind == "workflow.completed"

    evaluator = SelectorEvaluator()
    candidate = ResumeCandidate(resume_id="res_abc", registration=persisted)
    matches = ResumeMatcher(evaluator).match(event, [candidate])
    assert len(matches) == 1

    fake = FakeWorkflowServiceClient()
    dispatcher = Dispatcher(fake, Deduplicator(metadata_store))
    outcome = await dispatcher.dispatch_resume(event, matches[0], workspace_id=_WS)

    assert outcome.status is DispatchStatus.DISPATCHED
    assert len(fake.raise_event_calls) == 1
    run_id, step_id, request = fake.raise_event_calls[0]
    assert (run_id, step_id) == ("run-9", "step-3")
    assert request.workspace_id == _WS
    assert request.event_name == "workflow.completed"
    assert request.payload["outputs"] == {"ok": True}

    assert await _dedup_count(pg_pool, _WS) == 1

    # Redelivery of the same event collapses to DUPLICATE.
    replay = await dispatcher.dispatch_resume(event, matches[0], workspace_id=_WS)
    assert replay.status is DispatchStatus.DUPLICATE
    assert len(fake.raise_event_calls) == 1
    assert await _dedup_count(pg_pool, _WS) == 1


async def test_resume_triple_mismatch_does_not_dispatch(
    metadata_store: TriggerMetadataStore, pg_pool: Any
) -> None:
    resume_store = ResumeSubscriptionStore(metadata_store)
    registration = ResumeRegistration(
        run_id="run-9",
        step_id="step-3",
        event_key="workflow.completed",
    )
    await resume_store.register(
        registration,
        workspace_id=_WS,
        resume_id="res_def",
        expires_at=_NOW + timedelta(hours=24),
    )
    stored = await resume_store.get(_WS, "res_def")
    assert stored is not None
    persisted = stored.registration

    # Event for a different step — the triple cannot match.
    event = normalize_workflow_event(
        {
            "workflowVersionId": "wfv-9",
            "runId": "run-9",
            "stepId": "step-OTHER",
            "workspace": _WS,
            "status": "succeeded",
            "occurredAt": _NOW.isoformat(),
        }
    )
    candidate = ResumeCandidate(resume_id="res_def", registration=persisted)
    matches = ResumeMatcher(SelectorEvaluator()).match(event, [candidate])
    assert matches == []
    assert await _dedup_count(pg_pool, _WS) == 0

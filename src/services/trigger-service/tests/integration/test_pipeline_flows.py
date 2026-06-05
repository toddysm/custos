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

The Pg adapter exposes only the locked SPL *write* surface, so the read-back is
performed via raw ``asyncpg`` queries that rebuild the SPL dataclasses and feed
them through the production ``*_from_spl`` rehydrators — proving the JSON-blob
round-trip the Trigger Service rides on the locked v1 schema.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Any

import pytest
from custos_spl.interfaces.metadata_store import (
    ResumeSubscription as SplResumeSubscription,
)
from custos_spl.interfaces.metadata_store import (
    Subscription as SplSubscription,
)
from custos_spl.interfaces.metadata_store import (
    SubscriptionSelector as SplSubscriptionSelector,
)

from custos_trigger.clients.workflow import FakeWorkflowServiceClient
from custos_trigger.dedup import Deduplicator
from custos_trigger.models import (
    ResumeRegistration,
    SourceType,
    Subscription,
    SubscriptionKind,
    SubscriptionState,
    resume_registration_from_spl,
    subscription_from_spl,
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


def _frozen_jsonb(value: Any) -> MappingProxyType[str, Any]:
    """Freeze a JSONB column into a read-only mapping.

    ``asyncpg`` returns JSONB as a raw string unless a codec is registered (our
    loop-local pool registers none), but a future codec change could surface a
    decoded mapping instead. Tolerate both so the read-back stays robust.
    """
    decoded = json.loads(value) if isinstance(value, str) else value
    return MappingProxyType(dict(decoded))


# ---------------------------------------------------------------------------
# Raw-SQL read-back helpers (the Pg adapter has no trigger read surface)
# ---------------------------------------------------------------------------


async def _rehydrate_subscription(
    pool: Any, *, workspace_id: str, subscription_id: str
) -> Subscription:
    """Rebuild the domain :class:`Subscription` from its persisted SPL rows."""
    async with pool.acquire() as conn:
        base = await conn.fetchrow(
            "SELECT workspace_id, subscription_id, workflow_id, state, "
            "created_at, updated_at FROM custos_state.subscription "
            "WHERE workspace_id = $1 AND subscription_id = $2",
            workspace_id,
            subscription_id,
        )
        sel = await conn.fetchrow(
            "SELECT workspace_id, subscription_id, selector, added_at "
            "FROM custos_state.subscription_selector "
            "WHERE workspace_id = $1 AND subscription_id = $2 "
            "ORDER BY added_at DESC LIMIT 1",
            workspace_id,
            subscription_id,
        )
    assert base is not None, "subscription row not persisted"
    assert sel is not None, "selector row not persisted"
    spl_sub = SplSubscription(
        workspace_id=base["workspace_id"],
        subscription_id=base["subscription_id"],
        workflow_id=base["workflow_id"],
        state=base["state"],
        created_at=base["created_at"],
        updated_at=base["updated_at"],
    )
    spl_sel = SplSubscriptionSelector(
        workspace_id=sel["workspace_id"],
        subscription_id=sel["subscription_id"],
        selector=_frozen_jsonb(sel["selector"]),
        added_at=sel["added_at"],
    )
    return subscription_from_spl(spl_sub, spl_sel)


async def _rehydrate_resume(pool: Any, *, workspace_id: str, resume_id: str) -> ResumeRegistration:
    """Rebuild the domain :class:`ResumeRegistration` from its persisted SPL row."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT workspace_id, resume_id, run_id, step_id, expires_at, payload "
            "FROM custos_state.resume_subscription "
            "WHERE workspace_id = $1 AND resume_id = $2",
            workspace_id,
            resume_id,
        )
    assert row is not None, "resume row not persisted"
    spl_resume = SplResumeSubscription(
        workspace_id=row["workspace_id"],
        resume_id=row["resume_id"],
        run_id=row["run_id"],
        step_id=row["step_id"],
        expires_at=row["expires_at"],
        payload=_frozen_jsonb(row["payload"]),
    )
    return resume_registration_from_spl(spl_resume)


async def _dedup_count(pool: Any, workspace_id: str) -> int:
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
    rehydrated = await _rehydrate_subscription(
        pg_pool, workspace_id=_WS, subscription_id="sub_manual_1"
    )
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
    rehydrated = await _rehydrate_subscription(
        pg_pool, workspace_id=_WS, subscription_id="sub_manual_2"
    )

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
    persisted = await _rehydrate_resume(pg_pool, workspace_id=_WS, resume_id="res_abc")
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
    persisted = await _rehydrate_resume(pg_pool, workspace_id=_WS, resume_id="res_def")

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

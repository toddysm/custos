"""Classifier + Start/Resume matcher tests (TS-IMPL-012)."""

from __future__ import annotations

from datetime import UTC, datetime

from custos_trigger.events import EventSource, NormalizedEvent
from custos_trigger.models import (
    ResumeRegistration,
    SourceType,
    Subscription,
    SubscriptionKind,
    SubscriptionState,
)
from custos_trigger.pipeline import (
    Classification,
    ResumeCandidate,
    ResumeKey,
    ResumeMatcher,
    StartMatcher,
    classify,
    resume_key_from_event,
)
from custos_trigger.selector import SelectorEvaluator

_OCCURRED_AT = "2026-06-04T12:00:00Z"
_NOW = datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC)


def _event(
    *,
    kind: str = "workflow.completed",
    source_type: SourceType = SourceType.INTERNAL,
    data: dict[str, object] | None = None,
) -> NormalizedEvent:
    return NormalizedEvent(
        event_id="evt-1",
        source=EventSource(type=source_type, occurred_at=_OCCURRED_AT),
        kind=kind,
        data=data or {},
    )


def _start_sub(
    *,
    subscription_id: str = "sub-1",
    selector: str | None = None,
    kind: SubscriptionKind = SubscriptionKind.START,
    state: SubscriptionState = SubscriptionState.ACTIVE,
) -> Subscription:
    return Subscription(
        workspace_id="ws-1",
        subscription_id=subscription_id,
        kind=kind,
        source_type=SourceType.INTERNAL,
        workflow_id="wf-1",
        target_workflow_version_id="wfv-1",
        selector=selector,
        state=state,
        created_at=_NOW,
        updated_at=_NOW,
    )


# --- classify ----------------------------------------------------------------


def test_classify_internal_routes_to_both_arms() -> None:
    result = classify(_event(source_type=SourceType.INTERNAL))
    assert result == Classification(to_start=True, to_resume=True)


def test_classify_manual_fire_is_start_only() -> None:
    result = classify(_event(kind="manual.fire", source_type=SourceType.MANUAL))
    assert result == Classification(to_start=True, to_resume=False)


def test_classify_webhook_routes_to_both_arms() -> None:
    result = classify(_event(source_type=SourceType.WEBHOOK))
    assert result.to_start is True
    assert result.to_resume is True


# --- StartMatcher ------------------------------------------------------------


def test_start_matcher_no_selector_is_unconditional() -> None:
    matcher = StartMatcher(SelectorEvaluator())
    sub = _start_sub(selector=None)
    matches = matcher.match(_event(), [sub])
    assert [m.subscription for m in matches] == [sub]


def test_start_matcher_selector_gates_match() -> None:
    matcher = StartMatcher(SelectorEvaluator())
    hit = _start_sub(subscription_id="hit", selector='event.data.status == "succeeded"')
    miss = _start_sub(subscription_id="miss", selector='event.data.status == "failed"')
    event = _event(data={"status": "succeeded"})
    matches = matcher.match(event, [hit, miss])
    assert [m.subscription.subscription_id for m in matches] == ["hit"]


def test_start_matcher_skips_non_start_kind() -> None:
    matcher = StartMatcher(SelectorEvaluator())
    sub = _start_sub(kind=SubscriptionKind.RESUME)
    assert matcher.match(_event(), [sub]) == []


def test_start_matcher_skips_inactive() -> None:
    matcher = StartMatcher(SelectorEvaluator())
    paused = _start_sub(state=SubscriptionState.PAUSED)
    expired = _start_sub(state=SubscriptionState.EXPIRED)
    assert matcher.match(_event(), [paused, expired]) == []


def test_start_matcher_non_bool_selector_is_no_match() -> None:
    matcher = StartMatcher(SelectorEvaluator())
    sub = _start_sub(selector="event.kind")  # evaluates to a string, not a bool
    assert matcher.match(_event(), [sub]) == []


# --- resume_key_from_event ---------------------------------------------------


def test_resume_key_from_event_extracts_triple() -> None:
    event = _event(kind="pr.merged", data={"runId": "run-1", "stepId": "step-1"})
    assert resume_key_from_event(event) == ResumeKey(
        run_id="run-1", step_id="step-1", event_key="pr.merged"
    )


def test_resume_key_from_event_missing_run_is_none() -> None:
    assert resume_key_from_event(_event(data={"stepId": "step-1"})) is None


def test_resume_key_from_event_missing_step_is_none() -> None:
    assert resume_key_from_event(_event(data={"runId": "run-1"})) is None


def test_resume_key_from_event_non_string_run_is_none() -> None:
    assert resume_key_from_event(_event(data={"runId": 1, "stepId": "step-1"})) is None


def test_resume_key_from_event_empty_step_is_none() -> None:
    assert resume_key_from_event(_event(data={"runId": "run-1", "stepId": ""})) is None


# --- ResumeMatcher -----------------------------------------------------------


def _resume_candidate(
    *,
    resume_id: str = "res-1",
    run_id: str = "run-1",
    step_id: str = "step-1",
    event_key: str = "pr.merged",
    selector: str | None = None,
) -> ResumeCandidate:
    return ResumeCandidate(
        resume_id=resume_id,
        registration=ResumeRegistration(
            run_id=run_id, step_id=step_id, event_key=event_key, selector=selector
        ),
    )


def _resume_event(
    *,
    kind: str = "pr.merged",
    run_id: str = "run-1",
    step_id: str = "step-1",
    data: dict[str, object] | None = None,
) -> NormalizedEvent:
    payload: dict[str, object] = {"runId": run_id, "stepId": step_id}
    if data:
        payload.update(data)
    return _event(kind=kind, data=payload)


def test_resume_matcher_no_run_context_yields_nothing() -> None:
    matcher = ResumeMatcher(SelectorEvaluator())
    assert matcher.match(_event(data={}), [_resume_candidate()]) == []


def test_resume_matcher_exact_triple_match() -> None:
    matcher = ResumeMatcher(SelectorEvaluator())
    matches = matcher.match(_resume_event(), [_resume_candidate()])
    assert [m.resume_id for m in matches] == ["res-1"]


def test_resume_matcher_rejects_run_mismatch() -> None:
    matcher = ResumeMatcher(SelectorEvaluator())
    cand = _resume_candidate(run_id="other-run")
    assert matcher.match(_resume_event(), [cand]) == []


def test_resume_matcher_rejects_step_mismatch() -> None:
    matcher = ResumeMatcher(SelectorEvaluator())
    cand = _resume_candidate(step_id="other-step")
    assert matcher.match(_resume_event(), [cand]) == []


def test_resume_matcher_rejects_event_key_mismatch() -> None:
    matcher = ResumeMatcher(SelectorEvaluator())
    cand = _resume_candidate(event_key="pr.closed")
    assert matcher.match(_resume_event(), [cand]) == []


def test_resume_matcher_selector_narrows_match() -> None:
    matcher = ResumeMatcher(SelectorEvaluator())
    cand = _resume_candidate(selector='event.data.merged == "true"')
    hit = _resume_event(data={"merged": "true"})
    miss = _resume_event(data={"merged": "false"})
    assert [m.resume_id for m in matcher.match(hit, [cand])] == ["res-1"]
    assert matcher.match(miss, [cand]) == []


def test_resume_matcher_non_bool_selector_is_no_match() -> None:
    matcher = ResumeMatcher(SelectorEvaluator())
    cand = _resume_candidate(selector="event.kind")
    assert matcher.match(_resume_event(), [cand]) == []


def test_event_can_match_both_arms() -> None:
    # A workflow.completed event both starts a chained workflow and resumes a
    # parent run waiting on its child.
    evaluator = SelectorEvaluator()
    start_matcher = StartMatcher(evaluator)
    resume_matcher = ResumeMatcher(evaluator)
    event = _resume_event(kind="workflow.completed", run_id="run-1", step_id="step-1")

    routing = classify(event)
    assert routing.to_start and routing.to_resume

    start_sub = _start_sub(selector='event.kind == "workflow.completed"')
    resume_cand = _resume_candidate(event_key="workflow.completed")

    assert len(start_matcher.match(event, [start_sub])) == 1
    assert len(resume_matcher.match(event, [resume_cand])) == 1

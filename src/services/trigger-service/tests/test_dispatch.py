"""Dispatcher tests (TS-IMPL-014)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest

from custos_trigger.clients.workflow import (
    RaiseExternalEventRequest,
    RunRef,
    StartRunRequest,
    WorkflowClientStatusError,
)
from custos_trigger.dedup import Deduplicator, compute_dedup_key
from custos_trigger.errors import TriggerError, TriggerErrorKind
from custos_trigger.events import EventSource, NormalizedEvent
from custos_trigger.models import (
    ResumeRegistration,
    SourceType,
    Subscription,
    SubscriptionKind,
    SubscriptionState,
)
from custos_trigger.pipeline.dispatch import (
    AUDIT_DISPATCH_FAILED,
    AUDIT_DISPATCHED,
    AUDIT_LOOP_DETECTED,
    AUDIT_MATCHED,
    AUDIT_RESUME_DELIVERED,
    Dispatcher,
    DispatchStatus,
    NoopAuditSink,
)
from custos_trigger.pipeline.match_resume import ResumeMatch
from custos_trigger.pipeline.match_start import StartMatch
from custos_trigger.providers import InMemoryTriggerMetadataStore

pytestmark = pytest.mark.asyncio

_OCCURRED_AT = "2026-06-04T12:00:00Z"
_NOW = datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC)


def _event(*, event_id: str = "evt-1", data: dict[str, Any] | None = None) -> NormalizedEvent:
    return NormalizedEvent(
        event_id=event_id,
        source=EventSource(type=SourceType.INTERNAL, occurred_at=_OCCURRED_AT),
        kind="workflow.completed",
        data=data or {},
    )


def _start_match(
    *,
    subscription_id: str = "sub-1",
    target_version: str | None = "wfv-1",
    input_mapping: dict[str, Any] | None = None,
) -> StartMatch:
    sub = Subscription(
        workspace_id="ws-1",
        subscription_id=subscription_id,
        kind=SubscriptionKind.START,
        source_type=SourceType.INTERNAL,
        workflow_id="wf-1",
        target_workflow_version_id=target_version,
        input_mapping=input_mapping or {},
        state=SubscriptionState.ACTIVE,
        created_at=_NOW,
        updated_at=_NOW,
    )
    return StartMatch(subscription=sub)


def _resume_match(*, resume_id: str = "res-1") -> ResumeMatch:
    reg = ResumeRegistration(run_id="run-9", step_id="step-3", event_key="workflow.completed")
    return ResumeMatch(resume_id=resume_id, registration=reg)


@dataclass(slots=True)
class _RecordingAudit:
    events: list[tuple[str, str, dict[str, Any]]] = field(default_factory=list)

    async def emit(
        self, event_name: str, *, workspace_id: str, attributes: Mapping[str, Any]
    ) -> None:
        self.events.append((event_name, workspace_id, dict(attributes)))

    def names(self) -> list[str]:
        return [name for name, _ws, _attrs in self.events]


@dataclass(slots=True)
class _FlakyClient:
    fail_times: int = 0
    error: Exception = field(
        default_factory=lambda: WorkflowClientStatusError("boom", status_code=503)
    )
    run_ref: RunRef = field(
        default_factory=lambda: RunRef(
            run_id="run-1", status="queued", workspace_id="ws-1", workflow_version_id="wfv-1"
        )
    )
    start_calls: list[StartRunRequest] = field(default_factory=list)
    raise_calls: list[tuple[str, str, RaiseExternalEventRequest]] = field(default_factory=list)
    _start_attempts: int = 0
    _raise_attempts: int = 0

    async def start_run(self, request: StartRunRequest) -> RunRef:
        self.start_calls.append(request)
        if self._start_attempts < self.fail_times:
            self._start_attempts += 1
            raise self.error
        return self.run_ref

    async def raise_external_event(
        self, run_id: str, step_id: str, request: RaiseExternalEventRequest
    ) -> None:
        self.raise_calls.append((run_id, step_id, request))
        if self._raise_attempts < self.fail_times:
            self._raise_attempts += 1
            raise self.error
        return None


@dataclass(slots=True)
class _SleepRecorder:
    delays: list[float] = field(default_factory=list)

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


@pytest.fixture
def store() -> InMemoryTriggerMetadataStore:
    return InMemoryTriggerMetadataStore(now=lambda: _NOW)


@pytest.fixture
def dedup(store: InMemoryTriggerMetadataStore) -> Deduplicator:
    return Deduplicator(store)


def _dispatcher(
    client: object,
    dedup: Deduplicator,
    *,
    audit: _RecordingAudit | None = None,
    sleep: _SleepRecorder | None = None,
    max_retries: int = 3,
    max_fanout_depth: int = 2,
) -> Dispatcher:
    return Dispatcher(
        client,  # type: ignore[arg-type]
        dedup,
        max_retries=max_retries,
        max_fanout_depth=max_fanout_depth,
        audit=audit,
        sleep=sleep if sleep is not None else _SleepRecorder(),
    )


# --- start dispatch ----------------------------------------------------------


async def test_dispatch_start_success(dedup: Deduplicator) -> None:
    client = _FlakyClient()
    audit = _RecordingAudit()
    dispatcher = _dispatcher(client, dedup, audit=audit)

    outcome = await dispatcher.dispatch_start(_event(), _start_match(input_mapping={"a": 1}))

    assert outcome.status is DispatchStatus.DISPATCHED
    assert outcome.is_dispatched is True
    assert outcome.is_duplicate is False
    assert outcome.is_dead_lettered is False
    assert outcome.is_loop_rejected is False
    assert outcome.run_ref is client.run_ref
    assert len(client.start_calls) == 1
    request = client.start_calls[0]
    assert request.workspace_id == "ws-1"
    assert request.workflow_version_id == "wfv-1"
    assert request.inputs == {"a": 1}
    assert request.idempotency_key == compute_dedup_key("sub-1", "evt-1")
    assert audit.names() == [AUDIT_MATCHED, AUDIT_DISPATCHED]


async def test_dispatch_start_duplicate_skips_second_call(dedup: Deduplicator) -> None:
    client = _FlakyClient()
    dispatcher = _dispatcher(client, dedup)

    first = await dispatcher.dispatch_start(_event(), _start_match())
    second = await dispatcher.dispatch_start(_event(), _start_match())

    assert first.status is DispatchStatus.DISPATCHED
    assert second.status is DispatchStatus.DUPLICATE
    assert second.is_duplicate is True
    assert len(client.start_calls) == 1


async def test_dispatch_start_missing_version_dead_letters(dedup: Deduplicator) -> None:
    client = _FlakyClient()
    audit = _RecordingAudit()
    dispatcher = _dispatcher(client, dedup, audit=audit)

    outcome = await dispatcher.dispatch_start(_event(), _start_match(target_version=None))

    assert outcome.status is DispatchStatus.DEAD_LETTERED
    assert outcome.is_dead_lettered is True
    assert isinstance(outcome.error, TriggerError)
    assert outcome.error.kind is TriggerErrorKind.DISPATCH_FAILED
    assert client.start_calls == []
    assert audit.names() == [AUDIT_MATCHED, AUDIT_DISPATCH_FAILED]


async def test_dispatch_start_retries_then_succeeds(dedup: Deduplicator) -> None:
    client = _FlakyClient(fail_times=2)
    sleep = _SleepRecorder()
    dispatcher = _dispatcher(client, dedup, sleep=sleep)

    outcome = await dispatcher.dispatch_start(_event(), _start_match())

    assert outcome.status is DispatchStatus.DISPATCHED
    assert len(client.start_calls) == 3
    # Exponential backoff: base * 2**attempt for attempts 0 and 1.
    assert sleep.delays == [0.5, 1.0]


async def test_dispatch_start_exhausts_retries_then_dead_letters(dedup: Deduplicator) -> None:
    client = _FlakyClient(fail_times=99)
    audit = _RecordingAudit()
    sleep = _SleepRecorder()
    dispatcher = _dispatcher(client, dedup, audit=audit, sleep=sleep, max_retries=3)

    outcome = await dispatcher.dispatch_start(_event(), _start_match())

    assert outcome.status is DispatchStatus.DEAD_LETTERED
    assert isinstance(outcome.error, WorkflowClientStatusError)
    # 1 initial attempt + 3 retries = 4 calls; 3 backoff sleeps.
    assert len(client.start_calls) == 4
    assert len(sleep.delays) == 3
    assert audit.names() == [AUDIT_MATCHED, AUDIT_DISPATCH_FAILED]


async def test_dispatch_start_non_retryable_dead_letters_without_retry(
    dedup: Deduplicator,
) -> None:
    client = _FlakyClient(fail_times=99, error=WorkflowClientStatusError("bad", status_code=400))
    sleep = _SleepRecorder()
    dispatcher = _dispatcher(client, dedup, sleep=sleep)

    outcome = await dispatcher.dispatch_start(_event(), _start_match())

    assert outcome.status is DispatchStatus.DEAD_LETTERED
    assert len(client.start_calls) == 1
    assert sleep.delays == []


async def test_dispatch_start_dead_letter_rolls_back_dedup(dedup: Deduplicator) -> None:
    failing = _FlakyClient(fail_times=99)
    dispatcher = _dispatcher(failing, dedup)
    failed = await dispatcher.dispatch_start(_event(), _start_match())
    assert failed.status is DispatchStatus.DEAD_LETTERED

    # The dedup key was rolled back, so a fresh attempt is not a duplicate.
    healthy = _FlakyClient()
    dispatcher2 = _dispatcher(healthy, dedup)
    retried = await dispatcher2.dispatch_start(_event(), _start_match())
    assert retried.status is DispatchStatus.DISPATCHED


async def test_dispatch_start_loop_rejected_above_limit(dedup: Deduplicator) -> None:
    client = _FlakyClient()
    audit = _RecordingAudit()
    dispatcher = _dispatcher(client, dedup, audit=audit, max_fanout_depth=2)

    outcome = await dispatcher.dispatch_start(_event(), _start_match(), depth=3)

    assert outcome.status is DispatchStatus.LOOP_REJECTED
    assert outcome.is_loop_rejected is True
    assert client.start_calls == []
    assert audit.names() == [AUDIT_MATCHED, AUDIT_LOOP_DETECTED]
    assert audit.events[-1][2]["depth"] == 3
    assert audit.events[-1][2]["limit"] == 2


async def test_dispatch_start_at_depth_limit_still_dispatches(dedup: Deduplicator) -> None:
    client = _FlakyClient()
    dispatcher = _dispatcher(client, dedup, max_fanout_depth=2)

    outcome = await dispatcher.dispatch_start(_event(), _start_match(), depth=2)

    assert outcome.status is DispatchStatus.DISPATCHED
    assert len(client.start_calls) == 1


# --- resume dispatch ---------------------------------------------------------


async def test_dispatch_resume_success(dedup: Deduplicator) -> None:
    client = _FlakyClient()
    audit = _RecordingAudit()
    dispatcher = _dispatcher(client, dedup, audit=audit)

    outcome = await dispatcher.dispatch_resume(
        _event(data={"x": "y"}), _resume_match(), workspace_id="ws-1"
    )

    assert outcome.status is DispatchStatus.DISPATCHED
    assert outcome.run_ref is None
    assert len(client.raise_calls) == 1
    run_id, step_id, request = client.raise_calls[0]
    assert (run_id, step_id) == ("run-9", "step-3")
    assert request.workspace_id == "ws-1"
    assert request.event_name == "workflow.completed"
    assert request.payload == {"x": "y"}
    assert request.idempotency_key == compute_dedup_key("res-1", "evt-1")
    assert audit.names() == [AUDIT_MATCHED, AUDIT_RESUME_DELIVERED]


async def test_dispatch_resume_dead_letters_on_persistent_failure(dedup: Deduplicator) -> None:
    client = _FlakyClient(fail_times=99)
    audit = _RecordingAudit()
    dispatcher = _dispatcher(client, dedup, audit=audit, max_retries=1)

    outcome = await dispatcher.dispatch_resume(_event(), _resume_match(), workspace_id="ws-1")

    assert outcome.status is DispatchStatus.DEAD_LETTERED
    assert len(client.raise_calls) == 2  # 1 initial + 1 retry
    assert audit.names() == [AUDIT_MATCHED, AUDIT_DISPATCH_FAILED]


async def test_dispatch_resume_duplicate_skips_second_call(dedup: Deduplicator) -> None:
    client = _FlakyClient()
    dispatcher = _dispatcher(client, dedup)

    first = await dispatcher.dispatch_resume(_event(), _resume_match(), workspace_id="ws-1")
    second = await dispatcher.dispatch_resume(_event(), _resume_match(), workspace_id="ws-1")

    assert first.status is DispatchStatus.DISPATCHED
    assert second.status is DispatchStatus.DUPLICATE
    assert len(client.raise_calls) == 1


# --- defaults ----------------------------------------------------------------


async def test_default_audit_sink_is_noop(dedup: Deduplicator) -> None:
    dispatcher = Dispatcher(_FlakyClient(), dedup)
    outcome = await dispatcher.dispatch_start(_event(), _start_match())
    assert outcome.status is DispatchStatus.DISPATCHED


async def test_noop_audit_sink_emit_returns_none() -> None:
    sink = NoopAuditSink()
    await sink.emit("x", workspace_id="ws-1", attributes={})

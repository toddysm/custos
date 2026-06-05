"""Dispatcher — turns matches into Workflow Service RPCs (TS-IMPL-014).

The dispatcher is the tail of the linear pipeline (``Classify -> Match -> Dedup
-> Dispatch``). It takes a :class:`~custos_trigger.pipeline.match_start.StartMatch`
or :class:`~custos_trigger.pipeline.match_resume.ResumeMatch` produced for a
:class:`~custos_trigger.events.NormalizedEvent` and drives the corresponding
Workflow Service Internal RPC through the
:class:`~custos_trigger.clients.workflow.WorkflowServiceClient`:

* a **start** match -> ``StartRun(workflowVersionId, inputs)``;
* a **resume** match -> ``RaiseExternalEvent(runId, stepId, eventName, payload)``.

Three cross-cutting guarantees from design ``§ Failure Modes`` are enforced here:

1. **Retry then dead-letter.** Transient failures (transport blips,
   ``408``/``429``/``5xx``) are retried with exponential backoff up to
   ``TRIGGER_DISPATCH_MAX_RETRIES``; a permanent failure (a non-retryable
   ``4xx`` / decode error / misconfiguration) or an exhausted retry budget is
   dead-lettered with a ``trigger.dispatch.failed`` audit event.
2. **Dedup commits only after a confirmed dispatch.** The dispatch runs inside
   :meth:`~custos_trigger.dedup.Deduplicator.guard`, so the dedup key is rolled
   back when the dispatch ultimately fails — the redelivery can re-attempt
   instead of being suppressed as a false duplicate (failure-mode row 1).
3. **Fan-out loop guard.** Each dispatch carries the inbound event chain
   ``depth``; when it exceeds the per-tenant ``TRIGGER_FANOUT_MAX_DEPTH`` limit
   the dispatch is rejected and ``trigger.loop.detected`` is audited, breaking
   internal-event loops (workflow A starts B starts A).

The hard idempotency guarantee remains the Workflow Service ``idempotencyKey``
(design ``§ Internal RPCs``): every RPC carries the deterministic dedup key as
its ``idempotencyKey`` so a retry that actually reached the Workflow Service can
never start a second run.

``inputMapping`` placeholder resolution (the ``${{ ... }}`` event-root mapping)
is performed by the receiver before it hands the subscription to the dispatcher;
the dispatcher forwards the subscription's already-resolved ``input_mapping`` as
the start inputs.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from custos_trigger.clients.workflow import (
    RaiseExternalEventRequest,
    RunRef,
    StartRunRequest,
    WorkflowClientError,
    WorkflowServiceClient,
)
from custos_trigger.dedup import Deduplicator, compute_dedup_key
from custos_trigger.errors import TriggerError, TriggerErrorKind
from custos_trigger.events import NormalizedEvent
from custos_trigger.pipeline.match_resume import ResumeMatch
from custos_trigger.pipeline.match_start import StartMatch
from custos_trigger.settings import DEFAULT_DISPATCH_MAX_RETRIES, DEFAULT_FANOUT_MAX_DEPTH

__all__ = [
    "AUDIT_DISPATCHED",
    "AUDIT_DISPATCH_FAILED",
    "AUDIT_LOOP_DETECTED",
    "AUDIT_RESUME_DELIVERED",
    "DEFAULT_BACKOFF_BASE_SECONDS",
    "AuditSink",
    "DispatchOutcome",
    "DispatchStatus",
    "Dispatcher",
    "NoopAuditSink",
]

#: Audit event names (design ``§ Dependencies`` / ``§ Failure Modes``).
AUDIT_DISPATCHED: str = "trigger.dispatched"
AUDIT_RESUME_DELIVERED: str = "resume.delivered"
AUDIT_DISPATCH_FAILED: str = "trigger.dispatch.failed"
AUDIT_LOOP_DETECTED: str = "trigger.loop.detected"

#: Base of the exponential backoff (seconds): delay = base * 2**attempt.
DEFAULT_BACKOFF_BASE_SECONDS: float = 0.5


@runtime_checkable
class AuditSink(Protocol):
    """The audit surface the dispatcher emits to.

    The real OTel/audit pipeline lands in TS-IMPL-019; until then the app wires
    :class:`NoopAuditSink`. ``attributes`` is a JSON-safe mapping of event
    context (subscription / run / step ids, depth, error reason).
    """

    async def emit(
        self, event_name: str, *, workspace_id: str, attributes: Mapping[str, Any]
    ) -> None: ...


@dataclass(slots=True)
class NoopAuditSink:
    """An audit sink that drops every event (default until TS-IMPL-019)."""

    async def emit(
        self, event_name: str, *, workspace_id: str, attributes: Mapping[str, Any]
    ) -> None:
        return None


class DispatchStatus(StrEnum):
    """Terminal outcome of a single dispatch."""

    #: The RPC succeeded (a run was started or a step resumed).
    DISPATCHED = "dispatched"
    #: The event was a replay; no RPC was issued.
    DUPLICATE = "duplicate"
    #: Retries were exhausted (or the failure was permanent); dead-lettered.
    DEAD_LETTERED = "dead_lettered"
    #: The fan-out depth limit was exceeded; the dispatch was rejected.
    LOOP_REJECTED = "loop_rejected"


@dataclass(frozen=True, slots=True)
class DispatchOutcome:
    """The result of dispatching one match.

    ``run_ref`` is populated only on a successful **start** dispatch; ``error``
    carries the failure that caused a dead-letter.
    """

    status: DispatchStatus
    run_ref: RunRef | None = None
    error: Exception | None = None

    @property
    def is_dispatched(self) -> bool:
        return self.status is DispatchStatus.DISPATCHED

    @property
    def is_duplicate(self) -> bool:
        return self.status is DispatchStatus.DUPLICATE

    @property
    def is_dead_lettered(self) -> bool:
        return self.status is DispatchStatus.DEAD_LETTERED

    @property
    def is_loop_rejected(self) -> bool:
        return self.status is DispatchStatus.LOOP_REJECTED


class Dispatcher:
    """Drives matches to Workflow Service RPCs with retry, dedup, and loop guard."""

    def __init__(
        self,
        client: WorkflowServiceClient,
        deduplicator: Deduplicator,
        *,
        max_retries: int = DEFAULT_DISPATCH_MAX_RETRIES,
        max_fanout_depth: int = DEFAULT_FANOUT_MAX_DEPTH,
        audit: AuditSink | None = None,
        backoff_base_seconds: float = DEFAULT_BACKOFF_BASE_SECONDS,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._client = client
        self._dedup = deduplicator
        self._max_retries = max_retries
        self._max_fanout_depth = max_fanout_depth
        self._audit: AuditSink = audit if audit is not None else NoopAuditSink()
        self._backoff_base_seconds = backoff_base_seconds
        self._sleep = sleep

    async def dispatch_start(
        self, event: NormalizedEvent, match: StartMatch, *, depth: int = 0
    ) -> DispatchOutcome:
        """Dispatch a start match as a Workflow Service ``StartRun``."""
        sub = match.subscription
        version = sub.target_workflow_version_id
        if not version:
            # A start subscription with no resolved target version cannot start
            # a run: a permanent misconfiguration, dead-lettered without ever
            # touching the dedup window.
            error = TriggerError(
                TriggerErrorKind.DISPATCH_FAILED,
                "start subscription has no target workflow version",
                details={"subscriptionId": sub.subscription_id},
            )
            await self._audit.emit(
                AUDIT_DISPATCH_FAILED,
                workspace_id=sub.workspace_id,
                attributes={
                    "subscriptionId": sub.subscription_id,
                    "eventId": event.event_id,
                    "reason": error.message,
                },
            )
            return DispatchOutcome(status=DispatchStatus.DEAD_LETTERED, error=error)

        idempotency_key = compute_dedup_key(sub.subscription_id, event.event_id)
        request = StartRunRequest(
            workspace_id=sub.workspace_id,
            workflow_version_id=version,
            inputs=dict(sub.input_mapping),
            idempotency_key=idempotency_key,
        )

        async def _call() -> RunRef | None:
            return await self._client.start_run(request)

        return await self._dispatch(
            workspace_id=sub.workspace_id,
            dedup_subscription_id=sub.subscription_id,
            event_id=event.event_id,
            depth=depth,
            success_event=AUDIT_DISPATCHED,
            audit_attributes={
                "subscriptionId": sub.subscription_id,
                "workflowVersionId": version,
                "eventId": event.event_id,
            },
            call=_call,
        )

    async def dispatch_resume(
        self,
        event: NormalizedEvent,
        match: ResumeMatch,
        *,
        workspace_id: str,
        depth: int = 0,
    ) -> DispatchOutcome:
        """Dispatch a resume match as a Workflow Service ``RaiseExternalEvent``.

        ``workspace_id`` is supplied by the caller: the resume registration
        references a Workflow Service-owned run/step by opaque id and carries no
        workspace of its own, so the tenant context comes from the receiver.
        """
        reg = match.registration
        idempotency_key = compute_dedup_key(match.resume_id, event.event_id)
        request = RaiseExternalEventRequest(
            workspace_id=workspace_id,
            event_name=reg.event_key,
            payload=dict(event.data),
            idempotency_key=idempotency_key,
        )

        async def _call() -> RunRef | None:
            await self._client.raise_external_event(reg.run_id, reg.step_id, request)
            return None

        return await self._dispatch(
            workspace_id=workspace_id,
            dedup_subscription_id=match.resume_id,
            event_id=event.event_id,
            depth=depth,
            success_event=AUDIT_RESUME_DELIVERED,
            audit_attributes={
                "resumeId": match.resume_id,
                "runId": reg.run_id,
                "stepId": reg.step_id,
                "eventKey": reg.event_key,
                "eventId": event.event_id,
            },
            call=_call,
        )

    async def _dispatch(
        self,
        *,
        workspace_id: str,
        dedup_subscription_id: str,
        event_id: str,
        depth: int,
        success_event: str,
        audit_attributes: dict[str, Any],
        call: Callable[[], Awaitable[RunRef | None]],
    ) -> DispatchOutcome:
        if depth > self._max_fanout_depth:
            await self._audit.emit(
                AUDIT_LOOP_DETECTED,
                workspace_id=workspace_id,
                attributes={**audit_attributes, "depth": depth, "limit": self._max_fanout_depth},
            )
            return DispatchOutcome(status=DispatchStatus.LOOP_REJECTED)

        run_ref: RunRef | None = None
        duplicate = False
        try:
            async with self._dedup.guard(
                workspace_id=workspace_id,
                subscription_id=dedup_subscription_id,
                event_id=event_id,
            ) as reservation:
                if reservation.is_duplicate:
                    duplicate = True
                else:
                    run_ref = await self._call_with_retry(call)
        except WorkflowClientError as exc:
            # The dedup guard rolled the reservation back, so the redelivery can
            # re-attempt; emit the dead-letter audit and surface the failure.
            await self._audit.emit(
                AUDIT_DISPATCH_FAILED,
                workspace_id=workspace_id,
                attributes={**audit_attributes, "reason": str(exc), "retryable": exc.retryable},
            )
            return DispatchOutcome(status=DispatchStatus.DEAD_LETTERED, error=exc)

        if duplicate:
            return DispatchOutcome(status=DispatchStatus.DUPLICATE)

        await self._audit.emit(
            success_event, workspace_id=workspace_id, attributes=audit_attributes
        )
        return DispatchOutcome(status=DispatchStatus.DISPATCHED, run_ref=run_ref)

    async def _call_with_retry(self, call: Callable[[], Awaitable[RunRef | None]]) -> RunRef | None:
        """Invoke ``call``, retrying transient failures with exponential backoff.

        A non-retryable failure raises immediately; a retryable one is retried
        up to ``max_retries`` times (so ``max_retries + 1`` total attempts)
        before being re-raised for the caller to dead-letter.
        """
        attempt = 0
        while True:
            try:
                return await call()
            except WorkflowClientError as exc:
                if not exc.retryable or attempt >= self._max_retries:
                    raise
                await self._sleep(self._backoff_base_seconds * (2**attempt))
                attempt += 1

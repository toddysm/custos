"""``step.*`` lifecycle event emission (WF-IMPL-056).

The Step Coordinator dispatcher (WF-IMPL-055) is the routing
surface for execution; this module is the **publishing** surface
that wraps the dispatcher's per-step lifecycle into the
``custos.workflow.events`` topic the Trigger Service Internal
Event Receiver subscribes to.

Locked event taxonomy
=====================

The full ``step.*`` taxonomy is pinned by
:data:`LOCKED_STEP_EVENT_KINDS` (a :class:`frozenset`). Six kinds
ship in v1:

+--------------------------------+---------------------------------------------+
| Kind                           | Producer trigger                            |
+================================+=============================================+
| ``step.started``               | Dispatcher entry into a node (one per       |
|                                | attempt).                                   |
+--------------------------------+---------------------------------------------+
| ``step.completed``             | Handler returned :class:`StepSucceeded`.    |
+--------------------------------+---------------------------------------------+
| ``step.failed``                | Handler returned :class:`StepFailed` after  |
|                                | the retry policy exhausted (or the route    |
|                                | resolved to ``do: fail``).                  |
+--------------------------------+---------------------------------------------+
| ``step.skipped``               | Handler returned :class:`StepSkipped` —     |
|                                | gate excluded the step, or the on-error    |
|                                | route resolved to ``do: skip``.             |
+--------------------------------+---------------------------------------------+
| ``step.waiting``               | Step suspended on a durable signal (timer   |
|                                | / external event) — the Run Controller's    |
|                                | resume subscription manager owns this trace.|
+--------------------------------+---------------------------------------------+
| ``step.retry_scheduled``       | Retry driver returned :class:`RetryNow` —   |
|                                | the convenience wrapper                     |
|                                | :func:`emit_step_retry_scheduled` delegates |
|                                | to                                          |
|                                | :func:`retry_driver.build_retry_scheduled_event` |
|                                | for envelope construction.                  |
+--------------------------------+---------------------------------------------+

Adding a new ``step.*`` kind requires extending both the
:data:`LOCKED_STEP_EVENT_KINDS` set AND the
:class:`StepLifecyclePublisher` Protocol with a typed emit method.
The two are pinned by the module-level ``assert`` below so a
mismatch fails at import time.

Single HTTP path
================

:class:`LifecycleEventPublisherAdapter` *adapts* — it does not
implement — the wire transport. Its inner
:class:`LifecycleEventPublisher` is the same surface the Run
Controller already drives for ``workflow.*`` events, so every
``custos.workflow.events`` publication funnels through one HTTP
client and one Dapr Pub/Sub endpoint. Operators tuning the topic
(rate limits, dead-letter, retry) configure it in one place.

Producer-side dedup
===================

Dapr Workflow's at-least-once activity-execution semantics mean a
single step can drive the same emit boundary multiple times on
replay. The adapter maintains an in-memory LRU keyed on
``(run_id, step_id, attempt, kind)`` (note the four-tuple — the
existing :class:`DedupingLifecyclePublisher` keys on
``(run_id, kind, occurred_at)`` which is too coarse for step
events because the same ``kind`` legitimately fires for every
step in a graph). On a cache hit the emit method returns without
forwarding; on a miss the key is **reserved before the awaited
inner publish** so two concurrent emits for the same key cannot
both observe the key as absent. If the inner publish raises, the
reservation is dropped so a retry forwards the event.

Envelope schema
===============

The adapter builds a :class:`LifecycleEvent` whose
:attr:`extra` carries ``step_id`` + ``attempt`` + kind-specific
payload (``outputs`` / ``error`` / ``retry`` / ``reason`` /
``wait_token``). :meth:`LifecycleEvent.to_wire` was extended by
this task to surface those fields as first-class wire keys
(``stepId`` / ``attempt`` / ``error`` / ``retry`` / ``reason`` /
``waitToken``). Subscribers therefore see one envelope shape
regardless of whether the producer was the Run Controller (for
``workflow.*``) or this adapter (for ``step.*``).
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any, Final, Protocol, runtime_checkable

from custos_workflow.runs.controller import (
    LifecycleEvent,
    LifecycleEventPublisher,
)
from custos_workflow.steps.retry_driver import (
    LIFECYCLE_KIND_STEP_RETRY_SCHEDULED,
    RetryNow,
    build_retry_scheduled_event,
)

if TYPE_CHECKING:
    from custos_workflow.runs.ids import RunId

__all__ = [
    "DEFAULT_STEP_DEDUP_CACHE_SIZE",
    "LIFECYCLE_KIND_STEP_COMPLETED",
    "LIFECYCLE_KIND_STEP_FAILED",
    "LIFECYCLE_KIND_STEP_RETRY_SCHEDULED",
    "LIFECYCLE_KIND_STEP_SKIPPED",
    "LIFECYCLE_KIND_STEP_STARTED",
    "LIFECYCLE_KIND_STEP_WAITING",
    "LOCKED_STEP_EVENT_KINDS",
    "LifecycleEventPublisherAdapter",
    "StepLifecyclePublisher",
]


# ---------------------------------------------------------------------------
# Locked taxonomy
# ---------------------------------------------------------------------------


#: ``step.*`` event kind constants (wire-stable strings).
LIFECYCLE_KIND_STEP_STARTED: Final[str] = "step.started"
LIFECYCLE_KIND_STEP_COMPLETED: Final[str] = "step.completed"
LIFECYCLE_KIND_STEP_FAILED: Final[str] = "step.failed"
LIFECYCLE_KIND_STEP_SKIPPED: Final[str] = "step.skipped"
LIFECYCLE_KIND_STEP_WAITING: Final[str] = "step.waiting"
# ``step.retry_scheduled`` already exists in :mod:`steps.retry_driver`
# (WF-IMPL-053). Re-exported from this module so a future caller
# looking for the full taxonomy only needs one import.


#: Every ``step.*`` event kind the v1 platform publishes. Adding
#: a new kind requires extending this set AND
#: :class:`StepLifecyclePublisher` with a typed emit method —
#: the assertion below pins the two surfaces together.
LOCKED_STEP_EVENT_KINDS: Final[frozenset[str]] = frozenset(
    {
        LIFECYCLE_KIND_STEP_STARTED,
        LIFECYCLE_KIND_STEP_COMPLETED,
        LIFECYCLE_KIND_STEP_FAILED,
        LIFECYCLE_KIND_STEP_SKIPPED,
        LIFECYCLE_KIND_STEP_WAITING,
        LIFECYCLE_KIND_STEP_RETRY_SCHEDULED,
    }
)


#: Default maximum number of ``(run_id, step_id, attempt, kind)``
#: keys :class:`LifecycleEventPublisherAdapter` will remember.
#: Sized so a replay storm of one run cannot push out unrelated
#: runs' dedup state (mirrors
#: :data:`custos_workflow.runs.events.DEFAULT_DEDUP_CACHE_SIZE`).
DEFAULT_STEP_DEDUP_CACHE_SIZE: Final[int] = 10_000


#: Maps each locked ``step.*`` kind to the
#: :class:`StepLifecyclePublisher` method that produces it.
#: Pinned here as the single source of truth for the
#: kind ↔ emit-method correspondence so the module-level
#: :keyword:`assert` below catches a half-finished extension
#: (new kind added but no emit method, or vice versa) at import
#: time. Mirrors WF-IMPL-035's ``_STEP_RESULT_VARIANTS`` /
#: WF-IMPL-055's ``_EXPECTED_PRIMITIVE_HANDLERS`` pattern.
_EMIT_METHOD_FOR_KIND: Final[Mapping[str, str]] = {
    LIFECYCLE_KIND_STEP_STARTED: "emit_step_started",
    LIFECYCLE_KIND_STEP_COMPLETED: "emit_step_completed",
    LIFECYCLE_KIND_STEP_FAILED: "emit_step_failed",
    LIFECYCLE_KIND_STEP_SKIPPED: "emit_step_skipped",
    LIFECYCLE_KIND_STEP_WAITING: "emit_step_waiting",
    LIFECYCLE_KIND_STEP_RETRY_SCHEDULED: "emit_step_retry_scheduled",
}
assert frozenset(_EMIT_METHOD_FOR_KIND) == LOCKED_STEP_EVENT_KINDS, (
    "LOCKED_STEP_EVENT_KINDS and _EMIT_METHOD_FOR_KIND drifted — extend both "
    "AND add a typed emit method to StepLifecyclePublisher + "
    "LifecycleEventPublisherAdapter."
)


# ---------------------------------------------------------------------------
# StepLifecyclePublisher Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class StepLifecyclePublisher(Protocol):
    """Typed sink for ``step.*`` lifecycle events.

    Every concrete implementation forwards through an underlying
    :class:`LifecycleEventPublisher` so the wire transport (Dapr
    Pub/Sub HTTP) remains a single HTTP path. Method signatures
    are kind-specific so the call site cannot accidentally
    construct a malformed envelope (no ``outputs=`` on
    ``step.failed``, no ``error=`` on ``step.started``, etc.).

    All methods are ``async`` because the underlying publisher is
    async (the production Dapr Pub/Sub HTTP adapter awaits the
    outgoing POST). Implementations are expected to dedup on
    ``(run_id, step_id, attempt, kind)``.
    """

    async def emit_step_started(
        self,
        *,
        workspace_id: str,
        run_id: RunId,
        workflow_version_id: str,
        step_id: str,
        attempt: int,
        occurred_at: datetime,
    ) -> None:
        """Publish ``step.started`` for the start of *attempt*."""
        ...

    async def emit_step_completed(
        self,
        *,
        workspace_id: str,
        run_id: RunId,
        workflow_version_id: str,
        step_id: str,
        attempt: int,
        outputs: Mapping[str, Any],
        occurred_at: datetime,
    ) -> None:
        """Publish ``step.completed`` with the produced ``outputs``."""
        ...

    async def emit_step_failed(
        self,
        *,
        workspace_id: str,
        run_id: RunId,
        workflow_version_id: str,
        step_id: str,
        attempt: int,
        error: Mapping[str, Any],
        occurred_at: datetime,
    ) -> None:
        """Publish ``step.failed`` with the terminal error envelope."""
        ...

    async def emit_step_skipped(
        self,
        *,
        workspace_id: str,
        run_id: RunId,
        workflow_version_id: str,
        step_id: str,
        attempt: int,
        reason: str,
        occurred_at: datetime,
    ) -> None:
        """Publish ``step.skipped`` with a log-safe ``reason``."""
        ...

    async def emit_step_waiting(
        self,
        *,
        workspace_id: str,
        run_id: RunId,
        workflow_version_id: str,
        step_id: str,
        attempt: int,
        wait_token: str,
        occurred_at: datetime,
    ) -> None:
        """Publish ``step.waiting`` for a durable-timer / event suspension."""
        ...

    async def emit_step_retry_scheduled(
        self,
        *,
        workspace_id: str,
        run_id: RunId,
        workflow_version_id: str,
        step_id: str,
        decision: RetryNow,
        envelope: Mapping[str, Any],
        occurred_at: datetime,
    ) -> None:
        """Publish ``step.retry_scheduled`` (delegates to
        :func:`build_retry_scheduled_event` for the envelope).
        """
        ...


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class LifecycleEventPublisherAdapter:
    """Adapt a :class:`LifecycleEventPublisher` for ``step.*`` events.

    Stateful in two minimal dimensions: the injected ``inner``
    publisher and the producer-side dedup cache. Otherwise pure
    event construction.

    :param inner: The underlying :class:`LifecycleEventPublisher`
        — same surface the Run Controller drives for
        ``workflow.*``, so every publication funnels through one
        HTTP path.
    :param max_seen_keys: Optional override for the dedup LRU
        size. Defaults to :data:`DEFAULT_STEP_DEDUP_CACHE_SIZE`.
    """

    __slots__ = ("_inner", "_max_seen_keys", "_seen")

    def __init__(
        self,
        inner: LifecycleEventPublisher,
        *,
        max_seen_keys: int = DEFAULT_STEP_DEDUP_CACHE_SIZE,
    ) -> None:
        self._inner: Final[LifecycleEventPublisher] = inner
        self._max_seen_keys: Final[int] = max_seen_keys
        self._seen: OrderedDict[tuple[str, str, int, str], None] = OrderedDict()

    # ------------------------------------------------------------------
    # Public emit surface (mirrors StepLifecyclePublisher Protocol)
    # ------------------------------------------------------------------

    async def emit_step_started(
        self,
        *,
        workspace_id: str,
        run_id: RunId,
        workflow_version_id: str,
        step_id: str,
        attempt: int,
        occurred_at: datetime,
    ) -> None:
        event = LifecycleEvent(
            kind=LIFECYCLE_KIND_STEP_STARTED,
            workspace_id=workspace_id,
            run_id=run_id,
            workflow_version_id=workflow_version_id,
            occurred_at=occurred_at,
            extra={"step_id": step_id, "attempt": attempt},
        )
        await self._dedup_and_publish(run_id, step_id, attempt, event)

    async def emit_step_completed(
        self,
        *,
        workspace_id: str,
        run_id: RunId,
        workflow_version_id: str,
        step_id: str,
        attempt: int,
        outputs: Mapping[str, Any],
        occurred_at: datetime,
    ) -> None:
        event = LifecycleEvent(
            kind=LIFECYCLE_KIND_STEP_COMPLETED,
            workspace_id=workspace_id,
            run_id=run_id,
            workflow_version_id=workflow_version_id,
            occurred_at=occurred_at,
            extra={
                "step_id": step_id,
                "attempt": attempt,
                "outputs": dict(outputs),
            },
        )
        await self._dedup_and_publish(run_id, step_id, attempt, event)

    async def emit_step_failed(
        self,
        *,
        workspace_id: str,
        run_id: RunId,
        workflow_version_id: str,
        step_id: str,
        attempt: int,
        error: Mapping[str, Any],
        occurred_at: datetime,
    ) -> None:
        event = LifecycleEvent(
            kind=LIFECYCLE_KIND_STEP_FAILED,
            workspace_id=workspace_id,
            run_id=run_id,
            workflow_version_id=workflow_version_id,
            occurred_at=occurred_at,
            extra={
                "step_id": step_id,
                "attempt": attempt,
                "error": dict(error),
            },
        )
        await self._dedup_and_publish(run_id, step_id, attempt, event)

    async def emit_step_skipped(
        self,
        *,
        workspace_id: str,
        run_id: RunId,
        workflow_version_id: str,
        step_id: str,
        attempt: int,
        reason: str,
        occurred_at: datetime,
    ) -> None:
        event = LifecycleEvent(
            kind=LIFECYCLE_KIND_STEP_SKIPPED,
            workspace_id=workspace_id,
            run_id=run_id,
            workflow_version_id=workflow_version_id,
            occurred_at=occurred_at,
            extra={
                "step_id": step_id,
                "attempt": attempt,
                "reason": reason,
            },
        )
        await self._dedup_and_publish(run_id, step_id, attempt, event)

    async def emit_step_waiting(
        self,
        *,
        workspace_id: str,
        run_id: RunId,
        workflow_version_id: str,
        step_id: str,
        attempt: int,
        wait_token: str,
        occurred_at: datetime,
    ) -> None:
        event = LifecycleEvent(
            kind=LIFECYCLE_KIND_STEP_WAITING,
            workspace_id=workspace_id,
            run_id=run_id,
            workflow_version_id=workflow_version_id,
            occurred_at=occurred_at,
            extra={
                "step_id": step_id,
                "attempt": attempt,
                "wait_token": wait_token,
            },
        )
        await self._dedup_and_publish(run_id, step_id, attempt, event)

    async def emit_step_retry_scheduled(
        self,
        *,
        workspace_id: str,
        run_id: RunId,
        workflow_version_id: str,
        step_id: str,
        decision: RetryNow,
        envelope: Mapping[str, Any],
        occurred_at: datetime,
    ) -> None:
        # Delegate envelope construction to retry_driver so the
        # ``previous_*`` audit-correlation fields stay in lockstep
        # with WF-IMPL-053. The retry_driver returns a fully-built
        # LifecycleEvent; we just dedup + publish.
        event = build_retry_scheduled_event(
            workspace_id=workspace_id,
            run_id=run_id,
            workflow_version_id=workflow_version_id,
            step_id=step_id,
            decision=decision,
            envelope=envelope,
            occurred_at=occurred_at,
        )
        # Dedup key uses the attempt that TRIGGERED the retry
        # (``decision.next_attempt - 1``) so a replay re-firing
        # the same decision is absorbed. Using ``next_attempt``
        # would let a logically-distinct second retry from the
        # same source attempt slip through.
        await self._dedup_and_publish(run_id, step_id, decision.next_attempt - 1, event)

    # ------------------------------------------------------------------
    # Private dedup gate
    # ------------------------------------------------------------------

    async def _dedup_and_publish(
        self,
        run_id: RunId,
        step_id: str,
        attempt: int,
        event: LifecycleEvent,
    ) -> None:
        """Forward *event* exactly once per
        ``(run_id, step_id, attempt, kind)`` tuple.

        On a cache hit returns without calling ``inner``. On a
        miss reserves the key BEFORE the awaited inner publish
        so two concurrent emits for the same key cannot both
        observe absence and both forward — the second caller
        sees the reservation and short-circuits. If the inner
        publish raises, the reservation is removed so a retry
        still forwards the event.
        """
        key = (str(run_id), step_id, attempt, event.kind)
        if key in self._seen:
            # Touch — keep this key as recently-seen.
            self._seen.move_to_end(key)
            return
        # Reserve BEFORE the await so a concurrent emit() for the
        # same key short-circuits.
        self._seen[key] = None
        try:
            await self._inner.publish(event)
        except BaseException:
            # Drop the reservation so a retry forwards.
            self._seen.pop(key, None)
            raise
        # Evict the oldest key once we exceed the bound. The key
        # we just inserted is at the tail, so popitem(last=False)
        # cannot evict it.
        if len(self._seen) > self._max_seen_keys:
            self._seen.popitem(last=False)

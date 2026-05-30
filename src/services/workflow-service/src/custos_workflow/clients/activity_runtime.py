"""``ActivityRuntimeClient`` Protocol + result envelope (WF-IMPL-049).

The Step Coordinator's :class:`ActivityStepHandler` (WF-IMPL-054)
schedules each ``ACTIVITY`` step through an
:class:`ActivityRuntimeClient` and reacts to the returned
:class:`ActivityResultEnvelope`. The Protocol is the only thing
the handler talks to — production wires the real
Dapr-Workflow-backed adapter behind the Protocol (deferred
sub-module: *Real ARM Client + Connector Client adapters*),
and unit tests wire :class:`FakeActivityRuntimeClient` to drive
deterministic scenarios.

The Protocol's method signatures match the synchronous
:meth:`custos_workflow.runs.step_handler.StepHandler.execute`
contract: the production adapter is what calls Dapr Workflow's
``ctx.call_activity()`` and yields on the orchestrator's behalf,
hiding the generator dance from every downstream consumer.

Acceptance criteria (mirrored from #420):

* Protocol is ``runtime_checkable``.
* :attr:`ActivityResultEnvelope.class_` is constrained to the four
  ``design.md`` values
  (``"success"``, ``"retryable"``, ``"permanent"``, ``"cancelled"``)
  via a :data:`typing.Literal` alias that mypy enforces in tests.
* 100 % coverage on this module.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final, Literal, Protocol, get_args, runtime_checkable

__all__ = [
    "ACTIVITY_RESULT_CLASSES",
    "ActivityResultClass",
    "ActivityResultEnvelope",
    "ActivityRuntimeClient",
    "FakeActivityRuntimeClient",
    "NoopActivityRuntimeClient",
    "ScheduleActivityRequest",
]


# ---------------------------------------------------------------------------
# Outcome classification
# ---------------------------------------------------------------------------

ActivityResultClass = Literal["success", "retryable", "permanent", "cancelled"]
"""Closed set of outcome classes the Activity Runtime Manager can return.

Pinned to ``design.md`` § *Activity Result Envelope*; the
WF-IMPL-053 retry decision driver and the WF-IMPL-054
``ActivityStepHandler`` dispatch on this set exhaustively.
"""

ACTIVITY_RESULT_CLASSES: Final[frozenset[str]] = frozenset(get_args(ActivityResultClass))
"""Runtime-introspectable mirror of :data:`ActivityResultClass`.

Audit consumers and the WF-IMPL-058 OTel counter use this
frozenset as the closed label set.
"""


# ---------------------------------------------------------------------------
# Request / response envelopes
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ScheduleActivityRequest:
    """Frozen request envelope passed to :meth:`ActivityRuntimeClient.schedule_activity`.

    Immutable on purpose so the Step Coordinator can stash the
    request alongside the lifecycle event it emits without fear
    of any downstream consumer mutating it.

    The ``(run_id, step_id, attempt)`` triple is the same
    idempotency key the Activity Runtime Manager uses to
    deduplicate retries (see WF-IMPL-047 ``IdempotencyTriple``).
    """

    run_id: str
    step_id: str
    attempt: int
    activity_ref: str
    inputs: Mapping[str, Any]
    # ``connector_contexts`` maps ``slot_name -> ConnectorContext``.
    # WF-IMPL-050 introduces the concrete ``ConnectorContext``
    # frozen dataclass; until that lands we keep the value type
    # loose so this module stays dependency-free per the
    # implementation plan.
    connector_contexts: Mapping[str, Any]
    deadline: datetime


@dataclass(frozen=True, slots=True)
class ActivityResultEnvelope:
    """Frozen response envelope returned by :meth:`ActivityRuntimeClient.schedule_activity`.

    Mirrors the ``design.md`` *Activity Result Envelope* shape so
    a single object can flow unchanged from the activity worker
    → ARM → Step Coordinator → ``step.completed`` /
    ``step.failed`` audit event.

    Exactly one of :attr:`outputs` and :attr:`error` is populated
    for any given :attr:`class_`:

    * ``"success"``  — :attr:`outputs` populated, :attr:`error` is ``None``.
    * ``"retryable"`` / ``"permanent"`` / ``"cancelled"`` —
      :attr:`error` populated, :attr:`outputs` is ``None``.

    The retry decision driver (WF-IMPL-053) consumes
    :attr:`class_` + :attr:`error` to choose between scheduling
    a fresh attempt and tipping the step into terminal failure.
    """

    class_: ActivityResultClass
    outputs: Mapping[str, Any] | None
    error: Mapping[str, Any] | None
    attempt: int


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class ActivityRuntimeClient(Protocol):
    """Runtime-checkable Protocol the Step Coordinator depends on.

    The Step Coordinator only ever calls these two methods; the
    production adapter (Dapr Workflow ``ctx.call_activity()``
    bridge — deferred sub-module) and the
    :class:`FakeActivityRuntimeClient` test double both satisfy
    this Protocol structurally.
    """

    def schedule_activity(self, request: ScheduleActivityRequest) -> ActivityResultEnvelope:
        """Schedule one activity attempt and return its result envelope.

        The call is synchronous from the handler's perspective: the
        production adapter is the layer that suspends the
        orchestrator on the Dapr Workflow generator's behalf so the
        handler signature can stay flat.
        """
        ...

    def cancel_activity(self, run_id: str, step_id: str) -> None:
        """Cancel any in-flight attempt for the given step.

        Idempotent: cancelling an already-finished step is a no-op
        for the production adapter, and tests rely on that.
        """
        ...


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class NoopActivityRuntimeClient:
    """Safe default that explicitly :class:`NotImplementedError`-s every call.

    Wired by the FastAPI lifespan (WF-IMPL-057) at startup so the
    process does *not* silently accept activity scheduling
    requests before the real adapter is installed.
    """

    def schedule_activity(self, request: ScheduleActivityRequest) -> ActivityResultEnvelope:
        raise NotImplementedError(
            "NoopActivityRuntimeClient.schedule_activity: "
            "no production ActivityRuntimeClient adapter is wired yet "
            "(deferred sub-module: Real ARM Client adapter)."
        )

    def cancel_activity(self, run_id: str, step_id: str) -> None:
        raise NotImplementedError(
            "NoopActivityRuntimeClient.cancel_activity: "
            "no production ActivityRuntimeClient adapter is wired yet "
            "(deferred sub-module: Real ARM Client adapter)."
        )


@dataclass(slots=True)
class FakeActivityRuntimeClient:
    """In-memory test double that returns canned envelopes.

    Pass a list of pre-built :class:`ActivityResultEnvelope`
    instances on :attr:`results`; each call to
    :meth:`schedule_activity` pops the next envelope in order.
    Every call is recorded on :attr:`calls` and every cancellation
    on :attr:`cancellations` so tests can assert call patterns
    without monkey-patching.

    Raises :class:`IndexError` if a test schedules more activities
    than it queued — that almost always means the test is missing
    a canned envelope, so failing loud beats returning a default.
    """

    results: list[ActivityResultEnvelope] = field(default_factory=list)
    calls: list[ScheduleActivityRequest] = field(default_factory=list)
    cancellations: list[tuple[str, str]] = field(default_factory=list)

    def schedule_activity(self, request: ScheduleActivityRequest) -> ActivityResultEnvelope:
        self.calls.append(request)
        if not self.results:
            raise IndexError(
                "FakeActivityRuntimeClient.schedule_activity: "
                "no more canned envelopes queued "
                f"(called for run_id={request.run_id!r} "
                f"step_id={request.step_id!r} attempt={request.attempt!r})."
            )
        return self.results.pop(0)

    def cancel_activity(self, run_id: str, step_id: str) -> None:
        self.cancellations.append((run_id, step_id))

"""Locked structured error taxonomy for the Step Coordinator.

This module implements WF-IMPL-048 (issue #419): a single, frozen
hierarchy of error classes that the Step Coordinator
(WF-IMPL-053 / WF-IMPL-054 / WF-IMPL-055) raises and surfaces
through ``StepFailed`` envelopes. Each class carries a stable
:attr:`KIND` string used as the audit ``kind`` field for the
``step.*`` lifecycle pipeline.

Mirrors the WF-IMPL-024 compile-time taxonomy
(:mod:`custos_workflow.errors`) and the WF-IMPL-031 Run Controller
taxonomy (:mod:`custos_workflow.runs.errors`) — same base shape, same
``to_dict()`` JSON contract, same ``LOCKED_*_KINDS`` frozenset
discipline so the WF-IMPL-058 OTel error counter can pin its closed
``kind`` label set against this module at build time.

The hierarchy:

* :class:`StepCoordinatorError` — abstract base. Subclasses
  :class:`RuntimeError`. Defines the shared
  ``kind`` / ``message`` / ``run_id`` / ``step_id`` / ``attempt``
  attribute surface, hashable / equal-on-fields identity, and the
  :meth:`to_dict` JSON-safe serializer used by lifecycle event
  emission.
* :class:`StepKindNotImplementedError` — Dispatcher reached a
  ``StepKind`` whose handler has not yet shipped
  (``step.kind_not_implemented``). Also subclasses
  :class:`NotImplementedError`. Returned as a ``StepFailed``
  envelope (not raised across the orchestrator boundary).
* :class:`WithInputResolutionError` — ``with:`` expression
  evaluation failed (``step.with_input_resolution_error``). Also
  subclasses :class:`ValueError`. Wraps the originating
  ``custos_cel.CelError`` so the underlying ``kind`` can survive
  on :attr:`cause_kind`.
* :class:`ConnectorBindError` — ``ConnectorClient.bind_for_step``
  refused to lease a slot (``step.connector_bind_error``). Also
  subclasses :class:`RuntimeError`.
* :class:`ActivityScheduleError` — ``ActivityRuntimeClient.schedule_activity``
  refused the request structurally (``step.activity_schedule_error``).
  Distinct from a *retryable* / *permanent* activity result envelope:
  this fires when the schedule call itself could not be issued.
* :class:`RetryBudgetExhaustedError` — The retry decision driver
  walked the ``on_error`` routes and a ``do:retry`` arm matched but
  ``attempt + 1 > maxAttempts`` (``step.retry_budget_exhausted``).
  Carries the last underlying ``code`` / ``codePrefix`` / ``class``
  for audit correlation.

The :attr:`KIND` string is a class-level :data:`typing.Final`
constant so ``cls.KIND`` and ``instance.kind`` are always identical
and never accidentally overridden by callers.

The closed set of ``kind`` strings is published as
:data:`LOCKED_STEP_KINDS`. Adding or removing a subclass here is a
downstream contract break (the OTel counter, audit consumers, and
the WF-IMPL-059 integration suite all key off the frozenset).

See the issue: https://github.com/toddysm/custos/issues/419
"""

from __future__ import annotations

import builtins
from typing import Any, ClassVar, Final

__all__ = [
    "LOCKED_STEP_KINDS",
    "ActivityScheduleError",
    "ApprovalTimeoutError",
    "ConnectorBindError",
    "LoopExpansionError",
    "RetryBudgetExhaustedError",
    "StepCoordinatorError",
    "StepKindNotImplementedError",
    "SubOrchestrationSpawnError",
    "SubWorkflowFailedError",
    "WithInputResolutionError",
]


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------


class StepCoordinatorError(RuntimeError):
    """Base class for every structured Step Coordinator error.

    Concrete subclasses pin a stable :attr:`KIND` string. The
    constructor signature is intentionally narrow: ``message`` is
    positional, every other field is keyword-only. Subclasses keep
    the same shape so callers and pattern-matching consumers see a
    uniform surface.

    Attributes:
        kind: The :attr:`KIND` of this error's concrete class.
            Always a ``"step.*"`` string.
        message: Human-readable explanation. Mirrors
            ``str(exception)`` for the default formatter.
        run_id: The affected ``runId`` when known. ``None`` for
            failures detected before the run is resolved.
        step_id: The affected ``stepId`` when known. ``None`` for
            failures detected before the step is dispatched.
        attempt: The 1-indexed attempt number when the failure is
            attempt-specific. ``None`` when the failure is
            attempt-agnostic.
    """

    #: Subclasses pin this to a concrete ``"step.*"`` string. The
    #: base raises if instantiated directly because the empty
    #: kind would defeat the taxonomy.
    KIND: ClassVar[str] = ""

    def __init__(
        self,
        message: str,
        *,
        run_id: str | None = None,
        step_id: str | None = None,
        attempt: int | None = None,
    ) -> None:
        if not self.KIND:
            raise builtins.TypeError(
                "StepCoordinatorError is abstract; instantiate a concrete "
                "subclass (StepKindNotImplementedError, "
                "WithInputResolutionError, ConnectorBindError, "
                "ActivityScheduleError, RetryBudgetExhaustedError, "
                "LoopExpansionError, SubOrchestrationSpawnError, "
                "SubWorkflowFailedError, ApprovalTimeoutError) "
                "instead.",
            )
        super().__init__(message)
        self.kind: str = self.KIND
        self.message: str = message
        self.run_id: str | None = run_id
        self.step_id: str | None = step_id
        self.attempt: int | None = attempt

    def _extra_fields(self) -> dict[str, Any]:
        """Hook for subclasses to contribute extra fields to
        :meth:`to_dict` and :meth:`__repr__` / :meth:`__eq__` /
        :meth:`__hash__`.

        The base returns an empty mapping. Subclasses override
        and return only JSON-safe primitives. The mapping's
        iteration order is preserved by :meth:`to_dict` so audit
        serialization stays deterministic.
        """
        return {}

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dict for ``step.*`` lifecycle event emission.

        Shape (deterministic key order):

        ``{"kind": str, "message": str, "run_id": str | None,
        "step_id": str | None, "attempt": int | None, ...}``

        Subclasses extend the result with their structured fields
        (see :meth:`_extra_fields`). The result is deterministic
        in key order so byte-stable audit serialization is possible
        without an extra canonicalization step.
        """

        out: dict[str, Any] = {
            "kind": self.kind,
            "message": self.message,
            "run_id": self.run_id,
            "step_id": self.step_id,
            "attempt": self.attempt,
        }
        out.update(self._extra_fields())
        return out

    def __repr__(self) -> str:
        parts: list[str] = [
            f"kind={self.kind!r}",
            f"message={self.message!r}",
            f"run_id={self.run_id!r}",
            f"step_id={self.step_id!r}",
            f"attempt={self.attempt!r}",
        ]
        parts.extend(f"{name}={value!r}" for name, value in self._extra_fields().items())
        return f"{type(self).__name__}({', '.join(parts)})"

    def _identity(self) -> tuple[Any, ...]:
        """Hashable identity tuple used by :meth:`__eq__` and :meth:`__hash__`.

        Concrete instances of the same subclass with identical
        fields compare equal and hash identically — different
        from the default exception-by-identity semantics. This
        is intentional: audit consumers dedupe failures by
        structural identity rather than instance.

        All current subclass extras are JSON-scalar primitives
        (``str | int | None``), which are directly hashable. If
        a future subclass introduces a list/dict extra, this
        method MUST be revisited (the tuple cast would raise
        ``TypeError`` and the build will fail loudly).
        """
        return (
            type(self),
            self.kind,
            self.message,
            self.run_id,
            self.step_id,
            self.attempt,
            tuple(self._extra_fields().items()),
        )

    def __eq__(self, other: object) -> bool:
        if type(other) is not type(self):
            return NotImplemented
        return self._identity() == other._identity()

    def __hash__(self) -> int:
        return hash(self._identity())


# ---------------------------------------------------------------------------
# Concrete subclasses
# ---------------------------------------------------------------------------


class StepKindNotImplementedError(StepCoordinatorError, NotImplementedError):
    """The Step Coordinator dispatcher reached a ``StepKind`` whose
    handler has not yet shipped.

    Raised by WF-IMPL-055 for sub-orchestration / ``waitFor:`` step
    kinds (which land in the deferred Sub-Orchestration Manager and
    Resume Subscription Manager sub-modules respectively). Returned
    as a ``StepFailed`` envelope, not raised across the orchestrator
    boundary, so the run fails loudly but the audit pipeline still
    captures the event.

    Also subclasses :class:`NotImplementedError` so callers using
    ``except NotImplementedError:`` still catch it.

    Attributes:
        step_kind: The unsupported ``StepKind`` value, when known.
        primitive_handler: The compiled ``PrimitiveHandler`` tag the
            dispatcher resolved to, when known.
    """

    KIND: Final[str] = "step.kind_not_implemented"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        run_id: str | None = None,
        step_id: str | None = None,
        attempt: int | None = None,
        step_kind: str | None = None,
        primitive_handler: str | None = None,
    ) -> None:
        super().__init__(message, run_id=run_id, step_id=step_id, attempt=attempt)
        self.step_kind: str | None = step_kind
        self.primitive_handler: str | None = primitive_handler

    def _extra_fields(self) -> dict[str, Any]:
        return {
            "step_kind": self.step_kind,
            "primitive_handler": self.primitive_handler,
        }


class WithInputResolutionError(StepCoordinatorError, ValueError):
    """A ``with:`` expression failed to evaluate.

    Raised by the WF-IMPL-051 ``WithInputResolver`` when any
    ``custos_cel.CelError`` (parse, type, evaluation, timeout, sandbox)
    propagates out of one of the per-step ``with:`` CEL evaluations.

    The originating ``CelError`` ``kind`` is preserved verbatim on
    :attr:`cause_kind` so audit consumers can still dispatch on the
    underlying cause without traversing the wrapper hierarchy.

    Also subclasses :class:`ValueError` so callers using
    ``except ValueError:`` still catch it.

    Attributes:
        binding_name: The ``with:`` slot name whose expression
            failed, when known.
        cause_kind: The underlying ``custos_cel.CelError`` ``kind``
            (e.g. ``"cel.parse_error"``), when known.
        source: The CEL source string that failed, when small enough
            to record. ``None`` when omitted (e.g. for very large
            expressions or when the caller has already logged it).
    """

    KIND: Final[str] = "step.with_input_resolution_error"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        run_id: str | None = None,
        step_id: str | None = None,
        attempt: int | None = None,
        binding_name: str | None = None,
        cause_kind: str | None = None,
        source: str | None = None,
    ) -> None:
        super().__init__(message, run_id=run_id, step_id=step_id, attempt=attempt)
        self.binding_name: str | None = binding_name
        self.cause_kind: str | None = cause_kind
        self.source: str | None = source

    def _extra_fields(self) -> dict[str, Any]:
        return {
            "binding_name": self.binding_name,
            "cause_kind": self.cause_kind,
            "source": self.source,
        }


class ConnectorBindError(StepCoordinatorError):
    """``ConnectorClient.bind_for_step`` refused to lease a slot.

    Raised by WF-IMPL-054 when the per-attempt connector bind call
    fails structurally (the Connector Service rejected the request,
    or the underlying transport is unavailable). This is distinct
    from a *runtime* connector failure that surfaces inside an
    activity envelope — that failure path goes through the retry
    driver.

    Attributes:
        slot_name: The first slot whose bind failed, when known.
        connector_ref: The ``ConnectorRef`` for that slot, when known.
        cause: A short ``str`` summary of the underlying failure
            (typically ``repr(exc)``), when known.
    """

    KIND: Final[str] = "step.connector_bind_error"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        run_id: str | None = None,
        step_id: str | None = None,
        attempt: int | None = None,
        slot_name: str | None = None,
        connector_ref: str | None = None,
        cause: str | None = None,
    ) -> None:
        super().__init__(message, run_id=run_id, step_id=step_id, attempt=attempt)
        self.slot_name: str | None = slot_name
        self.connector_ref: str | None = connector_ref
        self.cause: str | None = cause

    def _extra_fields(self) -> dict[str, Any]:
        return {
            "slot_name": self.slot_name,
            "connector_ref": self.connector_ref,
            "cause": self.cause,
        }


class ActivityScheduleError(StepCoordinatorError):
    """``ActivityRuntimeClient.schedule_activity`` refused the request.

    Raised by WF-IMPL-054 when the schedule call itself fails before
    an activity envelope can be returned (ARM rejected the request,
    transport failed, deadline already in the past). This is distinct
    from a *retryable* / *permanent* / *cancelled* envelope class —
    those flow through the retry-driver path.

    Attributes:
        activity_ref: The compiled ``activityRef`` the schedule
            targeted, when known.
        cause: A short ``str`` summary of the underlying failure
            (typically ``repr(exc)``), when known.
    """

    KIND: Final[str] = "step.activity_schedule_error"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        run_id: str | None = None,
        step_id: str | None = None,
        attempt: int | None = None,
        activity_ref: str | None = None,
        cause: str | None = None,
    ) -> None:
        super().__init__(message, run_id=run_id, step_id=step_id, attempt=attempt)
        self.activity_ref: str | None = activity_ref
        self.cause: str | None = cause

    def _extra_fields(self) -> dict[str, Any]:
        return {
            "activity_ref": self.activity_ref,
            "cause": self.cause,
        }


class RetryBudgetExhaustedError(StepCoordinatorError):
    """The retry decision driver matched a ``do:retry`` arm but the
    next attempt would exceed ``maxAttempts``.

    Raised by WF-IMPL-053 and surfaced through WF-IMPL-054 as the
    terminal ``StepFailed`` envelope for retry-exhausted activity
    steps. Carries the last underlying activity envelope's ``code``
    / ``codePrefix`` / ``class`` so the audit pipeline can correlate
    the terminal failure back to the proximate cause.

    Attributes:
        max_attempts: The ``maxAttempts`` ceiling that was hit.
        last_code: The activity envelope's ``code`` from the final
            attempt, when known.
        last_code_prefix: The activity envelope's ``codePrefix`` from
            the final attempt, when known.
        last_class: The activity envelope's ``class``
            (``"retryable" | "permanent" | "cancelled"``) from the
            final attempt, when known.
    """

    KIND: Final[str] = "step.retry_budget_exhausted"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        run_id: str | None = None,
        step_id: str | None = None,
        attempt: int | None = None,
        max_attempts: int | None = None,
        last_code: str | None = None,
        last_code_prefix: str | None = None,
        last_class: str | None = None,
    ) -> None:
        super().__init__(message, run_id=run_id, step_id=step_id, attempt=attempt)
        self.max_attempts: int | None = max_attempts
        self.last_code: str | None = last_code
        self.last_code_prefix: str | None = last_code_prefix
        self.last_class: str | None = last_class

    def _extra_fields(self) -> dict[str, Any]:
        return {
            "max_attempts": self.max_attempts,
            "last_code": self.last_code,
            "last_code_prefix": self.last_code_prefix,
            "last_class": self.last_class,
        }


# ---------------------------------------------------------------------------
# Sub-Orchestration Manager subclasses (WF-IMPL-086)
# ---------------------------------------------------------------------------


class LoopExpansionError(StepCoordinatorError, ValueError):
    """A dynamic-loop (``forEach`` / ``where:``) expansion failed.

    Raised by the WF-IMPL-089 / WF-IMPL-090 loop fan-out path when the
    ``forEach`` (or ``where:``) CEL expression fails to evaluate, when
    the expression yields a non-iterable, or when two expanded items
    derive the *same* deterministic iteration key (a collision that
    would alias their child instance ids).

    The originating ``custos_cel.CelError`` ``kind`` is preserved on
    :attr:`cause_kind` (when the failure was an evaluation error) so
    audit consumers can dispatch on the underlying cause. For a
    duplicate-key collision :attr:`colliding_key` carries the offending
    iteration key.

    Also subclasses :class:`ValueError` so callers using
    ``except ValueError:`` still catch it.

    Attributes:
        cause_kind: The underlying ``custos_cel.CelError`` ``kind``
            (e.g. ``"cel.evaluation_error"``), when the failure was a
            CEL evaluation error. ``None`` for structural failures.
        source: The ``forEach`` / ``where:`` CEL source string that
            failed, when small enough to record. ``None`` when omitted.
        colliding_key: The duplicate iteration key when the failure was
            a key collision. ``None`` otherwise.
    """

    KIND: Final[str] = "step.loop_expansion_error"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        run_id: str | None = None,
        step_id: str | None = None,
        attempt: int | None = None,
        cause_kind: str | None = None,
        source: str | None = None,
        colliding_key: str | None = None,
    ) -> None:
        super().__init__(message, run_id=run_id, step_id=step_id, attempt=attempt)
        self.cause_kind: str | None = cause_kind
        self.source: str | None = source
        self.colliding_key: str | None = colliding_key

    def _extra_fields(self) -> dict[str, Any]:
        return {
            "cause_kind": self.cause_kind,
            "source": self.source,
            "colliding_key": self.colliding_key,
        }


class SubOrchestrationSpawnError(StepCoordinatorError):
    """Spawning a child workflow instance failed structurally.

    Raised by the WF-IMPL-088 / WF-IMPL-091 sub-orchestration path when
    the ``start_child_workflow`` call itself could not be issued (the
    Dapr workflow runtime rejected the request, the deterministic child
    instance id was malformed, or the transport is unavailable). This
    is distinct from a child that *ran* and returned a failure
    envelope — that path surfaces :class:`SubWorkflowFailedError`.

    Attributes:
        child_instance_id: The deterministic child instance id the
            spawn targeted, when known.
        iteration_key: The iteration key for the child, when the spawn
            was part of a dynamic loop. ``None`` for single
            sub-workflow invocations.
        cause: A short ``str`` summary of the underlying failure
            (typically ``repr(exc)``), when known.
    """

    KIND: Final[str] = "step.sub_orchestration_spawn_error"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        run_id: str | None = None,
        step_id: str | None = None,
        attempt: int | None = None,
        child_instance_id: str | None = None,
        iteration_key: str | None = None,
        cause: str | None = None,
    ) -> None:
        super().__init__(message, run_id=run_id, step_id=step_id, attempt=attempt)
        self.child_instance_id: str | None = child_instance_id
        self.iteration_key: str | None = iteration_key
        self.cause: str | None = cause

    def _extra_fields(self) -> dict[str, Any]:
        return {
            "child_instance_id": self.child_instance_id,
            "iteration_key": self.iteration_key,
            "cause": self.cause,
        }


class SubWorkflowFailedError(StepCoordinatorError):
    """A child sub-workflow ran and surfaced a terminal failure.

    Raised by the WF-IMPL-089 / WF-IMPL-091 merge path when an awaited
    child instance completes with a failure envelope. A single child
    failure short-circuits the parent loop / invocation. The child's
    underlying failure ``kind`` is preserved on :attr:`child_kind` so
    the audit pipeline can correlate the parent failure back to the
    proximate child cause.

    Attributes:
        child_instance_id: The deterministic child instance id that
            failed, when known.
        iteration_key: The iteration key for the failed child, when the
            failure was inside a dynamic loop. ``None`` for single
            sub-workflow invocations.
        child_kind: The child's underlying failure ``kind`` (e.g. a
            ``"step.*"`` taxonomy string), when known.
    """

    KIND: Final[str] = "step.sub_workflow_failed"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        run_id: str | None = None,
        step_id: str | None = None,
        attempt: int | None = None,
        child_instance_id: str | None = None,
        iteration_key: str | None = None,
        child_kind: str | None = None,
    ) -> None:
        super().__init__(message, run_id=run_id, step_id=step_id, attempt=attempt)
        self.child_instance_id: str | None = child_instance_id
        self.iteration_key: str | None = iteration_key
        self.child_kind: str | None = child_kind

    def _extra_fields(self) -> dict[str, Any]:
        return {
            "child_instance_id": self.child_instance_id,
            "iteration_key": self.iteration_key,
            "child_kind": self.child_kind,
        }


class ApprovalTimeoutError(StepCoordinatorError):
    """An ``approval:`` gate timed out before a signal arrived.

    Raised by the WF-IMPL-092 approval gate when the durable timer in
    the ``when_any([childInstance, durableTimer])`` race fires before
    the approval ``RaiseExternalEvent`` signal is delivered. The
    configured ``timeout:`` (ISO-8601 duration) is preserved on
    :attr:`timeout` for audit correlation.

    Attributes:
        child_instance_id: The deterministic approval child instance id
            (``<runId>/<stepId>/approval``), when known.
        timeout: The configured ISO-8601 ``timeout:`` duration that
            elapsed, when known.
    """

    KIND: Final[str] = "step.approval_timeout"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        run_id: str | None = None,
        step_id: str | None = None,
        attempt: int | None = None,
        child_instance_id: str | None = None,
        timeout: str | None = None,
    ) -> None:
        super().__init__(message, run_id=run_id, step_id=step_id, attempt=attempt)
        self.child_instance_id: str | None = child_instance_id
        self.timeout: str | None = timeout

    def _extra_fields(self) -> dict[str, Any]:
        return {
            "child_instance_id": self.child_instance_id,
            "timeout": self.timeout,
        }


# ---------------------------------------------------------------------------
# Locked kind set
# ---------------------------------------------------------------------------


#: The closed set of ``step.*`` ``kind`` strings published by this
#: taxonomy. The WF-IMPL-058 OTel error counter asserts that its
#: ``kind`` label set matches this frozenset at build time, so
#: adding or removing a subclass here is a downstream contract
#: break.
LOCKED_STEP_KINDS: Final[frozenset[str]] = frozenset(
    {
        StepKindNotImplementedError.KIND,
        WithInputResolutionError.KIND,
        ConnectorBindError.KIND,
        ActivityScheduleError.KIND,
        RetryBudgetExhaustedError.KIND,
        LoopExpansionError.KIND,
        SubOrchestrationSpawnError.KIND,
        SubWorkflowFailedError.KIND,
        ApprovalTimeoutError.KIND,
    }
)

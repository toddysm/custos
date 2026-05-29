"""Locked structured error taxonomy for the Run Controller.

This module implements WF-IMPL-031 (issue #383): a single, frozen
hierarchy of error classes that every public Run Controller entry
point raises. Each class carries a stable :attr:`KIND` string used
as the audit ``kind`` field for run-time failures.

Mirrors the WF-IMPL-024 compile-time taxonomy convention
(:mod:`custos_workflow.errors`): a :class:`RunControllerError` base
+ four canonical subclasses. Each subclass also subclasses a Python
builtin so callers using broad ``except`` blocks still catch it.

The hierarchy:

* :class:`RunControllerError` — abstract base. Subclasses
  :class:`RuntimeError`. Defines the shared ``kind`` / ``message``
  / ``run_id`` attribute surface, hashable / equal-on-fields
  identity, and the :meth:`to_dict` JSON-safe serializer used by
  audit emission.
* :class:`RunNotFoundError` — Lookup against an unknown
  ``runId`` (``run.not_found``). Also subclasses
  :class:`LookupError`.
* :class:`RunStateConflictError` — Status transition disallowed
  by the state machine (``run.state_conflict``).
* :class:`RunStateCorruptError` — Compiled-graph JSON failed
  deserialization (``run.state_corrupt``).
* :class:`WorkflowRuntimeUnavailableError` — Dapr sidecar
  unreachable (``run.runtime_unavailable``). Also subclasses
  :class:`ConnectionError`.

The :attr:`KIND` string is a class-level :data:`typing.Final`
constant so ``cls.KIND`` and ``instance.kind`` are always
identical and never accidentally overridden by callers.

The closed set of ``kind`` strings is published as
:data:`LOCKED_RUN_KINDS`. The WF-IMPL-044 OTel error counter
asserts that the ``outcome`` label set matches this frozenset at
build time, so adding or removing a subclass here is a downstream
contract break.

See the issue: https://github.com/toddysm/custos/issues/383
"""

from __future__ import annotations

import builtins
from typing import Any, ClassVar, Final

__all__ = [
    "LOCKED_RUN_KINDS",
    "RunControllerError",
    "RunNotFoundError",
    "RunStateConflictError",
    "RunStateCorruptError",
    "WorkflowRuntimeUnavailableError",
]


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------


class RunControllerError(RuntimeError):
    """Base class for every structured Run Controller error.

    Concrete subclasses pin a stable :attr:`KIND` string. The
    constructor signature is intentionally narrow: ``message`` is
    positional, every other field is keyword-only. Subclasses keep
    the same shape so callers and pattern-matching consumers see a
    uniform surface.

    Attributes:
        kind: The :attr:`KIND` of this error's concrete class.
            Always a ``"run.*"`` string.
        message: Human-readable explanation. Mirrors
            ``str(exception)`` for the default formatter.
        run_id: The affected ``runId`` when known. ``None`` for
            failures that arise before a run is resolved (e.g.
            a ``WorkflowRuntimeUnavailableError`` raised from
            ``ScheduleRun`` before the run row is even attempted).
    """

    #: Subclasses pin this to a concrete ``"run.*"`` string. The
    #: base raises if instantiated directly because the empty
    #: kind would defeat the taxonomy.
    KIND: ClassVar[str] = ""

    def __init__(
        self,
        message: str,
        *,
        run_id: str | None = None,
    ) -> None:
        if not self.KIND:
            raise builtins.TypeError(
                "RunControllerError is abstract; instantiate a concrete "
                "subclass (RunNotFoundError, RunStateConflictError, "
                "RunStateCorruptError, WorkflowRuntimeUnavailableError) "
                "instead.",
            )
        super().__init__(message)
        self.kind: str = self.KIND
        self.message: str = message
        self.run_id: str | None = run_id

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
        """Return a JSON-safe dict for audit-event emission.

        Shape (deterministic key order):

        ``{"kind": str, "message": str, "run_id": str | None, ...}``

        Subclasses extend the result with their structured fields
        (see :meth:`_extra_fields`). The result is deterministic
        in key order: ``kind`` first, then ``message``, then
        ``run_id``, then any subclass extras in their declaration
        order — so byte-stable audit serialization is possible
        without an extra canonicalization step.
        """
        out: dict[str, Any] = {
            "kind": self.kind,
            "message": self.message,
            "run_id": self.run_id,
        }
        out.update(self._extra_fields())
        return out

    def __repr__(self) -> str:
        parts: list[str] = [
            f"kind={self.kind!r}",
            f"message={self.message!r}",
            f"run_id={self.run_id!r}",
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
        (``str | None``), which are directly hashable. If a future
        subclass introduces a list/dict extra, this method MUST be
        revisited (the tuple cast would raise ``TypeError`` and
        the build will fail loudly).
        """
        return (
            type(self),
            self.kind,
            self.message,
            self.run_id,
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


class RunNotFoundError(RunControllerError, LookupError):
    """A Run Controller entry point received a ``runId`` that no
    persisted row matches.

    Raised by ``get_run`` and ``cancel_run`` (and any other
    lookup-style entry point) when the row is absent from the
    metadata store. Also subclasses :class:`LookupError` so
    callers using ``except LookupError:`` still catch it.
    """

    KIND: Final[str] = "run.not_found"  # type: ignore[misc]


class RunStateConflictError(RunControllerError):
    """A requested status transition is disallowed by the state machine.

    Raised when the caller asks the Run Controller to move a run
    into a status the WF-IMPL-032 transition table forbids — for
    example, ``cancel`` after ``succeeded``, or ``pause`` after
    ``failed``. Also raised by ``put_run`` when a duplicate insert
    on the same ``(workspace_id, run_id)`` carries a divergent
    payload (per WF-IMPL-032 idempotency rules).

    Attributes:
        current_status: The run's actual status at the time of
            the rejected transition, when known.
        attempted_status: The status the caller asked the run to
            move to, when known.
    """

    KIND: Final[str] = "run.state_conflict"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        run_id: str | None = None,
        current_status: str | None = None,
        attempted_status: str | None = None,
    ) -> None:
        super().__init__(message, run_id=run_id)
        self.current_status: str | None = current_status
        self.attempted_status: str | None = attempted_status

    def _extra_fields(self) -> dict[str, Any]:
        return {
            "current_status": self.current_status,
            "attempted_status": self.attempted_status,
        }


class RunStateCorruptError(RunControllerError):
    """A persisted run row failed deserialization.

    Raised when ``RunRecord.compiled_graph`` JSON cannot be
    rehydrated via the WF-IMPL-018 ``ExecutionGraph.from_json``
    (truncated payload, schema-version mismatch, garbage bytes).
    The originating exception is preserved verbatim under
    :attr:`cause` for audit correlation.

    Attributes:
        cause: A short ``str`` summary of the underlying
            deserialization failure (typically ``repr(exc)``),
            when known. ``None`` when the corruption was detected
            structurally without a wrapped exception.
    """

    KIND: Final[str] = "run.state_corrupt"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        run_id: str | None = None,
        cause: str | None = None,
    ) -> None:
        super().__init__(message, run_id=run_id)
        self.cause: str | None = cause

    def _extra_fields(self) -> dict[str, Any]:
        return {"cause": self.cause}


class WorkflowRuntimeUnavailableError(RunControllerError, ConnectionError):
    """A ``WorkflowClient`` call failed because the Dapr sidecar is unreachable.

    Raised by the runtime adapter when the underlying Dapr SDK
    surfaces a transport-level failure (sidecar not running, gRPC
    deadline exceeded, name-resolution failure). Also subclasses
    :class:`ConnectionError` so callers using
    ``except ConnectionError:`` still catch it.

    Attributes:
        cause: A short ``str`` summary of the underlying
            transport failure (typically ``repr(exc)``), when
            known. ``None`` when the unavailability was inferred
            structurally without a wrapped exception.
    """

    KIND: Final[str] = "run.runtime_unavailable"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        run_id: str | None = None,
        cause: str | None = None,
    ) -> None:
        super().__init__(message, run_id=run_id)
        self.cause: str | None = cause

    def _extra_fields(self) -> dict[str, Any]:
        return {"cause": self.cause}


# ---------------------------------------------------------------------------
# Locked kind set
# ---------------------------------------------------------------------------


#: The closed set of ``run.*`` ``kind`` strings published by this
#: taxonomy. The WF-IMPL-044 OTel error counter asserts that its
#: ``outcome`` label set matches this frozenset at build time, so
#: adding or removing a subclass here is a downstream contract
#: break.
LOCKED_RUN_KINDS: Final[frozenset[str]] = frozenset(
    {
        RunNotFoundError.KIND,
        RunStateConflictError.KIND,
        RunStateCorruptError.KIND,
        WorkflowRuntimeUnavailableError.KIND,
    }
)

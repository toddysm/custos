"""Deterministic ``(runId, stepId, attempt)`` idempotency triples (WF-IMPL-047).

This module locks the wire contract for the per-attempt idempotency
key shared by every downstream collaborator of the Step Coordinator:

* The **Activity Runtime Manager** (COMP-006) uses the canonical
  ``to_str()`` form as the ``ScheduleActivity`` idempotency key, so
  re-issued schedules during Dapr Workflow replay collapse to the
  same activation.
* The **Connector Service** (COMP-005) uses the same string as the
  per-step lease key when binding ``slots[]`` for an attempt.
* The **Observability + Audit Service** correlates ``step.*``
  lifecycle events back to attempts via this key.

The canonical wire form is::

    f"{run_id}|{step_id}|{attempt}"

Once a workflow has executed against this format, the encoding
**must not** change — any drift would split idempotency across
collaborators and silently double-execute work.

The triple is intentionally pure data: callers (the Step Coordinator
and its handlers) own the responsibility of deriving the right
``attempt`` number for the current pass.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

__all__ = [
    "IDEMPOTENCY_TRIPLE_SEPARATOR",
    "IdempotencyTriple",
    "IdempotencyTripleError",
    "derive_triple",
]


#: Canonical separator between the three components on the wire.
#:
#: **Locked.** The Activity Runtime Manager and Connector Service
#: both parse on this exact character; changing it would invalidate
#: every previously issued idempotency key.
IDEMPOTENCY_TRIPLE_SEPARATOR: Final[str] = "|"


class IdempotencyTripleError(ValueError):
    """Raised when an :class:`IdempotencyTriple` cannot be constructed
    or parsed.

    Inherits from :class:`ValueError` so callers that already catch
    validation failures pick this up uniformly. The Step Coordinator
    error taxonomy (WF-IMPL-048) wraps these with a stable ``kind``
    string when emitting lifecycle events.
    """


@dataclass(frozen=True, slots=True)
class IdempotencyTriple:
    """An immutable ``(run_id, step_id, attempt)`` triple.

    All three components are required; ``attempt`` is a 1-indexed
    positive integer matching the runtime's per-step attempt counter.

    The dataclass is :class:`frozen` and :data:`slots` so it is
    hashable and cheap to allocate during high-throughput step
    scheduling.
    """

    run_id: str
    step_id: str
    attempt: int

    def __post_init__(self) -> None:
        _validate_run_id(self.run_id)
        _validate_step_id(self.step_id)
        _validate_attempt(self.attempt)

    def to_str(self) -> str:
        """Return the canonical ``"run_id|step_id|attempt"`` wire form.

        Byte-equal for identical ``(run_id, step_id, attempt)`` inputs
        across processes, replays, and Python versions.
        """

        return (
            f"{self.run_id}"
            f"{IDEMPOTENCY_TRIPLE_SEPARATOR}{self.step_id}"
            f"{IDEMPOTENCY_TRIPLE_SEPARATOR}{self.attempt}"
        )

    @classmethod
    def from_str(cls, value: str) -> IdempotencyTriple:
        """Parse a canonical wire string back into an :class:`IdempotencyTriple`.

        :raises IdempotencyTripleError: If ``value`` does not contain
            exactly two separators, has empty ``run_id`` / ``step_id``
            components, or its ``attempt`` component is not a
            positive integer.
        """

        parts = value.split(IDEMPOTENCY_TRIPLE_SEPARATOR)
        if len(parts) != 3:
            raise IdempotencyTripleError(
                "idempotency triple wire form must contain exactly two "
                f"{IDEMPOTENCY_TRIPLE_SEPARATOR!r} separators; got {value!r}"
            )
        run_id, step_id, attempt_str = parts
        try:
            attempt = int(attempt_str)
        except ValueError as exc:
            raise IdempotencyTripleError(
                f"idempotency triple attempt component must be an integer; got {attempt_str!r}"
            ) from exc
        # The dataclass constructor re-runs the full validation pipeline
        # (separator-free run_id / step_id, attempt >= 1) via
        # ``__post_init__`` so parsing and construction share one code
        # path.
        try:
            return cls(run_id=run_id, step_id=step_id, attempt=attempt)
        except ValueError as exc:
            raise IdempotencyTripleError(str(exc)) from exc


def derive_triple(run_id: str, step_id: str, attempt: int) -> IdempotencyTriple:
    """Construct an :class:`IdempotencyTriple` from its components.

    Thin convenience wrapper around the dataclass constructor that
    keeps the call site readable at the Step Coordinator dispatch
    layer::

        triple = derive_triple(run_id=ctx.run_id, step_id=node.step_id, attempt=attempt)

    :raises ValueError: If ``attempt < 1``.
    :raises IdempotencyTripleError: If ``run_id`` or ``step_id`` is
        empty or contains the canonical separator.
    """

    return IdempotencyTriple(run_id=run_id, step_id=step_id, attempt=attempt)


# ---------------------------------------------------------------------------
# Internal validators
# ---------------------------------------------------------------------------


def _validate_run_id(run_id: str) -> None:
    if not run_id:
        raise IdempotencyTripleError("run_id must be a non-empty string")
    if IDEMPOTENCY_TRIPLE_SEPARATOR in run_id:
        raise IdempotencyTripleError(
            f"run_id must not contain the canonical separator "
            f"{IDEMPOTENCY_TRIPLE_SEPARATOR!r}; got {run_id!r}"
        )


def _validate_step_id(step_id: str) -> None:
    if not step_id:
        raise IdempotencyTripleError("step_id must be a non-empty string")
    if IDEMPOTENCY_TRIPLE_SEPARATOR in step_id:
        raise IdempotencyTripleError(
            f"step_id must not contain the canonical separator "
            f"{IDEMPOTENCY_TRIPLE_SEPARATOR!r}; got {step_id!r}"
        )


def _validate_attempt(attempt: int) -> None:
    if isinstance(attempt, bool) or not isinstance(attempt, int):
        # bool is a subclass of int in Python; reject explicitly so
        # ``attempt=True`` cannot accidentally pass the >= 1 check.
        raise ValueError(f"attempt must be an int, got {type(attempt).__name__}")
    if attempt < 1:
        raise ValueError(f"attempt must be >= 1, got {attempt}")

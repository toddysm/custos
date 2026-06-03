"""ActivityExecution store — per-attempt execution records + state machine.

ARM persists one execution record per ``(run_id, step_id, attempt)`` idempotency
triple. The record drives idempotent replay — a terminal record short-circuits a
re-scheduled attempt — and crash reconciliation.

The SPL ``MetadataStoreProvider`` exposes per-attempt state through the
append-only ``StepAttempt`` family: a write to an already-written triple raises
``ImmutableViolation``. That immutability is incompatible with an in-place,
update-per-transition row, so the state machine lives on the ARM-owned
:class:`ActivityExecution` model and the live (non-terminal) working set is held
in process. The repository persists exactly once — when an execution reaches a
terminal state — writing the immutable replay-dedup record. The full execution
snapshot rides in ``StepAttempt.error`` so :meth:`ExecutionRepository.get` can
faithfully reconstruct the record after a process restart.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Final

from custos_spl.errors import ImmutableViolation
from custos_spl.ids import RunId, StepId, WorkspaceId
from custos_spl.interfaces.metadata_store import MetadataStoreProvider, StepAttempt
from pydantic import BaseModel, ConfigDict

#: The idempotency triple plus its owning workspace, used as the in-process key.
ExecutionKey = tuple[str, str, str, int]


class ExecutionState(StrEnum):
    """Lifecycle states of a single activity attempt.

    The happy path is ``pending → resolving → materializing → running →
    finalizing → succeeded``. ``failed`` and ``cancelled`` are reachable from
    any non-terminal state. The three terminal states are absorbing.
    """

    PENDING = "pending"
    RESOLVING = "resolving"
    MATERIALIZING = "materializing"
    RUNNING = "running"
    FINALIZING = "finalizing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


#: States from which no further transition is permitted.
TERMINAL_STATES: Final[frozenset[ExecutionState]] = frozenset(
    {ExecutionState.SUCCEEDED, ExecutionState.FAILED, ExecutionState.CANCELLED}
)

#: Successful single-step progression from each live state.
_HAPPY_PATH: Final[dict[ExecutionState, ExecutionState]] = {
    ExecutionState.PENDING: ExecutionState.RESOLVING,
    ExecutionState.RESOLVING: ExecutionState.MATERIALIZING,
    ExecutionState.MATERIALIZING: ExecutionState.RUNNING,
    ExecutionState.RUNNING: ExecutionState.FINALIZING,
    ExecutionState.FINALIZING: ExecutionState.SUCCEEDED,
}

#: Terminal states a live execution may jump to at any point (failure / cancel).
_TERMINATING: Final[frozenset[ExecutionState]] = frozenset(
    {ExecutionState.FAILED, ExecutionState.CANCELLED}
)


def allowed_transitions(state: ExecutionState) -> frozenset[ExecutionState]:
    """Return the states reachable in one step from ``state``."""
    if state in TERMINAL_STATES:
        return frozenset()
    return frozenset({_HAPPY_PATH[state]}) | _TERMINATING


class ExecutionStoreError(Exception):
    """Base class for execution-store failures."""


class DuplicateExecutionError(ExecutionStoreError):
    """Raised when a record for the idempotency triple already exists."""


class IllegalTransitionError(ExecutionStoreError):
    """Raised when a state transition violates the lifecycle state machine."""


class UnknownExecutionError(ExecutionStoreError):
    """Raised when transitioning an execution that was never inserted."""


class ActivityExecution(BaseModel):
    """One attempt of one step, keyed by ``(workspace_id, run_id, step_id, attempt)``."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    workspace_id: str
    run_id: str
    step_id: str
    attempt: int
    activity_ref: str
    deadline: datetime
    started_at: datetime
    state: ExecutionState = ExecutionState.PENDING
    resolved_digest: str | None = None
    isolation_tier: str | None = None
    runtime_class: str | None = None
    result_class: str | None = None
    error_code: str | None = None
    sandbox_ref: str | None = None
    finished_at: datetime | None = None

    @property
    def key(self) -> ExecutionKey:
        """The in-process key — the idempotency triple plus its workspace."""
        return (self.workspace_id, self.run_id, self.step_id, self.attempt)

    @property
    def is_terminal(self) -> bool:
        """``True`` when the execution has reached an absorbing state."""
        return self.state in TERMINAL_STATES

    def transition(self, to_state: ExecutionState, **changes: Any) -> ActivityExecution:
        """Return a copy advanced to ``to_state``, applying ``changes``.

        Raises :class:`IllegalTransitionError` when ``to_state`` is not reachable
        from the current state.
        """
        if to_state not in allowed_transitions(self.state):
            raise IllegalTransitionError(
                f"illegal transition {self.state.value} -> {to_state.value} "
                f"for {self.run_id}/{self.step_id}/{self.attempt}"
            )
        return self.model_copy(update={"state": to_state, **changes})


def _to_step_attempt(execution: ActivityExecution) -> StepAttempt:
    """Map a terminal execution onto an append-only SPL ``StepAttempt``."""
    snapshot = execution.model_dump(mode="json", exclude_none=True)
    return StepAttempt(
        workspace_id=WorkspaceId(execution.workspace_id),
        run_id=RunId(execution.run_id),
        step_id=StepId(execution.step_id),
        attempt=execution.attempt,
        status=execution.state.value,
        started_at=execution.started_at,
        finished_at=execution.finished_at,
        error=snapshot,
    )


def _from_step_attempt(attempt: StepAttempt) -> ActivityExecution:
    """Reconstruct an execution from the snapshot carried in ``StepAttempt.error``."""
    if attempt.error is None:
        raise ExecutionStoreError(
            f"step attempt {attempt.run_id}/{attempt.step_id}/{attempt.attempt} "
            "is missing its ARM execution snapshot"
        )
    return ActivityExecution.model_validate(dict(attempt.error))


class ExecutionRepository:
    """``MetadataStoreProvider``-backed repository for activity executions.

    Live (non-terminal) executions are tracked in process; terminal records are
    persisted once to the append-only store for idempotent replay.
    """

    def __init__(self, store: MetadataStoreProvider, *, idempotency_ttl: timedelta) -> None:
        self._store = store
        self._idempotency_ttl = idempotency_ttl
        self._live: dict[ExecutionKey, ActivityExecution] = {}

    async def insert(self, execution: ActivityExecution) -> ActivityExecution:
        """Register a new execution, rejecting a duplicate idempotency triple."""
        key = execution.key
        if key in self._live:
            raise DuplicateExecutionError(f"execution {key} already exists")
        if await self._terminal_record(*key) is not None:
            raise DuplicateExecutionError(f"terminal execution record already exists for {key}")
        self._live[key] = execution
        if execution.is_terminal:
            await self._persist_terminal(execution)
        return execution

    async def transition(
        self, execution: ActivityExecution, to_state: ExecutionState, **changes: Any
    ) -> ActivityExecution:
        """Advance ``execution`` to ``to_state`` and persist if it becomes terminal."""
        key = execution.key
        if key not in self._live:
            raise UnknownExecutionError(f"no live execution for {key}")
        updated = execution.transition(to_state, **changes)
        self._live[key] = updated
        if updated.is_terminal:
            await self._persist_terminal(updated)
        return updated

    async def get(
        self, workspace_id: str, run_id: str, step_id: str, attempt: int
    ) -> ActivityExecution | None:
        """Return the execution for the triple, preferring the live working set."""
        key: ExecutionKey = (workspace_id, run_id, step_id, attempt)
        live = self._live.get(key)
        if live is not None:
            return live
        record = await self._terminal_record(*key)
        if record is not None:
            self._live[key] = record
        return record

    async def purge_expired(self, *, now: datetime) -> int:
        """Drop terminal live records past ``ARM_IDEMPOTENCY_TTL``; return the count."""
        expired = [
            key
            for key, execution in self._live.items()
            if execution.is_terminal
            and execution.finished_at is not None
            and execution.finished_at + self._idempotency_ttl <= now
        ]
        for key in expired:
            del self._live[key]
        return len(expired)

    async def _persist_terminal(self, execution: ActivityExecution) -> None:
        try:
            await self._store.append_step_attempt(
                WorkspaceId(execution.workspace_id),
                RunId(execution.run_id),
                StepId(execution.step_id),
                _to_step_attempt(execution),
            )
        except ImmutableViolation as exc:
            raise DuplicateExecutionError(
                f"terminal execution record already exists for {execution.key}"
            ) from exc

    async def _terminal_record(
        self, workspace_id: str, run_id: str, step_id: str, attempt: int
    ) -> ActivityExecution | None:
        attempts = await self._store.get_step_attempts(
            WorkspaceId(workspace_id), RunId(run_id), StepId(step_id)
        )
        for stored in attempts:
            if stored.attempt == attempt:
                return _from_step_attempt(stored)
        return None


__all__ = [
    "TERMINAL_STATES",
    "ActivityExecution",
    "DuplicateExecutionError",
    "ExecutionKey",
    "ExecutionRepository",
    "ExecutionState",
    "ExecutionStoreError",
    "IllegalTransitionError",
    "UnknownExecutionError",
    "allowed_transitions",
]

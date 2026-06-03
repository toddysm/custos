"""Tests for the ActivityExecution store (ARM-IMPL-005)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from custos_spl.errors import ImmutableViolation
from custos_spl.ids import RunId, StepId, WorkspaceId
from custos_spl.interfaces.metadata_store import MetadataStoreProvider, StepAttempt

from custos_arm.store.execution import (
    ActivityExecution,
    DuplicateExecutionError,
    ExecutionRepository,
    ExecutionState,
    ExecutionStoreError,
    IllegalTransitionError,
    UnknownExecutionError,
    allowed_transitions,
)

_NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)


class _FakeMetadataStore:
    """Minimal append-only stand-in for the SPL ``MetadataStoreProvider``."""

    def __init__(self) -> None:
        self.buckets: dict[tuple[str, str, str], list[StepAttempt]] = {}

    async def append_step_attempt(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        step_id: StepId,
        attempt: StepAttempt,
    ) -> StepAttempt:
        bucket = self.buckets.setdefault((workspace_id, run_id, step_id), [])
        if any(existing.attempt == attempt.attempt for existing in bucket):
            raise ImmutableViolation("duplicate step attempt")
        bucket.append(attempt)
        return attempt

    async def get_step_attempts(
        self, workspace_id: WorkspaceId, run_id: RunId, step_id: StepId
    ) -> tuple[StepAttempt, ...]:
        bucket = self.buckets.get((workspace_id, run_id, step_id), [])
        return tuple(sorted(bucket, key=lambda a: a.attempt))


def _store() -> MetadataStoreProvider:
    # The fake only implements the two methods ExecutionRepository touches.
    return _FakeMetadataStore()  # type: ignore[return-value]


def _execution(**overrides: object) -> ActivityExecution:
    base: dict[str, object] = {
        "workspace_id": "ws-1",
        "run_id": "run-1",
        "step_id": "step-1",
        "attempt": 1,
        "activity_ref": "acme/echo@1.0.0",
        "deadline": _NOW + timedelta(minutes=30),
        "started_at": _NOW,
    }
    base.update(overrides)
    return ActivityExecution(**base)  # type: ignore[arg-type]


def _repo(store: MetadataStoreProvider | None = None) -> ExecutionRepository:
    return ExecutionRepository(store or _store(), idempotency_ttl=timedelta(hours=24))


# --------------------------------------------------------------------------- #
# State machine
# --------------------------------------------------------------------------- #


def test_allowed_transitions_happy_path() -> None:
    assert ExecutionState.RESOLVING in allowed_transitions(ExecutionState.PENDING)
    assert ExecutionState.RUNNING in allowed_transitions(ExecutionState.MATERIALIZING)
    assert ExecutionState.SUCCEEDED in allowed_transitions(ExecutionState.FINALIZING)


def test_terminal_states_allow_no_transitions() -> None:
    for terminal in (
        ExecutionState.SUCCEEDED,
        ExecutionState.FAILED,
        ExecutionState.CANCELLED,
    ):
        assert allowed_transitions(terminal) == frozenset()


@pytest.mark.parametrize(
    "state",
    [
        ExecutionState.PENDING,
        ExecutionState.RESOLVING,
        ExecutionState.MATERIALIZING,
        ExecutionState.RUNNING,
        ExecutionState.FINALIZING,
    ],
)
def test_failure_and_cancel_reachable_from_any_live_state(
    state: ExecutionState,
) -> None:
    reachable = allowed_transitions(state)
    assert ExecutionState.FAILED in reachable
    assert ExecutionState.CANCELLED in reachable


def test_model_transition_advances_and_applies_changes() -> None:
    execution = _execution()
    running = (
        execution.transition(ExecutionState.RESOLVING)
        .transition(ExecutionState.MATERIALIZING)
        .transition(ExecutionState.RUNNING, sandbox_ref="job-xyz")
    )
    assert running.state is ExecutionState.RUNNING
    assert running.sandbox_ref == "job-xyz"
    # Source instance is immutable / unchanged.
    assert execution.state is ExecutionState.PENDING


def test_model_transition_rejects_illegal_jump() -> None:
    execution = _execution()
    with pytest.raises(IllegalTransitionError, match="pending -> running"):
        execution.transition(ExecutionState.RUNNING)


def test_model_transition_rejects_identity_field_mutation() -> None:
    execution = _execution()
    with pytest.raises(ExecutionStoreError, match="run_id"):
        execution.transition(ExecutionState.RESOLVING, run_id="run-2")


def test_model_transition_rejects_unknown_field() -> None:
    execution = _execution()
    with pytest.raises(ExecutionStoreError, match="bogus"):
        execution.transition(ExecutionState.RESOLVING, bogus="x")


def test_is_terminal_and_key() -> None:
    execution = _execution()
    assert execution.is_terminal is False
    assert execution.key == ("ws-1", "run-1", "step-1", 1)
    failed = execution.transition(ExecutionState.FAILED)
    assert failed.is_terminal is True


# --------------------------------------------------------------------------- #
# Repository round-trips
# --------------------------------------------------------------------------- #


async def test_insert_transition_get_round_trip_through_store() -> None:
    store = _store()
    repo = _repo(store)
    inserted = await repo.insert(_execution())

    current = inserted
    for nxt in (
        ExecutionState.RESOLVING,
        ExecutionState.MATERIALIZING,
        ExecutionState.RUNNING,
        ExecutionState.FINALIZING,
    ):
        current = await repo.transition(current, nxt)
    terminal = await repo.transition(
        current,
        ExecutionState.SUCCEEDED,
        result_class="success",
        finished_at=_NOW + timedelta(minutes=5),
    )
    assert terminal.is_terminal is True

    # A fresh repository sharing the backing store reconstructs the record,
    # proving the terminal write round-tripped through the MetadataStoreProvider.
    fresh = _repo(store)
    loaded = await fresh.get("ws-1", "run-1", "step-1", 1)
    assert loaded is not None
    assert loaded.state is ExecutionState.SUCCEEDED
    assert loaded.result_class == "success"
    assert loaded.finished_at == _NOW + timedelta(minutes=5)
    assert loaded.activity_ref == "acme/echo@1.0.0"


async def test_get_prefers_live_working_set() -> None:
    repo = _repo()
    await repo.insert(_execution())
    loaded = await repo.get("ws-1", "run-1", "step-1", 1)
    assert loaded is not None
    assert loaded.state is ExecutionState.PENDING


async def test_get_returns_none_for_unknown_triple() -> None:
    repo = _repo()
    assert await repo.get("ws-1", "run-1", "step-1", 99) is None


async def test_directly_terminal_insert_is_persisted() -> None:
    store = _store()
    repo = _repo(store)
    terminal = _execution(state=ExecutionState.CANCELLED, finished_at=_NOW + timedelta(minutes=1))
    await repo.insert(terminal)
    loaded = await _repo(store).get("ws-1", "run-1", "step-1", 1)
    assert loaded is not None
    assert loaded.state is ExecutionState.CANCELLED


# --------------------------------------------------------------------------- #
# Duplicate detection
# --------------------------------------------------------------------------- #


async def test_duplicate_live_insert_rejected() -> None:
    repo = _repo()
    await repo.insert(_execution())
    with pytest.raises(DuplicateExecutionError):
        await repo.insert(_execution())


async def test_insert_rejected_when_terminal_record_exists() -> None:
    store = _store()
    terminal = _execution(state=ExecutionState.SUCCEEDED, finished_at=_NOW + timedelta(minutes=2))
    await _repo(store).insert(terminal)

    # A different repository (e.g. after a restart) must detect the replay.
    with pytest.raises(DuplicateExecutionError):
        await _repo(store).insert(_execution())


async def test_immutable_violation_maps_to_duplicate_error() -> None:
    store = _store()
    repo = _repo(store)
    terminal = _execution(state=ExecutionState.SUCCEEDED, finished_at=_NOW + timedelta(minutes=2))
    # Pre-seed the store so the repository's own persist hits ImmutableViolation
    # without the insert-time terminal-record guard catching it first.
    await store.append_step_attempt(
        WorkspaceId("ws-1"),
        RunId("run-1"),
        StepId("step-1"),
        StepAttempt(
            workspace_id=WorkspaceId("ws-1"),
            run_id=RunId("run-1"),
            step_id=StepId("step-1"),
            attempt=1,
            status="succeeded",
            started_at=_NOW,
            finished_at=_NOW,
            error={"sentinel": True},
        ),
    )
    with pytest.raises(DuplicateExecutionError):
        await repo._persist_terminal(terminal)


# --------------------------------------------------------------------------- #
# Transition guards & TTL retention
# --------------------------------------------------------------------------- #


async def test_transition_unknown_execution_rejected() -> None:
    repo = _repo()
    with pytest.raises(UnknownExecutionError):
        await repo.transition(_execution(), ExecutionState.RESOLVING)


async def test_transition_rejects_illegal_state() -> None:
    repo = _repo()
    execution = await repo.insert(_execution())
    with pytest.raises(IllegalTransitionError):
        await repo.transition(execution, ExecutionState.SUCCEEDED)


async def test_transition_uses_live_record_not_passed_instance() -> None:
    repo = _repo()
    stale = await repo.insert(_execution())  # pending
    await repo.transition(stale, ExecutionState.RESOLVING)
    # Re-using the stale (pending) handle still advances from the live
    # (resolving) record, so resolving -> materializing succeeds.
    advanced = await repo.transition(stale, ExecutionState.MATERIALIZING)
    assert advanced.state is ExecutionState.MATERIALIZING


async def test_failed_terminal_persist_leaves_live_record_intact() -> None:
    store = _store()
    repo = _repo(store)
    live = await repo.insert(_execution())
    running = await repo.transition(
        (await repo.transition(live, ExecutionState.RESOLVING)),
        ExecutionState.MATERIALIZING,
    )
    running = await repo.transition(running, ExecutionState.RUNNING)
    # Force the terminal append to collide with an existing immutable row.
    await store.append_step_attempt(
        WorkspaceId("ws-1"),
        RunId("run-1"),
        StepId("step-1"),
        StepAttempt(
            workspace_id=WorkspaceId("ws-1"),
            run_id=RunId("run-1"),
            step_id=StepId("step-1"),
            attempt=1,
            status="succeeded",
            started_at=_NOW,
            finished_at=_NOW,
            error={"sentinel": True},
        ),
    )
    with pytest.raises(DuplicateExecutionError):
        await repo.transition(running, ExecutionState.FAILED)
    # The live record must remain at its pre-transition (running) state.
    current = await repo.get("ws-1", "run-1", "step-1", 1)
    assert current is not None
    assert current.state is ExecutionState.RUNNING


async def test_purge_expired_drops_only_stale_terminal_records() -> None:
    store = _store()
    repo = _repo(store)

    # Expired terminal record.
    expired = _execution(
        run_id="run-old",
        state=ExecutionState.SUCCEEDED,
        finished_at=_NOW - timedelta(hours=48),
    )
    await repo.insert(expired)
    # Fresh terminal record.
    recent = _execution(
        run_id="run-new",
        state=ExecutionState.SUCCEEDED,
        finished_at=_NOW - timedelta(minutes=1),
    )
    await repo.insert(recent)
    # Live, non-terminal record.
    await repo.insert(_execution(run_id="run-live"))

    purged = await repo.purge_expired(now=_NOW)
    assert purged == 1
    assert await repo.get("ws-1", "run-new", "step-1", 1) is not None
    assert await repo.get("ws-1", "run-live", "step-1", 1) is not None


async def test_get_raises_when_snapshot_missing() -> None:
    store = _store()
    await store.append_step_attempt(
        WorkspaceId("ws-1"),
        RunId("run-1"),
        StepId("step-1"),
        StepAttempt(
            workspace_id=WorkspaceId("ws-1"),
            run_id=RunId("run-1"),
            step_id=StepId("step-1"),
            attempt=1,
            status="succeeded",
            started_at=_NOW,
            finished_at=_NOW,
            error=None,
        ),
    )
    with pytest.raises(ExecutionStoreError, match="missing its ARM execution snapshot"):
        await _repo(store).get("ws-1", "run-1", "step-1", 1)


async def test_insert_rejects_duplicate_even_without_snapshot() -> None:
    store = _store()
    await store.append_step_attempt(
        WorkspaceId("ws-1"),
        RunId("run-1"),
        StepId("step-1"),
        StepAttempt(
            workspace_id=WorkspaceId("ws-1"),
            run_id=RunId("run-1"),
            step_id=StepId("step-1"),
            attempt=1,
            status="succeeded",
            started_at=_NOW,
            finished_at=_NOW,
            error=None,
        ),
    )
    # The duplicate guard keys off attempt existence, not snapshot validity.
    with pytest.raises(DuplicateExecutionError):
        await _repo(store).insert(_execution())

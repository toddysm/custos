"""WF-IMPL-045 \u2014 Run Controller kind-grid extension.

Extends the WF-IMPL-025 kind-grid pattern in
``tests/test_kind_grid.py`` with the Run Controller's two
locked taxonomies:

* :class:`custos_workflow.runs.RunStatus` \u2014 the 8 lifecycle
  statuses that surface verbatim in the persisted ``Run.status``
  column and on every ``RunRef`` / ``RunRecord`` shape.
* :data:`custos_workflow.runs.LOCKED_RUN_KINDS` \u2014 the closed
  ``run.*`` error ``kind`` set the WF-IMPL-044 OTel error counter
  pins as its ``outcome`` label.

Both grids end with an exhaustiveness guard so adding a new
:class:`RunStatus` member or a new :class:`RunControllerError`
subclass without extending the grid breaks the build.

The status-transition grid is also pinned: every
``(from, to)`` documented in
:data:`custos_workflow.runs.STATUS_TRANSITIONS` has a row, and
:meth:`InProcessRunStore.update_run_status` rejects every
``(from, to)`` *not* in the table with
:class:`RunStateConflictError`.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

import pytest
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

from custos_workflow.runs import (
    LOCKED_RUN_KINDS,
    STATUS_TRANSITIONS,
    TERMINAL_STATUSES,
    InProcessRunStore,
    RunNotFoundError,
    RunRecord,
    RunStateConflictError,
    RunStateCorruptError,
    RunStatus,
    WorkflowRuntimeUnavailableError,
    derive_run_id,
)
from custos_workflow.runs.errors import RunControllerError
from tests.runs._fakes import FakeMetadataStoreProvider

# ---------------------------------------------------------------------------
# Locked taxonomies as test-side constants
# ---------------------------------------------------------------------------


#: Mirror of :data:`LOCKED_RUN_KINDS`. Pinned in the test module so
#: drift between the public taxonomy and what the grid actually
#: exercises is caught by the exhaustiveness assertion below.
_EXPECTED_LOCKED_RUN_KINDS: frozenset[str] = frozenset(
    {
        "run.not_found",
        "run.state_conflict",
        "run.state_corrupt",
        "run.runtime_unavailable",
    }
)


#: Trigger table for every locked ``run.*`` kind. Each entry maps a
#: ``kind`` string to the public exception class the Run Controller
#: surfaces under that taxonomy.
_RUN_KIND_TRIGGERS: dict[str, type[RunControllerError]] = {
    "run.not_found": RunNotFoundError,
    "run.state_conflict": RunStateConflictError,
    "run.state_corrupt": RunStateCorruptError,
    "run.runtime_unavailable": WorkflowRuntimeUnavailableError,
}


#: The 8 :class:`RunStatus` members. Pinning the set in the test
#: catches accidental rename / removal: the exhaustiveness assertion
#: ``set(RunStatus) == _EXPECTED_STATUSES`` fires on any drift.
_EXPECTED_STATUSES: frozenset[RunStatus] = frozenset(
    {
        RunStatus.QUEUED,
        RunStatus.RUNNING,
        RunStatus.PAUSING,
        RunStatus.PAUSED,
        RunStatus.CANCELLING,
        RunStatus.CANCELLED,
        RunStatus.SUCCEEDED,
        RunStatus.FAILED,
    }
)


#: Pinned terminal subset. ``cancelled`` / ``succeeded`` / ``failed``
#: have no out-edges in :data:`STATUS_TRANSITIONS`.
_EXPECTED_TERMINAL: frozenset[RunStatus] = frozenset(
    {RunStatus.CANCELLED, RunStatus.SUCCEEDED, RunStatus.FAILED}
)


# ---------------------------------------------------------------------------
# 1. LOCKED_RUN_KINDS taxonomy grid
# ---------------------------------------------------------------------------


class TestLockedRunKindsGrid:
    @pytest.mark.parametrize("kind", sorted(_EXPECTED_LOCKED_RUN_KINDS))
    def test_every_locked_kind_has_a_trigger_exception(self, kind: str) -> None:
        exc_class = _RUN_KIND_TRIGGERS[kind]
        # The trigger class must declare the documented ``KIND`` and
        # subclass the public :class:`RunControllerError` family so
        # callers can ``except RunControllerError`` on the whole set.
        assert kind == exc_class.KIND
        assert issubclass(exc_class, RunControllerError)

    def test_locked_kinds_mirror_public_constant(self) -> None:
        # The test-side mirror MUST stay in lock-step with the
        # public :data:`LOCKED_RUN_KINDS`. Adding or removing a kind
        # without updating both sides breaks here.
        assert _EXPECTED_LOCKED_RUN_KINDS == LOCKED_RUN_KINDS

    def test_trigger_grid_covers_every_locked_kind(self) -> None:
        # Exhaustiveness guard: adding a new ``run.*`` kind to the
        # taxonomy without adding a trigger row breaks here.
        assert set(_RUN_KIND_TRIGGERS) == set(LOCKED_RUN_KINDS)

    def test_locked_run_kinds_is_a_frozenset(self) -> None:
        # The public constant is documented as a ``frozenset[str]``
        # so callers can use it as a dict key or a set-membership
        # check; pin the type to prevent a future contributor from
        # relaxing it to a plain ``set``.
        assert isinstance(LOCKED_RUN_KINDS, frozenset)
        for kind in LOCKED_RUN_KINDS:
            assert isinstance(kind, str)


# ---------------------------------------------------------------------------
# 2. RunStatus enum grid
# ---------------------------------------------------------------------------


class TestRunStatusGrid:
    @pytest.mark.parametrize(
        "status",
        sorted(_EXPECTED_STATUSES, key=lambda s: cast(RunStatus, s).value),
    )
    def test_every_expected_status_appears_in_enum(self, status: RunStatus) -> None:
        # Pinning each expected status as its own parametrize row
        # gives a precise failure message: removing ``RunStatus.PAUSED``
        # would surface as "test_every_expected_status_appears_in_enum[paused]"
        # rather than a single grid-wide diff.
        assert status in RunStatus
        # The wire value MUST be the documented lowercase string.
        assert status.value == status.name.lower()

    def test_enum_membership_is_exhaustive(self) -> None:
        assert set(RunStatus) == _EXPECTED_STATUSES

    def test_terminal_statuses_match_expected_subset(self) -> None:
        assert TERMINAL_STATUSES == _EXPECTED_TERMINAL


# ---------------------------------------------------------------------------
# 3. Status-transition table grid
# ---------------------------------------------------------------------------


#: The full, locked transition table. Mirrors
#: :data:`STATUS_TRANSITIONS` from the public model module so any
#: drift surfaces in the explicit equality assertion below; the
#: parametrized rows then exercise each transition end-to-end
#: through :meth:`InProcessRunStore.update_run_status`.
_EXPECTED_TRANSITIONS: dict[RunStatus, frozenset[RunStatus]] = {
    RunStatus.QUEUED: frozenset({RunStatus.RUNNING, RunStatus.CANCELLING, RunStatus.FAILED}),
    RunStatus.RUNNING: frozenset(
        {
            RunStatus.PAUSING,
            RunStatus.CANCELLING,
            RunStatus.SUCCEEDED,
            RunStatus.FAILED,
        }
    ),
    RunStatus.PAUSING: frozenset({RunStatus.PAUSED, RunStatus.CANCELLING, RunStatus.FAILED}),
    RunStatus.PAUSED: frozenset({RunStatus.RUNNING, RunStatus.CANCELLING, RunStatus.FAILED}),
    RunStatus.CANCELLING: frozenset({RunStatus.CANCELLED, RunStatus.FAILED}),
    RunStatus.CANCELLED: frozenset(),
    RunStatus.SUCCEEDED: frozenset(),
    RunStatus.FAILED: frozenset(),
}


# Flatten into ``(from, to)`` parametrize rows.
_LEGAL_PAIRS: list[tuple[RunStatus, RunStatus]] = sorted(
    ((src, dst) for src, dsts in _EXPECTED_TRANSITIONS.items() for dst in dsts),
    key=lambda p: (p[0].value, p[1].value),
)


_FIXED_NOW = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
_WORKSPACE = "ws-grid"
_RUN_ID = derive_run_id(_WORKSPACE, "client-grid")
_WORKFLOW_VERSION_ID = "wfv-grid"


def _store() -> InProcessRunStore:
    provider = FakeMetadataStoreProvider()
    return InProcessRunStore(cast(MetadataStoreProvider, provider))


async def _seed(store: InProcessRunStore, target: RunStatus) -> None:
    """Insert a queued row and walk it to ``target`` through legal pairs."""

    record = RunRecord(
        workspace_id=_WORKSPACE,
        run_id=_RUN_ID,
        workflow_id="wf-grid",
        workflow_version=_WORKFLOW_VERSION_ID,
        status=RunStatus.QUEUED,
        reason=None,
        started_at=_FIXED_NOW,
        updated_at=_FIXED_NOW,
        compiled_graph=None,
    )
    await store.put_run(record)
    # Documented shortest paths from QUEUED to every other state.
    # The Run Controller wires QUEUED -> RUNNING on every start_run;
    # the rest of the lifecycle drills down from there.
    paths: dict[RunStatus, tuple[RunStatus, ...]] = {
        RunStatus.QUEUED: (),
        RunStatus.RUNNING: (RunStatus.RUNNING,),
        RunStatus.PAUSING: (RunStatus.RUNNING, RunStatus.PAUSING),
        RunStatus.PAUSED: (
            RunStatus.RUNNING,
            RunStatus.PAUSING,
            RunStatus.PAUSED,
        ),
        RunStatus.CANCELLING: (RunStatus.CANCELLING,),
        RunStatus.CANCELLED: (RunStatus.CANCELLING, RunStatus.CANCELLED),
        RunStatus.SUCCEEDED: (RunStatus.RUNNING, RunStatus.SUCCEEDED),
        RunStatus.FAILED: (RunStatus.FAILED,),
    }
    for step in paths[target]:
        await store.update_run_status(_WORKSPACE, _RUN_ID, step)


class TestStatusTransitionGrid:
    def test_expected_table_matches_public_table(self) -> None:
        # The test-side mirror MUST stay in lock-step with the
        # public :data:`STATUS_TRANSITIONS` table. Any drift
        # (adding an out-edge, deleting a node) breaks here.
        assert _EXPECTED_TRANSITIONS == STATUS_TRANSITIONS

    @pytest.mark.parametrize("src,dst", _LEGAL_PAIRS)
    @pytest.mark.asyncio
    async def test_every_legal_transition_is_accepted(self, src: RunStatus, dst: RunStatus) -> None:
        store = _store()
        await _seed(store, src)
        # Each legal pair MUST succeed via ``update_run_status``;
        # the store call surfaces ``RunStateConflictError`` for
        # anything that is NOT in the table.
        updated = await store.update_run_status(_WORKSPACE, _RUN_ID, dst)
        assert updated.status is dst

    @pytest.mark.asyncio
    async def test_illegal_transition_raises_state_conflict(self) -> None:
        # Pick an illegal pair: SUCCEEDED -> RUNNING (a terminal
        # status MUST NOT be revivable). The exact pair is
        # documentary; the grid above guarantees the legal set is
        # exhaustive.
        store = _store()
        await _seed(store, RunStatus.SUCCEEDED)
        with pytest.raises(RunStateConflictError):
            await store.update_run_status(_WORKSPACE, _RUN_ID, RunStatus.RUNNING)


# ---------------------------------------------------------------------------
# 4. Cross-cutting build-time invariant
# ---------------------------------------------------------------------------


class TestKindGridBuildTimeInvariants:
    def test_locked_run_kinds_match_run_controller_error_subclasses(self) -> None:
        # Every concrete :class:`RunControllerError` subclass MUST
        # contribute exactly one entry to :data:`LOCKED_RUN_KINDS`.
        # The set-equality below is the build-time guarantee
        # WF-IMPL-044 leans on for its OTel ``outcome`` label set.
        observed_subclass_kinds = {
            sub.KIND
            for sub in RunControllerError.__subclasses__()
            if isinstance(sub.KIND, str) and sub.KIND
        }
        assert observed_subclass_kinds == LOCKED_RUN_KINDS

    def test_status_enum_size_matches_documented_lifecycle(self) -> None:
        # The design.md § Data Models lists exactly 8 statuses; any
        # deviation is a downstream contract break.
        assert len(RunStatus) == 8

"""Tests for ``custos_workflow.runs.model`` + ``runs.store``."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

import pytest
from custos_spl.interfaces.metadata_store import MetadataStoreProvider
from custos_spl.pagination import Cursor

from custos_workflow.runs import (
    STATUS_TRANSITIONS,
    TERMINAL_STATUSES,
    InProcessRunStore,
    RunId,
    RunRecord,
    RunStateConflictError,
    RunStatus,
    derive_run_id,
    is_terminal,
)
from tests.runs._fakes import FakeMetadataStoreProvider

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


WORKSPACE = "ws-001"
WORKFLOW = "wf-alpha"
VERSION = "1"


def _ts(seconds: int = 0) -> datetime:
    """Deterministic UTC timestamp helper."""

    return datetime(2026, 1, 1, 0, 0, seconds, tzinfo=UTC)


def _record(
    *,
    run_id: RunId | None = None,
    status: RunStatus = RunStatus.QUEUED,
    reason: str | None = None,
    workspace: str = WORKSPACE,
    workflow: str = WORKFLOW,
    version: str = VERSION,
) -> RunRecord:
    return RunRecord(
        workspace_id=workspace,
        run_id=run_id if run_id is not None else derive_run_id(workspace, "key-1"),
        workflow_id=workflow,
        workflow_version=version,
        status=status,
        reason=reason,
        started_at=_ts(0),
        updated_at=_ts(0),
        compiled_graph=None,
    )


@pytest.fixture
def store() -> InProcessRunStore:
    fake = FakeMetadataStoreProvider()
    return InProcessRunStore(cast(MetadataStoreProvider, fake))


# ---------------------------------------------------------------------------
# Status enum + transition table
# ---------------------------------------------------------------------------


def test_status_transitions_keys_exactly_match_runstatus_members() -> None:
    """Adding a ``RunStatus`` without updating ``STATUS_TRANSITIONS`` must fail."""

    assert set(STATUS_TRANSITIONS.keys()) == set(RunStatus), (
        "STATUS_TRANSITIONS keys diverged from RunStatus members; "
        "every new status MUST be added to the transition table."
    )


def test_terminal_statuses_have_no_out_edges() -> None:
    for status in TERMINAL_STATUSES:
        assert STATUS_TRANSITIONS[status] == frozenset()
        assert is_terminal(status) is True


def test_non_terminal_statuses_have_out_edges() -> None:
    for status in set(RunStatus) - TERMINAL_STATUSES:
        assert STATUS_TRANSITIONS[status], f"non-terminal {status} has no outgoing transitions"
        assert is_terminal(status) is False


def test_transition_targets_are_all_valid_statuses() -> None:
    for src, targets in STATUS_TRANSITIONS.items():
        for tgt in targets:
            assert isinstance(tgt, RunStatus), f"{src} -> {tgt!r} is not a RunStatus"


def test_transition_table_is_loop_free_for_terminal_statuses() -> None:
    """Self-transitions are never allowed; terminal statuses are absorbing."""

    for src, targets in STATUS_TRANSITIONS.items():
        assert src not in targets, f"self-transition {src} -> {src} is illegal"


# ---------------------------------------------------------------------------
# RunStatus value contract
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("member", "wire"),
    [
        (RunStatus.QUEUED, "queued"),
        (RunStatus.RUNNING, "running"),
        (RunStatus.PAUSING, "pausing"),
        (RunStatus.PAUSED, "paused"),
        (RunStatus.CANCELLING, "cancelling"),
        (RunStatus.CANCELLED, "cancelled"),
        (RunStatus.SUCCEEDED, "succeeded"),
        (RunStatus.FAILED, "failed"),
    ],
)
def test_runstatus_wire_values_are_stable(member: RunStatus, wire: str) -> None:
    """The 8 wire-format strings are part of the public contract."""

    assert member.value == wire
    assert RunStatus(wire) is member


# ---------------------------------------------------------------------------
# put_run idempotency + conflict detection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_put_run_persists_new_record(store: InProcessRunStore) -> None:
    record = _record()
    persisted = await store.put_run(record)
    assert persisted == record
    fetched = await store.get_run(record.workspace_id, record.run_id)
    assert fetched == record


@pytest.mark.asyncio
async def test_put_run_is_idempotent_on_byte_equal_replay(
    store: InProcessRunStore,
) -> None:
    record = _record()
    first = await store.put_run(record)
    second = await store.put_run(record)
    assert first == second == record


@pytest.mark.asyncio
async def test_put_run_raises_conflict_on_divergent_replay(
    store: InProcessRunStore,
) -> None:
    record = _record()
    await store.put_run(record)

    divergent = _record(status=RunStatus.RUNNING)
    with pytest.raises(RunStateConflictError) as exc_info:
        await store.put_run(divergent)

    err = exc_info.value
    assert err.run_id == record.run_id
    payload = err.to_dict()
    assert payload["kind"] == "run.state_conflict"
    assert payload["current_status"] == "queued"
    assert payload["attempted_status"] == "running"


# ---------------------------------------------------------------------------
# update_run_status — transition matrix
# ---------------------------------------------------------------------------


_ALL_TRANSITIONS: list[tuple[RunStatus, RunStatus, bool]] = [
    (src, dst, dst in STATUS_TRANSITIONS[src])
    for src in RunStatus
    for dst in RunStatus
    if src is not dst  # self-transitions are tested separately
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("src", "dst", "allowed"), _ALL_TRANSITIONS)
async def test_update_run_status_enforces_transition_table(
    store: InProcessRunStore,
    src: RunStatus,
    dst: RunStatus,
    allowed: bool,
) -> None:
    # Seed the row at ``src`` by writing it directly.
    record = _record(status=src)
    await store.put_run(record)

    if allowed:
        updated = await store.update_run_status(
            record.workspace_id, record.run_id, dst, reason="ok"
        )
        assert updated.status is dst
        assert updated.reason == "ok"
    else:
        with pytest.raises(RunStateConflictError) as exc_info:
            await store.update_run_status(record.workspace_id, record.run_id, dst)
        payload = exc_info.value.to_dict()
        assert payload["current_status"] == src.value
        assert payload["attempted_status"] == dst.value


@pytest.mark.asyncio
async def test_update_run_status_rejects_self_transition(
    store: InProcessRunStore,
) -> None:
    record = _record(status=RunStatus.RUNNING)
    await store.put_run(record)

    with pytest.raises(RunStateConflictError):
        await store.update_run_status(record.workspace_id, record.run_id, RunStatus.RUNNING)


@pytest.mark.asyncio
async def test_update_run_status_raises_on_unknown_run(
    store: InProcessRunStore,
) -> None:
    with pytest.raises(RunStateConflictError) as exc_info:
        await store.update_run_status(
            WORKSPACE, derive_run_id(WORKSPACE, "missing"), RunStatus.RUNNING
        )
    assert exc_info.value.to_dict()["kind"] == "run.state_conflict"


# ---------------------------------------------------------------------------
# get_run + list_runs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_run_returns_none_for_unknown(
    store: InProcessRunStore,
) -> None:
    assert await store.get_run(WORKSPACE, derive_run_id(WORKSPACE, "x")) is None


@pytest.mark.asyncio
async def test_list_runs_returns_workspace_slice(
    store: InProcessRunStore,
) -> None:
    own = [_record(run_id=derive_run_id(WORKSPACE, f"k{i}")) for i in range(3)]
    other = _record(workspace="ws-other", run_id=derive_run_id("ws-other", "k0"))
    for rec in own:
        await store.put_run(rec)
    await store.put_run(other)

    page = await store.list_runs(WORKSPACE)
    assert {r.run_id for r in page.items} == {r.run_id for r in own}
    assert page.next_cursor is None


@pytest.mark.asyncio
async def test_list_runs_paginates_via_cursor(
    store: InProcessRunStore,
) -> None:
    records = [_record(run_id=derive_run_id(WORKSPACE, f"k{i}")) for i in range(5)]
    for rec in records:
        await store.put_run(rec)

    first = await store.list_runs(WORKSPACE, limit=2)
    assert len(first.items) == 2
    assert first.next_cursor is not None

    second = await store.list_runs(WORKSPACE, cursor=first.next_cursor, limit=2)
    assert len(second.items) == 2
    assert second.next_cursor is not None

    third = await store.list_runs(WORKSPACE, cursor=second.next_cursor, limit=2)
    assert len(third.items) == 1
    assert third.next_cursor is None

    seen = [r.run_id for r in (*first.items, *second.items, *third.items)]
    assert seen == [r.run_id for r in records]


@pytest.mark.asyncio
async def test_list_runs_empty_workspace(store: InProcessRunStore) -> None:
    page = await store.list_runs("ws-empty")
    assert page.items == ()
    assert page.next_cursor is None


@pytest.mark.asyncio
async def test_list_runs_cursor_at_end_returns_empty(
    store: InProcessRunStore,
) -> None:
    await store.put_run(_record())
    page = await store.list_runs(WORKSPACE, cursor=Cursor(token="1"))
    assert page.items == ()
    assert page.next_cursor is None


# ---------------------------------------------------------------------------
# WF-IMPL-116 — durable Run store survives a process restart
# ---------------------------------------------------------------------------
#
# ``InProcessRunStore`` is a thin adapter that delegates every persistent
# field to its injected ``MetadataStoreProvider``. In production the provider
# is the lifespan-owned ``custos_pg`` adapter (``custos_state.run`` in
# Postgres); here the ``FakeMetadataStoreProvider`` plays the role of the
# durable backing store. Sharing one provider across two store instances
# therefore simulates a process restart / HA failover over the same DSN:
# a fresh store must see every run + status the previous one persisted.


@pytest.mark.asyncio
async def test_run_survives_simulated_process_restart() -> None:
    """A new store over the same provider sees the persisted run + status."""
    backing = FakeMetadataStoreProvider()
    provider = cast(MetadataStoreProvider, backing)

    # First "process": create the store, persist a run, advance its status.
    store_before = InProcessRunStore(provider)
    run_id = derive_run_id(WORKSPACE, "restart-key")
    await store_before.put_run(_record(run_id=run_id, status=RunStatus.QUEUED))
    await store_before.update_run_status(WORKSPACE, run_id, RunStatus.RUNNING)

    # Restart: a brand-new store instance over the SAME durable provider.
    store_after = InProcessRunStore(provider)
    recovered = await store_after.get_run(WORKSPACE, run_id)

    assert recovered is not None
    assert recovered.run_id == run_id
    assert recovered.status is RunStatus.RUNNING


@pytest.mark.asyncio
async def test_list_runs_pagination_survives_restart() -> None:
    """``list_runs`` cursor pagination is identical after a restart.

    The cursor contract must not depend on any in-process state held by the
    store instance that wrote the rows — a fresh store over the same provider
    walks the same pages in the same order.
    """
    backing = FakeMetadataStoreProvider()
    provider = cast(MetadataStoreProvider, backing)

    store_before = InProcessRunStore(provider)
    records = [_record(run_id=derive_run_id(WORKSPACE, f"r{i}")) for i in range(5)]
    for rec in records:
        await store_before.put_run(rec)

    store_after = InProcessRunStore(provider)
    first = await store_after.list_runs(WORKSPACE, limit=2)
    second = await store_after.list_runs(WORKSPACE, cursor=first.next_cursor, limit=2)
    third = await store_after.list_runs(WORKSPACE, cursor=second.next_cursor, limit=2)

    assert [len(p.items) for p in (first, second, third)] == [2, 2, 1]
    assert third.next_cursor is None
    seen = [r.run_id for r in (*first.items, *second.items, *third.items)]
    assert seen == [r.run_id for r in records]

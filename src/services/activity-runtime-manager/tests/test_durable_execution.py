"""Durable attempt replay/ownership through the real repository and scheduler."""

import asyncio
import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest
from custos_spl.errors import ImmutableViolation, LeaseBusy, LeaseExpired
from custos_spl.interfaces.activity_execution import (
    ActivityExecutionRecord,
    activity_snapshot,
    validate_activity_claim,
)
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

from custos_arm.result import ResultClass
from custos_arm.scheduler import ActivityScheduler
from custos_arm.store import ExecutionRepository
from tests.test_scheduler import (
    _FakeDriver,
    _FakeMetadataStore,
    _request,
    _scheduler,
    _success_outputs,
)


class SharedAttempts:
    """Atomic external-store double shared across distinct process/repository instances."""

    def __init__(self) -> None:
        """Keep durable rows outside any repository's live/cache dictionaries."""
        self.rows: dict[tuple[str, str, str, int], ActivityExecutionRecord] = {}
        self.lock = asyncio.Lock()

    async def check_activity_execution_ready(self) -> None:
        """The explicit test capability has no external migration dependency."""

    async def claim_activity_execution(
        self,
        workspace_id: str,
        run_id: str,
        step_id: str,
        attempt: int,
        request_hash: str,
        owner: str,
        snapshot_json: str,
    ) -> ActivityExecutionRecord:
        """Match provider claim, step fence and immutable request semantics atomically."""
        validate_activity_claim(workspace_id, run_id, step_id, attempt, request_hash, owner)
        key = (workspace_id, run_id, step_id, attempt)
        async with self.lock:
            if key in self.rows:
                row = self.rows[key]
                if row.request_hash != request_hash:
                    raise ImmutableViolation("different request")
                return row
            for other, row in self.rows.items():
                if other[:3] == key[:3] and (
                    not row.completed or json.loads(row.snapshot_json)["state"] == "succeeded"
                ):
                    raise LeaseBusy("unresolved or successful earlier attempt")
            row = ActivityExecutionRecord(
                *key, request_hash, owner, activity_snapshot(snapshot_json), False
            )
            self.rows[key] = row
            return row

    async def read_activity_execution(
        self, workspace_id: str, run_id: str, step_id: str, attempt: int
    ) -> ActivityExecutionRecord | None:
        """Return the retained row, never the caller's in-memory execution state."""
        return self.rows.get((workspace_id, run_id, step_id, attempt))

    async def write_activity_execution(
        self, record: ActivityExecutionRecord, snapshot_json: str, *, completed: bool
    ) -> ActivityExecutionRecord:
        """Reject stale owners/snapshots and any overwrite of a terminal result."""
        key = (record.workspace_id, record.run_id, record.step_id, record.attempt)
        async with self.lock:
            if self.rows.get(key) != record or record.completed:
                raise LeaseExpired("stale owner or state")
            row = replace(
                record, snapshot_json=activity_snapshot(snapshot_json), completed=completed
            )
            self.rows[key] = row
            return row


def repository(store: SharedAttempts) -> ExecutionRepository:
    """Create an independent runtime process over shared durable rows."""
    return ExecutionRepository(
        cast(MetadataStoreProvider, _FakeMetadataStore()),
        idempotency_ttl=timedelta(days=31),
        durable_store=store,
    )


async def test_restart_replays_exact_outputs_without_second_execution(tmp_path: Path) -> None:
    """A fresh repository/scheduler returns the persisted result, not an empty success."""
    store = SharedAttempts()
    request = _request()
    first = await _scheduler(
        _FakeDriver(tmp_path / "first", outputs=_success_outputs({"nested": {"answer": 42}})),
        repo=repository(store),
    ).schedule(request)
    assert first.class_ is ResultClass.SUCCESS
    driver = _FakeDriver(tmp_path / "restart", outputs=None)
    replay = await _scheduler(driver, repo=repository(store)).schedule(request)
    assert replay == first and replay.outputs == {"nested": {"answer": 42}}
    assert driver.prepared == 0


async def test_pending_owner_blocks_restart_and_later_attempt(tmp_path: Path) -> None:
    """A process crash or transport timeout cannot turn pending ownership into a new effect."""
    from custos_arm.store import ActivityExecution
    from tests.test_scheduler import _NOW

    store = SharedAttempts()
    request = _request()
    first = repository(store)
    await first.insert(
        ActivityExecution(
            workspace_id=request.workspace_id,
            run_id=request.step.run_id,
            step_id=request.step.step_id,
            attempt=1,
            activity_ref=request.activity_ref,
            deadline=_NOW + timedelta(hours=1),
            started_at=_NOW,
            request_hash=ActivityScheduler._request_hash(request),
        )
    )
    for candidate in (
        request,
        replace(request, step=request.step.model_copy(update={"attempt": 2})),
    ):
        driver = _FakeDriver(tmp_path / str(candidate.step.attempt), outputs=None)
        result = await _scheduler(driver, repo=repository(store)).schedule(candidate)
        assert result.class_ is ResultClass.PERMANENT
        assert result.error is not None and result.error.code == "activity.execution_unconfirmed"
        assert driver.prepared == 0
    assert len(store.rows) == 1 and not next(iter(store.rows.values())).completed


async def test_replay_cannot_change_inputs_or_caller(tmp_path: Path) -> None:
    """Neither stale credentials nor a changed request can acquire a prior result."""
    store = SharedAttempts()
    request = _request(principal_id="original")
    await _scheduler(
        _FakeDriver(tmp_path / "first", outputs=_success_outputs()), repo=repository(store)
    ).schedule(request)
    for changed in (
        replace(request, inputs={"different": True}),
        replace(request, principal_id="other"),
    ):
        driver = _FakeDriver(tmp_path / "changed", outputs=None)
        result = await _scheduler(driver, repo=repository(store)).schedule(changed)
        assert result.class_ is ResultClass.PERMANENT and driver.prepared == 0


async def test_terminal_snapshot_is_immutable(tmp_path: Path) -> None:
    """Even the original owner cannot replace an acknowledged terminal result."""
    store = SharedAttempts()
    await _scheduler(
        _FakeDriver(tmp_path, outputs=_success_outputs()), repo=repository(store)
    ).schedule(_request())
    row = next(iter(store.rows.values()))
    with pytest.raises(LeaseExpired):
        await store.write_activity_execution(row, row.snapshot_json, completed=True)

"""Unknown external effects retain ownership but must lose credential admission."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from custos_spl.errors import BackendUnavailable
from custos_spl.interfaces.activity_execution import ActivityExecutionRecord

from custos_arm.runtime import CancelReason, SandboxHandle, SandboxOutcome, SandboxSignal
from custos_arm.runtime.oci import SandboxFailureError
from custos_arm.scheduler import ActivityScheduler, ScheduleRequest
from custos_arm.scheduler.scheduler import _AttemptContext
from custos_arm.store import ActivityExecution, ExecutionState
from tests.test_durable_execution import SharedAttempts, repository
from tests.test_scheduler import (
    _FakeDriver,
    _request,
    _resolved,
    _scheduler,
)


class FailingDriver(_FakeDriver):
    """Keep normal driver bookkeeping while injecting start or monitoring uncertainty."""

    def __init__(self, root: Path, phase: str) -> None:
        """Select the external failure; all monitoring attempts remain unconfirmed."""
        super().__init__(root, outputs=None, await_error=SandboxFailureError("private-monitor"))
        self.phase = phase

    def start(self, handle: SandboxHandle) -> None:
        """Model a lost start response without asserting that the external effect did not run."""
        super().start(handle)
        if self.phase == "start":
            raise SandboxFailureError("private-start")


async def retained_attempt(
    root: Path,
) -> tuple[ActivityScheduler, SharedAttempts, FailingDriver, ScheduleRequest]:
    """Seed a real repository's running state with a context but no request worker."""
    store = SharedAttempts()
    driver = FailingDriver(root, "monitor")
    scheduler = _scheduler(driver, repo=repository(store))
    request = _request()
    now = datetime.now(UTC)
    await scheduler._repo.insert(
        ActivityExecution(
            workspace_id=request.workspace_id,
            run_id=request.step.run_id,
            step_id=request.step.step_id,
            attempt=1,
            activity_ref=request.activity_ref,
            started_at=now,
            deadline=now + timedelta(minutes=5),
            request_hash=scheduler._request_hash(request),
        )
    )
    key = scheduler._key(request)
    await scheduler._ensure(key, ExecutionState.RUNNING)
    handle = SandboxHandle(
        kind=driver.kind,
        reference="namespace/job",
        input_root=root / "in",
        output_root=root / "out",
    )
    scheduler._context[key] = _AttemptContext(
        handle=handle,
        resolved=_resolved(),
        driver=driver,
        deadline=now + timedelta(minutes=5),
    )
    return scheduler, store, driver, request


@pytest.mark.parametrize("phase", ["start", "monitor"])
async def test_uncertain_start_or_monitor_revokes_before_returning(
    tmp_path: Path,
    phase: str,
) -> None:
    """A completed failed request must not leave the live sandbox's credential scope open."""
    store = SharedAttempts()
    driver = FailingDriver(tmp_path, phase)
    scheduler = _scheduler(driver, repo=repository(store))
    request = _request()
    result = await scheduler.schedule(request)
    assert result.error is not None and result.error.code == "activity.execution_unconfirmed"
    row = next(iter(store.rows.values()))
    assert row.authority_revoked and not row.completed
    assert len(scheduler._context) == 1 and not scheduler._workers
    await scheduler.drain()
    assert driver.cancellations == [CancelReason.SHUTDOWN]
    later_driver = _FakeDriver(tmp_path / "later", outputs=None)
    later = await _scheduler(later_driver, repo=repository(store)).schedule(
        replace(request, step=request.step.model_copy(update={"attempt": 2})),
    )
    assert later.error is not None and later.error.code == "activity.execution_unconfirmed"
    assert later_driver.prepared == 0 and driver.prepared == 1


@pytest.mark.parametrize("phase", ["monitor", "deadline", "finalize", "missing-context"])
async def test_reconciliation_failure_revokes_owned_attempt(
    tmp_path: Path,
    phase: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every reconciliation uncertainty closes authority without inventing a terminal result."""
    scheduler, store, driver, request = await retained_attempt(tmp_path)
    if phase == "missing-context":
        scheduler._context.clear()
    elif phase in {"deadline", "finalize"}:
        driver._await_error = None
        if phase == "deadline":
            driver._outcome = SandboxOutcome(143, SandboxSignal.DEADLINE)
        else:

            def unavailable(handle: SandboxHandle) -> None:
                """Lose collection after monitoring; no exact final result is available."""
                raise SandboxFailureError("private-collection")

            monkeypatch.setattr(driver, "collect", unavailable)
    result = await scheduler.schedule(request)
    assert result.error is not None and result.error.code == "activity.execution_unconfirmed"
    row = next(iter(store.rows.values()))
    assert row.authority_revoked and not row.completed
    assert driver.prepared == 0


async def test_retained_stop_failure_retries_without_clearing_fence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed stop is not a drained context, even when no worker remains."""
    scheduler, store, driver, _ = await retained_attempt(tmp_path)
    calls = 0

    def stop(handle: SandboxHandle, reason: CancelReason) -> None:
        """Fail once after verifying the authority boundary, then accept the stop request."""
        nonlocal calls
        calls += 1
        assert next(iter(store.rows.values())).authority_revoked
        if calls == 1:
            raise SandboxFailureError("lost stop response")

    monkeypatch.setattr(driver, "cancel", stop)
    await asyncio.wait_for(scheduler.drain(), 2)
    assert calls == 2 and not next(iter(store.rows.values())).completed


async def test_terminal_race_is_already_closed_for_revocation(tmp_path: Path) -> None:
    """Completion racing with drain must not leave drain retrying a forgotten terminal owner."""
    from tests.test_scheduler import _success_outputs

    store = SharedAttempts()
    scheduler = _scheduler(
        _FakeDriver(tmp_path, outputs=_success_outputs()),
        repo=repository(store),
    )
    request = _request()
    result = await scheduler.schedule(request)
    await scheduler._repo.revoke_credentials(scheduler._key(request))
    assert next(iter(store.rows.values())).completed
    replay = await scheduler.schedule(request)
    assert replay == result


async def test_drain_retries_retained_context_without_workers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Shutdown cancellation cannot close clients while retained-context revocation is failing."""
    scheduler, store, driver, _ = await retained_attempt(tmp_path)
    attempted, recover, stopped = asyncio.Event(), asyncio.Event(), asyncio.Event()
    revoke = store.revoke_activity_authority
    cancel = driver.cancel
    loop = asyncio.get_running_loop()

    async def unavailable(record: ActivityExecutionRecord) -> ActivityExecutionRecord:
        """Retry the real store revocation only after explicit recovery."""
        attempted.set()
        if not recover.is_set():
            raise BackendUnavailable("private-store")
        return await revoke(record)

    def stop(handle: SandboxHandle, reason: CancelReason) -> None:
        """Require durable revocation before every stop request."""
        assert next(iter(store.rows.values())).authority_revoked
        cancel(handle, reason)
        loop.call_soon_threadsafe(stopped.set)

    monkeypatch.setattr(store, "revoke_activity_authority", unavailable)
    monkeypatch.setattr(driver, "cancel", stop)
    draining = asyncio.create_task(scheduler.drain())
    try:
        await asyncio.wait_for(attempted.wait(), 1)
        draining.cancel()
        await asyncio.sleep(0)
        assert not draining.done() and not driver.cancellations
    finally:
        recover.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(draining, 2)
    assert stopped.is_set() and not scheduler._workers
    assert not next(iter(store.rows.values())).completed
    assert "private-store" not in caplog.text


async def test_failed_worker_retries_revocation_before_leaving_ownership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A temporary revocation outage retains the request worker and its context until recovery."""
    store = SharedAttempts()
    scheduler = _scheduler(FailingDriver(tmp_path, "monitor"), repo=repository(store))
    attempted, recover = asyncio.Event(), asyncio.Event()
    revoke = store.revoke_activity_authority

    async def unavailable(record: ActivityExecutionRecord) -> ActivityExecutionRecord:
        """Expose a backend outage with deliberately private diagnostic text."""
        attempted.set()
        if not recover.is_set():
            raise BackendUnavailable("private-revocation")
        return await revoke(record)

    monkeypatch.setattr(store, "revoke_activity_authority", unavailable)
    caller = asyncio.create_task(scheduler.schedule(_request()))
    try:
        await asyncio.wait_for(attempted.wait(), 1)
        assert scheduler._workers and not caller.done()
    finally:
        recover.set()
    result = await asyncio.wait_for(caller, 2)
    assert result.error is not None and result.error.code == "activity.execution_unconfirmed"
    assert next(iter(store.rows.values())).authority_revoked
    assert "private-revocation" not in caplog.text

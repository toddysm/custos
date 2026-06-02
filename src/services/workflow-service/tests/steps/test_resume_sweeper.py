"""Tests for the :class:`ResumeSubscriptionTtlSweeper` (WF-IMPL-109).

The sweeper garbage-collects TTL-expired
:class:`~custos_workflow.steps.resume.ResumeSubscriptionMirror` rows on a
wall-clock interval (``design.md`` § *Data Models* — *TTL-expired
mirrors are garbage-collected on a periodic sweep*). Coverage targets
every acceptance criterion from #548:

* Expired mirrors are removed on the sweep; non-expired untouched.
* The sweep is restart-safe and idempotent.

plus the supporting edges: an empty / nothing-expired pass, the
boundary (``expires_at == now`` is expired), per-row delete-failure
isolation, ``list_expired`` failure propagation out of ``sweep_once``,
and the ``run_forever`` periodic driver's continue-on-error and
cancellation behaviour.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest

from custos_workflow.steps.resume import (
    DEFAULT_RESUME_SUB_SWEEP_INTERVAL_SECONDS,
    InMemoryResumeSubscriptionMirrorRepository,
    ResumeSubscriptionMirror,
    ResumeSubscriptionTtlSweeper,
    TtlSweepReport,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def _mirror(
    *,
    mirror_id: str,
    expires_at: datetime,
    run_id: str = "run-1",
    step_id: str = "await-event",
    event_key: str = "order-approved",
) -> ResumeSubscriptionMirror:
    return ResumeSubscriptionMirror(
        mirror_id=mirror_id,
        run_id=run_id,
        step_id=step_id,
        event_key=event_key,
        ts_subscription_id=f"ts-{mirror_id}",
        registered_at=_NOW - timedelta(hours=48),
        expires_at=expires_at,
    )


async def _seed(
    repo: InMemoryResumeSubscriptionMirrorRepository, *mirrors: ResumeSubscriptionMirror
) -> None:
    for mirror in mirrors:
        await repo.put(mirror)


def _fixed_clock(now: datetime) -> Callable[[], datetime]:
    return lambda: now


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _DeleteFailsRepo(InMemoryResumeSubscriptionMirrorRepository):
    """Mirror repo whose ``delete`` raises for one mirror id."""

    def __init__(self, *, failing_mirror_id: str) -> None:
        super().__init__()
        self._failing_mirror_id = failing_mirror_id

    async def delete(self, mirror_id: str) -> None:
        if mirror_id == self._failing_mirror_id:
            raise RuntimeError(f"store down deleting {mirror_id}")
        await super().delete(mirror_id)


class _ListExpiredFailsRepo(InMemoryResumeSubscriptionMirrorRepository):
    """Mirror repo whose ``list_expired`` raises, counting the calls."""

    def __init__(self) -> None:
        super().__init__()
        self.list_expired_calls = 0

    async def list_expired(self, before: datetime) -> tuple[ResumeSubscriptionMirror, ...]:
        self.list_expired_calls += 1
        raise RuntimeError("store down")


class _CountingRepo(InMemoryResumeSubscriptionMirrorRepository):
    """Mirror repo that counts ``list_expired`` calls (always empty)."""

    def __init__(self) -> None:
        super().__init__()
        self.list_expired_calls = 0

    async def list_expired(self, before: datetime) -> tuple[ResumeSubscriptionMirror, ...]:
        self.list_expired_calls += 1
        return await super().list_expired(before)


class _BlockingListExpiredRepo(InMemoryResumeSubscriptionMirrorRepository):
    """Mirror repo whose ``list_expired`` blocks until cancelled.

    Lets a test cancel ``run_forever`` while a sweep is in-flight so the
    in-``try`` :class:`asyncio.CancelledError` re-raise path is exercised.
    """

    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()

    async def list_expired(self, before: datetime) -> tuple[ResumeSubscriptionMirror, ...]:
        self.entered.set()
        await asyncio.Event().wait()  # never resolves; awaits cancellation
        raise AssertionError("unreachable")  # pragma: no cover


# ---------------------------------------------------------------------------
# Module surface / construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_default_interval_is_five_minutes(self) -> None:
        assert DEFAULT_RESUME_SUB_SWEEP_INTERVAL_SECONDS == 300.0

    def test_mirror_repo_property_exposes_injected_repo(self) -> None:
        repo = InMemoryResumeSubscriptionMirrorRepository()
        sweeper = ResumeSubscriptionTtlSweeper(repo)
        assert sweeper.mirror_repo is repo

    def test_report_defaults_are_empty(self) -> None:
        report = TtlSweepReport()
        assert report.deleted == ()
        assert report.failed == ()


# ---------------------------------------------------------------------------
# sweep_once
# ---------------------------------------------------------------------------


class TestSweepOnce:
    async def test_empty_repo_is_a_noop(self) -> None:
        sweeper = ResumeSubscriptionTtlSweeper(
            InMemoryResumeSubscriptionMirrorRepository(), clock=_fixed_clock(_NOW)
        )
        report = await sweeper.sweep_once()
        assert report == TtlSweepReport()

    async def test_expired_removed_and_non_expired_untouched(self) -> None:
        repo = InMemoryResumeSubscriptionMirrorRepository()
        expired_past = _mirror(mirror_id="rsm-expired", expires_at=_NOW - timedelta(hours=1))
        live_future = _mirror(mirror_id="rsm-live", expires_at=_NOW + timedelta(hours=1))
        await _seed(repo, expired_past, live_future)

        sweeper = ResumeSubscriptionTtlSweeper(repo, clock=_fixed_clock(_NOW))
        report = await sweeper.sweep_once()

        assert report.deleted == ("rsm-expired",)
        assert report.failed == ()
        # Only the live (non-expired) row survives.
        remaining = await repo.list_open("run-1")
        assert [row.mirror_id for row in remaining] == ["rsm-live"]

    async def test_boundary_expires_at_equal_now_is_expired(self) -> None:
        repo = InMemoryResumeSubscriptionMirrorRepository()
        boundary = _mirror(mirror_id="rsm-boundary", expires_at=_NOW)
        await _seed(repo, boundary)

        sweeper = ResumeSubscriptionTtlSweeper(repo, clock=_fixed_clock(_NOW))
        report = await sweeper.sweep_once()

        # ``expires_at <= now`` ⇒ the boundary row is reaped.
        assert report.deleted == ("rsm-boundary",)
        assert await repo.list_expired(_NOW + timedelta(days=365)) == ()

    async def test_multiple_expired_all_reaped(self) -> None:
        repo = InMemoryResumeSubscriptionMirrorRepository()
        await _seed(
            repo,
            _mirror(mirror_id="rsm-a", expires_at=_NOW - timedelta(hours=2)),
            _mirror(mirror_id="rsm-b", expires_at=_NOW - timedelta(hours=3)),
            _mirror(mirror_id="rsm-c", expires_at=_NOW + timedelta(hours=4)),
        )
        sweeper = ResumeSubscriptionTtlSweeper(repo, clock=_fixed_clock(_NOW))
        report = await sweeper.sweep_once()

        # Deterministic mirror_id order from the in-memory repo.
        assert report.deleted == ("rsm-a", "rsm-b")
        assert report.failed == ()

    async def test_default_clock_reaps_already_expired_row(self) -> None:
        # No injected clock ⇒ the sweeper uses the real UTC wall clock;
        # a row whose ``expires_at`` is well in the past is reaped.
        repo = InMemoryResumeSubscriptionMirrorRepository()
        long_expired = _mirror(
            mirror_id="rsm-old",
            expires_at=datetime(2000, 1, 1, tzinfo=UTC),
        )
        await _seed(repo, long_expired)

        sweeper = ResumeSubscriptionTtlSweeper(repo)
        report = await sweeper.sweep_once()

        assert report.deleted == ("rsm-old",)

    async def test_idempotent_second_sweep_after_full_reap_is_noop(self) -> None:
        repo = InMemoryResumeSubscriptionMirrorRepository()
        await _seed(repo, _mirror(mirror_id="rsm-x", expires_at=_NOW - timedelta(hours=1)))
        sweeper = ResumeSubscriptionTtlSweeper(repo, clock=_fixed_clock(_NOW))

        first = await sweeper.sweep_once()
        second = await sweeper.sweep_once()

        assert first.deleted == ("rsm-x",)
        # Restart-safe / idempotent: re-running the sweep finds nothing.
        assert second == TtlSweepReport()

    async def test_per_row_delete_failure_is_isolated(self) -> None:
        repo = _DeleteFailsRepo(failing_mirror_id="rsm-bad")
        await _seed(
            repo,
            _mirror(mirror_id="rsm-good", expires_at=_NOW - timedelta(hours=1)),
            _mirror(mirror_id="rsm-bad", expires_at=_NOW - timedelta(hours=1)),
        )
        sweeper = ResumeSubscriptionTtlSweeper(repo, clock=_fixed_clock(_NOW))
        report = await sweeper.sweep_once()

        # The good row is reaped; the failing row is recorded, not raised.
        assert report.deleted == ("rsm-good",)
        assert report.failed == ("rsm-bad",)
        # The failing row is left in place for the next sweep to retry.
        remaining = await repo.list_open("run-1")
        assert [row.mirror_id for row in remaining] == ["rsm-bad"]

    async def test_list_expired_failure_propagates(self) -> None:
        sweeper = ResumeSubscriptionTtlSweeper(_ListExpiredFailsRepo(), clock=_fixed_clock(_NOW))
        with pytest.raises(RuntimeError, match="store down"):
            await sweeper.sweep_once()


# ---------------------------------------------------------------------------
# run_forever — periodic driver
# ---------------------------------------------------------------------------


class TestRunForever:
    async def test_sweeps_repeatedly_until_cancelled(self) -> None:
        repo = _CountingRepo()
        sweeper = ResumeSubscriptionTtlSweeper(repo, clock=_fixed_clock(_NOW))

        task = asyncio.create_task(sweeper.run_forever(0.0))
        # Yield control enough times for several sweep passes to run.
        for _ in range(5):
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert repo.list_expired_calls >= 2

    async def test_continues_after_a_failing_sweep(self) -> None:
        # ``list_expired`` always raises; ``run_forever`` must log and
        # continue rather than let the task die, so the call count keeps
        # climbing across passes.
        repo = _ListExpiredFailsRepo()
        sweeper = ResumeSubscriptionTtlSweeper(repo, clock=_fixed_clock(_NOW))

        task = asyncio.create_task(sweeper.run_forever(0.0))
        for _ in range(5):
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # The task survived the repeated failures (did not crash) and
        # kept sweeping past the first error.
        assert repo.list_expired_calls >= 2
        assert task.cancelled()

    async def test_cancelled_mid_sweep_propagates(self) -> None:
        # Cancelling while a sweep is in-flight must re-raise
        # ``CancelledError`` straight out of ``run_forever`` rather than
        # being swallowed by the continue-on-error handler.
        repo = _BlockingListExpiredRepo()
        sweeper = ResumeSubscriptionTtlSweeper(repo, clock=_fixed_clock(_NOW))

        task = asyncio.create_task(sweeper.run_forever(0.0))
        await repo.entered.wait()  # ensure the sweep is mid-flight
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert task.cancelled()

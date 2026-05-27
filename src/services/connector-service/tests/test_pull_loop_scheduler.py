"""Unit tests for :class:`custos_connector.scheduler.PullLoopScheduler`.

The scheduler talks to the rest of the connector-service only via
:class:`CursorService.tick`, so these tests drive a tiny scripted
cursor double rather than wiring the full cursor / metadata / catalog
fake stack. Tick happy/halt/lease-busy paths against the real
:class:`CursorService` are already covered by ``test_cursor_service``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest
from custos_spl import LeaseBusy

from custos_connector.cursor.service import (
    CursorEncodingMismatchHalt,
    CursorExpiredHalt,
    CursorInstanceUnavailable,
    TickResult,
)
from custos_connector.scheduler import (
    DEFAULT_BACKOFF_CAP_SECONDS,
    PULL_LOOP_HARD_FLOOR_SEC,
    PullLoopScheduler,
)

pytestmark = pytest.mark.asyncio


_WORKSPACE = "ws-1"


# ---------------------------------------------------------------------------
# Doubles
# ---------------------------------------------------------------------------


class _ScriptedCursorService:
    """A :class:`CursorService` stand-in keyed by ``(workspace, instance)``.

    Each registered queue is consumed in FIFO order. A queued
    :class:`BaseException` instance is raised, anything else (a
    callable returning :class:`TickResult` or a literal
    :class:`TickResult`) yields a tick result. Callables are
    awaited if they return an awaitable; that lets a test inject
    "tick takes N steps of fake-clock time" behaviour.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self._scripts: dict[
            tuple[str, str],
            list[TickResult | BaseException | Callable[[], Awaitable[TickResult]]],
        ] = {}

    def queue(
        self,
        workspace_id: str,
        instance_id: str,
        item: TickResult | BaseException | Callable[[], Awaitable[TickResult]],
    ) -> None:
        self._scripts.setdefault((workspace_id, instance_id), []).append(item)

    async def tick(self, workspace_id: str, instance_id: str) -> TickResult:
        self.calls.append((workspace_id, instance_id))
        script = self._scripts.get((workspace_id, instance_id))
        if not script:
            raise AssertionError(f"no scripted tick result for {workspace_id!r}/{instance_id!r}")
        nxt = script.pop(0)
        if isinstance(nxt, BaseException):
            raise nxt
        if callable(nxt):
            return await nxt()
        return nxt


def _make_tick_result(event_count: int = 1) -> TickResult:
    """Build a TickResult with a MagicMock for the envelope records.

    The scheduler only reads ``event_count`` off the result, so we
    can satisfy the dataclass shape cheaply.
    """
    mock_envelope = MagicMock()
    return TickResult(
        instance_id="inst-test",
        event_count=event_count,
        cursor_before=mock_envelope,
        cursor_after=mock_envelope,
        receiver_endpoint=None,
    )


class _Clock:
    """Manual monotonic clock for deterministic interval tests."""

    def __init__(self, start: datetime | None = None) -> None:
        self._now = start or datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now = self._now + timedelta(seconds=seconds)


def _build_scheduler(
    cursor: _ScriptedCursorService,
    *,
    clock: _Clock | None = None,
    min_interval_seconds: int = PULL_LOOP_HARD_FLOOR_SEC,
    backoff_cap_seconds: int = DEFAULT_BACKOFF_CAP_SECONDS,
    max_concurrent_ticks: int = 16,
) -> tuple[PullLoopScheduler, _Clock]:
    clk = clock or _Clock()
    sched = PullLoopScheduler(
        cursor_service=cursor,  # type: ignore[arg-type]
        min_interval_seconds=min_interval_seconds,
        backoff_cap_seconds=backoff_cap_seconds,
        max_concurrent_ticks=max_concurrent_ticks,
        clock=clk,
    )
    return sched, clk


# ---------------------------------------------------------------------------
# Construction guards
# ---------------------------------------------------------------------------


async def test_min_interval_below_design_floor_rejected() -> None:
    """The design pins the ≥10s floor; constructor MUST refuse smaller values."""
    cursor = _ScriptedCursorService()
    with pytest.raises(ValueError, match=">= 10"):
        PullLoopScheduler(cursor_service=cursor, min_interval_seconds=5)  # type: ignore[arg-type]


async def test_backoff_cap_below_min_interval_rejected() -> None:
    """``backoff_cap < min_interval`` would mean failures shorten the cadence."""
    cursor = _ScriptedCursorService()
    with pytest.raises(ValueError, match="backoff_cap_seconds"):
        PullLoopScheduler(
            cursor_service=cursor,  # type: ignore[arg-type]
            min_interval_seconds=30,
            backoff_cap_seconds=10,
        )


async def test_max_concurrent_ticks_must_be_positive() -> None:
    cursor = _ScriptedCursorService()
    with pytest.raises(ValueError, match="max_concurrent_ticks"):
        PullLoopScheduler(cursor_service=cursor, max_concurrent_ticks=0)  # type: ignore[arg-type]


async def test_scan_interval_must_be_positive() -> None:
    cursor = _ScriptedCursorService()
    with pytest.raises(ValueError, match="scan_interval_seconds"):
        PullLoopScheduler(cursor_service=cursor, scan_interval_seconds=0)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


async def test_register_is_idempotent() -> None:
    """Re-register MUST return the existing schedule without resetting state."""
    cursor = _ScriptedCursorService()
    sched, _ = _build_scheduler(cursor)
    first = sched.register(_WORKSPACE, "inst-A")
    first.consecutive_failures = 3
    second = sched.register(_WORKSPACE, "inst-A")
    assert second is first
    assert second.consecutive_failures == 3


async def test_register_rejects_interval_below_min() -> None:
    cursor = _ScriptedCursorService()
    sched, _ = _build_scheduler(cursor, min_interval_seconds=15)
    with pytest.raises(ValueError, match=">= 15"):
        sched.register(_WORKSPACE, "inst-A", interval_seconds=10)


async def test_unregister_returns_true_when_present() -> None:
    cursor = _ScriptedCursorService()
    sched, _ = _build_scheduler(cursor)
    sched.register(_WORKSPACE, "inst-A")
    assert sched.unregister(_WORKSPACE, "inst-A") is True
    assert sched.unregister(_WORKSPACE, "inst-A") is False


async def test_first_pass_after_register_ticks_immediately() -> None:
    """A freshly registered instance MUST be eligible on the next pass."""
    cursor = _ScriptedCursorService()
    cursor.queue(_WORKSPACE, "inst-A", _make_tick_result(event_count=2))
    sched, _ = _build_scheduler(cursor)
    sched.register(_WORKSPACE, "inst-A")

    result = await sched.tick_once()

    assert result.eligible == 1
    assert result.outcomes[0].status == "ticked"
    assert result.outcomes[0].event_count == 2
    assert cursor.calls == [(_WORKSPACE, "inst-A")]


# ---------------------------------------------------------------------------
# Interval coalescing
# ---------------------------------------------------------------------------


async def test_within_interval_ticks_are_coalesced() -> None:
    """Second pass within the interval MUST skip the instance."""
    cursor = _ScriptedCursorService()
    cursor.queue(_WORKSPACE, "inst-A", _make_tick_result())
    cursor.queue(_WORKSPACE, "inst-A", _make_tick_result())
    sched, clk = _build_scheduler(cursor, min_interval_seconds=10)
    sched.register(_WORKSPACE, "inst-A")

    first = await sched.tick_once()
    assert first.outcomes[0].status == "ticked"

    # Only 5 s have elapsed — second pass MUST not call the cursor.
    clk.advance(5)
    second = await sched.tick_once()
    assert second.eligible == 0
    assert second.skipped == 1
    assert len(cursor.calls) == 1

    # After the interval, the next pass MUST tick again.
    clk.advance(6)
    third = await sched.tick_once()
    assert third.outcomes[0].status == "ticked"
    assert len(cursor.calls) == 2


async def test_per_instance_interval_override_widens_cadence() -> None:
    """An override above the floor MUST be honoured."""
    cursor = _ScriptedCursorService()
    cursor.queue(_WORKSPACE, "inst-A", _make_tick_result())
    cursor.queue(_WORKSPACE, "inst-A", _make_tick_result())
    sched, clk = _build_scheduler(cursor, min_interval_seconds=10)
    sched.register(_WORKSPACE, "inst-A", interval_seconds=30)

    await sched.tick_once()
    # 11 s would be enough at the global floor but the per-instance
    # override is 30 s — MUST still be coalesced.
    clk.advance(11)
    result = await sched.tick_once()
    assert result.eligible == 0
    # After the override interval the second tick fires.
    clk.advance(20)
    result = await sched.tick_once()
    assert result.outcomes[0].status == "ticked"


# ---------------------------------------------------------------------------
# Pause / resume
# ---------------------------------------------------------------------------


async def test_pause_skips_subsequent_ticks() -> None:
    cursor = _ScriptedCursorService()
    cursor.queue(_WORKSPACE, "inst-A", _make_tick_result())
    sched, clk = _build_scheduler(cursor)
    sched.register(_WORKSPACE, "inst-A")

    await sched.tick_once()
    sched.pause(_WORKSPACE, "inst-A")

    clk.advance(60)
    result = await sched.tick_once()
    assert result.eligible == 0
    assert result.skipped == 1


async def test_resume_re_eligibles_and_clears_failure_counter() -> None:
    cursor = _ScriptedCursorService()
    cursor.queue(_WORKSPACE, "inst-A", RuntimeError("boom"))
    cursor.queue(_WORKSPACE, "inst-A", _make_tick_result())
    sched, clk = _build_scheduler(cursor)
    sched.register(_WORKSPACE, "inst-A")

    # First tick fails — backoff and failure counter set.
    await sched.tick_once()
    state = sched.schedules()[(_WORKSPACE, "inst-A")]
    assert state.consecutive_failures == 1
    assert state.next_earliest_tick_at > clk()

    sched.pause(_WORKSPACE, "inst-A")
    sched.resume(_WORKSPACE, "inst-A")
    after = sched.schedules()[(_WORKSPACE, "inst-A")]
    assert after.paused is False
    assert after.halted is False
    assert after.consecutive_failures == 0

    result = await sched.tick_once()
    assert result.outcomes[0].status == "ticked"


async def test_pause_and_resume_on_unknown_instance_are_noops() -> None:
    cursor = _ScriptedCursorService()
    sched, _ = _build_scheduler(cursor)
    # Must not raise.
    sched.pause(_WORKSPACE, "nope")
    sched.resume(_WORKSPACE, "nope")


# ---------------------------------------------------------------------------
# Lease busy (single-writer enforcement)
# ---------------------------------------------------------------------------


async def test_lease_busy_is_treated_as_benign_skip() -> None:
    """``LeaseBusy`` MUST NOT count as a failure or bump backoff."""
    cursor = _ScriptedCursorService()
    cursor.queue(_WORKSPACE, "inst-A", LeaseBusy("held by other replica"))
    sched, clk = _build_scheduler(cursor, min_interval_seconds=10)
    sched.register(_WORKSPACE, "inst-A")

    result = await sched.tick_once()
    assert result.outcomes[0].status == "lease_busy"
    state = sched.schedules()[(_WORKSPACE, "inst-A")]
    assert state.consecutive_failures == 0
    # Next eligibility MUST be ~one interval away (not the backoff cap).
    expected = clk() + timedelta(seconds=10)
    assert state.next_earliest_tick_at == expected


# ---------------------------------------------------------------------------
# Halt paths drop the instance from active rotation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("halt_exc", "halt_name"),
    [
        (
            CursorEncodingMismatchHalt(
                "encoding bumped",
                persisted_encoding="v1",
                plugin_encoding="v2",
            ),
            "CursorEncodingMismatchHalt",
        ),
        (CursorExpiredHalt("upstream rejected cursor"), "CursorExpiredHalt"),
    ],
)
async def test_cursor_halted_marks_instance_halted(halt_exc: Exception, halt_name: str) -> None:
    """A halt MUST stop scheduling until ``resume()`` is called."""
    cursor = _ScriptedCursorService()
    cursor.queue(_WORKSPACE, "inst-A", halt_exc)
    sched, clk = _build_scheduler(cursor)
    sched.register(_WORKSPACE, "inst-A")

    result = await sched.tick_once()
    assert result.outcomes[0].status == "halted"
    state = sched.schedules()[(_WORKSPACE, "inst-A")]
    assert state.halted is True
    assert state.halt_reason == halt_name

    # Next pass MUST skip even with plenty of time elapsed.
    clk.advance(3600)
    result = await sched.tick_once()
    assert result.eligible == 0
    assert result.skipped == 1


async def test_cursor_instance_unavailable_removes_from_rotation() -> None:
    """A config error MUST drop the instance entirely."""
    cursor = _ScriptedCursorService()
    cursor.queue(
        _WORKSPACE,
        "inst-A",
        CursorInstanceUnavailable("inst-A", "instance is disabled"),
    )
    sched, _ = _build_scheduler(cursor)
    sched.register(_WORKSPACE, "inst-A")

    result = await sched.tick_once()
    assert result.outcomes[0].status == "unavailable"
    assert (_WORKSPACE, "inst-A") not in sched.schedules()


# ---------------------------------------------------------------------------
# Generic failure → exponential backoff
# ---------------------------------------------------------------------------


async def test_generic_failure_bumps_counter_and_applies_exponential_backoff() -> None:
    cursor = _ScriptedCursorService()
    cursor.queue(_WORKSPACE, "inst-A", RuntimeError("fail-1"))
    cursor.queue(_WORKSPACE, "inst-A", RuntimeError("fail-2"))
    cursor.queue(_WORKSPACE, "inst-A", RuntimeError("fail-3"))
    sched, clk = _build_scheduler(cursor, min_interval_seconds=10)
    sched.register(_WORKSPACE, "inst-A")

    await sched.tick_once()  # failure 1 → backoff 10s
    state = sched.schedules()[(_WORKSPACE, "inst-A")]
    assert state.consecutive_failures == 1
    assert state.next_earliest_tick_at == clk() + timedelta(seconds=10)

    clk.advance(10)
    await sched.tick_once()  # failure 2 → backoff 20s
    state = sched.schedules()[(_WORKSPACE, "inst-A")]
    assert state.consecutive_failures == 2
    assert state.next_earliest_tick_at == clk() + timedelta(seconds=20)

    clk.advance(20)
    await sched.tick_once()  # failure 3 → backoff 40s
    state = sched.schedules()[(_WORKSPACE, "inst-A")]
    assert state.consecutive_failures == 3
    assert state.next_earliest_tick_at == clk() + timedelta(seconds=40)


async def test_backoff_is_capped_at_configured_value() -> None:
    cursor = _ScriptedCursorService()
    sched, clk = _build_scheduler(
        cursor,
        min_interval_seconds=10,
        backoff_cap_seconds=30,
    )
    sched.register(_WORKSPACE, "inst-A")
    # Fast-forward to a high failure count and assert the cap holds.
    state = sched.schedules()[(_WORKSPACE, "inst-A")]
    state.consecutive_failures = 10  # 10 * 2^9 = 5120s candidate
    cursor.queue(_WORKSPACE, "inst-A", RuntimeError("fail-11"))

    await sched.tick_once()
    state = sched.schedules()[(_WORKSPACE, "inst-A")]
    assert state.next_earliest_tick_at == clk() + timedelta(seconds=30)


async def test_successful_tick_resets_failure_counter() -> None:
    cursor = _ScriptedCursorService()
    cursor.queue(_WORKSPACE, "inst-A", RuntimeError("transient"))
    cursor.queue(_WORKSPACE, "inst-A", _make_tick_result())
    sched, clk = _build_scheduler(cursor)
    sched.register(_WORKSPACE, "inst-A")

    await sched.tick_once()  # failure
    state = sched.schedules()[(_WORKSPACE, "inst-A")]
    assert state.consecutive_failures == 1

    clk.advance(state.interval_seconds * 2)  # past the backoff
    await sched.tick_once()  # success
    state = sched.schedules()[(_WORKSPACE, "inst-A")]
    assert state.consecutive_failures == 0


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


async def test_concurrent_ticks_across_instances_bounded_by_semaphore() -> None:
    """Two instances both eligible MUST both tick, but concurrency is bounded."""
    cursor = _ScriptedCursorService()

    barrier = asyncio.Event()
    in_flight = 0
    peak = 0

    async def _slow_tick() -> TickResult:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        try:
            await barrier.wait()
        finally:
            in_flight -= 1
        return _make_tick_result()

    cursor.queue(_WORKSPACE, "inst-A", _slow_tick)
    cursor.queue(_WORKSPACE, "inst-B", _slow_tick)
    cursor.queue(_WORKSPACE, "inst-C", _slow_tick)

    sched, _ = _build_scheduler(cursor, max_concurrent_ticks=2)
    sched.register(_WORKSPACE, "inst-A")
    sched.register(_WORKSPACE, "inst-B")
    sched.register(_WORKSPACE, "inst-C")

    pass_task = asyncio.create_task(sched.tick_once())
    # Give the scheduler a chance to dispatch up to ``max_concurrent_ticks``
    # ticks before releasing the barrier.
    for _ in range(10):
        await asyncio.sleep(0)
    assert peak <= 2  # bounded by semaphore
    barrier.set()
    result = await pass_task

    assert result.eligible == 3
    assert all(outcome.status == "ticked" for outcome in result.outcomes)
    assert peak == 2


async def test_in_flight_tick_is_not_re_dispatched_by_concurrent_pass() -> None:
    """A second ``tick_once`` while a tick is in-flight MUST skip that instance."""
    cursor = _ScriptedCursorService()
    barrier = asyncio.Event()

    async def _hang() -> TickResult:
        await barrier.wait()
        return _make_tick_result()

    cursor.queue(_WORKSPACE, "inst-A", _hang)
    sched, _ = _build_scheduler(cursor)
    sched.register(_WORKSPACE, "inst-A")

    first = asyncio.create_task(sched.tick_once())
    for _ in range(10):
        await asyncio.sleep(0)
    # Run a second pass while the first tick is in-flight.
    second_result = await sched.tick_once()
    assert second_result.eligible == 0
    assert second_result.skipped == 1

    barrier.set()
    first_result = await first
    assert first_result.outcomes[0].status == "ticked"


# ---------------------------------------------------------------------------
# Long-running driver
# ---------------------------------------------------------------------------


async def test_run_drives_passes_until_stop_is_called() -> None:
    """``run`` MUST cleanly exit when ``stop`` is called between passes."""
    cursor = _ScriptedCursorService()
    cursor.queue(_WORKSPACE, "inst-A", _make_tick_result())
    cursor.queue(_WORKSPACE, "inst-A", _make_tick_result())
    cursor.queue(_WORKSPACE, "inst-A", _make_tick_result())
    sched = PullLoopScheduler(
        cursor_service=cursor,  # type: ignore[arg-type]
        scan_interval_seconds=0.01,
    )
    sched.register(_WORKSPACE, "inst-A", interval_seconds=10)

    async def _stopper() -> None:
        await asyncio.sleep(0.05)
        sched.stop()

    await asyncio.gather(sched.run(), _stopper())

    # At least one tick fired before stop().
    assert len(cursor.calls) >= 1


# ---------------------------------------------------------------------------
# Multi-instance ordering
# ---------------------------------------------------------------------------


async def test_multi_instance_pass_returns_outcome_per_eligible_instance() -> None:
    cursor = _ScriptedCursorService()
    cursor.queue(_WORKSPACE, "inst-A", _make_tick_result(event_count=1))
    cursor.queue(_WORKSPACE, "inst-B", LeaseBusy("other replica owns it"))
    cursor.queue(_WORKSPACE, "inst-C", _make_tick_result(event_count=4))
    sched, _ = _build_scheduler(cursor)
    sched.register(_WORKSPACE, "inst-A")
    sched.register(_WORKSPACE, "inst-B")
    sched.register(_WORKSPACE, "inst-C")

    result = await sched.tick_once()

    statuses: dict[str, str] = {outcome.instance_id: outcome.status for outcome in result.outcomes}
    assert statuses == {
        "inst-A": "ticked",
        "inst-B": "lease_busy",
        "inst-C": "ticked",
    }
    assert result.ticks_attempted == 3
    assert result.eligible == 3
    assert result.skipped == 0


# ---------------------------------------------------------------------------
# Schedule snapshot
# ---------------------------------------------------------------------------


async def test_schedules_snapshot_is_a_copy() -> None:
    """Mutating the returned mapping MUST NOT corrupt internal state."""
    cursor = _ScriptedCursorService()
    sched, _ = _build_scheduler(cursor)
    sched.register(_WORKSPACE, "inst-A")
    snapshot: dict[tuple[str, str], Any] = dict(sched.schedules())
    snapshot.clear()
    assert (_WORKSPACE, "inst-A") in sched.schedules()

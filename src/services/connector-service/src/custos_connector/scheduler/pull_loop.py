"""``PullLoopScheduler`` — per-instance pull-loop driver.

See ``custos_connector.scheduler`` (package docstring) for the
design contract this module implements.

Scope boundary
--------------

This module is the **scheduler** only. It does not own the cursor
lifecycle (that's :mod:`custos_connector.cursor.service`), it does
not own SPL leases (those are claimed inside the tick by
:meth:`MetadataStoreProvider.acquire_cursor_lease`), and it does
not enumerate eligible instances from storage (callers register
explicitly — see § v1 instance discovery model in the package
docstring). Its only job is: decide which registered instance
should tick next, and call :meth:`CursorService.tick`.

Concurrency model
-----------------

A scheduler "pass" walks the registry once and fires ticks for every
eligible instance. Per-instance ticks are dispatched as concurrent
:class:`asyncio.Task`\\s bounded by a :class:`asyncio.Semaphore` so a
single misbehaving plugin cannot exhaust the event loop. The
per-instance ``in_flight`` guard prevents a slow tick from being
re-scheduled on the next pass before it finishes.

The long-running :meth:`run` loop simply alternates between
:meth:`tick_once` and ``await asyncio.sleep(scan_interval)``. Tests
drive the scheduler synchronously via :meth:`tick_once` so they do
not depend on real-time sleeps.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from custos_spl import LeaseBusy

from custos_connector.cursor.service import (
    CursorHalted,
    CursorInstanceUnavailable,
)
from custos_connector.settings import PULL_LOOP_HARD_FLOOR_SEC as _PULL_LOOP_HARD_FLOOR_SEC

if TYPE_CHECKING:
    from custos_connector.cursor.service import CursorService, TickResult


# Re-export under the scheduler namespace so callers don't have to
# reach into ``settings`` for the floor when validating their own
# interval overrides.
PULL_LOOP_HARD_FLOOR_SEC: Final[int] = _PULL_LOOP_HARD_FLOOR_SEC

#: Default cap for the exponential backoff applied after a generic
#: tick failure (5 minutes). The design pins ``≥10s`` as the floor
#: and is silent on the cap; we pick 5 minutes as a balance between
#: "don't hammer a flapping upstream" and "don't sit idle for hours
#: after a transient error".
DEFAULT_BACKOFF_CAP_SECONDS: Final[int] = 300

#: Default fleet-wide concurrent-tick cap. The design does not
#: prescribe a value; this matches the lease-manager
#: concurrent-lease cap (16) so the scheduler's outbound pressure
#: tracks the rest of the connector-service plumbing.
DEFAULT_MAX_CONCURRENT_TICKS: Final[int] = 16

#: Default sleep between scheduler passes when no instance is
#: ready. Tight enough that a freshly-resumed instance starts
#: ticking promptly, loose enough that an idle scheduler doesn't
#: spin. Tests use :meth:`PullLoopScheduler.tick_once` directly
#: and never observe this.
DEFAULT_SCAN_INTERVAL_SECONDS: Final[float] = 1.0


_LOGGER = logging.getLogger("custos_connector.scheduler")


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class InstanceSchedule:
    """In-memory scheduling state for one registered instance.

    Mutable on purpose — the scheduler updates the same record on
    every pass rather than allocating a new dataclass per tick.

    ``interval_seconds`` MAY be larger than the global floor but
    MUST NOT be smaller; :meth:`PullLoopScheduler.register` rejects
    smaller values at the boundary so this field is trustworthy at
    use sites.
    """

    workspace_id: str
    instance_id: str
    interval_seconds: int
    next_earliest_tick_at: datetime
    last_tick_at: datetime | None = None
    consecutive_failures: int = 0
    paused: bool = False
    halted: bool = False
    halt_reason: str | None = None
    in_flight: bool = False

    def is_eligible_at(self, now: datetime) -> bool:
        """Return ``True`` when this instance may tick at ``now``."""
        if self.paused or self.halted or self.in_flight:
            return False
        return now >= self.next_earliest_tick_at


@dataclass(frozen=True, slots=True)
class TickOutcome:
    """Per-instance outcome of one :meth:`PullLoopScheduler.tick_once` dispatch.

    Used by tests and by the optional :func:`SchedulerPassResult`
    observer; production code typically only cares about the
    aggregate counts on :class:`SchedulerPassResult`.
    """

    workspace_id: str
    instance_id: str
    #: One of ``"ticked"`` (success), ``"lease_busy"`` (skipped — another
    #: replica owned the cursor lease), ``"halted"`` (the cursor service
    #: raised :class:`CursorHalted`; the schedule is marked ``halted`` and
    #: skipped on subsequent passes until :meth:`PullLoopScheduler.resume`
    #: is called, but the entry remains in the registry), ``"unavailable"``
    #: (the instance is no longer a valid tick target — actually removed
    #: from the registry), or ``"failed"`` (generic exception; backoff
    #: applied).
    status: str
    event_count: int | None = None
    error: BaseException | None = None


@dataclass(frozen=True, slots=True)
class SchedulerPassResult:
    """Aggregate result of one :meth:`PullLoopScheduler.tick_once` pass."""

    started_at: datetime
    finished_at: datetime
    eligible: int
    skipped: int
    outcomes: tuple[TickOutcome, ...] = field(default_factory=tuple)

    @property
    def ticks_attempted(self) -> int:
        return len(self.outcomes)


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------


class PullLoopScheduler:
    """Per-instance pull-loop driver.

    The scheduler is workspace-aware via the ``(workspace_id,
    instance_id)`` registry key but is otherwise a single shared
    object across the connector-service pod — one instance drives
    every registered cursor.
    """

    def __init__(
        self,
        *,
        cursor_service: CursorService,
        min_interval_seconds: int = PULL_LOOP_HARD_FLOOR_SEC,
        backoff_cap_seconds: int = DEFAULT_BACKOFF_CAP_SECONDS,
        max_concurrent_ticks: int = DEFAULT_MAX_CONCURRENT_TICKS,
        scan_interval_seconds: float = DEFAULT_SCAN_INTERVAL_SECONDS,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if min_interval_seconds < PULL_LOOP_HARD_FLOOR_SEC:
            raise ValueError(
                f"min_interval_seconds must be >= {PULL_LOOP_HARD_FLOOR_SEC} "
                f"(design § Single-writer safety pins this floor); got "
                f"{min_interval_seconds!r}"
            )
        if backoff_cap_seconds < min_interval_seconds:
            raise ValueError(
                f"backoff_cap_seconds ({backoff_cap_seconds!r}) must be >= "
                f"min_interval_seconds ({min_interval_seconds!r})"
            )
        if max_concurrent_ticks <= 0:
            raise ValueError(
                f"max_concurrent_ticks must be positive (got {max_concurrent_ticks!r})"
            )
        if scan_interval_seconds <= 0:
            raise ValueError(
                f"scan_interval_seconds must be positive (got {scan_interval_seconds!r})"
            )
        self._cursor = cursor_service
        self._min_interval = min_interval_seconds
        self._backoff_cap = backoff_cap_seconds
        self._scan_interval = scan_interval_seconds
        self._sem = asyncio.Semaphore(max_concurrent_ticks)
        self._clock = clock if clock is not None else lambda: datetime.now(UTC)
        self._schedules: dict[tuple[str, str], InstanceSchedule] = {}
        self._stop_event: asyncio.Event | None = None

    # ------------------------------------------------------------------
    # Registry API
    # ------------------------------------------------------------------

    def register(
        self,
        workspace_id: str,
        instance_id: str,
        *,
        interval_seconds: int | None = None,
    ) -> InstanceSchedule:
        """Add an instance to the tick rotation.

        Re-registering an already-known instance is idempotent: the
        existing :class:`InstanceSchedule` is returned unchanged. To
        change the interval, :meth:`unregister` first and re-register.

        ``interval_seconds`` defaults to ``min_interval_seconds`` and
        MUST NOT go below the design floor of
        :data:`PULL_LOOP_HARD_FLOOR_SEC`.
        """
        key = (workspace_id, instance_id)
        existing = self._schedules.get(key)
        if existing is not None:
            return existing
        interval = interval_seconds if interval_seconds is not None else self._min_interval
        if interval < self._min_interval:
            raise ValueError(
                f"interval_seconds for ({workspace_id!r}, {instance_id!r}) must be "
                f">= {self._min_interval} (got {interval!r})"
            )
        # First-touch instances are eligible immediately so the first
        # pass after registration drives a tick rather than sitting
        # idle for one full interval.
        schedule = InstanceSchedule(
            workspace_id=workspace_id,
            instance_id=instance_id,
            interval_seconds=interval,
            next_earliest_tick_at=self._clock(),
        )
        self._schedules[key] = schedule
        return schedule

    def unregister(self, workspace_id: str, instance_id: str) -> bool:
        """Drop an instance from the rotation. Returns ``True`` if it was registered."""
        return self._schedules.pop((workspace_id, instance_id), None) is not None

    def pause(self, workspace_id: str, instance_id: str) -> None:
        """Stop scheduling ticks for an instance without dropping its state.

        Any in-flight tick completes; no new ticks are dispatched
        until :meth:`resume` is called. Cursor state and consecutive
        failure count are preserved. Mirrors the operator
        ``pull-loop:pause`` endpoint described in design § Pull-loop
        lifecycle operations (CONN-IMPL-024 wires the REST surface).
        """
        sched = self._schedules.get((workspace_id, instance_id))
        if sched is None:
            return
        sched.paused = True

    def resume(self, workspace_id: str, instance_id: str) -> None:
        """Resume tick scheduling. Clears halt state if any.

        Resuming an instance that was halted by a previous
        :class:`CursorHalted` clears the in-memory ``halted`` flag —
        operators are expected to also clear the durable
        ``status`` flip via the admin rewind (CONN-IMPL-024). If
        ``status`` is still a halt sentinel on the next tick, the
        cursor service will refuse the tick with
        :class:`CursorInstanceUnavailable` and the scheduler will
        drop the instance again.
        """
        sched = self._schedules.get((workspace_id, instance_id))
        if sched is None:
            return
        sched.paused = False
        sched.halted = False
        sched.halt_reason = None
        sched.consecutive_failures = 0
        sched.next_earliest_tick_at = self._clock()

    def schedules(self) -> Mapping[tuple[str, str], InstanceSchedule]:
        """Snapshot of the current registry. Intended for tests / metrics.

        The returned mapping is an immutable :class:`MappingProxyType`
        over a shallow copy of the registry — adding or removing entries
        on it has no effect on the scheduler. The contained
        :class:`InstanceSchedule` objects are the live registry values
        and remain mutable; callers must treat them as read-only outside
        of tests (use :meth:`pause` / :meth:`resume` / :meth:`register` /
        :meth:`unregister` to mutate scheduler state).
        """
        return MappingProxyType(dict(self._schedules))

    # ------------------------------------------------------------------
    # Pass driver
    # ------------------------------------------------------------------

    async def tick_once(self) -> SchedulerPassResult:
        """Run one scheduler pass and return its aggregate result.

        Eligible instances are dispatched as concurrent
        :class:`asyncio.Task`\\s bounded by
        ``max_concurrent_ticks``. The method awaits every dispatched
        task before returning, so callers can drive the scheduler
        synchronously from a test.
        """
        started_at = self._clock()
        eligible: list[InstanceSchedule] = []
        skipped = 0
        for sched in list(self._schedules.values()):
            if sched.is_eligible_at(started_at):
                eligible.append(sched)
            else:
                skipped += 1

        if not eligible:
            return SchedulerPassResult(
                started_at=started_at,
                finished_at=self._clock(),
                eligible=0,
                skipped=skipped,
            )

        # Reserve the in-flight slot synchronously so a second
        # concurrent ``tick_once`` cannot pick the same instance.
        for sched in eligible:
            sched.in_flight = True

        outcomes = await asyncio.gather(
            *(self._dispatch(sched) for sched in eligible),
        )
        return SchedulerPassResult(
            started_at=started_at,
            finished_at=self._clock(),
            eligible=len(eligible),
            skipped=skipped,
            outcomes=tuple(outcomes),
        )

    async def _dispatch(self, sched: InstanceSchedule) -> TickOutcome:
        """Run one tick for one instance, releasing the in-flight slot."""
        try:
            async with self._sem:
                return await self._run_tick(sched)
        finally:
            sched.in_flight = False

    async def _run_tick(self, sched: InstanceSchedule) -> TickOutcome:
        """Execute one cursor tick and update the per-instance schedule."""
        try:
            result: TickResult = await self._cursor.tick(sched.workspace_id, sched.instance_id)
        except LeaseBusy as exc:
            # Another replica owns the cursor lease. Treat as a
            # benign skip — schedule the next attempt at now+interval
            # without bumping the failure counter. The "single-writer"
            # claim is enforced by SPL; we do not have to coordinate.
            _LOGGER.debug(
                "skip tick for %s/%s: lease held by another replica (%s)",
                sched.workspace_id,
                sched.instance_id,
                exc,
            )
            self._schedule_next(sched, success=True)
            return TickOutcome(
                workspace_id=sched.workspace_id,
                instance_id=sched.instance_id,
                status="lease_busy",
                error=exc,
            )
        except CursorHalted as exc:
            # The cursor service has already flipped the instance
            # status to a halt sentinel and emitted the matching
            # audit event. Mark the schedule halted — the entry stays
            # in the registry but is_eligible_at() will return False
            # for it until resume() (typically via the admin rewind in
            # CONN-IMPL-024) clears the flag.
            sched.halted = True
            sched.halt_reason = type(exc).__name__
            _LOGGER.info(
                "halted pull loop for %s/%s after %s; awaiting operator action",
                sched.workspace_id,
                sched.instance_id,
                type(exc).__name__,
            )
            return TickOutcome(
                workspace_id=sched.workspace_id,
                instance_id=sched.instance_id,
                status="halted",
                error=exc,
            )
        except CursorInstanceUnavailable as exc:
            # The instance has been deleted, disabled, or is
            # missing a catalog entry. Remove it from the rotation
            # entirely — re-registering is the caller's responsibility
            # once the underlying config is fixed.
            _LOGGER.info(
                "dropping pull loop for %s/%s: %s",
                sched.workspace_id,
                sched.instance_id,
                exc.reason,
            )
            self._schedules.pop((sched.workspace_id, sched.instance_id), None)
            return TickOutcome(
                workspace_id=sched.workspace_id,
                instance_id=sched.instance_id,
                status="unavailable",
                error=exc,
            )
        except Exception as exc:
            now = self._clock()
            sched.consecutive_failures += 1
            backoff = self._backoff_seconds(sched)
            sched.next_earliest_tick_at = now + timedelta(seconds=backoff)
            sched.last_tick_at = now
            _LOGGER.warning(
                "pull tick for %s/%s failed (attempt %d); backing off %ds",
                sched.workspace_id,
                sched.instance_id,
                sched.consecutive_failures,
                backoff,
                exc_info=True,
            )
            return TickOutcome(
                workspace_id=sched.workspace_id,
                instance_id=sched.instance_id,
                status="failed",
                error=exc,
            )

        # Success path.
        self._schedule_next(sched, success=True)
        return TickOutcome(
            workspace_id=sched.workspace_id,
            instance_id=sched.instance_id,
            status="ticked",
            event_count=result.event_count,
        )

    def _schedule_next(self, sched: InstanceSchedule, *, success: bool) -> None:
        """Advance ``next_earliest_tick_at`` after a successful tick / benign skip."""
        if success:
            sched.consecutive_failures = 0
        now = self._clock()
        sched.last_tick_at = now
        sched.next_earliest_tick_at = now + timedelta(seconds=sched.interval_seconds)

    def _backoff_seconds(self, sched: InstanceSchedule) -> int:
        """Exponential backoff capped at ``backoff_cap_seconds``.

        ``2 ** (failures - 1)`` doubles each consecutive failure
        starting from the per-instance interval; capped to avoid
        absurd waits after many failures. Computed in integer
        seconds so the audit / log line is stable.
        """
        # ``failures`` is always >= 1 when this is called from the
        # except branch. Guard against pathological negative inputs.
        attempt = max(sched.consecutive_failures, 1)
        # Cap the exponent so ``2 ** attempt`` doesn't overflow for
        # an instance that has been flapping for a very long time;
        # at ``attempt >= 30`` we're already pinned to the cap.
        capped_attempt = min(attempt, 30)
        candidate = sched.interval_seconds * (2 ** (capped_attempt - 1))
        return int(min(candidate, self._backoff_cap))

    # ------------------------------------------------------------------
    # Long-running driver
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Long-running driver — alternate :meth:`tick_once` and a short sleep.

        Returns when :meth:`stop` is called from another task.
        Production code wires this into the connector-service
        lifespan; tests drive :meth:`tick_once` directly.
        """
        self._stop_event = asyncio.Event()
        while not self._stop_event.is_set():
            try:
                await self.tick_once()
            except Exception:  # pragma: no cover - defensive
                _LOGGER.exception("scheduler pass raised unexpectedly")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._scan_interval)
            except TimeoutError:
                continue

    def stop(self) -> None:
        """Signal :meth:`run` to exit after the current pass."""
        if self._stop_event is not None:
            self._stop_event.set()


__all__ = [
    "DEFAULT_BACKOFF_CAP_SECONDS",
    "DEFAULT_MAX_CONCURRENT_TICKS",
    "DEFAULT_SCAN_INTERVAL_SECONDS",
    "PULL_LOOP_HARD_FLOOR_SEC",
    "InstanceSchedule",
    "PullLoopScheduler",
    "SchedulerPassResult",
    "TickOutcome",
]

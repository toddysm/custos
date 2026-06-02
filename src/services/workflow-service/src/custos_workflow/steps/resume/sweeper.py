"""Periodic TTL garbage-collection of resume-subscription mirrors (WF-IMPL-109).

``design.md`` § *Data Models* states that *TTL-expired mirrors are
garbage-collected on a periodic sweep* — independently of the WF mirror
writes the :class:`~custos_workflow.steps.resume.WaitForStepHandler`
performs on the register / resume / cancel path. The Trigger Service
garbage-collects the *subscription* on ``expiresAt`` (the **TTL expiry**
rule, ``design.md`` § *Resume Subscription Replay Protocol*); this sweep
is the WF-side counterpart that reaps the now-orphaned
:class:`~custos_workflow.steps.resume.ResumeSubscriptionMirror` rows so
the table does not grow without bound for runs that parked on a
``waitFor:`` step and were never resumed (the event never arrived before
TTL).

The :class:`ResumeSubscriptionTtlSweeper` owns that sweep. It is the
unattended, time-driven sibling of the
:class:`~custos_workflow.steps.resume.ResumeSubscriptionCanceller`
(terminal-transition teardown) and the
:class:`~custos_workflow.steps.resume.ResumeSubscriptionReplayReconciler`
(replay re-registration): all three read mirror rows and mutate the
table, but the sweeper fires on a wall-clock interval rather than on a
run event.

Restart-safety & idempotency
-----------------------------

The sweep is **restart-safe**: its only inputs are the mirror rows and
the current time, so a process that crashes mid-sweep simply re-derives
the expired set on the next pass — there is no in-flight state to lose.
It is **idempotent**: deleting an already-deleted mirror is a no-op
(:meth:`ResumeSubscriptionMirrorRepository.delete` swallows an unknown
id), so re-reaping a row another replica already removed is harmless.

Isolation
---------

A single sweep is **best-effort and isolating**: a failure deleting one
expired mirror is logged and recorded on the returned
:class:`TtlSweepReport`, but never aborts the sweep of the remaining
expired rows — one flaky row must not wedge garbage collection. Only a
failure *listing* the expired mirrors propagates out of
:meth:`~ResumeSubscriptionTtlSweeper.sweep_once`; the periodic driver
:meth:`~ResumeSubscriptionTtlSweeper.run_forever` is the resilience
layer that logs and continues so a transient store error cannot kill the
background task.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from custos_workflow.steps.resume.mirror import ResumeSubscriptionMirrorRepository

__all__ = [
    "DEFAULT_RESUME_SUB_SWEEP_INTERVAL_SECONDS",
    "ResumeSubscriptionTtlSweeper",
    "TtlSweepReport",
]

_LOGGER = logging.getLogger(__name__)

#: Default wall-clock interval between TTL sweeps when
#: :data:`~custos_workflow.providers.ENV_RESUME_SUB_SWEEP_INTERVAL`
#: pins no explicit value. Five minutes is coarse relative to the
#: hours/days TTLs ``waitFor:`` subscriptions carry, so expired rows
#: are reaped promptly without the sweep itself becoming a hot loop.
DEFAULT_RESUME_SUB_SWEEP_INTERVAL_SECONDS: float = 300.0


# ---------------------------------------------------------------------------
# Sweep report
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TtlSweepReport:
    """Outcome of a single :class:`ResumeSubscriptionTtlSweeper` pass.

    Every field is a tuple of ``mirror_id``s. ``deleted`` holds the
    expired rows successfully reaped; a ``mirror_id`` lands in
    ``failed`` when its delete raised (its row is left in place for the
    next sweep). A fully clean sweep has an empty ``failed``; a sweep
    that found nothing expired has both tuples empty.
    """

    deleted: tuple[str, ...] = field(default_factory=tuple)
    failed: tuple[str, ...] = field(default_factory=tuple)


# ---------------------------------------------------------------------------
# Sweeper
# ---------------------------------------------------------------------------


class ResumeSubscriptionTtlSweeper:
    """Periodically reaps TTL-expired resume-subscription mirror rows.

    Depends only on the
    :class:`~custos_workflow.steps.resume.ResumeSubscriptionMirrorRepository`
    Protocol (``list_expired`` + idempotent ``delete``), so the
    production Dapr-backed adapter and the in-memory fake are both
    drop-in. The ``clock`` seam lets tests pin "now" deterministically;
    production leaves it at the default UTC wall clock.
    """

    __slots__ = ("_clock", "_mirror_repo")

    def __init__(
        self,
        mirror_repo: ResumeSubscriptionMirrorRepository,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._mirror_repo = mirror_repo
        self._clock = clock

    @property
    def mirror_repo(self) -> ResumeSubscriptionMirrorRepository:
        """The mirror repository this sweeper garbage-collects."""
        return self._mirror_repo

    async def sweep_once(self) -> TtlSweepReport:
        """Reap every mirror whose ``expires_at`` is at or before now.

        Lists the expired rows (``expires_at <= clock()``) and deletes
        each, isolating a per-row delete failure onto the returned
        report so one bad row cannot abort the rest of the sweep.

        :returns: A :class:`TtlSweepReport` describing the pass.
        :raises Exception: Propagates a failure to *list* the expired
            mirrors; :meth:`run_forever` owns that retry. Per-row delete
            failures are isolated onto the report instead.
        """
        now = self._clock()
        expired = await self._mirror_repo.list_expired(now)
        deleted: list[str] = []
        failed: list[str] = []
        for mirror in expired:
            try:
                await self._mirror_repo.delete(mirror.mirror_id)
            except Exception:
                # Isolate one row's delete failure; leave the row so the
                # next sweep retries the idempotent delete.
                _LOGGER.exception(
                    "failed to delete expired resume subscription mirror %s "
                    "(run %s, step %s, event %s); leaving the row for the next sweep",
                    mirror.mirror_id,
                    mirror.run_id,
                    mirror.step_id,
                    mirror.event_key,
                )
                failed.append(mirror.mirror_id)
                continue
            deleted.append(mirror.mirror_id)
        if deleted or failed:
            _LOGGER.info(
                "resume subscription TTL sweep reaped %d expired mirror(s) (%d failed)",
                len(deleted),
                len(failed),
            )
        return TtlSweepReport(deleted=tuple(deleted), failed=tuple(failed))

    async def run_forever(self, interval_seconds: float) -> None:
        """Sweep on a fixed wall-clock interval until cancelled.

        Runs :meth:`sweep_once`, then sleeps ``interval_seconds`` and
        repeats. The loop is the resilience layer: any exception a sweep
        raises (e.g. a transient store error listing the expired rows)
        is logged and swallowed so the background task survives to the
        next pass. The loop exits only on
        :class:`asyncio.CancelledError`, which the FastAPI lifespan
        raises by cancelling the task on shutdown.

        :param interval_seconds: Seconds to wait between sweeps.
        """
        while True:
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOGGER.exception(
                    "resume subscription TTL sweep iteration failed; continuing to the next pass"
                )
            await asyncio.sleep(interval_seconds)

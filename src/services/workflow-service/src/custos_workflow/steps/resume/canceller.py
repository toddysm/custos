"""Terminal cancellation of open resume subscriptions (WF-IMPL-106).

When a step or a whole run reaches a terminal state — including an
operator-initiated *Cancel Run* (``design.md`` § *Operation: Cancel
Run*) — every still-open resume subscription for that run must be
cancelled with the Trigger Service and its mirror row removed, per the
Resume Subscription Replay Protocol's **cancellation rule** (rule 5):

    On step or run terminal transition, WF calls
    ``CancelResumeSubscription(runId, stepId, eventKey)`` for each open
    mirror, then deletes the mirror rows. TS treats
    ``CancelResumeSubscription`` as idempotent — cancelling an unknown
    or already-expired key is a no-op.

The :class:`ResumeSubscriptionCanceller` owns that sweep. It is the
Resume Subscription Manager counterpart to the
:class:`~custos_workflow.steps.resume.ResumeSubscriptionReplayReconciler`
(replay re-registration) — both read the run's open
:class:`~custos_workflow.steps.resume.ResumeSubscriptionMirror` rows and
drive the Trigger Service, but the canceller tears subscriptions *down*
where the reconciler builds them back *up*.

Sweep ordering & isolation
---------------------------

Each mirror is cancelled with the Trigger Service **before** its row is
deleted, so a crash between the cancel and the delete leaves an open
mirror that a later sweep (or the replay reconciler) will re-process —
never a deleted row pointing at a still-live Trigger Service
subscription. The Trigger Service cancel is idempotent, so re-cancelling
on the retry is safe.

The sweep is **best-effort and isolating**: a failure cancelling or
deleting one mirror is logged and recorded on the returned
:class:`CancelSweepReport`, but never aborts the sweep of the remaining
mirrors — a run terminal transition must not wedge on a single flaky
key. Only a failure *listing* the open mirrors propagates (the caller
owns that retry, exactly as the reconciler does for ``list_open``).

The async ``cancel_run`` / ``cancel_step`` entry points match the
async :class:`~custos_workflow.steps.resume.ResumeSubscriptionMirrorRepository`
boundary and the async ``RunController.cancel_run`` call site; the
production controller wiring lands with the rest of the resume path in
WF-IMPL-108.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass, field

from custos_workflow.clients.trigger import (
    CancelResumeSubscriptionRequest,
    TriggerServiceClient,
)
from custos_workflow.steps.resume.mirror import (
    ResumeSubscriptionMirror,
    ResumeSubscriptionMirrorRepository,
)

__all__ = [
    "CancelSweepReport",
    "ResumeSubscriptionCanceller",
]

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Sweep report
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CancelSweepReport:
    """Outcome of a :class:`ResumeSubscriptionCanceller` sweep.

    Every field is a tuple of ``mirror_id``s. ``deleted`` is the subset
    of ``cancelled`` whose mirror row was also removed; a ``mirror_id``
    appears in ``failed`` when either its Trigger Service cancel or its
    mirror-row delete raised. A fully clean sweep has
    ``cancelled == deleted`` and an empty ``failed``.
    """

    cancelled: tuple[str, ...] = field(default_factory=tuple)
    deleted: tuple[str, ...] = field(default_factory=tuple)
    failed: tuple[str, ...] = field(default_factory=tuple)


# ---------------------------------------------------------------------------
# Canceller
# ---------------------------------------------------------------------------


class ResumeSubscriptionCanceller:
    """Cancels a run's (or a single step's) open resume subscriptions.

    Depends only on the
    :class:`~custos_workflow.clients.trigger.TriggerServiceClient`
    Protocol (cancel transport) and the
    :class:`~custos_workflow.steps.resume.ResumeSubscriptionMirrorRepository`
    Protocol (open-mirror source of truth), so the production Dapr
    adapter and the in-memory fakes are both drop-in.
    """

    __slots__ = ("_mirror_repo", "_trigger_client")

    def __init__(
        self,
        mirror_repo: ResumeSubscriptionMirrorRepository,
        trigger_client: TriggerServiceClient,
    ) -> None:
        self._mirror_repo = mirror_repo
        self._trigger_client = trigger_client

    @property
    def mirror_repo(self) -> ResumeSubscriptionMirrorRepository:
        """The mirror repository this canceller sweeps."""
        return self._mirror_repo

    async def cancel_run(self, run_id: str) -> CancelSweepReport:
        """Cancel every open resume subscription for ``run_id``.

        Implements ``design.md`` § *Operation: Cancel Run* — lists the
        run's open mirrors and cancels + deletes each. Issues exactly
        one (idempotent) Trigger Service cancel per open mirror.

        :param run_id: The run whose open subscriptions to tear down.
        :returns: A :class:`CancelSweepReport` describing the sweep.
        :raises Exception: Propagates a failure to *list* the open
            mirrors; the caller owns that retry. Per-mirror cancel /
            delete failures are isolated onto the report instead.
        """
        mirrors = await self._mirror_repo.list_open(run_id)
        return await self._sweep(mirrors)

    async def cancel_step(self, run_id: str, step_id: str) -> CancelSweepReport:
        """Cancel the open resume subscriptions for a single terminal step.

        The step-scoped counterpart to :meth:`cancel_run`, used when a
        single ``waitFor:`` step reaches a terminal transition while the
        rest of the run keeps running.

        :param run_id: The owning run.
        :param step_id: The terminal step whose subscriptions to cancel.
        :returns: A :class:`CancelSweepReport` describing the sweep.
        :raises Exception: Propagates a failure to *list* the step's
            open mirrors; per-mirror failures are isolated onto the
            report.
        """
        mirrors = await self._mirror_repo.list_open_for_step(run_id, step_id)
        return await self._sweep(mirrors)

    async def _sweep(self, mirrors: Iterable[ResumeSubscriptionMirror]) -> CancelSweepReport:
        """Cancel + delete each mirror, isolating per-mirror failures."""
        cancelled: list[str] = []
        deleted: list[str] = []
        failed: list[str] = []
        for mirror in mirrors:
            try:
                self._trigger_client.cancel_resume_subscription(
                    CancelResumeSubscriptionRequest(
                        run_id=mirror.run_id,
                        step_id=mirror.step_id,
                        event_key=mirror.event_key,
                    )
                )
            except Exception:
                # Isolate one mirror's cancel failure; keep its row so a
                # later sweep / reconcile retries the idempotent cancel.
                _LOGGER.exception(
                    "failed to cancel resume subscription for mirror %s "
                    "(run %s, step %s, event %s); leaving the mirror row in place",
                    mirror.mirror_id,
                    mirror.run_id,
                    mirror.step_id,
                    mirror.event_key,
                )
                failed.append(mirror.mirror_id)
                continue
            cancelled.append(mirror.mirror_id)
            try:
                await self._mirror_repo.delete(mirror.mirror_id)
            except Exception:
                # The subscription is already cancelled, but the stale row
                # is NOT harmless: the replay reconciler re-registers every
                # mirror returned by ``list_open`` on the next orchestrator
                # entry, so a row that never deletes will resurrect the
                # just-cancelled subscription with the Trigger Service
                # (churn / resource leak). It is recorded as ``failed`` so a
                # later sweep retries the delete; the cancel itself stays
                # idempotent.
                _LOGGER.exception(
                    "cancelled resume subscription for mirror %s (run %s, "
                    "step %s, event %s) but failed to delete the mirror row; "
                    "the replay reconciler may re-register it until the row "
                    "is removed",
                    mirror.mirror_id,
                    mirror.run_id,
                    mirror.step_id,
                    mirror.event_key,
                )
                failed.append(mirror.mirror_id)
                continue
            deleted.append(mirror.mirror_id)
        return CancelSweepReport(
            cancelled=tuple(cancelled),
            deleted=tuple(deleted),
            failed=tuple(failed),
        )

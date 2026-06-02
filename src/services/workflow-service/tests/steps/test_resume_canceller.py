"""Tests for the :class:`ResumeSubscriptionCanceller` (WF-IMPL-106).

The canceller tears down a run's (or a single step's) open
:class:`~custos_workflow.steps.resume.ResumeSubscriptionMirror` rows on a
terminal transition (``design.md`` § *Operation: Cancel Run* / Resume
Subscription Replay Protocol rule 5). Coverage targets every acceptance
criterion from #545:

* Cancelling a run with N open waits issues N idempotent cancels and
  removes all mirror rows.
* Cancelling an unknown / already-expired key is a no-op.

plus the supporting edges: empty open set, step-scoped sweep, per-mirror
cancel-failure isolation, delete-failure isolation, and ``list_open``
failure propagation.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from custos_workflow.clients.trigger import (
    CancelResumeSubscriptionRequest,
    FakeTriggerServiceClient,
    RegisterResumeSubscriptionRequest,
    RegisterResumeSubscriptionResponse,
)
from custos_workflow.steps.resume import (
    CancelSweepReport,
    InMemoryResumeSubscriptionMirrorRepository,
    ResumeSubscriptionCanceller,
    ResumeSubscriptionMirror,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_EXPIRES = _NOW + timedelta(hours=24)


def _mirror(
    *,
    step_id: str = "await-event",
    event_key: str = "order-approved",
    ts_subscription_id: str = "ts-sub-1",
    run_id: str = "run-1",
    mirror_id: str | None = None,
) -> ResumeSubscriptionMirror:
    return ResumeSubscriptionMirror(
        mirror_id=mirror_id or f"rsm-{step_id}",
        run_id=run_id,
        step_id=step_id,
        event_key=event_key,
        ts_subscription_id=ts_subscription_id,
        registered_at=_NOW,
        expires_at=_EXPIRES,
    )


async def _seed(
    repo: InMemoryResumeSubscriptionMirrorRepository, *mirrors: ResumeSubscriptionMirror
) -> None:
    for mirror in mirrors:
        await repo.put(mirror)


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _OneCancelFailsTrigger:
    """Wraps a :class:`FakeTriggerServiceClient`, raising for one step_id."""

    def __init__(self, inner: FakeTriggerServiceClient, *, failing_step_id: str) -> None:
        self._inner = inner
        self._failing_step_id = failing_step_id

    def register_resume_subscription(
        self, request: RegisterResumeSubscriptionRequest
    ) -> RegisterResumeSubscriptionResponse:
        return self._inner.register_resume_subscription(request)

    def cancel_resume_subscription(self, request: CancelResumeSubscriptionRequest) -> None:
        if request.step_id == self._failing_step_id:
            raise RuntimeError(f"trigger service down for step {request.step_id}")
        self._inner.cancel_resume_subscription(request)


class _DeleteFailsRepo(InMemoryResumeSubscriptionMirrorRepository):
    """Mirror repo whose ``delete`` raises for one mirror id."""

    def __init__(self, *, failing_mirror_id: str) -> None:
        super().__init__()
        self._failing_mirror_id = failing_mirror_id

    async def delete(self, mirror_id: str) -> None:
        if mirror_id == self._failing_mirror_id:
            raise RuntimeError(f"store down deleting {mirror_id}")
        await super().delete(mirror_id)


class _ListFailsRepo(InMemoryResumeSubscriptionMirrorRepository):
    """Mirror repo whose ``list_open`` raises."""

    async def list_open(self, run_id: str) -> tuple[ResumeSubscriptionMirror, ...]:
        raise RuntimeError("store down")


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_mirror_repo_property(self) -> None:
        repo = InMemoryResumeSubscriptionMirrorRepository()
        canceller = ResumeSubscriptionCanceller(repo, FakeTriggerServiceClient())
        assert canceller.mirror_repo is repo

    def test_report_defaults_empty(self) -> None:
        report = CancelSweepReport()
        assert report.cancelled == ()
        assert report.deleted == ()
        assert report.failed == ()


# ---------------------------------------------------------------------------
# cancel_run
# ---------------------------------------------------------------------------


class TestCancelRun:
    async def test_empty_open_set_is_noop(self) -> None:
        repo = InMemoryResumeSubscriptionMirrorRepository()
        trigger = FakeTriggerServiceClient()
        canceller = ResumeSubscriptionCanceller(repo, trigger)

        report = await canceller.cancel_run("run-1")

        assert report == CancelSweepReport()
        assert trigger.cancel_calls == []

    async def test_cancels_all_open_and_removes_rows(self) -> None:
        repo = InMemoryResumeSubscriptionMirrorRepository()
        trigger = FakeTriggerServiceClient()
        mirrors = (
            _mirror(step_id="a", event_key="ev-a", mirror_id="rsm-a"),
            _mirror(step_id="b", event_key="ev-b", mirror_id="rsm-b"),
            _mirror(step_id="c", event_key="ev-c", mirror_id="rsm-c"),
        )
        # Pre-register each so the fake holds a live subscription per key.
        for m in mirrors:
            trigger.subscriptions[(m.run_id, m.step_id, m.event_key)] = m.ts_subscription_id
        await _seed(repo, *mirrors)
        canceller = ResumeSubscriptionCanceller(repo, trigger)

        report = await canceller.cancel_run("run-1")

        # N idempotent cancels, all rows removed.
        assert len(trigger.cancel_calls) == 3
        assert sorted(report.cancelled) == ["rsm-a", "rsm-b", "rsm-c"]
        assert sorted(report.deleted) == ["rsm-a", "rsm-b", "rsm-c"]
        assert report.failed == ()
        assert await repo.list_open("run-1") == ()
        assert trigger.subscriptions == {}

    async def test_unknown_key_is_noop(self) -> None:
        # A mirror whose key the Trigger Service no longer knows (expired
        # / already cancelled): cancel is a clean no-op, row still removed.
        repo = InMemoryResumeSubscriptionMirrorRepository()
        trigger = FakeTriggerServiceClient()  # no seeded subscriptions
        await _seed(repo, _mirror(mirror_id="rsm-x"))
        canceller = ResumeSubscriptionCanceller(repo, trigger)

        report = await canceller.cancel_run("run-1")

        assert len(trigger.cancel_calls) == 1
        assert report.cancelled == ("rsm-x",)
        assert report.deleted == ("rsm-x",)
        assert report.failed == ()
        assert await repo.list_open("run-1") == ()

    async def test_only_target_run_swept(self) -> None:
        repo = InMemoryResumeSubscriptionMirrorRepository()
        trigger = FakeTriggerServiceClient()
        await _seed(
            repo,
            _mirror(step_id="a", run_id="run-1", mirror_id="rsm-1a"),
            _mirror(step_id="b", run_id="run-2", mirror_id="rsm-2b"),
        )
        canceller = ResumeSubscriptionCanceller(repo, trigger)

        report = await canceller.cancel_run("run-1")

        assert report.deleted == ("rsm-1a",)
        # run-2's mirror is untouched.
        remaining = await repo.list_open("run-2")
        assert [m.mirror_id for m in remaining] == ["rsm-2b"]

    async def test_cancel_failure_isolated(self) -> None:
        repo = InMemoryResumeSubscriptionMirrorRepository()
        trigger = _OneCancelFailsTrigger(FakeTriggerServiceClient(), failing_step_id="bad")
        await _seed(
            repo,
            _mirror(step_id="a", event_key="ev-a", mirror_id="rsm-a"),
            _mirror(step_id="bad", event_key="ev-bad", mirror_id="rsm-bad"),
            _mirror(step_id="c", event_key="ev-c", mirror_id="rsm-c"),
        )
        canceller = ResumeSubscriptionCanceller(repo, trigger)

        report = await canceller.cancel_run("run-1")

        assert sorted(report.cancelled) == ["rsm-a", "rsm-c"]
        assert sorted(report.deleted) == ["rsm-a", "rsm-c"]
        assert report.failed == ("rsm-bad",)
        # The failed mirror's row is kept for a later retry.
        remaining = await repo.list_open("run-1")
        assert [m.mirror_id for m in remaining] == ["rsm-bad"]

    async def test_delete_failure_isolated(self) -> None:
        repo = _DeleteFailsRepo(failing_mirror_id="rsm-bad")
        trigger = FakeTriggerServiceClient()
        await _seed(
            repo,
            _mirror(step_id="a", event_key="ev-a", mirror_id="rsm-a"),
            _mirror(step_id="bad", event_key="ev-bad", mirror_id="rsm-bad"),
        )
        canceller = ResumeSubscriptionCanceller(repo, trigger)

        report = await canceller.cancel_run("run-1")

        # Both were cancelled; only the good one's row was deleted.
        assert sorted(report.cancelled) == ["rsm-a", "rsm-bad"]
        assert report.deleted == ("rsm-a",)
        assert report.failed == ("rsm-bad",)

    async def test_list_open_failure_propagates(self) -> None:
        canceller = ResumeSubscriptionCanceller(_ListFailsRepo(), FakeTriggerServiceClient())
        with pytest.raises(RuntimeError, match="store down"):
            await canceller.cancel_run("run-1")


# ---------------------------------------------------------------------------
# cancel_step
# ---------------------------------------------------------------------------


class TestCancelStep:
    async def test_only_target_step_swept(self) -> None:
        repo = InMemoryResumeSubscriptionMirrorRepository()
        trigger = FakeTriggerServiceClient()
        await _seed(
            repo,
            _mirror(step_id="wait-a", event_key="ev-a", mirror_id="rsm-a"),
            _mirror(step_id="wait-b", event_key="ev-b", mirror_id="rsm-b"),
        )
        canceller = ResumeSubscriptionCanceller(repo, trigger)

        report = await canceller.cancel_step("run-1", "wait-a")

        assert report.deleted == ("rsm-a",)
        assert len(trigger.cancel_calls) == 1
        assert trigger.cancel_calls[0].step_id == "wait-a"
        # The sibling step's subscription is untouched.
        remaining = await repo.list_open("run-1")
        assert [m.mirror_id for m in remaining] == ["rsm-b"]

    async def test_no_open_for_step_is_noop(self) -> None:
        repo = InMemoryResumeSubscriptionMirrorRepository()
        trigger = FakeTriggerServiceClient()
        await _seed(repo, _mirror(step_id="wait-a", mirror_id="rsm-a"))
        canceller = ResumeSubscriptionCanceller(repo, trigger)

        report = await canceller.cancel_step("run-1", "wait-zzz")

        assert report == CancelSweepReport()
        assert trigger.cancel_calls == []

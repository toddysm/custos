"""Scheduler capability invariants independent of a particular storage backend."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from custos_spl.errors import ImmutableViolation, LeaseExpired
from custos_spl.ids import SubscriptionId, WorkflowId, WorkspaceId
from custos_spl.interfaces.metadata_store import Schedule, Subscription, SubscriptionSelector
from custos_spl.interfaces.scheduler import (
    SchedulerLease,
    lease_duration,
    prepare_schedule_fire,
    require_scheduler_lease,
)
from custos_spl.interfaces.trigger_queries import SubscriptionSnapshot

_NOW = datetime(2026, 11, 1, 7, tzinfo=UTC)
_WS = WorkspaceId("ws")


def _snapshot() -> SubscriptionSnapshot:
    """Create one active, revisioned source with explicit managed-schedule ownership."""
    row = Subscription(_WS, SubscriptionId("s"), WorkflowId("wf"), "active", _NOW, _NOW)
    selector = SubscriptionSelector(
        _WS,
        SubscriptionId("s"),
        {
            "sourceType": "scheduled",
            "sourceConfig": {"cron": "30 1 * * *"},
            "_revision": 1,
            "_scheduleId": "s",
        },
        _NOW,
    )
    return SubscriptionSnapshot(row, selector)


def _lease() -> SchedulerLease:
    """Return a live provider observation."""
    return SchedulerLease("d", "holder", 1, _NOW + timedelta(seconds=30), _NOW)


@pytest.mark.parametrize("seconds", [0, -1, True, 10**30])
def test_invalid_lease_durations(seconds: int) -> None:
    """Invalid or unrepresentable durations fail rather than becoming defaults."""
    with pytest.raises(ValueError):
        lease_duration("d", "holder", seconds)


def test_fencing_uses_provider_state_not_token_expiry() -> None:
    """Client-side expiry changes cannot extend ownership or invalidate a renewed live epoch."""
    token = _lease()
    forged = replace(token, expires_at=datetime.max.replace(tzinfo=UTC))
    with pytest.raises(LeaseExpired):
        require_scheduler_lease(token, forged, token.expires_at)
    renewed = replace(token, expires_at=token.expires_at + timedelta(seconds=30))
    require_scheduler_lease(renewed, token, token.expires_at)
    with pytest.raises(LeaseExpired):
        require_scheduler_lease(replace(renewed, epoch=2), token, _NOW)


def test_planned_fire_identity_uses_utc_instants_not_fold_equality() -> None:
    """Equivalent zones give identical IDs while repeated-hour occurrences stay distinct."""
    token = _lease()
    first = datetime(2026, 11, 1, 1, 30, tzinfo=ZoneInfo("America/New_York"), fold=0)
    second = first.replace(fold=1)
    pending = []
    for due in (first, first.astimezone(UTC), second):
        pending.append(
            prepare_schedule_fire(
                _WS,
                token,
                token,
                _snapshot(),
                Schedule(_WS, "s", WorkflowId("wf"), "", due, True),
                expected_next_fire_at=due,
                expected_revision=1,
                planned_fire_at=due,
                next_fire_at=_NOW + timedelta(hours=1),
                now=_NOW,
            )
        )
    assert pending[0].fire_id == pending[1].fire_id
    assert pending[0].fire_id != pending[2].fire_id


@pytest.mark.parametrize("change", ["disabled", "foreign", "workflow", "revision", "unowned"])
def test_changed_schedule_or_subscription_is_rejected(change: str) -> None:
    """Eligibility, workspace, workflow, revision and managed association are all fenced."""
    snapshot = _snapshot()
    due = _NOW - timedelta(seconds=30)
    schedule = Schedule(_WS, "s", WorkflowId("wf"), "", due, True)
    revision = 1
    if change == "disabled":
        schedule = replace(schedule, enabled=False)
    elif change == "foreign":
        schedule = replace(schedule, workspace_id=WorkspaceId("other"))
    elif change == "workflow":
        schedule = replace(schedule, workflow_id=WorkflowId("other"))
    elif change == "revision":
        revision = 2
    else:
        assert snapshot.selector is not None
        snapshot = replace(snapshot, selector=replace(snapshot.selector, selector={}))
    with pytest.raises(ImmutableViolation):
        prepare_schedule_fire(
            _WS,
            _lease(),
            _lease(),
            snapshot,
            schedule,
            expected_next_fire_at=due,
            expected_revision=revision,
            planned_fire_at=due,
            next_fire_at=_NOW + timedelta(minutes=1),
            now=_NOW,
        )


@pytest.mark.parametrize(
    ("planned", "following"),
    [
        (_NOW + timedelta(seconds=1), _NOW + timedelta(minutes=1)),
        (_NOW - timedelta(seconds=60), _NOW + timedelta(minutes=1)),
        (_NOW, _NOW),
        (_NOW.replace(tzinfo=None), _NOW + timedelta(minutes=1)),
    ],
)
def test_invalid_planned_fire_windows(planned: datetime, following: datetime) -> None:
    """Caller timestamps cannot advance an undeliverable, future or non-monotonic occurrence."""
    due = _NOW - timedelta(seconds=30)
    with pytest.raises(ValueError):
        prepare_schedule_fire(
            _WS,
            _lease(),
            _lease(),
            _snapshot(),
            Schedule(_WS, "s", WorkflowId("wf"), "", due, True),
            expected_next_fire_at=due,
            expected_revision=1,
            planned_fire_at=planned,
            next_fire_at=following,
            now=_NOW,
        )


def test_elapsed_valid_window_is_a_retryable_conflict() -> None:
    """Time crossing a valid plan's upper bound is not malformed caller input."""
    due = _NOW - timedelta(seconds=2)
    with pytest.raises(ImmutableViolation, match="planned-fire window"):
        prepare_schedule_fire(
            _WS,
            _lease(),
            _lease(),
            _snapshot(),
            Schedule(_WS, "s", WorkflowId("wf"), "", due, True),
            expected_next_fire_at=due,
            expected_revision=1,
            planned_fire_at=_NOW - timedelta(seconds=1),
            next_fire_at=_NOW,
            now=_NOW,
        )

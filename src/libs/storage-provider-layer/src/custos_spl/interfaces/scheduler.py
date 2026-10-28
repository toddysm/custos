"""Deployment-scoped scheduler fencing and durable planned-fire contracts.

The provider clock is authoritative. Epochs survive release/expiry and increase
on acquisition, not renewal. Scheduler writers must use this capability rather
than the legacy unfenced schedule maintenance methods.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol, runtime_checkable

from custos_spl.errors import ImmutableViolation, LeaseExpired
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.metadata_store import Schedule
from custos_spl.interfaces.trigger_queries import SubscriptionSnapshot
from custos_spl.pagination import Cursor, Page

SCHEDULER_SCHEMA_REVISION = 6


def utc_instant(value: datetime) -> datetime:
    """Reject ambiguous naive values and compare repeated-hour timestamps as instants."""
    if value.utcoffset() is None:
        raise ValueError("scheduler timestamps must be timezone-aware")
    return value.astimezone(UTC)


def lease_duration(deployment_id: str, holder_id: str, seconds: int) -> timedelta:
    """Validate identities and a representable positive lease duration before backend access."""
    if (
        not isinstance(deployment_id, str)
        or not deployment_id.strip()
        or not isinstance(holder_id, str)
        or not holder_id.strip()
    ):
        raise ValueError("deployment_id and holder_id must be nonblank")
    if type(seconds) is not int or seconds <= 0:
        raise ValueError("lease seconds must be a positive integer")
    try:
        duration = timedelta(seconds=seconds)
    except OverflowError as exc:
        raise ValueError("lease duration is out of range") from exc
    if duration > datetime.max - datetime.min:
        raise ValueError("lease duration exceeds the datetime range")
    return duration


@dataclass(frozen=True, slots=True)
class SchedulerLease:
    """A fencing token and provider-time observation, not a caller-owned expiry decision."""

    deployment_id: str
    holder_id: str
    epoch: int
    expires_at: datetime
    observed_at: datetime

    def __post_init__(self) -> None:
        """Validate token shape without trusting its expiry at the storage boundary."""
        lease_duration(self.deployment_id, self.holder_id, 1)
        if type(self.epoch) is not int or self.epoch <= 0:
            raise ValueError("scheduler epoch must be a positive integer")
        utc_instant(self.expires_at)
        utc_instant(self.observed_at)


@dataclass(frozen=True, slots=True)
class PendingScheduleFire:
    """Immutable planned occurrence retained until acknowledged by a current leader.

    The creation epoch is provenance, not a delivery lock: a later leader can
    recover the same occurrence. Publication itself belongs to the receiver.
    """

    workspace_id: WorkspaceId
    fire_id: str
    schedule_id: str
    workflow_id: str
    deployment_id: str
    epoch: int
    subscription_revision: int
    planned_fire_at: datetime
    created_at: datetime
    delivered_at: datetime | None = None


def require_scheduler_lease(
    current: SchedulerLease | None, token: SchedulerLease, now: datetime
) -> None:
    """Fence expired, released, foreign-holder and old-epoch tokens using provider state."""
    if (
        current is None
        or current.deployment_id != token.deployment_id
        or current.holder_id != token.holder_id
        or current.epoch != token.epoch
        or utc_instant(current.expires_at) <= utc_instant(now)
    ):
        raise LeaseExpired("scheduler lease is expired or no longer owned")


def prepare_schedule_fire(
    workspace_id: WorkspaceId,
    token: SchedulerLease,
    current_lease: SchedulerLease | None,
    subscription: SubscriptionSnapshot | None,
    schedule: Schedule | None,
    *,
    expected_next_fire_at: datetime,
    expected_revision: int,
    planned_fire_at: datetime,
    next_fire_at: datetime,
    now: datetime,
) -> PendingScheduleFire:
    """Validate the fenced due occurrence before either schedule or outbox changes."""
    require_scheduler_lease(current_lease, token, now)
    due = utc_instant(expected_next_fire_at)
    planned, following, clock = map(utc_instant, (planned_fire_at, next_fire_at, now))
    if type(expected_revision) is not int or expected_revision < 0:
        raise ValueError("expected_revision must be a non-negative integer")
    if not due <= planned <= clock < following:
        raise ValueError("planned fire must be due and next fire must follow provider time")
    if subscription is None or subscription.selector is None or schedule is None:
        raise ImmutableViolation("managed schedule or subscription is missing")
    row, blob = subscription.subscription, subscription.selector.selector
    if (
        row.workspace_id != workspace_id
        or schedule.workspace_id != workspace_id
        or row.subscription_id != schedule.schedule_id
        or row.workflow_id != schedule.workflow_id
        or row.state != "active"
        or not schedule.enabled
        or utc_instant(schedule.next_fire_at) != due
        or blob.get("_revision", 0) != expected_revision
        or blob.get("_scheduleId") != schedule.schedule_id
        or blob.get("sourceType") != "scheduled"
        or not isinstance(blob.get("sourceConfig"), dict)
    ):
        raise ImmutableViolation("schedule eligibility or configuration changed before advancement")
    identity = json.dumps(
        [schedule.schedule_id, planned.isoformat(timespec="microseconds")],
        separators=(",", ":"),
    ).encode()
    return PendingScheduleFire(
        workspace_id=workspace_id,
        fire_id="sched_" + hashlib.sha256(identity).hexdigest(),
        schedule_id=schedule.schedule_id,
        workflow_id=str(schedule.workflow_id),
        deployment_id=token.deployment_id,
        epoch=token.epoch,
        subscription_revision=expected_revision,
        planned_fire_at=planned,
        created_at=clock,
    )


@runtime_checkable
class SchedulerStateProvider(Protocol):
    """Optional storage capability; all mutations use provider time and authoritative fencing."""

    async def check_scheduler_ready(self) -> None:
        """Raise MigrationRequired or BackendUnavailable if scheduler storage is unavailable."""
        ...

    async def scheduler_now(self) -> datetime:
        """Return provider time for planning a due scan, never the caller's wall clock."""
        ...

    async def acquire_scheduler_lease(
        self, deployment_id: str, holder_id: str, *, ttl_seconds: int
    ) -> SchedulerLease | None:
        """Acquire after expiry/release; return None for contention.

        A live same-holder retry returns the existing epoch/expiry without an
        implicit renewal. Every acquisition after expiry advances the epoch.
        """
        ...

    async def renew_scheduler_lease(
        self, lease: SchedulerLease, *, ttl_seconds: int
    ) -> SchedulerLease:
        """Extend only the current live epoch; otherwise raise LeaseExpired."""
        ...

    async def release_scheduler_lease(self, lease: SchedulerLease) -> None:
        """Expire the current live lease without deleting/resetting its fencing epoch."""
        ...

    async def advance_schedule(
        self,
        workspace_id: WorkspaceId,
        schedule_id: str,
        lease: SchedulerLease,
        *,
        expected_next_fire_at: datetime,
        expected_revision: int,
        planned_fire_at: datetime,
        next_fire_at: datetime,
    ) -> PendingScheduleFire:
        """Atomically persist one pending occurrence and advance a due managed schedule.

        LeaseExpired rejects stale holders; ImmutableViolation rejects changed
        eligibility/revisions or duplicate occurrences. No partial write survives.
        """
        ...

    async def query_pending_schedule_fires(
        self,
        workspace_id: WorkspaceId,
        deployment_id: str,
        *,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[PendingScheduleFire]:
        """Return a bounded workspace/deployment-bound page of undelivered occurrences."""
        ...

    async def acknowledge_schedule_fire(
        self, workspace_id: WorkspaceId, fire_id: str, lease: SchedulerLease
    ) -> None:
        """Record acknowledgment under a live fence, including recovery of an older epoch."""
        ...

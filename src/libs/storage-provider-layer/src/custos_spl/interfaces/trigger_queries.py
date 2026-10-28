"""Workspace-scoped reads and atomic resume registration for trigger metadata.

This additive capability complements ``MetadataStoreProvider`` without requiring
unrelated metadata consumers to implement trigger queries. Pages use ascending
identifier order; callers must retain the same filters and time cutoff while
paging. Concurrent writes are visible on later calls, not a snapshot transaction.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Literal, Protocol, runtime_checkable

from custos_spl.errors import ImmutableViolation, WorkspaceMismatch
from custos_spl.ids import SubscriptionId, WorkspaceId
from custos_spl.interfaces.metadata_store import (
    ResumeSubscription,
    Schedule,
    Subscription,
    SubscriptionSelector,
)
from custos_spl.pagination import Cursor, Page


@dataclass(frozen=True, slots=True)
class SubscriptionSnapshot:
    """A base subscription and its most recently appended selector revision.

    Append order, not the caller-supplied ``added_at`` timestamp, determines
    the latest revision. ``selector`` is absent before the first append.
    """

    subscription: Subscription
    selector: SubscriptionSelector | None


@runtime_checkable
class TriggerQueryProvider(Protocol):
    """Read capability required by production Trigger Service stores.

    Missing point reads return ``None``. Query limits default to 50 and are
    capped at 200; non-positive limits and foreign/malformed cursors raise
    ``ValueError``. Every time cutoff must be timezone-aware.
    """

    async def get_subscription(
        self, workspace_id: WorkspaceId, subscription_id: SubscriptionId
    ) -> SubscriptionSnapshot | None:
        """Read a subscription and latest selector in one provider operation."""
        ...

    async def query_subscriptions(
        self,
        workspace_id: WorkspaceId,
        *,
        state: str | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[SubscriptionSnapshot]:
        """Page candidates by subscription ID, optionally restricted to state."""
        ...

    async def get_resume_subscription(
        self, workspace_id: WorkspaceId, resume_id: str
    ) -> ResumeSubscription | None:
        """Read a resume registration, including expired rows for re-registration."""
        ...

    async def query_resume_subscriptions(
        self,
        workspace_id: WorkspaceId,
        *,
        connector_instance_id: str,
        delivery_mode: Literal["push", "pull"],
        as_of: datetime,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ResumeSubscription]:
        """Page unexpired source-associated waits, ordered by resume ID.

        Match ``payload.source.connectorInstanceId`` and membership in
        ``payload.source.deliveryModes``. Legacy waits without a source are
        excluded; equality with ``as_of`` counts as expired.
        """
        ...

    async def get_schedule(self, workspace_id: WorkspaceId, schedule_id: str) -> Schedule | None:
        """Read one schedule without filtering its enabled or due state."""
        ...

    async def query_due_schedules(
        self,
        workspace_id: WorkspaceId,
        *,
        due_at: datetime,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[Schedule]:
        """Page enabled schedules with next_fire_at <= due_at, by schedule ID."""
        ...


@runtime_checkable
class ResumeRegistrationProvider(Protocol):
    """Atomic original-wins registration, separate from immutable raw resume writes."""

    async def register_resume_subscription(
        self, workspace_id: WorkspaceId, resume: ResumeSubscription, *, as_of: datetime
    ) -> ResumeSubscription:
        """Insert or replace an expired row; return the winner when a live row exists.

        ``as_of`` must be timezone-aware. A row expiring at the cutoff is expired.
        Live registrations retain their payload, identity and expiry unchanged.
        A different source association never replaces an existing row, even if
        expired; callers compare the returned winner and reject rebinding.
        """
        ...


@runtime_checkable
class SubscriptionMutationProvider(Protocol):
    """Atomic subscription, selector-revision and managed-schedule writes."""

    async def write_subscription(
        self,
        workspace_id: WorkspaceId,
        subscription: Subscription,
        selector: SubscriptionSelector,
        *,
        expected_revision: int | None,
        schedule: Schedule | None,
        reset_schedule: bool,
    ) -> SubscriptionSnapshot:
        """Create or compare-and-swap a subscription and its receiver eligibility.

        ``None`` expects absence; an integer expects the latest selector's
        ``_revision`` (zero for legacy rows). Conflicts raise ImmutableViolation.
        Managed schedules use the subscription ID and record ``_scheduleId`` in
        the selector blob. Without a configuration change, next-fire progress
        must survive state-only updates. No partial writes survive failure.
        """
        ...


def prepare_subscription_write(
    workspace_id: str,
    subscription: Subscription,
    selector: SubscriptionSelector,
    current: SubscriptionSnapshot | None,
    current_schedule: Schedule | None,
    *,
    expected_revision: int | None,
    schedule: Schedule | None,
    reset_schedule: bool,
) -> tuple[SubscriptionSnapshot, Schedule | None]:
    """Validate a complete atomic write before adapters change any state.

    The private selector keys ``_revision`` and ``_scheduleId`` are provider-owned.
    Legacy selector blobs have revision zero. Schedule IDs equal subscription IDs;
    a pre-existing, unassociated schedule cannot be adopted accidentally.
    """
    identifier = subscription.subscription_id
    if (
        subscription.workspace_id != workspace_id
        or selector.workspace_id != workspace_id
        or selector.subscription_id != identifier
    ):
        raise WorkspaceMismatch("subscription write contains mismatched workspace or identifier")
    if expected_revision is not None and (
        type(expected_revision) is not int or expected_revision < 0
    ):
        raise ValueError("expected_revision must be a non-negative integer")
    if subscription.created_at.utcoffset() is None or subscription.updated_at.utcoffset() is None:
        raise ValueError("subscription timestamps must be timezone-aware")
    prior = dict(current.selector.selector) if current is not None and current.selector else {}
    revision = prior.get("_revision", 0)
    if type(revision) is not int or revision < 0:
        raise ValueError("stored subscription revision is invalid")
    if current is None:
        if expected_revision is not None:
            raise ImmutableViolation("subscription disappeared before update")
    else:
        if expected_revision is None or expected_revision != revision:
            raise ImmutableViolation("subscription changed before update")
        if (
            current.subscription.workflow_id != subscription.workflow_id
            or current.subscription.created_at.utcoffset() is None
            or current.subscription.created_at.astimezone(UTC)
            != subscription.created_at.astimezone(UTC)
        ):
            raise ImmutableViolation("subscription identity is immutable")
    payload = json.loads(json.dumps(dict(selector.selector)))
    payload["_revision"] = revision + 1
    owned_schedule = prior.get("_scheduleId")
    if owned_schedule not in (None, identifier):
        raise ValueError("stored schedule association is invalid")
    if (
        current_schedule is not None
        and (owned_schedule is not None or schedule is not None)
        and (
            current_schedule.workspace_id != workspace_id
            or current_schedule.schedule_id != identifier
            or current_schedule.workflow_id != subscription.workflow_id
        )
    ):
        raise ImmutableViolation("managed schedule belongs to another subscription or workflow")
    if owned_schedule is not None:
        payload["_scheduleId"] = identifier
    else:
        payload.pop("_scheduleId", None)
    next_schedule = None
    if schedule is not None:
        if (
            schedule.workspace_id != workspace_id
            or schedule.schedule_id != identifier
            or schedule.workflow_id != subscription.workflow_id
            or schedule.enabled != (subscription.state == "active")
        ):
            raise ValueError("managed schedule must match its subscription and eligibility")
        if current_schedule is not None and owned_schedule is None:
            raise ImmutableViolation("schedule identifier is already owned")
        payload["_scheduleId"] = identifier
        next_schedule = schedule
        if current_schedule is not None and not reset_schedule:
            next_schedule = replace(schedule, next_fire_at=current_schedule.next_fire_at)
    elif owned_schedule is not None and current_schedule is not None:
        next_schedule = replace(current_schedule, enabled=False)
    if next_schedule is not None and next_schedule.workflow_id != subscription.workflow_id:
        raise ImmutableViolation("managed schedule belongs to another workflow")
    return (
        SubscriptionSnapshot(subscription, replace(selector, selector=payload)),
        next_schedule,
    )


__all__ = [
    "ResumeRegistrationProvider",
    "SubscriptionMutationProvider",
    "SubscriptionSnapshot",
    "TriggerQueryProvider",
    "prepare_subscription_write",
]

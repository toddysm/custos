"""Workspace-scoped reads and atomic resume registration for trigger metadata.

This additive capability complements ``MetadataStoreProvider`` without requiring
unrelated metadata consumers to implement trigger queries. Pages use ascending
identifier order; callers must retain the same filters and time cutoff while
paging. Concurrent writes are visible on later calls, not a snapshot transaction.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol, runtime_checkable

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
        """
        ...


__all__ = ["ResumeRegistrationProvider", "SubscriptionSnapshot", "TriggerQueryProvider"]

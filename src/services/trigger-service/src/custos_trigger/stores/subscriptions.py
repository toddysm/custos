"""``SubscriptionStore`` — domain ↔ SPL adapter for trigger subscriptions.

Uses the optional atomic mutation capability to commit the minimal SPL row,
append-only selector revision and managed schedule in one transaction.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from custos_spl.ids import SubscriptionId, WorkspaceId
from custos_spl.interfaces.metadata_store import Schedule
from custos_spl.interfaces.trigger_queries import SubscriptionMutationProvider, TriggerQueryProvider
from custos_spl.pagination import Cursor, Page

from custos_trigger.models import (
    Subscription,
    SubscriptionState,
    subscription_from_spl,
    to_spl_subscription,
    to_spl_subscription_selector,
)
from custos_trigger.source_config import next_schedule_fire
from custos_trigger.stores.base import TriggerMetadataStore

__all__ = [
    "SubscriptionListUnsupportedError",
    "SubscriptionReadUnsupportedError",
    "SubscriptionStore",
]


def _utcnow() -> datetime:
    return datetime.now(UTC)


class SubscriptionReadUnsupportedError(RuntimeError):
    """Raised when the bound backend exposes no subscription read surface.

    The backend must implement the asynchronous ``TriggerQueryProvider``
    capability. Both the in-memory and PostgreSQL providers implement it.
    """


class SubscriptionListUnsupportedError(RuntimeError):
    """Raised when the bound backend exposes no subscription list surface.

    Candidate pages include latest selector revisions; a backend without the
    asynchronous ``TriggerQueryProvider`` capability cannot safely enumerate them.
    """


class SubscriptionStore:
    """Persist + transition trigger subscriptions through the SPL provider.

    All mutations return selector-hydrated state and fence stale updates with
    internal revisions. Managed schedule IDs equal subscription IDs. Interval
    schedules store an empty ``cron`` in the locked SPL row; their interval
    lives in ``sourceConfig`` and their UTC anchor is subscription creation.
    """

    def __init__(
        self,
        store: TriggerMetadataStore,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._store = store
        self._now = now if now is not None else _utcnow

    async def create(self, subscription: Subscription) -> Subscription:
        """Create base state, selector and optional managed schedule atomically."""
        return await self._write(subscription, expected_revision=None, reset_schedule=True)

    async def update(
        self,
        subscription: Subscription,
        *,
        expected_revision: int,
        reset_schedule: bool = False,
        preserve_state: bool = False,
    ) -> Subscription:
        """Update metadata atomically, optionally retaining the locked lifecycle state."""
        return await self._write(
            subscription,
            expected_revision=expected_revision,
            reset_schedule=reset_schedule,
            preserve_state=preserve_state,
        )

    async def _write(
        self,
        subscription: Subscription,
        *,
        expected_revision: int | None,
        reset_schedule: bool,
        preserve_state: bool = False,
    ) -> Subscription:
        """Build a complete provider transaction before touching persisted state."""
        store = self._store
        if not isinstance(store, SubscriptionMutationProvider):
            raise SubscriptionReadUnsupportedError(
                "metadata store does not support atomic subscription configuration"
            )
        schedule = None
        config = subscription.source_config
        if subscription.source_type.value == "scheduled" and config is not None:
            previous_schedule = None
            if (
                expected_revision is not None
                and not reset_schedule
                and isinstance(store, TriggerQueryProvider)
            ):
                previous_schedule = await store.get_schedule(
                    WorkspaceId(subscription.workspace_id), subscription.subscription_id
                )
            schedule = Schedule(
                workspace_id=WorkspaceId(subscription.workspace_id),
                schedule_id=subscription.subscription_id,
                workflow_id=to_spl_subscription(subscription).workflow_id,
                cron=config.cron if config.cron is not None else "",
                next_fire_at=(
                    previous_schedule.next_fire_at
                    if previous_schedule is not None
                    else next_schedule_fire(
                        config, anchor=subscription.created_at, after=self._now()
                    )
                ),
                enabled=subscription.state is SubscriptionState.ACTIVE,
            )
        snapshot = await store.write_subscription(
            WorkspaceId(subscription.workspace_id),
            to_spl_subscription(subscription),
            to_spl_subscription_selector(subscription, added_at=self._now()),
            expected_revision=expected_revision,
            schedule=schedule,
            reset_schedule=reset_schedule,
            preserve_state=preserve_state,
        )
        return subscription_from_spl(snapshot.subscription, snapshot.selector)

    async def get(self, workspace_id: str, subscription_id: str) -> Subscription | None:
        """Read one subscription back by id, or ``None`` when absent.

        Rebuilds the full :class:`Subscription` from the base row plus its
        latest selector revision in one query. Raises
        :class:`SubscriptionReadUnsupportedError` for a write-only backend.
        """
        store = self._store
        if not isinstance(store, TriggerQueryProvider):
            raise SubscriptionReadUnsupportedError(
                "the bound metadata store exposes no subscription read surface"
            )
        snapshot = await store.get_subscription(
            WorkspaceId(workspace_id), SubscriptionId(subscription_id)
        )
        if snapshot is None:
            return None
        return subscription_from_spl(snapshot.subscription, snapshot.selector)

    async def query(
        self,
        workspace_id: str,
        *,
        state: SubscriptionState | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[Subscription]:
        """Read one bounded, selector-hydrated candidate page from the provider."""
        store = self._store
        if not isinstance(store, TriggerQueryProvider):
            raise SubscriptionListUnsupportedError(
                "the bound metadata store exposes no asynchronous subscription query surface"
            )
        page = await store.query_subscriptions(
            WorkspaceId(workspace_id),
            state=state.value if state is not None else None,
            cursor=cursor,
            limit=limit,
        )
        return Page(
            (subscription_from_spl(row.subscription, row.selector) for row in page.items),
            page.next_cursor,
        )

    async def list_in_workspace(self, workspace_id: str) -> list[Subscription]:
        """Return every subscription in *workspace_id* as match candidates.

        Drain bounded provider pages for the existing internal receiver.
        Filtering to START/ACTIVE remains the matcher's responsibility;
        selectors are hydrated by the provider rather than an N+1 read loop.
        """
        candidates: list[Subscription] = []
        cursor: Cursor | None = None
        while True:
            page = await self.query(workspace_id, cursor=cursor)
            candidates.extend(page.items)
            cursor = page.next_cursor
            if cursor is None:
                return candidates

    async def reauthor_selector(self, subscription: Subscription) -> None:
        """Reauthor a caller's current revision without overwriting concurrent edits."""
        current = await self.get(subscription.workspace_id, subscription.subscription_id)
        if current is None:
            raise ValueError(f"unknown subscription: {subscription.subscription_id!r}")
        candidate = subscription.model_copy(
            update={
                "state": current.state,
                "created_at": current.created_at,
                "updated_at": self._now(),
            }
        )
        await self.update(
            candidate,
            expected_revision=subscription.store_revision,
            preserve_state=True,
            reset_schedule=(
                current.source_config != candidate.source_config
                or current.source_type != candidate.source_type
            ),
        )

    async def set_state(
        self,
        workspace_id: str,
        subscription_id: str,
        state: SubscriptionState,
    ) -> Subscription:
        """Transition state and managed-schedule eligibility atomically."""
        current = await self.get(workspace_id, subscription_id)
        if current is None:
            raise ValueError(f"unknown subscription: {workspace_id!r}/{subscription_id!r}")
        return await self.update(
            current.model_copy(update={"state": state, "updated_at": self._now()}),
            expected_revision=current.store_revision,
        )

"""``SubscriptionStore`` — domain ↔ SPL adapter for trigger subscriptions.

Wraps the SPL ``put_subscription`` / ``append_subscription_selector`` /
``update_subscription_state`` writes, mapping the :class:`Subscription`
domain model onto the minimal SPL row plus its free-form selector blob
(see :mod:`custos_trigger.models`).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from custos_spl.ids import SubscriptionId, WorkspaceId
from custos_spl.interfaces.trigger_queries import TriggerQueryProvider
from custos_spl.pagination import Cursor, Page

from custos_trigger.models import (
    Subscription,
    SubscriptionState,
    subscription_from_spl,
    to_spl_subscription,
    to_spl_subscription_selector,
)
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

    The selector blob is append-only on the SPL side, so :meth:`create`
    writes the base row and the first selector revision in one call and
    :meth:`reauthor_selector` appends a fresh revision without rewriting
    prior rows. :meth:`set_state` returns the subscription rebuilt from the
    minimal SPL row alone — the rich blob fields default because
    ``update_subscription_state`` touches only the base row (design
    ``§ Data Models``); callers needing the full record re-read the latest
    selector.
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
        """Persist *subscription*: base row + its first selector revision."""
        workspace = WorkspaceId(subscription.workspace_id)
        row = await self._store.put_subscription(
            workspace,
            to_spl_subscription(subscription),
        )
        selector = await self._store.append_subscription_selector(
            workspace,
            SubscriptionId(subscription.subscription_id),
            to_spl_subscription_selector(subscription, added_at=self._now()),
        )
        return subscription_from_spl(row, selector)

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
        """Append a fresh selector revision built from *subscription*."""
        await self._store.append_subscription_selector(
            WorkspaceId(subscription.workspace_id),
            SubscriptionId(subscription.subscription_id),
            to_spl_subscription_selector(subscription, added_at=self._now()),
        )

    async def set_state(
        self,
        workspace_id: str,
        subscription_id: str,
        state: SubscriptionState,
    ) -> Subscription:
        """Transition a subscription's lifecycle state."""
        row = await self._store.update_subscription_state(
            WorkspaceId(workspace_id),
            SubscriptionId(subscription_id),
            state.value,
        )
        return subscription_from_spl(row, None)

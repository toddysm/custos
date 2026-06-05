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

from custos_trigger.models import (
    Subscription,
    SubscriptionState,
    subscription_from_spl,
    to_spl_subscription,
    to_spl_subscription_selector,
)
from custos_trigger.stores.base import (
    SubscriptionListable,
    SubscriptionReadable,
    TriggerMetadataStore,
)

__all__ = [
    "SubscriptionListUnsupportedError",
    "SubscriptionReadUnsupportedError",
    "SubscriptionStore",
]


def _utcnow() -> datetime:
    return datetime.now(UTC)


class SubscriptionReadUnsupportedError(RuntimeError):
    """Raised when the bound backend exposes no subscription read surface.

    The locked SPL write Protocol has no subscription read method; only a
    backend that also satisfies
    :class:`~custos_trigger.stores.base.SubscriptionReadable` can serve
    :meth:`SubscriptionStore.get`. The in-process backend does; the Postgres
    adapter gains the capability in a later task.
    """


class SubscriptionListUnsupportedError(RuntimeError):
    """Raised when the bound backend exposes no subscription list surface.

    The internal event receiver (TS-IMPL-017) enumerates a workspace's start
    subscriptions as match candidates; only a backend that also satisfies
    :class:`~custos_trigger.stores.base.SubscriptionListable` can serve
    :meth:`SubscriptionStore.list_in_workspace`. The in-process backend does;
    the Postgres adapter gains the capability in a later task.
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
        latest selector revision (which carries the rich metadata blob). Raises
        :class:`SubscriptionReadUnsupportedError` when the bound backend has no
        :class:`~custos_trigger.stores.base.SubscriptionReadable` surface.
        """
        store = self._store
        if not isinstance(store, SubscriptionReadable):
            raise SubscriptionReadUnsupportedError(
                "the bound metadata store exposes no subscription read surface"
            )
        row = store.subscription(workspace_id, subscription_id)
        if row is None:
            return None
        selectors = store.subscription_selectors(workspace_id, subscription_id)
        latest = selectors[-1] if selectors else None
        return subscription_from_spl(row, latest)

    async def list_in_workspace(self, workspace_id: str) -> list[Subscription]:
        """Return every subscription in *workspace_id* as match candidates.

        Each row is rehydrated with its latest selector revision (the rich
        metadata blob the matcher's CEL selector reads). Filtering to ``START``
        kind / ``ACTIVE`` state is the matcher's job, so the full set is
        returned here. Raises :class:`SubscriptionListUnsupportedError` when the
        bound backend has no
        :class:`~custos_trigger.stores.base.SubscriptionListable` surface.
        """
        store = self._store
        if not isinstance(store, SubscriptionListable):
            raise SubscriptionListUnsupportedError(
                "the bound metadata store exposes no subscription list surface"
            )
        readable = store if isinstance(store, SubscriptionReadable) else None
        candidates: list[Subscription] = []
        for row in store.list_subscriptions(workspace_id):
            latest = None
            if readable is not None:
                selectors = readable.subscription_selectors(workspace_id, str(row.subscription_id))
                latest = selectors[-1] if selectors else None
            candidates.append(subscription_from_spl(row, latest))
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

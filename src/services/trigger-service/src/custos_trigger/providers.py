"""SPL provider wiring for the Trigger Service (TS-IMPL-008).

The Trigger Service persists Subscription / SubscriptionSelector /
ResumeSubscription / DedupKey / Schedule rows through a
:class:`~custos_trigger.stores.base.TriggerMetadataStore` — the narrow write
surface of :class:`custos_spl.interfaces.metadata_store.MetadataStoreProvider`.

:func:`load_providers` selects the backend from the ``TRIGGER_METADATA_STORE``
env knob: an empty value or a ``memory`` sentinel binds the in-process
:class:`InMemoryTriggerMetadataStore` (local dev + tests); any other value is
treated as a Postgres DSN and bound to ``custos_pg.PgMetadataAdapter`` via a
lazily-constructed pool. No new schema is invented — the rich Trigger Service
metadata rides in the locked SPL rows' free-form JSON blobs (see
:mod:`custos_trigger.models`).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import cast

from custos_spl.errors import ImmutableViolation
from custos_spl.ids import SubscriptionId, WorkspaceId
from custos_spl.interfaces.metadata_store import (
    DedupDuplicate,
    DedupReserved,
    PutDedupKeyResult,
)
from custos_spl.interfaces.metadata_store import (
    DedupKey as SplDedupKey,
)
from custos_spl.interfaces.metadata_store import (
    ResumeSubscription as SplResumeSubscription,
)
from custos_spl.interfaces.metadata_store import (
    Schedule as SplSchedule,
)
from custos_spl.interfaces.metadata_store import (
    Subscription as SplSubscription,
)
from custos_spl.interfaces.metadata_store import (
    SubscriptionSelector as SplSubscriptionSelector,
)

from custos_trigger.stores.base import TriggerMetadataStore

__all__ = [
    "InMemoryTriggerMetadataStore",
    "Providers",
    "is_memory_dsn",
    "load_providers",
]

#: ``TRIGGER_METADATA_STORE`` values (case-insensitive) that select the
#: in-process backend. An empty/unset value defaults to in-memory too so the
#: app factory boots without a database for local development and tests.
_MEMORY_DSN_SENTINELS: frozenset[str] = frozenset({"memory", "inmemory", "in-memory"})


def _utcnow() -> datetime:
    return datetime.now(UTC)


def is_memory_dsn(dsn: str) -> bool:
    """True when *dsn* selects the in-process metadata store."""
    lowered = dsn.strip().lower()
    return lowered == "" or lowered in _MEMORY_DSN_SENTINELS or lowered.startswith("memory://")


class InMemoryTriggerMetadataStore:
    """In-process :class:`TriggerMetadataStore` for local dev + tests.

    Implements the eight Trigger-Service write methods of
    ``MetadataStoreProvider`` with faithful semantics — immutable
    ``put_subscription``, not-found ``ValueError`` on state/next-fire updates,
    TTL-aware reserve-or-read dedup — mirroring
    ``custos_pg.PgMetadataAdapter``. The run / cursor / idempotency / audit
    families are intentionally absent; this backend exists only to exercise
    the Trigger Service store adapters without a database.

    The read accessors (:meth:`subscription`, :meth:`subscription_selectors`,
    …) are dev/test conveniences outside the SPL Protocol; production code
    reads through the Postgres adapter's own query surface.
    """

    def __init__(self, *, now: Callable[[], datetime] | None = None) -> None:
        self._now = now if now is not None else _utcnow
        self._subscriptions: dict[tuple[str, str], SplSubscription] = {}
        self._selectors: dict[tuple[str, str], list[SplSubscriptionSelector]] = {}
        self._resume: dict[tuple[str, str], SplResumeSubscription] = {}
        self._dedup: dict[tuple[str, str], SplDedupKey] = {}
        self._schedules: dict[tuple[str, str], SplSchedule] = {}

    # ----- Subscriptions -----

    async def put_subscription(
        self, workspace_id: WorkspaceId, subscription: SplSubscription
    ) -> SplSubscription:
        key = (str(workspace_id), str(subscription.subscription_id))
        if key in self._subscriptions:
            raise ImmutableViolation(f"subscription already exists: {key[0]!r}/{key[1]!r}")
        self._subscriptions[key] = subscription
        return subscription

    async def update_subscription_state(
        self,
        workspace_id: WorkspaceId,
        subscription_id: SubscriptionId,
        state: str,
    ) -> SplSubscription:
        key = (str(workspace_id), str(subscription_id))
        existing = self._subscriptions.get(key)
        if existing is None:
            raise ValueError(f"unknown subscription: {key[0]!r}/{key[1]!r}")
        updated = replace(existing, state=state, updated_at=self._now())
        self._subscriptions[key] = updated
        return updated

    async def append_subscription_selector(
        self,
        workspace_id: WorkspaceId,
        subscription_id: SubscriptionId,
        selector: SplSubscriptionSelector,
    ) -> SplSubscriptionSelector:
        key = (str(workspace_id), str(subscription_id))
        self._selectors.setdefault(key, []).append(selector)
        return selector

    # ----- Resume subscriptions -----

    async def put_resume_subscription(
        self, workspace_id: WorkspaceId, resume: SplResumeSubscription
    ) -> SplResumeSubscription:
        self._resume[(str(workspace_id), resume.resume_id)] = resume
        return resume

    async def delete_resume_subscription(self, workspace_id: WorkspaceId, resume_id: str) -> None:
        self._resume.pop((str(workspace_id), resume_id), None)

    # ----- Dedup -----

    async def put_dedup_key(
        self, workspace_id: WorkspaceId, key: str, ttl_seconds: int
    ) -> PutDedupKeyResult:
        now = self._now()
        dedup_key = (str(workspace_id), key)
        existing = self._dedup.get(dedup_key)
        if existing is not None and existing.expires_at > now:
            return DedupDuplicate(existing=existing)
        reserved = SplDedupKey(
            workspace_id=WorkspaceId(str(workspace_id)),
            key=key,
            expires_at=now + timedelta(seconds=ttl_seconds),
        )
        self._dedup[dedup_key] = reserved
        return DedupReserved(key=reserved)

    # ----- Schedules -----

    async def put_schedule(self, workspace_id: WorkspaceId, schedule: SplSchedule) -> SplSchedule:
        self._schedules[(str(workspace_id), schedule.schedule_id)] = schedule
        return schedule

    async def update_schedule_next_fire(
        self,
        workspace_id: WorkspaceId,
        schedule_id: str,
        next_fire_at: datetime,
    ) -> SplSchedule:
        key = (str(workspace_id), schedule_id)
        existing = self._schedules.get(key)
        if existing is None:
            raise ValueError(f"unknown schedule: {key[0]!r}/{key[1]!r}")
        updated = replace(existing, next_fire_at=next_fire_at)
        self._schedules[key] = updated
        return updated

    # ----- Read accessors (dev/test only; outside the SPL Protocol) -----

    def subscription(self, workspace_id: str, subscription_id: str) -> SplSubscription | None:
        """Return the stored subscription row, or ``None`` when absent."""
        return self._subscriptions.get((workspace_id, subscription_id))

    def subscription_selectors(
        self, workspace_id: str, subscription_id: str
    ) -> tuple[SplSubscriptionSelector, ...]:
        """Return the append-only selector revisions in write order."""
        return tuple(self._selectors.get((workspace_id, subscription_id), ()))

    def resume_subscription(
        self, workspace_id: str, resume_id: str
    ) -> SplResumeSubscription | None:
        """Return the stored resume token, or ``None`` when absent."""
        return self._resume.get((workspace_id, resume_id))

    def schedule(self, workspace_id: str, schedule_id: str) -> SplSchedule | None:
        """Return the stored schedule row, or ``None`` when absent."""
        return self._schedules.get((workspace_id, schedule_id))

    def dedup_key(self, workspace_id: str, key: str) -> SplDedupKey | None:
        """Return the stored dedup row, or ``None`` when absent."""
        return self._dedup.get((workspace_id, key))


@dataclass(frozen=True, slots=True)
class Providers:
    """Bundle of the SPL providers Trigger Service consumes.

    Held on ``app.state.providers`` and exposed to handlers via the
    dependency helpers in :mod:`custos_trigger.dependencies`.
    """

    metadata_store: TriggerMetadataStore


def load_providers(metadata_store_dsn: str) -> Providers:
    """Construct the SPL providers from the ``TRIGGER_METADATA_STORE`` value.

    An empty value or a ``memory`` sentinel binds the in-process backend; any
    other value is a Postgres DSN bound to ``custos_pg.PgMetadataAdapter`` via
    a lazily-constructed pool, so this factory never opens a socket. The
    Postgres imports are deferred so unit tests injecting the in-memory store
    never drag asyncpg onto the import path.
    """
    if is_memory_dsn(metadata_store_dsn):
        return Providers(metadata_store=InMemoryTriggerMetadataStore())

    from custos_pg import PgMetadataAdapter
    from custos_pg.pool import LazyPool

    # The adapter declares ``SCHEMA_REVISION`` as a bare class attr rather
    # than a ``ClassVar[int]``, so mypy can't structurally see it as
    # Protocol-conforming at this consumer boundary. custos-postgres has its
    # own strict mypy job verifying conformance at the implementation site;
    # the cast keeps the consumer view typed.
    adapter = PgMetadataAdapter(lazy=LazyPool(metadata_store_dsn.strip()))
    return Providers(metadata_store=cast(TriggerMetadataStore, adapter))

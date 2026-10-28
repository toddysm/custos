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

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Literal, TypeVar, cast

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
from custos_spl.interfaces.trigger_queries import SubscriptionSnapshot
from custos_spl.pagination import (
    Cursor,
    Page,
    decode_query_cursor,
    encode_query_cursor,
    query_page_limit,
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

_Row = TypeVar("_Row")


def _query_page(
    rows: Sequence[_Row],
    scope: tuple[str | None, ...],
    cursor: Cursor | None,
    limit: int | None,
    key: Callable[[_Row], str],
) -> Page[_Row]:
    """Apply the shared scoped keyset contract to an in-memory row collection."""
    size = query_page_limit(limit)
    after = decode_query_cursor(cursor, scope)
    ordered = sorted((row for row in rows if after is None or key(row) > after), key=key)
    next_cursor = (
        encode_query_cursor(scope, key(ordered[size - 1])) if len(ordered) > size else None
    )
    return Page(ordered[:size], next_cursor)


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

    Async reads implement the shared ``TriggerQueryProvider`` capability with
    the same ordering, filtering and cursor validation as PostgreSQL. Legacy
    synchronous accessors remain test conveniences, not production store APIs.
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
        # The Postgres `subscription_selector` row carries a foreign key to
        # `custos_state.subscription`; mirror that referential integrity so a
        # selector appended for a never-created subscription fails fast here
        # instead of masking a production-only error.
        if key not in self._subscriptions:
            raise ValueError(f"unknown subscription: {key[0]!r}/{key[1]!r}")
        self._selectors.setdefault(key, []).append(selector)
        return selector

    # ----- Resume subscriptions -----

    async def put_resume_subscription(
        self, workspace_id: WorkspaceId, resume: SplResumeSubscription
    ) -> SplResumeSubscription:
        key = (str(workspace_id), resume.resume_id)
        # The Postgres adapter INSERTs on the `(workspace_id, resume_id)`
        # primary key and maps the 23505 collision to `ImmutableViolation`;
        # match that immutability so dev/test runs can't silently overwrite.
        if key in self._resume:
            raise ImmutableViolation(f"resume subscription already exists: {key[0]!r}/{key[1]!r}")
        self._resume[key] = resume
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

    async def release_dedup_key(self, workspace_id: WorkspaceId, key: str) -> None:
        """Drop a reserved dedup key (best-effort, idempotent).

        Outside the SPL ``MetadataStoreProvider`` Protocol — it backs the
        :class:`custos_trigger.dedup.Deduplicator` reserve-before-dispatch
        rollback so a failed dispatch doesn't poison the dedup window. The
        Postgres adapter has no equivalent in v1 (design ``§ TODO-007`` defers
        the selective dedup-clear admin API), so production rollback falls
        back to TTL expiry there.
        """
        self._dedup.pop((str(workspace_id), key), None)

    # ----- Schedules -----

    async def put_schedule(self, workspace_id: WorkspaceId, schedule: SplSchedule) -> SplSchedule:
        key = (str(workspace_id), schedule.schedule_id)
        # The Postgres adapter INSERTs on the `(workspace_id, schedule_id)`
        # primary key and maps the 23505 collision to `ImmutableViolation`;
        # match that immutability to catch accidental double-creates.
        if key in self._schedules:
            raise ImmutableViolation(f"schedule already exists: {key[0]!r}/{key[1]!r}")
        self._schedules[key] = schedule
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

    def list_subscriptions(self, workspace_id: str) -> tuple[SplSubscription, ...]:
        """Return every subscription row stored under *workspace_id*.

        Legacy test inspection returns insertion order. Production candidate
        enumeration uses the asynchronous, identifier-ordered query capability.
        """
        return tuple(row for (ws, _sid), row in self._subscriptions.items() if ws == workspace_id)

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

    async def get_subscription(
        self, workspace_id: WorkspaceId, subscription_id: SubscriptionId
    ) -> SubscriptionSnapshot | None:
        """Return the base row and latest append-ordered selector together."""
        row = self.subscription(workspace_id, subscription_id)
        if row is None:
            return None
        selectors = self.subscription_selectors(workspace_id, subscription_id)
        return SubscriptionSnapshot(row, selectors[-1] if selectors else None)

    async def query_subscriptions(
        self,
        workspace_id: WorkspaceId,
        *,
        state: str | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[SubscriptionSnapshot]:
        """Page workspace subscriptions and their latest selector revisions."""
        snapshots: list[SubscriptionSnapshot] = []
        for row in self.list_subscriptions(workspace_id):
            if state is None or row.state == state:
                selectors = self.subscription_selectors(workspace_id, row.subscription_id)
                snapshots.append(SubscriptionSnapshot(row, selectors[-1] if selectors else None))
        return _query_page(
            snapshots,
            ("subscriptions", workspace_id, state),
            cursor,
            limit,
            lambda snapshot: snapshot.subscription.subscription_id,
        )

    async def get_resume_subscription(
        self, workspace_id: WorkspaceId, resume_id: str
    ) -> SplResumeSubscription | None:
        """Read a resume registration, including expired rows for re-registration."""
        return self.resume_subscription(workspace_id, resume_id)

    async def register_resume_subscription(
        self, workspace_id: WorkspaceId, resume: SplResumeSubscription, *, as_of: datetime
    ) -> SplResumeSubscription:
        """Register without yielding between checking expiry and preserving the live winner."""
        if as_of.utcoffset() is None:
            raise ValueError("resume registration cutoff must be timezone-aware")
        key = (str(workspace_id), resume.resume_id)
        existing = self._resume.get(key)
        if existing is not None and existing.expires_at.astimezone(UTC) > as_of.astimezone(UTC):
            return existing
        self._resume[key] = resume
        return resume

    async def query_resume_subscriptions(
        self,
        workspace_id: WorkspaceId,
        *,
        connector_instance_id: str,
        delivery_mode: Literal["push", "pull"],
        as_of: datetime,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[SplResumeSubscription]:
        """Page unexpired source-scoped waits; legacy registrations never match."""
        if as_of.utcoffset() is None:
            raise ValueError("resume query cutoff must be timezone-aware")
        if not connector_instance_id or delivery_mode not in ("push", "pull"):
            raise ValueError("resume queries require a connector instance and push/pull mode")
        rows: list[SplResumeSubscription] = []
        for (workspace, _), row in self._resume.items():
            if workspace != workspace_id or row.expires_at.astimezone(UTC) <= as_of.astimezone(UTC):
                continue
            source = row.payload.get("source")
            if not isinstance(source, Mapping):
                continue
            modes = source.get("deliveryModes")
            if (
                source.get("connectorInstanceId") == connector_instance_id
                and isinstance(modes, list)
                and delivery_mode in modes
            ):
                rows.append(row)
        return _query_page(
            rows,
            ("resume", workspace_id, connector_instance_id, delivery_mode, as_of.isoformat()),
            cursor,
            limit,
            lambda row: row.resume_id,
        )

    async def get_schedule(self, workspace_id: WorkspaceId, schedule_id: str) -> SplSchedule | None:
        """Read one schedule without filtering disabled or future rows."""
        return self.schedule(workspace_id, schedule_id)

    async def query_due_schedules(
        self,
        workspace_id: WorkspaceId,
        *,
        due_at: datetime,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[SplSchedule]:
        """Page enabled schedules due by the timezone-aware cutoff."""
        if due_at.utcoffset() is None:
            raise ValueError("schedule query cutoff must be timezone-aware")
        rows = [
            row
            for (workspace, _), row in self._schedules.items()
            if workspace == workspace_id
            and row.enabled
            and row.next_fire_at.astimezone(UTC) <= due_at.astimezone(UTC)
        ]
        return _query_page(
            rows,
            ("schedules", workspace_id, due_at.isoformat()),
            cursor,
            limit,
            lambda row: row.schedule_id,
        )


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

    # ``PgMetadataAdapter`` implements the full ``MetadataStoreProvider`` but
    # types its workspace-scoped args as plain ``str`` rather than the
    # ``WorkspaceId`` / ``SubscriptionId`` NewTypes this Protocol declares, so
    # mypy can't structurally see it as ``TriggerMetadataStore``-conforming at
    # this consumer boundary. custos-postgres has its own strict mypy job
    # verifying the adapter against the SPL Protocol at the implementation
    # site; the cast keeps the consumer view typed.
    adapter = PgMetadataAdapter(lazy=LazyPool(metadata_store_dsn.strip()))
    return Providers(metadata_store=cast(TriggerMetadataStore, adapter))

"""SPL provider wiring for the Trigger Service (TS-IMPL-008).

The Trigger Service persists Subscription / SubscriptionSelector /
ResumeSubscription / DedupKey / Schedule rows through a
:class:`~custos_trigger.stores.base.TriggerMetadataStore` — the narrow write
surface of :class:`custos_spl.interfaces.metadata_store.MetadataStoreProvider`.

:func:`load_providers` selects the backend from the ``TRIGGER_METADATA_STORE``
env knob: an empty value or a ``memory`` sentinel binds the in-process
:class:`InMemoryTriggerMetadataStore` (local dev + tests); any other value is
treated as a Postgres DSN and bound to ``custos_pg.PgMetadataAdapter`` via a
lazily-constructed pool. Rich subscription configuration remains in selector
blobs; the optional scheduler capability adds versioned lease/pending-fire
tables without changing the existing row contracts.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Literal, TypeVar, cast

from custos_spl.delivery_memory import MemoryDeliveryState
from custos_spl.errors import ImmutableViolation, LeaseBusy, LeaseExpired
from custos_spl.ids import SubscriptionId, WorkspaceId
from custos_spl.interfaces.bulk_replay import (
    BulkReplayCommand,
    bulk_entry_identity,
    bulk_request_hash,
    finish_bulk_item,
    require_bulk_entry,
    require_bulk_owner,
    reserve_bulk_replay,
)
from custos_spl.interfaces.connector_instance_store import ConnectorReferenceReader
from custos_spl.interfaces.dead_letters import (
    DeadLetterData,
    DeadLetterEntry,
    DeadLetterFilter,
    DispatchAttempt,
    ReplayClaim,
    claim_replay,
    dead_letter_transition_audits,
    decode_dead_letter_cursor,
    defer_replay,
    discard_dead_letter,
    finish_replay,
    new_dead_letter,
    recover_replay,
    validate_dispatch_identifier,
)
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
from custos_spl.interfaces.scheduler import (
    PendingScheduleFire,
    SchedulerLease,
    lease_duration,
    prepare_schedule_fire,
    require_scheduler_lease,
    utc_instant,
    validate_schedule_discard,
)
from custos_spl.interfaces.trigger_queries import SubscriptionSnapshot, prepare_subscription_write
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


class InMemoryTriggerMetadataStore(MemoryDeliveryState):
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
        super().__init__(now=now)
        self._now = now if now is not None else _utcnow
        self._subscriptions: dict[tuple[str, str], SplSubscription] = {}
        self._selectors: dict[tuple[str, str], list[SplSubscriptionSelector]] = {}
        self._resume: dict[tuple[str, str], SplResumeSubscription] = {}
        self._dedup: dict[tuple[str, str], SplDedupKey] = {}
        self._schedules: dict[tuple[str, str], SplSchedule] = {}
        self._scheduler_leases: dict[str, SchedulerLease] = {}
        self._pending_fires: dict[tuple[str, str], PendingScheduleFire] = {}
        self._dead_letters: dict[tuple[str, str], DeadLetterEntry] = self._delivery_dead_letters
        self._bulk_replays: dict[tuple[str, str], BulkReplayCommand] = {}
        self._dead_letter_commands: dict[tuple[str, str], tuple[str, str]] = {}

    async def begin_bulk_replay(
        self,
        workspace_id: WorkspaceId,
        command_id: str,
        actor: str,
        filters: DeadLetterFilter,
        *,
        cursor: Cursor | None = None,
        limit: int = 50,
        retention_seconds: int = 2_592_000,
    ) -> BulkReplayCommand:
        """Persist selection and claim without yielding; retries never rerun the filter."""
        fingerprint = bulk_request_hash(workspace_id, filters, cursor, limit)
        current = self._bulk_replays.get((workspace_id, command_id))
        selection = (
            self._dead_letter_page(workspace_id, filters, cursor=cursor, limit=limit)
            if current is None
            else None
        )
        updated = reserve_bulk_replay(
            current,
            workspace_id,
            command_id,
            actor,
            fingerprint,
            self._now(),
            retention_seconds,
            selection,
        )
        self._bulk_replays[(workspace_id, command_id)] = updated
        return updated

    async def get_bulk_replay(
        self,
        workspace_id: WorkspaceId,
        command_id: str,
    ) -> BulkReplayCommand | None:
        """Read only one workspace's frozen command/progress snapshot."""
        return self._bulk_replays.get((workspace_id, command_id))

    def _finished_bulk_item(self, claim: ReplayClaim, accepted: bool) -> BulkReplayCommand | None:
        """Prepare per-entry command progress before publishing any replay state mutation."""
        if claim.bulk_id is None:
            return None
        command = self._bulk_replays.get((claim.workspace_id, claim.bulk_id))
        if command is None:
            raise ImmutableViolation("bulk command is missing")
        if command.items is None and command.expires_at <= self._now():
            return None
        return finish_bulk_item(command, claim.entry_id, accepted)

    async def record_bulk_replay_result(
        self,
        claim: BulkReplayCommand,
        entry_id: str,
        accepted: bool,
    ) -> None:
        """Record a non-effect result under current authority without losing prior progress."""
        command = self._bulk_replays.get((claim.workspace_id, claim.command_id))
        if command is None or command.items is None:
            raise ImmutableViolation("bulk selection is unavailable")
        previous = next((item for item in command.items if item.entry_id == entry_id), None)
        if previous is not None and previous.accepted is not None:
            return
        require_bulk_owner(command, claim.token, self._now())
        entry = self._dead_letters.get((str(claim.workspace_id), entry_id))
        if (
            entry
            and entry.claim
            and entry.claim.bulk_id == command.command_id
            and entry.claim.expires_at > self._now()
        ):
            raise LeaseBusy("the original bulk attempt is still in progress")
        self._bulk_replays[(claim.workspace_id, claim.command_id)] = finish_bulk_item(
            command, entry_id, accepted
        )

    async def purge_bulk_replay_selections(
        self, workspace_id: WorkspaceId, *, limit: int = 100
    ) -> int:
        """Erase expired selections while retaining non-rebindable command identities."""
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("bulk cleanup limit must be 1..100")
        count = 0
        for key, command in sorted(self._bulk_replays.items()):
            if key[0] != workspace_id or command.items is None or command.expires_at > self._now():
                continue
            if any(
                row.claim
                and row.claim.bulk_id == command.command_id
                and row.data.workspace_id == workspace_id
                and row.claim.expires_at > self._now()
                for row in self._dead_letters.values()
            ):
                continue
            self._bulk_replays[key] = replace(
                command, items=None, next_cursor=None, token=None, lease_until=None
            )
            count += 1
            if count == limit:
                break
        return count

    async def check_dead_letters_ready(self) -> None:
        """Memory state is ready at construction, without implying restart durability."""

    async def put_dead_letter(self, data: DeadLetterData) -> DeadLetterEntry:
        """Append one deeply immutable capture, rejecting duplicate identity."""
        key = (str(data.workspace_id), data.entry_id)
        if key in self._dead_letters:
            raise ImmutableViolation("dead-letter entry already exists")
        entry = new_dead_letter(data, self._now())
        self._dead_letter_audits.extend(dead_letter_transition_audits(None, entry, self._now()))
        self._dead_letters[key] = entry
        return entry

    async def get_dead_letter(
        self, workspace_id: WorkspaceId, entry_id: str
    ) -> DeadLetterEntry | None:
        """Read only the requested workspace; payloads are deeply immutable."""
        return self._dead_letters.get((str(workspace_id), entry_id))

    async def query_dead_letters(
        self,
        workspace_id: WorkspaceId,
        filters: DeadLetterFilter | None = None,
        *,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[DeadLetterEntry]:
        """Match PostgreSQL filters and identifier keyset ordering."""
        return self._dead_letter_page(workspace_id, filters, cursor=cursor, limit=limit)

    def _dead_letter_page(
        self,
        workspace_id: WorkspaceId,
        filters: DeadLetterFilter | None = None,
        *,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[DeadLetterEntry]:
        """Build a bounded page synchronously for atomic in-memory command selection."""
        filters = filters if filters is not None else DeadLetterFilter()
        scope = filters.scope(workspace_id)
        decode_dead_letter_cursor(cursor, scope)
        rows = [
            row
            for (ws, _), row in self._dead_letters.items()
            if ws == workspace_id
            and (
                filters.subscription_id is None
                or row.data.subscription_id == filters.subscription_id
            )
            and (filters.status is None or row.status == filters.status)
            and (filters.since is None or row.created_at >= utc_instant(filters.since))
            and (filters.until is None or row.created_at <= utc_instant(filters.until))
        ]
        return _query_page(rows, scope, cursor, limit, lambda row: row.data.entry_id)

    def _dead_letter(self, workspace_id: WorkspaceId, entry_id: str) -> DeadLetterEntry:
        """Require a scoped row for lifecycle mutations without disclosing other tenants."""
        entry = self._dead_letters.get((str(workspace_id), entry_id))
        if entry is None:
            raise ImmutableViolation("dead-letter entry does not exist")
        return entry

    async def bind_dead_letter_command(
        self, workspace_id: WorkspaceId, command_id: str, request_hash: str, entry_identity: str
    ) -> None:
        """Retain a key's first request/generation without keeping business payloads."""
        for value in (workspace_id, command_id, request_hash, entry_identity):
            validate_dispatch_identifier(value)
        binding = self._dead_letter_commands.setdefault(
            (workspace_id, command_id), (request_hash, entry_identity)
        )
        if binding != (request_hash, entry_identity):
            raise ImmutableViolation("recovery key is bound to another request or generation")

    async def claim_dead_letter(
        self,
        workspace_id: WorkspaceId,
        entry_id: str,
        worker_id: str,
        *,
        ttl_seconds: int = 60,
        bulk_id: str | None = None,
        bulk_token: str | None = None,
        expected_identity: str | None = None,
    ) -> DeadLetterEntry:
        """Consume one fenced attempt without yielding between read and replacement."""
        previous = self._dead_letter(workspace_id, entry_id)
        if expected_identity is not None and bulk_entry_identity(previous) != expected_identity:
            raise ImmutableViolation("dead-letter generation changed before replay claim")
        now = self._now()
        if bulk_id is not None:
            command = self._bulk_replays.get((workspace_id, bulk_id))
            require_bulk_owner(command, bulk_token, now)
            assert command is not None
            require_bulk_entry(command, previous, worker_id)
        entry = claim_replay(previous, worker_id, ttl_seconds, now, bulk_id=bulk_id)
        self._dead_letter_audits.extend(
            dead_letter_transition_audits(previous, entry, now, actor=worker_id)
        )
        self._dead_letters[(str(workspace_id), entry_id)] = entry
        return entry

    async def finish_dead_letter(
        self,
        claim: ReplayClaim,
        *,
        failure: DispatchAttempt | None = None,
        result_id: str | None = None,
    ) -> DeadLetterEntry:
        """Reject expired/foreign tokens, including tokens predating deletion and recreation."""
        entry = self._dead_letters.get((str(claim.workspace_id), claim.entry_id))
        if entry is None:
            raise LeaseExpired("replay claim no longer exists")
        finished = finish_replay(entry, claim, self._now(), failure=failure, result_id=result_id)
        bulk = self._finished_bulk_item(claim, failure is None)
        self._dead_letter_audits.extend(
            dead_letter_transition_audits(
                entry,
                finished,
                self._now(),
                actor=claim.worker_id or entry.replay_history[-1].worker_id,
            )
        )
        self._dead_letters[(str(claim.workspace_id), claim.entry_id)] = finished
        if bulk is not None:
            self._bulk_replays[(bulk.workspace_id, bulk.command_id)] = bulk
        return finished

    async def defer_dead_letter(self, claim: ReplayClaim) -> DeadLetterEntry:
        """Publish acknowledgement uncertainty atomically without completing bulk progress."""
        entry = self._dead_letters.get((str(claim.workspace_id), claim.entry_id))
        if entry is None:
            raise LeaseExpired("replay claim no longer exists")
        pending = defer_replay(entry, claim, self._now())
        self._dead_letter_audits.extend(
            dead_letter_transition_audits(
                entry,
                pending,
                self._now(),
                actor=claim.worker_id or entry.replay_history[-1].worker_id,
            )
        )
        self._dead_letters[(str(claim.workspace_id), claim.entry_id)] = pending
        return pending

    async def recover_dead_letter(
        self, workspace_id: WorkspaceId, entry_id: str, *, expected_identity: str | None = None
    ) -> DeadLetterEntry:
        """Recover an abandoned attempt only in the inspected immutable generation."""
        previous = self._dead_letter(workspace_id, entry_id)
        if expected_identity is not None and bulk_entry_identity(previous) != expected_identity:
            raise ImmutableViolation("dead-letter generation changed before recovery")
        entry = recover_replay(previous, self._now())
        bulk = (
            self._finished_bulk_item(previous.claim, False)
            if previous.claim and entry.status == "discarded"
            else None
        )
        self._dead_letter_audits.extend(dead_letter_transition_audits(previous, entry, self._now()))
        self._dead_letters[(str(workspace_id), entry_id)] = entry
        if bulk is not None:
            self._bulk_replays[(bulk.workspace_id, bulk.command_id)] = bulk
        return entry

    async def discard_dead_letter(
        self,
        workspace_id: WorkspaceId,
        entry_id: str,
        *,
        actor: str = "trigger-service",
        reason: str | None = None,
        expected_identity: str | None = None,
    ) -> DeadLetterEntry:
        """Keep active replay and successful outcomes safe from concurrent operator discard."""
        previous = self._dead_letter(workspace_id, entry_id)
        if expected_identity is not None and bulk_entry_identity(previous) != expected_identity:
            raise ImmutableViolation("dead-letter generation changed before discard")
        entry = discard_dead_letter(previous, self._now())
        bulk = self._finished_bulk_item(previous.claim, False) if previous.claim else None
        self._dead_letter_audits.extend(
            dead_letter_transition_audits(previous, entry, self._now(), actor=actor, reason=reason)
        )
        self._dead_letters[(str(workspace_id), entry_id)] = entry
        if bulk is not None:
            self._bulk_replays[(bulk.workspace_id, bulk.command_id)] = bulk
        return entry

    async def delete_expired_dead_letter(self, workspace_id: WorkspaceId, entry_id: str) -> bool:
        """Atomically delete eligible retained data, never a currently owned replay."""
        key = (str(workspace_id), entry_id)
        entry = self._dead_letters.get(key)
        now = utc_instant(self._now())
        if (
            entry is None
            or entry.expires_at > now
            or (entry.claim and entry.claim.expires_at > now)
        ):
            return False
        bulk = self._finished_bulk_item(entry.claim, False) if entry.claim else None
        self._dead_letter_audits.extend(dead_letter_transition_audits(entry, None, now))
        del self._dead_letters[key]
        if bulk is not None:
            self._bulk_replays[(bulk.workspace_id, bulk.command_id)] = bulk
        return True

    async def check_scheduler_ready(self) -> None:
        """Memory scheduler structures exist at construction; no durable backend is implied."""

    async def scheduler_now(self) -> datetime:
        """Expose the injected provider clock, independent of receiver-local wall clocks."""
        return utc_instant(self._now())

    async def acquire_scheduler_lease(
        self, deployment_id: str, holder_id: str, *, ttl_seconds: int
    ) -> SchedulerLease | None:
        """Acquire atomically on one event loop, retaining epochs across expiry and release."""
        duration = lease_duration(deployment_id, holder_id, ttl_seconds)
        now = utc_instant(self._now())
        current = self._scheduler_leases.get(deployment_id)
        if current is not None and current.expires_at > now:
            return replace(current, observed_at=now) if current.holder_id == holder_id else None
        try:
            expires = now + duration
        except OverflowError as exc:
            raise ValueError("lease expiry exceeds the timestamp range") from exc
        lease = SchedulerLease(
            deployment_id, holder_id, current.epoch + 1 if current else 1, expires, now
        )
        self._scheduler_leases[deployment_id] = lease
        return lease

    async def renew_scheduler_lease(
        self, lease: SchedulerLease, *, ttl_seconds: int
    ) -> SchedulerLease:
        """Renew a live epoch without rotating its fencing token."""
        duration = lease_duration(lease.deployment_id, lease.holder_id, ttl_seconds)
        now = utc_instant(self._now())
        current = self._scheduler_leases.get(lease.deployment_id)
        require_scheduler_lease(current, lease, now)
        assert current is not None
        try:
            expires = max(current.expires_at, now + duration)
        except OverflowError as exc:
            raise ValueError("lease expiry exceeds the timestamp range") from exc
        renewed = replace(current, expires_at=expires, observed_at=now)
        self._scheduler_leases[lease.deployment_id] = renewed
        return renewed

    async def release_scheduler_lease(self, lease: SchedulerLease) -> None:
        """Retain the fencing epoch while making the row immediately acquirable."""
        now = utc_instant(self._now())
        current = self._scheduler_leases.get(lease.deployment_id)
        require_scheduler_lease(current, lease, now)
        assert current is not None
        self._scheduler_leases[lease.deployment_id] = replace(
            current, expires_at=now, observed_at=now
        )

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
        skipped_count: int = 0,
    ) -> PendingScheduleFire:
        """Validate all state before publishing either the pending occurrence or advancement."""
        key = (str(workspace_id), schedule_id)
        row = self._subscriptions.get(key)
        selectors = self._selectors.get(key, [])
        snapshot = SubscriptionSnapshot(row, selectors[-1] if selectors else None) if row else None
        schedule = self._schedules.get(key)
        pending = prepare_schedule_fire(
            workspace_id,
            lease,
            self._scheduler_leases.get(lease.deployment_id),
            snapshot,
            schedule,
            expected_next_fire_at=expected_next_fire_at,
            expected_revision=expected_revision,
            planned_fire_at=planned_fire_at,
            next_fire_at=next_fire_at,
            now=self._now(),
            skipped_count=skipped_count,
        )
        pending_key = (str(workspace_id), pending.fire_id)
        if pending_key in self._pending_fires:
            raise ImmutableViolation("planned schedule occurrence already exists")
        commit_now = utc_instant(self._now())
        require_scheduler_lease(self._scheduler_leases.get(lease.deployment_id), lease, commit_now)
        if not pending.planned_fire_at <= commit_now < utc_instant(next_fire_at):
            raise ImmutableViolation("provider time moved outside the planned-fire window")
        assert schedule is not None
        self._pending_fires[pending_key] = pending
        self._schedules[key] = replace(schedule, next_fire_at=utc_instant(next_fire_at))
        return pending

    async def get_schedule_fire(
        self,
        workspace_id: WorkspaceId,
        fire_id: str,
    ) -> PendingScheduleFire | None:
        """Read retained occurrence proof without exposing another workspace's records."""
        return self._pending_fires.get((str(workspace_id), fire_id))

    async def query_pending_schedule_fires(
        self,
        workspace_id: WorkspaceId,
        deployment_id: str,
        *,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[PendingScheduleFire]:
        """Read the same bounded recovery pages and scoped cursors as PostgreSQL."""
        rows = [
            row
            for row in self._pending_fires.values()
            if row.workspace_id == workspace_id
            and row.deployment_id == deployment_id
            and row.delivered_at is None
            and row.discarded_at is None
        ]
        return _query_page(
            rows,
            ("pending-schedule-fires", str(workspace_id), deployment_id),
            cursor,
            limit,
            lambda row: row.fire_id,
        )

    async def acknowledge_schedule_fire(
        self, workspace_id: WorkspaceId, fire_id: str, lease: SchedulerLease
    ) -> None:
        """Mark a recovered occurrence delivered without letting stale holders acknowledge it."""
        now = utc_instant(self._now())
        require_scheduler_lease(self._scheduler_leases.get(lease.deployment_id), lease, now)
        key = (str(workspace_id), fire_id)
        row = self._pending_fires.get(key)
        if row is None or row.deployment_id != lease.deployment_id:
            raise ImmutableViolation("pending fire is absent from this workspace/deployment")
        if row.discarded_at is not None:
            raise ImmutableViolation("pending fire has been suppressed")
        if row.delivered_at is None:
            self._pending_fires[key] = replace(row, delivered_at=now)

    async def discard_schedule_fire(
        self,
        workspace_id: WorkspaceId,
        fire_id: str,
        lease: SchedulerLease,
        *,
        reason: str,
    ) -> None:
        """Persist suppression under the current fence, leaving an already published fire intact."""
        validate_schedule_discard(reason)
        now = utc_instant(self._now())
        require_scheduler_lease(self._scheduler_leases.get(lease.deployment_id), lease, now)
        key = (str(workspace_id), fire_id)
        row = self._pending_fires.get(key)
        if row is None or row.deployment_id != lease.deployment_id or row.delivered_at is not None:
            raise ImmutableViolation("pending fire is absent or already published")
        if row.discarded_at is None:
            self._pending_fires[key] = replace(row, discarded_at=now, discard_reason=reason)

    # ----- Subscriptions -----

    async def write_subscription(
        self,
        workspace_id: WorkspaceId,
        subscription: SplSubscription,
        selector: SplSubscriptionSelector,
        *,
        expected_revision: int | None,
        schedule: SplSchedule | None,
        reset_schedule: bool,
        preserve_state: bool = False,
    ) -> SubscriptionSnapshot:
        """Apply the shared transaction contract without yielding or publishing partial state."""
        key = (str(workspace_id), str(subscription.subscription_id))
        prior_row = self._subscriptions.get(key)
        prior_selectors = self._selectors.get(key, [])
        current = (
            SubscriptionSnapshot(prior_row, prior_selectors[-1] if prior_selectors else None)
            if prior_row is not None
            else None
        )
        snapshot, next_schedule = prepare_subscription_write(
            workspace_id,
            subscription,
            selector,
            current,
            self._schedules.get(key),
            expected_revision=expected_revision,
            schedule=schedule,
            reset_schedule=reset_schedule,
            preserve_state=preserve_state,
        )
        assert snapshot.selector is not None
        self._subscriptions[key] = snapshot.subscription
        self._selectors.setdefault(key, []).append(snapshot.selector)
        if next_schedule is not None:
            self._schedules[key] = next_schedule
        return snapshot

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
        if existing is not None and (
            existing.payload.get("source") != resume.payload.get("source")
            or existing.payload.get("deliveryProtocol", 1)
            != resume.payload.get("deliveryProtocol", 1)
            or existing.expires_at.astimezone(UTC) > as_of.astimezone(UTC)
        ):
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
    connector_references: ConnectorReferenceReader | None = None


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

    from custos_pg import PgConnectorInstanceAdapter, PgMetadataAdapter
    from custos_pg.pool import LazyPool

    # ``PgMetadataAdapter`` implements the full ``MetadataStoreProvider`` but
    # types its workspace-scoped args as plain ``str`` rather than the
    # ``WorkspaceId`` / ``SubscriptionId`` NewTypes this Protocol declares, so
    # mypy can't structurally see it as ``TriggerMetadataStore``-conforming at
    # this consumer boundary. custos-postgres has its own strict mypy job
    # verifying the adapter against the SPL Protocol at the implementation
    # site; the cast keeps the consumer view typed.
    lazy = LazyPool(metadata_store_dsn.strip())
    adapter = PgMetadataAdapter(lazy=lazy)
    return Providers(
        metadata_store=cast(TriggerMetadataStore, adapter),
        connector_references=PgConnectorInstanceAdapter(lazy=lazy),
    )

"""Postgres `MetadataStoreProvider` adapter — runtime/trigger/cursors slice.

Implements the SPL-013 (#127) scope:

  - Runtime execution state (`Run`, `Step`, `StepAttempt`).
  - Trigger Service state (subscriptions, selectors, resume tokens,
    dedup keys, schedules).
  - Connector pull cursors with the `SELECT ... FOR UPDATE NOWAIT`
    lease primitive (one row per `(workspace_id, instance_id)`,
    single-writer enforcement via `lease_holder` + `lease_expires_at`).
  - Artifact backrefs (append-only `ArtifactUse`).
  - Intra-provider `with_transaction` (powered by an asyncpg connection
    pinned for the body of the callback; handle ownership enforced by
    `custos_spl.middleware.transactions`).

Gateway short-lived state (idempotency records, device-code sessions)
and the audit writer + outbox drain live in #128 and #129; those
Protocol methods are present as `NotImplementedError` stubs so the
adapter still satisfies the runtime-checkable Protocol while the other
slices are landing iteratively.

Append-only contracts are enforced by primary keys. Postgres surfaces
key collisions as SQLSTATE `23505`, which `_classify` maps to
`ImmutableViolation`. The lease NOWAIT contention surfaces as `55P03`
which maps to `LeaseBusy`.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Awaitable, Callable, Mapping
from collections.abc import Set as AbstractSet
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, TypeVar, cast

from custos_spl.errors import (
    BackendUnavailable,
    ImmutableViolation,
    InvalidTransactionHandle,
    LeaseBusy,
    LeaseExpired,
    NotReserved,
)
from custos_spl.interfaces.metadata_store import (
    ArtifactUse,
    AuditEvent,
    AuditFilter,
    AuditOutboxBatch,
    ConnectorCursor,
    DedupDuplicate,
    DedupKey,
    DedupReserved,
    DeviceCodeSession,
    ExistingCompleted,
    ExistingInFlight,
    IdempotencyRecord,
    IdemReserved,
    KeyReuse,
    LeaseHandle,
    NotifyEvent,
    PutDedupKeyResult,
    ReserveIdempotencyResult,
    ResumeSubscription,
    Run,
    RunFilter,
    Schedule,
    Step,
    StepAttempt,
    Subscription,
    SubscriptionSelector,
    TransactionHandle,
)
from custos_spl.middleware.transactions import bind_handle, check_handle
from custos_spl.pagination import Cursor, Page

from custos_pg.migrations.metadata import METADATA_REV1, METADATA_REV2, METADATA_REV3
from custos_pg.pool import LazyPool, read_dsn_from_env
from custos_pg.revisions import ensure_ledger, read_declared, record_revision

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from asyncpg import Connection, Record
    from asyncpg.pool import Pool

INTERFACE_NAME = "MetadataStoreProvider"
DEFAULT_LIST_LIMIT = 50
MAX_LIST_LIMIT = 200

T = TypeVar("T")


# ----- Handle types -----


class PgLeaseHandle(LeaseHandle):
    """Concrete handle returned by `acquire_cursor_lease`.

    Carries the identifiers the commit/release operations need to
    recognize the lease: workspace, instance, and the holder string
    the caller supplied at acquire time. The `lease_holder` column in
    the row is the abstract "fencing token" — a different holder
    re-acquiring invalidates this handle's `commit_cursor` (the
    `WHERE lease_holder = ?` clause matches zero rows and we raise
    `LeaseExpired`).
    """

    __slots__ = ("holder_id", "instance_id", "workspace_id")

    def __init__(self, workspace_id: str, instance_id: str, holder_id: str) -> None:
        self.workspace_id = workspace_id
        self.instance_id = instance_id
        self.holder_id = holder_id


class PgTransactionHandle(TransactionHandle):
    """Concrete handle pinning a single asyncpg connection inside `with_transaction`.

    The connection has an active `BEGIN`; tx-aware methods that
    receive this handle issue their statements on the pinned
    connection so they share atomicity with the rest of the callback.

    The handle is one-shot: `with_transaction` marks it `closed` in a
    `finally` block once the callback returns (success or failure).
    A caller that retains a reference and passes it back later gets
    `InvalidTransactionHandle` from tx-aware methods instead of
    silently running on a connection that has been returned to the
    pool — `check_handle` alone only verifies provider ownership, not
    liveness of the underlying transaction.
    """

    __slots__ = ("_closed", "_conn")

    def __init__(self, conn: Connection) -> None:
        self._conn = conn
        self._closed = False

    @property
    def conn(self) -> Connection:
        return self._conn

    @property
    def closed(self) -> bool:
        return self._closed

    def _mark_closed(self) -> None:
        self._closed = True


# ----- Cursor helpers (keyset pagination) -----


def _encode_cursor(*parts: Any) -> Cursor:
    raw = json.dumps([_jsonable(p) for p in parts]).encode("utf-8")
    return Cursor(token=base64.urlsafe_b64encode(raw).decode("ascii"))


def _decode_cursor(cursor: Cursor) -> list[Any]:
    raw = base64.urlsafe_b64decode(cursor.token.encode("ascii"))
    return cast(list[Any], json.loads(raw))


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _resolve_limit(limit: int | None) -> int:
    if limit is None:
        return DEFAULT_LIST_LIMIT
    if limit < 1:
        return DEFAULT_LIST_LIMIT
    return min(limit, MAX_LIST_LIMIT)


# ----- Row → dataclass mapping -----


def _json_payload(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, bytes, bytearray)):
        return json.loads(value)
    return value


def _row_to_run(row: Record) -> Run:
    return Run(
        workspace_id=row["workspace_id"],
        run_id=row["run_id"],
        workflow_id=row["workflow_id"],
        workflow_version=row["workflow_version"],
        status=row["status"],
        reason=row["reason"],
        started_at=row["started_at"],
        updated_at=row["updated_at"],
    )


def _row_to_step(row: Record) -> Step:
    return Step(
        workspace_id=row["workspace_id"],
        run_id=row["run_id"],
        step_id=row["step_id"],
        name=row["name"],
        status=row["status"],
        created_at=row["created_at"],
    )


def _row_to_step_attempt(row: Record) -> StepAttempt:
    err = _json_payload(row["error"])
    return StepAttempt(
        workspace_id=row["workspace_id"],
        run_id=row["run_id"],
        step_id=row["step_id"],
        attempt=row["attempt"],
        status=row["status"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        error=MappingProxyType(dict(err)) if err is not None else None,
    )


def _row_to_subscription(row: Record) -> Subscription:
    return Subscription(
        workspace_id=row["workspace_id"],
        subscription_id=row["subscription_id"],
        workflow_id=row["workflow_id"],
        state=row["state"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _row_to_subscription_selector(row: Record) -> SubscriptionSelector:
    sel = _json_payload(row["selector"]) or {}
    return SubscriptionSelector(
        workspace_id=row["workspace_id"],
        subscription_id=row["subscription_id"],
        selector=MappingProxyType(dict(sel)),
        added_at=row["added_at"],
    )


def _row_to_resume(row: Record) -> ResumeSubscription:
    payload = _json_payload(row["payload"]) or {}
    return ResumeSubscription(
        workspace_id=row["workspace_id"],
        resume_id=row["resume_id"],
        run_id=row["run_id"],
        step_id=row["step_id"],
        expires_at=row["expires_at"],
        payload=MappingProxyType(dict(payload)),
    )


def _row_to_dedup(row: Record) -> DedupKey:
    return DedupKey(
        workspace_id=row["workspace_id"],
        key=row["key"],
        expires_at=row["expires_at"],
    )


def _row_to_schedule(row: Record) -> Schedule:
    return Schedule(
        workspace_id=row["workspace_id"],
        schedule_id=row["schedule_id"],
        workflow_id=row["workflow_id"],
        cron=row["cron"],
        next_fire_at=row["next_fire_at"],
        enabled=row["enabled"],
    )


def _row_to_cursor(row: Record) -> ConnectorCursor:
    return ConnectorCursor(
        workspace_id=row["workspace_id"],
        instance_id=row["instance_id"],
        value=row["value"],
        advanced_at=row["advanced_at"],
        lease_holder=row["lease_holder"],
        lease_expires_at=row["lease_expires_at"],
    )


def _row_to_artifact_use(row: Record) -> ArtifactUse:
    return ArtifactUse(
        workspace_id=row["workspace_id"],
        run_id=row["run_id"],
        step_id=row["step_id"],
        artifact_id=row["artifact_id"],
        name=row["name"],
        recorded_at=row["recorded_at"],
    )


def _row_to_idempotency_record(row: Record) -> IdempotencyRecord:
    return IdempotencyRecord(
        workspace_id=row["workspace_id"],
        principal_id=row["principal_id"],
        route=row["route"],
        idempotency_key=row["idempotency_key"],
        request_hash=row["request_hash"],
        status=row["status"],
        response_snapshot=(
            MappingProxyType(dict(_json_payload(row["response_snapshot"])))
            if row["response_snapshot"] is not None
            else None
        ),
        reserved_at=row["reserved_at"],
        expires_at=row["expires_at"],
    )


def _row_to_device_code_session(row: Record) -> DeviceCodeSession:
    return DeviceCodeSession(
        workspace_id=row["workspace_id"],
        device_code=row["device_code"],
        user_code=row["user_code"],
        issuer_alias=row["issuer_alias"],
        status=row["status"],
        token_bundle=(
            MappingProxyType(dict(_json_payload(row["token_bundle"])))
            if row["token_bundle"] is not None
            else None
        ),
        created_at=row["created_at"],
        expires_at=row["expires_at"],
    )


class PgMetadataAdapter:
    """asyncpg implementation of `MetadataStoreProvider` — #127 slice.

    Construction mirrors the existing Pg adapters: tests inject a live
    `pool`; production callers receive an adapter from `make_adapter()`
    which wraps a `LazyPool` so the DSN is captured synchronously and
    the pool is built inside the running event loop.
    """

    SCHEMA_REVISION = 4

    def __init__(
        self,
        pool: Pool | None = None,
        *,
        lazy: LazyPool | None = None,
    ) -> None:
        if pool is None and lazy is None:
            raise ValueError(
                "PgMetadataAdapter requires either `pool` or `lazy`."
            )
        self._pool: Pool | None = pool
        self._lazy = lazy
        self._applied_revisions: set[int] = set()

    async def _pool_ref(self) -> Pool:
        if self._pool is None:
            assert self._lazy is not None
            self._pool = await self._lazy.get()
        return self._pool

    # ----- MigrationCapable -----

    @property
    def declared_revisions(self) -> Mapping[str, AbstractSet[int]]:
        return MappingProxyType({INTERFACE_NAME: frozenset(self._applied_revisions)})

    async def refresh_declared(self) -> None:
        pool = await self._pool_ref()
        declared = await read_declared(pool, (INTERFACE_NAME,))
        self._applied_revisions = set(declared.get(INTERFACE_NAME, set()))

    async def apply_pending(self) -> list[str]:
        """Apply revisions in scope for this adapter slice.

        Revisions 1-3 live in this PR and #128; revision 4 (audit
        outbox) lands in #129. `declared_revisions` reports whatever was
        applied (typically {1, 2, 3} after both PRs merge), leaving a gap
        for the platform's `SCHEMA_REVISION = 4` until the audit PR lands
        — this is an intentional, transparent iteration step.
        """
        summaries: list[str] = []
        pool = await self._pool_ref()
        async with pool.acquire() as conn, conn.transaction():
            await ensure_ledger(conn)
            applied = await conn.fetch(
                "SELECT revision FROM custos_meta.adapter_revisions "
                "WHERE interface_name = $1",
                INTERFACE_NAME,
            )
            already = {int(r["revision"]) for r in applied}
            for rev in (METADATA_REV1, METADATA_REV2, METADATA_REV3):
                if rev.number in already:
                    continue
                for stmt in rev.statements:
                    await conn.execute(stmt)
                await record_revision(conn, INTERFACE_NAME, rev.number)
                summaries.append(
                    f"applied {INTERFACE_NAME} rev{rev.number} "
                    f"({len(rev.statements)} statements)"
                )
        await self.refresh_declared()
        return summaries

    # ----- Runtime execution state -----

    async def put_run(self, workspace_id: str, run: Run) -> Run:
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "INSERT INTO custos_state.run "
                    "(workspace_id, run_id, workflow_id, workflow_version, "
                    " status, reason, started_at, updated_at) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) "
                    "RETURNING workspace_id, run_id, workflow_id, workflow_version, "
                    "status, reason, started_at, updated_at",
                    workspace_id,
                    run.run_id,
                    run.workflow_id,
                    run.workflow_version,
                    run.status,
                    run.reason,
                    run.started_at,
                    run.updated_at,
                )
        except Exception as exc:
            raise self._classify(exc) from exc
        assert row is not None
        return _row_to_run(row)

    async def update_run_status(
        self,
        workspace_id: str,
        run_id: str,
        status: str,
        reason: str | None = None,
    ) -> Run:
        # TODO(#129): every transition is audited — wire `appendAudit`
        # once the audit writer slice lands.
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE custos_state.run "
                "SET status = $3, reason = $4, updated_at = now() "
                "WHERE workspace_id = $1 AND run_id = $2 "
                "RETURNING workspace_id, run_id, workflow_id, workflow_version, "
                "status, reason, started_at, updated_at",
                workspace_id,
                run_id,
                status,
                reason,
            )
        if row is None:
            raise ValueError(f"unknown run: {workspace_id!r}/{run_id!r}")
        return _row_to_run(row)

    async def get_run(self, workspace_id: str, run_id: str) -> Run | None:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT workspace_id, run_id, workflow_id, workflow_version, "
                "status, reason, started_at, updated_at "
                "FROM custos_state.run "
                "WHERE workspace_id = $1 AND run_id = $2",
                workspace_id,
                run_id,
            )
        return _row_to_run(row) if row is not None else None

    async def list_runs(
        self,
        workspace_id: str,
        filter: RunFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[Run]:
        eff_limit = _resolve_limit(limit)
        params: list[Any] = [workspace_id]
        where = ["workspace_id = $1"]
        if filter is not None:
            if filter.workflow_id is not None:
                params.append(filter.workflow_id)
                where.append(f"workflow_id = ${len(params)}")
            if filter.status is not None:
                params.append(filter.status)
                where.append(f"status = ${len(params)}")
            if filter.started_after is not None:
                params.append(filter.started_after)
                where.append(f"started_at >= ${len(params)}")
            if filter.started_before is not None:
                params.append(filter.started_before)
                where.append(f"started_at < ${len(params)}")
        if cursor is not None:
            ts_iso, rid = _decode_cursor(cursor)
            params.extend([datetime.fromisoformat(ts_iso), rid])
            where.append(
                f"(started_at, run_id) < (${len(params) - 1}, ${len(params)})"
            )
        params.append(eff_limit + 1)
        sql = (
            "SELECT workspace_id, run_id, workflow_id, workflow_version, "
            "status, reason, started_at, updated_at "
            "FROM custos_state.run "
            f"WHERE {' AND '.join(where)} "
            "ORDER BY started_at DESC, run_id DESC "
            f"LIMIT ${len(params)}"
        )
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        next_cursor: Cursor | None = None
        if len(rows) > eff_limit:
            last = rows[eff_limit - 1]
            next_cursor = _encode_cursor(last["started_at"], last["run_id"])
            rows = rows[:eff_limit]
        return Page(items=[_row_to_run(r) for r in rows], next_cursor=next_cursor)

    async def append_step(self, workspace_id: str, run_id: str, step: Step) -> Step:
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "INSERT INTO custos_state.step "
                    "(workspace_id, run_id, step_id, name, status, created_at) "
                    "VALUES ($1, $2, $3, $4, $5, $6) "
                    "RETURNING workspace_id, run_id, step_id, name, status, created_at",
                    workspace_id,
                    run_id,
                    step.step_id,
                    step.name,
                    step.status,
                    step.created_at,
                )
        except Exception as exc:
            raise self._classify(exc) from exc
        assert row is not None
        return _row_to_step(row)

    async def append_step_attempt(
        self,
        workspace_id: str,
        run_id: str,
        step_id: str,
        attempt: StepAttempt,
    ) -> StepAttempt:
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "INSERT INTO custos_state.step_attempt "
                    "(workspace_id, run_id, step_id, attempt, status, "
                    " started_at, finished_at, error) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb) "
                    "RETURNING workspace_id, run_id, step_id, attempt, status, "
                    "started_at, finished_at, error",
                    workspace_id,
                    run_id,
                    step_id,
                    attempt.attempt,
                    attempt.status,
                    attempt.started_at,
                    attempt.finished_at,
                    json.dumps(dict(attempt.error)) if attempt.error is not None else None,
                )
        except Exception as exc:
            raise self._classify(exc) from exc
        assert row is not None
        return _row_to_step_attempt(row)

    async def get_step_attempts(
        self, workspace_id: str, run_id: str, step_id: str
    ) -> tuple[StepAttempt, ...]:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT workspace_id, run_id, step_id, attempt, status, "
                "started_at, finished_at, error "
                "FROM custos_state.step_attempt "
                "WHERE workspace_id = $1 AND run_id = $2 AND step_id = $3 "
                "ORDER BY attempt ASC",
                workspace_id,
                run_id,
                step_id,
            )
        return tuple(_row_to_step_attempt(r) for r in rows)

    # ----- Trigger Service state -----

    async def put_subscription(
        self, workspace_id: str, subscription: Subscription
    ) -> Subscription:
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "INSERT INTO custos_state.subscription "
                    "(workspace_id, subscription_id, workflow_id, state, "
                    " created_at, updated_at) "
                    "VALUES ($1, $2, $3, $4, $5, $6) "
                    "RETURNING workspace_id, subscription_id, workflow_id, state, "
                    "created_at, updated_at",
                    workspace_id,
                    subscription.subscription_id,
                    subscription.workflow_id,
                    subscription.state,
                    subscription.created_at,
                    subscription.updated_at,
                )
        except Exception as exc:
            raise self._classify(exc) from exc
        assert row is not None
        return _row_to_subscription(row)

    async def update_subscription_state(
        self,
        workspace_id: str,
        subscription_id: str,
        state: str,
    ) -> Subscription:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE custos_state.subscription "
                "SET state = $3, updated_at = now() "
                "WHERE workspace_id = $1 AND subscription_id = $2 "
                "RETURNING workspace_id, subscription_id, workflow_id, state, "
                "created_at, updated_at",
                workspace_id,
                subscription_id,
                state,
            )
        if row is None:
            raise ValueError(
                f"unknown subscription: {workspace_id!r}/{subscription_id!r}"
            )
        return _row_to_subscription(row)

    async def append_subscription_selector(
        self,
        workspace_id: str,
        subscription_id: str,
        selector: SubscriptionSelector,
    ) -> SubscriptionSelector:
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "INSERT INTO custos_state.subscription_selector "
                    "(workspace_id, subscription_id, selector, added_at) "
                    "VALUES ($1, $2, $3::jsonb, $4) "
                    "RETURNING workspace_id, subscription_id, selector, added_at",
                    workspace_id,
                    subscription_id,
                    json.dumps(dict(selector.selector)),
                    selector.added_at,
                )
        except Exception as exc:
            raise self._classify(exc) from exc
        assert row is not None
        return _row_to_subscription_selector(row)

    async def put_resume_subscription(
        self, workspace_id: str, resume: ResumeSubscription
    ) -> ResumeSubscription:
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "INSERT INTO custos_state.resume_subscription "
                    "(workspace_id, resume_id, run_id, step_id, expires_at, payload) "
                    "VALUES ($1, $2, $3, $4, $5, $6::jsonb) "
                    "RETURNING workspace_id, resume_id, run_id, step_id, "
                    "expires_at, payload",
                    workspace_id,
                    resume.resume_id,
                    resume.run_id,
                    resume.step_id,
                    resume.expires_at,
                    json.dumps(dict(resume.payload)),
                )
        except Exception as exc:
            raise self._classify(exc) from exc
        assert row is not None
        return _row_to_resume(row)

    async def delete_resume_subscription(
        self, workspace_id: str, resume_id: str
    ) -> None:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM custos_state.resume_subscription "
                "WHERE workspace_id = $1 AND resume_id = $2",
                workspace_id,
                resume_id,
            )

    async def put_dedup_key(
        self, workspace_id: str, key: str, ttl_seconds: int
    ) -> PutDedupKeyResult:
        # Reserve-or-read in one round trip: the `ON CONFLICT DO
        # UPDATE … WHERE expires_at <= now()` clause atomically claims
        # the slot if a stale row is present, otherwise the existing
        # un-expired row blocks the update and RETURNING is empty.
        expires_at = datetime.now(UTC) + timedelta(seconds=ttl_seconds)
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO custos_state.dedup_key (workspace_id, key, expires_at) "
                "VALUES ($1, $2, $3) "
                "ON CONFLICT (workspace_id, key) DO UPDATE "
                "SET expires_at = EXCLUDED.expires_at "
                "WHERE custos_state.dedup_key.expires_at <= now() "
                "RETURNING workspace_id, key, expires_at",
                workspace_id,
                key,
                expires_at,
            )
            if row is not None:
                return DedupReserved(key=_row_to_dedup(row))
            existing = await conn.fetchrow(
                "SELECT workspace_id, key, expires_at "
                "FROM custos_state.dedup_key "
                "WHERE workspace_id = $1 AND key = $2",
                workspace_id,
                key,
            )
        assert existing is not None
        return DedupDuplicate(existing=_row_to_dedup(existing))

    async def put_schedule(self, workspace_id: str, schedule: Schedule) -> Schedule:
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "INSERT INTO custos_state.schedule "
                    "(workspace_id, schedule_id, workflow_id, cron, "
                    " next_fire_at, enabled) "
                    "VALUES ($1, $2, $3, $4, $5, $6) "
                    "RETURNING workspace_id, schedule_id, workflow_id, cron, "
                    "next_fire_at, enabled",
                    workspace_id,
                    schedule.schedule_id,
                    schedule.workflow_id,
                    schedule.cron,
                    schedule.next_fire_at,
                    schedule.enabled,
                )
        except Exception as exc:
            raise self._classify(exc) from exc
        assert row is not None
        return _row_to_schedule(row)

    async def update_schedule_next_fire(
        self,
        workspace_id: str,
        schedule_id: str,
        next_fire_at: datetime,
    ) -> Schedule:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE custos_state.schedule "
                "SET next_fire_at = $3 "
                "WHERE workspace_id = $1 AND schedule_id = $2 "
                "RETURNING workspace_id, schedule_id, workflow_id, cron, "
                "next_fire_at, enabled",
                workspace_id,
                schedule_id,
                next_fire_at,
            )
        if row is None:
            raise ValueError(f"unknown schedule: {workspace_id!r}/{schedule_id!r}")
        return _row_to_schedule(row)

    # ----- Connector pull cursors -----

    async def acquire_cursor_lease(
        self,
        workspace_id: str,
        instance_id: str,
        holder_id: str,
        ttl_seconds: int,
    ) -> tuple[ConnectorCursor, LeaseHandle]:
        """Acquire the single-writer lease.

        Strategy:

        1. `pg_try_advisory_xact_lock(hashtext(ws), hashtext(inst))` —
           a non-blocking serializer on the `(workspace_id,
           instance_id)` key. Without this, two concurrent first-touch
           acquires both hit `INSERT … ON CONFLICT DO NOTHING` and the
           loser blocks waiting for the winner's unique-index check
           to commit, which defeats the NOWAIT semantics we promise.
           The advisory lock returns immediately and auto-releases at
           COMMIT/ROLLBACK. Failure → `LeaseBusy`.
        2. INSERT-if-missing (race-free now that we hold the advisory
           lock).
        3. `SELECT … FOR UPDATE NOWAIT` to take the row lock against
           concurrent `commit_cursor` / `release_cursor_lease` on
           other connections — `55P03` here also classifies into
           `LeaseBusy`.
        4. If the row already carries an un-expired lease held by a
           different holder, raise `LeaseBusy` ourselves.
        5. Write the lease and return a `PgLeaseHandle` carrying the
           identifiers needed by `commit_cursor` /
           `release_cursor_lease`.
        """
        new_expires = datetime.now(UTC) + timedelta(seconds=ttl_seconds)
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn, conn.transaction():
                locked = await conn.fetchval(
                    "SELECT pg_try_advisory_xact_lock("
                    "hashtext($1), hashtext($2))",
                    workspace_id,
                    instance_id,
                )
                if not locked:
                    raise LeaseBusy(
                        f"connector_cursor {workspace_id!r}/{instance_id!r} "
                        f"is being acquired by another caller"
                    )
                # First-touch insert; subsequent acquires hit the SELECT.
                await conn.execute(
                    "INSERT INTO custos_state.connector_cursor "
                    "(workspace_id, instance_id, value, advanced_at) "
                    "VALUES ($1, $2, '', now()) "
                    "ON CONFLICT DO NOTHING",
                    workspace_id,
                    instance_id,
                )
                row = await conn.fetchrow(
                    "SELECT workspace_id, instance_id, value, advanced_at, "
                    "lease_holder, lease_expires_at "
                    "FROM custos_state.connector_cursor "
                    "WHERE workspace_id = $1 AND instance_id = $2 "
                    "FOR UPDATE NOWAIT",
                    workspace_id,
                    instance_id,
                )
                assert row is not None
                if (
                    row["lease_holder"] is not None
                    and row["lease_holder"] != holder_id
                    and row["lease_expires_at"] is not None
                    and row["lease_expires_at"] > datetime.now(UTC)
                ):
                    raise LeaseBusy(
                        f"connector_cursor {workspace_id!r}/{instance_id!r} "
                        f"held by {row['lease_holder']!r} until "
                        f"{row['lease_expires_at'].isoformat()}"
                    )
                updated = await conn.fetchrow(
                    "UPDATE custos_state.connector_cursor "
                    "SET lease_holder = $3, lease_expires_at = $4 "
                    "WHERE workspace_id = $1 AND instance_id = $2 "
                    "RETURNING workspace_id, instance_id, value, advanced_at, "
                    "lease_holder, lease_expires_at",
                    workspace_id,
                    instance_id,
                    holder_id,
                    new_expires,
                )
                assert updated is not None
        except LeaseBusy:
            raise
        except Exception as exc:
            raise self._classify(exc) from exc
        return _row_to_cursor(updated), PgLeaseHandle(
            workspace_id=workspace_id,
            instance_id=instance_id,
            holder_id=holder_id,
        )

    async def commit_cursor(
        self,
        workspace_id: str,
        lease: LeaseHandle,
        new_value: str,
        new_advanced_at: datetime,
    ) -> ConnectorCursor:
        if not isinstance(lease, PgLeaseHandle):
            raise LeaseExpired(
                f"lease handle was not issued by PgMetadataAdapter "
                f"(got {type(lease).__name__})"
            )
        if lease.workspace_id != workspace_id:
            raise LeaseExpired(
                "lease handle workspace does not match commit_cursor argument"
            )
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE custos_state.connector_cursor "
                "SET value = $4, advanced_at = $5, "
                "    lease_holder = NULL, lease_expires_at = NULL "
                "WHERE workspace_id = $1 AND instance_id = $2 "
                "  AND lease_holder = $3 "
                "  AND lease_expires_at IS NOT NULL "
                "  AND lease_expires_at > now() "
                "RETURNING workspace_id, instance_id, value, advanced_at, "
                "lease_holder, lease_expires_at",
                workspace_id,
                lease.instance_id,
                lease.holder_id,
                new_value,
                new_advanced_at,
            )
        if row is None:
            raise LeaseExpired(
                f"lease on {workspace_id!r}/{lease.instance_id!r} held by "
                f"{lease.holder_id!r} has expired or been reassigned"
            )
        return _row_to_cursor(row)

    async def release_cursor_lease(
        self, workspace_id: str, lease: LeaseHandle
    ) -> None:
        if not isinstance(lease, PgLeaseHandle):
            return  # idempotent: foreign handles are no-ops here
        if lease.workspace_id != workspace_id:
            return
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE custos_state.connector_cursor "
                "SET lease_holder = NULL, lease_expires_at = NULL "
                "WHERE workspace_id = $1 AND instance_id = $2 "
                "  AND lease_holder = $3",
                workspace_id,
                lease.instance_id,
                lease.holder_id,
            )

    async def read_cursor(
        self, workspace_id: str, instance_id: str
    ) -> ConnectorCursor | None:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT workspace_id, instance_id, value, advanced_at, "
                "lease_holder, lease_expires_at "
                "FROM custos_state.connector_cursor "
                "WHERE workspace_id = $1 AND instance_id = $2",
                workspace_id,
                instance_id,
            )
        return _row_to_cursor(row) if row is not None else None

    async def rewind_cursor(
        self,
        workspace_id: str,
        instance_id: str,
        new_value: str,
        actor: str,
        reason: str,
    ) -> ConnectorCursor:
        # TODO(#129): emit a `cursor.rewound` audit event with actor +
        # reason inside the same transaction once the audit writer
        # slice lands. The arguments are accepted here so callers can
        # already use the operator API; the audit row will appear
        # retroactively when #129 wires `appendAudit` into this body.
        del actor, reason
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE custos_state.connector_cursor "
                "SET value = $3, advanced_at = now(), "
                "    lease_holder = NULL, lease_expires_at = NULL "
                "WHERE workspace_id = $1 AND instance_id = $2 "
                "RETURNING workspace_id, instance_id, value, advanced_at, "
                "lease_holder, lease_expires_at",
                workspace_id,
                instance_id,
                new_value,
            )
        if row is None:
            raise ValueError(
                f"unknown connector_cursor: {workspace_id!r}/{instance_id!r}"
            )
        return _row_to_cursor(row)

    # ----- Artifact backrefs -----

    async def append_artifact_use(
        self, workspace_id: str, use: ArtifactUse
    ) -> ArtifactUse:
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "INSERT INTO custos_state.artifact_use "
                    "(workspace_id, run_id, step_id, artifact_id, name, recorded_at) "
                    "VALUES ($1, $2, $3, $4, $5, $6) "
                    "RETURNING workspace_id, run_id, step_id, artifact_id, name, "
                    "recorded_at",
                    workspace_id,
                    use.run_id,
                    use.step_id,
                    use.artifact_id,
                    use.name,
                    use.recorded_at,
                )
        except Exception as exc:
            raise self._classify(exc) from exc
        assert row is not None
        return _row_to_artifact_use(row)

    async def list_artifact_uses(
        self,
        workspace_id: str,
        artifact_id: str,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ArtifactUse]:
        eff_limit = _resolve_limit(limit)
        params: list[Any] = [workspace_id, artifact_id]
        where = ["workspace_id = $1", "artifact_id = $2"]
        if cursor is not None:
            (seq,) = _decode_cursor(cursor)
            params.append(int(seq))
            where.append(f"seq < ${len(params)}")
        params.append(eff_limit + 1)
        sql = (
            "SELECT workspace_id, seq, run_id, step_id, artifact_id, name, recorded_at "
            "FROM custos_state.artifact_use "
            f"WHERE {' AND '.join(where)} "
            "ORDER BY seq DESC "
            f"LIMIT ${len(params)}"
        )
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        next_cursor: Cursor | None = None
        if len(rows) > eff_limit:
            last = rows[eff_limit - 1]
            next_cursor = _encode_cursor(int(last["seq"]))
            rows = rows[:eff_limit]
        return Page(
            items=[_row_to_artifact_use(r) for r in rows],
            next_cursor=next_cursor,
        )

    # ----- Gateway short-lived state (#128) -----

    async def reserve_idempotency_record(
        self,
        workspace_id: str,
        principal_id: str,
        route: str,
        idempotency_key: str,
        request_hash: str,
        ttl_seconds: int,
    ) -> ReserveIdempotencyResult:
        """Atomic reserve-or-read: CAS to insert a fresh record or detect conflicts.

        Returns one of:
        - `IdemReserved` if the key was not present (caller should perform work).
        - `ExistingCompleted` if an identical hash is already done (return snapshot).
        - `ExistingInFlight` if an identical hash is still in progress (retry).
        - `KeyReuse` if the key exists with a different hash (422 response).
        """
        expires_at = datetime.now(UTC) + timedelta(seconds=ttl_seconds)
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            # Try to insert a fresh record; if it exists, fetch the existing one.
            row = await conn.fetchrow(
                "INSERT INTO custos_state.idempotency_record "
                "(workspace_id, principal_id, route, idempotency_key, "
                " request_hash, status, reserved_at, expires_at) "
                "VALUES ($1, $2, $3, $4, $5, 'in_progress', now(), $6) "
                "ON CONFLICT (workspace_id, principal_id, route, idempotency_key) "
                "DO NOTHING "
                "RETURNING workspace_id, principal_id, route, idempotency_key, "
                "request_hash, status, response_snapshot, reserved_at, expires_at",
                workspace_id,
                principal_id,
                route,
                idempotency_key,
                request_hash,
                expires_at,
            )
            if row is not None:
                return IdemReserved(record=_row_to_idempotency_record(row))
            # Key exists; fetch it to detect conflicts or in-flight status.
            existing = await conn.fetchrow(
                "SELECT workspace_id, principal_id, route, idempotency_key, "
                "request_hash, status, response_snapshot, reserved_at, expires_at "
                "FROM custos_state.idempotency_record "
                "WHERE workspace_id = $1 AND principal_id = $2 "
                "  AND route = $3 AND idempotency_key = $4",
                workspace_id,
                principal_id,
                route,
                idempotency_key,
            )
            assert existing is not None
            rec = _row_to_idempotency_record(existing)
            if existing["request_hash"] != request_hash:
                return KeyReuse(record=rec)
            if existing["status"] == "in_progress":
                return ExistingInFlight(record=rec)
            # status == "completed"
            assert existing["response_snapshot"] is not None
            return ExistingCompleted(
                record=rec,
                response_snapshot=MappingProxyType(
                    dict(_json_payload(existing["response_snapshot"]))
                ),
            )

    async def complete_idempotency_record(
        self,
        workspace_id: str,
        principal_id: str,
        route: str,
        idempotency_key: str,
        response_snapshot: Mapping[str, Any],
    ) -> IdempotencyRecord:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE custos_state.idempotency_record "
                "SET status = 'completed', response_snapshot = $5::jsonb "
                "WHERE workspace_id = $1 AND principal_id = $2 "
                "  AND route = $3 AND idempotency_key = $4 "
                "  AND status = 'in_progress' "
                "RETURNING workspace_id, principal_id, route, idempotency_key, "
                "request_hash, status, response_snapshot, reserved_at, expires_at",
                workspace_id,
                principal_id,
                route,
                idempotency_key,
                json.dumps(dict(response_snapshot)),
            )
        if row is None:
            raise NotReserved(
                f"no in-progress reservation for "
                f"{workspace_id!r}/{principal_id!r}/{route!r}/{idempotency_key!r}"
            )
        return _row_to_idempotency_record(row)

    async def delete_expired_idempotency_records(self, before: datetime) -> int:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM custos_state.idempotency_record "
                "WHERE expires_at < $1",
                before,
            )
        # result is a string like "DELETE 42"; extract count.
        count = int(result.split()[-1]) if result else 0
        return count

    async def put_device_code_session(
        self, workspace_id: str, session: DeviceCodeSession
    ) -> DeviceCodeSession:
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "INSERT INTO custos_state.device_code_session "
                    "(workspace_id, device_code, user_code, issuer_alias, status, "
                    " created_at, expires_at) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7) "
                    "RETURNING workspace_id, device_code, user_code, issuer_alias, "
                    "status, token_bundle, created_at, expires_at",
                    workspace_id,
                    session.device_code,
                    session.user_code,
                    session.issuer_alias,
                    session.status,
                    session.created_at,
                    session.expires_at,
                )
        except Exception as exc:
            raise self._classify(exc) from exc
        assert row is not None
        return _row_to_device_code_session(row)

    async def get_device_code_session_by_device_code(
        self, workspace_id: str, device_code: str
    ) -> DeviceCodeSession | None:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT workspace_id, device_code, user_code, issuer_alias, status, "
                "token_bundle, created_at, expires_at "
                "FROM custos_state.device_code_session "
                "WHERE workspace_id = $1 AND device_code = $2",
                workspace_id,
                device_code,
            )
        return _row_to_device_code_session(row) if row is not None else None

    async def get_device_code_session_by_user_code(
        self, workspace_id: str, user_code: str
    ) -> DeviceCodeSession | None:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT workspace_id, device_code, user_code, issuer_alias, status, "
                "token_bundle, created_at, expires_at "
                "FROM custos_state.device_code_session "
                "WHERE workspace_id = $1 AND user_code = $2",
                workspace_id,
                user_code,
            )
        return _row_to_device_code_session(row) if row is not None else None

    async def complete_device_code_session(
        self,
        workspace_id: str,
        device_code: str,
        token_bundle: Mapping[str, Any],
    ) -> DeviceCodeSession:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE custos_state.device_code_session "
                "SET status = 'completed', token_bundle = $3::jsonb "
                "WHERE workspace_id = $1 AND device_code = $2 "
                "RETURNING workspace_id, device_code, user_code, issuer_alias, status, "
                "token_bundle, created_at, expires_at",
                workspace_id,
                device_code,
                json.dumps(dict(token_bundle)),
            )
        if row is None:
            raise ValueError(
                f"no device-code session for {workspace_id!r}/{device_code!r}"
            )
        return _row_to_device_code_session(row)

    async def delete_expired_device_code_sessions(self, before: datetime) -> int:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM custos_state.device_code_session "
                "WHERE expires_at < $1",
                before,
            )
        count = int(result.split()[-1]) if result else 0
        return count

    # ----- Audit writer + outbox drain (#129 — stubs) -----

    async def append_audit(
        self,
        workspace_id: str,
        event: AuditEvent,
        tx: TransactionHandle | None = None,
    ) -> None:
        self._check_tx_handle(tx)
        raise NotImplementedError("append_audit lands in SPL-015 (#129)")

    async def query_audit(
        self,
        workspace_id: str,
        filter: AuditFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[AuditEvent]:
        raise NotImplementedError("query_audit lands in SPL-015 (#129)")

    async def stream_audit_outbox(
        self, cursor: int, batch_size: int
    ) -> AuditOutboxBatch:
        raise NotImplementedError("stream_audit_outbox lands in SPL-015 (#129)")

    async def commit_audit_outbox_cursor(
        self, pipeline_id: str, cursor: int
    ) -> None:
        raise NotImplementedError(
            "commit_audit_outbox_cursor lands in SPL-015 (#129)"
        )

    def listen_audit_outbox(self) -> AsyncIterator[NotifyEvent]:
        # Declared as a coroutine-free method on the Protocol; if a
        # caller awaits this stub the NotImplementedError surfaces
        # at iterator-call time rather than as a `coroutine was never
        # awaited` warning.
        raise NotImplementedError("listen_audit_outbox lands in SPL-015 (#129)")

    # ----- Transactions -----

    async def with_transaction(
        self, fn: Callable[[TransactionHandle], Awaitable[T]]
    ) -> T:
        pool = await self._pool_ref()
        async with pool.acquire() as conn, conn.transaction():
            handle = PgTransactionHandle(conn)
            bind_handle(handle, self)
            try:
                return await fn(handle)
            finally:
                # Mark the handle dead the moment control leaves the
                # callback. The asyncpg `async with` block then ends
                # the transaction and returns the connection to the
                # pool, after which any retained reference must not be
                # usable via `_check_tx_handle`.
                handle._mark_closed()

    def _check_tx_handle(self, tx: TransactionHandle | None) -> None:
        """Validate a transaction handle for tx-aware methods.

        Extends `custos_spl.middleware.transactions.check_handle`
        (provider-ownership check) with a closed-state check, so
        retained handles raise `InvalidTransactionHandle` instead of
        running on a connection that has been released to the pool.
        """
        check_handle(tx, self)
        if isinstance(tx, PgTransactionHandle) and tx.closed:
            raise InvalidTransactionHandle(
                "transaction handle was closed when `with_transaction` returned"
            )

    # ----- Error classification -----

    @staticmethod
    def _classify(exc: BaseException) -> Exception:
        """Map asyncpg driver errors to SPL-typed errors.

        - `23505` unique-violation → `ImmutableViolation` (append-only
          contract on the keyed entities).
        - `55P03` lock-not-available → `LeaseBusy` (NOWAIT contention).
        - Connection-level failures → `BackendUnavailable` (transient).
        - Anything else is returned unchanged so the caller's
          `raise self._classify(exc) from exc` preserves the cause.
        """
        sqlstate = getattr(exc, "sqlstate", None)
        if sqlstate == "23505":
            return ImmutableViolation(str(exc))
        if sqlstate == "55P03":
            return LeaseBusy(str(exc))
        cls_name = type(exc).__name__
        if "Connection" in cls_name or "Interface" in cls_name:
            return BackendUnavailable(str(exc), cause=exc)
        return exc if isinstance(exc, Exception) else RuntimeError(str(exc))


def make_adapter() -> PgMetadataAdapter:
    """Entry-point factory (synchronous). See definition.make_adapter."""
    return PgMetadataAdapter(lazy=LazyPool(read_dsn_from_env()))


__all__ = ["INTERFACE_NAME", "PgLeaseHandle", "PgMetadataAdapter", "make_adapter"]

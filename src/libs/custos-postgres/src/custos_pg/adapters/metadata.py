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
from collections.abc import Awaitable, Callable, Mapping, Sequence
from collections.abc import Set as AbstractSet
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast
from uuid import uuid4

from asyncpg import InterfaceError, PostgresError, UndefinedTableError, UniqueViolationError
from custos_spl.errors import (
    BackendUnavailable,
    ImmutableViolation,
    InvalidTransactionHandle,
    LeaseBusy,
    LeaseExpired,
    MigrationRequired,
    NotReserved,
    QueryUnsupported,
)
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.bulk_replay import (
    BulkReplayCommand,
    BulkReplayItem,
    bulk_entry_identity,
    bulk_request_hash,
    encode_bulk_selection,
    finish_bulk_item,
    require_bulk_entry,
    require_bulk_owner,
    reserve_bulk_replay,
)
from custos_spl.interfaces.dead_letters import (
    DEAD_LETTER_SCHEMA_REVISION,
    DeadLetterData,
    DeadLetterEntry,
    DeadLetterFilter,
    DispatchAttempt,
    ReplayClaim,
    claim_replay,
    dead_letter_transition_audits,
    decode_dead_letter,
    decode_dead_letter_cursor,
    defer_replay,
    discard_dead_letter,
    encode_dead_letter,
    finish_replay,
    new_dead_letter,
    recover_replay,
    validate_dispatch_identifier,
)
from custos_spl.interfaces.delivery import (
    DELIVERY_SCHEMA_REVISION,
    DeliveryNamespace,
    DeliveryReceipt,
    WorkflowMirror,
    WorkflowStart,
    decode_delivery_request,
    delivery_instant,
    delivery_result,
    encode_delivery_request,
    require_delivery,
    reserve_delivery,
    validate_delivery_key,
    validate_failure_capture,
)
from custos_spl.interfaces.metadata_store import (
    ArtifactUse,
    AuditEvent,
    AuditFilter,
    AuditOutboxBatch,
    AuditOutboxRow,
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
from custos_spl.interfaces.pull_control import (
    MAX_PULL_LEASE_SECONDS,
    PULL_CONTROL_SCHEMA_REVISION,
    ConnectorPullControl,
    PullLease,
    collect_pull_demand,
    effective_pull_interval,
    pull_control_keys,
    pull_deadline,
    pull_interval,
    recover_pull_invocation,
    require_pull_invocation,
    require_pull_lease,
    require_pull_ready,
    validate_pause_audit,
)
from custos_spl.interfaces.scheduler import (
    SCHEDULER_SCHEMA_REVISION,
    PendingScheduleFire,
    SchedulerLease,
    lease_duration,
    prepare_schedule_fire,
    require_scheduler_lease,
    utc_instant,
    validate_schedule_discard,
)
from custos_spl.interfaces.source_delivery import SourceDeliverySelection
from custos_spl.interfaces.trigger_queries import SubscriptionSnapshot, prepare_subscription_write
from custos_spl.middleware.transactions import bind_handle, check_handle
from custos_spl.pagination import (
    Cursor,
    Page,
    decode_query_cursor,
    encode_query_cursor,
    query_page_limit,
)

from custos_pg.migrations.metadata import (
    AUDIT_OUTBOX_LOCK_KEY,
    METADATA_REV1,
    METADATA_REV2,
    METADATA_REV3,
    METADATA_REV4,
    METADATA_REV5,
    METADATA_REV6,
    METADATA_REV7,
    METADATA_REV8,
    METADATA_REV9,
    METADATA_REV10,
    METADATA_REV11,
    METADATA_REV12,
    METADATA_REV13,
)
from custos_pg.pool import LazyPool, read_dsn_from_env
from custos_pg.revisions import ensure_ledger, read_declared, record_revision

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from asyncpg import Connection, Record
    from asyncpg.pool import Pool

INTERFACE_NAME = "MetadataStoreProvider"
DEFAULT_LIST_LIMIT = 50
MAX_LIST_LIMIT = 200

_SUBSCRIPTION_QUERY = """
SELECT s.workspace_id, s.subscription_id, s.workflow_id, s.state,
       s.created_at, s.updated_at, latest.seq AS selector_seq,
       latest.selector, latest.added_at
FROM custos_state.subscription AS s
LEFT JOIN LATERAL (
    SELECT seq, selector, added_at
    FROM custos_state.subscription_selector
    WHERE workspace_id = s.workspace_id AND subscription_id = s.subscription_id
    ORDER BY seq DESC LIMIT 1
) AS latest ON true
"""

T = TypeVar("T")


def _delivery_receipt(row: Record) -> DeliveryReceipt:
    """Decode durable receipt state; pending rows never masquerade as completed results."""
    return DeliveryReceipt(
        WorkspaceId(row["workspace_id"]),
        row["namespace"],
        row["key"],
        row["request_hash"],
        row["retain_until"],
        row["epoch"],
        row["token"],
        row["lease_until"],
        row["result_json"],
        row["request_json"],
        row["submitted_at"],
    )


def _workflow_mirror(row: Record) -> WorkflowMirror:
    """Decode a workspace-bound mirror snapshot without exposing another tenant's rows."""
    return WorkflowMirror(
        WorkspaceId(row["workspace_id"]),
        row["mirror_id"],
        row["run_id"],
        row["step_id"],
        row["identity_hash"],
        row["expires_at"],
        row["payload"],
    )


def _scheduler_lease(row: Record, observed_at: datetime) -> SchedulerLease:
    """Decode authoritative lease state with a fresh provider-time observation."""
    return SchedulerLease(
        row["deployment_id"], row["holder_id"], row["epoch"], row["expires_at"], observed_at
    )


def _row_to_bulk_replay(row: Record) -> BulkReplayCommand:
    """Rehydrate a command snapshot, preserving purged identity tombstones."""
    payload = json.loads(row["payload"]) if row["payload"] is not None else None
    return BulkReplayCommand(
        WorkspaceId(row["workspace_id"]),
        row["command_id"],
        row["request_hash"],
        row["actor"],
        row["expires_at"],
        tuple(
            BulkReplayItem(item["entryId"], item["identity"], item["accepted"])
            for item in payload["items"]
        )
        if payload is not None
        else None,
        payload["nextCursor"] if payload is not None else None,
        row["lease_token"],
        row["lease_until"],
    )


def _row_to_pull_control(row: Record) -> ConnectorPullControl:
    """Validate a versioned database policy before exposing it to polling callers."""
    return ConnectorPullControl(
        workspace_id=WorkspaceId(row["workspace_id"]),
        instance_id=ConnectorInstanceId(row["instance_id"]),
        operator_paused=row["operator_paused"],
        revision=row["revision"],
        next_eligible_at=row["next_eligible_at"],
        updated_at=row["updated_at"],
        updated_by=row["updated_by"],
        effective_interval_seconds=row["effective_interval_seconds"],
        consecutive_failures=row["consecutive_failures"],
        schema_version=row["schema_version"],
        native_interval_seconds=row["native_interval_seconds"],
        trigger_interval_seconds=row["trigger_interval_seconds"],
        invocation_id=row["invocation_id"],
        invocation_owner=row["invocation_owner"],
    )


def _pending_fire(row: Record) -> PendingScheduleFire:
    """Decode a persisted planned occurrence without consulting mutable subscription state."""
    return PendingScheduleFire(
        workspace_id=WorkspaceId(row["workspace_id"]),
        fire_id=row["fire_id"],
        schedule_id=row["schedule_id"],
        workflow_id=row["workflow_id"],
        deployment_id=row["deployment_id"],
        epoch=row["epoch"],
        subscription_revision=row["subscription_revision"],
        planned_fire_at=row["planned_fire_at"],
        created_at=row["created_at"],
        delivered_at=row["delivered_at"],
        skipped_count=row["skipped_count"],
        discarded_at=row["discarded_at"],
        discard_reason=row["discard_reason"],
    )


async def _scheduler_now(conn: Connection) -> datetime:
    """Read wall-clock database time after lock acquisition, not transaction-start time."""
    now = await conn.fetchval("SELECT clock_timestamp()")
    if not isinstance(now, datetime):
        raise BackendUnavailable("scheduler clock returned an invalid timestamp")
    return utc_instant(now)


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


def _row_to_subscription_snapshot(row: Record) -> SubscriptionSnapshot:
    """Rehydrate a subscription/selector pair from one consistent SQL statement."""
    return SubscriptionSnapshot(
        subscription=_row_to_subscription(row),
        selector=_row_to_subscription_selector(row) if row["selector_seq"] is not None else None,
    )


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


def _row_to_audit_event(row: Record) -> AuditEvent:
    return AuditEvent(
        workspace_id=row["workspace_id"],
        event_id=row["event_id"],
        event_type=row["event_type"],
        actor=row["actor"],
        subject=MappingProxyType(dict(_json_payload(row["subject"]))),
        payload=MappingProxyType(dict(_json_payload(row["payload"]))),
        occurred_at=row["occurred_at"],
    )


def _row_to_audit_outbox_row(row: Record) -> AuditOutboxRow:
    return AuditOutboxRow(
        id=row["id"],
        workspace_id=row["workspace_id"],
        event_id=row["event_id"],
        event_type=row["event_type"],
        payload=MappingProxyType(dict(_json_payload(row["payload"]))),
        enqueued_at=row["enqueued_at"],
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
            raise ValueError("PgMetadataAdapter requires either `pool` or `lazy`.")
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

        Revisions 1-4 define the shared minimum. Later revisions add optional
        query, scheduler, delivery and durable Connector polling capabilities.
        """
        summaries: list[str] = []
        pool = await self._pool_ref()
        async with pool.acquire() as conn, conn.transaction():
            await ensure_ledger(conn)
            applied = await conn.fetch(
                "SELECT revision FROM custos_meta.adapter_revisions WHERE interface_name = $1",
                INTERFACE_NAME,
            )
            already = {int(r["revision"]) for r in applied}
            for rev in (
                METADATA_REV1,
                METADATA_REV2,
                METADATA_REV3,
                METADATA_REV4,
                METADATA_REV5,
                METADATA_REV6,
                METADATA_REV7,
                METADATA_REV8,
                METADATA_REV9,
                METADATA_REV10,
                METADATA_REV11,
                METADATA_REV12,
                METADATA_REV13,
            ):
                if rev.number in already:
                    continue
                for stmt in rev.statements:
                    await conn.execute(stmt)
                await record_revision(conn, INTERFACE_NAME, rev.number)
                summaries.append(
                    f"applied {INTERFACE_NAME} rev{rev.number} ({len(rev.statements)} statements)"
                )
        await self.refresh_declared()
        return summaries

    @asynccontextmanager
    async def _delivery_transaction(self) -> AsyncIterator[Connection]:
        """Rollback partial delivery writes and preserve concrete backend error causes."""
        try:
            pool = await self._pool_ref()
            async with pool.acquire() as conn, conn.transaction():
                yield conn
        except (OSError, InterfaceError, PostgresError) as exc:
            raise BackendUnavailable("delivery storage operation failed", cause=exc) from exc

    async def check_delivery_ready(self) -> None:
        """Require the additive receipt/mirror revision for opted-in durable consumers."""
        async with self._delivery_transaction() as conn:
            try:
                exists = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM custos_meta.adapter_revisions "
                    "WHERE interface_name=$1 AND revision=$2)",
                    INTERFACE_NAME,
                    DELIVERY_SCHEMA_REVISION,
                )
            except UndefinedTableError as exc:
                raise MigrationRequired([(INTERFACE_NAME, DELIVERY_SCHEMA_REVISION)]) from exc
            if not exists:
                raise MigrationRequired([(INTERFACE_NAME, DELIVERY_SCHEMA_REVISION)])

    async def delivery_now(self) -> datetime:
        """Return authoritative database time for retention planning."""
        async with self._delivery_transaction() as conn:
            return await _scheduler_now(conn)

    async def put_workflow_start(self, run: Run, start: WorkflowStart) -> WorkflowStart:
        """Commit Run and protocol atomically; never adopt an unversioned legacy run."""
        if run.workspace_id != start.workspace_id or run.run_id != start.run_id:
            raise ValueError("workflow start scope does not match Run")
        async with self._delivery_transaction() as conn:
            inserted = await conn.fetchval(
                "INSERT INTO custos_state.run "
                "(workspace_id,run_id,workflow_id,workflow_version,status,reason,"
                "started_at,updated_at) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING RETURNING run_id",
                run.workspace_id,
                run.run_id,
                run.workflow_id,
                run.workflow_version,
                run.status,
                run.reason,
                run.started_at,
                run.updated_at,
            )
            if inserted is not None:
                await conn.execute(
                    "INSERT INTO custos_state.workflow_delivery_start "
                    "(workspace_id,run_id,request_hash,input_json) VALUES ($1,$2,$3,$4::json)",
                    start.workspace_id,
                    start.run_id,
                    start.request_hash,
                    start.input_json,
                )
                return start
            row = await conn.fetchrow(
                "SELECT * FROM custos_state.workflow_delivery_start "
                "WHERE workspace_id=$1 AND run_id=$2",
                start.workspace_id,
                start.run_id,
            )
            if row is None:
                raise ImmutableViolation(
                    "replay_unsafe: existing run has no versioned start snapshot"
                )
            if row["request_hash"] != start.request_hash:
                raise ImmutableViolation("workflow start input is immutable")
            return WorkflowStart(
                start.workspace_id, start.run_id, row["request_hash"], row["input_json"]
            )

    async def get_workflow_start(
        self, workspace_id: WorkspaceId, run_id: str
    ) -> WorkflowStart | None:
        """Read immutable runtime input separately from the mutable receipt lifecycle."""
        async with self._delivery_transaction() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM custos_state.workflow_delivery_start "
                "WHERE workspace_id=$1 AND run_id=$2",
                workspace_id,
                run_id,
            )
            return (
                WorkflowStart(workspace_id, run_id, row["request_hash"], row["input_json"])
                if row
                else None
            )

    async def _locked_delivery(
        self,
        conn: Connection,
        workspace_id: WorkspaceId,
        namespace: DeliveryNamespace,
        key: str,
    ) -> DeliveryReceipt | None:
        """Serialize receipt transitions before sampling provider time."""
        row = await conn.fetchrow(
            "SELECT * FROM custos_state.delivery_receipt "
            "WHERE workspace_id=$1 AND namespace=$2 AND key=$3 FOR UPDATE",
            workspace_id,
            namespace,
            key,
        )
        return _delivery_receipt(row) if row is not None else None

    async def _write_delivery(
        self,
        conn: Connection,
        receipt: DeliveryReceipt,
        *,
        fenced: bool = False,
    ) -> None:
        """Update a locked receipt, rechecking live producer time at the mutation statement."""
        changed = await conn.fetchval(
            "UPDATE custos_state.delivery_receipt SET retain_until=$4, epoch=$5, token=$6, "
            "lease_until=$7, result_json=$8::json, request_json=$10::json, submitted_at=$11 "
            "WHERE workspace_id=$1 AND namespace=$2 AND key=$3 "
            "AND (NOT $9::bool OR lease_until > clock_timestamp()) RETURNING key",
            receipt.workspace_id,
            receipt.namespace,
            receipt.key,
            receipt.retain_until,
            receipt.epoch,
            receipt.token,
            receipt.lease_until,
            receipt.result_json,
            fenced,
            receipt.request_json,
            receipt.submitted_at,
        )
        if changed is None:
            raise LeaseExpired("delivery claim expired before mutation")

    async def get_delivery(
        self,
        workspace_id: WorkspaceId,
        namespace: DeliveryNamespace,
        key: str,
    ) -> DeliveryReceipt | None:
        """Read a scoped receipt without interpreting pending work as a successful effect."""
        validate_delivery_key(workspace_id, namespace, key)
        async with self._delivery_transaction() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM custos_state.delivery_receipt "
                "WHERE workspace_id=$1 AND namespace=$2 AND key=$3",
                workspace_id,
                namespace,
                key,
            )
            return _delivery_receipt(row) if row is not None else None

    async def reserve_delivery(
        self,
        workspace_id: WorkspaceId,
        namespace: DeliveryNamespace,
        key: str,
        request_hash: str,
        retain_until: datetime,
        *,
        lease_seconds: int = 300,
        request: DeadLetterData | None = None,
    ) -> DeliveryReceipt:
        """Atomically bind input identity and acquire or recover producer ownership."""
        validate_delivery_key(workspace_id, namespace, key)
        retain_until = delivery_instant(retain_until)
        busy: LeaseBusy | None = None
        async with self._delivery_transaction() as conn:
            await conn.execute(
                "INSERT INTO custos_state.delivery_receipt "
                "(workspace_id,namespace,key,request_hash,retain_until,epoch) "
                "VALUES ($1,$2,$3,$4,$5,0) "
                "ON CONFLICT DO NOTHING",
                workspace_id,
                namespace,
                key,
                request_hash,
                retain_until,
            )
            current = await self._locked_delivery(conn, workspace_id, namespace, key)
            try:
                row = reserve_delivery(
                    current,
                    workspace_id,
                    namespace,
                    key,
                    request_hash,
                    retain_until,
                    await _scheduler_now(conn),
                    lease_seconds,
                    encode_delivery_request(request) if request is not None else None,
                )
            except LeaseBusy as exc:
                assert current is not None
                row = replace(current, retain_until=max(current.retain_until, retain_until))
                busy = exc
            await self._write_delivery(conn, row)
        if busy is not None:
            raise busy
        return row

    async def complete_delivery(self, claim: DeliveryReceipt, result_json: str) -> DeliveryReceipt:
        """Finish a current producer or read an identical consumer-committed result."""
        result = delivery_result(result_json)
        async with self._delivery_transaction() as conn:
            current = await self._locked_delivery(
                conn, claim.workspace_id, claim.namespace, claim.key
            )
            if current is not None and current.completed:
                if current.request_hash != claim.request_hash or current.result_json != result:
                    raise ImmutableViolation("delivery result is immutable")
                return current
            require_delivery(current, claim, await _scheduler_now(conn))
            assert current is not None
            completed = replace(
                current, token=None, lease_until=None, result_json=result, request_json=None
            )
            await self._write_delivery(conn, completed, fenced=True)
            return completed

    async def mark_delivery_submitted(self, claim: DeliveryReceipt) -> DeliveryReceipt:
        """Persist submission without prematurely marking the durable consumer complete."""
        if claim.namespace != "workflow-resume":
            raise ValueError("only resume receipts have runtime submission state")
        async with self._delivery_transaction() as conn:
            current = await self._locked_delivery(
                conn, claim.workspace_id, claim.namespace, claim.key
            )
            if (
                current is not None
                and current.completed
                and current.request_hash == claim.request_hash
            ):
                return current
            now = await _scheduler_now(conn)
            require_delivery(current, claim, now)
            assert current is not None
            submitted = replace(
                current,
                token=None,
                lease_until=None,
                submitted_at=current.submitted_at or now,
            )
            await self._write_delivery(conn, submitted, fenced=True)
            return submitted

    async def clear_expired_delivery_requests(
        self,
        workspace_id: WorkspaceId,
        *,
        limit: int | None = None,
    ) -> int:
        """Erase expired unowned payloads without losing accepted source-selection recovery."""
        validate_delivery_key(workspace_id, "trigger", "cleanup")
        size = query_page_limit(limit)
        async with self._delivery_transaction() as conn:
            await self._lock_source_selections(conn, workspace_id)
            has_selections = await conn.fetchval(
                "SELECT to_regclass('custos_state.source_delivery_selection') IS NOT NULL"
            )
            protection = (
                "AND (result_json IS NOT NULL OR NOT EXISTS ("
                "SELECT 1 FROM custos_state.source_delivery_selection s "
                "WHERE s.workspace_id=d.workspace_id AND s.payload IS NOT NULL "
                "AND d.key=ANY(s.delivery_keys))) "
                if has_selections
                else ""
            )
            rows = await conn.fetch(
                "WITH candidates AS (SELECT workspace_id,namespace,key "
                "FROM custos_state.delivery_receipt d "
                "WHERE workspace_id=$1 AND namespace='trigger' "
                "AND request_json IS NOT NULL AND retain_until<=clock_timestamp() "
                "AND (lease_until IS NULL OR lease_until<=clock_timestamp()) "
                + protection
                + 'ORDER BY key COLLATE "C" LIMIT $2 FOR UPDATE SKIP LOCKED) '
                "UPDATE custos_state.delivery_receipt AS receipt SET request_json=NULL "
                "FROM candidates WHERE receipt.workspace_id=candidates.workspace_id "
                "AND receipt.namespace=candidates.namespace AND receipt.key=candidates.key "
                "AND receipt.retain_until<=clock_timestamp() "
                "AND (receipt.lease_until IS NULL OR receipt.lease_until<=clock_timestamp()) "
                "RETURNING receipt.key",
                workspace_id,
                size,
            )
            return len(rows)

    async def release_delivery(self, claim: DeliveryReceipt) -> None:
        """Release only current live authority; never erase identity or a completed result."""
        async with self._delivery_transaction() as conn:
            current = await self._locked_delivery(
                conn, claim.workspace_id, claim.namespace, claim.key
            )
            require_delivery(current, claim, await _scheduler_now(conn))
            assert current is not None
            await self._write_delivery(
                conn, replace(current, token=None, lease_until=None), fenced=True
            )

    async def capture_failed_delivery(
        self,
        claim: DeliveryReceipt,
        data: DeadLetterData,
    ) -> DeadLetterEntry:
        """Commit capture and acknowledgement in one transaction, fencing again after lock waits."""
        validate_failure_capture(claim, data)
        async with self._delivery_transaction() as conn:
            current = await self._locked_delivery(
                conn, claim.workspace_id, claim.namespace, claim.key
            )
            now = await _scheduler_now(conn)
            require_delivery(current, claim, now)
            assert current is not None
            entry = await self._locked_dead_letter(conn, data.workspace_id, data.entry_id)
            if entry is not None:
                if (
                    entry.data.idempotency_key != data.idempotency_key
                    or entry.data.target != data.target
                ):
                    raise ImmutableViolation("failure capture identity is already bound")
            else:
                entry = new_dead_letter(data, await _scheduler_now(conn))
                await conn.execute(
                    "INSERT INTO custos_state.dead_letter "
                    "(workspace_id,entry_id,subscription_id,schema_version,status,"
                    "created_at,expires_at,payload) "
                    "VALUES ($1,$2,$3,1,'pending',$4,$5,$6::json)",
                    data.workspace_id,
                    data.entry_id,
                    data.subscription_id,
                    entry.created_at,
                    entry.expires_at,
                    encode_dead_letter(entry),
                )
                await self._audit_dead_letter_transition(conn, None, entry)
            result = delivery_result(json.dumps({"deadLetterId": data.entry_id}))
            await self._write_delivery(
                conn,
                replace(
                    current, token=None, lease_until=None, result_json=result, request_json=None
                ),
                fenced=True,
            )
            return entry

    async def acknowledge_delivery(
        self,
        workspace_id: WorkspaceId,
        key: str,
        request_hash: str,
        result_json: str,
    ) -> DeliveryReceipt:
        """Let the durable resume consumer close the send/commit crash window."""
        result = delivery_result(result_json)
        async with self._delivery_transaction() as conn:
            current = await self._locked_delivery(conn, workspace_id, "workflow-resume", key)
            if current is None or current.request_hash != request_hash:
                raise ImmutableViolation(
                    "resume acknowledgement does not match an accepted request"
                )
            if current.completed and current.result_json != result:
                raise ImmutableViolation("delivery result is immutable")
            completed = replace(current, token=None, lease_until=None, result_json=result)
            await self._write_delivery(conn, completed)
            return completed

    async def put_workflow_mirror(self, mirror: WorkflowMirror) -> None:
        """Upsert retry metadata without rebinding the original run/step/source association."""
        async with self._delivery_transaction() as conn:
            changed = await conn.fetchval(
                "INSERT INTO custos_state.workflow_resume_mirror "
                "(workspace_id,mirror_id,run_id,step_id,identity_hash,expires_at,payload) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7::json) ON CONFLICT (workspace_id,mirror_id) "
                "DO UPDATE SET expires_at=EXCLUDED.expires_at,payload=EXCLUDED.payload "
                "WHERE workflow_resume_mirror.identity_hash=EXCLUDED.identity_hash "
                "RETURNING mirror_id",
                mirror.workspace_id,
                mirror.mirror_id,
                mirror.run_id,
                mirror.step_id,
                mirror.identity_hash,
                mirror.expires_at,
                mirror.payload_json,
            )
            if changed is None:
                raise ImmutableViolation("workflow mirror association cannot be rebound")

    async def query_workflow_mirrors(
        self,
        workspace_id: WorkspaceId,
        *,
        run_id: str | None = None,
        step_id: str | None = None,
        expired_before: datetime | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[WorkflowMirror]:
        """Page workspace-bound mirrors using indexed run/step/expiry filters."""
        cutoff = delivery_instant(expired_before) if expired_before else None
        scope = (
            "workflow-mirrors",
            workspace_id,
            run_id,
            step_id,
            cutoff.isoformat() if cutoff else None,
        )
        after, size = decode_query_cursor(cursor, scope), query_page_limit(limit)
        async with self._delivery_transaction() as conn:
            rows = await conn.fetch(
                "SELECT * FROM custos_state.workflow_resume_mirror WHERE workspace_id=$1 "
                "AND ($2::text IS NULL OR run_id=$2) AND ($3::text IS NULL OR step_id=$3) "
                "AND ($4::timestamptz IS NULL OR expires_at<=$4) "
                'AND ($5::text IS NULL OR mirror_id COLLATE "C">$5) '
                'ORDER BY mirror_id COLLATE "C" LIMIT $6',
                workspace_id,
                run_id,
                step_id,
                cutoff,
                after,
                size + 1,
            )
            values = [_workflow_mirror(row) for row in rows]
            return Page(
                values[:size],
                encode_query_cursor(scope, values[size - 1].mirror_id)
                if len(values) > size
                else None,
            )

    async def delete_workflow_mirror(self, workspace_id: WorkspaceId, mirror_id: str) -> None:
        """Delete a scoped mirror idempotently after consumer cleanup."""
        async with self._delivery_transaction() as conn:
            await conn.execute(
                "DELETE FROM custos_state.workflow_resume_mirror "
                "WHERE workspace_id=$1 AND mirror_id=$2",
                workspace_id,
                mirror_id,
            )

    @asynccontextmanager
    async def _dead_letter_transaction(self) -> AsyncIterator[Connection]:
        """Rollback partial writes and expose backend failures rather than empty results."""
        try:
            pool = await self._pool_ref()
            async with pool.acquire() as conn, conn.transaction():
                yield conn
        except UniqueViolationError as exc:
            raise ImmutableViolation("dead-letter entry already exists") from exc
        except (OSError, InterfaceError, PostgresError) as exc:
            raise BackendUnavailable("dead-letter storage operation failed", cause=exc) from exc

    async def check_dead_letters_ready(self) -> None:
        """Require only this optional capability's additive metadata revision."""
        async with self._dead_letter_transaction() as conn:
            try:
                exists = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM custos_meta.adapter_revisions "
                    "WHERE interface_name=$1 AND revision=$2)",
                    INTERFACE_NAME,
                    DEAD_LETTER_SCHEMA_REVISION,
                )
            except UndefinedTableError as exc:
                raise MigrationRequired([(INTERFACE_NAME, DEAD_LETTER_SCHEMA_REVISION)]) from exc
            if not exists:
                raise MigrationRequired([(INTERFACE_NAME, DEAD_LETTER_SCHEMA_REVISION)])

    async def put_dead_letter(self, data: DeadLetterData) -> DeadLetterEntry:
        """Append immutable replay inputs using database-time retention."""
        async with self._dead_letter_transaction() as conn:
            entry = new_dead_letter(data, await _scheduler_now(conn))
            await conn.execute(
                "INSERT INTO custos_state.dead_letter "
                "(workspace_id, entry_id, subscription_id, schema_version, status, "
                "created_at, expires_at, payload) VALUES ($1,$2,$3,1,'pending',$4,$5,$6::json)",
                data.workspace_id,
                data.entry_id,
                data.subscription_id,
                entry.created_at,
                entry.expires_at,
                encode_dead_letter(entry),
            )
            await self._audit_dead_letter_transition(conn, None, entry)
            return entry

    async def get_dead_letter(
        self, workspace_id: WorkspaceId, entry_id: str
    ) -> DeadLetterEntry | None:
        """Read only one tenant's immutable capture and replay state."""
        async with self._dead_letter_transaction() as conn:
            payload = await conn.fetchval(
                "SELECT payload::text FROM custos_state.dead_letter "
                "WHERE workspace_id=$1 AND entry_id=$2",
                workspace_id,
                entry_id,
            )
            return decode_dead_letter(payload) if payload is not None else None

    async def query_dead_letters(
        self,
        workspace_id: WorkspaceId,
        filters: DeadLetterFilter | None = None,
        *,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[DeadLetterEntry]:
        """Use scoped identifier keysets with optional indexed status/subscription/time filters."""
        async with self._dead_letter_transaction() as conn:
            return await self._dead_letter_page(
                conn, workspace_id, filters, cursor=cursor, limit=limit
            )

    async def _dead_letter_page(
        self,
        conn: Connection,
        workspace_id: WorkspaceId,
        filters: DeadLetterFilter | None = None,
        *,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[DeadLetterEntry]:
        """Read a bounded selection on the caller's transaction for atomic command creation."""
        filters = filters if filters is not None else DeadLetterFilter()
        scope, size = filters.scope(workspace_id), query_page_limit(limit)
        after = decode_dead_letter_cursor(cursor, scope)
        rows = await conn.fetch(
            "SELECT payload::text FROM custos_state.dead_letter WHERE workspace_id=$1 "
            "AND ($2::text IS NULL OR subscription_id=$2) "
            "AND ($3::text IS NULL OR status=$3) "
            "AND ($4::timestamptz IS NULL OR created_at >= $4) "
            "AND ($5::timestamptz IS NULL OR created_at <= $5) "
            'AND ($6::text IS NULL OR entry_id COLLATE "C" > $6) '
            'ORDER BY entry_id COLLATE "C" LIMIT $7',
            workspace_id,
            filters.subscription_id,
            filters.status,
            filters.since,
            filters.until,
            after,
            size + 1,
        )
        entries = [decode_dead_letter(row["payload"]) for row in rows]
        cursor = (
            encode_query_cursor(scope, entries[size - 1].data.entry_id)
            if len(entries) > size
            else None
        )
        return Page(entries[:size], cursor)

    async def _bulk_replay_row(
        self,
        conn: Connection,
        workspace_id: WorkspaceId,
        command_id: str,
        *,
        locked: bool = True,
    ) -> BulkReplayCommand | None:
        """Read the exact command, serializing progress writers when requested."""
        row = await conn.fetchrow(
            "SELECT workspace_id,command_id,request_hash,actor,expires_at,lease_token,lease_until,"
            "payload::text FROM custos_state.bulk_replay_command "
            "WHERE workspace_id=$1 AND command_id=$2" + (" FOR UPDATE" if locked else ""),
            workspace_id,
            command_id,
        )
        return _row_to_bulk_replay(row) if row is not None else None

    async def _save_bulk_replay(self, conn: Connection, command: BulkReplayCommand) -> None:
        """Write progress without permitting identity, actor or fingerprint rebinding."""
        await conn.execute(
            "INSERT INTO custos_state.bulk_replay_command "
            "(workspace_id,command_id,request_hash,actor,expires_at,"
            "lease_token,lease_until,payload) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb) "
            "ON CONFLICT (workspace_id,command_id) DO UPDATE "
            "SET lease_token=EXCLUDED.lease_token,lease_until=EXCLUDED.lease_until,"
            "payload=EXCLUDED.payload",
            command.workspace_id,
            command.command_id,
            command.request_hash,
            command.actor,
            command.expires_at,
            command.token,
            command.lease_until,
            encode_bulk_selection(command),
        )

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
        """Persist first selection atomically; retries recover its original fenced progress."""
        fingerprint = bulk_request_hash(workspace_id, filters, cursor, limit)
        async with self._dead_letter_transaction() as conn:
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext($1),hashtext($2))",
                workspace_id,
                "bulk-replay:" + command_id,
            )
            current = await self._bulk_replay_row(conn, workspace_id, command_id)
            selection = (
                await self._dead_letter_page(
                    conn, workspace_id, filters, cursor=cursor, limit=limit
                )
                if current is None
                else None
            )
            updated = reserve_bulk_replay(
                current,
                workspace_id,
                command_id,
                actor,
                fingerprint,
                await _scheduler_now(conn),
                retention_seconds,
                selection,
            )
            await self._save_bulk_replay(conn, updated)
            return updated

    async def get_bulk_replay(
        self,
        workspace_id: WorkspaceId,
        command_id: str,
    ) -> BulkReplayCommand | None:
        """Read one workspace's immutable selection and committed results."""
        async with self._dead_letter_transaction() as conn:
            return await self._bulk_replay_row(conn, workspace_id, command_id, locked=False)

    async def _finish_bulk_item(
        self,
        conn: Connection,
        claim: ReplayClaim,
        accepted: bool,
    ) -> None:
        """Commit entry outcome and command progress together, even across coordinator restart."""
        if claim.bulk_id is None:
            return
        command = await self._bulk_replay_row(conn, claim.workspace_id, claim.bulk_id)
        if command is None:
            raise ImmutableViolation("bulk command is missing")
        if command.items is None and command.expires_at <= await _scheduler_now(conn):
            return
        await self._save_bulk_replay(conn, finish_bulk_item(command, claim.entry_id, accepted))

    async def record_bulk_replay_result(
        self,
        claim: BulkReplayCommand,
        entry_id: str,
        accepted: bool,
    ) -> None:
        """Fence non-effect results; an original active command attempt cannot be rejected early."""
        async with self._dead_letter_transaction() as conn:
            entry = await self._locked_dead_letter(conn, claim.workspace_id, entry_id)
            command = await self._bulk_replay_row(conn, claim.workspace_id, claim.command_id)
            if command is None or command.items is None:
                raise ImmutableViolation("bulk selection is unavailable")
            previous = next((item for item in command.items if item.entry_id == entry_id), None)
            if previous is not None and previous.accepted is not None:
                return
            now = await _scheduler_now(conn)
            require_bulk_owner(command, claim.token, now)
            if (
                entry
                and entry.claim
                and entry.claim.bulk_id == command.command_id
                and entry.claim.expires_at > now
            ):
                raise LeaseBusy("the original bulk attempt is still in progress")
            await self._save_bulk_replay(conn, finish_bulk_item(command, entry_id, accepted))

    async def purge_bulk_replay_selections(
        self, workspace_id: WorkspaceId, *, limit: int = 100
    ) -> int:
        """Erase expired selections while retaining non-rebindable command identities."""
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("bulk cleanup limit must be 1..100")
        async with self._dead_letter_transaction() as conn:
            rows = await conn.fetch(
                "SELECT command_id FROM custos_state.bulk_replay_command c "
                "WHERE c.workspace_id=$1 AND c.expires_at<=clock_timestamp() "
                "AND c.payload IS NOT NULL AND NOT EXISTS(SELECT 1 FROM custos_state.dead_letter d "
                "WHERE d.workspace_id=c.workspace_id "
                "AND d.bulk_command_id=c.command_id AND d.claim_expires_at>clock_timestamp()) "
                "ORDER BY c.command_id LIMIT $2 FOR UPDATE OF c SKIP LOCKED",
                workspace_id,
                limit,
            )
            erased = 0
            for row in rows:
                updated = await conn.fetchval(
                    "UPDATE custos_state.bulk_replay_command c "
                    "SET payload=NULL,lease_token=NULL,lease_until=NULL "
                    "WHERE c.workspace_id=$1 AND c.command_id=$2 AND c.payload IS NOT NULL "
                    "AND c.expires_at<=clock_timestamp() "
                    "AND NOT EXISTS(SELECT 1 FROM custos_state.dead_letter d "
                    "WHERE d.workspace_id=c.workspace_id AND d.bulk_command_id=c.command_id "
                    "AND d.claim_expires_at>clock_timestamp()) RETURNING command_id",
                    workspace_id,
                    row["command_id"],
                )
                erased += updated is not None
            return erased

    async def _locked_dead_letter(
        self,
        conn: Connection,
        workspace_id: WorkspaceId,
        entry_id: str,
    ) -> DeadLetterEntry | None:
        """Lock before reading provider time so blocked workers cannot reuse pre-lock authority."""
        payload = await conn.fetchval(
            "SELECT payload::text FROM custos_state.dead_letter "
            "WHERE workspace_id=$1 AND entry_id=$2 FOR UPDATE",
            workspace_id,
            entry_id,
        )
        return decode_dead_letter(payload) if payload is not None else None

    async def _audit_dead_letter_transition(
        self,
        conn: Connection,
        before: DeadLetterEntry | None,
        after: DeadLetterEntry | None,
        *,
        actor: str = "trigger-service",
        reason: str | None = None,
    ) -> None:
        """Append lifecycle audit/outbox entries inside the caller's state transaction."""
        events = dead_letter_transition_audits(
            before, after, await _scheduler_now(conn), actor=actor, reason=reason
        )
        handle = PgTransactionHandle(conn)
        bind_handle(handle, self)
        try:
            for event in events:
                await self.append_audit(event.workspace_id, event, tx=handle)
        finally:
            handle._mark_closed()

    async def _write_dead_letter(
        self,
        conn: Connection,
        entry: DeadLetterEntry,
        *,
        claiming: bool = False,
        finishing: bool = False,
    ) -> None:
        """Keep indexed state and payload atomic, rechecking time at the mutation statement."""
        updated = await conn.fetchval(
            "UPDATE custos_state.dead_letter SET status=$3, payload=$4::json, claim_expires_at=$5, "
            "bulk_command_id=$8 "
            "WHERE workspace_id=$1 AND entry_id=$2 "
            "AND (NOT $6::bool OR (expires_at > clock_timestamp() AND $5 > clock_timestamp())) "
            "AND (NOT $7::bool OR claim_expires_at > clock_timestamp()) RETURNING entry_id",
            entry.data.workspace_id,
            entry.data.entry_id,
            entry.status,
            encode_dead_letter(entry),
            entry.claim.expires_at if entry.claim else None,
            claiming,
            finishing,
            entry.claim.bulk_id if entry.claim else None,
        )
        if updated is None:
            raise LeaseExpired("dead-letter retention or claim expired before mutation")

    @staticmethod
    async def _lock_source_selections(conn: Connection, workspace_id: WorkspaceId) -> None:
        """Serialize source acceptance with receipt cleanup before either reads its snapshot."""
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            json.dumps(["source-selection", workspace_id]),
        )

    async def _source_delivery_row(
        self, conn: Connection, workspace_id: WorkspaceId, key: str
    ) -> SourceDeliverySelection | None:
        """Decode a committed selection without interpreting a missing payload as new intent."""
        row = await conn.fetchrow(
            "SELECT request_hash,expires_at,payload::text "
            "FROM custos_state.source_delivery_selection "
            "WHERE workspace_id=$1 AND key=$2",
            workspace_id,
            key,
        )
        if row is None:
            return None
        return SourceDeliverySelection(
            workspace_id,
            key,
            row["request_hash"],
            row["expires_at"],
            tuple(json.loads(row["payload"])) if row["payload"] is not None else None,
        )

    async def get_source_delivery(
        self, workspace_id: WorkspaceId, key: str
    ) -> SourceDeliverySelection | None:
        """Read the first accepted event targets independently of current registration state."""
        async with self._dead_letter_transaction() as conn:
            return await self._source_delivery_row(conn, workspace_id, key)

    async def put_source_delivery(
        self, selection: SourceDeliverySelection
    ) -> SourceDeliverySelection:
        """Resolve concurrent planners through a unique key before any remote effects."""
        if selection.requests is None:
            raise ValueError("new source selection requires retained requests")
        keys = [
            decode_delivery_request(raw, selection.expires_at).idempotency_key
            for raw in selection.requests
        ]
        async with self._dead_letter_transaction() as conn:
            await self._lock_source_selections(conn, selection.workspace_id)
            current = await self._source_delivery_row(conn, selection.workspace_id, selection.key)
            if current is not None:
                if current.request_hash != selection.request_hash:
                    raise ImmutableViolation("source event identity is bound to another request")
                return current
            missing = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM custos_state.delivery_receipt "
                "WHERE workspace_id=$1 AND namespace='trigger' AND key=ANY($2::text[]) "
                "AND result_json IS NULL AND request_json IS NULL)",
                selection.workspace_id,
                keys,
            )
            if missing:
                raise ImmutableViolation("pending delivery lost its original request")
            await conn.execute(
                "INSERT INTO custos_state.source_delivery_selection "
                "(workspace_id,key,request_hash,expires_at,delivery_keys,payload) "
                "VALUES($1,$2,$3,$4,$5,$6::json) ON CONFLICT(workspace_id,key) DO NOTHING",
                selection.workspace_id,
                selection.key,
                selection.request_hash,
                selection.expires_at,
                keys,
                json.dumps(selection.requests, ensure_ascii=False),
            )
            current = await self._source_delivery_row(conn, selection.workspace_id, selection.key)
            if current is None or current.request_hash != selection.request_hash:
                raise ImmutableViolation("source event identity is bound to another request")
            return current

    async def purge_source_delivery_selections(
        self, workspace_id: WorkspaceId, *, limit: int = 100
    ) -> int:
        """Retain incomplete source plans even past expiry; completed receipt state is monotonic."""
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("source cleanup limit must be 1..100")
        async with self._dead_letter_transaction() as conn:
            rows = await conn.fetch(
                "SELECT s.key FROM custos_state.source_delivery_selection s "
                "WHERE s.workspace_id=$1 AND s.expires_at<=clock_timestamp() "
                "AND s.payload IS NOT NULL AND NOT EXISTS ("
                "SELECT 1 FROM unnest(s.delivery_keys) AS selected(key) "
                "LEFT JOIN custos_state.delivery_receipt d "
                "ON d.workspace_id=s.workspace_id AND d.namespace='trigger' AND d.key=selected.key "
                "WHERE d.result_json IS NULL) "
                "ORDER BY s.key LIMIT $2 FOR UPDATE OF s SKIP LOCKED",
                workspace_id,
                limit,
            )
            for row in rows:
                await conn.execute(
                    "UPDATE custos_state.source_delivery_selection "
                    "SET payload=NULL,delivery_keys='{}' "
                    "WHERE workspace_id=$1 AND key=$2",
                    workspace_id,
                    row["key"],
                )
            return len(rows)

    async def bind_dead_letter_command(
        self, workspace_id: WorkspaceId, command_id: str, request_hash: str, entry_identity: str
    ) -> None:
        """Preserve the first request/generation binding through payload expiry and recreation."""
        for value in (workspace_id, command_id, request_hash, entry_identity):
            validate_dispatch_identifier(value)
        async with self._dead_letter_transaction() as conn:
            await conn.execute(
                "INSERT INTO custos_state.dead_letter_command "
                "(workspace_id,command_id,request_hash,entry_identity) VALUES($1,$2,$3,$4) "
                "ON CONFLICT(workspace_id,command_id) DO NOTHING",
                workspace_id,
                command_id,
                request_hash,
                entry_identity,
            )
            row = await conn.fetchrow(
                "SELECT request_hash,entry_identity FROM custos_state.dead_letter_command "
                "WHERE workspace_id=$1 AND command_id=$2",
                workspace_id,
                command_id,
            )
            if row is None or (row["request_hash"], row["entry_identity"]) != (
                request_hash,
                entry_identity,
            ):
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
        """Consume a bounded attempt under the row lock and mint a fresh, fenced claim token."""
        async with self._dead_letter_transaction() as conn:
            current = await self._locked_dead_letter(conn, workspace_id, entry_id)
            if current is None:
                raise ImmutableViolation("dead-letter entry does not exist")
            if expected_identity is not None and bulk_entry_identity(current) != expected_identity:
                raise ImmutableViolation("dead-letter generation changed before replay claim")
            if bulk_id is not None:
                command = await self._bulk_replay_row(conn, workspace_id, bulk_id)
                require_bulk_owner(command, bulk_token, await _scheduler_now(conn))
                assert command is not None
                require_bulk_entry(command, current, worker_id)
            entry = claim_replay(
                current, worker_id, ttl_seconds, await _scheduler_now(conn), bulk_id=bulk_id
            )
            await self._write_dead_letter(conn, entry, claiming=True)
            await self._audit_dead_letter_transition(conn, current, entry, actor=worker_id)
            return entry

    async def finish_dead_letter(
        self,
        claim: ReplayClaim,
        *,
        failure: DispatchAttempt | None = None,
        result_id: str | None = None,
    ) -> DeadLetterEntry:
        """Finish using authoritative token/epoch/expiry; retention cannot steal a live claim."""
        async with self._dead_letter_transaction() as conn:
            current = await self._locked_dead_letter(conn, claim.workspace_id, claim.entry_id)
            if current is None:
                raise LeaseExpired("replay claim no longer exists")
            entry = finish_replay(
                current, claim, await _scheduler_now(conn), failure=failure, result_id=result_id
            )
            await self._write_dead_letter(conn, entry, finishing=True)
            await self._finish_bulk_item(conn, claim, failure is None)
            await self._audit_dead_letter_transition(
                conn, current, entry, actor=claim.worker_id or current.replay_history[-1].worker_id
            )
            return entry

    async def defer_dead_letter(self, claim: ReplayClaim) -> DeadLetterEntry:
        """Persist acknowledgement uncertainty without completing the entry or its bulk item."""
        async with self._dead_letter_transaction() as conn:
            current = await self._locked_dead_letter(conn, claim.workspace_id, claim.entry_id)
            if current is None:
                raise LeaseExpired("replay claim no longer exists")
            entry = defer_replay(current, claim, await _scheduler_now(conn))
            await self._write_dead_letter(conn, entry, finishing=True)
            await self._audit_dead_letter_transition(
                conn, current, entry, actor=claim.worker_id or current.replay_history[-1].worker_id
            )
            return entry

    async def recover_dead_letter(
        self, workspace_id: WorkspaceId, entry_id: str, *, expected_identity: str | None = None
    ) -> DeadLetterEntry:
        """Recover only the inspected generation, checking its identity under the row lock."""
        async with self._dead_letter_transaction() as conn:
            current = await self._locked_dead_letter(conn, workspace_id, entry_id)
            if current is None:
                raise ImmutableViolation("dead-letter entry does not exist")
            if expected_identity is not None and bulk_entry_identity(current) != expected_identity:
                raise ImmutableViolation("dead-letter generation changed before recovery")
            entry = recover_replay(current, await _scheduler_now(conn))
            if entry != current:
                await self._write_dead_letter(conn, entry)
                if current.claim is not None and entry.status == "discarded":
                    await self._finish_bulk_item(conn, current.claim, False)
                await self._audit_dead_letter_transition(conn, current, entry)
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
        """Serialize operator discard against replay acquisition/completion."""
        async with self._dead_letter_transaction() as conn:
            current = await self._locked_dead_letter(conn, workspace_id, entry_id)
            if current is None:
                raise ImmutableViolation("dead-letter entry does not exist")
            if expected_identity is not None and bulk_entry_identity(current) != expected_identity:
                raise ImmutableViolation("dead-letter generation changed before discard")
            entry = discard_dead_letter(current, await _scheduler_now(conn))
            await self._write_dead_letter(conn, entry)
            if current.claim is not None:
                await self._finish_bulk_item(conn, current.claim, False)
            await self._audit_dead_letter_transition(
                conn, current, entry, actor=actor, reason=reason
            )
            return entry

    async def delete_expired_dead_letter(self, workspace_id: WorkspaceId, entry_id: str) -> bool:
        """Recheck retention and live claims after locking; deletion never races a live owner."""
        async with self._dead_letter_transaction() as conn:
            current = await self._locked_dead_letter(conn, workspace_id, entry_id)
            now = await _scheduler_now(conn)
            if (
                current is None
                or current.expires_at > now
                or (current.claim and current.claim.expires_at > now)
            ):
                return False
            if current.claim is not None:
                await self._finish_bulk_item(conn, current.claim, False)
            await self._audit_dead_letter_transition(conn, current, None)
            await conn.execute(
                "DELETE FROM custos_state.dead_letter WHERE workspace_id=$1 AND entry_id=$2",
                workspace_id,
                entry_id,
            )
            return True

    @asynccontextmanager
    async def _scheduler_transaction(self) -> AsyncIterator[Connection]:
        """Rollback partial scheduler writes and classify concrete backend failures."""
        try:
            pool = await self._pool_ref()
            async with pool.acquire() as conn, conn.transaction():
                yield conn
        except UniqueViolationError as exc:
            raise ImmutableViolation("planned schedule occurrence already exists") from exc
        except (OSError, InterfaceError, PostgresError) as exc:
            raise BackendUnavailable("scheduler storage operation failed", cause=exc) from exc

    async def check_scheduler_ready(self) -> None:
        """Require the additive scheduler revision without raising the shared metadata minimum."""
        async with self._scheduler_transaction() as conn:
            try:
                exists = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM custos_meta.adapter_revisions "
                    "WHERE interface_name = $1 AND revision = $2)",
                    INTERFACE_NAME,
                    SCHEDULER_SCHEMA_REVISION,
                )
            except UndefinedTableError as exc:
                raise MigrationRequired([(INTERFACE_NAME, SCHEDULER_SCHEMA_REVISION)]) from exc
            if not exists:
                raise MigrationRequired([(INTERFACE_NAME, SCHEDULER_SCHEMA_REVISION)])

    async def scheduler_now(self) -> datetime:
        """Return authoritative database time for due-scan planning."""
        async with self._scheduler_transaction() as conn:
            return await _scheduler_now(conn)

    async def acquire_scheduler_lease(
        self, deployment_id: str, holder_id: str, *, ttl_seconds: int
    ) -> SchedulerLease | None:
        """Serialize acquisition and advance the epoch only when ownership is newly acquired."""
        duration = lease_duration(deployment_id, holder_id, ttl_seconds)
        async with self._scheduler_transaction() as conn:
            inserted = await conn.fetchval(
                "INSERT INTO custos_state.scheduler_lease "
                "(deployment_id, holder_id, epoch, expires_at) "
                "VALUES ($1, $2, 1, clock_timestamp()) "
                "ON CONFLICT (deployment_id) DO NOTHING RETURNING deployment_id",
                deployment_id,
                holder_id,
            )
            row = await conn.fetchrow(
                "SELECT * FROM custos_state.scheduler_lease WHERE deployment_id = $1 FOR UPDATE",
                deployment_id,
            )
            assert row is not None
            now = await _scheduler_now(conn)
            current = _scheduler_lease(row, now)
            if inserted is None and current.expires_at > now:
                return current if current.holder_id == holder_id else None
            try:
                expires = now + duration
            except OverflowError as exc:
                raise ValueError("lease expiry exceeds the timestamp range") from exc
            epoch = current.epoch if inserted is not None else current.epoch + 1
            await conn.execute(
                "UPDATE custos_state.scheduler_lease SET holder_id=$2, epoch=$3, expires_at=$4 "
                "WHERE deployment_id=$1",
                deployment_id,
                holder_id,
                epoch,
                expires,
            )
            return SchedulerLease(deployment_id, holder_id, epoch, expires, now)

    async def renew_scheduler_lease(
        self, lease: SchedulerLease, *, ttl_seconds: int
    ) -> SchedulerLease:
        """Renew a live owner using fresh database time while preserving its epoch."""
        duration = lease_duration(lease.deployment_id, lease.holder_id, ttl_seconds)
        async with self._scheduler_transaction() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM custos_state.scheduler_lease WHERE deployment_id=$1 FOR UPDATE",
                lease.deployment_id,
            )
            now = await _scheduler_now(conn)
            current = _scheduler_lease(row, now) if row is not None else None
            require_scheduler_lease(current, lease, now)
            assert current is not None
            try:
                expires = max(current.expires_at, now + duration)
            except OverflowError as exc:
                raise ValueError("lease expiry exceeds the timestamp range") from exc
            updated = await conn.fetchval(
                "UPDATE custos_state.scheduler_lease SET expires_at=$4 "
                "WHERE deployment_id=$1 AND holder_id=$2 AND epoch=$3 "
                "AND expires_at > clock_timestamp() RETURNING epoch",
                lease.deployment_id,
                lease.holder_id,
                lease.epoch,
                expires,
            )
            if updated is None:
                raise LeaseExpired("scheduler lease expired during renewal")
            return SchedulerLease(lease.deployment_id, lease.holder_id, lease.epoch, expires, now)

    async def release_scheduler_lease(self, lease: SchedulerLease) -> None:
        """Retain the epoch tombstone while expiring only the current live ownership."""
        async with self._scheduler_transaction() as conn:
            current = await conn.fetchrow(
                "SELECT * FROM custos_state.scheduler_lease WHERE deployment_id=$1 FOR UPDATE",
                lease.deployment_id,
            )
            now = await _scheduler_now(conn)
            require_scheduler_lease(
                _scheduler_lease(current, now) if current is not None else None, lease, now
            )
            row = await conn.fetchval(
                "UPDATE custos_state.scheduler_lease SET expires_at=clock_timestamp() "
                "WHERE deployment_id=$1 AND holder_id=$2 AND epoch=$3 "
                "AND expires_at > clock_timestamp() RETURNING epoch",
                lease.deployment_id,
                lease.holder_id,
                lease.epoch,
            )
            if row is None:
                raise LeaseExpired("scheduler lease is expired or no longer owned")

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
        """Commit a durable pending fire and next-fire advancement under one live fence."""
        async with self._scheduler_transaction() as conn:
            lease_row = await conn.fetchrow(
                "SELECT * FROM custos_state.scheduler_lease WHERE deployment_id=$1 FOR UPDATE",
                lease.deployment_id,
            )
            await conn.fetchrow(
                "SELECT subscription_id FROM custos_state.subscription "
                "WHERE workspace_id=$1 AND subscription_id=$2 FOR UPDATE",
                workspace_id,
                schedule_id,
            )
            subscription = await conn.fetchrow(
                _SUBSCRIPTION_QUERY + " WHERE s.workspace_id=$1 AND s.subscription_id=$2",
                workspace_id,
                schedule_id,
            )
            schedule = await conn.fetchrow(
                "SELECT * FROM custos_state.schedule "
                "WHERE workspace_id=$1 AND schedule_id=$2 FOR UPDATE",
                workspace_id,
                schedule_id,
            )
            now = await _scheduler_now(conn)
            pending = prepare_schedule_fire(
                workspace_id,
                lease,
                _scheduler_lease(lease_row, now) if lease_row is not None else None,
                _row_to_subscription_snapshot(subscription) if subscription is not None else None,
                _row_to_schedule(schedule) if schedule is not None else None,
                expected_next_fire_at=expected_next_fire_at,
                expected_revision=expected_revision,
                planned_fire_at=planned_fire_at,
                next_fire_at=next_fire_at,
                now=now,
                skipped_count=skipped_count,
            )
            await conn.execute(
                "INSERT INTO custos_state.pending_schedule_fire "
                "(workspace_id, fire_id, schedule_id, workflow_id, deployment_id, epoch, "
                "subscription_revision, planned_fire_at, created_at, skipped_count) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
                workspace_id,
                pending.fire_id,
                schedule_id,
                pending.workflow_id,
                lease.deployment_id,
                lease.epoch,
                expected_revision,
                pending.planned_fire_at,
                now,
                skipped_count,
            )
            updated = await conn.fetchval(
                "UPDATE custos_state.schedule SET next_fire_at=$3 "
                "WHERE workspace_id=$1 AND schedule_id=$2 "
                "AND $3 > clock_timestamp() AND $7 <= clock_timestamp() "
                "AND EXISTS(SELECT 1 FROM custos_state.scheduler_lease "
                "WHERE deployment_id=$4 AND holder_id=$5 AND epoch=$6 "
                "AND expires_at > clock_timestamp()) RETURNING schedule_id",
                workspace_id,
                schedule_id,
                utc_instant(next_fire_at),
                lease.deployment_id,
                lease.holder_id,
                lease.epoch,
                pending.planned_fire_at,
            )
            if updated is None:
                require_scheduler_lease(
                    _scheduler_lease(lease_row, now) if lease_row is not None else None,
                    lease,
                    await _scheduler_now(conn),
                )
                raise ImmutableViolation("provider time moved outside the planned-fire window")
            return pending

    async def get_schedule_fire(
        self,
        workspace_id: WorkspaceId,
        fire_id: str,
    ) -> PendingScheduleFire | None:
        """Read scoped occurrence proof without filtering its settlement state."""
        async with self._scheduler_transaction() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM custos_state.pending_schedule_fire "
                "WHERE workspace_id=$1 AND fire_id=$2",
                workspace_id,
                fire_id,
            )
            return _pending_fire(row) if row is not None else None

    async def query_pending_schedule_fires(
        self,
        workspace_id: WorkspaceId,
        deployment_id: str,
        *,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[PendingScheduleFire]:
        """Read a bounded, tenant/deployment-bound recovery page without an N+1 lookup."""
        scope = ("pending-schedule-fires", str(workspace_id), deployment_id)
        after = decode_query_cursor(cursor, scope)
        size = query_page_limit(limit)
        async with self._scheduler_transaction() as conn:
            rows = await conn.fetch(
                "SELECT * FROM custos_state.pending_schedule_fire "
                "WHERE workspace_id=$1 AND deployment_id=$2 AND delivered_at IS NULL "
                "AND discarded_at IS NULL "
                'AND ($3::text IS NULL OR fire_id COLLATE "C" > $3) '
                'ORDER BY fire_id COLLATE "C" LIMIT $4',
                workspace_id,
                deployment_id,
                after,
                size + 1,
            )
        cursor_out = (
            encode_query_cursor(scope, rows[size - 1]["fire_id"]) if len(rows) > size else None
        )
        return Page((_pending_fire(row) for row in rows[:size]), cursor_out)

    async def acknowledge_schedule_fire(
        self, workspace_id: WorkspaceId, fire_id: str, lease: SchedulerLease
    ) -> None:
        """A current holder may acknowledge an older epoch's recovered publication."""
        async with self._scheduler_transaction() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM custos_state.scheduler_lease WHERE deployment_id=$1 FOR UPDATE",
                lease.deployment_id,
            )
            await conn.fetchrow(
                "SELECT fire_id FROM custos_state.pending_schedule_fire "
                "WHERE workspace_id=$1 AND fire_id=$2 AND deployment_id=$3 FOR UPDATE",
                workspace_id,
                fire_id,
                lease.deployment_id,
            )
            now = await _scheduler_now(conn)
            require_scheduler_lease(_scheduler_lease(row, now) if row else None, lease, now)
            updated = await conn.fetchval(
                "UPDATE custos_state.pending_schedule_fire "
                "SET delivered_at=COALESCE(delivered_at, clock_timestamp()) "
                "WHERE workspace_id=$1 AND fire_id=$2 AND deployment_id=$3 "
                "AND discarded_at IS NULL "
                "AND EXISTS(SELECT 1 FROM custos_state.scheduler_lease "
                "WHERE deployment_id=$3 AND holder_id=$4 AND epoch=$5 "
                "AND expires_at > clock_timestamp()) RETURNING fire_id",
                workspace_id,
                fire_id,
                lease.deployment_id,
                lease.holder_id,
                lease.epoch,
            )
            if updated is None:
                require_scheduler_lease(
                    _scheduler_lease(row, now) if row else None, lease, await _scheduler_now(conn)
                )
                raise ImmutableViolation("pending fire is absent from this workspace/deployment")

    async def discard_schedule_fire(
        self,
        workspace_id: WorkspaceId,
        fire_id: str,
        lease: SchedulerLease,
        *,
        reason: str,
    ) -> None:
        """Settle an ineligible occurrence without misreporting publication or stealing a lease."""
        validate_schedule_discard(reason)
        async with self._scheduler_transaction() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM custos_state.scheduler_lease WHERE deployment_id=$1 FOR UPDATE",
                lease.deployment_id,
            )
            await conn.fetchrow(
                "SELECT fire_id FROM custos_state.pending_schedule_fire "
                "WHERE workspace_id=$1 AND fire_id=$2 AND deployment_id=$3 FOR UPDATE",
                workspace_id,
                fire_id,
                lease.deployment_id,
            )
            now = await _scheduler_now(conn)
            require_scheduler_lease(_scheduler_lease(row, now) if row else None, lease, now)
            updated = await conn.fetchval(
                "UPDATE custos_state.pending_schedule_fire "
                "SET discarded_at=COALESCE(discarded_at,clock_timestamp()), "
                "discard_reason=COALESCE(discard_reason,$6) "
                "WHERE workspace_id=$1 AND fire_id=$2 AND deployment_id=$3 "
                "AND delivered_at IS NULL AND EXISTS ("
                "SELECT 1 FROM custos_state.scheduler_lease WHERE deployment_id=$3 "
                "AND holder_id=$4 AND epoch=$5 AND expires_at>clock_timestamp()) RETURNING fire_id",
                workspace_id,
                fire_id,
                lease.deployment_id,
                lease.holder_id,
                lease.epoch,
                reason,
            )
            if updated is None:
                require_scheduler_lease(
                    _scheduler_lease(row, now) if row else None,
                    lease,
                    await _scheduler_now(conn),
                )
                raise ImmutableViolation("pending fire is absent or already published")

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
            where.append(f"(started_at, run_id) < (${len(params) - 1}, ${len(params)})")
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

    async def get_subscription(
        self, workspace_id: str, subscription_id: str
    ) -> SubscriptionSnapshot | None:
        """Read a workspace-scoped subscription with its latest appended selector."""
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                _SUBSCRIPTION_QUERY + " WHERE s.workspace_id = $1 AND s.subscription_id = $2",
                workspace_id,
                subscription_id,
            )
        return _row_to_subscription_snapshot(row) if row is not None else None

    async def query_subscriptions(
        self,
        workspace_id: str,
        *,
        state: str | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[SubscriptionSnapshot]:
        """Page subscriptions and latest selectors atomically in identifier order."""
        size = query_page_limit(limit)
        scope = ("subscriptions", workspace_id, state)
        after = decode_query_cursor(cursor, scope)
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                _SUBSCRIPTION_QUERY
                + """
                WHERE s.workspace_id = $1 AND ($2::text IS NULL OR s.state = $2)
                  AND ($3::text IS NULL OR s.subscription_id COLLATE "C" > $3)
                ORDER BY s.subscription_id COLLATE "C" LIMIT $4
                """,
                workspace_id,
                state,
                after,
                size + 1,
            )
        next_cursor = (
            encode_query_cursor(scope, rows[size - 1]["subscription_id"])
            if len(rows) > size
            else None
        )
        return Page((_row_to_subscription_snapshot(row) for row in rows[:size]), next_cursor)

    async def get_resume_subscription(
        self, workspace_id: str, resume_id: str
    ) -> ResumeSubscription | None:
        """Read a resume row, retaining expired records for idempotent re-registration."""
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT workspace_id, resume_id, run_id, step_id, expires_at, payload "
                "FROM custos_state.resume_subscription WHERE workspace_id = $1 AND resume_id = $2",
                workspace_id,
                resume_id,
            )
        return _row_to_resume(row) if row is not None else None

    async def register_resume_subscription(
        self, workspace_id: str, resume: ResumeSubscription, *, as_of: datetime
    ) -> ResumeSubscription:
        """Atomically register a new/expired wait or return the unchanged live winner."""
        if as_of.utcoffset() is None:
            raise ValueError("resume registration cutoff must be timezone-aware")
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO custos_state.resume_subscription AS stored
                    (workspace_id, resume_id, run_id, step_id, expires_at, payload)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb)
                ON CONFLICT (workspace_id, resume_id) DO UPDATE SET
                    run_id = CASE WHEN stored.expires_at <= $7 AND
                        NULLIF(stored.payload->'source', 'null'::jsonb) IS NOT DISTINCT FROM
                        NULLIF(EXCLUDED.payload->'source', 'null'::jsonb)
                        AND COALESCE(stored.payload->>'deliveryProtocol','1') =
                            COALESCE(EXCLUDED.payload->>'deliveryProtocol','1')
                        THEN EXCLUDED.run_id ELSE stored.run_id END,
                    step_id = CASE WHEN stored.expires_at <= $7 AND
                        NULLIF(stored.payload->'source', 'null'::jsonb) IS NOT DISTINCT FROM
                        NULLIF(EXCLUDED.payload->'source', 'null'::jsonb)
                        AND COALESCE(stored.payload->>'deliveryProtocol','1') =
                            COALESCE(EXCLUDED.payload->>'deliveryProtocol','1')
                        THEN EXCLUDED.step_id ELSE stored.step_id END,
                    expires_at = CASE WHEN stored.expires_at <= $7 AND
                        NULLIF(stored.payload->'source', 'null'::jsonb) IS NOT DISTINCT FROM
                        NULLIF(EXCLUDED.payload->'source', 'null'::jsonb)
                        AND COALESCE(stored.payload->>'deliveryProtocol','1') =
                            COALESCE(EXCLUDED.payload->>'deliveryProtocol','1')
                        THEN EXCLUDED.expires_at ELSE stored.expires_at END,
                    payload = CASE WHEN stored.expires_at <= $7 AND
                        NULLIF(stored.payload->'source', 'null'::jsonb) IS NOT DISTINCT FROM
                        NULLIF(EXCLUDED.payload->'source', 'null'::jsonb)
                        AND COALESCE(stored.payload->>'deliveryProtocol','1') =
                            COALESCE(EXCLUDED.payload->>'deliveryProtocol','1')
                        THEN EXCLUDED.payload ELSE stored.payload END
                RETURNING workspace_id, resume_id, run_id, step_id, expires_at, payload
                """,
                workspace_id,
                resume.resume_id,
                resume.run_id,
                resume.step_id,
                resume.expires_at,
                json.dumps(dict(resume.payload)),
                as_of,
            )
        assert row is not None
        return _row_to_resume(row)

    async def query_resume_subscriptions(
        self,
        workspace_id: str,
        *,
        connector_instance_id: str,
        delivery_mode: Literal["push", "pull"],
        as_of: datetime,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ResumeSubscription]:
        """Page unexpired waits for the exact workspace/source and delivery mode."""
        size = query_page_limit(limit)
        if as_of.utcoffset() is None:
            raise ValueError("resume query cutoff must be timezone-aware")
        if not connector_instance_id or delivery_mode not in ("push", "pull"):
            raise ValueError("resume queries require a connector instance and push/pull mode")
        scope = ("resume", workspace_id, connector_instance_id, delivery_mode, as_of.isoformat())
        after = decode_query_cursor(cursor, scope)
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT workspace_id, resume_id, run_id, step_id, expires_at, payload
                FROM custos_state.resume_subscription
                WHERE workspace_id = $1
                  AND jsonb_typeof(payload #> '{source,connectorInstanceId}') = 'string'
                  AND payload #>> '{source,connectorInstanceId}' = $2
                  AND jsonb_typeof(payload #> '{source,deliveryModes}') = 'array'
                  AND (payload #> '{source,deliveryModes}') ? $3
                  AND expires_at > $4
                  AND ($5::text IS NULL OR resume_id COLLATE "C" > $5)
                ORDER BY resume_id COLLATE "C" LIMIT $6
                """,
                workspace_id,
                connector_instance_id,
                delivery_mode,
                as_of,
                after,
                size + 1,
            )
        next_cursor = (
            encode_query_cursor(scope, rows[size - 1]["resume_id"]) if len(rows) > size else None
        )
        return Page((_row_to_resume(row) for row in rows[:size]), next_cursor)

    async def get_schedule(self, workspace_id: str, schedule_id: str) -> Schedule | None:
        """Read a schedule by its exact workspace and identifier."""
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT workspace_id, schedule_id, workflow_id, cron, next_fire_at, enabled "
                "FROM custos_state.schedule WHERE workspace_id = $1 AND schedule_id = $2",
                workspace_id,
                schedule_id,
            )
        return _row_to_schedule(row) if row is not None else None

    async def query_due_schedules(
        self,
        workspace_id: str,
        *,
        due_at: datetime,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[Schedule]:
        """Page enabled schedules due at or before the fixed cutoff, by identifier."""
        size = query_page_limit(limit)
        if due_at.utcoffset() is None:
            raise ValueError("schedule query cutoff must be timezone-aware")
        scope = ("schedules", workspace_id, due_at.isoformat())
        after = decode_query_cursor(cursor, scope)
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT workspace_id, schedule_id, workflow_id, cron, next_fire_at, enabled
                FROM custos_state.schedule
                WHERE workspace_id = $1 AND enabled AND next_fire_at <= $2
                  AND ($3::text IS NULL OR schedule_id COLLATE "C" > $3)
                ORDER BY schedule_id COLLATE "C" LIMIT $4
                """,
                workspace_id,
                due_at,
                after,
                size + 1,
            )
        next_cursor = (
            encode_query_cursor(scope, rows[size - 1]["schedule_id"]) if len(rows) > size else None
        )
        return Page((_row_to_schedule(row) for row in rows[:size]), next_cursor)

    async def write_subscription(
        self,
        workspace_id: str,
        subscription: Subscription,
        selector: SubscriptionSelector,
        *,
        expected_revision: int | None,
        schedule: Schedule | None,
        reset_schedule: bool,
        preserve_state: bool = False,
    ) -> SubscriptionSnapshot:
        """Commit base state, selector revision and schedule eligibility in one transaction."""
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn, conn.transaction():
                locked = await conn.fetchrow(
                    "SELECT subscription_id FROM custos_state.subscription "
                    "WHERE workspace_id = $1 AND subscription_id = $2 FOR UPDATE",
                    workspace_id,
                    subscription.subscription_id,
                )
                # Read selectors with a fresh READ COMMITTED snapshot after acquiring
                # the lock: a contender may have appended a revision while we waited.
                current_row = (
                    await conn.fetchrow(
                        _SUBSCRIPTION_QUERY
                        + " WHERE s.workspace_id = $1 AND s.subscription_id = $2",
                        workspace_id,
                        subscription.subscription_id,
                    )
                    if locked is not None
                    else None
                )
                schedule_row = await conn.fetchrow(
                    "SELECT workspace_id, schedule_id, workflow_id, cron, next_fire_at, enabled "
                    "FROM custos_state.schedule WHERE workspace_id = $1 AND schedule_id = $2 "
                    "FOR UPDATE",
                    workspace_id,
                    subscription.subscription_id,
                )
                snapshot, next_schedule = prepare_subscription_write(
                    workspace_id,
                    subscription,
                    selector,
                    _row_to_subscription_snapshot(current_row) if current_row is not None else None,
                    _row_to_schedule(schedule_row) if schedule_row is not None else None,
                    expected_revision=expected_revision,
                    schedule=schedule,
                    reset_schedule=reset_schedule,
                    preserve_state=preserve_state,
                )
                if current_row is None:
                    await conn.execute(
                        "INSERT INTO custos_state.subscription "
                        "(workspace_id, subscription_id, workflow_id, state, "
                        "created_at, updated_at) "
                        "VALUES ($1, $2, $3, $4, $5, $6)",
                        workspace_id,
                        subscription.subscription_id,
                        subscription.workflow_id,
                        snapshot.subscription.state,
                        subscription.created_at,
                        subscription.updated_at,
                    )
                else:
                    await conn.execute(
                        "UPDATE custos_state.subscription SET state = $3, updated_at = $4 "
                        "WHERE workspace_id = $1 AND subscription_id = $2",
                        workspace_id,
                        subscription.subscription_id,
                        snapshot.subscription.state,
                        subscription.updated_at,
                    )
                assert snapshot.selector is not None
                await conn.execute(
                    "INSERT INTO custos_state.subscription_selector "
                    "(workspace_id, subscription_id, selector, added_at) "
                    "VALUES ($1, $2, $3::jsonb, $4)",
                    workspace_id,
                    subscription.subscription_id,
                    json.dumps(dict(snapshot.selector.selector)),
                    snapshot.selector.added_at,
                )
                if next_schedule is not None:
                    statement = (
                        "INSERT INTO custos_state.schedule "
                        "(workspace_id, schedule_id, workflow_id, cron, next_fire_at, enabled) "
                        "VALUES ($1, $2, $3, $4, $5, $6)"
                        if schedule_row is None
                        else "UPDATE custos_state.schedule "
                        "SET cron = $4, next_fire_at = $5, enabled = $6 "
                        "WHERE workspace_id = $1 AND schedule_id = $2 AND workflow_id = $3"
                    )
                    await conn.execute(
                        statement,
                        workspace_id,
                        next_schedule.schedule_id,
                        next_schedule.workflow_id,
                        next_schedule.cron,
                        next_schedule.next_fire_at,
                        next_schedule.enabled,
                    )
                return snapshot
        except UniqueViolationError as exc:
            raise ImmutableViolation("subscription or schedule was created concurrently") from exc

    async def put_subscription(self, workspace_id: str, subscription: Subscription) -> Subscription:
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
            raise ValueError(f"unknown subscription: {workspace_id!r}/{subscription_id!r}")
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

    async def delete_resume_subscription(self, workspace_id: str, resume_id: str) -> None:
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

    # ----- Durable Connector polling -----

    @asynccontextmanager
    async def _pull_transaction(self) -> AsyncIterator[Connection]:
        """Rollback partial policy/cursor/audit changes and classify backend failures."""
        try:
            pool = await self._pool_ref()
            async with pool.acquire() as conn, conn.transaction():
                yield conn
        except UndefinedTableError as exc:
            raise MigrationRequired([(INTERFACE_NAME, PULL_CONTROL_SCHEMA_REVISION)]) from exc
        except UniqueViolationError as exc:
            raise ImmutableViolation("pull-control identity already exists") from exc
        except (OSError, InterfaceError, PostgresError) as exc:
            raise BackendUnavailable("pull-control storage operation failed", cause=exc) from exc

    async def check_pull_control_ready(self) -> None:
        """Require the invocation-fence revision without raising the shared minimum."""
        async with self._pull_transaction() as conn:
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM custos_meta.adapter_revisions "
                "WHERE interface_name=$1 AND revision=$2)",
                INTERFACE_NAME,
                PULL_CONTROL_SCHEMA_REVISION,
            )
            if not exists:
                raise MigrationRequired([(INTERFACE_NAME, PULL_CONTROL_SCHEMA_REVISION)])

    async def pull_now(self) -> datetime:
        """Return database time for shared scheduler eligibility."""
        async with self._pull_transaction() as conn:
            return await _scheduler_now(conn)

    async def get_pull_control(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
    ) -> ConnectorPullControl | None:
        """Read exact scoped intent; missing legacy policy is not permission to run."""
        validate_dispatch_identifier(workspace_id)
        validate_dispatch_identifier(instance_id)
        async with self._pull_transaction() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM custos_state.connector_pull_control "
                "WHERE workspace_id=$1 AND instance_id=$2",
                workspace_id,
                instance_id,
            )
            return _row_to_pull_control(row) if row is not None else None

    async def _locked_pull_control(
        self,
        conn: Connection,
        workspace_id: str,
        instance_id: str,
    ) -> ConnectorPullControl | None:
        """Serialize policy changes before acquiring the associated cursor row lock."""
        row = await conn.fetchrow(
            "SELECT * FROM custos_state.connector_pull_control "
            "WHERE workspace_id=$1 AND instance_id=$2 FOR UPDATE",
            workspace_id,
            instance_id,
        )
        return _row_to_pull_control(row) if row is not None else None

    async def read_pull_controls(
        self,
        workspace_id: WorkspaceId,
        instance_ids: Sequence[ConnectorInstanceId],
    ) -> tuple[ConnectorPullControl, ...]:
        """Read a bounded batch in one transaction, including paused and no-demand rows."""
        identifiers = pull_control_keys(workspace_id, instance_ids)
        async with self._pull_transaction() as conn:
            rows = await conn.fetch(
                "SELECT * FROM custos_state.connector_pull_control "
                "WHERE workspace_id=$1 AND instance_id=ANY($2::text[]) "
                'ORDER BY instance_id COLLATE "C"',
                workspace_id,
                list(identifiers),
            )
        return tuple(_row_to_pull_control(row) for row in rows)

    async def _ensure_pull_control(
        self,
        conn: Connection,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        *,
        actor: str,
        paused: bool,
    ) -> ConnectorPullControl:
        """Initialize only absence; a racing operator's existing policy always survives."""
        for value in (workspace_id, instance_id, actor):
            validate_dispatch_identifier(value)
        now = await _scheduler_now(conn)
        await conn.execute(
            "INSERT INTO custos_state.connector_pull_control "
            "(workspace_id,instance_id,operator_paused,revision,"
            "next_eligible_at,updated_at,updated_by) "
            "VALUES ($1,$2,$3,1,$4,$4,$5) ON CONFLICT DO NOTHING",
            workspace_id,
            instance_id,
            paused,
            now,
            actor,
        )
        current = await self._locked_pull_control(conn, workspace_id, instance_id)
        if current is None:
            raise ImmutableViolation("pull control disappeared during initialization")
        return current

    async def _save_pull_control(self, conn: Connection, control: ConnectorPullControl) -> None:
        """Write a previously locked, validated policy without crossing workspace scope."""
        revision = await conn.fetchval(
            "UPDATE custos_state.connector_pull_control SET operator_paused=$3, revision=$4, "
            "next_eligible_at=$5, updated_at=$6, updated_by=$7, effective_interval_seconds=$8, "
            "consecutive_failures=$9,native_interval_seconds=$10,trigger_interval_seconds=$11, "
            "invocation_id=$12,invocation_owner=$13 "
            "WHERE workspace_id=$1 AND instance_id=$2 RETURNING revision",
            control.workspace_id,
            control.instance_id,
            control.operator_paused,
            control.revision,
            control.next_eligible_at,
            control.updated_at,
            control.updated_by,
            control.effective_interval_seconds,
            control.consecutive_failures,
            control.native_interval_seconds,
            control.trigger_interval_seconds,
            control.invocation_id,
            control.invocation_owner,
        )
        if revision is None:
            raise ImmutableViolation("locked pull control disappeared")

    async def initialize_pull_control(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        *,
        actor: str,
    ) -> ConnectorPullControl:
        """Initialize a server-generated new identity without clearing existing policy."""
        async with self._pull_transaction() as conn:
            return await self._ensure_pull_control(
                conn, workspace_id, instance_id, actor=actor, paused=False
            )

    async def set_pull_paused(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        *,
        paused: bool,
        audit: AuditEvent,
    ) -> ConnectorPullControl:
        """Commit operator intent and its existing audit/outbox contract in one transaction."""
        validate_pause_audit(workspace_id, instance_id, paused, audit)
        async with self._pull_transaction() as conn:
            current = await self._ensure_pull_control(
                conn, workspace_id, instance_id, actor=audit.actor, paused=True
            )
            updated = replace(
                current,
                operator_paused=paused,
                revision=current.revision + 1,
                updated_at=await _scheduler_now(conn),
                updated_by=audit.actor,
                consecutive_failures=current.consecutive_failures if paused else 0,
            )
            handle = PgTransactionHandle(conn)
            bind_handle(handle, self)
            try:
                await self.append_audit(workspace_id, audit, tx=handle)
            finally:
                handle._mark_closed()
            await self._save_pull_control(conn, updated)
            return updated

    async def reconcile_pull_demand(
        self,
        workspace_id: WorkspaceId,
        *,
        default_interval_seconds: int,
        minimum_interval_seconds: int,
    ) -> None:
        """Read one current intent snapshot after serialization, never a caller's stale demand."""
        validate_dispatch_identifier(workspace_id)
        pull_interval(default_interval_seconds)
        pull_interval(minimum_interval_seconds)
        async with self._pull_transaction() as conn:
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext('custos.pull-demand'), hashtext($1))",
                workspace_id,
            )
            now = await _scheduler_now(conn)
            rows = await conn.fetch(
                "SELECT 'start' AS kind, latest.selector->'sourceConfig' AS source "
                "FROM custos_state.subscription s JOIN LATERAL "
                "(SELECT selector FROM custos_state.subscription_selector ss "
                "WHERE ss.workspace_id=s.workspace_id AND ss.subscription_id=s.subscription_id "
                "ORDER BY seq DESC LIMIT 1) latest ON TRUE "
                "WHERE s.workspace_id=$1 AND s.state='active' "
                "AND latest.selector->>'sourceType'='pull' "
                "UNION ALL SELECT 'resume', payload->'source' "
                "FROM custos_state.resume_subscription WHERE workspace_id=$1 AND expires_at>$2 "
                "AND (payload->'source'->'deliveryModes') ? 'pull'",
                workspace_id,
                now,
            )
            demand = collect_pull_demand(
                (_json_payload(row["source"]) for row in rows if row["kind"] == "start"),
                (_json_payload(row["source"]) for row in rows if row["kind"] == "resume"),
                default_interval_seconds=default_interval_seconds,
                minimum_interval_seconds=minimum_interval_seconds,
            )
            controls = await conn.fetch(
                "SELECT * FROM custos_state.connector_pull_control WHERE workspace_id=$1 "
                'ORDER BY instance_id COLLATE "C" FOR UPDATE',
                workspace_id,
            )
            current = {row["instance_id"]: _row_to_pull_control(row) for row in controls}
            for identifier in sorted(set(current) | set(demand)):
                control = current.get(identifier)
                if control is None:
                    control = await self._ensure_pull_control(
                        conn,
                        workspace_id,
                        ConnectorInstanceId(identifier),
                        actor="trigger-service",
                        paused=True,
                    )
                interval = demand.get(identifier)
                native = (
                    max(control.native_interval_seconds, minimum_interval_seconds)
                    if control.native_interval_seconds is not None
                    else None
                )
                if (
                    control.trigger_interval_seconds != interval
                    or control.native_interval_seconds != native
                ):
                    await self._save_pull_control(
                        conn,
                        replace(
                            control,
                            trigger_interval_seconds=interval,
                            native_interval_seconds=native,
                            effective_interval_seconds=effective_pull_interval(native, interval),
                            revision=control.revision + 1,
                            updated_at=now,
                            updated_by="trigger-service",
                        ),
                    )

    async def set_pull_native_demand(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        *,
        interval_seconds: int | None,
        actor: str,
    ) -> ConnectorPullControl:
        """Persist native listening independently of Trigger demand and operator pause."""
        if interval_seconds is not None:
            pull_interval(interval_seconds)
        async with self._pull_transaction() as conn:
            current = await self._ensure_pull_control(
                conn, workspace_id, instance_id, actor=actor, paused=True
            )
            updated = replace(
                current,
                native_interval_seconds=interval_seconds,
                effective_interval_seconds=effective_pull_interval(
                    interval_seconds, current.trigger_interval_seconds
                ),
                revision=current.revision + 1,
                updated_at=await _scheduler_now(conn),
                updated_by=actor,
            )
            await self._save_pull_control(conn, updated)
            return updated

    async def query_pull_controls(
        self,
        workspace_id: WorkspaceId,
        *,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ConnectorPullControl]:
        """Page active demand using scoped C-collation keysets, including paused instances."""
        validate_dispatch_identifier(workspace_id)
        size = query_page_limit(limit)
        scope = ("pull-controls", workspace_id)
        after = decode_query_cursor(cursor, scope)
        async with self._pull_transaction() as conn:
            rows = await conn.fetch(
                "SELECT * FROM custos_state.connector_pull_control WHERE workspace_id=$1 "
                "AND effective_interval_seconds IS NOT NULL "
                'AND ($2::text IS NULL OR instance_id COLLATE "C">$2) '
                'ORDER BY instance_id COLLATE "C" LIMIT $3',
                workspace_id,
                after,
                size + 1,
            )
        selected = [_row_to_pull_control(row) for row in rows[:size]]
        return Page(
            selected,
            encode_query_cursor(scope, selected[-1].instance_id) if len(rows) > size else None,
        )

    async def acquire_pull_tick(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        holder_id: str,
        *,
        ttl_seconds: int,
    ) -> tuple[ConnectorCursor, PullLease]:
        """Reserve shared due time and unique cursor ownership under database-time row locks."""
        for value in (workspace_id, instance_id, holder_id):
            validate_dispatch_identifier(value)
        if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= MAX_PULL_LEASE_SECONDS:
            raise ValueError("pull lease duration must be from 1 to 300 seconds")
        async with self._pull_transaction() as conn:
            control = await self._locked_pull_control(conn, workspace_id, instance_id)
            require_pull_ready(control, await _scheduler_now(conn))
            assert control is not None
            await conn.execute(
                "INSERT INTO custos_state.connector_cursor "
                "(workspace_id,instance_id,value,advanced_at) VALUES ($1,$2,'',clock_timestamp()) "
                "ON CONFLICT DO NOTHING",
                workspace_id,
                instance_id,
            )
            row = await conn.fetchrow(
                "SELECT * FROM custos_state.connector_cursor "
                "WHERE workspace_id=$1 AND instance_id=$2 FOR UPDATE",
                workspace_id,
                instance_id,
            )
            assert row is not None
            now = await _scheduler_now(conn)
            interval = require_pull_ready(control, now)
            if (
                row["lease_holder"] is not None
                and row["lease_expires_at"] is not None
                and row["lease_expires_at"] > now
            ):
                raise LeaseBusy("pull cursor already has live ownership")
            lease = PullLease(
                workspace_id,
                instance_id,
                holder_id,
                "pull:" + uuid4().hex,
                pull_deadline(now, ttl_seconds),
                interval,
            )
            updated = replace(
                control,
                next_eligible_at=pull_deadline(now, interval),
                invocation_id=lease.token,
                invocation_owner=holder_id,
                revision=control.revision + 1,
                updated_at=now,
                updated_by=holder_id,
            )
            await self._save_pull_control(conn, updated)
            row = await conn.fetchrow(
                "UPDATE custos_state.connector_cursor SET lease_holder=$3,lease_expires_at=$4 "
                "WHERE workspace_id=$1 AND instance_id=$2 RETURNING *",
                workspace_id,
                instance_id,
                lease.token,
                lease.expires_at,
            )
            assert row is not None
            return _row_to_cursor(row), lease

    async def _owned_pull_tick(
        self,
        conn: Connection,
        workspace_id: WorkspaceId,
        lease: PullLease,
        *,
        allow_expired: bool = False,
    ) -> tuple[ConnectorPullControl, ConnectorCursor, datetime]:
        """Lock policy before cursor consistently, then validate the exact live token."""
        if not isinstance(lease, PullLease) or lease.workspace_id != workspace_id:
            raise LeaseExpired("foreign pull cursor lease")
        control = await self._locked_pull_control(conn, workspace_id, lease.instance_id)
        row = await conn.fetchrow(
            "SELECT * FROM custos_state.connector_cursor "
            "WHERE workspace_id=$1 AND instance_id=$2 FOR UPDATE",
            workspace_id,
            lease.instance_id,
        )
        now = await _scheduler_now(conn)
        cursor = _row_to_cursor(row) if row is not None else None
        require_pull_lease(workspace_id, lease, cursor, now, allow_expired=allow_expired)
        require_pull_invocation(control, lease)
        if control is None or cursor is None:
            raise LeaseExpired("pull ownership policy disappeared")
        return control, cursor, now

    async def commit_pull_tick(
        self,
        workspace_id: WorkspaceId,
        lease: PullLease,
        new_value: str,
        new_advanced_at: datetime,
    ) -> ConnectorCursor:
        """Commit acknowledged cursor progress without clearing the reserved polling interval."""
        instant = utc_instant(new_advanced_at)
        async with self._pull_transaction() as conn:
            control, _, now = await self._owned_pull_tick(conn, workspace_id, lease)
            await self._save_pull_control(
                conn,
                replace(
                    control,
                    invocation_id=None,
                    invocation_owner=None,
                    consecutive_failures=0,
                    revision=control.revision + 1,
                    updated_at=now,
                    updated_by=lease.holder_id,
                ),
            )
            row = await conn.fetchrow(
                "UPDATE custos_state.connector_cursor SET value=$3,advanced_at=$4,"
                "lease_holder=NULL,lease_expires_at=NULL "
                "WHERE workspace_id=$1 AND instance_id=$2 RETURNING *",
                workspace_id,
                lease.instance_id,
                new_value,
                instant,
            )
            assert row is not None
            return _row_to_cursor(row)

    async def fail_pull_tick(
        self,
        workspace_id: WorkspaceId,
        lease: PullLease,
        *,
        backoff_cap_seconds: int,
    ) -> None:
        """Persist shared backoff and release ownership without advancing the cursor."""
        cap = pull_interval(backoff_cap_seconds)
        async with self._pull_transaction() as conn:
            control, _, now = await self._owned_pull_tick(
                conn, workspace_id, lease, allow_expired=True
            )
            failures = min(30, control.consecutive_failures + 1)
            delay = max(
                lease.interval_seconds, min(cap, lease.interval_seconds * 2 ** (failures - 1))
            )
            await self._save_pull_control(
                conn,
                replace(
                    control,
                    invocation_id=None,
                    invocation_owner=None,
                    consecutive_failures=failures,
                    revision=control.revision + 1,
                    next_eligible_at=max(control.next_eligible_at, pull_deadline(now, delay)),
                    updated_at=now,
                    updated_by=lease.holder_id,
                ),
            )
            await conn.execute(
                "UPDATE custos_state.connector_cursor SET lease_holder=NULL,lease_expires_at=NULL "
                "WHERE workspace_id=$1 AND instance_id=$2",
                workspace_id,
                lease.instance_id,
            )

    async def recover_pull_invocation(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        *,
        invocation_id: str,
        audit: AuditEvent,
    ) -> ConnectorPullControl:
        """Recover only the confirmed expired invocation and commit its audit atomically."""
        async with self._pull_transaction() as conn:
            control = await self._locked_pull_control(conn, workspace_id, instance_id)
            row = await conn.fetchrow(
                "SELECT * FROM custos_state.connector_cursor "
                "WHERE workspace_id=$1 AND instance_id=$2 FOR UPDATE",
                workspace_id,
                instance_id,
            )
            updated = recover_pull_invocation(
                control,
                _row_to_cursor(row) if row is not None else None,
                workspace_id,
                instance_id,
                invocation_id,
                audit,
                await _scheduler_now(conn),
            )
            handle = PgTransactionHandle(conn)
            bind_handle(handle, self)
            try:
                await self.append_audit(workspace_id, audit, tx=handle)
            finally:
                handle._mark_closed()
            await self._save_pull_control(conn, updated)
            await conn.execute(
                "UPDATE custos_state.connector_cursor SET lease_holder=NULL,lease_expires_at=NULL "
                "WHERE workspace_id=$1 AND instance_id=$2",
                workspace_id,
                instance_id,
            )
            return updated

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
                    "SELECT pg_try_advisory_xact_lock(hashtext($1), hashtext($2))",
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
                f"lease handle was not issued by PgMetadataAdapter (got {type(lease).__name__})"
            )
        if lease.workspace_id != workspace_id:
            raise LeaseExpired("lease handle workspace does not match commit_cursor argument")
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

    async def release_cursor_lease(self, workspace_id: str, lease: LeaseHandle) -> None:
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

    async def read_cursor(self, workspace_id: str, instance_id: str) -> ConnectorCursor | None:
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
            raise ValueError(f"unknown connector_cursor: {workspace_id!r}/{instance_id!r}")
        return _row_to_cursor(row)

    # ----- Artifact backrefs -----

    async def append_artifact_use(self, workspace_id: str, use: ArtifactUse) -> ArtifactUse:
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
            # Try to insert a fresh record; if it exists, reclaim it only if expired.
            row = await conn.fetchrow(
                "INSERT INTO custos_state.idempotency_record "
                "(workspace_id, principal_id, route, idempotency_key, "
                " request_hash, status, reserved_at, expires_at) "
                "VALUES ($1, $2, $3, $4, $5, 'in_progress', now(), $6) "
                "ON CONFLICT (workspace_id, principal_id, route, idempotency_key) "
                "DO UPDATE SET "
                "request_hash = EXCLUDED.request_hash, "
                "status = EXCLUDED.status, "
                "response_snapshot = NULL, "
                "reserved_at = EXCLUDED.reserved_at, "
                "expires_at = EXCLUDED.expires_at "
                "WHERE custos_state.idempotency_record.expires_at <= now() "
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
                "DELETE FROM custos_state.idempotency_record WHERE expires_at <= $1",
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
            raise ValueError(f"no device-code session for {workspace_id!r}/{device_code!r}")
        return _row_to_device_code_session(row)

    async def delete_expired_device_code_sessions(self, before: datetime) -> int:
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM custos_state.device_code_session WHERE expires_at <= $1",
                before,
            )
        count = int(result.split()[-1]) if result else 0
        return count

    # ----- Audit writer + outbox drain -----

    async def append_audit(
        self,
        workspace_id: str,
        event: AuditEvent,
        tx: TransactionHandle | None = None,
    ) -> None:
        """Write an audit event and queue it for outbox drain.

        If `tx` is provided, both writes participate in the caller's
        transaction so rolled-back mutations don't leak audit rows.
        Otherwise writes are standalone.
        """
        self._check_tx_handle(tx)
        pool = await self._pool_ref()

        if tx is not None:
            # Use the provided transaction handle's connection.
            conn = cast(PgTransactionHandle, tx).conn
            try:
                await conn.execute(
                    "INSERT INTO custos_state.audit_event "
                    "(workspace_id, event_id, event_type, actor, subject, payload, "
                    "occurred_at) "
                    "VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7)",
                    workspace_id,
                    event.event_id,
                    event.event_type,
                    event.actor,
                    json.dumps(dict(event.subject)),
                    json.dumps(dict(event.payload)),
                    event.occurred_at,
                )
                await conn.execute(
                    "INSERT INTO custos_state.audit_outbox "
                    "(workspace_id, event_id, event_type, payload) "
                    "VALUES ($1, $2, $3, $4::jsonb)",
                    workspace_id,
                    event.event_id,
                    event.event_type,
                    json.dumps(dict(event.payload)),
                )
            except Exception as exc:
                raise self._classify(exc) from exc
        else:
            async with pool.acquire() as conn:
                try:
                    await conn.execute(
                        "INSERT INTO custos_state.audit_event "
                        "(workspace_id, event_id, event_type, actor, subject, "
                        "payload, occurred_at) "
                        "VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7)",
                        workspace_id,
                        event.event_id,
                        event.event_type,
                        event.actor,
                        json.dumps(dict(event.subject)),
                        json.dumps(dict(event.payload)),
                        event.occurred_at,
                    )
                    await conn.execute(
                        "INSERT INTO custos_state.audit_outbox "
                        "(workspace_id, event_id, event_type, payload) "
                        "VALUES ($1, $2, $3, $4::jsonb)",
                        workspace_id,
                        event.event_id,
                        event.event_type,
                        json.dumps(dict(event.payload)),
                    )
                except Exception as exc:
                    raise self._classify(exc) from exc

    async def query_audit(
        self,
        workspace_id: str,
        filter: AuditFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[AuditEvent]:
        """Query audit events with optional filtering and keyset pagination.

        Ordered by (occurred_at DESC, event_id DESC) for consistent pagination.
        """
        eff_limit = _resolve_limit(limit)
        params: list[Any] = [workspace_id]
        where = ["workspace_id = $1"]

        if filter is not None:
            if filter.event_type is not None:
                params.append(filter.event_type)
                where.append(f"event_type = ${len(params)}")
            if filter.actor is not None:
                params.append(filter.actor)
                where.append(f"actor = ${len(params)}")
            if filter.occurred_after is not None:
                params.append(filter.occurred_after)
                where.append(f"occurred_at >= ${len(params)}")
            if filter.occurred_before is not None:
                params.append(filter.occurred_before)
                where.append(f"occurred_at < ${len(params)}")

        if cursor is not None:
            ts_iso, eid = _decode_cursor(cursor)
            params.extend([datetime.fromisoformat(ts_iso), eid])
            where.append(f"(occurred_at, event_id) < (${len(params) - 1}, ${len(params)})")

        params.append(eff_limit + 1)
        sql = (
            "SELECT workspace_id, event_id, event_type, actor, subject, payload, occurred_at "
            "FROM custos_state.audit_event "
            f"WHERE {' AND '.join(where)} "
            "ORDER BY occurred_at DESC, event_id DESC "
            f"LIMIT ${len(params)}"
        )

        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        next_cursor: Cursor | None = None
        if len(rows) > eff_limit:
            last = rows[eff_limit - 1]
            next_cursor = _encode_cursor(last["occurred_at"], last["event_id"])
            rows = rows[:eff_limit]

        return Page(
            items=[_row_to_audit_event(r) for r in rows],
            next_cursor=next_cursor,
        )

    async def _check_audit_outbox_ready(self, conn: Connection) -> None:
        """Require the outbox fence without raising the general metadata minimum."""
        try:
            ready = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM custos_meta.adapter_revisions "
                "WHERE interface_name=$1 AND revision=$2)",
                INTERFACE_NAME,
                METADATA_REV12.number,
            )
        except UndefinedTableError as exc:
            raise MigrationRequired([(INTERFACE_NAME, METADATA_REV12.number)]) from exc
        if not ready:
            raise MigrationRequired([(INTERFACE_NAME, METADATA_REV12.number)])

    async def check_audit_outbox_ready(self) -> None:
        """Expose revision12 as an explicit preflight for every outbox consumer."""
        try:
            pool = await self._pool_ref()
            async with pool.acquire() as conn:
                await self._check_audit_outbox_ready(conn)
        except (OSError, InterfaceError, PostgresError) as exc:
            raise BackendUnavailable("audit outbox readiness unavailable", cause=exc) from exc

    async def stream_audit_outbox(self, cursor: int, batch_size: int) -> AuditOutboxBatch:
        """Stream audit outbox rows for drain.

        Returns rows with id > cursor, up to batch_size, ordered by id ASC.
        A bounded advisory fence waits for existing outbox ID allocators before
        sampling the committed prefix, preventing late lower IDs from being skipped.
        """
        pool = await self._pool_ref()
        try:
            async with pool.acquire() as conn, conn.transaction(isolation="read_committed"):
                await conn.execute("SET LOCAL lock_timeout = '5s'")
                await conn.execute(
                    "SELECT pg_advisory_xact_lock($1::bigint)", AUDIT_OUTBOX_LOCK_KEY
                )
                await self._check_audit_outbox_ready(conn)
                rows = await conn.fetch(
                    "SELECT id, workspace_id, event_id, event_type, payload, "
                    "enqueued_at "
                    "FROM custos_state.audit_outbox "
                    "WHERE id > $1 "
                    "ORDER BY id ASC "
                    "LIMIT $2",
                    cursor,
                    batch_size,
                )
        except (OSError, InterfaceError, PostgresError) as exc:
            raise BackendUnavailable(
                "could not read a commit-safe audit outbox prefix", cause=exc
            ) from exc

        if not rows:
            return AuditOutboxBatch(rows=(), next_cursor=cursor)

        next_cursor = rows[-1]["id"]
        return AuditOutboxBatch(
            rows=tuple(_row_to_audit_outbox_row(r) for r in rows),
            next_cursor=next_cursor,
        )

    async def commit_audit_outbox_cursor(self, pipeline_id: str, cursor: int) -> None:
        """Record the high-water mark for a drain pipeline.

        Each pipeline keeps its own cursor so slow consumers don't block fast ones.
        Implicitly creates the row on first call.
        """
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO custos_state.audit_outbox_cursor "
                "(pipeline_id, cursor, updated_at) "
                "VALUES ($1, $2, now()) "
                "ON CONFLICT (pipeline_id) DO UPDATE "
                "SET cursor = EXCLUDED.cursor, updated_at = now()",
                pipeline_id,
                cursor,
            )

    def listen_audit_outbox(self) -> AsyncIterator[NotifyEvent]:
        """Optional notify stream for low-latency drain alerting.

        This adapter does not implement notification-based audit outbox
        listening, so raise QueryUnsupported to signal the drainer to
        fall back to polling.
        """
        raise QueryUnsupported("listen_audit_outbox not implemented by this adapter")

    # ----- Transactions -----

    async def with_transaction(self, fn: Callable[[TransactionHandle], Awaitable[T]]) -> T:
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

"""MetadataStoreProvider — runtime + trigger + cursor + idempotency + audit state.

Owns the seven entity families described in
`design/components/storage-provider-layer/design.md` § MetadataStoreProvider:

1. Runtime execution state (`Run`, `Step`, `StepAttempt`)
2. Trigger Service state (`Subscription`, `SubscriptionSelector`,
   `ResumeSubscription`, `DedupKey`, `Schedule`)
3. Connector pull cursors (`ConnectorCursor` + lease primitive)
4. Artifact backrefs (`ArtifactUse`)
5. Gateway short-lived state (`IdempotencyRecord`, `DeviceCodeSession`)
6. Audit writer + outbox drain (`AuditEvent`, `AuditOutboxRow`; the
   per-pipeline high-water mark is stored in the adapter's
   `audit_outbox_cursor` table and is exposed to callers only as the
   opaque `int` cursor passed to `commit_audit_outbox_cursor`)
7. Intra-provider transactions (`with_transaction`)

All methods are workspace-scoped; `workspace_id` is the first arg on every
call except the audit-outbox drain (operated by Observability Service in
its own bookkeeping namespace).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar, Protocol, TypeVar, runtime_checkable

from custos_spl.ids import (
    ArtifactId,
    ConnectorInstanceId,
    PrincipalId,
    RunId,
    StepId,
    SubscriptionId,
    WorkflowId,
    WorkspaceId,
)
from custos_spl.pagination import Cursor, Page

T = TypeVar("T")


# ============================================================================
# Section 1 — Runtime execution state
# ============================================================================


@dataclass(frozen=True, slots=True)
class Run:
    """A single workflow run row."""

    workspace_id: WorkspaceId
    run_id: RunId
    workflow_id: WorkflowId
    workflow_version: str
    status: str
    reason: str | None
    started_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class Step:
    """A step instance within a run."""

    workspace_id: WorkspaceId
    run_id: RunId
    step_id: StepId
    name: str
    status: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class StepAttempt:
    """One attempt of a step.

    Append-only: writes are keyed by `(run_id, step_id, attempt)`. Any
    write attempting to mutate an existing triple raises
    `ImmutableViolation`.
    """

    workspace_id: WorkspaceId
    run_id: RunId
    step_id: StepId
    attempt: int
    status: str
    started_at: datetime
    finished_at: datetime | None
    error: Mapping[str, Any] | None


@dataclass(frozen=True, slots=True)
class RunFilter:
    """Optional filter for `list_runs`."""

    workflow_id: WorkflowId | None = None
    status: str | None = None
    started_after: datetime | None = None
    started_before: datetime | None = None


# ============================================================================
# Section 2 — Trigger Service state
# ============================================================================


@dataclass(frozen=True, slots=True)
class Subscription:
    """A trigger subscription owned by Trigger Service."""

    workspace_id: WorkspaceId
    subscription_id: SubscriptionId
    workflow_id: WorkflowId
    state: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class SubscriptionSelector:
    """Append-only selector row attached to a subscription."""

    workspace_id: WorkspaceId
    subscription_id: SubscriptionId
    selector: Mapping[str, Any]
    added_at: datetime


@dataclass(frozen=True, slots=True)
class ResumeSubscription:
    """A pending resume token; swept by Trigger Service on TTL expiry."""

    workspace_id: WorkspaceId
    resume_id: str
    run_id: RunId
    step_id: StepId
    expires_at: datetime
    payload: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class DedupKey:
    """A trigger dedup key row."""

    workspace_id: WorkspaceId
    key: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class DedupReserved:
    """`put_dedup_key` succeeded — caller may proceed with the trigger."""

    key: DedupKey


@dataclass(frozen=True, slots=True)
class DedupDuplicate:
    """`put_dedup_key` found an existing un-expired row for this key.

    Caller MUST treat the trigger as already handled.
    """

    existing: DedupKey


PutDedupKeyResult = DedupReserved | DedupDuplicate


@dataclass(frozen=True, slots=True)
class Schedule:
    """A scheduled trigger row."""

    workspace_id: WorkspaceId
    schedule_id: str
    workflow_id: WorkflowId
    cron: str
    next_fire_at: datetime
    enabled: bool


# ============================================================================
# Section 3 — Connector pull cursors
# ============================================================================


class LeaseHandle:
    """Opaque marker base class for connector-cursor leases.

    Adapters subclass this to attach backend-specific state (e.g. the
    Postgres adapter carries the fencing token / row version). Callers
    treat instances as fully opaque — pass them back to `commit_cursor`
    or `release_cursor_lease` and inspect nothing.
    """

    __slots__ = ()


@dataclass(frozen=True, slots=True)
class ConnectorCursor:
    """A pull-cursor row for a connector instance."""

    workspace_id: WorkspaceId
    instance_id: ConnectorInstanceId
    value: str
    advanced_at: datetime
    lease_holder: str | None
    lease_expires_at: datetime | None


# ============================================================================
# Section 4 — Artifact backrefs
# ============================================================================


@dataclass(frozen=True, slots=True)
class ArtifactUse:
    """Append-only backref recording that a run cited an artifact."""

    workspace_id: WorkspaceId
    run_id: RunId
    step_id: StepId
    artifact_id: ArtifactId
    name: str
    recorded_at: datetime


# ============================================================================
# Section 5 — Gateway short-lived state
# ============================================================================


@dataclass(frozen=True, slots=True)
class IdempotencyRecord:
    """An API-gateway idempotency row.

    Key is `(workspace_id, principal_id, route, idempotency_key)`. The
    state machine is `in_progress → completed`; expired rows are reaped
    by `delete_expired_idempotency_records`.
    """

    workspace_id: WorkspaceId
    principal_id: PrincipalId
    route: str
    idempotency_key: str
    request_hash: str
    status: str
    response_snapshot: Mapping[str, Any] | None
    reserved_at: datetime
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class IdemReserved:
    """Caller acquired a fresh reservation; proceed to perform the work."""

    record: IdempotencyRecord


@dataclass(frozen=True, slots=True)
class ExistingCompleted:
    """An identical-hash request already completed; return its snapshot."""

    record: IdempotencyRecord
    response_snapshot: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class ExistingInFlight:
    """An identical-hash request is in flight; caller MUST wait/retry."""

    record: IdempotencyRecord


@dataclass(frozen=True, slots=True)
class KeyReuse:
    """The idempotency key is reused with a *different* request hash.

    Gateway maps this to HTTP 422 — the client violated idempotency-key
    semantics.
    """

    record: IdempotencyRecord


ReserveIdempotencyResult = IdemReserved | ExistingCompleted | ExistingInFlight | KeyReuse


@dataclass(frozen=True, slots=True)
class DeviceCodeSession:
    """An OIDC device-code session row."""

    workspace_id: WorkspaceId
    device_code: str
    user_code: str
    issuer_alias: str
    status: str
    token_bundle: Mapping[str, Any] | None
    created_at: datetime
    expires_at: datetime


# ============================================================================
# Section 6 — Audit (writer + outbox drain)
# ============================================================================


@dataclass(frozen=True, slots=True)
class AuditEvent:
    """An audit event row. Append-only by enforcement (see Audit Partition)."""

    workspace_id: WorkspaceId
    event_id: str
    event_type: str
    actor: str
    subject: Mapping[str, Any]
    payload: Mapping[str, Any]
    occurred_at: datetime


@dataclass(frozen=True, slots=True)
class AuditFilter:
    """Optional filter for `query_audit`."""

    event_type: str | None = None
    actor: str | None = None
    occurred_after: datetime | None = None
    occurred_before: datetime | None = None


@dataclass(frozen=True, slots=True)
class AuditOutboxRow:
    """A row in the audit outbox, awaiting drain to a downstream pipeline."""

    id: int
    workspace_id: WorkspaceId
    event_id: str
    event_type: str
    payload: Mapping[str, Any]
    enqueued_at: datetime


@dataclass(frozen=True, slots=True)
class AuditOutboxBatch:
    """A batch returned by `stream_audit_outbox`.

    `next_cursor` is the high-water mark to pass to the next call (and
    to `commit_audit_outbox_cursor` after the batch is persisted).
    """

    rows: tuple[AuditOutboxRow, ...] = field(default=())
    next_cursor: int = 0


@dataclass(frozen=True, slots=True)
class NotifyEvent:
    """An audit-outbox notification (adapter-optional optimization).

    Carries the post-notify high-water mark so drainers can skip an
    initial `SELECT` when only one row was inserted. Adapters that lack
    notify primitives never emit these; the drainer falls back to polling.
    """

    cursor: int


# ============================================================================
# Section 7 — Transactions
# ============================================================================


class TransactionHandle:
    """Opaque marker base class for `with_transaction` callbacks.

    Adapters subclass this to carry the backend transaction (e.g.
    asyncpg connection in a `BEGIN`/`COMMIT` block). The handle is tied
    to the issuing provider; passing it to a different provider raises
    `InvalidTransactionHandle`. Callers MUST NOT inspect or persist
    these handles outside the scope of the callback.
    """

    __slots__ = ()


# ============================================================================
# Protocol
# ============================================================================


@runtime_checkable
class MetadataStoreProvider(Protocol):
    """Runtime + trigger + cursor + idempotency + audit metadata store.

    All workspace-scoped methods take `workspace_id` as the first
    argument. Audit-outbox drain methods (`stream_audit_outbox`,
    `commit_audit_outbox_cursor`, `listen_audit_outbox`) are operated by
    Observability Service across all workspaces and therefore omit it.

    The schema revision required by this build is `SCHEMA_REVISION = 4`
    (adds the `audit_outbox_cursor` table for per-pipeline drain).
    """

    SCHEMA_REVISION: ClassVar[int] = 4

    # ----- Runtime execution state -----

    async def put_run(self, workspace_id: WorkspaceId, run: Run) -> Run:
        """Insert a new run row. Re-puts of the same `run_id` raise `ImmutableViolation`."""
        ...

    async def update_run_status(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        status: str,
        reason: str | None = None,
    ) -> Run:
        """Last-writer-wins status update. Every transition is audited."""
        ...

    async def get_run(self, workspace_id: WorkspaceId, run_id: RunId) -> Run | None: ...

    async def list_runs(
        self,
        workspace_id: WorkspaceId,
        filter: RunFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[Run]: ...

    async def append_step(self, workspace_id: WorkspaceId, run_id: RunId, step: Step) -> Step:
        """One step instance per call. Re-puts of `(run_id, step_id)` raise `ImmutableViolation`."""
        ...

    async def append_step_attempt(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        step_id: StepId,
        attempt: StepAttempt,
    ) -> StepAttempt:
        """Append-only on `(run_id, step_id, attempt)`. Updates raise `ImmutableViolation`."""
        ...

    async def get_step_attempts(
        self, workspace_id: WorkspaceId, run_id: RunId, step_id: StepId
    ) -> tuple[StepAttempt, ...]:
        """Return all attempts for one step in attempt order."""
        ...

    # ----- Trigger Service state -----

    async def put_subscription(
        self, workspace_id: WorkspaceId, subscription: Subscription
    ) -> Subscription: ...

    async def update_subscription_state(
        self,
        workspace_id: WorkspaceId,
        subscription_id: SubscriptionId,
        state: str,
    ) -> Subscription: ...

    async def append_subscription_selector(
        self,
        workspace_id: WorkspaceId,
        subscription_id: SubscriptionId,
        selector: SubscriptionSelector,
    ) -> SubscriptionSelector:
        """Append-only; selectors accumulate without rewriting prior rows."""
        ...

    async def put_resume_subscription(
        self, workspace_id: WorkspaceId, resume: ResumeSubscription
    ) -> ResumeSubscription: ...

    async def delete_resume_subscription(
        self, workspace_id: WorkspaceId, resume_id: str
    ) -> None: ...

    async def put_dedup_key(
        self, workspace_id: WorkspaceId, key: str, ttl_seconds: int
    ) -> PutDedupKeyResult:
        """Atomic reserve-or-read. Returns `DedupReserved` on success or
        `DedupDuplicate` if an un-expired row already exists for `key`.
        """
        ...

    async def put_schedule(self, workspace_id: WorkspaceId, schedule: Schedule) -> Schedule: ...

    async def update_schedule_next_fire(
        self,
        workspace_id: WorkspaceId,
        schedule_id: str,
        next_fire_at: datetime,
    ) -> Schedule: ...

    # ----- Connector pull cursors -----

    async def acquire_cursor_lease(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        holder_id: str,
        ttl_seconds: int,
    ) -> tuple[ConnectorCursor, LeaseHandle]:
        """Acquire single-writer lease on a cursor.

        Raises `LeaseBusy` if another holder owns an un-expired lease.
        See § Lease Primitive Abstraction for the contract.
        """
        ...

    async def commit_cursor(
        self,
        workspace_id: WorkspaceId,
        lease: LeaseHandle,
        new_value: str,
        new_advanced_at: datetime,
    ) -> ConnectorCursor:
        """Atomic write of the cursor. Raises `LeaseExpired` if TTL elapsed."""
        ...

    async def release_cursor_lease(self, workspace_id: WorkspaceId, lease: LeaseHandle) -> None:
        """Explicit early release. Idempotent."""
        ...

    async def read_cursor(
        self, workspace_id: WorkspaceId, instance_id: ConnectorInstanceId
    ) -> ConnectorCursor | None:
        """Read-only; does not acquire a lease."""
        ...

    async def rewind_cursor(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        new_value: str,
        actor: str,
        reason: str,
    ) -> ConnectorCursor:
        """Operator-initiated rewind. Records a `cursor.rewound` audit event."""
        ...

    # ----- Artifact backrefs -----

    async def append_artifact_use(
        self, workspace_id: WorkspaceId, use: ArtifactUse
    ) -> ArtifactUse: ...

    async def list_artifact_uses(
        self,
        workspace_id: WorkspaceId,
        artifact_id: ArtifactId,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ArtifactUse]:
        """Used by the retention sweeper to refuse deletion of live-refs."""
        ...

    # ----- Gateway short-lived state -----

    async def reserve_idempotency_record(
        self,
        workspace_id: WorkspaceId,
        principal_id: PrincipalId,
        route: str,
        idempotency_key: str,
        request_hash: str,
        ttl_seconds: int,
    ) -> ReserveIdempotencyResult:
        """Atomic reserve-or-read on `(workspace_id, principal_id, route, idempotency_key)`.

        Returns:
          - `IdemReserved` — caller may proceed.
          - `ExistingCompleted(response_snapshot)` — identical hash, return snapshot.
          - `ExistingInFlight` — identical hash, still in progress.
          - `KeyReuse` — same key but different request hash; gateway → 422.
        """
        ...

    async def complete_idempotency_record(
        self,
        workspace_id: WorkspaceId,
        principal_id: PrincipalId,
        route: str,
        idempotency_key: str,
        response_snapshot: Mapping[str, Any],
    ) -> IdempotencyRecord:
        """Mark an in-progress reservation completed. Raises `NotReserved` otherwise."""
        ...

    async def delete_expired_idempotency_records(self, before: datetime) -> int:
        """Sweeper-only. Returns the number of rows deleted."""
        ...

    async def put_device_code_session(
        self, workspace_id: WorkspaceId, session: DeviceCodeSession
    ) -> DeviceCodeSession: ...

    async def get_device_code_session_by_device_code(
        self, workspace_id: WorkspaceId, device_code: str
    ) -> DeviceCodeSession | None: ...

    async def get_device_code_session_by_user_code(
        self, workspace_id: WorkspaceId, user_code: str
    ) -> DeviceCodeSession | None: ...

    async def complete_device_code_session(
        self,
        workspace_id: WorkspaceId,
        device_code: str,
        token_bundle: Mapping[str, Any],
    ) -> DeviceCodeSession: ...

    async def delete_expired_device_code_sessions(self, before: datetime) -> int:
        """Sweeper-only. Returns the number of rows deleted."""
        ...

    # ----- Audit (writer side) -----

    async def append_audit(
        self,
        workspace_id: WorkspaceId,
        event: AuditEvent,
        tx: TransactionHandle | None = None,
    ) -> None:
        """Write an audit event.

        When `tx` is provided, the outbox insert participates in the
        caller's transaction (so a rolled-back state mutation does not
        leak an audit row). Without `tx`, the write is standalone.
        """
        ...

    async def query_audit(
        self,
        workspace_id: WorkspaceId,
        filter: AuditFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[AuditEvent]:
        """Read-only audit query. Used by Observability and the lease-audit wrapper."""
        ...

    # ----- Audit outbox drain (Observability Service only) -----

    async def stream_audit_outbox(self, cursor: int, batch_size: int) -> AuditOutboxBatch:
        """Returns up to `batch_size` rows with `id > cursor`, in `id` ascending order.

        Idempotent and side-effect-free. Operated by Observability
        Service across all workspaces; no `workspace_id` argument.
        """
        ...

    async def commit_audit_outbox_cursor(self, pipeline_id: str, cursor: int) -> None:
        """Persist the high-water-mark cursor for a named drain pipeline.

        Each pipeline (`audit-store`, `audit-alert`, …) keeps its own
        cursor in the adapter's `audit_outbox_cursor` table; slow
        consumers cannot block fast ones.
        """
        ...

    def listen_audit_outbox(self) -> AsyncIterator[NotifyEvent]:
        """Optional notify stream for low-latency drain.

        Adapters that lack a notify primitive raise `QueryUnsupported`;
        the drainer falls back to polling at
        `CUSTOS_AUDIT_OUTBOX_POLL_INTERVAL_S`. NOT an `async def` — the
        body is `async def` on the adapter side but the Protocol-level
        signature returns the iterator directly so duck-typed adapters
        can declare either an async generator or an explicit return.
        """
        ...

    # ----- Transactions -----

    async def with_transaction(self, fn: Callable[[TransactionHandle], Awaitable[T]]) -> T:
        """Run `fn` inside a single transaction on this provider.

        Atomicity is intra-provider only. Passing the handle to a
        different provider raises `InvalidTransactionHandle`.
        """
        ...


__all__ = [
    "ArtifactUse",
    "AuditEvent",
    "AuditFilter",
    "AuditOutboxBatch",
    "AuditOutboxRow",
    "ConnectorCursor",
    "DedupDuplicate",
    "DedupKey",
    "DedupReserved",
    "DeviceCodeSession",
    "ExistingCompleted",
    "ExistingInFlight",
    "IdemReserved",
    "IdempotencyRecord",
    "KeyReuse",
    "LeaseHandle",
    "MetadataStoreProvider",
    "NotifyEvent",
    "PutDedupKeyResult",
    "ReserveIdempotencyResult",
    "ResumeSubscription",
    "Run",
    "RunFilter",
    "Schedule",
    "Step",
    "StepAttempt",
    "Subscription",
    "SubscriptionSelector",
    "TransactionHandle",
]

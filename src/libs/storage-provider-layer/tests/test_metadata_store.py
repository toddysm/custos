"""Tests for MetadataStoreProvider Protocol and its data shapes."""

from __future__ import annotations

import inspect
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

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
from custos_spl.interfaces import (
    ArtifactUse,
    AuditEvent,
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
    MetadataStoreProvider,
    PutDedupKeyResult,
    ReserveIdempotencyResult,
    ResumeSubscription,
    Run,
    Schedule,
    Step,
    StepAttempt,
    Subscription,
    SubscriptionSelector,
    TransactionHandle,
)


def _now() -> datetime:
    return datetime(2026, 1, 1, tzinfo=UTC)


# ----- Data shape immutability -----


def test_run_is_frozen() -> None:
    r = Run(
        workspace_id=WorkspaceId("ws-1"),
        run_id=RunId("r-1"),
        workflow_id=WorkflowId("wf-1"),
        workflow_version="1.0.0",
        status="running",
        reason=None,
        started_at=_now(),
        updated_at=_now(),
    )
    with pytest.raises(FrozenInstanceError):
        r.status = "completed"  # type: ignore[misc]


def test_step_attempt_is_frozen() -> None:
    a = StepAttempt(
        workspace_id=WorkspaceId("ws-1"),
        run_id=RunId("r-1"),
        step_id=StepId("s-1"),
        attempt=1,
        status="completed",
        started_at=_now(),
        finished_at=_now(),
        error=None,
    )
    with pytest.raises(FrozenInstanceError):
        a.attempt = 2  # type: ignore[misc]


def test_idempotency_record_is_frozen() -> None:
    rec = IdempotencyRecord(
        workspace_id=WorkspaceId("ws-1"),
        principal_id=PrincipalId("p-1"),
        route="POST /v1/runs",
        idempotency_key="k1",
        request_hash="sha256:abc",
        status="in_progress",
        response_snapshot=None,
        reserved_at=_now(),
        expires_at=_now(),
    )
    with pytest.raises(FrozenInstanceError):
        rec.status = "completed"  # type: ignore[misc]


def test_audit_outbox_batch_defaults() -> None:
    b = AuditOutboxBatch()
    assert b.rows == ()
    assert b.next_cursor == 0


# ----- Sentinel union shapes -----


def test_dedup_result_is_union_of_two_shapes() -> None:
    key = DedupKey(workspace_id=WorkspaceId("ws-1"), key="k1", expires_at=_now())
    reserved: PutDedupKeyResult = DedupReserved(key=key)
    duplicate: PutDedupKeyResult = DedupDuplicate(existing=key)
    assert isinstance(reserved, DedupReserved)
    assert isinstance(duplicate, DedupDuplicate)


def test_reserve_idempotency_result_has_four_variants() -> None:
    rec = IdempotencyRecord(
        workspace_id=WorkspaceId("ws-1"),
        principal_id=PrincipalId("p-1"),
        route="POST /v1/runs",
        idempotency_key="k1",
        request_hash="sha256:abc",
        status="in_progress",
        response_snapshot=None,
        reserved_at=_now(),
        expires_at=_now(),
    )
    variants: list[ReserveIdempotencyResult] = [
        IdemReserved(record=rec),
        ExistingCompleted(record=rec, response_snapshot={"ok": True}),
        ExistingInFlight(record=rec),
        KeyReuse(record=rec),
    ]
    assert len(variants) == 4


# ----- Marker base classes -----


def test_lease_handle_is_subclassable() -> None:
    class _PgLease(LeaseHandle):
        __slots__ = ("token",)

        def __init__(self, token: str) -> None:
            self.token = token

    lease = _PgLease("abc")
    assert isinstance(lease, LeaseHandle)
    assert lease.token == "abc"


def test_transaction_handle_is_subclassable() -> None:
    class _PgTx(TransactionHandle):
        pass

    assert isinstance(_PgTx(), TransactionHandle)


# ----- Protocol shape -----


def test_protocol_declares_required_schema_revision() -> None:
    """SCHEMA_REVISION == 4 — outbox-cursor-per-pipeline support is required."""
    assert MetadataStoreProvider.SCHEMA_REVISION == 4


REQUIRED_METHODS = [
    # Runtime
    "put_run",
    "update_run_status",
    "get_run",
    "list_runs",
    "append_step",
    "append_step_attempt",
    "get_step_attempts",
    # Trigger
    "put_subscription",
    "update_subscription_state",
    "append_subscription_selector",
    "put_resume_subscription",
    "delete_resume_subscription",
    "put_dedup_key",
    "put_schedule",
    "update_schedule_next_fire",
    # Cursors
    "acquire_cursor_lease",
    "commit_cursor",
    "release_cursor_lease",
    "read_cursor",
    "rewind_cursor",
    # Artifact backrefs
    "append_artifact_use",
    "list_artifact_uses",
    # Gateway
    "reserve_idempotency_record",
    "complete_idempotency_record",
    "delete_expired_idempotency_records",
    "put_device_code_session",
    "get_device_code_session_by_device_code",
    "get_device_code_session_by_user_code",
    "complete_device_code_session",
    "delete_expired_device_code_sessions",
    # Audit
    "append_audit",
    "query_audit",
    "stream_audit_outbox",
    "commit_audit_outbox_cursor",
    "listen_audit_outbox",
    # Transactions
    "with_transaction",
]


@pytest.mark.parametrize("method", REQUIRED_METHODS)
def test_protocol_exposes_method(method: str) -> None:
    assert hasattr(MetadataStoreProvider, method)


# `listen_audit_outbox` returns an AsyncIterator directly (not a coroutine);
# every other method must be `async def`.
_NON_COROUTINE_METHODS = {"listen_audit_outbox"}


@pytest.mark.parametrize("method", [m for m in REQUIRED_METHODS if m not in _NON_COROUTINE_METHODS])
def test_protocol_methods_are_async(method: str) -> None:
    fn = getattr(MetadataStoreProvider, method)
    assert inspect.iscoroutinefunction(fn), f"{method} must be async"


# ----- Workspace-scoping rule -----


_DRAIN_METHODS = {
    "stream_audit_outbox",
    "commit_audit_outbox_cursor",
    "listen_audit_outbox",
    "with_transaction",
    "delete_expired_idempotency_records",
    "delete_expired_device_code_sessions",
}


@pytest.mark.parametrize("method", [m for m in REQUIRED_METHODS if m not in _DRAIN_METHODS])
def test_workspace_scoped_methods_take_workspace_id_first(method: str) -> None:
    """Every non-drain, non-transaction method takes `workspace_id` first.

    Drain methods (audit outbox) and sweeper methods (delete_expired_*)
    are operated across all workspaces and are intentionally exempt.
    """
    sig = inspect.signature(getattr(MetadataStoreProvider, method))
    params = list(sig.parameters)
    # params[0] is self
    assert params[1] == "workspace_id", (
        f"{method} must take workspace_id as the first non-self argument"
    )


def test_drain_methods_do_not_take_workspace_id() -> None:
    """Outbox drain is operated by Observability Service platform-wide."""
    for name in ("stream_audit_outbox", "commit_audit_outbox_cursor"):
        sig = inspect.signature(getattr(MetadataStoreProvider, name))
        assert "workspace_id" not in sig.parameters, f"{name} must not take workspace_id"


# ----- runtime_checkable conformance -----


class _MinimalMetadataStore:
    """Just enough of the Protocol to satisfy isinstance() at runtime."""

    SCHEMA_REVISION = 4

    async def put_run(self, *a: object, **kw: object) -> None: ...
    async def update_run_status(self, *a: object, **kw: object) -> None: ...
    async def get_run(self, *a: object, **kw: object) -> None: ...
    async def list_runs(self, *a: object, **kw: object) -> None: ...
    async def append_step(self, *a: object, **kw: object) -> None: ...
    async def append_step_attempt(self, *a: object, **kw: object) -> None: ...
    async def get_step_attempts(self, *a: object, **kw: object) -> None: ...
    async def put_subscription(self, *a: object, **kw: object) -> None: ...
    async def update_subscription_state(self, *a: object, **kw: object) -> None: ...
    async def append_subscription_selector(self, *a: object, **kw: object) -> None: ...
    async def put_resume_subscription(self, *a: object, **kw: object) -> None: ...
    async def delete_resume_subscription(self, *a: object, **kw: object) -> None: ...
    async def put_dedup_key(self, *a: object, **kw: object) -> None: ...
    async def put_schedule(self, *a: object, **kw: object) -> None: ...
    async def update_schedule_next_fire(self, *a: object, **kw: object) -> None: ...
    async def acquire_cursor_lease(self, *a: object, **kw: object) -> None: ...
    async def commit_cursor(self, *a: object, **kw: object) -> None: ...
    async def release_cursor_lease(self, *a: object, **kw: object) -> None: ...
    async def read_cursor(self, *a: object, **kw: object) -> None: ...
    async def rewind_cursor(self, *a: object, **kw: object) -> None: ...
    async def append_artifact_use(self, *a: object, **kw: object) -> None: ...
    async def list_artifact_uses(self, *a: object, **kw: object) -> None: ...
    async def reserve_idempotency_record(self, *a: object, **kw: object) -> None: ...
    async def complete_idempotency_record(self, *a: object, **kw: object) -> None: ...
    async def delete_expired_idempotency_records(self, *a: object, **kw: object) -> None: ...
    async def put_device_code_session(self, *a: object, **kw: object) -> None: ...
    async def get_device_code_session_by_device_code(self, *a: object, **kw: object) -> None: ...
    async def get_device_code_session_by_user_code(self, *a: object, **kw: object) -> None: ...
    async def complete_device_code_session(self, *a: object, **kw: object) -> None: ...
    async def delete_expired_device_code_sessions(self, *a: object, **kw: object) -> None: ...
    async def append_audit(self, *a: object, **kw: object) -> None: ...
    async def query_audit(self, *a: object, **kw: object) -> None: ...
    async def stream_audit_outbox(self, *a: object, **kw: object) -> None: ...
    async def commit_audit_outbox_cursor(self, *a: object, **kw: object) -> None: ...

    def listen_audit_outbox(self) -> None: ...  # async iterator in real impls

    async def with_transaction(self, *a: object, **kw: object) -> None: ...


def test_runtime_checkable_recognizes_duck_typed_impl() -> None:
    assert isinstance(_MinimalMetadataStore(), MetadataStoreProvider)


def test_runtime_checkable_rejects_partial_impl() -> None:
    class Partial:
        async def put_run(self, *a: object, **kw: object) -> None: ...

    assert not isinstance(Partial(), MetadataStoreProvider)


# ----- Smoke: unused symbol imports compile -----


def test_smoke_imports() -> None:
    """All publicly exported symbols are importable from custos_spl.interfaces.

    Touches each one to keep import-only regressions from sneaking in.
    """
    _ = (
        ArtifactUse,
        AuditEvent,
        AuditOutboxBatch,
        ConnectorCursor,
        DeviceCodeSession,
        ResumeSubscription,
        Schedule,
        Step,
        Subscription,
        SubscriptionSelector,
        ArtifactId,
        ConnectorInstanceId,
        SubscriptionId,
    )

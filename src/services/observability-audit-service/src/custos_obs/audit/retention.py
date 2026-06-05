"""Audit Retention Worker (OBS-IMPL-007).

A lifespan-managed background task that periodically:

1. **Enforces audit retention.** Deletes ``custos_state.audit_event`` rows older
   than ``CUSTOS_AUDIT_RETENTION_DAYS`` (default 90). Audit rows are append-only;
   retention is the *only* deletion path, and the window is configurable upward
   without bound (never downward — that would lose data).
2. **Garbage-collects the outbox.** Deletes ``custos_state.audit_outbox`` rows
   whose ``id`` is below ``min(cursor)`` across all registered drain pipelines
   **and** whose age exceeds ``CUSTOS_AUDIT_OUTBOX_RETENTION_MARGIN`` (default
   24h). A row is GC-eligible only once *every* pipeline has drained past it, so
   a stuck pipeline (low cursor) keeps outbox rows around indefinitely —
   operators observe that via ``obs.outbox.lagging`` (OBS-IMPL-006) and fix the
   slow pipeline rather than silently losing un-drained rows.

A registered pipeline that has never committed a cursor is treated as cursor
``0`` (it pins ``min(cursor)`` to zero), so the worker never GCs rows a freshly
registered or stuck consumer still needs.

When a sweep deletes anything it emits ``obs.retention.applied`` carrying the
two deleted-row counts. Empty sweeps emit nothing — emitting would itself write
an audit row through the outbox and create perpetual churn.

The retention store operations the worker needs (bulk audit delete, cursor read,
outbox GC) are not part of the drain protocol the rest of the service consumes,
so the worker depends on a narrow :class:`AuditRetentionStore` seam rather than
the full ``MetadataStoreProvider``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Protocol

from custos_obs.audit.pipeline import AUDIT_ALERT_PIPELINE_ID, AUDIT_STORE_PIPELINE_ID
from custos_obs.events import RetentionApplied

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence

    from custos_spl import AuditEvent
    from custos_spl.ids import WorkspaceId

logger = logging.getLogger("custos_obs.audit.retention")

#: Pipelines whose cursors gate outbox GC by default (the two audit consumers).
DEFAULT_RETENTION_PIPELINE_IDS: tuple[str, ...] = (
    AUDIT_STORE_PIPELINE_ID,
    AUDIT_ALERT_PIPELINE_ID,
)


class AuditRetentionStore(Protocol):
    """Bulk-deletion + cursor-read seam used by the retention worker.

    These operations sit outside the audit-outbox *drain* protocol; an adapter
    that supports retention implements them in addition to
    ``MetadataStoreProvider``.
    """

    async def delete_audit_events_before(self, cutoff: datetime) -> int:
        """Delete audit events with ``occurred_at < cutoff``; return the count."""
        ...

    async def read_audit_outbox_cursors(self) -> Mapping[str, int]:
        """Return the committed cursor for every pipeline that has one."""
        ...

    async def delete_audit_outbox_before(
        self, max_id_exclusive: int, enqueued_before: datetime
    ) -> int:
        """Delete outbox rows with ``id < max_id_exclusive`` AND ``enqueued_at <
        enqueued_before``; return the count.
        """
        ...

    async def append_audit(self, workspace_id: WorkspaceId, event: AuditEvent) -> None:
        """Append an audit event (used to emit ``obs.retention.applied``)."""
        ...


@dataclass(frozen=True, slots=True)
class RetentionResult:
    """Row counts deleted by a single retention sweep."""

    audit_rows_deleted: int
    outbox_rows_deleted: int

    @property
    def deleted_anything(self) -> bool:
        return self.audit_rows_deleted > 0 or self.outbox_rows_deleted > 0


class AuditRetentionWorker:
    """Periodically enforces audit retention and garbage-collects the outbox."""

    def __init__(
        self,
        *,
        store: AuditRetentionStore,
        retention_days: int,
        outbox_retention_margin_s: int,
        sweep_interval_s: float,
        pipeline_ids: Sequence[str] = DEFAULT_RETENTION_PIPELINE_IDS,
        emit_event: Callable[[AuditEvent], Awaitable[None]] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if not pipeline_ids:
            raise ValueError("AuditRetentionWorker requires at least one pipeline id")
        self._store = store
        self._retention_days = retention_days
        self._outbox_retention_margin_s = outbox_retention_margin_s
        self._sweep_interval_s = sweep_interval_s
        self._pipeline_ids = tuple(pipeline_ids)
        self._emit_event = emit_event if emit_event is not None else self._default_emit
        self._now = now if now is not None else lambda: datetime.now(UTC)
        self._task: asyncio.Task[None] | None = None

    async def sweep_once(self) -> RetentionResult:
        """Run one retention + GC pass and emit ``obs.retention.applied`` if any
        rows were deleted.
        """
        now = self._now()

        audit_cutoff = now - timedelta(days=self._retention_days)
        audit_deleted = await self._store.delete_audit_events_before(audit_cutoff)

        outbox_deleted = await self._gc_outbox(now)

        result = RetentionResult(
            audit_rows_deleted=audit_deleted,
            outbox_rows_deleted=outbox_deleted,
        )
        if result.deleted_anything:
            logger.info(
                "retention sweep deleted %d audit row(s) and %d outbox row(s)",
                result.audit_rows_deleted,
                result.outbox_rows_deleted,
            )
            await self._emit_applied(result)
        return result

    async def _gc_outbox(self, now: datetime) -> int:
        cursors = await self._store.read_audit_outbox_cursors()
        # A registered-but-uncommitted pipeline is cursor 0 and pins the floor,
        # so its still-undrained rows are never collected.
        min_cursor = min(cursors.get(pid, 0) for pid in self._pipeline_ids)
        if min_cursor <= 0:
            return 0
        enqueued_before = now - timedelta(seconds=self._outbox_retention_margin_s)
        return await self._store.delete_audit_outbox_before(min_cursor, enqueued_before)

    async def _emit_applied(self, result: RetentionResult) -> None:
        event = RetentionApplied(
            audit_rows_deleted=result.audit_rows_deleted,
            outbox_rows_deleted=result.outbox_rows_deleted,
        ).to_audit_event()
        try:
            await self._emit_event(event)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("failed to emit obs.retention.applied")

    async def _default_emit(self, event: AuditEvent) -> None:
        await self._store.append_audit(event.workspace_id, event)

    async def run(self) -> None:
        """Sweep on startup, then on a fixed interval until cancelled."""
        await self._sweep_guarded()
        while True:
            try:
                await asyncio.sleep(self._sweep_interval_s)
            except asyncio.CancelledError:
                logger.info("audit retention worker stopping")
                raise
            await self._sweep_guarded()

    async def _sweep_guarded(self) -> None:
        """Run :meth:`sweep_once`, surviving transient store errors.

        A failed sweep is logged and retried next interval; a misbehaving
        backend must not silently disable retention. Cancellation propagates so
        shutdown stays prompt.
        """
        try:
            await self.sweep_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("audit retention sweep failed; will retry")

    def start(self) -> None:
        """Start the periodic sweep task (idempotent)."""
        if self._task is not None:
            return
        self._task = asyncio.create_task(self.run(), name="audit-retention-worker")

    async def stop(self) -> None:
        """Cancel and await the sweep task (idempotent)."""
        task = self._task
        if task is None:
            return
        self._task = None
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


__all__ = [
    "DEFAULT_RETENTION_PIPELINE_IDS",
    "AuditRetentionStore",
    "AuditRetentionWorker",
    "RetentionResult",
]

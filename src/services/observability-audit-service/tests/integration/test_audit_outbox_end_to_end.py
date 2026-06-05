"""End-to-end audit-outbox integration tests against a real ``custos_pg`` Postgres.

These drive the **drain → store → retention** path through the production
``PgMetadataAdapter`` (no fakes for the outbox/store surface):

* :class:`AuditOutboxDrainer` streams the real ``custos_state.audit_outbox`` and
  fans batches out through :class:`AuditPipeline` to the ``audit-store`` +
  ``audit-alert`` consumers, which commit their per-pipeline cursors via the
  real adapter.
* the durable store read-back is verified through the adapter's ``query_audit``.
* :class:`AuditRetentionWorker` enforces the retention window and GCs the outbox
  through a thin pool-backed :class:`AuditRetentionStore` (the bulk-delete +
  cursor-read methods the SPL *drain* protocol does not include), proving the
  cutoff math, the per-pipeline min-cursor GC floor, and the
  ``obs.retention.applied`` emission against real rows.

The suite is gated behind ``-m integration`` and skips cleanly when no Postgres
(``CUSTOS_PG_DSN`` / Docker) is available — see ``conftest.py``.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from custos_spl import AuditEvent
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.metadata_store import (
    AuditOutboxBatch,
    AuditOutboxRow,
    MetadataStoreProvider,
)

from custos_obs.audit.drainer import AuditOutboxDrainer
from custos_obs.audit.pipeline import (
    AUDIT_ALERT_PIPELINE_ID,
    AUDIT_STORE_PIPELINE_ID,
    AuditConsumer,
    AuditPipeline,
    AuditStoreConsumer,
)
from custos_obs.audit.retention import AuditRetentionWorker

pytestmark = [pytest.mark.integration]

_WS = "ws-integration"


def _event(
    event_id: str, occurred_at: datetime, *, event_type: str = "workflow.run.started"
) -> AuditEvent:
    """Build an audit event in the shape every Custos service emits."""
    return AuditEvent(
        workspace_id=WorkspaceId(_WS),
        event_id=event_id,
        event_type=event_type,
        actor="user:alice",
        subject={"run_id": event_id},
        payload={"event_id": event_id},
        occurred_at=occurred_at,
    )


class _RecordingWriter:
    """Idempotent-seam stand-in that records every row the store consumer drives."""

    def __init__(self) -> None:
        self.rows: list[AuditOutboxRow] = []

    async def __call__(self, row: AuditOutboxRow) -> None:
        self.rows.append(row)


class _RecordingBatchConsumer:
    """A batch handler that records every drained row (the ``audit-alert`` seam)."""

    def __init__(self) -> None:
        self.rows: list[AuditOutboxRow] = []

    async def __call__(self, batch: AuditOutboxBatch) -> None:
        self.rows.extend(batch.rows)


class _FailIfCalled:
    """A drainer handler that must never run (proves cursor durability)."""

    async def __call__(self, batch: AuditOutboxBatch) -> None:
        raise AssertionError("handler should not be called when the backlog is drained")


def _rowcount(command_tag: str) -> int:
    """Parse an asyncpg ``DELETE N`` command tag into its row count."""
    return int(command_tag.split()[-1])


async def _db_now(pool: Any) -> datetime:
    """Read the Postgres transaction clock.

    The retention worker's ``enqueued_before`` cutoff must be compared against
    ``audit_outbox.enqueued_at`` (set by the database ``DEFAULT now()``), so the
    worker's ``now`` is pinned to the database clock rather than the runner's to
    keep the comparison self-consistent under clock skew.
    """
    async with pool.acquire() as conn:
        value = await conn.fetchval("SELECT now()")
    assert isinstance(value, datetime)
    return value


class _PoolAuditRetentionStore:
    """Pool-backed :class:`AuditRetentionStore` over the real audit tables.

    Implements the three bulk-delete / cursor-read operations the SPL *drain*
    protocol omits (and which ``PgMetadataAdapter`` does not expose) with raw
    SQL against the same ``custos_state`` tables, delegating ``append_audit`` to
    the production adapter. This lets the retention worker run end-to-end
    against real rows.
    """

    def __init__(self, pool: Any, adapter: MetadataStoreProvider) -> None:
        self._pool = pool
        self._adapter = adapter

    async def delete_audit_events_before(self, cutoff: datetime) -> int:
        async with self._pool.acquire() as conn:
            tag = await conn.execute(
                "DELETE FROM custos_state.audit_event WHERE occurred_at < $1", cutoff
            )
        return _rowcount(tag)

    async def read_audit_outbox_cursors(self) -> Mapping[str, int]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT pipeline_id, cursor FROM custos_state.audit_outbox_cursor"
            )
        return {r["pipeline_id"]: int(r["cursor"]) for r in rows}

    async def delete_audit_outbox_before(
        self, max_id_exclusive: int, enqueued_before: datetime
    ) -> int:
        async with self._pool.acquire() as conn:
            tag = await conn.execute(
                "DELETE FROM custos_state.audit_outbox WHERE id < $1 AND enqueued_at < $2",
                max_id_exclusive,
                enqueued_before,
            )
        return _rowcount(tag)

    async def append_audit(self, workspace_id: WorkspaceId, event: AuditEvent) -> None:
        await self._adapter.append_audit(workspace_id, event)


def _build_pipeline(
    store: MetadataStoreProvider,
) -> tuple[AuditPipeline, _RecordingWriter, _RecordingBatchConsumer]:
    """Build a two-consumer pipeline (store + alert) with recording seams."""
    writer = _RecordingWriter()
    alert = _RecordingBatchConsumer()
    pipeline = AuditPipeline(
        store=store,
        consumers=[
            AuditConsumer(AUDIT_STORE_PIPELINE_ID, AuditStoreConsumer(writer)),
            AuditConsumer(AUDIT_ALERT_PIPELINE_ID, alert),
        ],
        lag_threshold=1000,
    )
    return pipeline, writer, alert


async def _seed(store: MetadataStoreProvider, events: list[AuditEvent]) -> None:
    for event in events:
        await store.append_audit(WorkspaceId(_WS), event)


async def test_drain_store_and_commit_against_postgres(
    metadata_store: MetadataStoreProvider, pg_pool: Any
) -> None:
    """Seed → drain across pages → both consumers see every row → cursors persist."""
    now = datetime.now(UTC)
    events = [_event(f"e{i}", now - timedelta(minutes=i)) for i in range(3)]
    await _seed(metadata_store, events)

    pipeline, writer, alert = _build_pipeline(metadata_store)
    drainer = AuditOutboxDrainer(
        store=metadata_store,
        handler=pipeline,
        mode="poll",
        poll_interval_s=0.01,
        batch_size=2,  # force multi-page drain
    )

    drained = await drainer.drain_once()

    assert drained == 3
    assert {r.event_id for r in writer.rows} == {"e0", "e1", "e2"}
    assert {r.event_id for r in alert.rows} == {"e0", "e1", "e2"}

    async with pg_pool.acquire() as conn:
        head = await conn.fetchval("SELECT max(id) FROM custos_state.audit_outbox")
        cursor_rows = await conn.fetch(
            "SELECT pipeline_id, cursor FROM custos_state.audit_outbox_cursor"
        )
    persisted = {r["pipeline_id"]: r["cursor"] for r in cursor_rows}
    assert persisted == {AUDIT_STORE_PIPELINE_ID: head, AUDIT_ALERT_PIPELINE_ID: head}
    assert pipeline.committed_cursors[AUDIT_STORE_PIPELINE_ID] == head

    page = await metadata_store.query_audit(WorkspaceId(_WS))
    assert {ev.event_id for ev in page.items} == {"e0", "e1", "e2"}


async def test_committed_cursor_is_durable_no_redelivery(
    metadata_store: MetadataStoreProvider, pg_pool: Any
) -> None:
    """A drainer resuming from the committed high-water mark re-delivers nothing."""
    now = datetime.now(UTC)
    await _seed(metadata_store, [_event(f"e{i}", now) for i in range(3)])

    pipeline, _writer, _alert = _build_pipeline(metadata_store)
    drainer = AuditOutboxDrainer(
        store=metadata_store, handler=pipeline, mode="poll", poll_interval_s=0.01
    )
    assert await drainer.drain_once() == 3

    resume = AuditOutboxDrainer(
        store=metadata_store,
        handler=_FailIfCalled(),
        mode="poll",
        poll_interval_s=0.01,
        start_cursor=pipeline.min_committed_cursor(),
    )
    assert await resume.drain_once() == 0


async def test_retention_deletes_aged_audit_and_gcs_outbox(
    metadata_store: MetadataStoreProvider, pg_pool: Any
) -> None:
    """Retention deletes aged audit rows, GCs drained outbox, emits the event."""
    now = datetime.now(UTC)
    await _seed(
        metadata_store,
        [
            _event("old", now - timedelta(days=200)),
            _event("new1", now - timedelta(minutes=2)),
            _event("new2", now - timedelta(minutes=1)),
        ],
    )

    pipeline, _writer, _alert = _build_pipeline(metadata_store)
    drainer = AuditOutboxDrainer(
        store=metadata_store, handler=pipeline, mode="poll", poll_interval_s=0.01
    )
    assert await drainer.drain_once() == 3
    head = pipeline.min_committed_cursor()

    emitted: list[AuditEvent] = []

    async def _record(event: AuditEvent) -> None:
        emitted.append(event)

    # Pin the worker clock to the database clock (read after draining) so the
    # outbox GC's enqueued_before cutoff is self-consistent with the
    # database-stamped enqueued_at, regardless of runner/container clock skew.
    pinned_now = await _db_now(pg_pool)
    worker = AuditRetentionWorker(
        store=_PoolAuditRetentionStore(pg_pool, metadata_store),
        retention_days=90,
        outbox_retention_margin_s=0,
        sweep_interval_s=3600,
        pipeline_ids=(AUDIT_STORE_PIPELINE_ID, AUDIT_ALERT_PIPELINE_ID),
        emit_event=_record,
        now=lambda: pinned_now,
    )
    result = await worker.sweep_once()

    # Only the 200-day-old event falls outside the 90-day window.
    assert result.audit_rows_deleted == 1
    # Outbox GC deletes every row strictly below the min committed cursor; the
    # last drained row (id == head) stays until a newer row supersedes it.
    assert result.outbox_rows_deleted == head - 1
    assert result.deleted_anything

    page = await metadata_store.query_audit(WorkspaceId(_WS))
    assert {ev.event_id for ev in page.items} == {"new1", "new2"}

    assert len(emitted) == 1
    assert emitted[0].event_type == "obs.retention.applied"
    assert emitted[0].payload["audit_rows_deleted"] == 1
    assert emitted[0].payload["outbox_rows_deleted"] == head - 1


async def test_lagging_pipeline_blocks_outbox_gc_and_empty_sweep_is_silent(
    metadata_store: MetadataStoreProvider, pg_pool: Any
) -> None:
    """A stuck consumer pins the GC floor to 0 and the empty sweep emits nothing."""
    now = datetime.now(UTC)
    await _seed(metadata_store, [_event(f"e{i}", now) for i in range(3)])

    async with pg_pool.acquire() as conn:
        head = await conn.fetchval("SELECT max(id) FROM custos_state.audit_outbox")
    # Only the store pipeline drains; audit-alert never commits (treated as 0).
    await metadata_store.commit_audit_outbox_cursor(AUDIT_STORE_PIPELINE_ID, head)

    emitted: list[AuditEvent] = []

    async def _record(event: AuditEvent) -> None:
        emitted.append(event)

    worker = AuditRetentionWorker(
        store=_PoolAuditRetentionStore(pg_pool, metadata_store),
        retention_days=3650,  # nothing is old enough to delete
        outbox_retention_margin_s=0,
        sweep_interval_s=3600,
        pipeline_ids=(AUDIT_STORE_PIPELINE_ID, AUDIT_ALERT_PIPELINE_ID),
        emit_event=_record,
    )
    result = await worker.sweep_once()

    assert result.audit_rows_deleted == 0
    assert result.outbox_rows_deleted == 0
    assert not result.deleted_anything
    assert emitted == []

    async with pg_pool.acquire() as conn:
        outbox_count = await conn.fetchval("SELECT count(*) FROM custos_state.audit_outbox")
        audit_count = await conn.fetchval("SELECT count(*) FROM custos_state.audit_event")
    assert outbox_count == 3
    assert audit_count == 3

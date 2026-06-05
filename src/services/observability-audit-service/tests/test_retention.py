"""Tests for the Audit Retention Worker (OBS-IMPL-007).

Drive :class:`AuditRetentionWorker` against an in-memory fake store: audit
retention only deletes rows past the window, outbox GC respects
``min(cursor)`` across registered pipelines (a stuck/uncommitted pipeline pins
the floor and preserves rows indefinitely), ``obs.retention.applied`` carries
accurate counts and is suppressed on empty sweeps, emit failures are swallowed,
cancellation propagates, and the start/stop lifecycle is idempotent.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from custos_spl import AuditEvent
from custos_spl.ids import WorkspaceId

from custos_obs.audit.retention import (
    DEFAULT_RETENTION_PIPELINE_IDS,
    AuditRetentionStore,
    AuditRetentionWorker,
    RetentionResult,
)
from custos_obs.events import ObsEventName

FIXED_NOW = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)


class FakeRetentionStore:
    """Records retention calls and serves canned cursor state."""

    def __init__(self, *, cursors: dict[str, int] | None = None) -> None:
        self.cursors: dict[str, int] = dict(cursors or {})
        self.audit_deleted = 7
        self.outbox_deleted = 4
        self.audit_cutoffs: list[datetime] = []
        self.outbox_calls: list[tuple[int, datetime]] = []
        self.appended: list[AuditEvent] = []

    async def delete_audit_events_before(self, cutoff: datetime) -> int:
        self.audit_cutoffs.append(cutoff)
        return self.audit_deleted

    async def read_audit_outbox_cursors(self) -> dict[str, int]:
        return dict(self.cursors)

    async def delete_audit_outbox_before(
        self, max_id_exclusive: int, enqueued_before: datetime
    ) -> int:
        self.outbox_calls.append((max_id_exclusive, enqueued_before))
        return self.outbox_deleted

    async def append_audit(self, workspace_id: WorkspaceId, event: AuditEvent) -> None:
        self.appended.append(event)


def _worker(
    store: object,
    *,
    retention_days: int = 90,
    outbox_retention_margin_s: int = 86_400,
    sweep_interval_s: float = 3_600,
    pipeline_ids: tuple[str, ...] = DEFAULT_RETENTION_PIPELINE_IDS,
    emit_event: object | None = None,
    now: datetime = FIXED_NOW,
) -> AuditRetentionWorker:
    return AuditRetentionWorker(
        store=cast(AuditRetentionStore, store),
        retention_days=retention_days,
        outbox_retention_margin_s=outbox_retention_margin_s,
        sweep_interval_s=sweep_interval_s,
        pipeline_ids=pipeline_ids,
        emit_event=emit_event,  # type: ignore[arg-type]
        now=lambda: now,
    )


# --------------------------------------------------------------------------- #
# Construction                                                                 #
# --------------------------------------------------------------------------- #


async def test_requires_at_least_one_pipeline_id() -> None:
    with pytest.raises(ValueError, match="at least one pipeline id"):
        _worker(FakeRetentionStore(), pipeline_ids=())


# --------------------------------------------------------------------------- #
# Audit retention                                                              #
# --------------------------------------------------------------------------- #


async def test_audit_cutoff_is_now_minus_retention_window() -> None:
    store = FakeRetentionStore(cursors={p: 0 for p in DEFAULT_RETENTION_PIPELINE_IDS})
    worker = _worker(store, retention_days=90)

    result = await worker.sweep_once()

    assert store.audit_cutoffs == [FIXED_NOW - timedelta(days=90)]
    assert result.audit_rows_deleted == 7


# --------------------------------------------------------------------------- #
# Outbox GC                                                                    #
# --------------------------------------------------------------------------- #


async def test_outbox_gc_uses_min_cursor_and_age_margin() -> None:
    store = FakeRetentionStore(cursors={p: 0 for p in DEFAULT_RETENTION_PIPELINE_IDS})
    store.cursors = {DEFAULT_RETENTION_PIPELINE_IDS[0]: 50, DEFAULT_RETENTION_PIPELINE_IDS[1]: 30}
    worker = _worker(store, outbox_retention_margin_s=86_400)

    result = await worker.sweep_once()

    # min(50, 30) == 30 is the exclusive upper id bound.
    assert store.outbox_calls == [(30, FIXED_NOW - timedelta(seconds=86_400))]
    assert result.outbox_rows_deleted == 4


async def test_stuck_pipeline_preserves_outbox_rows_indefinitely() -> None:
    # One pipeline has drained far ahead; the other is stuck at cursor 0.
    store = FakeRetentionStore(
        cursors={DEFAULT_RETENTION_PIPELINE_IDS[0]: 10_000, DEFAULT_RETENTION_PIPELINE_IDS[1]: 0}
    )
    worker = _worker(store)

    result = await worker.sweep_once()

    # min cursor is 0 -> no outbox GC at all.
    assert store.outbox_calls == []
    assert result.outbox_rows_deleted == 0


async def test_uncommitted_pipeline_pins_floor_to_zero() -> None:
    # Only one of the two registered pipelines has ever committed a cursor.
    store = FakeRetentionStore(cursors={DEFAULT_RETENTION_PIPELINE_IDS[0]: 999})
    worker = _worker(store)

    result = await worker.sweep_once()

    assert store.outbox_calls == []
    assert result.outbox_rows_deleted == 0


async def test_outbox_gc_runs_when_all_pipelines_have_advanced() -> None:
    store = FakeRetentionStore(
        cursors={DEFAULT_RETENTION_PIPELINE_IDS[0]: 200, DEFAULT_RETENTION_PIPELINE_IDS[1]: 200}
    )
    worker = _worker(store)

    await worker.sweep_once()

    assert store.outbox_calls == [(200, FIXED_NOW - timedelta(seconds=86_400))]


# --------------------------------------------------------------------------- #
# obs.retention.applied emission                                              #
# --------------------------------------------------------------------------- #


async def test_emits_retention_applied_with_accurate_counts() -> None:
    store = FakeRetentionStore(
        cursors={DEFAULT_RETENTION_PIPELINE_IDS[0]: 100, DEFAULT_RETENTION_PIPELINE_IDS[1]: 100}
    )
    store.audit_deleted = 5
    store.outbox_deleted = 3
    worker = _worker(store)

    await worker.sweep_once()

    assert len(store.appended) == 1
    event = store.appended[0]
    assert event.event_type == ObsEventName.RETENTION_APPLIED.value
    assert event.payload["audit_rows_deleted"] == 5
    assert event.payload["outbox_rows_deleted"] == 3


async def test_empty_sweep_emits_nothing() -> None:
    store = FakeRetentionStore(
        cursors={DEFAULT_RETENTION_PIPELINE_IDS[0]: 0, DEFAULT_RETENTION_PIPELINE_IDS[1]: 0}
    )
    store.audit_deleted = 0
    store.outbox_deleted = 0
    worker = _worker(store)

    result = await worker.sweep_once()

    assert result == RetentionResult(audit_rows_deleted=0, outbox_rows_deleted=0)
    assert result.deleted_anything is False
    assert store.appended == []


async def test_outbox_only_deletion_still_emits() -> None:
    store = FakeRetentionStore(
        cursors={DEFAULT_RETENTION_PIPELINE_IDS[0]: 100, DEFAULT_RETENTION_PIPELINE_IDS[1]: 100}
    )
    store.audit_deleted = 0
    store.outbox_deleted = 2
    worker = _worker(store)

    await worker.sweep_once()

    assert len(store.appended) == 1


async def test_custom_emit_event_is_used() -> None:
    store = FakeRetentionStore(
        cursors={DEFAULT_RETENTION_PIPELINE_IDS[0]: 100, DEFAULT_RETENTION_PIPELINE_IDS[1]: 100}
    )
    emitted: list[AuditEvent] = []

    async def emit(event: AuditEvent) -> None:
        emitted.append(event)

    worker = _worker(store, emit_event=emit)

    await worker.sweep_once()

    assert len(emitted) == 1
    assert store.appended == []  # default emit not used


async def test_emit_failure_is_swallowed() -> None:
    store = FakeRetentionStore(
        cursors={DEFAULT_RETENTION_PIPELINE_IDS[0]: 100, DEFAULT_RETENTION_PIPELINE_IDS[1]: 100}
    )

    async def emit(event: AuditEvent) -> None:
        raise RuntimeError("sink down")

    worker = _worker(store, emit_event=emit)

    # Must not propagate; sweep result is still returned.
    result = await worker.sweep_once()
    assert result.deleted_anything is True


async def test_emit_cancellation_propagates() -> None:
    store = FakeRetentionStore(
        cursors={DEFAULT_RETENTION_PIPELINE_IDS[0]: 100, DEFAULT_RETENTION_PIPELINE_IDS[1]: 100}
    )

    async def emit(event: AuditEvent) -> None:
        raise asyncio.CancelledError

    worker = _worker(store, emit_event=emit)

    with pytest.raises(asyncio.CancelledError):
        await worker.sweep_once()


# --------------------------------------------------------------------------- #
# Lifecycle                                                                    #
# --------------------------------------------------------------------------- #


async def test_run_sweeps_on_startup_then_on_interval() -> None:
    store = FakeRetentionStore(
        cursors={DEFAULT_RETENTION_PIPELINE_IDS[0]: 100, DEFAULT_RETENTION_PIPELINE_IDS[1]: 100}
    )
    worker = _worker(store, sweep_interval_s=0.01)

    worker.start()
    # Let the startup sweep plus at least one interval sweep run.
    await asyncio.sleep(0.05)
    await worker.stop()

    assert len(store.audit_cutoffs) >= 2


async def test_start_is_idempotent() -> None:
    store = FakeRetentionStore()
    worker = _worker(store, sweep_interval_s=10)

    worker.start()
    first = worker._task
    worker.start()
    assert worker._task is first

    await worker.stop()


async def test_stop_without_start_is_noop() -> None:
    worker = _worker(FakeRetentionStore())
    await worker.stop()  # must not raise


async def test_guarded_sweep_propagates_cancellation() -> None:
    class CancellingStore(FakeRetentionStore):
        async def delete_audit_events_before(self, cutoff: datetime) -> int:
            raise asyncio.CancelledError

    worker = _worker(CancellingStore())

    with pytest.raises(asyncio.CancelledError):
        await worker._sweep_guarded()


async def test_sweep_failure_does_not_kill_worker() -> None:
    class FlakyStore(FakeRetentionStore):
        def __init__(self) -> None:
            super().__init__(cursors={p: 0 for p in DEFAULT_RETENTION_PIPELINE_IDS})
            self.calls = 0

        async def delete_audit_events_before(self, cutoff: datetime) -> int:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("transient db error")
            return 0

    store = FlakyStore()
    worker = _worker(store, sweep_interval_s=0.01)

    worker.start()
    await asyncio.sleep(0.05)
    await worker.stop()

    # The first sweep raised, the worker survived and kept sweeping.
    assert store.calls >= 2

"""Tests for the Audit Pipeline (OBS-IMPL-006).

Drive :class:`AuditPipeline` against in-memory fakes: per-consumer cursor
commit, independent advance on partial failure, idempotent store writes,
edge-triggered ``obs.outbox.lagging`` emission, and cancellation propagation.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import cast

import pytest
from custos_spl import AuditEvent
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.metadata_store import (
    AuditOutboxBatch,
    AuditOutboxRow,
    MetadataStoreProvider,
)

from custos_obs.audit.pipeline import (
    AUDIT_ALERT_PIPELINE_ID,
    AUDIT_STORE_PIPELINE_ID,
    AuditConsumer,
    AuditPipeline,
    AuditStoreConsumer,
)
from custos_obs.events import ObsEventName


def _row(row_id: int) -> AuditOutboxRow:
    return AuditOutboxRow(
        id=row_id,
        workspace_id=WorkspaceId("ws"),
        event_id=f"event-{row_id}",
        event_type="custos.test.event",
        payload={},
        enqueued_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


def _batch(next_cursor: int, *row_ids: int) -> AuditOutboxBatch:
    return AuditOutboxBatch(rows=tuple(_row(i) for i in row_ids), next_cursor=next_cursor)


class FakeStore:
    """Records cursor commits and appended audit events."""

    def __init__(self) -> None:
        self.commit_log: list[tuple[str, int]] = []
        self.appended: list[AuditEvent] = []

    async def commit_audit_outbox_cursor(self, pipeline_id: str, cursor: int) -> None:
        self.commit_log.append((pipeline_id, cursor))

    async def append_audit(self, workspace_id: WorkspaceId, event: AuditEvent) -> None:
        self.appended.append(event)


class RecordingHandler:
    """A consumer handler that records the batches it accepts."""

    def __init__(self) -> None:
        self.batches: list[AuditOutboxBatch] = []

    async def __call__(self, batch: AuditOutboxBatch) -> None:
        self.batches.append(batch)


class FailingHandler:
    """A consumer handler that always raises."""

    async def __call__(self, batch: AuditOutboxBatch) -> None:
        raise RuntimeError("consumer boom")


class ScriptedHandler:
    """A consumer handler that fails while ``fail`` is set, else records."""

    def __init__(self) -> None:
        self.fail = False
        self.batches: list[AuditOutboxBatch] = []

    async def __call__(self, batch: AuditOutboxBatch) -> None:
        if self.fail:
            raise RuntimeError("scripted failure")
        self.batches.append(batch)


class CancellingHandler:
    """A consumer handler that raises :class:`asyncio.CancelledError`."""

    async def __call__(self, batch: AuditOutboxBatch) -> None:
        raise asyncio.CancelledError


class DedupWriter:
    """Idempotent row writer: ignores an already-seen ``event_id``."""

    def __init__(self) -> None:
        self.written: list[str] = []

    async def __call__(self, row: AuditOutboxRow) -> None:
        if row.event_id in self.written:
            return
        self.written.append(row.event_id)


def _pipeline(
    store: FakeStore,
    consumers: list[AuditConsumer],
    *,
    lag_threshold: int = 1_000,
    emit_event: Callable[[AuditEvent], Awaitable[None]] | None = None,
    start_cursors: dict[str, int] | None = None,
) -> AuditPipeline:
    return AuditPipeline(
        store=cast(MetadataStoreProvider, store),
        consumers=consumers,
        lag_threshold=lag_threshold,
        emit_event=emit_event,
        start_cursors=start_cursors,
    )


# --- construction ------------------------------------------------------------


def test_requires_at_least_one_consumer() -> None:
    with pytest.raises(ValueError, match="at least one consumer"):
        _pipeline(FakeStore(), [])


def test_rejects_duplicate_pipeline_ids() -> None:
    handler = RecordingHandler()
    consumers = [AuditConsumer("dup", handler), AuditConsumer("dup", handler)]
    with pytest.raises(ValueError, match="duplicate pipeline ids"):
        _pipeline(FakeStore(), consumers)


def test_start_cursors_seed_committed() -> None:
    store = FakeStore()
    pipeline = _pipeline(
        store,
        [AuditConsumer("a", RecordingHandler()), AuditConsumer("b", RecordingHandler())],
        start_cursors={"a": 3},
    )
    assert dict(pipeline.committed_cursors) == {"a": 3, "b": 0}
    assert pipeline.min_committed_cursor() == 0


# --- dispatch / cursors ------------------------------------------------------


async def test_dispatch_commits_each_consumer_cursor() -> None:
    store = FakeStore()
    store_h, alert_h = RecordingHandler(), RecordingHandler()
    pipeline = _pipeline(
        store,
        [
            AuditConsumer(AUDIT_STORE_PIPELINE_ID, store_h),
            AuditConsumer(AUDIT_ALERT_PIPELINE_ID, alert_h),
        ],
    )

    await pipeline.dispatch(_batch(5, 1, 2, 3))

    assert dict(pipeline.committed_cursors) == {
        AUDIT_STORE_PIPELINE_ID: 5,
        AUDIT_ALERT_PIPELINE_ID: 5,
    }
    assert (AUDIT_STORE_PIPELINE_ID, 5) in store.commit_log
    assert (AUDIT_ALERT_PIPELINE_ID, 5) in store.commit_log
    assert len(store_h.batches) == 1
    assert len(alert_h.batches) == 1


async def test_call_delegates_to_dispatch() -> None:
    store = FakeStore()
    handler = RecordingHandler()
    pipeline = _pipeline(store, [AuditConsumer("a", handler)])

    await pipeline(_batch(7, 7))

    assert pipeline.committed_cursors["a"] == 7
    assert len(handler.batches) == 1


async def test_consumers_advance_independently_on_partial_failure() -> None:
    store = FakeStore()
    store_h = RecordingHandler()
    pipeline = _pipeline(
        store,
        [
            AuditConsumer(AUDIT_STORE_PIPELINE_ID, store_h),
            AuditConsumer(AUDIT_ALERT_PIPELINE_ID, FailingHandler()),
        ],
        lag_threshold=1_000,
    )

    # No exception bubbles up even though the alerter raised.
    await pipeline.dispatch(_batch(9, 1))

    assert pipeline.committed_cursors[AUDIT_STORE_PIPELINE_ID] == 9
    assert pipeline.committed_cursors[AUDIT_ALERT_PIPELINE_ID] == 0
    assert store.commit_log == [(AUDIT_STORE_PIPELINE_ID, 9)]
    assert len(store_h.batches) == 1


# --- idempotent store writes -------------------------------------------------


async def test_audit_store_consumer_dedups_redelivery_by_event_id() -> None:
    store = FakeStore()
    writer = DedupWriter()
    consumer = AuditConsumer(AUDIT_STORE_PIPELINE_ID, AuditStoreConsumer(writer))
    pipeline = _pipeline(store, [consumer])

    batch = _batch(3, 1, 2, 3)
    await pipeline.dispatch(batch)
    # At-least-once: the same batch is re-streamed and re-dispatched.
    await pipeline.dispatch(batch)

    assert writer.written == ["event-1", "event-2", "event-3"]
    assert pipeline.committed_cursors[AUDIT_STORE_PIPELINE_ID] == 3


# --- lag signal --------------------------------------------------------------


async def test_lag_below_threshold_emits_nothing() -> None:
    store = FakeStore()
    pipeline = _pipeline(store, [AuditConsumer("a", RecordingHandler())], lag_threshold=10)

    await pipeline.dispatch(_batch(5, 1))

    assert store.appended == []


async def test_lag_crossing_threshold_emits_once_per_pipeline() -> None:
    store = FakeStore()
    pipeline = _pipeline(
        store,
        [
            AuditConsumer(AUDIT_STORE_PIPELINE_ID, RecordingHandler()),
            AuditConsumer(AUDIT_ALERT_PIPELINE_ID, FailingHandler()),
        ],
        lag_threshold=2,
    )

    # Store keeps up (lag 0); alerter falls behind (lag 5 > 2) -> one emit.
    await pipeline.dispatch(_batch(5, 1))
    # Alerter still behind (lag 8 > 2) but already flagged -> no second emit.
    await pipeline.dispatch(_batch(8, 1))

    assert len(store.appended) == 1
    event = store.appended[0]
    assert event.event_type == ObsEventName.OUTBOX_LAGGING.value
    assert event.subject["pipeline_id"] == AUDIT_ALERT_PIPELINE_ID
    assert event.payload["lag_rows"] == 5
    assert event.payload["threshold_rows"] == 2


async def test_lag_recovers_then_recrosses_emits_again() -> None:
    store = FakeStore()
    scripted = ScriptedHandler()
    pipeline = _pipeline(store, [AuditConsumer("a", scripted)], lag_threshold=2)

    scripted.fail = True
    await pipeline.dispatch(_batch(5))  # lag 5 -> emit (1)
    scripted.fail = False
    await pipeline.dispatch(_batch(6))  # commits 6 -> lag 0 -> reset
    scripted.fail = True
    await pipeline.dispatch(_batch(10))  # lag 4 -> emit (2)

    assert len(store.appended) == 2


async def test_emit_failure_is_swallowed() -> None:
    store = FakeStore()

    async def boom(event: AuditEvent) -> None:
        raise RuntimeError("sink down")

    pipeline = _pipeline(
        store,
        [AuditConsumer("a", FailingHandler())],
        lag_threshold=1,
        emit_event=boom,
    )

    # The emit raising must not break dispatch.
    await pipeline.dispatch(_batch(5, 1))

    assert pipeline.committed_cursors["a"] == 0


async def test_default_emit_writes_lagging_event_via_append_audit() -> None:
    store = FakeStore()
    pipeline = _pipeline(store, [AuditConsumer("a", FailingHandler())], lag_threshold=1)

    await pipeline.dispatch(_batch(5, 1))

    assert len(store.appended) == 1
    assert store.appended[0].event_type == ObsEventName.OUTBOX_LAGGING.value


# --- cancellation ------------------------------------------------------------


async def test_consumer_cancellation_propagates() -> None:
    store = FakeStore()
    pipeline = _pipeline(store, [AuditConsumer("a", CancellingHandler())])

    with pytest.raises(asyncio.CancelledError):
        await pipeline.dispatch(_batch(5, 1))


async def test_emit_cancellation_propagates() -> None:
    store = FakeStore()

    async def cancel(event: AuditEvent) -> None:
        raise asyncio.CancelledError

    pipeline = _pipeline(
        store,
        [AuditConsumer("a", FailingHandler())],
        lag_threshold=1,
        emit_event=cancel,
    )

    with pytest.raises(asyncio.CancelledError):
        await pipeline.dispatch(_batch(5, 1))

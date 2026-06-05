"""Tests for the Audit Outbox Drainer (OBS-IMPL-005).

Drive the drainer against in-memory fake stores: cursor advance, multi-page
catch-up, crash-before-commit re-stream, poll-mode draining, listen-mode
draining on notify, and listen-unsupported fallback to polling.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import cast

import pytest
from custos_spl.errors import QueryUnsupported
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.metadata_store import (
    AuditOutboxBatch,
    AuditOutboxRow,
    MetadataStoreProvider,
    NotifyEvent,
)

from custos_obs.audit import AuditOutboxDrainer
from custos_obs.audit.drainer import (
    DEFAULT_AUDIT_OUTBOX_BATCH_SIZE,
    AuditOutboxBatchHandler,
)


def _row(row_id: int) -> AuditOutboxRow:
    return AuditOutboxRow(
        id=row_id,
        workspace_id=WorkspaceId("ws"),
        event_id=f"event-{row_id}",
        event_type="custos.test.event",
        payload={},
        enqueued_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


class FakeOutboxStore:
    """Minimal stand-in exposing only the outbox-drain methods the drainer uses."""

    def __init__(self, row_ids: list[int]) -> None:
        self.rows = [_row(i) for i in sorted(row_ids)]
        self.stream_calls = 0

    async def stream_audit_outbox(self, cursor: int, batch_size: int) -> AuditOutboxBatch:
        self.stream_calls += 1
        page = [r for r in self.rows if r.id > cursor][:batch_size]
        if not page:
            return AuditOutboxBatch(rows=(), next_cursor=cursor)
        return AuditOutboxBatch(rows=tuple(page), next_cursor=page[-1].id)


class ListenOutboxStore(FakeOutboxStore):
    """Fake whose ``listen_audit_outbox`` yields queued notifications."""

    def __init__(self, row_ids: list[int]) -> None:
        super().__init__(row_ids)
        self.notifications: asyncio.Queue[int | None] = asyncio.Queue()

    def listen_audit_outbox(self) -> AsyncIterator[NotifyEvent]:
        async def _gen() -> AsyncIterator[NotifyEvent]:
            while True:
                item = await self.notifications.get()
                if item is None:
                    return
                yield NotifyEvent(cursor=item)

        return _gen()


class UnsupportedListenStore(FakeOutboxStore):
    """Fake that refuses LISTEN/NOTIFY, like the asyncpg metadata adapter."""

    def listen_audit_outbox(self) -> AsyncIterator[NotifyEvent]:
        raise QueryUnsupported("listen_audit_outbox not implemented by this adapter")


class RecordingHandler:
    def __init__(self) -> None:
        self.batches: list[AuditOutboxBatch] = []
        self.drained_event = asyncio.Event()

    async def __call__(self, batch: AuditOutboxBatch) -> None:
        self.batches.append(batch)
        self.drained_event.set()


def _drainer(
    store: object,
    handler: object,
    *,
    mode: str = "poll",
    poll_interval_s: float = 0.01,
    batch_size: int = DEFAULT_AUDIT_OUTBOX_BATCH_SIZE,
    start_cursor: int = 0,
) -> AuditOutboxDrainer:
    return AuditOutboxDrainer(
        store=cast(MetadataStoreProvider, store),
        handler=cast(AuditOutboxBatchHandler, handler),
        mode=mode,
        poll_interval_s=poll_interval_s,
        batch_size=batch_size,
        start_cursor=start_cursor,
    )


# --- drain_once --------------------------------------------------------------


async def test_drain_once_advances_cursor_over_a_single_page() -> None:
    store = FakeOutboxStore([1, 2, 3])
    handler = RecordingHandler()
    drainer = _drainer(store, handler)

    drained = await drainer.drain_once()

    assert drained == 3
    assert drainer.cursor == 3
    assert [r.id for r in handler.batches[0].rows] == [1, 2, 3]


async def test_drain_once_pages_through_a_backlog_then_stops_on_short_page() -> None:
    store = FakeOutboxStore([1, 2, 3])
    handler = RecordingHandler()
    drainer = _drainer(store, handler, batch_size=2)

    drained = await drainer.drain_once()

    assert drained == 3
    assert drainer.cursor == 3
    # Two full-ish pages: [1,2] (full -> keep going), [3] (short -> stop).
    assert [[r.id for r in b.rows] for b in handler.batches] == [[1, 2], [3]]


async def test_drain_once_pages_through_exact_multiple_then_reads_empty() -> None:
    store = FakeOutboxStore([1, 2, 3, 4])
    handler = RecordingHandler()
    drainer = _drainer(store, handler, batch_size=2)

    drained = await drainer.drain_once()

    assert drained == 4
    assert drainer.cursor == 4
    # [1,2] full, [3,4] full, then an empty read terminates the loop.
    assert [[r.id for r in b.rows] for b in handler.batches] == [[1, 2], [3, 4]]


async def test_drain_once_is_noop_on_empty_outbox() -> None:
    store = FakeOutboxStore([])
    handler = RecordingHandler()
    drainer = _drainer(store, handler)

    drained = await drainer.drain_once()

    assert drained == 0
    assert drainer.cursor == 0
    assert handler.batches == []


async def test_start_cursor_skips_already_committed_rows() -> None:
    store = FakeOutboxStore([1, 2, 3, 4])
    handler = RecordingHandler()
    drainer = _drainer(store, handler, start_cursor=2)

    drained = await drainer.drain_once()

    assert drained == 2
    assert [r.id for r in handler.batches[0].rows] == [3, 4]


# --- crash-before-commit -----------------------------------------------------


async def test_handler_failure_leaves_cursor_unchanged_and_restreams() -> None:
    store = FakeOutboxStore([1, 2])
    calls = {"n": 0}

    async def flaky(batch: AuditOutboxBatch) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("crash mid-batch")

    drainer = _drainer(store, flaky)

    with pytest.raises(RuntimeError, match="crash mid-batch"):
        await drainer.drain_once()
    # Cursor unchanged because the handler never accepted the batch.
    assert drainer.cursor == 0

    # On the next cycle the same batch is re-streamed and now succeeds.
    drained = await drainer.drain_once()
    assert drained == 2
    assert drainer.cursor == 2


# --- run(): poll mode --------------------------------------------------------


async def test_poll_mode_drains_initial_backlog_then_keeps_polling() -> None:
    store = FakeOutboxStore([1, 2])
    handler = RecordingHandler()
    drainer = _drainer(store, handler, mode="poll", poll_interval_s=0.01)

    drainer.start()
    try:
        await asyncio.wait_for(handler.drained_event.wait(), timeout=2)
    finally:
        await drainer.stop()

    assert drainer.cursor == 2
    assert [r.id for r in handler.batches[0].rows] == [1, 2]


# --- run(): listen mode ------------------------------------------------------


async def test_listen_mode_drains_on_notification() -> None:
    store = ListenOutboxStore([])
    handler = RecordingHandler()
    drainer = _drainer(store, handler, mode="listen")

    drainer.start()
    try:
        # Append a row, then notify; the drainer should pick it up.
        store.rows.append(_row(1))
        await store.notifications.put(1)
        await asyncio.wait_for(handler.drained_event.wait(), timeout=2)
    finally:
        await drainer.stop()

    assert drainer.cursor == 1
    assert any(r.id == 1 for b in handler.batches for r in b.rows)


async def test_listen_unsupported_falls_back_to_polling() -> None:
    store = UnsupportedListenStore([1, 2])
    handler = RecordingHandler()
    drainer = _drainer(store, handler, mode="listen", poll_interval_s=0.01)

    drainer.start()
    try:
        await asyncio.wait_for(handler.drained_event.wait(), timeout=2)
    finally:
        await drainer.stop()

    assert drainer.cursor == 2


async def test_listen_stream_ending_falls_through_to_polling() -> None:
    store = ListenOutboxStore([])
    handler = RecordingHandler()
    drainer = _drainer(store, handler, mode="listen", poll_interval_s=0.01)

    drainer.start()
    try:
        # Let the startup catch-up drain run against an empty outbox, then end
        # the listen generator. A row that arrives only afterwards must still be
        # drained by the poll loop -- forward progress cannot depend on LISTEN.
        await asyncio.sleep(0)
        await store.notifications.put(None)
        store.rows.append(_row(1))
        await asyncio.wait_for(handler.drained_event.wait(), timeout=2)
    finally:
        await drainer.stop()

    assert drainer.cursor == 1


# --- guarded loop ------------------------------------------------------------


async def test_poll_loop_survives_a_failing_cycle() -> None:
    store = FakeOutboxStore([1])
    attempts = {"n": 0}
    handler = RecordingHandler()

    async def flaky(batch: AuditOutboxBatch) -> None:
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("transient")
        await handler(batch)

    drainer = _drainer(store, flaky, mode="poll", poll_interval_s=0.01)

    drainer.start()
    try:
        await asyncio.wait_for(handler.drained_event.wait(), timeout=2)
    finally:
        await drainer.stop()

    assert attempts["n"] >= 2
    assert drainer.cursor == 1


async def test_guarded_drain_propagates_cancellation() -> None:
    class CancellingStore(FakeOutboxStore):
        async def stream_audit_outbox(self, cursor: int, batch_size: int) -> AuditOutboxBatch:
            raise asyncio.CancelledError

    drainer = _drainer(CancellingStore([]), RecordingHandler())

    # Cancellation must bubble out of the guarded cycle so shutdown stays prompt
    # rather than being swallowed as a "transient" error.
    with pytest.raises(asyncio.CancelledError):
        await drainer._drain_guarded()


# --- start/stop lifecycle ----------------------------------------------------


async def test_start_is_idempotent_and_stop_without_start_is_noop() -> None:
    store = FakeOutboxStore([])
    handler = RecordingHandler()
    drainer = _drainer(store, handler, poll_interval_s=0.01)

    # stop() before any start() must be a clean no-op.
    await drainer.stop()

    drainer.start()
    first = drainer._task
    drainer.start()  # second start must not replace the running task
    assert drainer._task is first

    await drainer.stop()
    assert drainer._task is None

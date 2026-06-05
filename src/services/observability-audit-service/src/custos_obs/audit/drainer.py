"""Audit Outbox Drainer (OBS-IMPL-005).

A lifespan-managed background task that drains the SPL audit outbox and hands
each batch to a downstream handler (the audit pipeline, wired in OBS-IMPL-006).

Drain model — polling is the load-bearing baseline; LISTEN/NOTIFY is an optional
low-latency optimisation layered on top:

* ``poll`` mode (``CUSTOS_AUDIT_OUTBOX_DRAIN_MODE=poll``): the drainer streams
  the outbox every ``CUSTOS_AUDIT_OUTBOX_POLL_INTERVAL_S`` seconds.
* ``listen`` mode: the drainer subscribes to
  :meth:`MetadataStoreProvider.listen_audit_outbox` and drains on each
  notification. The SPL contract allows that method to be unsupported (it raises
  :class:`~custos_spl.errors.QueryUnsupported`); when it does, the drainer
  transparently falls back to polling. **Forward progress never depends on
  LISTEN/NOTIFY being available.**

Delivery is at-least-once. The in-memory read cursor advances to a batch's
``next_cursor`` only *after* the handler accepts it, so a crash (or handler
error) mid-batch leaves the cursor unchanged and the batch is re-streamed on the
next cycle — downstream de-duplicates on ``event_id`` (OBS-IMPL-006). The
``start_cursor`` lets the pipeline resume from its committed high-water mark;
re-streaming from ``0`` is always safe because of that dedup.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Protocol

from custos_spl.errors import QueryUnsupported

if TYPE_CHECKING:
    from custos_spl.interfaces.metadata_store import (
        AuditOutboxBatch,
        MetadataStoreProvider,
    )

logger = logging.getLogger("custos_obs.audit.drainer")

#: Default page size for ``stream_audit_outbox`` reads (per the design's
#: ``batchSize=500``). A full page triggers an immediate follow-up read so a
#: backlog is caught up within a single drain cycle rather than one page per
#: poll tick.
DEFAULT_AUDIT_OUTBOX_BATCH_SIZE = 500


class AuditOutboxBatchHandler(Protocol):
    """Consumes a drained batch.

    The handler owns durability: it must fully persist (or otherwise accept
    responsibility for) the batch before returning. Raising signals the drainer
    to leave its cursor unchanged so the batch is re-streamed.
    """

    async def __call__(self, batch: AuditOutboxBatch) -> None: ...


class AuditOutboxDrainer:
    """Drains the SPL audit outbox into a handler on notify or on an interval.

    The drainer is single-consumer: it tracks one in-memory read cursor and
    hands every batch to ``handler``. Per-pipeline cursor commit and fan-out to
    multiple consumers are the pipeline's concern (OBS-IMPL-006); this class
    only guarantees ordered, at-least-once delivery with forward progress that
    does not depend on LISTEN/NOTIFY.
    """

    def __init__(
        self,
        *,
        store: MetadataStoreProvider,
        handler: AuditOutboxBatchHandler,
        mode: str,
        poll_interval_s: float,
        batch_size: int = DEFAULT_AUDIT_OUTBOX_BATCH_SIZE,
        start_cursor: int = 0,
    ) -> None:
        self._store = store
        self._handler = handler
        self._mode = mode
        self._poll_interval_s = poll_interval_s
        self._batch_size = batch_size
        self._cursor = start_cursor
        self._task: asyncio.Task[None] | None = None

    @property
    def cursor(self) -> int:
        """The current in-memory read high-water mark."""
        return self._cursor

    async def drain_once(self) -> int:
        """Stream and dispatch every available row, returning the count drained.

        Pages through ``stream_audit_outbox`` from the current cursor until a
        short or empty page signals the backlog is exhausted. The cursor is
        advanced only after ``handler`` accepts each page, so a handler error
        propagates with the cursor unchanged (the batch re-streams next cycle).
        """
        drained = 0
        while True:
            batch = await self._store.stream_audit_outbox(self._cursor, self._batch_size)
            if not batch.rows:
                return drained
            await self._handler(batch)
            self._cursor = batch.next_cursor
            drained += len(batch.rows)
            if len(batch.rows) < self._batch_size:
                return drained

    async def run(self) -> None:
        """Drain forever until cancelled.

        Performs an initial catch-up drain (so a backlog accumulated while the
        service was down is cleared on startup regardless of mode), then either
        subscribes to notifications (``listen`` mode, when supported) or polls.
        Falls back to polling if ``listen`` is requested but unsupported.
        """
        await self._drain_guarded()
        if self._mode == "listen":
            await self._run_listen()
        await self._poll_loop()

    async def _run_listen(self) -> None:
        """Drain on notifications until the listen stream ends.

        Returns when the adapter does not support LISTEN/NOTIFY or the
        notification stream ends, so the caller falls back to polling and
        forward progress never depends on LISTEN/NOTIFY. Re-raises
        :class:`asyncio.CancelledError` for clean shutdown.
        """
        try:
            notifications = self._store.listen_audit_outbox()
            async for _notify in notifications:
                await self._drain_guarded()
        except QueryUnsupported:
            logger.info(
                "listen_audit_outbox unsupported by the metadata store; "
                "falling back to polling every %ss",
                self._poll_interval_s,
            )
            return
        except asyncio.CancelledError:
            logger.info("audit outbox drainer stopping (listen)")
            raise
        logger.info(
            "listen_audit_outbox stream ended; falling back to polling every %ss",
            self._poll_interval_s,
        )

    async def _poll_loop(self) -> None:
        """Drain on a fixed interval until cancelled."""
        logger.info("audit outbox drainer polling every %ss", self._poll_interval_s)
        while True:
            try:
                await asyncio.sleep(self._poll_interval_s)
            except asyncio.CancelledError:
                logger.info("audit outbox drainer stopping (poll)")
                raise
            await self._drain_guarded()

    async def _drain_guarded(self) -> None:
        """Run :meth:`drain_once`, surviving transient store/handler errors.

        A failed cycle leaves the cursor unchanged and is retried on the next
        notify/poll; a misbehaving backend must not silently disable the drain.
        :class:`asyncio.CancelledError` propagates so shutdown stays prompt.
        """
        try:
            drained = await self.drain_once()
            if drained:
                logger.info("audit outbox drained %d row(s) up to cursor %d", drained, self._cursor)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("audit outbox drain cycle failed; will retry")

    def start(self) -> None:
        """Launch the drain loop as a background task (idempotent)."""
        if self._task is None:
            self._task = asyncio.create_task(self.run(), name="audit-outbox-drainer")

    async def stop(self) -> None:
        """Cancel and await the drain loop (idempotent)."""
        task = self._task
        if task is None:
            return
        self._task = None
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


__all__ = [
    "DEFAULT_AUDIT_OUTBOX_BATCH_SIZE",
    "AuditOutboxBatchHandler",
    "AuditOutboxDrainer",
]

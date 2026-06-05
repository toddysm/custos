"""Audit Pipeline (OBS-IMPL-006).

The pipeline is the drainer's batch handler (it satisfies the
:class:`~custos_obs.audit.drainer.AuditOutboxBatchHandler` protocol). Each
drained batch is fanned out to a set of independent, named **consumers**
(``audit-store``, ``audit-alert``, …). Every consumer:

* processes the whole batch via its own handler, and
* commits its own high-water mark via
  :meth:`MetadataStoreProvider.commit_audit_outbox_cursor`,

so a slow or failing alerter cannot block — or roll back — the store writer.
Consumers run concurrently; a failure in one is logged and isolated, and only
the consumers that succeeded advance their cursor.

Delivery is at-least-once (the drainer re-streams a batch whose handler raised).
De-duplication is keyed on ``event_id``: the store consumer writes through an
idempotent seam (the ``custos_audit.events`` ``event_id`` primary key), so a
redelivered row is a no-op rather than a duplicate.

Lag is observed per consumer as ``head_cursor - committed_cursor`` (rows behind
the outbox head). When a consumer's lag crosses ``lag_threshold`` the pipeline
emits :class:`~custos_obs.events.OutboxLagging` once (edge-triggered, reset when
it recovers) so operators are alerted to the stuck pipeline without the lag
check ever blocking the writers.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Protocol

from custos_obs.events import OutboxLagging

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence

    from custos_spl import AuditEvent
    from custos_spl.interfaces.metadata_store import (
        AuditOutboxBatch,
        AuditOutboxRow,
        MetadataStoreProvider,
    )

    from custos_obs.audit.drainer import AuditOutboxBatchHandler

logger = logging.getLogger("custos_obs.audit.pipeline")

#: Pipeline id for the durable audit-store consumer.
AUDIT_STORE_PIPELINE_ID = "audit-store"

#: Pipeline id for the alerting consumer.
AUDIT_ALERT_PIPELINE_ID = "audit-alert"


class AuditOutboxRowWriter(Protocol):
    """Idempotent single-row sink for the audit-store consumer.

    Implementations must be idempotent on ``row.event_id`` (the durable
    ``custos_audit.events`` primary key) so an at-least-once redelivery writes
    no duplicate.
    """

    async def __call__(self, row: AuditOutboxRow) -> None: ...


@dataclass(frozen=True, slots=True)
class AuditConsumer:
    """A named downstream consumer of drained audit-outbox batches.

    ``pipeline_id`` is the durable cursor key; ``handler`` does the work. The
    pipeline commits the cursor for this id only after ``handler`` returns.
    """

    pipeline_id: str
    handler: AuditOutboxBatchHandler


class AuditStoreConsumer:
    """``audit-store`` consumer: persist each row through an idempotent writer.

    Satisfies :class:`~custos_obs.audit.drainer.AuditOutboxBatchHandler`. The
    write seam owns idempotency on ``event_id``; this consumer simply drives it
    once per row, preserving outbox order.
    """

    def __init__(self, writer: AuditOutboxRowWriter) -> None:
        self._writer = writer

    async def __call__(self, batch: AuditOutboxBatch) -> None:
        for row in batch.rows:
            await self._writer(row)


class AuditPipeline:
    """Fan a drained batch out to independent, per-cursor consumers.

    Implements :class:`~custos_obs.audit.drainer.AuditOutboxBatchHandler`, so an
    instance is handed straight to the drainer. Construct it with the SPL
    metadata store and the consumers to drive; optionally override the lag
    threshold and the operational-event sink (defaults to writing
    ``obs.outbox.lagging`` back through ``store.append_audit``).
    """

    def __init__(
        self,
        *,
        store: MetadataStoreProvider,
        consumers: Sequence[AuditConsumer],
        lag_threshold: int,
        emit_event: Callable[[AuditEvent], Awaitable[None]] | None = None,
        start_cursors: Mapping[str, int] | None = None,
    ) -> None:
        if not consumers:
            raise ValueError("AuditPipeline requires at least one consumer")
        pipeline_ids = [c.pipeline_id for c in consumers]
        if len(set(pipeline_ids)) != len(pipeline_ids):
            raise ValueError(f"AuditPipeline consumers have duplicate pipeline ids: {pipeline_ids}")
        self._store = store
        self._consumers = tuple(consumers)
        self._lag_threshold = lag_threshold
        self._emit_event = emit_event if emit_event is not None else self._default_emit
        starts = dict(start_cursors or {})
        self._committed: dict[str, int] = {pid: starts.get(pid, 0) for pid in pipeline_ids}
        self._lagging: dict[str, bool] = {pid: False for pid in pipeline_ids}

    @property
    def committed_cursors(self) -> Mapping[str, int]:
        """A read-only snapshot of each consumer's committed high-water mark."""
        return MappingProxyType(dict(self._committed))

    def min_committed_cursor(self) -> int:
        """The lowest committed cursor across all consumers.

        The drainer should resume from this on startup so the slowest consumer
        never misses a row (faster consumers re-receive already-committed rows
        and de-duplicate on ``event_id``).
        """
        return min(self._committed.values())

    async def __call__(self, batch: AuditOutboxBatch) -> None:
        await self.dispatch(batch)

    async def dispatch(self, batch: AuditOutboxBatch) -> None:
        """Run every consumer for ``batch`` and surface lag.

        Consumers run concurrently and independently: a failure in one is
        logged and isolated (its cursor stays put) while the others still
        commit. The lag check runs after, never blocking the writers.
        """
        results = await asyncio.gather(
            *(self._run_consumer(consumer, batch) for consumer in self._consumers),
            return_exceptions=True,
        )
        for consumer, result in zip(self._consumers, results, strict=True):
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, BaseException):
                logger.error(
                    "audit pipeline consumer %r failed at cursor %d; leaving its cursor unchanged",
                    consumer.pipeline_id,
                    batch.next_cursor,
                    exc_info=result,
                )
        await self._check_lag(batch.next_cursor)

    async def _run_consumer(self, consumer: AuditConsumer, batch: AuditOutboxBatch) -> None:
        await consumer.handler(batch)
        await self._store.commit_audit_outbox_cursor(consumer.pipeline_id, batch.next_cursor)
        self._committed[consumer.pipeline_id] = batch.next_cursor

    async def _check_lag(self, head_cursor: int) -> None:
        for pipeline_id, committed in self._committed.items():
            lag = head_cursor - committed
            if lag > self._lag_threshold:
                if not self._lagging[pipeline_id]:
                    self._lagging[pipeline_id] = True
                    await self._emit_lagging(pipeline_id, lag)
            else:
                self._lagging[pipeline_id] = False

    async def _emit_lagging(self, pipeline_id: str, lag: int) -> None:
        logger.warning(
            "audit pipeline %r is lagging by %d row(s) (threshold %d)",
            pipeline_id,
            lag,
            self._lag_threshold,
        )
        event = OutboxLagging(
            pipeline_id=pipeline_id,
            lag_rows=lag,
            threshold_rows=self._lag_threshold,
        ).to_audit_event()
        try:
            await self._emit_event(event)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("failed to emit obs.outbox.lagging for pipeline %r", pipeline_id)

    async def _default_emit(self, event: AuditEvent) -> None:
        await self._store.append_audit(event.workspace_id, event)


__all__ = [
    "AUDIT_ALERT_PIPELINE_ID",
    "AUDIT_STORE_PIPELINE_ID",
    "AuditConsumer",
    "AuditOutboxRowWriter",
    "AuditPipeline",
    "AuditStoreConsumer",
]

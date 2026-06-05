"""External Exporter Loader (OBS-IMPL-011).

The loader is the *runtime* around the pure merge algebra in
:mod:`custos_obs.exporters.merge`. It watches the customer exporter ConfigMap
(``custos-otel-exporters``), merges each observed block into the base Collector
config, writes the resulting **effective** Collector ConfigMap, and signals the
OTel Collector to reload — rolling back to the last-good config when a customer
block is invalid (design TODO-002).

Rollback contract: a malformed customer block is *rejected* by
:class:`~custos_obs.exporters.merge.CollectorConfigMerger` (it keeps the
last-good effective config and captures the reason), so the loader never writes
a bad config and the running Collector is left untouched. Only a successful
merge that changes the effective config triggers a write + reload; an unchanged
merge is a no-op so a duplicate ConfigMap event cannot churn the Collector.

Events: a successful (changing) merge emits ``obs.exporter.config.applied``
carrying the customer exporter names; a rejected merge emits
``obs.exporter.config.rejected`` carrying the rejection reason.

Kubernetes wiring is **out of scope** here: the ConfigMap watch, the effective
ConfigMap write, and the reload signal are injected as narrow
:class:`ExporterConfigSource`, :class:`CollectorConfigWriter`, and
:class:`CollectorReloadSignaller` seams, mirroring the injected-store pattern the
rest of the service uses. The Kubernetes-backed implementations are wired at the
deployment boundary (a later phase), keeping this module pure and testable.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from custos_obs.events import ExporterConfigApplied, ExporterConfigRejected
from custos_obs.exporters.merge import CollectorConfigMerger

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
    from typing import Any

    from custos_spl import AuditEvent

logger = logging.getLogger("custos_obs.exporters.loader")


class ExporterConfigSource(Protocol):
    """Yields the customer exporter block each time the ConfigMap changes.

    Implementations watch the ``custos-otel-exporters`` ConfigMap and yield its
    current contents (YAML text, a parsed mapping, or ``None`` when the ConfigMap
    is absent/empty) on every change, including the initial value. The iterator
    completes only when the watch is shut down.
    """

    def watch(self) -> AsyncIterator[str | Mapping[str, Any] | None]: ...


class CollectorConfigWriter(Protocol):
    """Persists the effective Collector config (the merged ConfigMap)."""

    async def write(self, effective_config: str) -> None: ...


class CollectorReloadSignaller(Protocol):
    """Signals the OTel Collector to reload its (just-written) config."""

    async def signal_reload(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ReconcileOutcome:
    """The result of reconciling a single observed customer exporter block."""

    effective_config: str
    exporter_names: tuple[str, ...]
    applied: bool
    reloaded: bool
    rejection_reason: str | None

    @property
    def rejected(self) -> bool:
        """Whether the customer block was rejected (running config untouched)."""
        return not self.applied


class ExporterLoader:
    """Watches the exporter ConfigMap and reconciles the effective Collector config.

    Constructed from the base Collector config (which seeds the merger's
    last-good); :meth:`reconcile` merges one observed customer block, and
    :meth:`run` drives :meth:`reconcile` over the injected
    :class:`ExporterConfigSource` until cancelled. :meth:`start` / :meth:`stop`
    provide the lifespan-managed background-task lifecycle.
    """

    def __init__(
        self,
        *,
        base_config: str | Mapping[str, Any],
        source: ExporterConfigSource,
        writer: CollectorConfigWriter,
        signaller: CollectorReloadSignaller,
        emit_event: Callable[[AuditEvent], Awaitable[None]],
        exporters_configmap: str,
    ) -> None:
        self._merger = CollectorConfigMerger(base_config=base_config)
        self._source = source
        self._writer = writer
        self._signaller = signaller
        self._emit_event = emit_event
        self._configmap = exporters_configmap
        self._last_written: str | None = None
        self._task: asyncio.Task[None] | None = None

    @property
    def last_written(self) -> str | None:
        """The effective config most recently written to the Collector ConfigMap."""
        return self._last_written

    async def reconcile(self, customer: str | Mapping[str, Any] | None) -> ReconcileOutcome:
        """Merge one customer block; write + reload on a successful change.

        A rejected merge emits ``obs.exporter.config.rejected`` and leaves the
        running config untouched. A successful merge that changes the effective
        config writes the new ConfigMap, signals a reload, and emits
        ``obs.exporter.config.applied``; an unchanged merge is a no-op.
        """
        outcome = self._merger.apply(customer)
        if outcome.rejected:
            reason = outcome.rejection_reason or "unknown error"
            logger.warning("rejected exporter ConfigMap %s: %s", self._configmap, reason)
            await self._emit_rejected(reason)
            return ReconcileOutcome(
                effective_config=outcome.effective_config,
                exporter_names=(),
                applied=False,
                reloaded=False,
                rejection_reason=reason,
            )

        if outcome.effective_config == self._last_written:
            return ReconcileOutcome(
                effective_config=outcome.effective_config,
                exporter_names=outcome.exporter_names,
                applied=True,
                reloaded=False,
                rejection_reason=None,
            )

        await self._writer.write(outcome.effective_config)
        await self._signaller.signal_reload()
        self._last_written = outcome.effective_config
        logger.info(
            "applied exporter ConfigMap %s with exporter(s): %s",
            self._configmap,
            ", ".join(outcome.exporter_names) or "(none)",
        )
        await self._emit_applied(outcome.exporter_names)
        return ReconcileOutcome(
            effective_config=outcome.effective_config,
            exporter_names=outcome.exporter_names,
            applied=True,
            reloaded=True,
            rejection_reason=None,
        )

    async def run(self) -> None:
        """Reconcile every observed customer block until the watch ends/cancels."""
        async for customer in self._source.watch():
            await self._reconcile_guarded(customer)

    async def _reconcile_guarded(self, customer: str | Mapping[str, Any] | None) -> None:
        """Reconcile one block, surviving transient write/reload/emit errors.

        A merge rejection is handled inside :meth:`reconcile` (it is normal
        operation). A failure to *write* or *signal reload* a good config is a
        transient infrastructure error: it is logged and retried on the next
        observed block rather than tearing down the watch. Cancellation
        propagates so shutdown stays prompt.
        """
        try:
            await self.reconcile(customer)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("failed to reconcile exporter ConfigMap %s", self._configmap)

    async def _emit_applied(self, exporter_names: tuple[str, ...]) -> None:
        await self._emit(
            ExporterConfigApplied(
                configmap=self._configmap,
                exporter_names=exporter_names,
            ).to_audit_event()
        )

    async def _emit_rejected(self, reason: str) -> None:
        await self._emit(
            ExporterConfigRejected(
                configmap=self._configmap,
                reason=reason,
            ).to_audit_event()
        )

    async def _emit(self, event: AuditEvent) -> None:
        """Emit an operational audit event best-effort.

        Emitting must never abort or mask a reconcile: a failed emit is logged
        and swallowed, while cancellation propagates.
        """
        try:
            await self._emit_event(event)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "failed to emit %s for ConfigMap %s", event.event_type, self._configmap
            )

    def start(self) -> None:
        """Start the watch/reconcile task (idempotent)."""
        if self._task is not None:
            return
        self._task = asyncio.create_task(self.run(), name="exporter-loader")

    async def stop(self) -> None:
        """Cancel and await the watch/reconcile task (idempotent)."""
        task = self._task
        if task is None:
            return
        self._task = None
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


__all__ = [
    "CollectorConfigWriter",
    "CollectorReloadSignaller",
    "ExporterConfigSource",
    "ExporterLoader",
    "ReconcileOutcome",
]

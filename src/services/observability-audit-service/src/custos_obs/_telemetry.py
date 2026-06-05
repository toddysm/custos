"""OTel self-instrumentation (dogfooding) — OBS-IMPL-015.

The service watches every other component, so it must also answer "is
observability itself working?". This module provides:

* a single ``custos_obs`` :func:`tracer <get_tracer>` + :func:`meter <get_meter>`
  obtained through the OpenTelemetry **API**, plus a :func:`span` helper for the
  drain / dispatch / merge / query paths;
* a :class:`ServiceMetrics` registry of the operational gauges + counters that
  answer the question above — drainer lag, retention last-run, exporter-config
  status, and alert-dispatch outcomes — exposed on ``GET /metrics`` in the
  Prometheus text exposition format.

**No-op without an SDK.** Only ``opentelemetry-api`` is a runtime dependency.
With no ``MeterProvider`` / ``TracerProvider`` configured, :func:`get_tracer` and
:func:`get_meter` return the API's no-op implementations, the OTel instruments
created by :class:`ServiceMetrics` do nothing, and :func:`span` is a transparent
pass-through. The in-process :class:`ServiceMetrics` state — and therefore the
``/metrics`` exposition — is maintained independently of any SDK, so the
operational metrics are always queryable while OTLP export lights up only when a
collector pipeline is wired in.

**Riding the emit seam.** Every background worker already hands its operational
``obs.*`` events to an injected ``emit`` callback. :func:`instrument_emit` wraps
that callback so :meth:`ServiceMetrics.observe_event` updates the matching metric
before forwarding — no worker internals change.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Iterable, Iterator
from contextlib import contextmanager
from threading import Lock
from typing import TYPE_CHECKING

from opentelemetry import metrics, trace
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.trace import Status, StatusCode

from custos_obs._version import __version__
from custos_obs.events import ObsEventName

if TYPE_CHECKING:
    from custos_spl import AuditEvent
    from fastapi import APIRouter
    from opentelemetry.metrics import Meter
    from opentelemetry.trace import Span, Tracer

logger = logging.getLogger("custos_obs.telemetry")

#: Instrumentation scope name for the service's tracer + meter.
INSTRUMENTATION_NAME = "custos_obs"

#: Content type for the Prometheus text exposition format (version 0.0.4).
PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

#: Alert-dispatch outcome labels.
ALERT_DISPATCHED = "dispatched"
ALERT_FAILED = "failed"

#: Exporter-config status gauge values.
_STATUS_APPLIED = 1
_STATUS_REJECTED = 0

EmitCallback = Callable[["AuditEvent"], Awaitable[None]]

__all__ = [
    "ALERT_DISPATCHED",
    "ALERT_FAILED",
    "INSTRUMENTATION_NAME",
    "PROMETHEUS_CONTENT_TYPE",
    "ServiceMetrics",
    "build_metrics_router",
    "get_meter",
    "get_tracer",
    "instrument_emit",
    "span",
]


def get_tracer() -> Tracer:
    """Return the ``custos_obs`` tracer (no-op until a ``TracerProvider`` exists)."""
    return trace.get_tracer(INSTRUMENTATION_NAME, __version__)


def get_meter() -> Meter:
    """Return the ``custos_obs`` meter (no-op until a ``MeterProvider`` exists)."""
    return metrics.get_meter(INSTRUMENTATION_NAME, __version__)


@contextmanager
def span(name: str, **attributes: str | bool | int | float) -> Iterator[Span]:
    """Open a ``custos_obs`` span around a drain / dispatch / merge / query path.

    Records any raised :class:`Exception` on the span and marks it ``ERROR``
    before re-raising. ``BaseException`` subclasses that signal control flow
    rather than failure — notably :class:`asyncio.CancelledError` during
    shutdown/timeouts — propagate untouched and are never recorded as errors.
    A transparent pass-through when no ``TracerProvider`` is set.
    """
    tracer = get_tracer()
    with tracer.start_as_current_span(name) as current:
        for key, value in attributes.items():
            current.set_attribute(key, value)
        try:
            yield current
        except Exception as exc:
            current.record_exception(exc)
            current.set_status(Status(StatusCode.ERROR, str(exc)))
            raise


def _escape_label_value(value: str) -> str:
    """Escape a label value per the Prometheus text exposition format."""
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _render_labels(labels: dict[str, str]) -> str:
    """Render a sorted ``{k="v",...}`` label block (empty string when no labels)."""
    if not labels:
        return ""
    inner = ",".join(
        f'{key}="{_escape_label_value(value)}"' for key, value in sorted(labels.items())
    )
    return f"{{{inner}}}"


class ServiceMetrics:
    """In-process operational metrics for the service, exposed on ``/metrics``.

    Holds the authoritative gauge + counter state rendered in the Prometheus
    text format, and mirrors every update onto OpenTelemetry instruments so the
    same values flow to OTLP when a collector pipeline is configured (and are
    no-ops otherwise). All mutators are guarded by a lock so the background
    workers and the ``/metrics`` request handler can touch the state from
    different tasks/threads safely.
    """

    def __init__(self, *, meter: Meter | None = None) -> None:
        self._lock = Lock()
        # Authoritative in-process state for the Prometheus exposition.
        self._drainer_lag: dict[str, int] = {}
        self._retention_last_run: float | None = None
        self._retention_rows: dict[str, int] = {"audit": 0, "outbox": 0}
        self._exporter_status: dict[str, int] = {}
        self._exporter_changes: dict[tuple[str, str], int] = {}
        self._alert_dispatch: dict[tuple[str, str, str], int] = {}

        # OTel mirror. Gauges are observable (read state on collection); counters
        # are additive and incremented inline. No-ops until an SDK is configured.
        active_meter = meter if meter is not None else get_meter()
        active_meter.create_observable_gauge(
            "custos_obs.audit.outbox.lag",
            callbacks=[self._observe_lag],
            unit="{row}",
            description="Audit outbox drain lag in rows, per pipeline.",
        )
        active_meter.create_observable_gauge(
            "custos_obs.audit.retention.last_run",
            callbacks=[self._observe_retention_last_run],
            unit="s",
            description="Unix timestamp of the retention worker's last sweep.",
        )
        active_meter.create_observable_gauge(
            "custos_obs.exporter.config.status",
            callbacks=[self._observe_exporter_status],
            description="Exporter config status per ConfigMap (1 applied, 0 rejected).",
        )
        self._alert_counter = active_meter.create_counter(
            "custos_obs.alert.dispatch",
            unit="{alert}",
            description="Alert dispatch outcomes by sink + rule.",
        )
        self._retention_rows_counter = active_meter.create_counter(
            "custos_obs.audit.retention.rows_deleted",
            unit="{row}",
            description="Rows deleted by the retention worker, by kind.",
        )
        self._exporter_changes_counter = active_meter.create_counter(
            "custos_obs.exporter.config.changes",
            unit="{change}",
            description="Exporter config apply/reject transitions, by ConfigMap.",
        )

    # ----- OTel observable-gauge callbacks -----

    def _observe_lag(self, _options: CallbackOptions) -> Iterable[Observation]:
        with self._lock:
            return [
                Observation(lag, {"pipeline_id": pipeline_id})
                for pipeline_id, lag in self._drainer_lag.items()
            ]

    def _observe_retention_last_run(self, _options: CallbackOptions) -> Iterable[Observation]:
        with self._lock:
            if self._retention_last_run is None:
                return []
            return [Observation(self._retention_last_run)]

    def _observe_exporter_status(self, _options: CallbackOptions) -> Iterable[Observation]:
        with self._lock:
            return [
                Observation(status, {"configmap": configmap})
                for configmap, status in self._exporter_status.items()
            ]

    # ----- Mutators -----

    def set_drainer_lag(self, pipeline_id: str, lag_rows: int) -> None:
        """Record the current drain lag (in rows) for a pipeline."""
        with self._lock:
            self._drainer_lag[pipeline_id] = lag_rows

    def mark_retention_run(
        self,
        *,
        occurred_at_epoch: float,
        audit_rows_deleted: int = 0,
        outbox_rows_deleted: int = 0,
    ) -> None:
        """Record a retention sweep: its timestamp and the rows it deleted."""
        with self._lock:
            self._retention_last_run = occurred_at_epoch
            self._retention_rows["audit"] += audit_rows_deleted
            self._retention_rows["outbox"] += outbox_rows_deleted
        if audit_rows_deleted:
            self._retention_rows_counter.add(audit_rows_deleted, {"kind": "audit"})
        if outbox_rows_deleted:
            self._retention_rows_counter.add(outbox_rows_deleted, {"kind": "outbox"})

    def set_exporter_status(self, configmap: str, *, applied: bool) -> None:
        """Record the latest exporter-config outcome for a ConfigMap."""
        outcome = "applied" if applied else "rejected"
        with self._lock:
            self._exporter_status[configmap] = _STATUS_APPLIED if applied else _STATUS_REJECTED
            key = (configmap, outcome)
            self._exporter_changes[key] = self._exporter_changes.get(key, 0) + 1
        self._exporter_changes_counter.add(1, {"configmap": configmap, "outcome": outcome})

    def record_alert(self, *, outcome: str, sink: str, rule: str) -> None:
        """Increment the alert-dispatch counter for an outcome/sink/rule."""
        key = (outcome, sink, rule)
        with self._lock:
            self._alert_dispatch[key] = self._alert_dispatch.get(key, 0) + 1
        self._alert_counter.add(1, {"outcome": outcome, "sink": sink, "rule": rule})

    def observe_event(self, event: AuditEvent) -> None:
        """Update the matching metric from one operational ``obs.*`` event.

        Events from other components (which also flow through the audit
        pipeline) carry non-``obs.*`` types and are ignored.
        """
        payload = event.payload
        match event.event_type:
            case ObsEventName.OUTBOX_LAGGING.value:
                self.set_drainer_lag(str(payload["pipeline_id"]), int(payload["lag_rows"]))
            case ObsEventName.RETENTION_APPLIED.value:
                self.mark_retention_run(
                    occurred_at_epoch=event.occurred_at.timestamp(),
                    audit_rows_deleted=int(payload["audit_rows_deleted"]),
                    outbox_rows_deleted=int(payload["outbox_rows_deleted"]),
                )
            case ObsEventName.EXPORTER_CONFIG_APPLIED.value:
                self.set_exporter_status(str(payload["configmap"]), applied=True)
            case ObsEventName.EXPORTER_CONFIG_REJECTED.value:
                self.set_exporter_status(str(payload["configmap"]), applied=False)
            case ObsEventName.ALERT_DISPATCHED.value:
                self.record_alert(
                    outcome=ALERT_DISPATCHED,
                    sink=str(payload["sink"]),
                    rule=str(payload["rule_name"]),
                )
            case ObsEventName.ALERT_FAILED.value:
                self.record_alert(
                    outcome=ALERT_FAILED,
                    sink=str(payload["sink"]),
                    rule=str(payload["rule_name"]),
                )
            case _:
                return

    # ----- Exposition -----

    def render(self) -> str:
        """Render the current metrics in the Prometheus text exposition format."""
        with self._lock:
            lag = dict(self._drainer_lag)
            last_run = self._retention_last_run
            retention_rows = dict(self._retention_rows)
            exporter_status = dict(self._exporter_status)
            exporter_changes = dict(self._exporter_changes)
            alert_dispatch = dict(self._alert_dispatch)

        lines: list[str] = []

        lines.append("# HELP custos_obs_audit_outbox_lag_rows Audit outbox drain lag in rows.")
        lines.append("# TYPE custos_obs_audit_outbox_lag_rows gauge")
        for pipeline_id, value in sorted(lag.items()):
            labels = _render_labels({"pipeline_id": pipeline_id})
            lines.append(f"custos_obs_audit_outbox_lag_rows{labels} {value}")

        lines.append(
            "# HELP custos_obs_audit_retention_last_run_timestamp_seconds "
            "Unix timestamp of the retention worker's last sweep."
        )
        lines.append("# TYPE custos_obs_audit_retention_last_run_timestamp_seconds gauge")
        if last_run is not None:
            lines.append(f"custos_obs_audit_retention_last_run_timestamp_seconds {last_run}")

        lines.append(
            "# HELP custos_obs_audit_retention_rows_deleted_total "
            "Rows deleted by the retention worker, by kind."
        )
        lines.append("# TYPE custos_obs_audit_retention_rows_deleted_total counter")
        for kind, value in sorted(retention_rows.items()):
            labels = _render_labels({"kind": kind})
            lines.append(f"custos_obs_audit_retention_rows_deleted_total{labels} {value}")

        lines.append(
            "# HELP custos_obs_exporter_config_status "
            "Exporter config status per ConfigMap (1 applied, 0 rejected)."
        )
        lines.append("# TYPE custos_obs_exporter_config_status gauge")
        for configmap, value in sorted(exporter_status.items()):
            labels = _render_labels({"configmap": configmap})
            lines.append(f"custos_obs_exporter_config_status{labels} {value}")

        lines.append(
            "# HELP custos_obs_exporter_config_changes_total "
            "Exporter config apply/reject transitions."
        )
        lines.append("# TYPE custos_obs_exporter_config_changes_total counter")
        for (configmap, outcome), value in sorted(exporter_changes.items()):
            labels = _render_labels({"configmap": configmap, "outcome": outcome})
            lines.append(f"custos_obs_exporter_config_changes_total{labels} {value}")

        lines.append(
            "# HELP custos_obs_alert_dispatch_total Alert dispatch outcomes by sink + rule."
        )
        lines.append("# TYPE custos_obs_alert_dispatch_total counter")
        for (outcome, sink, rule), value in sorted(alert_dispatch.items()):
            labels = _render_labels({"outcome": outcome, "rule": rule, "sink": sink})
            lines.append(f"custos_obs_alert_dispatch_total{labels} {value}")

        return "\n".join(lines) + "\n"


def instrument_emit(emit: EmitCallback, metrics: ServiceMetrics) -> EmitCallback:
    """Wrap an ``emit`` callback so each event also updates :class:`ServiceMetrics`.

    The metric update happens before forwarding, but the durable write stays
    authoritative: a failure inside :meth:`ServiceMetrics.observe_event` (e.g. a
    malformed payload) is logged and swallowed so ``emit`` — which owns
    persistence to the SPL audit outbox — always runs.
    """

    async def _emit(event: AuditEvent) -> None:
        try:
            metrics.observe_event(event)
        except Exception:
            logger.exception(
                "failed to record metrics for audit event %s (type=%s); "
                "forwarding to the durable write regardless",
                event.event_id,
                event.event_type,
            )
        await emit(event)

    return _emit


def build_metrics_router(metrics: ServiceMetrics) -> APIRouter:
    """Build a router serving ``GET /metrics`` from a :class:`ServiceMetrics`."""
    from fastapi import APIRouter
    from starlette.responses import PlainTextResponse

    router = APIRouter()

    @router.get("/metrics", include_in_schema=False)
    async def metrics_endpoint() -> PlainTextResponse:
        return PlainTextResponse(metrics.render(), media_type=PROMETHEUS_CONTENT_TYPE)

    return router

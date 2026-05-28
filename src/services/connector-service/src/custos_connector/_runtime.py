"""SDK wiring for in-process OpenTelemetry + Prometheus ``/metrics``.

CONN-IMPL-029 (Phase K). The connector-service exposes its own
``/metrics`` endpoint as part of the Phase K acceptance criteria. The
service-level metric definitions live in
:mod:`custos_connector._telemetry`; this module installs the OTel SDK
providers and mounts the ``/metrics`` ASGI app on the FastAPI router.

Production wiring
-----------------

* :class:`~opentelemetry.sdk.metrics.MeterProvider` is configured with a
  :class:`~opentelemetry.exporter.prometheus.PrometheusMetricReader`,
  which registers every meter with the global
  ``prometheus_client`` registry that backs the ``/metrics`` endpoint.
* :class:`~opentelemetry.sdk.trace.TracerProvider` is wired with an
  OTLP HTTP/gRPC span exporter when ``OTEL_EXPORTER_OTLP_ENDPOINT`` (or
  the trace-specific ``OTEL_EXPORTER_OTLP_TRACES_ENDPOINT``) is set in
  the process environment, sending spans to the Helm-managed Collector
  sidecar via a :class:`~opentelemetry.sdk.trace.export.BatchSpanProcessor`.
  When neither variable is set (development / unit-test mode) the
  provider is installed without a span processor; spans are still ended
  in-process so the helpers in :mod:`custos_connector._telemetry`
  behave correctly, but nothing is exported off-host.

Idempotency
-----------

:func:`install_otel_providers` is safe to call multiple times in the
same process. It sets the global provider exactly once; subsequent
calls are no-ops. Tests that want to inject in-memory exporters bypass
this module and configure their own provider directly via
:func:`opentelemetry.trace.set_tracer_provider` /
:func:`opentelemetry.metrics.set_meter_provider` before importing the
service.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Final

from opentelemetry import metrics, trace
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import SERVICE_NAME, SERVICE_VERSION, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from custos_connector._telemetry import _INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION

logger = logging.getLogger("custos_connector.runtime")

_INIT_LOCK: Final[threading.Lock] = threading.Lock()
_INITIALIZED: bool = False


def _maybe_build_otlp_span_processor() -> BatchSpanProcessor | None:
    """Build an OTLP :class:`BatchSpanProcessor` from environment vars.

    Returns ``None`` when no OTLP endpoint is configured. The OTel SDK
    does NOT automatically wire an OTLP exporter from
    ``OTEL_EXPORTER_OTLP_*`` env vars — those variables only control
    the *configuration* of an explicitly-constructed exporter. This
    helper inspects the standard env vars and constructs the matching
    exporter so spans actually reach the collector sidecar.

    The function is import-tolerant: when the
    ``opentelemetry-exporter-otlp-proto-http`` /
    ``opentelemetry-exporter-otlp-proto-grpc`` packages are not
    installed (development setups), it returns ``None`` and logs a
    warning, leaving the tracer provider exporter-less. Production
    images carry one of the exporter packages.
    """
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") or os.environ.get(
        "OTEL_EXPORTER_OTLP_ENDPOINT"
    )
    if not endpoint:
        return None
    protocol = (
        os.environ.get("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
        or os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL")
        or "http/protobuf"
    ).lower()
    try:
        if protocol.startswith("grpc"):
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                OTLPSpanExporter as _GrpcExporter,
            )

            return BatchSpanProcessor(_GrpcExporter())
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter as _HttpExporter,
        )
    except ImportError:
        logger.warning(
            "OTEL_EXPORTER_OTLP_ENDPOINT=%r set but no matching OTLP exporter "
            "package is installed; tracing will not be exported",
            endpoint,
        )
        return None
    return BatchSpanProcessor(_HttpExporter())


def install_otel_providers() -> None:
    """Install process-wide OTel SDK providers.

    Idempotent: the first call wins, subsequent calls are no-ops. When
    ``OTEL_EXPORTER_OTLP_ENDPOINT`` (or the trace-specific variant) is
    set in the environment, an OTLP :class:`BatchSpanProcessor` is
    attached to the tracer provider so spans actually leave the
    process. Without the env var, the tracer provider has no span
    processor — spans are still ended in-process (the helpers in
    :mod:`custos_connector._telemetry` rely on that) but nothing is
    exported off-host.
    """
    global _INITIALIZED
    if _INITIALIZED:
        return
    with _INIT_LOCK:
        if _INITIALIZED:
            return

        resource = Resource.create(
            {
                SERVICE_NAME: _INSTRUMENTATION_NAME,
                SERVICE_VERSION: _INSTRUMENTATION_VERSION,
            }
        )

        # ----- Tracer provider -----
        tracer_provider = TracerProvider(resource=resource)
        span_processor = _maybe_build_otlp_span_processor()
        if span_processor is not None:
            tracer_provider.add_span_processor(span_processor)
            logger.info("OTLP span exporter wired from environment")
        trace.set_tracer_provider(tracer_provider)

        # ----- Meter provider -----
        # :class:`PrometheusMetricReader` registers each metric with the
        # default ``prometheus_client`` registry; the ASGI app mounted
        # by ``create_app`` at ``/metrics`` then exposes them.
        reader = PrometheusMetricReader()
        meter_provider = MeterProvider(
            resource=resource,
            metric_readers=[reader],
        )
        metrics.set_meter_provider(meter_provider)

        _INITIALIZED = True
        logger.info(
            "OTel providers installed (service=%s version=%s)",
            _INSTRUMENTATION_NAME,
            _INSTRUMENTATION_VERSION,
        )


__all__ = ["install_otel_providers"]

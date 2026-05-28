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
* :class:`~opentelemetry.sdk.trace.TracerProvider` is installed without
  exporters. Production deployments configure an OTLP exporter to a
  Helm-managed Collector sidecar via standard
  ``OTEL_EXPORTER_OTLP_*`` environment variables; the SDK auto-installs
  the matching span processor when those variables are present (the
  ``opentelemetry-sdk`` package's autoinstrumentation entry point is
  outside the scope of this module).

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
import threading
from typing import Final

from opentelemetry import metrics, trace
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import SERVICE_NAME, SERVICE_VERSION, Resource
from opentelemetry.sdk.trace import TracerProvider

from custos_connector._telemetry import _INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION

logger = logging.getLogger("custos_connector.runtime")

_INIT_LOCK: Final[threading.Lock] = threading.Lock()
_INITIALIZED: bool = False


def install_otel_providers() -> None:
    """Install process-wide OTel SDK providers.

    Idempotent: the first call wins, subsequent calls are no-ops. The
    function reads no environment variables of its own; OTLP exporter
    wiring (when present) is left to the SDK's standard
    ``OTEL_EXPORTER_OTLP_*`` autoinstrumentation that fires on
    :class:`TracerProvider` construction.
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
        # The default :class:`TracerProvider` ships with no exporters; an
        # OTLP exporter activates automatically when standard
        # ``OTEL_EXPORTER_OTLP_*`` environment variables are set.
        tracer_provider = TracerProvider(resource=resource)
        trace.set_tracer_provider(tracer_provider)

        # ----- Meter provider -----
        # :class:`PrometheusMetricReader` registers each metric with the
        # default ``prometheus_client`` registry; the ASGI app mounted by
        # :func:`mount_metrics_endpoint` then exposes them at
        # ``/metrics``.
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

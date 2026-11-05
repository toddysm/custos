"""Own a plaintext metrics-only listener separately from the private HTTPS application."""

from collections.abc import Iterator
from contextlib import contextmanager

from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.view import View
from opentelemetry.sdk.resources import Resource
from prometheus_client import start_http_server


@contextmanager
def metrics_listener(
    *,
    enabled: bool,
    port: int = 8090,
    host: str = "127.0.0.1",
) -> Iterator[int | None]:
    """Expose operational metrics only; keep arbitrary activity error codes out of labels.

    Called once by the process entry point. Disabled mode preserves legacy
    startup. The listener and provider close after application/worker shutdown.
    Port zero is available to isolated tests; production validates its configured port.
    """
    if not enabled:
        yield None
        return
    reader = PrometheusMetricReader()
    provider = MeterProvider(
        resource=Resource.create({"service.name": "custos_arm"}),
        metric_readers=[reader],
        shutdown_on_exit=False,
        views=[View(instrument_name="custos_arm_attempts_total", attribute_keys={"class"})],
    )
    metrics.set_meter_provider(provider)
    try:
        server, thread = start_http_server(port, addr=host)
        try:
            yield server.server_port
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
    finally:
        provider.shutdown()

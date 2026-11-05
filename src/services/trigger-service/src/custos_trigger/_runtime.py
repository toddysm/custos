"""Own production receiver metrics/trace exporters at the process boundary."""

import os
from collections.abc import Iterator
from contextlib import contextmanager
from urllib.parse import urlsplit

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


@contextmanager
def receiver_telemetry(*, enabled: bool) -> Iterator[None]:
    """Export bounded receiver instruments over OTLP/HTTP and close providers after server drain.

    Disabled mode changes no global providers. Enabled mode requires an explicit
    credential-free collector origin; exporter queues and timeouts are bounded.
    """
    if not enabled:
        yield
        return
    endpoint = os.environ.get("TRIGGER_TELEMETRY_ENDPOINT", "").rstrip("/")
    url = urlsplit(endpoint)
    if (
        url.scheme not in {"http", "https"}
        or not url.hostname
        or url.username
        or url.password
        or url.query
        or url.fragment
        or url.path
    ):
        raise ValueError("receiver telemetry requires a credential-free collector origin")
    resource = Resource.create({"service.name": "custos_trigger"})
    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=endpoint + "/v1/metrics", timeout=5),
        export_interval_millis=10000,
        export_timeout_millis=5000,
    )
    meter = MeterProvider(resource=resource, metric_readers=[reader], shutdown_on_exit=False)
    tracer = TracerProvider(resource=resource, shutdown_on_exit=False)
    tracer.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=endpoint + "/v1/traces", timeout=5),
            max_queue_size=512,
            max_export_batch_size=128,
            export_timeout_millis=5000,
        )
    )
    metrics.set_meter_provider(meter)
    trace.set_tracer_provider(tracer)
    try:
        yield
    finally:
        try:
            tracer.shutdown()
        finally:
            meter.shutdown(timeout_millis=5000)

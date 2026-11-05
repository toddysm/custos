"""Verify that production receiver instruments actually reach bounded OTLP/HTTP exporters."""

import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

from custos_trigger._runtime import receiver_telemetry


@pytest.mark.parametrize(
    "endpoint",
    ["", "ftp://collector", "http://user:token@collector", "https://collector?token=private"],
)
def test_invalid_collector_origin_fails_before_startup(
    monkeypatch: pytest.MonkeyPatch,
    endpoint: str,
) -> None:
    """Activated telemetry cannot silently choose no exporter or expose endpoint credentials."""
    monkeypatch.setenv("TRIGGER_TELEMETRY_ENDPOINT", endpoint)
    with pytest.raises(ValueError, match="credential-free"), receiver_telemetry(enabled=True):
        pytest.fail("invalid collector was accepted")


def test_disabled_telemetry_does_not_require_a_collector(monkeypatch: pytest.MonkeyPatch) -> None:
    """Legacy profile startup leaves global instrumentation under its existing owner."""
    monkeypatch.delenv("TRIGGER_TELEMETRY_ENDPOINT", raising=False)
    with receiver_telemetry(enabled=False):
        pass


def test_actual_receiver_metrics_and_traces_export_from_process() -> None:
    """Use a fresh process and real HTTP/protobuf exporters, replacing only the collector."""
    received: list[tuple[str, bytes]] = []

    class Collector(BaseHTTPRequestHandler):
        """Accept synthetic telemetry without logging identifiers or request headers."""

        def do_POST(self) -> None:
            """Record actual protobuf transport bytes and acknowledge them without a retry."""
            received.append((self.path, self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            """Keep HTTP request logging out of production-data redaction assertions."""

    with HTTPServer(("127.0.0.1", 0), Collector) as collector:
        thread = threading.Thread(target=collector.serve_forever)
        thread.start()
        try:
            env = {key: value for key, value in os.environ.items() if not key.startswith("OTEL_")}
            env["TRIGGER_TELEMETRY_ENDPOINT"] = f"http://127.0.0.1:{collector.server_port}"
            env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
            child = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "import asyncio\n"
                    "from custos_trigger._runtime import receiver_telemetry\n"
                    "with receiver_telemetry(enabled=True):\n"
                    "    from custos_trigger.receiver_runtime import "
                    "_passes, _checkpoints, _expired, _discovered, _pruned\n"
                    "    from custos_trigger._telemetry import TelemetryAuditSink\n"
                    "    _passes.add(1, {'receiver': 'retention', 'outcome': 'success'})\n"
                    "    _checkpoints.add(1, {'receiver': 'delivery', 'outcome': 'success'})\n"
                    "    _expired.add(1, {'receiver': 'delivery'})\n"
                    "    _discovered.add(51)\n"
                    "    _pruned.add(1)\n"
                    "    asyncio.run(TelemetryAuditSink().emit('trigger.dispatched', "
                    "workspace_id='test-workspace', attributes={}))\n",
                ],
                env=env,
                capture_output=True,
                text=True,
                timeout=15,
            )
            assert child.returncode == 0, child.stderr
        finally:
            collector.shutdown()
            thread.join()
    metrics = [
        ExportMetricsServiceRequest.FromString(body)
        for path, body in received
        if path == "/v1/metrics"
    ]
    traces = [
        ExportTraceServiceRequest.FromString(body)
        for path, body in received
        if path == "/v1/traces"
    ]
    assert metrics and traces
    points = [
        point
        for batch in metrics
        for resource in batch.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
        if metric.name == "custos_trigger_receiver_passes_total"
        for point in metric.sum.data_points
    ]
    assert points and {entry.key for point in points for entry in point.attributes} == {
        "receiver",
        "outcome",
    }
    exported = {
        metric.name
        for batch in metrics
        for resource in batch.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
    }
    assert {
        "custos_trigger_receiver_checkpoints_total",
        "custos_trigger_receiver_claim_expirations_total",
        "custos_trigger_receiver_discovered_total",
        "custos_trigger_receiver_progress_pruned_total",
    } <= exported
    assert any(
        scope.spans
        for batch in traces
        for resource in batch.resource_spans
        for scope in resource.scope_spans
    )

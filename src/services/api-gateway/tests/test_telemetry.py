"""Telemetry tests for the gateway request pipeline (AGW-IMPL-018).

Bootstraps the OpenTelemetry SDK with in-memory exporters *before* importing any
module that transitively imports :mod:`custos_gateway._telemetry`, then drives a
fully wired :func:`create_app` through ``TestClient`` and asserts the per-request
span attributes plus every counter and histogram the design pins are recorded
(``gateway_requests_total``, ``gateway_rate_limit_denials_total``,
``gateway_idempotency_replays_total``, ``gateway_request_duration_seconds``,
``gateway_downstream_duration_seconds``).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

# --- SDK bootstrap (must run before importing the telemetry-bound modules) ----
_SPAN_EXPORTER = InMemorySpanExporter()
_TRACE_PROVIDER = TracerProvider()
_TRACE_PROVIDER.add_span_processor(SimpleSpanProcessor(_SPAN_EXPORTER))
trace.set_tracer_provider(_TRACE_PROVIDER)

_METRIC_READER = InMemoryMetricReader()
_METER_PROVIDER = MeterProvider(metric_readers=[_METRIC_READER])
metrics.set_meter_provider(_METER_PROVIDER)

from custos_spl import (  # noqa: E402
    ExistingCompleted,
    IdempotencyRecord,
    IdemReserved,
    PrincipalId,
    ReserveIdempotencyResult,
    WorkspaceId,
)
from fastapi.testclient import TestClient  # noqa: E402

from custos_gateway import _telemetry as telemetry  # noqa: E402
from custos_gateway.app import create_app  # noqa: E402
from custos_gateway.clients.auth import DeclaredPermission, FakeAuthServiceClient  # noqa: E402
from custos_gateway.middleware.ratelimit import BucketConfig, RateLimiter  # noqa: E402
from custos_gateway.router import DownstreamResponse, DownstreamRouter  # noqa: E402
from custos_gateway.routes._forwarding import response_snapshot  # noqa: E402
from custos_gateway.routes.registry import registry_required_permissions  # noqa: E402
from custos_gateway.settings import Settings  # noqa: E402

WRITE_PATH = "/v1/workspaces/ws-1/runs"
#: The FastAPI route template the registry mounts (the ``http.route`` value).
WRITE_ROUTE = "/v1/workspaces/{workspaceId}/runs"
AUTH_HEADERS = {"authorization": "Bearer token-123"}


def _auth_client() -> FakeAuthServiceClient:
    return FakeAuthServiceClient(
        permissions=[
            DeclaredPermission(name=name, description=name, declared_by="test")
            for name in registry_required_permissions()
        ]
    )


def _recording_router() -> tuple[DownstreamRouter, list[httpx.Request]]:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(201, headers={"x-downstream": "yes"}, content=b"created")

    router = DownstreamRouter(
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        host="127.0.0.1",
        http_port=3500,
    )
    return router, calls


def _record(status: str = "in_progress") -> IdempotencyRecord:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return IdempotencyRecord(
        workspace_id=WorkspaceId("ws-1"),
        principal_id=PrincipalId("principal-fake"),
        route=WRITE_PATH,
        idempotency_key="key-1",
        request_hash="hash",
        status=status,
        response_snapshot=None,
        reserved_at=now,
        expires_at=now,
    )


@dataclass(slots=True)
class _FakeStore:
    outcome: ReserveIdempotencyResult = field(
        default_factory=lambda: IdemReserved(record=_record())
    )
    reserve_calls: list[tuple[str, str, str, str, str, int]] = field(default_factory=list)
    complete_calls: list[Mapping[str, Any]] = field(default_factory=list)

    async def reserve_idempotency_record(
        self,
        workspace_id: WorkspaceId,
        principal_id: PrincipalId,
        route: str,
        idempotency_key: str,
        request_hash: str,
        ttl_seconds: int,
    ) -> ReserveIdempotencyResult:
        self.reserve_calls.append(
            (workspace_id, principal_id, route, idempotency_key, request_hash, ttl_seconds)
        )
        return self.outcome

    async def complete_idempotency_record(
        self,
        workspace_id: WorkspaceId,
        principal_id: PrincipalId,
        route: str,
        idempotency_key: str,
        response_snapshot: Mapping[str, Any],
    ) -> IdempotencyRecord:
        self.complete_calls.append(response_snapshot)
        return _record(status="completed")


def _data_points(name: str) -> list[Any]:
    """All recorded data points for the metric ``name`` across the SDK reader."""
    data = _METRIC_READER.get_metrics_data()
    points: list[Any] = []
    if data is None:
        return points
    for resource_metric in data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                if metric.name == name:
                    points.extend(metric.data.data_points)
    return points


def _counter_total(name: str, **attributes: str) -> float:
    """Sum the values of the matching counter data points."""
    total = 0.0
    for point in _data_points(name):
        if all(point.attributes.get(key) == value for key, value in attributes.items()):
            total += point.value
    return total


def _histogram_count(name: str) -> int:
    """Sum the observation counts across the histogram's data points."""
    return sum(point.count for point in _data_points(name))


def _spans(name: str) -> Iterable[Any]:
    return [span for span in _SPAN_EXPORTER.get_finished_spans() if span.name == name]


def _span_for_correlation(correlation_id: str) -> Any:
    """Return the request span whose ``correlationId`` matches ``correlation_id``.

    Selecting by the response's correlation id (rather than positionally) keeps
    the assertion robust to other requests in the suite emitting request spans.
    """
    for span in _spans(telemetry.SPAN_NAME):
        if span.attributes.get(telemetry.ATTR_CORRELATION_ID) == correlation_id:
            return span
    raise AssertionError(f"no custos_gateway.request span for correlation id {correlation_id!r}")


def test_write_request_records_span_and_request_metrics(settings: Settings) -> None:
    auth = _auth_client()
    router, calls = _recording_router()
    app = create_app(
        settings=settings,
        auth_client=auth,
        downstream_router=router,
        metadata_store=_FakeStore(),
    )

    with TestClient(app) as client:
        response = client.post(
            WRITE_PATH,
            content=b'{"name":"demo"}',
            headers={**AUTH_HEADERS, "content-type": "application/json", "idempotency-key": "k-1"},
        )

    assert response.status_code == 201
    assert len(calls) == 1

    # The per-request span — selected by this response's correlation id — carries
    # the full design attribute set.
    correlation_id = response.headers["x-correlation-id"]
    attrs = _span_for_correlation(correlation_id).attributes
    assert attrs[telemetry.ATTR_HTTP_METHOD] == "POST"
    assert attrs[telemetry.ATTR_HTTP_ROUTE] == WRITE_ROUTE
    assert attrs[telemetry.ATTR_WORKSPACE_ID] == "ws-1"
    assert attrs[telemetry.ATTR_PRINCIPAL_ID]
    assert attrs[telemetry.ATTR_CORRELATION_ID] == correlation_id
    assert attrs[telemetry.ATTR_DECISION_AUDIT_EVENT_ID]

    # The request + downstream histograms and the request counter recorded.
    assert (
        _counter_total("gateway_requests_total", route=WRITE_ROUTE, method="POST", status="201")
        >= 1
    )
    assert _histogram_count("gateway_request_duration_seconds") >= 1
    assert _histogram_count("gateway_downstream_duration_seconds") >= 1


def test_rate_limit_denial_increments_counter(settings: Settings) -> None:
    auth = _auth_client()
    router, _ = _recording_router()
    limiter = RateLimiter(
        principal_config=BucketConfig(rps=1, burst=1),
        workspace_config=BucketConfig(rps=1000, burst=1000),
        time_source=lambda: 1000.0,
    )
    app = create_app(
        settings=settings,
        auth_client=auth,
        downstream_router=router,
        metadata_store=_FakeStore(),
        rate_limiter=limiter,
    )

    headers = {**AUTH_HEADERS, "content-type": "application/json", "idempotency-key": "k-1"}
    before = _counter_total("gateway_rate_limit_denials_total", route=WRITE_ROUTE, method="POST")
    with TestClient(app) as client:
        assert client.post(WRITE_PATH, content=b"{}", headers=headers).status_code == 201
        second = client.post(WRITE_PATH, content=b"{}", headers=headers)

    assert second.status_code == 429
    after = _counter_total("gateway_rate_limit_denials_total", route=WRITE_ROUTE, method="POST")
    assert after - before >= 1
    # The 429 is still counted as a completed request with its locked status.
    assert (
        _counter_total("gateway_requests_total", route=WRITE_ROUTE, method="POST", status="429")
        >= 1
    )


def test_idempotency_replay_increments_counter(settings: Settings) -> None:
    auth = _auth_client()
    router, calls = _recording_router()
    snapshot = response_snapshot(
        DownstreamResponse(status_code=200, headers=[("x-replay", "yes")], body=b"replayed")
    )
    store = _FakeStore(
        outcome=ExistingCompleted(record=_record(status="completed"), response_snapshot=snapshot)
    )
    app = create_app(
        settings=settings,
        auth_client=auth,
        downstream_router=router,
        metadata_store=store,
    )

    before = _counter_total("gateway_idempotency_replays_total", route=WRITE_ROUTE, method="POST")
    with TestClient(app) as client:
        response = client.post(
            WRITE_PATH,
            content=b'{"name":"demo"}',
            headers={**AUTH_HEADERS, "content-type": "application/json", "idempotency-key": "k-1"},
        )

    assert response.status_code == 200
    assert response.content == b"replayed"
    # The replay short-circuits before forwarding downstream.
    assert calls == []
    after = _counter_total("gateway_idempotency_replays_total", route=WRITE_ROUTE, method="POST")
    assert after - before >= 1

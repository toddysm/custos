"""OpenTelemetry instrumentation tests for the outbound RPC paths (WF-IMPL-081).

Each outbound Dapr Service-Invocation call from
:mod:`custos_workflow.clients` (``ScheduleActivity`` /
``CancelActivity`` on ARM, ``BindForStep`` on Connector) must
emit:

* Exactly one ``custos_workflow.outbound_rpc.call`` span with the
  locked attribute set pinned in
  :data:`LOCKED_OUTBOUND_RPC_SPAN_ATTRIBUTES`.
* Exactly one sample on
  :data:`OUTBOUND_RPC_DURATION_MS` (labelled by ``wf.client`` /
  ``wf.method`` / ``http.status_code``).
* Exactly one bump on :data:`OUTBOUND_RPC_TOTAL` (labelled by
  ``wf.client`` / ``wf.method`` / ``wf.outcome``).
* On any :class:`OutboundRpcError` exit, exactly one bump on
  :data:`OUTBOUND_RPC_ERRORS_TOTAL` (labelled by ``wf.error.kind``);
  on Cancel's HTTP-404 / HTTP-409 idempotent no-op branches the
  error counter is **not** touched.

Wires its own in-memory OTel SDK in the same shape as
``tests/test_observability.py`` (the SDK is a dev-only dependency;
production imports the API-only no-op providers).
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from opentelemetry.sdk.metrics import Counter, Histogram, MeterProvider
from opentelemetry.sdk.metrics.export import (
    AggregationTemporality,
    InMemoryMetricReader,
    MetricsData,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import StatusCode

import custos_workflow  # noqa: F401  — import to exercise the no-SDK fallback path.
from custos_workflow.clients._dapr_invoke import DaprInvokeEndpoint
from custos_workflow.clients._errors import (
    LOCKED_OUTBOUND_RPC_KINDS,
    OutboundRpcCancelledError,
    OutboundRpcDecodeError,
    OutboundRpcError,
    OutboundRpcStatusError,
    OutboundRpcTransportError,
)
from custos_workflow.clients.activity_runtime import (
    CANCEL_ACTIVITY_DAPR_METHOD,
    SCHEDULE_ACTIVITY_DAPR_METHOD,
    DaprActivityRuntimeClient,
    ScheduleActivityRequest,
)
from custos_workflow.clients.connector import (
    BIND_FOR_STEP_DAPR_METHOD,
    BindForStepRequest,
    DaprConnectorClient,
    SlotSpec,
)

# ---------------------------------------------------------------------------
# OTel SDK wiring (mirrors tests/test_observability.py)
# ---------------------------------------------------------------------------

_span_exporter = InMemorySpanExporter()
_tracer_provider = TracerProvider()
_tracer_provider.add_span_processor(SimpleSpanProcessor(_span_exporter))

_metric_reader = InMemoryMetricReader(
    preferred_temporality={
        Counter: AggregationTemporality.DELTA,
        Histogram: AggregationTemporality.DELTA,
    },
)
_meter_provider = MeterProvider(metric_readers=[_metric_reader])

# Bind the live tracer + the three outbound-RPC instruments straight
# to THIS module's local providers rather than the process-global
# ones. ``trace.set_tracer_provider`` / ``metrics.set_meter_provider``
# honour only their first caller, so reaching for the globals makes
# the captured-emission target depend on test-file import order — when
# ``tests/test_observability.py`` registers its providers first, our
# emissions would silently flow into *its* exporter / reader and this
# file's assertions would see nothing. Reading from the local
# providers keeps our spans + metrics in our own exporter / reader
# regardless of import order. The shared ``_telemetry._tracer`` global
# is swapped per-test (and restored) by ``_reset_otel_state`` so we
# never clobber the tracer that ``test_observability`` relies on.
from custos_workflow import _telemetry  # noqa: E402

_local_tracer = _tracer_provider.get_tracer("custos_workflow", "0.1.0")
_local_meter = _meter_provider.get_meter("custos_workflow", "0.1.0")
_telemetry.OUTBOUND_RPC_DURATION_MS = _local_meter.create_histogram(  # type: ignore[misc]
    name="custos_workflow_outbound_rpc_duration_ms",
    unit="ms",
    description="Outbound RPC duration histogram (test-bound).",
)
_telemetry.OUTBOUND_RPC_TOTAL = _local_meter.create_counter(  # type: ignore[misc]
    name="custos_workflow_outbound_rpc_total",
    description="Outbound RPC total counter (test-bound).",
)
_telemetry.OUTBOUND_RPC_ERRORS_TOTAL = _local_meter.create_counter(  # type: ignore[misc]
    name="custos_workflow_outbound_rpc_errors_total",
    description="Outbound RPC errors counter (test-bound).",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_otel_state() -> Iterator[None]:
    """Bind the live tracer to this file's provider, clear spans + drain metrics.

    ``_telemetry._tracer`` is a process-wide module global shared with
    every other telemetry test. We point it at this file's local
    tracer for the duration of each test (so outbound-RPC spans land in
    ``_span_exporter``) and restore the previous binding afterwards, so
    sibling suites such as ``tests/test_observability.py`` keep the
    tracer they wired at import time.
    """
    saved_tracer = _telemetry._tracer
    _telemetry._tracer = _local_tracer
    _span_exporter.clear()
    _metric_reader.get_metrics_data()
    try:
        yield
    finally:
        _telemetry._tracer = saved_tracer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _collect_points() -> list[tuple[str, dict[str, str], float | int]]:
    """Return ``[(instrument_name, attributes, value), ...]`` for points since last collect."""
    data: MetricsData | None = _metric_reader.get_metrics_data()
    if data is None:
        return []
    out: list[tuple[str, dict[str, str], float | int]] = []
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                for pt in metric.data.data_points:
                    attrs = {str(k): str(v) for k, v in (pt.attributes or {}).items()}
                    if hasattr(pt, "sum"):
                        out.append((metric.name, attrs, pt.sum))
                    else:
                        out.append((metric.name, attrs, pt.value))
    return out


def _by_name(
    points: list[tuple[str, dict[str, str], float | int]], name: str
) -> list[tuple[dict[str, str], float | int]]:
    return [(attrs, value) for n, attrs, value in points if n == name]


def _outbound_spans() -> list[Any]:
    return [
        s
        for s in _span_exporter.get_finished_spans()
        if s.name == "custos_workflow.outbound_rpc.call"
    ]


def _endpoint() -> DaprInvokeEndpoint:
    return DaprInvokeEndpoint(host="127.0.0.1", http_port=3500, app_id="arm")


def _connector_endpoint() -> DaprInvokeEndpoint:
    return DaprInvokeEndpoint(host="127.0.0.1", http_port=3500, app_id="connector")


def _schedule_request() -> ScheduleActivityRequest:
    return ScheduleActivityRequest(
        run_id="run-1",
        step_id="step-1",
        attempt=1,
        activity_ref="security/scan@1",
        inputs={"target": "x"},
        connector_contexts={},
        deadline=datetime(2099, 1, 1, tzinfo=UTC),
    )


def _bind_request() -> BindForStepRequest:
    return BindForStepRequest(
        step_key="stage1.scan",
        slots=(SlotSpec(name="primary", connector_ref="primary"),),
    )


def _mock_transport(handler: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# ---------------------------------------------------------------------------
# Locked-set exhaustiveness guards
# ---------------------------------------------------------------------------


def test_locked_outcomes_exhaustiveness() -> None:
    """``wf.outcome`` label values are pinned; the set must match the design verbatim."""
    assert (
        frozenset({"success", "transport", "retryable", "permanent", "cancelled"})
        == _telemetry.LOCKED_OUTBOUND_RPC_OUTCOMES
    )


def test_locked_span_attributes_exhaustiveness() -> None:
    """Span attribute keys are pinned; new attributes require editing the locked set."""
    assert (
        frozenset(
            {
                "wf.client",
                "wf.method",
                "wf.run.id",
                "wf.step.id",
                "wf.attempt",
                "http.method",
                "http.url",
                "http.status_code",
                "wf.outcome",
                "wf.error.kind",
            }
        )
        == _telemetry.LOCKED_OUTBOUND_RPC_SPAN_ATTRIBUTES
    )


def test_every_outbound_rpc_subclass_maps_to_a_locked_outcome() -> None:
    """Every concrete ``OutboundRpcError`` subclass must map into LOCKED_OUTBOUND_RPC_OUTCOMES."""
    # Drive one instance of each concrete subclass through
    # ``observe_outbound_rpc`` and assert the recorded ``wf.outcome``
    # lands in the locked outcome set (the real mapping under test),
    # *and* that each subclass's ``.kind`` is a locked error kind
    # (the value the error counter is labelled with). Mirrors the
    # build-time guard described in the ``_telemetry`` module
    # docstring.
    cases: list[OutboundRpcError] = [
        OutboundRpcTransportError("transport boom"),
        OutboundRpcCancelledError("cancelled upstream"),
        OutboundRpcDecodeError("bad json"),
        # ``OutboundRpcStatusError`` is parameterised by status code;
        # spot-check the two boundary buckets here (retryable 5xx +
        # permanent 4xx), leaving the full table to the per-status
        # tests below.
        OutboundRpcStatusError("upstream 503", status_code=503),
        OutboundRpcStatusError("upstream 400", status_code=400),
    ]

    async def _run(exc: OutboundRpcError) -> str:
        with pytest.raises(OutboundRpcError):
            async with _telemetry.observe_outbound_rpc(client="arm", method="ScheduleActivity"):
                raise exc
        totals = _by_name(_collect_points(), "custos_workflow_outbound_rpc_total")
        assert len(totals) == 1
        attrs, _value = totals[0]
        return attrs["wf.outcome"]

    for case in cases:
        outcome = asyncio.run(_run(case))
        assert outcome in _telemetry.LOCKED_OUTBOUND_RPC_OUTCOMES
        assert case.kind in LOCKED_OUTBOUND_RPC_KINDS


@pytest.mark.asyncio
async def test_unexpected_exception_records_permanent_with_no_error_counter() -> None:
    """An unexpected non-OutboundRpc exception still records one duration + total sample.

    The total counter must land in the ``permanent`` bucket and the
    error counter must NOT be bumped (the unexpected exception carries
    no locked ``wf.error.kind``), so the "one sample per call"
    invariant holds even on the unforeseen-failure path.
    """
    with pytest.raises(ValueError, match="boom"):
        async with _telemetry.observe_outbound_rpc(client="arm", method="ScheduleActivity"):
            raise ValueError("boom")

    points = _collect_points()
    totals = _by_name(points, "custos_workflow_outbound_rpc_total")
    assert len(totals) == 1
    attrs, value = totals[0]
    assert attrs["wf.outcome"] == "permanent"
    assert value == 1

    # No error counter — unexpected exceptions have no locked kind.
    assert _by_name(points, "custos_workflow_outbound_rpc_errors_total") == []

    # Still exactly one duration sample, labelled with the no-response
    # sentinel status code.
    durations = _by_name(points, "custos_workflow_outbound_rpc_duration_ms")
    assert len(durations) == 1
    assert durations[0][0]["http.status_code"] == "0"

    spans = _outbound_spans()
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes["wf.outcome"] == "permanent"
    assert "wf.error.kind" not in spans[0].attributes
    assert spans[0].status.status_code == StatusCode.ERROR


@pytest.mark.asyncio
async def test_cancelled_error_records_cancelled_with_no_error_counter() -> None:
    """An ``asyncio.CancelledError`` propagates untouched and records the ``cancelled`` outcome."""
    with pytest.raises(asyncio.CancelledError):
        async with _telemetry.observe_outbound_rpc(client="connector", method="BindForStep"):
            raise asyncio.CancelledError

    points = _collect_points()
    totals = _by_name(points, "custos_workflow_outbound_rpc_total")
    assert len(totals) == 1
    attrs, value = totals[0]
    assert attrs["wf.outcome"] == "cancelled"
    assert value == 1

    # No error counter — cancellation carries no locked kind.
    assert _by_name(points, "custos_workflow_outbound_rpc_errors_total") == []

    spans = _outbound_spans()
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes["wf.outcome"] == "cancelled"
    assert "wf.error.kind" not in spans[0].attributes


# ---------------------------------------------------------------------------
# Success path — ARM ScheduleActivity returns a 200 envelope
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schedule_activity_success_records_success_outcome() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "class": "success",
                "outputs": {"ok": True},
                "attempt": 1,
            },
        )

    async with _mock_transport(handler) as http_client:
        client = DaprActivityRuntimeClient(http_client=http_client, endpoint=_endpoint())
        envelope = await client.schedule_activity(_schedule_request())

    assert envelope.class_ == "success"

    points = _collect_points()
    totals = _by_name(points, "custos_workflow_outbound_rpc_total")
    assert len(totals) == 1
    attrs, value = totals[0]
    assert attrs == {
        "wf.client": "arm",
        "wf.method": SCHEDULE_ACTIVITY_DAPR_METHOD,
        "wf.outcome": "success",
    }
    assert value == 1

    # No error counter on the success path.
    assert _by_name(points, "custos_workflow_outbound_rpc_errors_total") == []

    # Duration sample labelled with the observed status code.
    durations = _by_name(points, "custos_workflow_outbound_rpc_duration_ms")
    assert len(durations) == 1
    dur_attrs, _ = durations[0]
    assert dur_attrs == {
        "wf.client": "arm",
        "wf.method": SCHEDULE_ACTIVITY_DAPR_METHOD,
        "http.status_code": "200",
    }

    # One span, locked attributes only, UNSET status, all required keys set.
    spans = _outbound_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.status.status_code is StatusCode.UNSET
    assert span.attributes is not None
    span_keys = set(span.attributes.keys())
    assert span_keys <= _telemetry.LOCKED_OUTBOUND_RPC_SPAN_ATTRIBUTES
    assert span.attributes["wf.client"] == "arm"
    assert span.attributes["wf.method"] == SCHEDULE_ACTIVITY_DAPR_METHOD
    assert span.attributes["wf.run.id"] == "run-1"
    assert span.attributes["wf.step.id"] == "step-1"
    assert span.attributes["wf.attempt"] == 1
    assert span.attributes["http.method"] == "POST"
    assert span.attributes["http.url"] == f"…/method/{SCHEDULE_ACTIVITY_DAPR_METHOD}"
    assert span.attributes["http.status_code"] == "200"
    assert span.attributes["wf.outcome"] == "success"


# ---------------------------------------------------------------------------
# Transport timeout — no response observed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schedule_activity_transport_timeout_records_transport_outcome() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("connect timeout")

    async with _mock_transport(handler) as http_client:
        client = DaprActivityRuntimeClient(http_client=http_client, endpoint=_endpoint())
        envelope = await client.schedule_activity(_schedule_request())

    # Transport failure surfaces as a retryable activity envelope.
    assert envelope.class_ == "retryable"

    points = _collect_points()
    totals = _by_name(points, "custos_workflow_outbound_rpc_total")
    assert (
        {
            "wf.client": "arm",
            "wf.method": SCHEDULE_ACTIVITY_DAPR_METHOD,
            "wf.outcome": "transport",
        },
        1,
    ) in totals

    errors = _by_name(points, "custos_workflow_outbound_rpc_errors_total")
    assert errors == [({"wf.error.kind": "workflow.client.transport"}, 1)]

    # http.status_code label is "0" when no response was observed.
    durations = _by_name(points, "custos_workflow_outbound_rpc_duration_ms")
    assert len(durations) == 1
    dur_attrs, _ = durations[0]
    assert dur_attrs["http.status_code"] == "0"

    span = _outbound_spans()[0]
    assert span.status.status_code is StatusCode.ERROR
    assert span.attributes is not None
    assert span.attributes["wf.outcome"] == "transport"
    assert span.attributes["wf.error.kind"] == "workflow.client.transport"
    # ``http.status_code`` attribute is intentionally NOT set when
    # no response was observed.
    assert "http.status_code" not in span.attributes


# ---------------------------------------------------------------------------
# HTTP 503 — retryable status error
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schedule_activity_http_503_records_retryable_outcome() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="upstream busy")

    async with _mock_transport(handler) as http_client:
        client = DaprActivityRuntimeClient(http_client=http_client, endpoint=_endpoint())
        envelope = await client.schedule_activity(_schedule_request())

    assert envelope.class_ == "retryable"

    points = _collect_points()
    totals = _by_name(points, "custos_workflow_outbound_rpc_total")
    assert (
        {
            "wf.client": "arm",
            "wf.method": SCHEDULE_ACTIVITY_DAPR_METHOD,
            "wf.outcome": "retryable",
        },
        1,
    ) in totals

    errors = _by_name(points, "custos_workflow_outbound_rpc_errors_total")
    assert errors == [({"wf.error.kind": "workflow.client.status"}, 1)]

    durations = _by_name(points, "custos_workflow_outbound_rpc_duration_ms")
    dur_attrs, _ = durations[0]
    assert dur_attrs["http.status_code"] == "503"

    span = _outbound_spans()[0]
    assert span.status.status_code is StatusCode.ERROR
    assert span.attributes is not None
    assert span.attributes["wf.outcome"] == "retryable"
    assert span.attributes["wf.error.kind"] == "workflow.client.status"
    assert span.attributes["http.status_code"] == "503"


# ---------------------------------------------------------------------------
# HTTP 404 on cancel — idempotent no-op, NO error counter
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_activity_http_404_is_success_with_no_error_counter() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="not found")

    async with _mock_transport(handler) as http_client:
        client = DaprActivityRuntimeClient(http_client=http_client, endpoint=_endpoint())
        # No raise expected — Cancel treats 404 as a no-op.
        await client.cancel_activity(run_id="run-1", step_id="step-1")

    points = _collect_points()
    totals = _by_name(points, "custos_workflow_outbound_rpc_total")
    assert totals == [
        (
            {
                "wf.client": "arm",
                "wf.method": CANCEL_ACTIVITY_DAPR_METHOD,
                "wf.outcome": "success",
            },
            1,
        )
    ]

    # Pinned by the acceptance criteria: HTTP-404 on cancel does
    # NOT touch the error counter.
    assert _by_name(points, "custos_workflow_outbound_rpc_errors_total") == []

    durations = _by_name(points, "custos_workflow_outbound_rpc_duration_ms")
    dur_attrs, _ = durations[0]
    assert dur_attrs["http.status_code"] == "404"

    span = _outbound_spans()[0]
    assert span.status.status_code is StatusCode.UNSET
    assert span.attributes is not None
    assert span.attributes["wf.outcome"] == "success"
    assert span.attributes["http.status_code"] == "404"
    # Cancel must not carry ``wf.attempt`` — cancellation is not
    # attempt-scoped.
    assert "wf.attempt" not in span.attributes


# ---------------------------------------------------------------------------
# Cancel 200 success path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_activity_http_200_is_success() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="")

    async with _mock_transport(handler) as http_client:
        client = DaprActivityRuntimeClient(http_client=http_client, endpoint=_endpoint())
        await client.cancel_activity(run_id="run-1", step_id="step-1")

    points = _collect_points()
    totals = _by_name(points, "custos_workflow_outbound_rpc_total")
    assert totals[0][0]["wf.outcome"] == "success"
    assert _by_name(points, "custos_workflow_outbound_rpc_errors_total") == []


# ---------------------------------------------------------------------------
# Cancel 500 — surfaces as error, bumps error counter
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_activity_http_500_records_retryable_outcome() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    async with _mock_transport(handler) as http_client:
        client = DaprActivityRuntimeClient(http_client=http_client, endpoint=_endpoint())
        with pytest.raises(OutboundRpcStatusError):
            await client.cancel_activity(run_id="run-1", step_id="step-1")

    points = _collect_points()
    totals = _by_name(points, "custos_workflow_outbound_rpc_total")
    assert (
        {
            "wf.client": "arm",
            "wf.method": CANCEL_ACTIVITY_DAPR_METHOD,
            "wf.outcome": "retryable",
        },
        1,
    ) in totals
    errors = _by_name(points, "custos_workflow_outbound_rpc_errors_total")
    assert errors == [({"wf.error.kind": "workflow.client.status"}, 1)]


# ---------------------------------------------------------------------------
# Cancelled upstream (HTTP 499)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schedule_activity_http_499_records_cancelled_outcome() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(499, text="client closed")

    async with _mock_transport(handler) as http_client:
        client = DaprActivityRuntimeClient(http_client=http_client, endpoint=_endpoint())
        envelope = await client.schedule_activity(_schedule_request())

    assert envelope.class_ == "cancelled"

    points = _collect_points()
    totals = _by_name(points, "custos_workflow_outbound_rpc_total")
    assert (
        {
            "wf.client": "arm",
            "wf.method": SCHEDULE_ACTIVITY_DAPR_METHOD,
            "wf.outcome": "cancelled",
        },
        1,
    ) in totals
    errors = _by_name(points, "custos_workflow_outbound_rpc_errors_total")
    assert errors == [({"wf.error.kind": "workflow.client.cancelled"}, 1)]


# ---------------------------------------------------------------------------
# Permanent / decode error
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schedule_activity_http_400_records_permanent_outcome() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, text="bad request")

    async with _mock_transport(handler) as http_client:
        client = DaprActivityRuntimeClient(http_client=http_client, endpoint=_endpoint())
        envelope = await client.schedule_activity(_schedule_request())

    assert envelope.class_ == "permanent"

    points = _collect_points()
    totals = _by_name(points, "custos_workflow_outbound_rpc_total")
    assert (
        {
            "wf.client": "arm",
            "wf.method": SCHEDULE_ACTIVITY_DAPR_METHOD,
            "wf.outcome": "permanent",
        },
        1,
    ) in totals
    errors = _by_name(points, "custos_workflow_outbound_rpc_errors_total")
    assert errors == [({"wf.error.kind": "workflow.client.status"}, 1)]


@pytest.mark.asyncio
async def test_schedule_activity_decode_error_records_permanent_outcome() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        # 200 + non-JSON body triggers OutboundRpcDecodeError.
        return httpx.Response(200, text="not-json")

    async with _mock_transport(handler) as http_client:
        client = DaprActivityRuntimeClient(http_client=http_client, endpoint=_endpoint())
        envelope = await client.schedule_activity(_schedule_request())

    assert envelope.class_ == "permanent"

    points = _collect_points()
    totals = _by_name(points, "custos_workflow_outbound_rpc_total")
    assert (
        {
            "wf.client": "arm",
            "wf.method": SCHEDULE_ACTIVITY_DAPR_METHOD,
            "wf.outcome": "permanent",
        },
        1,
    ) in totals
    errors = _by_name(points, "custos_workflow_outbound_rpc_errors_total")
    assert errors == [({"wf.error.kind": "workflow.client.decode"}, 1)]


# ---------------------------------------------------------------------------
# Connector BindForStep — exercises wf.client="connector"
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bind_for_step_success_records_connector_client() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "contexts": {
                    "primary": {
                        "slotName": "primary",
                        "handle": "tok-123",
                        "connectorKind": "primary",
                        "expiresAt": "2099-01-01T00:00:00+00:00",
                    }
                }
            },
        )

    async with _mock_transport(handler) as http_client:
        client = DaprConnectorClient(http_client=http_client, endpoint=_connector_endpoint())
        await client.bind_for_step(_bind_request())

    points = _collect_points()
    totals = _by_name(points, "custos_workflow_outbound_rpc_total")
    assert totals == [
        (
            {
                "wf.client": "connector",
                "wf.method": BIND_FOR_STEP_DAPR_METHOD,
                "wf.outcome": "success",
            },
            1,
        )
    ]
    assert _by_name(points, "custos_workflow_outbound_rpc_errors_total") == []

    span = _outbound_spans()[0]
    assert span.attributes is not None
    assert span.attributes["wf.client"] == "connector"
    assert span.attributes["wf.method"] == BIND_FOR_STEP_DAPR_METHOD
    # BindForStep surfaces step_key as wf.step.id; run.id and attempt
    # are intentionally absent (not carried on BindForStepRequest).
    assert span.attributes["wf.step.id"] == "stage1.scan"
    assert "wf.run.id" not in span.attributes
    assert "wf.attempt" not in span.attributes


@pytest.mark.asyncio
async def test_bind_for_step_transport_error_bumps_error_counter() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("read timeout")

    async with _mock_transport(handler) as http_client:
        client = DaprConnectorClient(http_client=http_client, endpoint=_connector_endpoint())
        with pytest.raises(OutboundRpcTransportError):
            await client.bind_for_step(_bind_request())

    points = _collect_points()
    errors = _by_name(points, "custos_workflow_outbound_rpc_errors_total")
    assert errors == [({"wf.error.kind": "workflow.client.transport"}, 1)]


# ---------------------------------------------------------------------------
# Bad client label — fails loudly at the decorator boundary
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_observe_outbound_rpc_rejects_unknown_client() -> None:
    with pytest.raises(ValueError, match=r"observe_outbound_rpc\.client"):
        async with _telemetry.observe_outbound_rpc(
            client="catalog",  # not in {"arm", "connector"}
            method="Whatever",
        ):
            pass


# ---------------------------------------------------------------------------
# Status-code classifier — boundary table
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "status_code,expected",
    [
        (200, "permanent"),  # not in 4xx/5xx — falls through to permanent
        (408, "retryable"),
        (429, "retryable"),
        (400, "permanent"),
        (499, "permanent"),
        (500, "retryable"),
        (503, "retryable"),
        (599, "retryable"),
        (600, "permanent"),
        (100, "permanent"),
    ],
)
def test_classify_status_outcome_boundary_table(status_code: int, expected: str) -> None:
    assert _telemetry._classify_status_outcome(status_code) == expected

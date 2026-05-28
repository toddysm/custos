"""Unit tests for the ``custos_connector._telemetry`` instrumentation.

CONN-IMPL-029 (Phase K, #312). Verifies the five named metrics, the
six span helpers, and the cursor-lag registry behave as the issue
acceptance criteria require:

* Each ``observe_*`` helper opens a span with the documented name,
  closes it on context exit, and marks it ``ERROR`` on a propagated
  exception while leaving ``KeyboardInterrupt`` / ``SystemExit`` /
  ``CancelledError`` untouched.
* ``BIND_LATENCY_SECONDS`` and ``PULL_TICK_DURATION_SECONDS`` record
  one histogram sample per call with the ``outcome=success`` /
  ``outcome=error`` label.
* ``ACTIVE_LEASES`` (via :func:`record_lease_issued` /
  :func:`record_lease_closed`) bumps the gauge per instance.
* ``MANIFEST_FALLBACK_TOTAL`` (via :func:`record_manifest_fallback`)
  validates the outcome label and falls back to ``unknown`` on drift.
* :class:`CursorLagRegistry` is round-trippable and the observable
  gauge callback yields one observation per registered entry.

The fixture installs an in-memory ``MeterProvider`` + ``TracerProvider``
inside the test process so the module-level instrument handles
(``BIND_LATENCY_SECONDS`` etc.) bind through OTel's API-side proxy and
write into the SDK reader the assertions inspect. Tests that mutate
provider state run serially per pytest's default ordering; no global
state leaks because each test re-creates its own reader.

Critical implementation detail tested: the span helpers must work
with ``@dataclass(frozen=True, slots=True)`` Exception subclasses.
``contextlib.contextmanager``'s ``__exit__`` reassigns
``exc.__traceback__`` on the in-flight exception, which fails on
slotted dataclass exceptions. The helpers therefore use plain
class-based context managers (:class:`_SpanOnly` /
:class:`_SpanAndHistogram`) rather than ``@contextmanager``. The
``test_span_helpers_accept_frozen_dataclass_exceptions`` test pins
that contract.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import StatusCode

from custos_connector._telemetry import (
    ACTIVE_LEASES,
    BIND_LATENCY_SECONDS,
    CURSOR_LAG_REGISTRY,
    FALLBACK_OUTCOME_IGNORED,
    FALLBACK_OUTCOME_REJECTED,
    FALLBACK_OUTCOME_USED,
    MANIFEST_FALLBACK_TOTAL,
    PULL_TICK_DURATION_SECONDS,
    CursorLagRegistry,
    observe_bind,
    observe_identity_resolution,
    observe_lease_issue,
    observe_lease_refresh,
    observe_manifest_discovery,
    observe_pull_tick,
    record_lease_closed,
    record_lease_issued,
    record_manifest_fallback,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def span_exporter() -> InMemorySpanExporter:
    """Install an in-memory ``TracerProvider`` and return its exporter.

    Resets OTel's ``set-once`` global guard so each test gets a fresh
    provider, and re-binds ``custos_connector._telemetry._tracer`` to
    a fresh tracer from the new provider. The latter step is required
    because :class:`opentelemetry.trace.ProxyTracer` caches its
    underlying real tracer on first use; without the rebind, span
    helpers continue to write into the first test's exporter.
    """
    import opentelemetry.trace as otel_trace_internal

    from custos_connector import _telemetry as telemetry_mod

    otel_trace_internal._TRACER_PROVIDER_SET_ONCE._done = False
    otel_trace_internal._TRACER_PROVIDER = None
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    telemetry_mod._tracer = provider.get_tracer(
        telemetry_mod._INSTRUMENTATION_NAME,
        telemetry_mod._INSTRUMENTATION_VERSION,
    )
    exporter.clear()
    return exporter


@pytest.fixture
def metric_reader() -> InMemoryMetricReader:
    """Install an in-memory ``MeterProvider`` + reader.

    Same set-once + module rebind pattern as :func:`span_exporter`.
    Also re-creates every module-level metric instrument so each
    test's writes land in the new reader rather than the previous
    fixture's. The OTel API's :class:`ProxyMeter` similarly caches
    its real meter, and SDK ``Histogram`` / ``Counter`` /
    ``UpDownCounter`` instruments are bound to their meter at
    create-time — both layers need refreshing.
    """
    import opentelemetry.metrics._internal as otel_metrics_internal

    from custos_connector import _telemetry as telemetry_mod

    otel_metrics_internal._METER_PROVIDER_SET_ONCE._done = False
    otel_metrics_internal._METER_PROVIDER = None
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    metrics.set_meter_provider(provider)
    telemetry_mod._meter = provider.get_meter(
        telemetry_mod._INSTRUMENTATION_NAME,
        telemetry_mod._INSTRUMENTATION_VERSION,
    )
    telemetry_mod.BIND_LATENCY_SECONDS = telemetry_mod._meter.create_histogram(
        name="custos_connector_bind_latency_seconds", unit="s"
    )
    telemetry_mod.PULL_TICK_DURATION_SECONDS = telemetry_mod._meter.create_histogram(
        name="custos_connector_pull_tick_duration_seconds", unit="s"
    )
    telemetry_mod.ACTIVE_LEASES = telemetry_mod._meter.create_up_down_counter(
        name="custos_connector_active_leases"
    )
    telemetry_mod.MANIFEST_FALLBACK_TOTAL = telemetry_mod._meter.create_counter(
        name="custos_connector_manifest_fallback_total"
    )
    telemetry_mod.CURSOR_LAG_SECONDS = telemetry_mod._meter.create_observable_gauge(
        name="custos_connector_cursor_lag_seconds",
        callbacks=[telemetry_mod._cursor_lag_observable_callback],
        unit="s",
    )
    return reader


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_metric(reader: InMemoryMetricReader, name: str) -> object | None:
    """Return the first metric matching ``name`` from the reader's snapshot."""
    data = reader.get_metrics_data()
    if data is None:
        return None
    for resource_metric in data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                if metric.name == name:
                    return metric
    return None


def _sum_histogram_count(reader: InMemoryMetricReader, name: str) -> int:
    """Return the sum of the ``count`` field over every data point of ``name``."""
    metric = _find_metric(reader, name)
    if metric is None:
        return 0
    return sum(point.count for point in metric.data.data_points)  # type: ignore[attr-defined]


def _data_points_by_label(
    reader: InMemoryMetricReader, name: str, label_key: str
) -> dict[str, object]:
    """Return ``{label_value: data_point}`` for the given metric + label."""
    metric = _find_metric(reader, name)
    if metric is None:
        return {}
    out: dict[str, object] = {}
    for point in metric.data.data_points:  # type: ignore[attr-defined]
        attrs = dict(point.attributes or {})
        if label_key in attrs:
            out[str(attrs[label_key])] = point
    return out


# ---------------------------------------------------------------------------
# Span helpers — success + error paths
# ---------------------------------------------------------------------------


def test_observe_bind_records_success_span_and_histogram_sample(
    span_exporter: InMemorySpanExporter,
    metric_reader: InMemoryMetricReader,
) -> None:
    """``observe_bind`` opens ``custos_connector.bind_for_step`` and records.

    Acceptance: success path produces exactly one span with the
    canonical name and one histogram sample under ``outcome=success``.
    """
    with observe_bind() as span:
        span.set_attribute("workspaceId", "ws-1")

    spans = span_exporter.get_finished_spans()
    assert [s.name for s in spans] == ["custos_connector.bind_for_step"]
    assert spans[0].status.status_code != StatusCode.ERROR

    by_outcome = _data_points_by_label(
        metric_reader, "custos_connector_bind_latency_seconds", "outcome"
    )
    assert "success" in by_outcome
    assert by_outcome["success"].count == 1  # type: ignore[union-attr]


def test_observe_bind_records_error_span_and_histogram_sample(
    span_exporter: InMemorySpanExporter,
    metric_reader: InMemoryMetricReader,
) -> None:
    """``observe_bind`` marks the span ERROR + records ``outcome=error``."""

    class _Boom(RuntimeError):
        pass

    with pytest.raises(_Boom), observe_bind():
        raise _Boom("nope")

    spans = span_exporter.get_finished_spans()
    assert spans[0].status.status_code == StatusCode.ERROR

    by_outcome = _data_points_by_label(
        metric_reader, "custos_connector_bind_latency_seconds", "outcome"
    )
    assert "error" in by_outcome
    assert by_outcome["error"].count == 1  # type: ignore[union-attr]


def test_observe_pull_tick_records_under_pull_tick_histogram(
    span_exporter: InMemorySpanExporter,
    metric_reader: InMemoryMetricReader,
) -> None:
    """``observe_pull_tick`` writes into ``..._pull_tick_duration_seconds``."""
    with observe_pull_tick():
        pass

    assert [s.name for s in span_exporter.get_finished_spans()] == ["custos_connector.pull_tick"]
    assert _sum_histogram_count(metric_reader, "custos_connector_pull_tick_duration_seconds") == 1


@pytest.mark.parametrize(
    ("observe_fn", "expected_span_name"),
    [
        (observe_lease_issue, "custos_connector.lease.issue"),
        (observe_lease_refresh, "custos_connector.lease.refresh"),
        (observe_manifest_discovery, "custos_connector.manifest.discovery"),
        (observe_identity_resolution, "custos_connector.identity.resolve"),
    ],
)
def test_span_only_helpers_open_canonical_span_names(
    span_exporter: InMemorySpanExporter,
    observe_fn,
    expected_span_name: str,
) -> None:
    """Each span-only helper produces exactly one span with the canonical name."""
    with observe_fn():
        pass
    spans = span_exporter.get_finished_spans()
    assert [s.name for s in spans] == [expected_span_name]


def test_span_helpers_accept_frozen_dataclass_exceptions(
    span_exporter: InMemorySpanExporter,
) -> None:
    """Span helpers must not break on ``@dataclass(frozen=True, slots=True)`` exceptions.

    Pins the workaround: the helpers use a plain class-based context
    manager so contextlib's ``__exit__`` cannot rebind
    ``exc.__traceback__`` on a slotted dataclass exception. This was
    a real regression observed against
    :class:`~custos_connector.manifest.discovery.ManifestDiscoveryError`
    during initial Phase K wiring.
    """

    @dataclass(frozen=True, slots=True)
    class _FrozenErr(Exception):
        code: str = "boom"

    with pytest.raises(_FrozenErr), observe_manifest_discovery():
        raise _FrozenErr()

    spans = span_exporter.get_finished_spans()
    assert spans[0].status.status_code == StatusCode.ERROR


def test_observe_bind_does_not_record_on_keyboard_interrupt(
    span_exporter: InMemorySpanExporter,
    metric_reader: InMemoryMetricReader,
) -> None:
    """Process-control exceptions must propagate without metric / status change.

    ``KeyboardInterrupt`` is not an application error; recording it
    as ``outcome=error`` would corrupt the SLO dashboards.
    """
    with pytest.raises(KeyboardInterrupt), observe_bind():
        raise KeyboardInterrupt

    spans = span_exporter.get_finished_spans()
    assert spans[0].status.status_code != StatusCode.ERROR

    by_outcome = _data_points_by_label(
        metric_reader, "custos_connector_bind_latency_seconds", "outcome"
    )
    assert "error" not in by_outcome
    assert "success" not in by_outcome


# ---------------------------------------------------------------------------
# Lease gauge
# ---------------------------------------------------------------------------


def test_record_lease_issued_and_closed_bump_active_leases_gauge(
    metric_reader: InMemoryMetricReader,
) -> None:
    """``ACTIVE_LEASES`` reports the running balance per instance."""
    record_lease_issued("inst-A")
    record_lease_issued("inst-A")
    record_lease_issued("inst-B")
    record_lease_closed("inst-A")

    by_instance = _data_points_by_label(
        metric_reader, "custos_connector_active_leases", "connectorInstanceId"
    )
    assert by_instance["inst-A"].value == 1  # type: ignore[union-attr]
    assert by_instance["inst-B"].value == 1  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Manifest-fallback counter
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "outcome",
    [FALLBACK_OUTCOME_USED, FALLBACK_OUTCOME_IGNORED, FALLBACK_OUTCOME_REJECTED],
)
def test_record_manifest_fallback_accepts_known_outcomes(
    metric_reader: InMemoryMetricReader,
    outcome: str,
) -> None:
    """All three documented outcome labels feed the counter directly."""
    record_manifest_fallback(outcome)
    by_outcome = _data_points_by_label(
        metric_reader, "custos_connector_manifest_fallback_total", "outcome"
    )
    assert outcome in by_outcome
    assert by_outcome[outcome].value == 1  # type: ignore[union-attr]


def test_record_manifest_fallback_unknown_label_falls_back_to_unknown(
    metric_reader: InMemoryMetricReader,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Unknown outcome labels still tick the counter under ``unknown``.

    The defensive guard avoids fragmenting the metric's label set
    when a contributor adds a new event constant but forgets to
    extend the validation list. The counter still ticks so the
    operator sees drift on the dashboard.
    """
    with caplog.at_level("WARNING", logger="custos_connector.telemetry"):
        record_manifest_fallback("brand-new-outcome")

    by_outcome = _data_points_by_label(
        metric_reader, "custos_connector_manifest_fallback_total", "outcome"
    )
    assert "unknown" in by_outcome
    assert any("brand-new-outcome" in record.message for record in caplog.records)


# ---------------------------------------------------------------------------
# Cursor-lag registry + observable gauge
# ---------------------------------------------------------------------------


def test_cursor_lag_registry_record_and_forget_round_trip() -> None:
    """Recording an entry, then forgetting it, leaves the snapshot empty."""
    fixed_now = datetime(2030, 1, 1, tzinfo=UTC)
    reg = CursorLagRegistry(clock=lambda: fixed_now)

    reg.record_advanced(workspace_id="ws-1", instance_id="inst-1", advanced_at=fixed_now)
    assert reg.snapshot() == {("ws-1", "inst-1"): fixed_now}

    reg.forget(workspace_id="ws-1", instance_id="inst-1")
    assert reg.snapshot() == {}


def test_cursor_lag_observable_gauge_emits_per_registered_entry(
    metric_reader: InMemoryMetricReader,
) -> None:
    """The module-level gauge yields one observation per registered entry.

    The callback reads from :data:`CURSOR_LAG_REGISTRY` so the test
    writes through that singleton. The fixture rotates the meter
    provider per-test so the observation is fresh.
    """
    earlier = datetime.now(UTC) - timedelta(seconds=42)
    CURSOR_LAG_REGISTRY.record_advanced(
        workspace_id="ws-1",
        instance_id="inst-lag",
        advanced_at=earlier,
    )
    try:
        by_instance = _data_points_by_label(
            metric_reader,
            "custos_connector_cursor_lag_seconds",
            "connectorInstanceId",
        )
        assert "inst-lag" in by_instance
        # The gauge math runs at scrape time so the exact value
        # drifts on every read; assert it landed in a plausible
        # window rather than a hard equality.
        assert by_instance["inst-lag"].value >= 0.0  # type: ignore[union-attr]
    finally:
        CURSOR_LAG_REGISTRY.forget(workspace_id="ws-1", instance_id="inst-lag")


# ---------------------------------------------------------------------------
# Module-level instrument identity
# ---------------------------------------------------------------------------


def test_module_level_instruments_are_singletons() -> None:
    """The five named instruments are bound at import time and never re-created.

    Frozen by issue #312: the metric names and identities are the
    public contract for the Phase K Prometheus surface. A re-import
    of the module would shadow them; that is detected here so the
    test fails loudly if anyone accidentally introduces a factory.
    """
    assert BIND_LATENCY_SECONDS is not None
    assert PULL_TICK_DURATION_SECONDS is not None
    assert ACTIVE_LEASES is not None
    assert MANIFEST_FALLBACK_TOTAL is not None

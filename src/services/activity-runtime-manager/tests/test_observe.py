"""Unit tests for the ``custos_arm.observe`` instrumentation (ARM-IMPL-020).

Verifies the attempt-lifecycle spans, the per-stage duration histogram,
and the terminal-result counter:

* ``observe_attempt`` opens ``custos_arm.attempt`` with the attempt
  coordinates + the ``activity.scheduled`` lifecycle event, and marks
  the span ``ERROR`` on a propagated exception.
* ``observe_stage`` opens ``custos_arm.attempt.<stage>`` and records one
  ``custos_arm_attempt_stage_duration_ms`` sample labelled by
  ``stage`` + ``outcome`` (``success`` / ``error``), leaving process
  control unwinds untouched.
* ``record_result`` bumps ``custos_arm_attempts_total`` labelled by
  ``class`` + ``code`` so every failure mode in the design's
  terminal-state table maps to its documented code/class, and emits the
  ``activity.terminal`` lifecycle event.

The fixtures install an in-memory ``MeterProvider`` + ``TracerProvider``
inside the test process and re-bind the module-level tracer/meter/metric
handles so the assertions read back the writes. Same set-once + rebind
pattern as the connector- and catalog-service telemetry tests.
"""

from __future__ import annotations

from typing import Any

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

from custos_arm.contract import StepRef
from custos_arm.observe import (
    EVENT_SCHEDULED,
    EVENT_TERMINAL,
    STAGE_FINALIZE,
    STAGE_MATERIALIZE,
    STAGE_RESOLVE,
    STAGE_RUN,
    observe_attempt,
    observe_stage,
    record_result,
)
from custos_arm.result import ActivityResultEnvelope, ResultClass
from custos_arm.scheduler.request import ScheduleRequest

_STAGE_DURATION = "custos_arm_attempt_stage_duration_ms"
_ATTEMPTS_TOTAL = "custos_arm_attempts_total"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def span_exporter() -> InMemorySpanExporter:
    """Install an in-memory ``TracerProvider`` and return its exporter."""
    import opentelemetry.trace as otel_trace_internal

    from custos_arm.observe import telemetry as telemetry_mod

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
    """Install an in-memory ``MeterProvider`` + reader and rebind instruments."""
    import opentelemetry.metrics._internal as otel_metrics_internal

    from custos_arm.observe import telemetry as telemetry_mod

    otel_metrics_internal._METER_PROVIDER_SET_ONCE._done = False
    otel_metrics_internal._METER_PROVIDER = None
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    metrics.set_meter_provider(provider)
    telemetry_mod._meter = provider.get_meter(
        telemetry_mod._INSTRUMENTATION_NAME,
        telemetry_mod._INSTRUMENTATION_VERSION,
    )
    telemetry_mod.STAGE_DURATION_MS = telemetry_mod._meter.create_histogram(  # type: ignore[misc]
        name=_STAGE_DURATION, unit="ms"
    )
    telemetry_mod.ATTEMPTS_TOTAL = telemetry_mod._meter.create_counter(  # type: ignore[misc]
        name=_ATTEMPTS_TOTAL
    )
    return reader


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _attrs(obj: Any) -> dict[str, Any]:
    """Coerce an OTel span/event attribute mapping to a plain dict."""
    return dict(obj.attributes or {})


def _find_metric(reader: InMemoryMetricReader, name: str) -> Any | None:
    data = reader.get_metrics_data()
    if data is None:
        return None
    for resource_metric in data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                if metric.name == name:
                    return metric
    return None


def _points_by_label(reader: InMemoryMetricReader, name: str, label_key: str) -> dict[str, Any]:
    metric = _find_metric(reader, name)
    if metric is None:
        return {}
    out: dict[str, Any] = {}
    for point in metric.data.data_points:
        attrs = dict(point.attributes or {})
        if label_key in attrs:
            out[str(attrs[label_key])] = point
    return out


def _stage_points(reader: InMemoryMetricReader) -> dict[tuple[str, str], Any]:
    """Return ``{(stage, outcome): data_point}`` for the stage histogram."""
    metric = _find_metric(reader, _STAGE_DURATION)
    if metric is None:
        return {}
    out: dict[tuple[str, str], Any] = {}
    for point in metric.data.data_points:
        attrs = dict(point.attributes or {})
        out[(str(attrs.get("stage")), str(attrs.get("outcome")))] = point
    return out


def _request() -> ScheduleRequest:
    return ScheduleRequest(
        workspace_id="ws-1",
        step=StepRef(runId="run-1", stepId="step-1", attempt=2),
        activity_ref="acme/echo@1.0.0",
        inputs={"message": "hi"},
    )


def _envelope(result_class: ResultClass, code: str | None) -> ActivityResultEnvelope:
    if code is None:
        return ActivityResultEnvelope.model_validate(
            {"class": result_class.value, "attempt": 1, "outputs": {}}
        )
    error = {"code": code, "class": result_class.value, "message": "boom"}
    return ActivityResultEnvelope.model_validate(
        {"class": result_class.value, "attempt": 1, "error": error}
    )


# ---------------------------------------------------------------------------
# observe_attempt
# ---------------------------------------------------------------------------


def test_observe_attempt_opens_span_with_scheduled_event(
    span_exporter: InMemorySpanExporter,
) -> None:
    with observe_attempt(_request()) as span:
        span.set_attribute("checked", True)

    spans = span_exporter.get_finished_spans()
    assert [s.name for s in spans] == ["custos_arm.attempt"]
    attempt = spans[0]
    assert attempt.status.status_code != StatusCode.ERROR
    attrs = _attrs(attempt)
    assert attrs["custos.run_id"] == "run-1"
    assert attrs["custos.step_id"] == "step-1"
    assert attrs["custos.attempt"] == 2
    assert attrs["custos.activity_ref"] == "acme/echo@1.0.0"
    assert [e.name for e in attempt.events] == [EVENT_SCHEDULED]


def test_observe_attempt_marks_error_and_reraises(
    span_exporter: InMemorySpanExporter,
) -> None:
    with pytest.raises(ValueError, match="nope"):  # noqa: SIM117
        with observe_attempt(_request()):
            raise ValueError("nope")

    spans = span_exporter.get_finished_spans()
    assert spans[0].status.status_code is StatusCode.ERROR
    assert any(e.name == "exception" for e in spans[0].events)


# ---------------------------------------------------------------------------
# observe_stage
# ---------------------------------------------------------------------------


def test_observe_stage_records_success_sample(
    span_exporter: InMemorySpanExporter,
    metric_reader: InMemoryMetricReader,
) -> None:
    with observe_stage(STAGE_RESOLVE):
        pass

    spans = span_exporter.get_finished_spans()
    assert [s.name for s in spans] == ["custos_arm.attempt.resolve"]

    points = _stage_points(metric_reader)
    assert (STAGE_RESOLVE, "success") in points
    assert points[(STAGE_RESOLVE, "success")].count == 1


def test_observe_stage_records_error_sample_and_reraises(
    span_exporter: InMemorySpanExporter,
    metric_reader: InMemoryMetricReader,
) -> None:
    with pytest.raises(RuntimeError, match="bad"):  # noqa: SIM117
        with observe_stage(STAGE_RUN):
            raise RuntimeError("bad")

    spans = span_exporter.get_finished_spans()
    assert spans[0].status.status_code is StatusCode.ERROR
    points = _stage_points(metric_reader)
    assert (STAGE_RUN, "error") in points
    assert points[(STAGE_RUN, "error")].count == 1


def test_observe_stage_lets_base_exception_through(
    metric_reader: InMemoryMetricReader,
) -> None:
    with pytest.raises(KeyboardInterrupt):  # noqa: SIM117
        with observe_stage(STAGE_MATERIALIZE):
            raise KeyboardInterrupt
    # No sample recorded — process control unwinds are not stage errors.
    assert _stage_points(metric_reader) == {}


def test_all_stage_labels_round_trip(
    span_exporter: InMemorySpanExporter,
    metric_reader: InMemoryMetricReader,
) -> None:
    for stage in (STAGE_RESOLVE, STAGE_MATERIALIZE, STAGE_RUN, STAGE_FINALIZE):
        with observe_stage(stage):
            pass
    points = _stage_points(metric_reader)
    for stage in (STAGE_RESOLVE, STAGE_MATERIALIZE, STAGE_RUN, STAGE_FINALIZE):
        assert (stage, "success") in points


# ---------------------------------------------------------------------------
# record_result — failure-mode -> code/class mapping
# ---------------------------------------------------------------------------


def test_record_result_success_uses_none_code(
    metric_reader: InMemoryMetricReader,
) -> None:
    record_result(_envelope(ResultClass.SUCCESS, None))
    by_code = _points_by_label(metric_reader, _ATTEMPTS_TOTAL, "code")
    assert "none" in by_code
    assert dict(by_code["none"].attributes)["class"] == "success"
    assert by_code["none"].value == 1


@pytest.mark.parametrize(
    ("result_class", "code"),
    [
        # The design's terminal-state table, one row per failure mode.
        (ResultClass.RETRYABLE, "activity.sandbox_failure"),
        (ResultClass.RETRYABLE, "activity.oom_killed"),
        (ResultClass.PERMANENT, "output.schema_violation"),
        (ResultClass.PERMANENT, "activity.contract_violation"),
        (ResultClass.CANCELLED, "activity.cancelled"),
        (ResultClass.CANCELLED, "activity.timeout"),
    ],
)
def test_record_result_maps_failure_mode_to_code_and_class(
    metric_reader: InMemoryMetricReader,
    result_class: ResultClass,
    code: str,
) -> None:
    record_result(_envelope(result_class, code))
    by_code = _points_by_label(metric_reader, _ATTEMPTS_TOTAL, "code")
    assert code in by_code
    point = by_code[code]
    assert dict(point.attributes)["class"] == result_class.value
    assert point.value == 1


def test_record_result_emits_terminal_event_on_attempt_span(
    span_exporter: InMemorySpanExporter,
) -> None:
    with observe_attempt(_request()):
        record_result(_envelope(ResultClass.SUCCESS, None))

    spans = span_exporter.get_finished_spans()
    names = [e.name for e in spans[0].events]
    assert names == [EVENT_SCHEDULED, EVENT_TERMINAL]
    terminal = next(e for e in spans[0].events if e.name == EVENT_TERMINAL)
    assert _attrs(terminal)["custos.result_class"] == "success"
    assert _attrs(terminal)["custos.error_code"] == "none"

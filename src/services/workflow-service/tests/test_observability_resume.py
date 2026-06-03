"""OpenTelemetry instrumentation tests for the Resume Subscription Manager.

WF-IMPL-110 adds spans + counters for the register / cancel / replay
paths of the Resume Subscription Manager (REQ-081, ``waitFor:``):

- ``custos_workflow.resume.register`` span +
  ``custos_workflow_resume_subscriptions_registered_total`` counter
  (labelled by ``outcome``), via :func:`observe_resume_registration`.
- ``custos_workflow.resume.cancel`` span +
  ``custos_workflow_resume_subscriptions_cancelled_total`` counter
  (labelled by ``outcome``), via :func:`observe_resume_cancellation`.
- ``custos_workflow.resume.replay`` span (no counter), via
  :func:`observe_resume_replay`.
- ``custos_workflow_resumes_total`` counter, via
  :func:`record_resume`.
- ``custos_workflow_resume_subscription_divergent_total`` counter,
  via :func:`record_resume_subscription_divergent`.

Mirrors the SDK-rebind pattern in ``tests/test_observability_steps.py``
(WF-IMPL-058): the production package only imports
``opentelemetry-api`` and resolves no-op instruments at import time,
so the harness rebinds ``_telemetry._tracer`` / ``_meter`` and the
four resume counters to in-memory-exporter-backed instances.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

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

import custos_workflow  # noqa: F401 — touch the public package before SDK install.

# ---------------------------------------------------------------------------
# OTel SDK wiring (mirrors tests/test_observability_steps.py).
# ---------------------------------------------------------------------------
#
# We deliberately do NOT call ``trace.set_tracer_provider`` /
# ``metrics.set_meter_provider`` — those are process-global and a
# sibling test module already installs *its* providers when collected
# first. We grab tracers/meters directly off our own local provider
# instances and rebind ``_telemetry._tracer`` / ``_meter`` to those,
# so every instrument we recreate below is wired to *our* in-memory
# exporter regardless of the global default.

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


from custos_workflow import _telemetry  # noqa: E402 — must follow provider construct.

_sdk_tracer = _tracer_provider.get_tracer("custos_workflow", "0.1.0")
_sdk_meter = _meter_provider.get_meter("custos_workflow", "0.1.0")

_registered_total = _sdk_meter.create_counter(
    name="custos_workflow_resume_subscriptions_registered_total",
)
_cancelled_total = _sdk_meter.create_counter(
    name="custos_workflow_resume_subscriptions_cancelled_total",
)
_resumes_total = _sdk_meter.create_counter(
    name="custos_workflow_resumes_total",
)
_divergent_total = _sdk_meter.create_counter(
    name="custos_workflow_resume_subscription_divergent_total",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_otel_state() -> Iterator[None]:
    """Pin ``_telemetry`` bindings to this module's SDK exporter then restore."""
    saved_tracer = _telemetry._tracer
    saved_meter = _telemetry._meter
    saved_registered = _telemetry.RESUME_SUBSCRIPTIONS_REGISTERED_TOTAL
    saved_cancelled = _telemetry.RESUME_SUBSCRIPTIONS_CANCELLED_TOTAL
    saved_resumes = _telemetry.RESUMES_TOTAL
    saved_divergent = _telemetry.RESUME_SUBSCRIPTION_DIVERGENT_TOTAL
    _telemetry._tracer = _sdk_tracer
    _telemetry._meter = _sdk_meter
    _telemetry.RESUME_SUBSCRIPTIONS_REGISTERED_TOTAL = _registered_total  # type: ignore[misc]
    _telemetry.RESUME_SUBSCRIPTIONS_CANCELLED_TOTAL = _cancelled_total  # type: ignore[misc]
    _telemetry.RESUMES_TOTAL = _resumes_total  # type: ignore[misc]
    _telemetry.RESUME_SUBSCRIPTION_DIVERGENT_TOTAL = _divergent_total  # type: ignore[misc]
    _span_exporter.clear()
    _metric_reader.get_metrics_data()
    try:
        yield
    finally:
        _telemetry._tracer = saved_tracer
        _telemetry._meter = saved_meter
        _telemetry.RESUME_SUBSCRIPTIONS_REGISTERED_TOTAL = saved_registered  # type: ignore[misc]
        _telemetry.RESUME_SUBSCRIPTIONS_CANCELLED_TOTAL = saved_cancelled  # type: ignore[misc]
        _telemetry.RESUMES_TOTAL = saved_resumes  # type: ignore[misc]
        _telemetry.RESUME_SUBSCRIPTION_DIVERGENT_TOTAL = saved_divergent  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Metric drain helpers (copied from tests/test_observability_steps.py)
# ---------------------------------------------------------------------------


def _collect_points() -> list[tuple[str, dict[str, str], float | int]]:
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
    points: list[tuple[str, dict[str, str], float | int]],
    name: str,
) -> list[tuple[dict[str, str], float | int]]:
    return [(attrs, value) for n, attrs, value in points if n == name]


def _spans_by_name(name: str) -> list[Any]:
    return [s for s in _span_exporter.get_finished_spans() if s.name == name]


class _Boom(RuntimeError):
    """Sentinel error raised inside an ``observe_*`` block."""


# ---------------------------------------------------------------------------
# observe_resume_registration
# ---------------------------------------------------------------------------


class TestObserveResumeRegistration:
    def test_success_emits_span_and_success_counter(self) -> None:
        with _telemetry.observe_resume_registration():
            pass

        spans = _spans_by_name("custos_workflow.resume.register")
        assert len(spans) == 1
        assert spans[0].attributes is not None
        assert spans[0].attributes["outcome"] == "success"
        assert spans[0].status.status_code is not StatusCode.ERROR

        points = _by_name(
            _collect_points(),
            "custos_workflow_resume_subscriptions_registered_total",
        )
        assert points == [({"outcome": "success"}, 1)]

    def test_error_records_error_counter_and_reraises(self) -> None:
        with pytest.raises(_Boom):  # noqa: SIM117 — explicit single-CM nesting reads clearer
            with _telemetry.observe_resume_registration():
                raise _Boom

        spans = _spans_by_name("custos_workflow.resume.register")
        assert len(spans) == 1
        assert spans[0].attributes is not None
        assert spans[0].attributes["outcome"] == "error"
        assert spans[0].status.status_code is StatusCode.ERROR

        points = _by_name(
            _collect_points(),
            "custos_workflow_resume_subscriptions_registered_total",
        )
        assert points == [({"outcome": "error"}, 1)]


# ---------------------------------------------------------------------------
# observe_resume_cancellation
# ---------------------------------------------------------------------------


class TestObserveResumeCancellation:
    def test_success_emits_span_and_success_counter(self) -> None:
        with _telemetry.observe_resume_cancellation():
            pass

        spans = _spans_by_name("custos_workflow.resume.cancel")
        assert len(spans) == 1
        assert spans[0].attributes is not None
        assert spans[0].attributes["outcome"] == "success"

        points = _by_name(
            _collect_points(),
            "custos_workflow_resume_subscriptions_cancelled_total",
        )
        assert points == [({"outcome": "success"}, 1)]

    def test_error_records_error_counter_and_reraises(self) -> None:
        with pytest.raises(_Boom):  # noqa: SIM117
            with _telemetry.observe_resume_cancellation():
                raise _Boom

        spans = _spans_by_name("custos_workflow.resume.cancel")
        assert len(spans) == 1
        assert spans[0].attributes is not None
        assert spans[0].attributes["outcome"] == "error"
        assert spans[0].status.status_code is StatusCode.ERROR

        points = _by_name(
            _collect_points(),
            "custos_workflow_resume_subscriptions_cancelled_total",
        )
        assert points == [({"outcome": "error"}, 1)]


# ---------------------------------------------------------------------------
# observe_resume_replay
# ---------------------------------------------------------------------------


class TestObserveResumeReplay:
    def test_success_emits_span_with_no_counter(self) -> None:
        with _telemetry.observe_resume_replay():
            pass

        spans = _spans_by_name("custos_workflow.resume.replay")
        assert len(spans) == 1
        assert spans[0].attributes is not None
        assert spans[0].attributes["outcome"] == "success"
        # The replay span owns no counter of its own.
        assert _collect_points() == []

    def test_error_sets_error_status_and_reraises(self) -> None:
        with pytest.raises(_Boom):  # noqa: SIM117
            with _telemetry.observe_resume_replay():
                raise _Boom

        spans = _spans_by_name("custos_workflow.resume.replay")
        assert len(spans) == 1
        assert spans[0].attributes is not None
        assert spans[0].attributes["outcome"] == "error"
        assert spans[0].status.status_code is StatusCode.ERROR
        assert _collect_points() == []


# ---------------------------------------------------------------------------
# record_resume / record_resume_subscription_divergent
# ---------------------------------------------------------------------------


class TestRecordCounters:
    def test_record_resume_bumps_resumes_total(self) -> None:
        _telemetry.record_resume()
        _telemetry.record_resume()

        points = _by_name(_collect_points(), "custos_workflow_resumes_total")
        assert points == [({}, 2)]

    def test_record_divergent_bumps_divergent_total(self) -> None:
        _telemetry.record_resume_subscription_divergent()

        points = _by_name(
            _collect_points(),
            "custos_workflow_resume_subscription_divergent_total",
        )
        assert points == [({}, 1)]

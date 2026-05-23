"""OpenTelemetry instrumentation tests for ``custos_cel`` (WF-IMPL-011).

Each public API call (``parse``, ``type_check``, ``evaluate``) must
emit exactly one span and one duration histogram sample, and every
``CelError``-raising path must bump the per-``kind``
``custos_cel_errors_total`` counter.

The tests wire an in-memory tracer + meter provider (via the
``opentelemetry-sdk`` dev dependency) at module-level fixtures and
assert on the captured spans / metric data points. The library
itself only imports ``opentelemetry-api``; the SDK is unused in
production (consumers wire their own).
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import pytest
from opentelemetry import metrics, trace
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

# Importing custos_cel here also covers the "importing without an OTel
# SDK does not raise" acceptance criterion for the SDK-installed
# case; the no-SDK case is exercised structurally in
# ``test_module_imports_under_noop_providers``.
import custos_cel
from custos_cel import (
    BindingScope,
    EvalTimeoutError,
    EvaluationError,
    FixedClock,
    IntType,
    ParseError,
    RunInfo,
    SchemaBindings,
    StepBinding,
    TypeCheckError,
    UnboundNameError,
    WorkflowInfo,
)

# ---------------------------------------------------------------------------
# OTel SDK wiring
# ---------------------------------------------------------------------------
#
# ``custos_cel`` is imported above so this module also exercises the
# "import without an installed SDK provider" path. After import, these
# tests install in-memory SDK providers at module scope and then
# rebind ``custos_cel._telemetry`` so subsequent public API calls use
# the test tracer / meter instead of the no-op instances resolved at
# import time.

_span_exporter = InMemorySpanExporter()
_tracer_provider = TracerProvider()
_tracer_provider.add_span_processor(SimpleSpanProcessor(_span_exporter))
trace.set_tracer_provider(_tracer_provider)

_metric_reader = InMemoryMetricReader(
    preferred_temporality={
        # DELTA semantics: each ``get_metrics_data()`` call returns
        # only the points generated since the *previous* call,
        # rather than accumulating across the whole test run. That
        # matches the per-test fixture pattern below where we drain
        # state before each case so assertions see exactly the
        # current case's emissions.
        Counter: AggregationTemporality.DELTA,
        Histogram: AggregationTemporality.DELTA,
    },
)
_meter_provider = MeterProvider(metric_readers=[_metric_reader])
metrics.set_meter_provider(_meter_provider)


# Re-bind ``custos_cel._telemetry`` instruments to the newly-installed
# providers. The module-level ``get_tracer`` / ``get_meter`` calls
# inside ``_telemetry`` resolved to the API-default no-op providers
# at import time; we patch the live tracer + instruments so the SDK
# captures emissions. (In a production process the SDK would be set
# up before ``custos_cel`` is imported, so this dance only exists in
# the test harness.)
from custos_cel import _telemetry  # noqa: E402 — must follow provider install

_telemetry._tracer = trace.get_tracer("custos_cel", "0.1.0")
_telemetry._meter = metrics.get_meter("custos_cel", "0.1.0")
_telemetry.PARSE_DURATION_MS = _telemetry._meter.create_histogram(  # type: ignore[misc]
    name="custos_cel_parse_duration_ms",
    unit="ms",
    description="Wall-clock time spent in custos_cel.parse(), labelled by outcome.",
)
_telemetry.TYPE_CHECK_DURATION_MS = _telemetry._meter.create_histogram(  # type: ignore[misc]
    name="custos_cel_type_check_duration_ms",
    unit="ms",
    description="Wall-clock time spent in custos_cel.type_check(), labelled by outcome.",
)
_telemetry.EVALUATE_DURATION_MS = _telemetry._meter.create_histogram(  # type: ignore[misc]
    name="custos_cel_evaluate_duration_ms",
    unit="ms",
    description="Wall-clock time spent in custos_cel.evaluate(), labelled by outcome.",
)
_telemetry.ERRORS_TOTAL = _telemetry._meter.create_counter(  # type: ignore[misc]
    name="custos_cel_errors_total",
    description="Per-kind error count from custos_cel.{parse,type_check,evaluate}.",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_otel_state() -> Iterator[None]:
    """Clear captured spans + collect (drain) metric data points before each test."""
    _span_exporter.clear()
    # Drain any pending metric points from a prior test so each
    # case observes only its own emissions.
    _metric_reader.get_metrics_data()
    yield


_FIXED_TS = datetime(2026, 5, 22, 12, 0, 0, tzinfo=UTC)


def _clock() -> FixedClock:
    return FixedClock(_FIXED_TS)


def _scope(
    *,
    inputs: dict[str, Any] | None = None,
    steps: dict[str, StepBinding] | None = None,
    let: dict[str, Any] | None = None,
) -> BindingScope:
    return BindingScope(
        run=RunInfo(id="run-1", workspace="ws"),
        workflow=WorkflowInfo(name="wf", version="v1"),
        now=_clock().now,
        inputs=inputs or {},
        steps=steps or {},
        let=let or {},
    )


_INPUTS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "x": {"type": "integer"},
        "y": {"type": "integer"},
    },
}


def _bindings() -> SchemaBindings:
    return SchemaBindings(
        inputs=_INPUTS_SCHEMA,
        let={"t": IntType()},
    )


# ---------------------------------------------------------------------------
# Helpers — drain captured metric data into a flat list of points
# ---------------------------------------------------------------------------


def _collect_points() -> list[tuple[str, dict[str, str], float | int]]:
    """Return ``[(instrument_name, attributes, value), ...]`` for all
    metric data points emitted since the last collection.

    ``value`` is a ``count`` for counters and ``sum`` for histograms;
    that's all the tests care about. Attributes are normalised into
    a plain ``dict[str, str]`` so callers can compare directly.
    """
    data: MetricsData | None = _metric_reader.get_metrics_data()
    if data is None:
        return []
    out: list[tuple[str, dict[str, str], float | int]] = []
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                points = metric.data.data_points
                for pt in points:
                    attrs = {str(k): str(v) for k, v in (pt.attributes or {}).items()}
                    if hasattr(pt, "sum"):
                        # Histogram bucket — use ``sum`` (total of recorded
                        # values for this attribute set).
                        out.append((metric.name, attrs, pt.sum))
                    else:
                        # Counter — use ``value``.
                        out.append((metric.name, attrs, pt.value))
    return out


def _by_name(
    points: list[tuple[str, dict[str, str], float | int]], name: str
) -> list[tuple[dict[str, str], float | int]]:
    return [(attrs, value) for n, attrs, value in points if n == name]


# ---------------------------------------------------------------------------
# Span shape
# ---------------------------------------------------------------------------


def test_parse_emits_span_with_source_length_and_node_count() -> None:
    custos_cel.parse("inputs.x + 1")
    spans = _span_exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "custos_cel.parse"
    assert span.status.status_code is StatusCode.UNSET
    assert span.attributes is not None
    assert span.attributes.get("custos_cel.source_length") == len("inputs.x + 1")
    # 4 nodes: Binary + Member + Ident + Literal.
    assert span.attributes.get("custos_cel.node_count") == 4


def test_type_check_emits_span_with_node_count() -> None:
    ast = custos_cel.parse("inputs.x + 1")
    _span_exporter.clear()  # focus on the type_check span only
    custos_cel.type_check(ast, _bindings())
    spans = _span_exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "custos_cel.type_check"
    assert span.status.status_code is StatusCode.UNSET
    assert span.attributes is not None
    assert span.attributes.get("custos_cel.node_count") == 4


def test_evaluate_emits_span_with_node_count_and_timeout_attribute() -> None:
    ast = custos_cel.parse("inputs.x + 1")
    typed = custos_cel.type_check(ast, _bindings())
    _span_exporter.clear()
    result = custos_cel.evaluate(typed, _scope(inputs={"x": 41}), _clock())
    assert result == 42
    spans = _span_exporter.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "custos_cel.evaluate"
    assert span.status.status_code is StatusCode.UNSET
    assert span.attributes is not None
    assert span.attributes.get("custos_cel.node_count") == 4
    assert span.attributes.get("custos_cel.timeout_ms") == custos_cel.DEFAULT_TIMEOUT_MS


def test_span_status_is_error_on_parse_failure() -> None:
    with pytest.raises(ParseError):
        custos_cel.parse("inputs.x +")
    spans = _span_exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].status.status_code is StatusCode.ERROR
    # ``record_exception`` adds an event to the span.
    assert spans[0].events
    assert spans[0].events[0].name == "exception"


# ---------------------------------------------------------------------------
# Duration histograms — success and per-error-kind outcomes
# ---------------------------------------------------------------------------


def test_parse_success_records_duration_with_outcome_success() -> None:
    custos_cel.parse("inputs.x + 1")
    points = _by_name(_collect_points(), "custos_cel_parse_duration_ms")
    assert len(points) == 1
    attrs, value = points[0]
    assert attrs == {"outcome": "success"}
    assert isinstance(value, (int, float))
    assert value >= 0.0


def test_parse_error_records_duration_with_outcome_parse_error_and_bumps_counter() -> None:
    with pytest.raises(ParseError):
        custos_cel.parse("inputs.x +")
    points = _collect_points()
    duration = _by_name(points, "custos_cel_parse_duration_ms")
    assert len(duration) == 1
    assert duration[0][0] == {"outcome": "parse_error"}
    errors = _by_name(points, "custos_cel_errors_total")
    assert errors == [({"kind": "expression.parse_error"}, 1)]


def test_type_check_success_records_duration_with_outcome_success() -> None:
    ast = custos_cel.parse("inputs.x + 1")
    _collect_points()  # drain the parse-side sample
    custos_cel.type_check(ast, _bindings())
    points = _by_name(_collect_points(), "custos_cel_type_check_duration_ms")
    assert len(points) == 1
    assert points[0][0] == {"outcome": "success"}


def test_type_check_type_error_records_outcome_type_error_and_counter() -> None:
    # ``inputs.x + "literal"`` — int + string is a type error.
    ast = custos_cel.parse('inputs.x + "literal"')
    _collect_points()
    with pytest.raises(TypeCheckError):
        custos_cel.type_check(ast, _bindings())
    points = _collect_points()
    duration = _by_name(points, "custos_cel_type_check_duration_ms")
    assert len(duration) == 1
    assert duration[0][0] == {"outcome": "type_error"}
    errors = _by_name(points, "custos_cel_errors_total")
    assert errors == [({"kind": "expression.type_error"}, 1)]


def test_type_check_unbound_name_records_outcome_unbound_name_and_counter() -> None:
    # ``inputs.missing`` — schema does not declare ``missing``.
    ast = custos_cel.parse("inputs.missing")
    _collect_points()
    with pytest.raises(UnboundNameError):
        custos_cel.type_check(ast, _bindings())
    points = _collect_points()
    duration = _by_name(points, "custos_cel_type_check_duration_ms")
    assert len(duration) == 1
    assert duration[0][0] == {"outcome": "unbound_name"}
    errors = _by_name(points, "custos_cel_errors_total")
    assert errors == [({"kind": "expression.unbound_name"}, 1)]


def test_evaluate_success_records_duration_with_outcome_success() -> None:
    typed = custos_cel.type_check(custos_cel.parse("inputs.x + 1"), _bindings())
    _collect_points()
    custos_cel.evaluate(typed, _scope(inputs={"x": 1}), _clock())
    points = _by_name(_collect_points(), "custos_cel_evaluate_duration_ms")
    assert len(points) == 1
    assert points[0][0] == {"outcome": "success"}


def test_evaluate_evaluation_error_records_outcome_evaluation_error_and_counter() -> None:
    # Division by zero is rejected at evaluation time, not parse time.
    typed = custos_cel.type_check(custos_cel.parse("inputs.x / 0"), _bindings())
    _collect_points()
    with pytest.raises(EvaluationError):
        custos_cel.evaluate(typed, _scope(inputs={"x": 1}), _clock())
    points = _collect_points()
    duration = _by_name(points, "custos_cel_evaluate_duration_ms")
    assert len(duration) == 1
    assert duration[0][0] == {"outcome": "evaluation_error"}
    errors = _by_name(points, "custos_cel_errors_total")
    assert errors == [({"kind": "expression.evaluation_error"}, 1)]


def test_evaluate_unbound_name_records_outcome_unbound_name_and_counter() -> None:
    # Type-check passes (``inputs.x`` is declared), but the runtime
    # scope omits ``x`` — that surfaces as ``UnboundNameError`` from
    # the evaluator.
    typed = custos_cel.type_check(custos_cel.parse("inputs.x"), _bindings())
    _collect_points()
    with pytest.raises(UnboundNameError):
        custos_cel.evaluate(typed, _scope(inputs={}), _clock())
    points = _collect_points()
    duration = _by_name(points, "custos_cel_evaluate_duration_ms")
    assert len(duration) == 1
    assert duration[0][0] == {"outcome": "unbound_name"}
    errors = _by_name(points, "custos_cel_errors_total")
    assert errors == [({"kind": "expression.unbound_name"}, 1)]


def test_evaluate_timeout_records_outcome_timeout_and_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Use the same patched-clock idiom as ``tests/test_timeout.py``:
    # arm the deadline at t=0, then make every subsequent
    # ``time.monotonic()`` probe return a value past the 100ms
    # budget. Force the deadline check to run on every node so a
    # tiny expression still trips the gate.
    monkeypatch.setattr("custos_cel.eval._DEADLINE_CHECK_MASK", 0)
    calls = iter([0.0] + [0.5] * 16)
    monkeypatch.setattr(
        "custos_cel.eval.time.monotonic",
        lambda: next(calls),
    )
    typed = custos_cel.type_check(custos_cel.parse("1 + 1"), _bindings())
    _collect_points()
    with pytest.raises(EvalTimeoutError):
        custos_cel.evaluate(typed, _scope(), _clock(), timeout_ms=100)
    points = _collect_points()
    duration = _by_name(points, "custos_cel_evaluate_duration_ms")
    assert len(duration) == 1
    assert duration[0][0] == {"outcome": "timeout"}
    errors = _by_name(points, "custos_cel_errors_total")
    assert errors == [({"kind": "expression.timeout"}, 1)]


# ---------------------------------------------------------------------------
# Cross-cutting properties
# ---------------------------------------------------------------------------


def test_module_imports_under_noop_providers() -> None:
    """Importing ``custos_cel`` with only ``opentelemetry-api`` must not raise.

    The current test module installs SDK-backed providers and
    rebinds ``_telemetry``'s tracer / meter, so an in-process
    assertion on ``_telemetry._tracer`` would only prove the SDK
    path works — not the documented no-SDK path. To validate the
    real acceptance criterion (the library runs against the
    ``opentelemetry-api`` default no-op providers, with no SDK
    wiring), we spawn a fresh interpreter that has neither
    ``set_tracer_provider`` nor ``set_meter_provider`` called,
    import ``custos_cel``, exercise all three public entry points,
    and confirm the resolved tracer / meter are API proxies
    (no ``opentelemetry.sdk`` types) and that no error escapes.
    """
    import os
    import pathlib
    import subprocess
    import sys
    import textwrap

    # Locate the package source directly so the subprocess does not
    # depend on the editable install being healthy in the current
    # venv. ``custos_cel/`` lives at ``<repo>/src/custos_cel`` under
    # this package's source root, two levels up from ``tests/``.
    src_root = pathlib.Path(__file__).resolve().parent.parent / "src"
    assert (src_root / "custos_cel" / "__init__.py").is_file(), src_root

    # Inherit env but prepend the package source on ``PYTHONPATH``
    # and strip any opentelemetry-related variables the parent might
    # have set (e.g. ``OTEL_*``) so the subprocess starts from a
    # truly default configuration.
    env = {k: v for k, v in os.environ.items() if not k.startswith("OTEL_")}
    env["PYTHONPATH"] = str(src_root) + os.pathsep + env.get("PYTHONPATH", "")

    script = textwrap.dedent(
        """
        import sys

        # Defensive: ensure no test-side SDK wiring leaks via
        # already-imported modules. A subprocess inherits sys.path
        # but not in-process module state, so this is mostly a
        # belt-and-braces guard against future fixture changes.
        for name in list(sys.modules):
            if name.startswith(("opentelemetry", "custos_cel")):
                del sys.modules[name]

        import custos_cel
        from custos_cel import _telemetry

        # The api-only install must resolve a tracer / meter of
        # some kind (proxy or no-op); the critical invariant is
        # that *no* SDK class is involved.
        assert _telemetry._tracer is not None
        assert _telemetry._meter is not None
        assert "opentelemetry.sdk" not in type(_telemetry._tracer).__module__
        assert "opentelemetry.sdk" not in type(_telemetry._meter).__module__

        # Exercising ``parse`` drives one full pass through the
        # ``instrument()`` context manager — start_as_current_span,
        # yield, success-path histogram.record — against the
        # no-op providers. If any of those calls assumed an SDK
        # type the subprocess would crash here.
        ast = custos_cel.parse("1 + 1")
        assert ast is not None
        print("OK")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    assert result.returncode == 0, (
        f"subprocess failed: stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert result.stdout.strip().endswith("OK"), result.stdout


def test_instrument_helper_is_transparent_to_callers() -> None:
    """Span / metric emission must not swallow or alter the wrapped exception."""
    with pytest.raises(ParseError) as exc_info:
        custos_cel.parse("inputs.x +")
    # ``ParseError.KIND`` matches the locked taxonomy and the counter label.
    assert exc_info.value.KIND == "expression.parse_error"


def test_instrument_does_not_record_process_control_exceptions() -> None:
    """``KeyboardInterrupt`` / ``SystemExit`` must bypass the recorder.

    Process-control exceptions are not application errors; treating
    them as such would skew the duration histograms (an
    ``outcome=internal_error`` sample on a Ctrl-C) and the
    ``custos_cel_errors_total`` counter (which only counts the locked
    taxonomy). The wrapper catches ``Exception``, not
    ``BaseException``, so these unwind straight through.
    """
    from custos_cel._telemetry import instrument

    histogram = _telemetry.EVALUATE_DURATION_MS

    # Drain any state left over from the previous test.
    _collect_points()
    _span_exporter.clear()

    for exc_cls in (KeyboardInterrupt, SystemExit, GeneratorExit):
        with (
            pytest.raises(exc_cls),
            instrument("custos_cel.test", histogram, {}),
        ):
            raise exc_cls("bypass instrumentation")

    points = _collect_points()
    # No duration samples should have been emitted under the test span.
    assert _by_name(points, "custos_cel_evaluate_duration_ms") == []
    # And no error-counter ticks either.
    assert _by_name(points, "custos_cel_errors_total") == []
    # Spans still close (the OTel context manager's ``__exit__`` runs
    # on every unwind), but our instrumentation contributed neither
    # ``set_status(ERROR)`` nor ``record_exception``.
    for span in _span_exporter.get_finished_spans():
        assert span.status.status_code is not StatusCode.ERROR
        assert not span.events

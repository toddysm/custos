"""OpenTelemetry instrumentation tests for the Definition Compiler (WF-IMPL-027).

Each :func:`custos_workflow.compiler.compile` call must emit exactly one
total-duration span, exactly one sample per pipeline stage that ran
(parse / topology / type_check / retry_policy), and — on failure —
exactly one ``custos_workflow_compile_errors_total`` counter bump
keyed by the structured :attr:`CompileError.kind`.

The tests wire an in-memory tracer + meter provider (via the
``opentelemetry-sdk`` dev dependency) at module-level fixtures and
assert on the captured spans / metric data points. The package
itself only imports ``opentelemetry-api``; the SDK is unused in
production (consumers wire their own). Mirrors
``src/libs/custos-cel/tests/test_observability.py`` (WF-IMPL-011).
"""

from __future__ import annotations

import textwrap
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

# Importing custos_workflow here also covers the "importing without
# an OTel SDK does not raise" acceptance criterion for the
# SDK-installed case; the no-SDK case is exercised structurally in
# ``test_module_imports_under_noop_providers``.
import custos_workflow  # noqa: F401  — see comment above.
from custos_workflow.bindings import InMemoryActivityTypeRegistry
from custos_workflow.compiler import (
    CompileError,
    RunMeta,
)
from custos_workflow.compiler import (
    compile as compile_workflow,
)
from custos_workflow.document import WorkflowDocument, parse_document

# ---------------------------------------------------------------------------
# OTel SDK wiring
# ---------------------------------------------------------------------------
#
# ``custos_workflow`` is imported above so this module also exercises
# the "import without an installed SDK provider" path. After import,
# these tests install in-memory SDK providers at module scope and
# then rebind ``custos_workflow._telemetry`` so subsequent compile
# calls use the test tracer / meter instead of the no-op instances
# resolved at import time.

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


# Re-bind ``custos_workflow._telemetry`` instruments to the newly
# installed providers. The module-level ``get_tracer`` / ``get_meter``
# calls inside ``_telemetry`` resolved to the API-default no-op
# providers at import time; we patch the live tracer + instruments so
# the SDK captures emissions. (In a production process the SDK would
# be set up before ``custos_workflow`` is imported, so this dance
# only exists in the test harness.)
from custos_workflow import _telemetry  # noqa: E402 — must follow provider install

_telemetry._tracer = trace.get_tracer("custos_workflow", "0.1.0")
_telemetry._meter = metrics.get_meter("custos_workflow", "0.1.0")
_telemetry.PARSE_DURATION_MS = _telemetry._meter.create_histogram(  # type: ignore[misc]
    name="custos_workflow_compile_parse_duration_ms",
    unit="ms",
    description="Wall-clock time spent in the call-site collection stage.",
)
_telemetry.TOPOLOGY_DURATION_MS = _telemetry._meter.create_histogram(  # type: ignore[misc]
    name="custos_workflow_compile_topology_duration_ms",
    unit="ms",
    description="Wall-clock time spent in the topology stage.",
)
_telemetry.TYPE_CHECK_DURATION_MS = _telemetry._meter.create_histogram(  # type: ignore[misc]
    name="custos_workflow_compile_type_check_duration_ms",
    unit="ms",
    description="Wall-clock time spent in the type-check stage.",
)
_telemetry.RETRY_POLICY_DURATION_MS = _telemetry._meter.create_histogram(  # type: ignore[misc]
    name="custos_workflow_compile_retry_policy_duration_ms",
    unit="ms",
    description="Wall-clock time spent in the retry-policy resolution stage.",
)
_telemetry.TOTAL_DURATION_MS = _telemetry._meter.create_histogram(  # type: ignore[misc]
    name="custos_workflow_compile_total_duration_ms",
    unit="ms",
    description="Wall-clock time spent in custos_workflow.compile() end-to-end.",
)
_telemetry.ERRORS_TOTAL = _telemetry._meter.create_counter(  # type: ignore[misc]
    name="custos_workflow_compile_errors_total",
    description="Count of compile failures, labelled by structured 'kind'.",
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


def _registry() -> InMemoryActivityTypeRegistry:
    return InMemoryActivityTypeRegistry(
        {
            "security/scan@1": {
                "type": "object",
                "properties": {
                    "critical": {"type": "integer"},
                    "findings": {"type": "array", "items": {"type": "string"}},
                },
            },
        }
    )


def _run_meta() -> RunMeta:
    return RunMeta(
        workspace_id="ws-001",
        workflow_version_id="wfv-001",
        workflow_name="pipeline",
        workflow_version_label="v1",
        started_at_default=datetime(2026, 5, 1, tzinfo=UTC),
    )


def _doc(steps: list[dict[str, Any]]) -> WorkflowDocument:
    return WorkflowDocument.model_validate(
        {
            "apiVersion": "custos.dev/v1",
            "kind": "Workflow",
            "metadata": {"name": "pipeline", "workspace": "security"},
            "spec": {
                "inputs": {
                    "target": {"type": "string", "required": True},
                    "threshold": {"type": "integer", "default": 10},
                },
                "steps": list(steps),
            },
        }
    )


_HAPPY_DOC = textwrap.dedent(
    """\
    apiVersion: custos.dev/v1
    kind: Workflow
    metadata:
      name: pipeline
      workspace: security
    spec:
      inputs:
        target:
          type: string
          required: true
      steps:
        - id: scan
          activity: security/scan@1
          connector: primary
          with:
            image: ${{ inputs.target }}
        - id: gate
          let:
            verdict: ${{ steps.scan.outputs.critical > 0 }}
    """
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
    points: list[tuple[str, dict[str, str], float | int]],
    name: str,
) -> list[tuple[dict[str, str], float | int]]:
    return [(attrs, value) for n, attrs, value in points if n == name]


# ---------------------------------------------------------------------------
# Span shape — happy path
# ---------------------------------------------------------------------------


def test_happy_path_emits_outer_and_four_stage_spans() -> None:
    doc = parse_document(_HAPPY_DOC)
    compile_workflow(doc, _run_meta(), _registry())
    # The compiler calls into ``custos_cel`` which has its own
    # WF-IMPL-011 instrumentation; filter to our own namespace so
    # the assertion only covers ``custos_workflow.compile.*``.
    span_names = sorted(
        s.name for s in _span_exporter.get_finished_spans() if s.name.startswith("custos_workflow.")
    )
    # Outer + parse + topology (preflight) + type_check + topology
    # (build) + retry_policy = 6 spans. Topology emits twice.
    assert span_names == [
        "custos_workflow.compile",
        "custos_workflow.compile.parse",
        "custos_workflow.compile.retry_policy",
        "custos_workflow.compile.topology",
        "custos_workflow.compile.topology",
        "custos_workflow.compile.type_check",
    ]


def test_outer_span_carries_step_edge_callsite_attributes() -> None:
    doc = parse_document(_HAPPY_DOC)
    compile_workflow(doc, _run_meta(), _registry())
    outer = [s for s in _span_exporter.get_finished_spans() if s.name == "custos_workflow.compile"]
    assert len(outer) == 1
    span = outer[0]
    assert span.status.status_code is StatusCode.UNSET
    assert span.attributes is not None
    # Happy doc: ``scan`` + ``gate`` = 2 steps.
    assert span.attributes.get("custos_workflow.step_count") == 2
    # 1 edge: ``scan`` -> ``gate`` (data-dep from ``let.verdict``).
    assert span.attributes.get("custos_workflow.edge_count") == 1
    # Call sites: ``scan.with.image`` + ``gate.let.verdict`` = 2.
    assert span.attributes.get("custos_workflow.call_site_count") == 2


def test_outer_span_is_parent_of_each_stage_span() -> None:
    doc = parse_document(_HAPPY_DOC)
    compile_workflow(doc, _run_meta(), _registry())
    spans = _span_exporter.get_finished_spans()
    outer = next(s for s in spans if s.name == "custos_workflow.compile")
    # The four direct stage spans (``custos_workflow.compile.*``)
    # are direct children of the outer span. ``custos_cel.*`` spans
    # raised by call-site parsing / type checking are nested under
    # the stage spans, not the outer span, so we filter to our
    # namespace and exclude the outer itself.
    stage_spans = [
        s
        for s in spans
        if s.name.startswith("custos_workflow.compile.") and s.name != "custos_workflow.compile"
    ]
    assert outer.context is not None
    assert len(stage_spans) == 5  # parse + 2*topology + type_check + retry_policy
    for stage in stage_spans:
        assert stage.parent is not None
        assert stage.parent.span_id == outer.context.span_id


# ---------------------------------------------------------------------------
# Duration histograms — success path
# ---------------------------------------------------------------------------


def test_happy_path_records_one_success_sample_per_stage_histogram() -> None:
    doc = parse_document(_HAPPY_DOC)
    compile_workflow(doc, _run_meta(), _registry())
    points = _collect_points()
    # Five histograms; topology gets 2 success samples (preflight +
    # build), every other gets 1. Counter must not have ticked at all.
    parse = _by_name(points, "custos_workflow_compile_parse_duration_ms")
    topology = _by_name(points, "custos_workflow_compile_topology_duration_ms")
    type_check = _by_name(points, "custos_workflow_compile_type_check_duration_ms")
    retry_policy = _by_name(points, "custos_workflow_compile_retry_policy_duration_ms")
    total = _by_name(points, "custos_workflow_compile_total_duration_ms")
    errors = _by_name(points, "custos_workflow_compile_errors_total")
    assert parse == [({"outcome": "success"}, parse[0][1])]
    # Topology gets a single bucket with attrs={outcome: success} but
    # ``sum`` is the total of both samples; the count is 2 — DELTA
    # temporality preserves that.
    assert len(topology) == 1
    assert topology[0][0] == {"outcome": "success"}
    assert len(type_check) == 1
    assert type_check[0][0] == {"outcome": "success"}
    assert len(retry_policy) == 1
    assert retry_policy[0][0] == {"outcome": "success"}
    assert len(total) == 1
    assert total[0][0] == {"outcome": "success"}
    assert errors == []


# ---------------------------------------------------------------------------
# Per-stage outcomes — failure paths
# ---------------------------------------------------------------------------


def test_parse_failure_records_parse_error_outcome_and_kind_counter() -> None:
    # Trailing ``.`` makes the CEL parser reject the placeholder.
    doc = _doc(
        [
            {
                "id": "scan",
                "activity": "security/scan@1",
                "connector": "primary",
                "with": {"image": "${{ inputs. }}"},
            },
        ]
    )
    with pytest.raises(CompileError):
        compile_workflow(doc, _run_meta(), _registry())
    points = _collect_points()
    parse = _by_name(points, "custos_workflow_compile_parse_duration_ms")
    total = _by_name(points, "custos_workflow_compile_total_duration_ms")
    errors = _by_name(points, "custos_workflow_compile_errors_total")
    assert len(parse) == 1
    assert parse[0][0] == {"outcome": "parse_error"}
    assert len(total) == 1
    assert total[0][0] == {"outcome": "parse_error"}
    # Counter bumps exactly once even though the error propagates
    # through both the per-stage and the outer wrapper.
    assert errors == [({"kind": "compile.parse_error"}, 1)]


def test_bindings_failure_records_only_total_with_bindings_error_kind() -> None:
    # ``unknown/activity@1`` is not in the registry — derive_bindings
    # raises ActivityTypeNotFoundError, wrapped as
    # BindingsCompileError. The bindings stage has no per-stage
    # histogram (issue #361 scope); only the total wrapper records.
    doc = _doc(
        [
            {
                "id": "scan",
                "activity": "unknown/activity@1",
                "connector": "primary",
            },
        ]
    )
    with pytest.raises(CompileError):
        compile_workflow(doc, _run_meta(), _registry())
    points = _collect_points()
    # Parse stage succeeded — recorded as success.
    parse = _by_name(points, "custos_workflow_compile_parse_duration_ms")
    assert len(parse) == 1
    assert parse[0][0] == {"outcome": "success"}
    # Topology / type-check / retry-policy never ran.
    assert _by_name(points, "custos_workflow_compile_topology_duration_ms") == []
    assert _by_name(points, "custos_workflow_compile_type_check_duration_ms") == []
    assert _by_name(points, "custos_workflow_compile_retry_policy_duration_ms") == []
    total = _by_name(points, "custos_workflow_compile_total_duration_ms")
    assert len(total) == 1
    assert total[0][0] == {"outcome": "bindings_error"}
    errors = _by_name(points, "custos_workflow_compile_errors_total")
    assert errors == [({"kind": "compile.bindings_error"}, 1)]


def test_topology_preflight_failure_records_topology_error_outcome() -> None:
    # ``scan`` references ``steps.gate.outputs.*`` but ``gate`` is
    # declared later → forward reference, surfaced by the topology
    # pre-flight pass (stage 2.5).
    doc = _doc(
        [
            {
                "id": "scan",
                "activity": "security/scan@1",
                "connector": "primary",
                "with": {"image": "${{ steps.gate.outputs.x }}"},
            },
            {
                "id": "gate",
                "let": {"x": "${{ inputs.target }}"},
            },
        ]
    )
    with pytest.raises(CompileError):
        compile_workflow(doc, _run_meta(), _registry())
    points = _collect_points()
    topology = _by_name(points, "custos_workflow_compile_topology_duration_ms")
    assert len(topology) == 1
    assert topology[0][0] == {"outcome": "topology_error"}
    total = _by_name(points, "custos_workflow_compile_total_duration_ms")
    assert len(total) == 1
    assert total[0][0] == {"outcome": "topology_error"}
    errors = _by_name(points, "custos_workflow_compile_errors_total")
    assert errors == [({"kind": "compile.topology_error"}, 1)]


def test_topology_build_failure_records_topology_error_outcome() -> None:
    # Two activity steps that both ``needs`` each other → cycle
    # caught at stage 4 (the explicit + implicit edge + cycle +
    # sort pass), not at the preflight.
    doc = _doc(
        [
            {
                "id": "scan",
                "activity": "security/scan@1",
                "connector": "primary",
                "needs": ["gate"],
            },
            {
                "id": "gate",
                "activity": "security/scan@1",
                "connector": "primary",
                "needs": ["scan"],
            },
        ]
    )
    with pytest.raises(CompileError):
        compile_workflow(doc, _run_meta(), _registry())
    points = _collect_points()
    topology = _by_name(points, "custos_workflow_compile_topology_duration_ms")
    # Preflight succeeded; build failed → two samples on the bucket.
    assert len(topology) == 2
    outcomes = sorted(attrs["outcome"] for attrs, _value in topology)
    assert outcomes == ["success", "topology_error"]
    total = _by_name(points, "custos_workflow_compile_total_duration_ms")
    assert len(total) == 1
    assert total[0][0] == {"outcome": "topology_error"}
    errors = _by_name(points, "custos_workflow_compile_errors_total")
    assert errors == [({"kind": "compile.topology_error"}, 1)]


def test_type_check_failure_records_type_error_outcome() -> None:
    # ``inputs.target`` is a string, ``inputs.threshold`` is an int —
    # comparing them with ``>`` is a CEL type error caught at the
    # type-check stage.
    doc = _doc(
        [
            {
                "id": "gate",
                "let": {"verdict": "${{ inputs.target > inputs.threshold }}"},
            },
        ]
    )
    with pytest.raises(CompileError):
        compile_workflow(doc, _run_meta(), _registry())
    points = _collect_points()
    type_check = _by_name(points, "custos_workflow_compile_type_check_duration_ms")
    assert len(type_check) == 1
    assert type_check[0][0] == {"outcome": "type_error"}
    total = _by_name(points, "custos_workflow_compile_total_duration_ms")
    assert len(total) == 1
    assert total[0][0] == {"outcome": "type_error"}
    errors = _by_name(points, "custos_workflow_compile_errors_total")
    assert errors == [({"kind": "compile.type_error"}, 1)]


def test_retry_policy_failure_records_retry_policy_error_outcome() -> None:
    # Malformed ISO-8601 duration in ``initialDelay`` survives
    # Pydantic (it's typed as ``str``) but fails at the
    # retry-policy resolution stage.
    doc = _doc(
        [
            {
                "id": "scan",
                "activity": "security/scan@1",
                "connector": "primary",
                "retry": {
                    "maxAttempts": 3,
                    "backoff": {"initialDelay": "NOT-AN-ISO-DURATION"},
                },
            },
        ]
    )
    with pytest.raises(CompileError):
        compile_workflow(doc, _run_meta(), _registry())
    points = _collect_points()
    retry = _by_name(points, "custos_workflow_compile_retry_policy_duration_ms")
    assert len(retry) == 1
    assert retry[0][0] == {"outcome": "retry_policy_error"}
    total = _by_name(points, "custos_workflow_compile_total_duration_ms")
    assert len(total) == 1
    assert total[0][0] == {"outcome": "retry_policy_error"}
    errors = _by_name(points, "custos_workflow_compile_errors_total")
    assert errors == [({"kind": "compile.retry_policy_error"}, 1)]


# ---------------------------------------------------------------------------
# Span status — every error path sets ERROR on the outer span
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "doc_steps",
    [
        # Parse failure.
        [
            {
                "id": "scan",
                "activity": "security/scan@1",
                "connector": "primary",
                "with": {"image": "${{ inputs. }}"},
            },
        ],
        # Bindings failure.
        [
            {
                "id": "scan",
                "activity": "unknown/activity@1",
                "connector": "primary",
            },
        ],
        # Type-check failure.
        [
            {
                "id": "gate",
                "let": {"verdict": "${{ inputs.target > inputs.threshold }}"},
            },
        ],
    ],
)
def test_outer_span_status_is_error_on_failure(doc_steps: list[dict[str, Any]]) -> None:
    doc = _doc(doc_steps)
    with pytest.raises(CompileError):
        compile_workflow(doc, _run_meta(), _registry())
    outer = next(
        s for s in _span_exporter.get_finished_spans() if s.name == "custos_workflow.compile"
    )
    assert outer.status.status_code is StatusCode.ERROR
    # ``record_exception`` adds an event to the span.
    assert outer.events
    assert outer.events[0].name == "exception"


# ---------------------------------------------------------------------------
# Cross-cutting properties
# ---------------------------------------------------------------------------


def test_module_imports_under_noop_providers() -> None:
    """Importing ``custos_workflow`` with only ``opentelemetry-api`` must not raise.

    The current test module installs SDK-backed providers and
    rebinds ``_telemetry``'s tracer / meter, so an in-process
    assertion on ``_telemetry._tracer`` would only prove the SDK
    path works — not the documented no-SDK path. To validate the
    real acceptance criterion (the package runs against the
    ``opentelemetry-api`` default no-op providers, with no SDK
    wiring), we spawn a fresh interpreter that has neither
    ``set_tracer_provider`` nor ``set_meter_provider`` called,
    import ``custos_workflow``, and confirm the resolved tracer /
    meter are API proxies (no ``opentelemetry.sdk`` types).
    """
    import os
    import subprocess
    import sys
    import textwrap as _tw

    env = {k: v for k, v in os.environ.items() if not k.startswith("OTEL_")}

    script = _tw.dedent(
        """
        import sys

        # Defensive: ensure no test-side SDK wiring leaks via
        # already-imported modules. A subprocess inherits sys.path
        # but not in-process module state, so this is mostly a
        # belt-and-braces guard against future fixture changes.
        for name in list(sys.modules):
            if name.startswith(("opentelemetry", "custos_workflow")):
                del sys.modules[name]

        import custos_workflow  # noqa: F401
        from custos_workflow import _telemetry

        # The api-only install must resolve a tracer / meter of
        # some kind (proxy or no-op); the critical invariant is
        # that *no* SDK class is involved.
        assert _telemetry._tracer is not None
        assert _telemetry._meter is not None
        assert "opentelemetry.sdk" not in type(_telemetry._tracer).__module__
        assert "opentelemetry.sdk" not in type(_telemetry._meter).__module__

        # Exercising the success-path of the wrapper drives a full
        # pass through start_as_current_span + histogram.record
        # against the no-op providers; if any call assumed an SDK
        # type the subprocess would crash here.
        with _telemetry.observe_compile_total():
            pass
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


def test_instrument_does_not_record_process_control_exceptions() -> None:
    """``KeyboardInterrupt`` / ``SystemExit`` must bypass the recorder.

    Process-control exceptions are not application errors; treating
    them as such would skew the duration histograms (an
    ``outcome=internal_error`` sample on a Ctrl-C) and the
    ``custos_workflow_compile_errors_total`` counter (which only
    counts the locked taxonomy). The wrapper catches ``Exception``,
    not ``BaseException``, so these unwind straight through.
    """
    from custos_workflow._telemetry import (
        _TOTAL_OUTCOMES,
        TOTAL_DURATION_MS,
        instrument,
    )

    # Drain any state left over from the previous test.
    _collect_points()
    _span_exporter.clear()

    for exc_cls in (KeyboardInterrupt, SystemExit, GeneratorExit):
        with (
            pytest.raises(exc_cls),
            instrument(
                "custos_workflow.compile.test",
                TOTAL_DURATION_MS,
                _TOTAL_OUTCOMES,
                count_errors=True,
            ),
        ):
            raise exc_cls("bypass instrumentation")

    points = _collect_points()
    # No duration samples should have been emitted under the test span.
    assert _by_name(points, "custos_workflow_compile_total_duration_ms") == []
    # And no error-counter ticks either.
    assert _by_name(points, "custos_workflow_compile_errors_total") == []
    # Spans still close (the OTel context manager's ``__exit__`` runs
    # on every unwind), but our instrumentation contributed neither
    # ``set_status(ERROR)`` nor ``record_exception``.
    for span in _span_exporter.get_finished_spans():
        assert span.status.status_code is not StatusCode.ERROR
        assert not span.events


def test_instrument_internal_error_outcome_when_kind_not_in_mapping() -> None:
    """An unexpected exception type maps to ``outcome=internal_error``."""
    from custos_workflow._telemetry import TOTAL_DURATION_MS, instrument

    _collect_points()

    with (
        pytest.raises(ValueError),
        instrument(
            "custos_workflow.compile.test",
            TOTAL_DURATION_MS,
            {"compile.parse_error": "parse_error"},  # narrow mapping
            count_errors=True,
        ),
    ):
        raise ValueError("not a CompileError")

    points = _by_name(_collect_points(), "custos_workflow_compile_total_duration_ms")
    assert len(points) == 1
    assert points[0][0] == {"outcome": "internal_error"}
    # The counter only bumps for CompileError instances, not for
    # arbitrary exceptions, so it stays empty.
    errors = _by_name(_collect_points(), "custos_workflow_compile_errors_total")
    assert errors == []

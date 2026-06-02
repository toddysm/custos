"""OpenTelemetry instrumentation tests for the Step Coordinator (WF-IMPL-058).

Every :meth:`StepCoordinator.execute` dispatch must emit exactly
one ``custos_workflow.step.execute`` span and exactly one
``custos_workflow_step_execute_duration_ms`` sample labelled by
``step_kind`` and ``outcome``. The ``ActivityStepHandler`` must
additionally emit:

- one ``custos_workflow.step.bind_connectors`` span per attempt;
- one ``custos_workflow.step.schedule_activity`` span + one
  ``custos_workflow_activity_schedule_duration_ms`` sample per
  attempt, labelled by the envelope's ``class`` (or
  ``internal_error`` when the call raises);
- one ``custos_workflow_step_attempts_total`` counter bump per
  attempt, labelled with the final attempt class;
- one ``custos_workflow.step.retry_decision`` span per retry
  driver consultation.

Step Coordinator-level failures (``StepFailed`` envelopes whose
``kind`` is in :data:`LOCKED_STEP_KINDS`) must bump
``custos_workflow_step_errors_total`` exactly once.

Mirrors the SDK-rebind pattern in ``tests/test_observability.py``
(WF-IMPL-027) and ``tests/test_telemetry.py`` (WF-IMPL-044).
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, cast

import pytest
from custos_cel import FixedClock
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
from custos_workflow.clients.activity_runtime import (
    ACTIVITY_RESULT_CLASSES,
    ActivityResultClass,
    ActivityResultEnvelope,
    ScheduleActivityRequest,
)
from custos_workflow.clients.connector import (
    BindForStepRequest,
    BindForStepResponse,
)
from custos_workflow.document import ActivityStep, LetStep, WaitStep, WorkflowStep
from custos_workflow.graph import (
    ExecutionGraph,
    ExecutionNode,
    GraphMetadata,
    OnErrorActionTag,
    OnErrorRoute,
    PrimitiveHandler,
    StepKind,
)
from custos_workflow.runs import (
    RunId,
    StepExecutionContext,
    StepFailed,
    StepResult,
    StepSucceeded,
)
from custos_workflow.runtime import FakeWorkflowContext
from custos_workflow.steps.activity_step import ActivityStepHandler
from custos_workflow.steps.coordinator import StepCoordinator
from custos_workflow.steps.errors import (
    LOCKED_STEP_KINDS,
    StepKindNotImplementedError,
)

# ---------------------------------------------------------------------------
# OTel SDK wiring (mirrors tests/test_observability.py).
# ---------------------------------------------------------------------------
#
# We deliberately do NOT call ``trace.set_tracer_provider`` /
# ``metrics.set_meter_provider`` here — those are process-global
# and a sibling test module (``tests/test_observability.py``)
# already installs *its* providers when collected first. OTel
# rejects the second installation as a no-op + warning, which
# means our exporter never sees emissions when the suite is run
# end-to-end. Instead we grab tracers/meters directly off our own
# local provider instances and rebind ``_telemetry._tracer`` +
# ``_telemetry._meter`` to those, so every instrument we recreate
# below is wired to *our* in-memory exporter regardless of what
# the global default is.

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

# WF-IMPL-058 instruments — re-bound to SDK-backed instances so the
# in-memory exporter captures emissions. The production package
# only imports ``opentelemetry-api`` and resolves no-op instruments
# at import time; the test harness has to swap them. We keep the
# SDK-backed instances as module-level locals and pin them onto
# ``_telemetry`` inside the autouse fixture (with save/restore on
# teardown) so a sibling test module's bindings survive after our
# tests run.
_sdk_tracer = _tracer_provider.get_tracer("custos_workflow", "0.1.0")
_sdk_meter = _meter_provider.get_meter("custos_workflow", "0.1.0")
_step_execute_duration_ms = _sdk_meter.create_histogram(
    name="custos_workflow_step_execute_duration_ms",
    unit="ms",
    description=(
        "Wall-clock time spent in StepCoordinator.execute, labelled by step_kind and outcome."
    ),
)
_activity_schedule_duration_ms = _sdk_meter.create_histogram(
    name="custos_workflow_activity_schedule_duration_ms",
    unit="ms",
    description=(
        "Wall-clock time spent in ActivityRuntimeClient.schedule_activity, "
        "labelled by step_kind and envelope class."
    ),
)
_step_attempts_total = _sdk_meter.create_counter(
    name="custos_workflow_step_attempts_total",
    description=(
        "Count of activity-step attempts, labelled by step_kind and the envelope's final_class."
    ),
)
_step_errors_total = _sdk_meter.create_counter(
    name="custos_workflow_step_errors_total",
    description=(
        "Count of Step Coordinator failures, labelled by the "
        "structured 'kind' from LOCKED_STEP_KINDS."
    ),
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_otel_state() -> Iterator[None]:
    """Pin ``_telemetry`` bindings to this module's SDK exporter then restore.

    Saves the current ``_telemetry._tracer`` / ``_meter`` and the
    four WF-IMPL-058 instruments, swaps in this module's
    in-memory-exporter-backed instances, runs the test, then
    restores the originals so a sibling test module's bindings
    (e.g. ``test_observability.py``) survive intact.
    """
    saved_tracer = _telemetry._tracer
    saved_meter = _telemetry._meter
    saved_step_exec = _telemetry.STEP_EXECUTE_DURATION_MS
    saved_sched = _telemetry.ACTIVITY_SCHEDULE_DURATION_MS
    saved_attempts = _telemetry.STEP_ATTEMPTS_TOTAL
    saved_errors = _telemetry.STEP_ERRORS_TOTAL
    _telemetry._tracer = _sdk_tracer
    _telemetry._meter = _sdk_meter
    _telemetry.STEP_EXECUTE_DURATION_MS = _step_execute_duration_ms  # type: ignore[misc]
    _telemetry.ACTIVITY_SCHEDULE_DURATION_MS = _activity_schedule_duration_ms  # type: ignore[misc]
    _telemetry.STEP_ATTEMPTS_TOTAL = _step_attempts_total  # type: ignore[misc]
    _telemetry.STEP_ERRORS_TOTAL = _step_errors_total  # type: ignore[misc]
    _span_exporter.clear()
    _metric_reader.get_metrics_data()
    try:
        yield
    finally:
        _telemetry._tracer = saved_tracer
        _telemetry._meter = saved_meter
        _telemetry.STEP_EXECUTE_DURATION_MS = saved_step_exec  # type: ignore[misc]
        _telemetry.ACTIVITY_SCHEDULE_DURATION_MS = saved_sched  # type: ignore[misc]
        _telemetry.STEP_ATTEMPTS_TOTAL = saved_attempts  # type: ignore[misc]
        _telemetry.STEP_ERRORS_TOTAL = saved_errors  # type: ignore[misc]


_CLOCK_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_CLOCK = FixedClock(_CLOCK_NOW)


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _StubActivityClient:
    """``ActivityRuntimeClient`` stand-in returning a fixed envelope."""

    def __init__(self, envelope: ActivityResultEnvelope | Exception) -> None:
        self._envelope = envelope
        self.calls: list[ScheduleActivityRequest] = []

    def schedule_activity(self, request: ScheduleActivityRequest) -> ActivityResultEnvelope:
        self.calls.append(request)
        if isinstance(self._envelope, Exception):
            raise self._envelope
        return self._envelope

    def cancel_activity(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        raise NotImplementedError


class _StubConnectorClient:
    """``ConnectorClient`` stand-in returning an empty contexts map."""

    def __init__(self, raises: Exception | None = None) -> None:
        self._raises = raises
        self.calls: list[BindForStepRequest] = []

    def bind_for_step(self, request: BindForStepRequest) -> BindForStepResponse:
        self.calls.append(request)
        if self._raises is not None:
            raise self._raises
        return BindForStepResponse(contexts=MappingProxyType({}))

    def release_step(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        raise NotImplementedError


class _RecordingLetHandler:
    """Minimal ``LetStepHandler`` stand-in for coordinator dispatch tests."""

    def __init__(self, result: StepResult | None = None) -> None:
        self._result: StepResult = (
            result if result is not None else StepSucceeded(outputs=MappingProxyType({"ok": True}))
        )

    def execute(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        step_id: str,
    ) -> StepResult:
        return self._result


class _RecordingActivityHandler:
    """Minimal ``ActivityStepHandler`` stand-in returning a fixed result."""

    def __init__(self, result: StepResult | None = None) -> None:
        self._result: StepResult = (
            result if result is not None else StepSucceeded(outputs=MappingProxyType({"ok": True}))
        )

    def execute(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        step_id: str,
    ) -> StepResult:
        return self._result


# ---------------------------------------------------------------------------
# Graph + context builders
# ---------------------------------------------------------------------------


def _activity_node(
    step_id: str = "scan",
    *,
    activity: str = "scanners/trivy@1",
    with_inputs: dict[str, Any] | None = None,
) -> ExecutionNode:
    payload: dict[str, Any] = {"id": step_id, "activity": activity}
    if with_inputs is not None:
        payload["with"] = with_inputs
    # Compiler-equivalent terminal fallback routes — every
    # non-success envelope class is consumed by exactly one
    # ``FailNow`` route so the retry driver never falls off the
    # end of the table.
    fallback_routes: tuple[OnErrorRoute, ...] = (
        OnErrorRoute(action=OnErrorActionTag.FAIL, cls="retryable"),
        OnErrorRoute(action=OnErrorActionTag.FAIL, cls="permanent"),
        OnErrorRoute(action=OnErrorActionTag.FAIL, cls="cancelled"),
    )
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.ACTIVITY,
        primitive_handler=PrimitiveHandler.ACTIVITY_RUNTIME,
        retry_policy=None,
        on_error_routes=fallback_routes,
        call_sites={},
        step_source=ActivityStep.model_validate(payload),
    )


def _let_node(step_id: str = "derive") -> ExecutionNode:
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.LET,
        primitive_handler=PrimitiveHandler.EXPRESSION_INLINE,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=LetStep.model_validate({"id": step_id, "let": {"x": 1}}),
    )


def _workflow_node(step_id: str = "child") -> ExecutionNode:
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.WORKFLOW,
        primitive_handler=PrimitiveHandler.SUB_ORCHESTRATION,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=WorkflowStep.model_validate({"id": step_id, "workflow": "ws/sub@1"}),
    )


def _wait_node(step_id: str = "pause") -> ExecutionNode:
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.WAIT,
        primitive_handler=PrimitiveHandler.RUN_CONTROLLER_TIMER,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=WaitStep.model_validate({"id": step_id, "wait": "PT5M"}),
    )


def _graph(*nodes: ExecutionNode) -> ExecutionGraph:
    return ExecutionGraph(
        nodes=tuple(nodes),
        edges=(),
        topological_order=tuple(n.step_id for n in nodes),
        metadata=GraphMetadata(
            workflow_name="pipeline",
            workflow_workspace="ws",
            document_api_version="custos.dev/v1",
        ),
    )


def _ctx(run_id: str = "run-1") -> StepExecutionContext:
    return StepExecutionContext(
        run_id=RunId(run_id),
        workspace_id="ws-1",
        workflow_version_id="wf-1",
        inputs=MappingProxyType({}),
        workflow_context=FakeWorkflowContext(instance_id=run_id, now=_CLOCK_NOW),
        outputs=MappingProxyType({}),
        clock=_CLOCK,
    )


# ---------------------------------------------------------------------------
# Metric drain helpers (copied from tests/test_observability.py)
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


# ---------------------------------------------------------------------------
# StepCoordinator.execute — span shape + outcome label
# ---------------------------------------------------------------------------


class TestStepExecuteSpanAndHistogram:
    def test_let_success_emits_one_span_and_one_ok_histogram_sample(self) -> None:
        coord = StepCoordinator(
            _RecordingActivityHandler(),
            let_handler=_RecordingLetHandler(),
        )

        result = coord.execute(_ctx(), _graph(_let_node()), "derive")

        assert isinstance(result, StepSucceeded)
        spans = _spans_by_name("custos_workflow.step.execute")
        assert len(spans) == 1
        assert spans[0].attributes is not None
        assert spans[0].attributes["step_kind"] == "let"
        assert spans[0].status.status_code is StatusCode.UNSET

        samples = _by_name(_collect_points(), "custos_workflow_step_execute_duration_ms")
        assert len(samples) == 1
        attrs, _value = samples[0]
        assert attrs == {"step_kind": "let", "outcome": "ok"}

    def test_activity_success_emits_one_span_and_one_ok_histogram_sample(self) -> None:
        coord = StepCoordinator(
            _RecordingActivityHandler(),
            let_handler=_RecordingLetHandler(),
        )

        coord.execute(_ctx(), _graph(_activity_node()), "scan")

        spans = _spans_by_name("custos_workflow.step.execute")
        assert len(spans) == 1
        assert spans[0].attributes is not None
        assert spans[0].attributes["step_kind"] == "activity"

        samples = _by_name(_collect_points(), "custos_workflow_step_execute_duration_ms")
        assert len(samples) == 1
        attrs, _value = samples[0]
        assert attrs == {"step_kind": "activity", "outcome": "ok"}

    def test_no_step_error_counter_bump_on_success(self) -> None:
        coord = StepCoordinator(
            _RecordingActivityHandler(),
            let_handler=_RecordingLetHandler(),
        )

        coord.execute(_ctx(), _graph(_let_node()), "derive")

        errors = _by_name(_collect_points(), "custos_workflow_step_errors_total")
        assert errors == []


# ---------------------------------------------------------------------------
# Sub-orchestration node — dispatched inline by the orchestrator, so the
# coordinator defensively raises StepKindNotImplementedError (WF-IMPL-093)
# ---------------------------------------------------------------------------


class TestSubOrchestrationDefensiveRaise:
    def test_raises_kind_not_implemented(self) -> None:
        coord = StepCoordinator(
            _RecordingActivityHandler(),
            let_handler=_RecordingLetHandler(),
        )

        with pytest.raises(StepKindNotImplementedError):
            coord.execute(_ctx(), _graph(_workflow_node()), "child")

    def test_sub_orchestration_records_error_counter_once(self) -> None:
        coord = StepCoordinator(
            _RecordingActivityHandler(),
            let_handler=_RecordingLetHandler(),
        )

        with pytest.raises(StepKindNotImplementedError):
            coord.execute(_ctx(), _graph(_workflow_node()), "child")

        errors = _by_name(_collect_points(), "custos_workflow_step_errors_total")
        assert len(errors) == 1
        attrs, value = errors[0]
        assert attrs == {"kind": "step.kind_not_implemented"}
        assert value == 1

    def test_sub_orchestration_records_histogram_with_kind_not_implemented(self) -> None:
        # The defensive arm now *raises* rather than returning a
        # StepFailed envelope, so the ``observe_step_execute`` wrapper
        # records the dispatch with ``outcome=kind_not_implemented``
        # (the bare suffix of the raised error's kind), mirroring the
        # wait / run_controller_timer nodes the orchestrator also
        # dispatches inline.
        coord = StepCoordinator(
            _RecordingActivityHandler(),
            let_handler=_RecordingLetHandler(),
        )

        with pytest.raises(StepKindNotImplementedError):
            coord.execute(_ctx(), _graph(_workflow_node()), "child")

        samples = _by_name(_collect_points(), "custos_workflow_step_execute_duration_ms")
        assert len(samples) == 1
        attrs, _value = samples[0]
        assert attrs == {"step_kind": "workflow", "outcome": "kind_not_implemented"}


# ---------------------------------------------------------------------------
# Wait node — raises StepKindNotImplementedError → outcome label
# ---------------------------------------------------------------------------


class TestWaitNodeRaisesAndRecords:
    def test_wait_step_raises_kind_not_implemented(self) -> None:
        coord = StepCoordinator(
            _RecordingActivityHandler(),
            let_handler=_RecordingLetHandler(),
        )

        with pytest.raises(StepKindNotImplementedError):
            coord.execute(_ctx(), _graph(_wait_node()), "pause")

    def test_wait_step_records_outcome_label_for_raised_error(self) -> None:
        coord = StepCoordinator(
            _RecordingActivityHandler(),
            let_handler=_RecordingLetHandler(),
        )

        with pytest.raises(StepKindNotImplementedError):
            coord.execute(_ctx(), _graph(_wait_node()), "pause")

        # Drain DELTA temporality once and assert both the
        # histogram sample and the error counter from the same
        # snapshot — Copilot review #444 thread:
        # ``StepCoordinator.execute`` must bump
        # ``custos_workflow_step_errors_total`` for *raised*
        # ``StepCoordinatorError``s, not just ``StepFailed``
        # envelopes.
        points = _collect_points()

        hist = _by_name(points, "custos_workflow_step_execute_duration_ms")
        assert len(hist) == 1
        attrs, _value = hist[0]
        # The outcome label is the bare suffix of the
        # ``step.kind_not_implemented`` kind, as pinned by
        # ``_STEP_EXECUTE_OUTCOMES``.
        assert attrs == {"step_kind": "wait", "outcome": "kind_not_implemented"}

        errors = _by_name(points, "custos_workflow_step_errors_total")
        assert len(errors) == 1
        err_attrs, err_value = errors[0]
        assert err_attrs == {"kind": "step.kind_not_implemented"}
        assert err_value == 1

    def test_wait_step_span_status_is_error_with_exception(self) -> None:
        coord = StepCoordinator(
            _RecordingActivityHandler(),
            let_handler=_RecordingLetHandler(),
        )

        with pytest.raises(StepKindNotImplementedError):
            coord.execute(_ctx(), _graph(_wait_node()), "pause")

        spans = _spans_by_name("custos_workflow.step.execute")
        assert len(spans) == 1
        assert spans[0].status.status_code is StatusCode.ERROR
        assert spans[0].attributes is not None
        assert spans[0].attributes["step_kind"] == "wait"


# ---------------------------------------------------------------------------
# ActivityStepHandler integration — schedule envelope class labels
# ---------------------------------------------------------------------------


def _envelope(class_: str, *, outputs: dict[str, Any] | None = None) -> ActivityResultEnvelope:
    return ActivityResultEnvelope(
        class_=cast(ActivityResultClass, class_),
        outputs=MappingProxyType(outputs or {}) if class_ == "success" else None,
        error=None
        if class_ == "success"
        else MappingProxyType(
            {
                "kind": "activity.failed",
                "message": f"stub envelope class={class_}",
            }
        ),
        attempt=1,
    )


class TestActivityScheduleEnvelopeClassLabel:
    @pytest.mark.parametrize("envelope_class", sorted(ACTIVITY_RESULT_CLASSES))
    def test_each_envelope_class_lands_on_schedule_histogram(self, envelope_class: str) -> None:
        handler = ActivityStepHandler(
            activity_client=_StubActivityClient(_envelope(envelope_class)),
            connector_client=_StubConnectorClient(),
        )
        handler.execute(_ctx(), _graph(_activity_node()), "scan")

        samples = _by_name(
            _collect_points(),
            "custos_workflow_activity_schedule_duration_ms",
        )
        # For non-success classes the retry driver may consume the
        # envelope on the first attempt and emit ``Skip`` / ``FailNow``
        # (the default retry policy on a node with no policy attached
        # bails immediately) — so we expect exactly one sample per
        # call across all envelope classes.
        assert len(samples) == 1
        attrs, _value = samples[0]
        assert attrs == {"step_kind": "activity", "class": envelope_class}

    @pytest.mark.parametrize("envelope_class", sorted(ACTIVITY_RESULT_CLASSES))
    def test_step_attempts_counter_bumps_with_envelope_class(self, envelope_class: str) -> None:
        handler = ActivityStepHandler(
            activity_client=_StubActivityClient(_envelope(envelope_class)),
            connector_client=_StubConnectorClient(),
        )
        handler.execute(_ctx(), _graph(_activity_node()), "scan")

        attempts = _by_name(
            _collect_points(),
            "custos_workflow_step_attempts_total",
        )
        assert len(attempts) == 1
        attrs, value = attempts[0]
        assert attrs == {"step_kind": "activity", "final_class": envelope_class}
        assert value == 1


class TestActivityScheduleRaiseLabelsInternalError:
    def test_schedule_raise_records_internal_error_class_sample(self) -> None:
        handler = ActivityStepHandler(
            activity_client=_StubActivityClient(RuntimeError("boom")),
            connector_client=_StubConnectorClient(),
        )

        result = handler.execute(_ctx(), _graph(_activity_node()), "scan")
        assert isinstance(result, StepFailed)

        samples = _by_name(
            _collect_points(),
            "custos_workflow_activity_schedule_duration_ms",
        )
        assert len(samples) == 1
        attrs, _value = samples[0]
        assert attrs == {"step_kind": "activity", "class": "internal_error"}

    def test_schedule_raise_records_attempt_with_internal_error_class(self) -> None:
        handler = ActivityStepHandler(
            activity_client=_StubActivityClient(RuntimeError("boom")),
            connector_client=_StubConnectorClient(),
        )
        handler.execute(_ctx(), _graph(_activity_node()), "scan")

        attempts = _by_name(
            _collect_points(),
            "custos_workflow_step_attempts_total",
        )
        assert len(attempts) == 1
        attrs, value = attempts[0]
        assert attrs == {"step_kind": "activity", "final_class": "internal_error"}
        assert value == 1


class TestBindConnectorsRaiseLabelsInternalError:
    def test_bind_raise_records_attempt_with_internal_error_class(self) -> None:
        handler = ActivityStepHandler(
            activity_client=_StubActivityClient(_envelope("success")),
            connector_client=_StubConnectorClient(raises=RuntimeError("bind failed")),
        )

        result = handler.execute(_ctx(), _graph(_activity_node()), "scan")
        assert isinstance(result, StepFailed)

        attempts = _by_name(
            _collect_points(),
            "custos_workflow_step_attempts_total",
        )
        assert len(attempts) == 1
        attrs, value = attempts[0]
        assert attrs == {"step_kind": "activity", "final_class": "internal_error"}
        assert value == 1

    def test_bind_raise_emits_no_schedule_histogram_sample(self) -> None:
        # Bind failures short-circuit before scheduling, so the
        # schedule histogram MUST NOT pick up an internal_error
        # sample (that label belongs to schedule-side raises).
        handler = ActivityStepHandler(
            activity_client=_StubActivityClient(_envelope("success")),
            connector_client=_StubConnectorClient(raises=RuntimeError("bind failed")),
        )
        handler.execute(_ctx(), _graph(_activity_node()), "scan")

        samples = _by_name(
            _collect_points(),
            "custos_workflow_activity_schedule_duration_ms",
        )
        assert samples == []


# ---------------------------------------------------------------------------
# Span-only wrappers (no histogram sample): bind_connectors, retry_decision
# ---------------------------------------------------------------------------


class TestSpanOnlyWrappersEmitNoHistogramSample:
    def test_bind_connectors_emits_span_only(self) -> None:
        handler = ActivityStepHandler(
            activity_client=_StubActivityClient(_envelope("success")),
            connector_client=_StubConnectorClient(),
        )
        handler.execute(_ctx(), _graph(_activity_node()), "scan")

        spans = _spans_by_name("custos_workflow.step.bind_connectors")
        assert len(spans) == 1
        assert spans[0].attributes is not None
        assert spans[0].attributes["step_kind"] == "activity"

    def test_retry_decision_emits_span_only_on_non_success_envelope(self) -> None:
        # ``cancelled`` envelopes go through the retry driver
        # because they're not the ``success`` short-circuit branch.
        handler = ActivityStepHandler(
            activity_client=_StubActivityClient(_envelope("cancelled")),
            connector_client=_StubConnectorClient(),
        )
        handler.execute(_ctx(), _graph(_activity_node()), "scan")

        spans = _spans_by_name("custos_workflow.step.retry_decision")
        assert len(spans) == 1
        assert spans[0].attributes is not None
        assert spans[0].attributes["step_kind"] == "activity"

    def test_retry_decision_skipped_on_success_envelope(self) -> None:
        # ``success`` short-circuits before the retry driver runs,
        # so no retry-decision span should appear.
        handler = ActivityStepHandler(
            activity_client=_StubActivityClient(_envelope("success")),
            connector_client=_StubConnectorClient(),
        )
        handler.execute(_ctx(), _graph(_activity_node()), "scan")

        assert _spans_by_name("custos_workflow.step.retry_decision") == []


# ---------------------------------------------------------------------------
# record_step_error — kind validation
# ---------------------------------------------------------------------------


class TestRecordStepErrorValidation:
    def test_unknown_kind_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="unknown step error kind"):
            _telemetry.record_step_error("not.a.locked.kind")

    @pytest.mark.parametrize("kind", sorted(LOCKED_STEP_KINDS))
    def test_each_locked_kind_bumps_counter_by_one(self, kind: str) -> None:
        _telemetry.record_step_error(kind)

        errors = _by_name(_collect_points(), "custos_workflow_step_errors_total")
        assert len(errors) == 1
        attrs, value = errors[0]
        assert attrs == {"kind": kind}
        assert value == 1


# ---------------------------------------------------------------------------
# record_activity_schedule_sample / record_step_attempt — class validation
# ---------------------------------------------------------------------------


class TestRecordActivityScheduleSampleValidation:
    def test_unknown_class_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="unknown activity result class"):
            _telemetry.record_activity_schedule_sample(1.0, "activity", "not_a_class")

    def test_negative_elapsed_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="elapsed_ms must be non-negative"):
            _telemetry.record_activity_schedule_sample(-1.0, "activity", "success")


class TestRecordStepAttemptValidation:
    def test_unknown_final_class_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="unknown final_class"):
            _telemetry.record_step_attempt("activity", "not_a_class")

    @pytest.mark.parametrize("final_class", sorted(ACTIVITY_RESULT_CLASSES))
    def test_each_envelope_class_accepted(self, final_class: str) -> None:
        _telemetry.record_step_attempt("activity", final_class)
        attempts = _by_name(_collect_points(), "custos_workflow_step_attempts_total")
        assert len(attempts) == 1
        attrs, value = attempts[0]
        assert attrs == {"step_kind": "activity", "final_class": final_class}
        assert value == 1

    def test_internal_error_accepted_as_final_class(self) -> None:
        _telemetry.record_step_attempt("activity", "internal_error")
        attempts = _by_name(_collect_points(), "custos_workflow_step_attempts_total")
        assert len(attempts) == 1
        attrs, value = attempts[0]
        assert attrs == {"step_kind": "activity", "final_class": "internal_error"}
        assert value == 1


# ---------------------------------------------------------------------------
# Build-time assertion guard
# ---------------------------------------------------------------------------


class TestModuleLevelAssertion:
    def test_step_execute_outcomes_set_pinned_to_locked_step_kinds(self) -> None:
        # Mirrors the module-level ``assert`` in ``_telemetry.py``.
        # Reproducing it here gives the test suite a single-point
        # failure if the locked set drifts from the outcome map.
        assert frozenset(_telemetry._STEP_EXECUTE_OUTCOMES) == LOCKED_STEP_KINDS

    def test_every_locked_kind_has_an_outcome_label(self) -> None:
        # Every value in the map is the bare suffix (no
        # ``step.`` prefix) so dashboards can pivot on the
        # ``outcome`` label without re-parsing.
        for kind, suffix in _telemetry._STEP_EXECUTE_OUTCOMES.items():
            assert kind.startswith("step."), kind
            assert suffix == kind.removeprefix("step."), (kind, suffix)

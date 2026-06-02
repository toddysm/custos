"""OpenTelemetry instrumentation tests for the Sub-Orchestration Manager (WF-IMPL-096).

Every Sub-Orchestration Manager primitive dispatched inline by the run
orchestrator (:func:`make_run_orchestrator`, WF-IMPL-093) must emit:

- exactly one ``custos_workflow.sub_orchestration.{loop,sub_workflow,approval}``
  span per dispatch, carrying ``primitive`` and ``outcome`` attributes;
- exactly one counter sample per dispatch — the children-spawned
  counter for ``loop`` / ``sub_workflow`` (value = number of children
  spawned), the approvals-timed-out counter for ``approval`` (value = 1
  on timeout, else 0) — labelled by ``outcome``.

This module asserts one span + one sample per primitive per outcome by
driving the orchestrator under :class:`FakeWorkflowRuntime` with a stub
manager that can deterministically produce each outcome. The "no-op
without an SDK" criterion is covered by importing :mod:`custos_workflow`
before the SDK providers are installed (mirrors
``tests/test_observability_steps.py``).
"""

from __future__ import annotations

import asyncio
from collections.abc import Generator, Iterator
from datetime import UTC, datetime
from typing import Any, cast

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
from custos_workflow.document import ApprovalStep, LetStep, WorkflowStep
from custos_workflow.graph import (
    ExecutionGraph,
    ExecutionNode,
    GraphMetadata,
    PrimitiveHandler,
    StepKind,
    to_json,
)
from custos_workflow.runs import (
    NoopStepHandler,
    RunInput,
    RunOutput,
    StepExecutionContext,
    make_run_orchestrator,
)
from custos_workflow.runs.orchestrator import WORKFLOW_NAME
from custos_workflow.runtime import (
    FakeWorkflowClient,
    FakeWorkflowRuntime,
    ScheduleWorkflowRequest,
)
from custos_workflow.runtime.fake import FakeWorkflowFn
from custos_workflow.steps.errors import (
    ApprovalTimeoutError,
    LoopExpansionError,
    StepCoordinatorError,
    SubWorkflowFailedError,
)

# ---------------------------------------------------------------------------
# OTel SDK wiring (mirrors tests/test_observability_steps.py).
# ---------------------------------------------------------------------------
#
# We deliberately do NOT call ``set_tracer_provider`` /
# ``set_meter_provider`` — those are process-global and a sibling
# observability module may have installed its own providers first.
# Instead we grab tracers/meters off our own local provider instances
# and rebind ``_telemetry._tracer`` / ``_telemetry._meter`` + the two
# WF-IMPL-096 counters inside the autouse fixture (save/restore on
# teardown) so emissions land in *our* in-memory exporter.

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
_children_spawned_total = _sdk_meter.create_counter(
    name="custos_workflow_sub_orchestration_children_spawned_total",
    description=(
        "Count of child workflow instances spawned by the "
        "Sub-Orchestration Manager loop / sub-workflow primitives."
    ),
)
_approvals_timed_out_total = _sdk_meter.create_counter(
    name="custos_workflow_sub_orchestration_approvals_timed_out_total",
    description=("Count of Sub-Orchestration Manager approval gates that resolved by timing out."),
)


@pytest.fixture(autouse=True)
def _reset_otel_state() -> Iterator[None]:
    """Pin ``_telemetry`` bindings to this module's SDK exporter then restore."""
    saved_tracer = _telemetry._tracer
    saved_meter = _telemetry._meter
    saved_children = _telemetry.SUB_ORCHESTRATION_CHILDREN_SPAWNED_TOTAL
    saved_approvals = _telemetry.SUB_ORCHESTRATION_APPROVALS_TIMED_OUT_TOTAL
    _telemetry._tracer = _sdk_tracer
    _telemetry._meter = _sdk_meter
    _telemetry.SUB_ORCHESTRATION_CHILDREN_SPAWNED_TOTAL = _children_spawned_total  # type: ignore[misc]
    _telemetry.SUB_ORCHESTRATION_APPROVALS_TIMED_OUT_TOTAL = _approvals_timed_out_total  # type: ignore[misc]
    _span_exporter.clear()
    _metric_reader.get_metrics_data()
    try:
        yield
    finally:
        _telemetry._tracer = saved_tracer
        _telemetry._meter = saved_meter
        _telemetry.SUB_ORCHESTRATION_CHILDREN_SPAWNED_TOTAL = saved_children  # type: ignore[misc]
        _telemetry.SUB_ORCHESTRATION_APPROVALS_TIMED_OUT_TOTAL = saved_approvals  # type: ignore[misc]


_NOW = datetime(2026, 6, 1, 9, 0, 0, tzinfo=UTC)
_STEP_ID = "scan"
_WORKFLOW_REF = "11111111-1111-1111-1111-111111111111"


# ---------------------------------------------------------------------------
# Metric-collection helpers (mirror tests/test_observability.py).
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
                    value = pt.sum if hasattr(pt, "sum") else pt.value
                    out.append((metric.name, attrs, value))
    return out


def _by_name(
    points: list[tuple[str, dict[str, str], float | int]],
    name: str,
) -> list[tuple[dict[str, str], float | int]]:
    return [(attrs, value) for n, attrs, value in points if n == name]


def _spans(name: str) -> list[Any]:
    return [s for s in _span_exporter.get_finished_spans() if s.name == name]


# ---------------------------------------------------------------------------
# Graph builders.
# ---------------------------------------------------------------------------


def _metadata() -> GraphMetadata:
    return GraphMetadata(
        workflow_name="pipeline",
        workflow_workspace="ws",
        document_api_version="custos.dev/v1",
    )


def _single_node_graph(node: ExecutionNode) -> ExecutionGraph:
    return ExecutionGraph(
        nodes=(node,),
        edges=(),
        topological_order=(node.step_id,),
        metadata=_metadata(),
    )


def _loop_graph() -> ExecutionGraph:
    node = ExecutionNode(
        step_id=_STEP_ID,
        kind=StepKind.LET,
        primitive_handler=PrimitiveHandler.SUB_ORCHESTRATION,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=LetStep.model_validate(
            {"id": _STEP_ID, "let": {"noop": 0}, "forEach": "${{ inputs.items }}"}
        ),
    )
    return _single_node_graph(node)


def _approval_graph() -> ExecutionGraph:
    node = ExecutionNode(
        step_id=_STEP_ID,
        kind=StepKind.APPROVAL,
        primitive_handler=PrimitiveHandler.SUB_ORCHESTRATION,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=ApprovalStep.model_validate(
            {"id": _STEP_ID, "approval": {"approvers": ["alice@example.com"]}}
        ),
    )
    return _single_node_graph(node)


def _workflow_graph() -> ExecutionGraph:
    node = ExecutionNode(
        step_id=_STEP_ID,
        kind=StepKind.WORKFLOW,
        primitive_handler=PrimitiveHandler.SUB_ORCHESTRATION,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=WorkflowStep.model_validate(
            {"id": _STEP_ID, "workflow": _WORKFLOW_REF, "with": None}
        ),
    )
    return _single_node_graph(node)


def _child_graph() -> ExecutionGraph:
    node = ExecutionNode(
        step_id="greet",
        kind=StepKind.LET,
        primitive_handler=PrimitiveHandler.EXPRESSION_INLINE,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=LetStep.model_validate({"id": "greet", "let": {"noop": 0}}),
    )
    return ExecutionGraph(
        nodes=(node,),
        edges=(),
        topological_order=("greet",),
        metadata=GraphMetadata(
            workflow_name="child",
            workflow_workspace="ws",
            document_api_version="custos.dev/v1",
        ),
    )


def _run_input(graph: ExecutionGraph, *, inputs: dict[str, Any] | None = None) -> RunInput:
    return RunInput(
        workspace_id="ws-001",
        workflow_version_id="wfv-001",
        compiled_graph_json=to_json(graph),
        inputs=inputs or {},
        idempotency_key="idem-1",
    )


# ---------------------------------------------------------------------------
# Stub Sub-Orchestration Manager — emits a canned result or raises.
# ---------------------------------------------------------------------------


class _StubManager:
    """Generator-shaped stand-in that returns a canned value or raises."""

    def __init__(
        self,
        *,
        loop_result: list[Any] | None = None,
        approval_result: dict[str, Any] | None = None,
        sub_workflow_result: dict[str, dict[str, Any]] | None = None,
        raise_on_loop: StepCoordinatorError | None = None,
        raise_on_approval: StepCoordinatorError | None = None,
        raise_on_sub_workflow: StepCoordinatorError | None = None,
    ) -> None:
        self._loop_result = loop_result if loop_result is not None else []
        self._approval_result = approval_result if approval_result is not None else {}
        self._sub_workflow_result = sub_workflow_result if sub_workflow_result is not None else {}
        self._raise_on_loop = raise_on_loop
        self._raise_on_approval = raise_on_approval
        self._raise_on_sub_workflow = raise_on_sub_workflow

    def run_loop(
        self, ctx: StepExecutionContext, graph: ExecutionGraph, step_id: str
    ) -> Generator[Any, Any, list[Any]]:
        if False:  # pragma: no cover - generator marker
            yield None
        if self._raise_on_loop is not None:
            raise self._raise_on_loop
        return list(self._loop_result)

    def run_approval(
        self, ctx: StepExecutionContext, graph: ExecutionGraph, step_id: str
    ) -> Generator[Any, Any, dict[str, Any]]:
        if False:  # pragma: no cover - generator marker
            yield None
        if self._raise_on_approval is not None:
            raise self._raise_on_approval
        return dict(self._approval_result)

    def run_sub_workflow(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        step_id: str,
        child_graph: ExecutionGraph,
    ) -> Generator[Any, Any, dict[str, dict[str, Any]]]:
        if False:  # pragma: no cover - generator marker
            yield None
        if self._raise_on_sub_workflow is not None:
            raise self._raise_on_sub_workflow
        return dict(self._sub_workflow_result)


# ---------------------------------------------------------------------------
# Driver.
# ---------------------------------------------------------------------------


@pytest.fixture
def runtime() -> FakeWorkflowRuntime:
    return FakeWorkflowRuntime(now=_NOW)


@pytest.fixture
def client(runtime: FakeWorkflowRuntime) -> FakeWorkflowClient:
    return runtime.client()


def _run(
    runtime: FakeWorkflowRuntime,
    client: FakeWorkflowClient,
    run_input: RunInput,
    *,
    manager: _StubManager,
) -> RunOutput:
    orchestrator = make_run_orchestrator(
        NoopStepHandler(),
        sub_orchestration_manager=cast(Any, manager),
        child_graph_resolver=lambda _ref: _child_graph(),
    )
    runtime.register_workflow(cast(FakeWorkflowFn, orchestrator), name=WORKFLOW_NAME)
    returned = asyncio.run(
        client.schedule_new_workflow(
            ScheduleWorkflowRequest(workflow=WORKFLOW_NAME, input=run_input, instance_id="parent-1")
        )
    )
    state = runtime.instance(returned)
    assert isinstance(state.output, RunOutput)
    return state.output


# ---------------------------------------------------------------------------
# loop primitive — success + failure outcomes.
# ---------------------------------------------------------------------------


def test_loop_success_emits_span_and_children_spawned_sample(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    manager = _StubManager(loop_result=[{"n": 1}, {"n": 2}, {"n": 3}])
    output = _run(runtime, client, _run_input(_loop_graph()), manager=manager)
    assert output.status == "succeeded"

    spans = _spans("custos_workflow.sub_orchestration.loop")
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes["primitive"] == "loop"
    assert spans[0].attributes["outcome"] == "ok"
    assert spans[0].status.status_code is not StatusCode.ERROR

    samples = _by_name(
        _collect_points(), "custos_workflow_sub_orchestration_children_spawned_total"
    )
    assert samples == [({"primitive": "loop", "outcome": "ok"}, 3)]


def test_loop_failure_emits_span_and_zero_children_sample(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    manager = _StubManager(
        raise_on_loop=LoopExpansionError(
            "duplicate iteration key", run_id="parent-1", step_id=_STEP_ID
        )
    )
    output = _run(runtime, client, _run_input(_loop_graph()), manager=manager)
    assert output.status == "failed"

    spans = _spans("custos_workflow.sub_orchestration.loop")
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes["outcome"] == "loop_expansion_error"
    assert spans[0].status.status_code is StatusCode.ERROR

    samples = _by_name(
        _collect_points(), "custos_workflow_sub_orchestration_children_spawned_total"
    )
    assert samples == [({"primitive": "loop", "outcome": "loop_expansion_error"}, 0)]


# ---------------------------------------------------------------------------
# sub_workflow primitive — success + failure outcomes.
# ---------------------------------------------------------------------------


def test_sub_workflow_success_emits_span_and_one_child_sample(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    manager = _StubManager(sub_workflow_result={"greet": {"ok": True}})
    output = _run(runtime, client, _run_input(_workflow_graph()), manager=manager)
    assert output.status == "succeeded"

    spans = _spans("custos_workflow.sub_orchestration.sub_workflow")
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes["primitive"] == "sub_workflow"
    assert spans[0].attributes["outcome"] == "ok"

    samples = _by_name(
        _collect_points(), "custos_workflow_sub_orchestration_children_spawned_total"
    )
    assert samples == [({"primitive": "sub_workflow", "outcome": "ok"}, 1)]


def test_sub_workflow_failure_emits_span_and_zero_children_sample(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    manager = _StubManager(
        raise_on_sub_workflow=SubWorkflowFailedError(
            "child failed", run_id="parent-1", step_id=_STEP_ID
        )
    )
    output = _run(runtime, client, _run_input(_workflow_graph()), manager=manager)
    assert output.status == "failed"

    spans = _spans("custos_workflow.sub_orchestration.sub_workflow")
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes["outcome"] == "sub_workflow_failed"
    assert spans[0].status.status_code is StatusCode.ERROR

    samples = _by_name(
        _collect_points(), "custos_workflow_sub_orchestration_children_spawned_total"
    )
    assert samples == [({"primitive": "sub_workflow", "outcome": "sub_workflow_failed"}, 0)]


# ---------------------------------------------------------------------------
# approval primitive — approved + timed-out outcomes.
# ---------------------------------------------------------------------------


def test_approval_approved_emits_span_and_zero_timeout_sample(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    manager = _StubManager(approval_result={"decision": "approved"})
    output = _run(runtime, client, _run_input(_approval_graph()), manager=manager)
    assert output.status == "succeeded"

    spans = _spans("custos_workflow.sub_orchestration.approval")
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes["primitive"] == "approval"
    assert spans[0].attributes["outcome"] == "ok"

    samples = _by_name(
        _collect_points(), "custos_workflow_sub_orchestration_approvals_timed_out_total"
    )
    assert samples == [({"outcome": "ok"}, 0)]


def test_approval_timeout_emits_span_and_one_timeout_sample(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    manager = _StubManager(
        raise_on_approval=ApprovalTimeoutError(
            "gate timed out", run_id="parent-1", step_id=_STEP_ID, timeout="PT24H"
        )
    )
    output = _run(runtime, client, _run_input(_approval_graph()), manager=manager)
    assert output.status == "failed"

    spans = _spans("custos_workflow.sub_orchestration.approval")
    assert len(spans) == 1
    assert spans[0].attributes is not None
    assert spans[0].attributes["outcome"] == "approval_timeout"
    assert spans[0].status.status_code is StatusCode.ERROR

    samples = _by_name(
        _collect_points(), "custos_workflow_sub_orchestration_approvals_timed_out_total"
    )
    assert samples == [({"outcome": "approval_timeout"}, 1)]


def test_unknown_primitive_label_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown sub-orchestration primitive"):  # noqa: SIM117
        with _telemetry.observe_sub_orchestration("bogus"):
            pass  # pragma: no cover - the context body never runs.

"""Tests for the child sub-workflow orchestrator function (WF-IMPL-088)."""

from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime
from typing import Any

import pytest
from custos_cel import (
    SchemaBindings,
    parse,
    type_check,
)

from custos_workflow.document import ActivityStep, LetStep
from custos_workflow.graph import (
    BackoffStrategyTag,
    CallSiteKind,
    ExecutionGraph,
    ExecutionNode,
    GraphMetadata,
    JitterStrategyTag,
    OnErrorActionTag,
    OnErrorRoute,
    PrimitiveHandler,
    ResolvedBackoffPolicy,
    ResolvedRetryPolicy,
    StepKind,
    TypedCallSite,
)
from custos_workflow.graph.serialize import to_json
from custos_workflow.runs import (
    StepExecutionContext,
    StepResult,
    StepSkipped,
    StepSucceeded,
)
from custos_workflow.runtime import (
    FakeWorkflowClient,
    FakeWorkflowContext,
    FakeWorkflowRuntime,
    RunStatus,
    ScheduleWorkflowRequest,
)
from custos_workflow.steps import LetStepHandler
from custos_workflow.steps.activity_step import ActivityStepHandler
from custos_workflow.steps.sub_orchestration import (
    CHILD_STEP_WORKFLOW_NAME,
    DEFAULT_LOOP_VAR,
    ChildStepInput,
    ChildStepResult,
    make_child_step_orchestrator,
)

_NOW = datetime(2026, 6, 1, 9, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def runtime() -> FakeWorkflowRuntime:
    return FakeWorkflowRuntime(now=_NOW)


@pytest.fixture
def client(runtime: FakeWorkflowRuntime) -> FakeWorkflowClient:
    return runtime.client()


# ---------------------------------------------------------------------------
# Graph builders
# ---------------------------------------------------------------------------


def _typed(source: str, *, inputs_schema: dict[str, Any] | None = None) -> Any:
    ast = parse(source)
    schema = inputs_schema or {"type": "object", "properties": {}, "required": []}
    return type_check(ast, SchemaBindings(inputs=schema))


def _let_call_site(
    source_cel: str, *, name: str, inputs_schema: dict[str, Any] | None = None
) -> TypedCallSite:
    return TypedCallSite(
        source=f"${{{{ {source_cel} }}}}",
        typed_ast=_typed(source_cel, inputs_schema=inputs_schema),
        kind=CallSiteKind.LET,
        document_path=f"spec.steps[0].let.{name}",
    )


def _let_graph(
    *,
    step_id: str = "body",
    let_block: dict[str, Any],
    call_sites: dict[str, TypedCallSite] | None = None,
) -> ExecutionGraph:
    node = ExecutionNode(
        step_id=step_id,
        kind=StepKind.LET,
        primitive_handler=PrimitiveHandler.EXPRESSION_INLINE,
        retry_policy=None,
        on_error_routes=(),
        call_sites=call_sites or {},
        step_source=LetStep.model_validate({"id": step_id, "let": let_block}),
    )
    return ExecutionGraph(
        nodes=(node,),
        edges=(),
        topological_order=(step_id,),
        metadata=GraphMetadata(
            workflow_name="pipeline",
            workflow_workspace="ws",
            document_api_version="custos.dev/v1",
        ),
    )


def _register_child(runtime: FakeWorkflowRuntime) -> None:
    fn = make_child_step_orchestrator(LetStepHandler())
    runtime.register_workflow(fn, name=CHILD_STEP_WORKFLOW_NAME)


async def _run_child(
    client: FakeWorkflowClient,
    runtime: FakeWorkflowRuntime,
    child_input: ChildStepInput,
    *,
    instance_id: str = "child-1",
) -> tuple[RunStatus, ChildStepResult]:
    returned = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(
            workflow=CHILD_STEP_WORKFLOW_NAME,
            input=child_input.to_dict(),
            instance_id=instance_id,
        )
    )
    state = runtime.instance(returned)
    result = ChildStepResult.from_dict(state.output) if state.output is not None else None
    assert result is not None
    return state.status, result


# ---------------------------------------------------------------------------
# Happy path — runs the inner step and returns its outputs
# ---------------------------------------------------------------------------


async def test_child_runs_let_body_and_returns_outputs(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    _register_child(runtime)
    graph = _let_graph(
        let_block={"sum": "${{ 1 + 2 }}"},
        call_sites={"let.sum": _let_call_site("1 + 2", name="sum")},
    )
    child_input = ChildStepInput(
        workspace_id="ws-1",
        workflow_version_id="wf-v1",
        compiled_graph_json=to_json(graph),
        step_id="body",
    )

    status, result = await _run_child(client, runtime, child_input)

    assert status == RunStatus.COMPLETED
    assert result.status == "succeeded"
    assert dict(result.outputs) == {"sum": 3}
    assert result.failure_envelope is None


async def test_child_surfaces_item_under_inputs(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """The per-iteration item is visible at ``inputs.<loop_var>.*``."""

    _register_child(runtime)
    inputs_schema = {
        "type": "object",
        "properties": {
            "item": {
                "type": "object",
                "properties": {"n": {"type": "integer"}},
                "required": ["n"],
            }
        },
        "required": ["item"],
    }
    graph = _let_graph(
        let_block={"doubled": "${{ inputs.item.n * 2 }}"},
        call_sites={
            "let.doubled": _let_call_site(
                "inputs.item.n * 2", name="doubled", inputs_schema=inputs_schema
            )
        },
    )
    child_input = ChildStepInput(
        workspace_id="ws-1",
        workflow_version_id="wf-v1",
        compiled_graph_json=to_json(graph),
        step_id="body",
        item={"n": 21},
    )

    status, result = await _run_child(client, runtime, child_input)

    assert status == RunStatus.COMPLETED
    assert result.status == "succeeded"
    assert dict(result.outputs) == {"doubled": 42}


async def test_child_reads_parent_inputs_and_outputs(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """Parent ``inputs.*`` and preceding ``steps.<id>.outputs.*`` resolve."""

    _register_child(runtime)
    inputs_schema = {
        "type": "object",
        "properties": {"base": {"type": "integer"}},
        "required": ["base"],
    }
    graph = _let_graph(
        let_block={
            "total": "${{ inputs.base + steps.prior.outputs.bonus }}",
        },
        call_sites={
            "let.total": TypedCallSite(
                source="${{ inputs.base + steps.prior.outputs.bonus }}",
                typed_ast=type_check(
                    parse("inputs.base + steps.prior.outputs.bonus"),
                    SchemaBindings(
                        inputs=inputs_schema,
                        prior_steps=(
                            (
                                "prior",
                                {
                                    "type": "object",
                                    "properties": {"bonus": {"type": "integer"}},
                                },
                            ),
                        ),
                    ),
                ),
                kind=CallSiteKind.LET,
                document_path="spec.steps[0].let.total",
            )
        },
    )
    child_input = ChildStepInput(
        workspace_id="ws-1",
        workflow_version_id="wf-v1",
        compiled_graph_json=to_json(graph),
        step_id="body",
        inputs={"base": 10},
        parent_outputs={"prior": {"bonus": 5}},
        loop_var="",
    )

    status, result = await _run_child(client, runtime, child_input)

    assert status == RunStatus.COMPLETED
    assert dict(result.outputs) == {"total": 15}


async def test_child_omits_item_when_loop_var_empty(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """An empty ``loop_var`` (sub-workflow mode) injects no ``inputs.item``."""

    _register_child(runtime)
    inputs_schema = {
        "type": "object",
        "properties": {
            "item": {
                "type": "object",
                "properties": {"n": {"type": "integer"}},
                "required": ["n"],
            }
        },
        "required": [],
    }
    graph = _let_graph(
        let_block={"echo": "${{ inputs.item.n }}"},
        call_sites={
            "let.echo": _let_call_site("inputs.item.n", name="echo", inputs_schema=inputs_schema)
        },
    )
    child_input = ChildStepInput(
        workspace_id="ws-1",
        workflow_version_id="wf-v1",
        compiled_graph_json=to_json(graph),
        step_id="body",
        item={"n": 7},
        loop_var="",
    )

    status, result = await _run_child(client, runtime, child_input)

    # The item was not injected, so ``inputs.item`` is unbound and the
    # inner let binding fails with a structured envelope.
    assert status == RunStatus.COMPLETED
    assert result.status == "failed"
    assert result.failure_envelope is not None
    assert result.failure_envelope["kind"] == "step.with_input_resolution_error"


# ---------------------------------------------------------------------------
# Failure path — inner step failure surfaces a structured envelope
# ---------------------------------------------------------------------------


async def test_child_surfaces_inner_failure_envelope(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    _register_child(runtime)
    inputs_schema = {
        "type": "object",
        "properties": {
            "values": {"type": "array", "items": {"type": "integer"}},
        },
        "required": ["values"],
    }
    graph = _let_graph(
        let_block={"first": "${{ inputs.values[0] }}"},
        call_sites={
            "let.first": _let_call_site(
                "inputs.values[0]", name="first", inputs_schema=inputs_schema
            )
        },
    )
    child_input = ChildStepInput(
        workspace_id="ws-1",
        workflow_version_id="wf-v1",
        compiled_graph_json=to_json(graph),
        step_id="body",
        inputs={"values": []},
        loop_var="",
    )

    status, result = await _run_child(client, runtime, child_input)

    assert status == RunStatus.COMPLETED
    assert result.status == "failed"
    assert result.failure_envelope is not None
    assert result.failure_envelope["kind"] == "step.with_input_resolution_error"


# ---------------------------------------------------------------------------
# Skipped path — a gated-out inner step contributes empty outputs
# ---------------------------------------------------------------------------


class _SkipHandler:
    """Stub :class:`StepHandler` that always skips."""

    def execute(self, ctx: StepExecutionContext, graph: ExecutionGraph, step_id: str) -> StepResult:
        return StepSkipped(reason="gated-out")


async def test_child_skipped_inner_step_yields_empty_outputs(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    fn = make_child_step_orchestrator(_SkipHandler())
    runtime.register_workflow(fn, name=CHILD_STEP_WORKFLOW_NAME)
    graph = _let_graph(let_block={"unused": 1})
    child_input = ChildStepInput(
        workspace_id="ws-1",
        workflow_version_id="wf-v1",
        compiled_graph_json=to_json(graph),
        step_id="body",
    )

    status, result = await _run_child(client, runtime, child_input)

    assert status == RunStatus.COMPLETED
    assert result.status == "succeeded"
    assert dict(result.outputs) == {}


# ---------------------------------------------------------------------------
# Replay / determinism — the child id is derived deterministically
# ---------------------------------------------------------------------------


async def test_child_is_replay_safe_for_same_input(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    """Two children scheduled with identical input produce identical output."""

    _register_child(runtime)
    graph = _let_graph(
        let_block={"sum": "${{ 40 + 2 }}"},
        call_sites={"let.sum": _let_call_site("40 + 2", name="sum")},
    )
    child_input = ChildStepInput(
        workspace_id="ws-1",
        workflow_version_id="wf-v1",
        compiled_graph_json=to_json(graph),
        step_id="body",
    )

    _, first = await _run_child(client, runtime, child_input, instance_id="child-a")
    _, second = await _run_child(client, runtime, child_input, instance_id="child-b")

    assert first.to_dict() == second.to_dict()
    assert dict(first.outputs) == {"sum": 42}


# ---------------------------------------------------------------------------
# Direct StepExecutionContext shaping
# ---------------------------------------------------------------------------


def test_child_step_ctx_run_id_is_child_instance_id() -> None:
    """The inner ``StepExecutionContext.run_id`` is the child instance id."""

    captured: dict[str, StepExecutionContext] = {}

    class _CaptureHandler:
        def execute(
            self, ctx: StepExecutionContext, graph: ExecutionGraph, step_id: str
        ) -> StepResult:
            captured["ctx"] = ctx
            return StepSucceeded(outputs={"ok": True})

    fn = make_child_step_orchestrator(_CaptureHandler())
    graph = _let_graph(let_block={"unused": 1})
    child_input = ChildStepInput(
        workspace_id="ws-7",
        workflow_version_id="wf-v9",
        compiled_graph_json=to_json(graph),
        step_id="body",
        inputs={"base": 1},
        item={"n": 3},
    )
    ctx = FakeWorkflowContext(instance_id="run-1/scan/iter-0", now=_NOW)

    gen = fn(ctx, child_input.to_dict())
    output = _drain(gen)

    assert output == {"status": "succeeded", "outputs": {"ok": True}, "failure_envelope": None}
    step_ctx = captured["ctx"]
    assert step_ctx.run_id == "run-1/scan/iter-0"
    assert step_ctx.workspace_id == "ws-7"
    assert step_ctx.workflow_version_id == "wf-v9"
    assert dict(step_ctx.inputs) == {"base": 1, "item": {"n": 3}}


def _drain(gen: Generator[Any, Any, dict[str, Any]]) -> dict[str, Any]:
    try:
        next(gen)
    except StopIteration as stop:
        return stop.value  # type: ignore[no-any-return]
    raise AssertionError("child generator unexpectedly yielded a durable token")


# ---------------------------------------------------------------------------
# Activity body — driven via the WF-IMPL-074 yield protocol
# ---------------------------------------------------------------------------


_ACTIVITY_TOKEN = object()


class _ExplodingHandler:
    """Stub :class:`StepHandler` that must never be dispatched."""

    def execute(self, ctx: StepExecutionContext, graph: ExecutionGraph, step_id: str) -> StepResult:
        raise AssertionError("handler.execute must not run for an ACTIVITY body")


class _YieldingActivityHandler(ActivityStepHandler):
    """Stub activity handler that drives one durable yield then returns."""

    def __init__(self) -> None:  # bypass the real (activity, connector) wiring
        self.calls: list[str] = []

    def iter_calls(
        self, ctx: StepExecutionContext, graph: ExecutionGraph, step_id: str
    ) -> Generator[Any, Any, StepResult]:
        self.calls.append(step_id)
        response = yield _ACTIVITY_TOKEN
        return StepSucceeded(outputs={"echoed": response})


def _activity_graph(*, step_id: str = "scan") -> ExecutionGraph:
    policy = ResolvedRetryPolicy(
        max_attempts=1,
        backoff=ResolvedBackoffPolicy(
            strategy=BackoffStrategyTag.EXPONENTIAL,
            initial_delay_ms=1_000,
            max_delay_ms=60_000,
            multiplier=2.0,
        ),
        jitter=JitterStrategyTag.NONE,
        respect_retry_after=True,
    )
    node = ExecutionNode(
        step_id=step_id,
        kind=StepKind.ACTIVITY,
        primitive_handler=PrimitiveHandler.ACTIVITY_RUNTIME,
        retry_policy=policy,
        on_error_routes=(OnErrorRoute(action=OnErrorActionTag.FAIL, cls="permanent"),),
        call_sites={},
        step_source=ActivityStep.model_validate(
            {"id": step_id, "activity": "scanners/trivy@1", "connector": "primary"}
        ),
    )
    return ExecutionGraph(
        nodes=(node,),
        edges=(),
        topological_order=(step_id,),
        metadata=GraphMetadata(
            workflow_name="pipeline",
            workflow_workspace="ws",
            document_api_version="custos.dev/v1",
        ),
    )


def test_child_drives_activity_body_via_yield_protocol() -> None:
    """An ``activity:`` body flows through ``activity_handler.iter_calls``.

    The child re-yields the activity handler's durable token (so the
    runtime resolves it as a Dapr activity) and never falls back to the
    non-durable synchronous ``handler.execute`` adapter.
    """

    activity_handler = _YieldingActivityHandler()
    fn = make_child_step_orchestrator(_ExplodingHandler(), activity_handler=activity_handler)
    graph = _activity_graph()
    child_input = ChildStepInput(
        workspace_id="ws-1",
        workflow_version_id="wf-v1",
        compiled_graph_json=to_json(graph),
        step_id="scan",
        loop_var="",
    )
    ctx = FakeWorkflowContext(instance_id="run-1/scan/iter-0", now=_NOW)

    gen = fn(ctx, child_input.to_dict())
    yielded: list[Any] = []
    sent: Any = None
    try:
        while True:
            yielded.append(gen.send(sent))
            sent = "activity-response"
    except StopIteration as stop:
        output = stop.value

    assert yielded == [_ACTIVITY_TOKEN]
    assert activity_handler.calls == ["scan"]
    assert output == {
        "status": "succeeded",
        "outputs": {"echoed": "activity-response"},
        "failure_envelope": None,
    }


def test_child_activity_body_without_handler_uses_sync_adapter() -> None:
    """With no ``activity_handler`` an ACTIVITY body uses ``handler.execute``."""

    class _SyncHandler:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def execute(
            self, ctx: StepExecutionContext, graph: ExecutionGraph, step_id: str
        ) -> StepResult:
            self.calls.append(step_id)
            return StepSucceeded(outputs={"sync": True})

    handler = _SyncHandler()
    fn = make_child_step_orchestrator(handler)
    graph = _activity_graph()
    child_input = ChildStepInput(
        workspace_id="ws-1",
        workflow_version_id="wf-v1",
        compiled_graph_json=to_json(graph),
        step_id="scan",
        loop_var="",
    )
    ctx = FakeWorkflowContext(instance_id="run-1/scan/iter-0", now=_NOW)

    output = _drain(fn(ctx, child_input.to_dict()))

    assert handler.calls == ["scan"]
    assert output == {"status": "succeeded", "outputs": {"sync": True}, "failure_envelope": None}


# ---------------------------------------------------------------------------
# Envelope serialization round-trips
# ---------------------------------------------------------------------------


def test_child_step_input_round_trips() -> None:
    original = ChildStepInput(
        workspace_id="ws",
        workflow_version_id="v",
        compiled_graph_json="{}",
        step_id="body",
        inputs={"a": 1},
        parent_outputs={"prior": {"out": 2}},
        loop_var="row",
        item=[1, 2, 3],
    )

    restored = ChildStepInput.from_dict(original.to_dict())

    assert restored == original


def test_child_step_input_defaults_apply() -> None:
    restored = ChildStepInput.from_dict(
        {
            "workspace_id": "ws",
            "workflow_version_id": "v",
            "compiled_graph_json": "{}",
            "step_id": "body",
        }
    )

    assert restored.inputs == {}
    assert restored.parent_outputs == {}
    assert restored.loop_var == DEFAULT_LOOP_VAR
    assert restored.item is None


def test_child_step_input_null_loop_var_falls_back_to_default() -> None:
    """An explicit JSON ``null`` loop_var is treated as missing."""

    restored = ChildStepInput.from_dict(
        {
            "workspace_id": "ws",
            "workflow_version_id": "v",
            "compiled_graph_json": "{}",
            "step_id": "body",
            "loop_var": None,
        }
    )

    assert restored.loop_var == DEFAULT_LOOP_VAR


def test_child_step_input_empty_loop_var_preserved() -> None:
    """An explicit empty string is kept as the no-injection signal."""

    restored = ChildStepInput.from_dict(
        {
            "workspace_id": "ws",
            "workflow_version_id": "v",
            "compiled_graph_json": "{}",
            "step_id": "body",
            "loop_var": "",
        }
    )

    assert restored.loop_var == ""


def test_child_step_result_succeeded_round_trips() -> None:
    result = ChildStepResult.succeeded({"k": "v"})

    restored = ChildStepResult.from_dict(result.to_dict())

    assert restored == result
    assert restored.status == "succeeded"
    assert restored.failure_envelope is None


def test_child_step_result_failed_round_trips() -> None:
    result = ChildStepResult.failed({"kind": "step.boom", "message": "nope"})

    restored = ChildStepResult.from_dict(result.to_dict())

    assert restored == result
    assert restored.status == "failed"
    assert dict(restored.outputs) == {}
    assert restored.failure_envelope == {"kind": "step.boom", "message": "nope"}


def test_child_workflow_name_is_set_on_function() -> None:
    fn = make_child_step_orchestrator(LetStepHandler())

    assert fn.__name__ == CHILD_STEP_WORKFLOW_NAME

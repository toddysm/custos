"""Tests for the Sub-Orchestration Manager ``workflow:`` invocation (WF-IMPL-091)."""

from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from custos_cel import (
    DaprWorkflowClock,
    FixedClock,
    SchemaBindings,
    parse,
    type_check,
)

from custos_workflow.document import LetStep, WorkflowStep
from custos_workflow.graph import (
    CallSiteKind,
    ExecutionGraph,
    ExecutionNode,
    GraphMetadata,
    PrimitiveHandler,
    StepKind,
    TypedCallSite,
)
from custos_workflow.runs import RunId, RunInput, RunOutput, StepExecutionContext
from custos_workflow.runtime import (
    ChildWorkflowError,
    FakeWorkflowClient,
    FakeWorkflowRuntime,
    RunStatus,
    ScheduleWorkflowRequest,
)
from custos_workflow.steps.errors import (
    SubOrchestrationSpawnError,
    SubWorkflowFailedError,
)
from custos_workflow.steps.sub_orchestration import SubOrchestrationManager
from custos_workflow.steps.sub_orchestration.ids import WORKFLOW_ITERATION_KEY

_NOW = datetime(2026, 6, 1, 9, 0, 0, tzinfo=UTC)
_STEP_ID = "sub"
_REF = "11111111-1111-1111-1111-111111111111"
_CHILD_WORKFLOW_NAME = "custos.workflow.run"

_INPUTS_SCHEMA = {
    "type": "object",
    "properties": {"who": {"type": "string"}},
    "required": ["who"],
}


# ---------------------------------------------------------------------------
# Graph builders
# ---------------------------------------------------------------------------


def _with_call_site(cel: str, *, key: str, inputs_schema: dict[str, Any]) -> TypedCallSite:
    ast = parse(cel)
    return TypedCallSite(
        source=f"${{{{ {cel} }}}}",
        typed_ast=type_check(ast, SchemaBindings(inputs=inputs_schema)),
        kind=CallSiteKind.WITH,
        document_path=f"spec.steps[0].with.{key}",
    )


def _workflow_graph(
    *,
    workflow_ref: str = _REF,
    with_block: dict[str, Any] | None = None,
    with_sites: dict[str, TypedCallSite] | None = None,
    step_id: str = _STEP_ID,
) -> ExecutionGraph:
    node = ExecutionNode(
        step_id=step_id,
        kind=StepKind.WORKFLOW,
        primitive_handler=PrimitiveHandler.SUB_ORCHESTRATION,
        retry_policy=None,
        on_error_routes=(),
        call_sites=with_sites or {},
        step_source=WorkflowStep.model_validate(
            {"id": step_id, "workflow": workflow_ref, "with": with_block}
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


def _child_graph() -> ExecutionGraph:
    """A trivial single-node graph standing in for the resolved sub-workflow."""
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


# ---------------------------------------------------------------------------
# Stub workflow context (for unit-driving ``run_sub_workflow`` directly)
# ---------------------------------------------------------------------------


class _Token:
    """Opaque child-task token the stub hands back from ``call_child_workflow``."""

    def __init__(self, instance_id: str | None) -> None:
        self.instance_id = instance_id


class _StubContext:
    """Minimal :class:`WorkflowContext` that records spawns and never executes."""

    instance_id = "parent-1"

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.when_all_tasks: list[Any] | None = None

    def call_child_workflow(
        self,
        workflow: Any,
        *,
        input: Any = None,
        instance_id: str | None = None,
    ) -> _Token:
        self.calls.append({"workflow": workflow, "input": input, "instance_id": instance_id})
        return _Token(instance_id)

    def when_all(self, tasks: Any) -> tuple[str, list[Any]]:
        self.when_all_tasks = list(tasks)
        return ("when_all", self.when_all_tasks)


def _ctx(
    *,
    inputs: dict[str, Any],
    workflow_context: Any,
    run_id: str = "parent-1",
    outputs: dict[str, Any] | None = None,
) -> StepExecutionContext:
    return StepExecutionContext(
        run_id=cast(RunId, run_id),
        workspace_id="ws-1",
        workflow_version_id="wf-v1",
        inputs=inputs,
        workflow_context=workflow_context,
        outputs=outputs or {},
        clock=FixedClock(_NOW),
    )


def _drive_to_when_all(gen: Generator[Any, Any, dict[str, dict[str, Any]]]) -> Any:
    """Advance ``run_sub_workflow`` to its single ``when_all`` yield."""
    return next(gen)


# ---------------------------------------------------------------------------
# Happy path — single deterministic child + with: inputs
# ---------------------------------------------------------------------------


def test_spawns_single_child_with_deterministic_id() -> None:
    ctx = _StubContext()
    graph = _workflow_graph(
        with_block={"name": "${{ inputs.who }}"},
        with_sites={
            "with.name": _with_call_site("inputs.who", key="name", inputs_schema=_INPUTS_SCHEMA)
        },
    )
    step_ctx = _ctx(inputs={"who": "ada"}, workflow_context=ctx)

    gen = SubOrchestrationManager().run_sub_workflow(step_ctx, graph, _STEP_ID, _child_graph())
    token = _drive_to_when_all(gen)

    assert token[0] == "when_all"
    assert len(ctx.calls) == 1
    call = ctx.calls[0]
    assert call["workflow"] == _CHILD_WORKFLOW_NAME
    assert call["instance_id"] == f"parent-1/{_STEP_ID}/workflow"
    decoded = RunInput.from_dict(call["input"])
    assert decoded.workflow_version_id == _REF
    assert decoded.workspace_id == "ws-1"
    # The with: block flows through as the child run's inputs.* namespace.
    assert decoded.inputs == {"name": "ada"}


def test_with_block_absent_passes_empty_inputs() -> None:
    ctx = _StubContext()
    graph = _workflow_graph(with_block=None)
    step_ctx = _ctx(inputs={"who": "ada"}, workflow_context=ctx)

    gen = SubOrchestrationManager().run_sub_workflow(step_ctx, graph, _STEP_ID, _child_graph())
    _drive_to_when_all(gen)

    decoded = RunInput.from_dict(ctx.calls[0]["input"])
    assert decoded.inputs == {}


def test_merges_child_outputs_on_success() -> None:
    ctx = _StubContext()
    graph = _workflow_graph(with_block=None)
    step_ctx = _ctx(inputs={"who": "ada"}, workflow_context=ctx)

    gen = SubOrchestrationManager().run_sub_workflow(step_ctx, graph, _STEP_ID, _child_graph())
    _drive_to_when_all(gen)

    child_output = RunOutput(
        status="succeeded",
        outputs={"greet": {"msg": "hi ada"}},
    ).to_dict()
    with pytest.raises(StopIteration) as si:
        gen.send([child_output])
    assert si.value.value == {"greet": {"msg": "hi ada"}}


# ---------------------------------------------------------------------------
# Failure surfaces → step.sub_workflow_failed
# ---------------------------------------------------------------------------


def test_child_failure_raises_sub_workflow_failed() -> None:
    ctx = _StubContext()
    graph = _workflow_graph(with_block=None)
    step_ctx = _ctx(inputs={"who": "ada"}, workflow_context=ctx)

    gen = SubOrchestrationManager().run_sub_workflow(step_ctx, graph, _STEP_ID, _child_graph())
    _drive_to_when_all(gen)

    child_output = RunOutput(
        status="failed",
        outputs={},
        failed_step="greet",
        failure_envelope={"kind": "step.boom", "message": "bad"},
    ).to_dict()
    with pytest.raises(SubWorkflowFailedError) as exc:
        gen.send([child_output])
    assert exc.value.kind == "step.sub_workflow_failed"
    assert exc.value.child_instance_id == f"parent-1/{_STEP_ID}/workflow"
    assert exc.value.iteration_key == WORKFLOW_ITERATION_KEY
    assert exc.value.child_kind == "step.boom"


def test_waiting_status_raises_sub_workflow_failed() -> None:
    ctx = _StubContext()
    graph = _workflow_graph(with_block=None)
    step_ctx = _ctx(inputs={"who": "ada"}, workflow_context=ctx)

    gen = SubOrchestrationManager().run_sub_workflow(step_ctx, graph, _STEP_ID, _child_graph())
    _drive_to_when_all(gen)

    child_output = RunOutput(
        status="waiting",
        outputs={},
        waiting_step="gate",
        waiting_reason="timer",
    ).to_dict()
    with pytest.raises(SubWorkflowFailedError) as exc:
        gen.send([child_output])
    assert exc.value.kind == "step.sub_workflow_failed"
    assert exc.value.child_kind is None


def test_abnormal_child_failure_raises_sub_workflow_failed() -> None:
    ctx = _StubContext()
    graph = _workflow_graph(with_block=None)
    step_ctx = _ctx(inputs={"who": "ada"}, workflow_context=ctx)

    gen = SubOrchestrationManager().run_sub_workflow(step_ctx, graph, _STEP_ID, _child_graph())
    _drive_to_when_all(gen)

    with pytest.raises(SubWorkflowFailedError) as exc:
        gen.throw(ChildWorkflowError(f"parent-1/{_STEP_ID}/workflow", "RuntimeError", "kaboom"))
    assert exc.value.kind == "step.sub_workflow_failed"
    assert exc.value.child_instance_id == f"parent-1/{_STEP_ID}/workflow"
    assert exc.value.iteration_key == WORKFLOW_ITERATION_KEY
    assert exc.value.child_kind == "RuntimeError"


# ---------------------------------------------------------------------------
# Spawn-id failure → step.sub_orchestration_spawn_error
# ---------------------------------------------------------------------------


def test_malformed_child_id_raises_spawn_error() -> None:
    ctx = _StubContext()
    graph = _workflow_graph(with_block=None)
    # A parent run id containing the reserved separator makes the
    # deterministic child instance id malformed.
    step_ctx = _ctx(inputs={"who": "ada"}, workflow_context=ctx, run_id="parent/oops")

    gen = SubOrchestrationManager().run_sub_workflow(step_ctx, graph, _STEP_ID, _child_graph())
    with pytest.raises(SubOrchestrationSpawnError) as exc:
        next(gen)
    assert exc.value.kind == "step.sub_orchestration_spawn_error"
    assert exc.value.step_id == _STEP_ID
    assert exc.value.cause is not None
    assert ctx.calls == []


def test_non_workflow_step_raises_spawn_error() -> None:
    ctx = _StubContext()
    # Build a node whose source is *not* a WorkflowStep to exercise the
    # defensive guard.
    node = ExecutionNode(
        step_id=_STEP_ID,
        kind=StepKind.LET,
        primitive_handler=PrimitiveHandler.SUB_ORCHESTRATION,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=LetStep.model_validate({"id": _STEP_ID, "let": {"noop": 0}}),
    )
    graph = ExecutionGraph(
        nodes=(node,),
        edges=(),
        topological_order=(_STEP_ID,),
        metadata=GraphMetadata(
            workflow_name="pipeline",
            workflow_workspace="ws",
            document_api_version="custos.dev/v1",
        ),
    )
    step_ctx = _ctx(inputs={"who": "ada"}, workflow_context=ctx)

    gen = SubOrchestrationManager().run_sub_workflow(step_ctx, graph, _STEP_ID, _child_graph())
    with pytest.raises(SubOrchestrationSpawnError) as exc:
        next(gen)
    assert exc.value.kind == "step.sub_orchestration_spawn_error"
    assert ctx.calls == []


def test_unknown_step_id_raises_key_error() -> None:
    ctx = _StubContext()
    graph = _workflow_graph(with_block=None)
    step_ctx = _ctx(inputs={"who": "ada"}, workflow_context=ctx)

    gen = SubOrchestrationManager().run_sub_workflow(step_ctx, graph, "nope", _child_graph())
    with pytest.raises(KeyError):
        next(gen)


# ---------------------------------------------------------------------------
# End-to-end under the FakeWorkflowRuntime
# ---------------------------------------------------------------------------


def _echo_run(ctx: Any, raw: Any) -> Generator[Any, Any, dict[str, Any]]:
    run_input = RunInput.from_dict(raw)
    if False:  # pragma: no cover - generator marker
        yield None
    return RunOutput(
        status="succeeded",
        outputs={"greet": {"msg": f"hi {run_input.inputs['name']}"}},
    ).to_dict()


def _make_parent(graph: ExecutionGraph, child_graph: ExecutionGraph) -> Any:
    def run_parent(ctx: Any, raw: Any) -> Generator[Any, Any, dict[str, dict[str, Any]]]:
        step_ctx = StepExecutionContext(
            run_id=cast(RunId, ctx.instance_id),
            workspace_id="ws-1",
            workflow_version_id="wf-v1",
            inputs=raw,
            workflow_context=ctx,
            outputs={},
            clock=DaprWorkflowClock(ctx),
        )
        result = yield from SubOrchestrationManager().run_sub_workflow(
            step_ctx, graph, _STEP_ID, child_graph
        )
        return result

    return run_parent


@pytest.fixture
def runtime() -> FakeWorkflowRuntime:
    return FakeWorkflowRuntime(now=_NOW)


@pytest.fixture
def client(runtime: FakeWorkflowRuntime) -> FakeWorkflowClient:
    return runtime.client()


async def test_end_to_end_sub_workflow_invocation(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    graph = _workflow_graph(
        with_block={"name": "${{ inputs.who }}"},
        with_sites={
            "with.name": _with_call_site("inputs.who", key="name", inputs_schema=_INPUTS_SCHEMA)
        },
    )
    runtime.register_workflow(_echo_run, name=_CHILD_WORKFLOW_NAME)
    runtime.register_workflow(_make_parent(graph, _child_graph()), name="parent")

    returned = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input={"who": "ada"}, instance_id="parent-1")
    )
    state = runtime.instance(returned)

    assert state.status == RunStatus.COMPLETED
    assert state.output == {"greet": {"msg": "hi ada"}}
    assert state.spawned_children == [f"parent-1/{_STEP_ID}/workflow"]

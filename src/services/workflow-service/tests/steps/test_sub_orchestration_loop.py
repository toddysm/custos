"""Tests for the Sub-Orchestration Manager loop fan-out (WF-IMPL-089)."""

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

from custos_workflow.document import LetStep
from custos_workflow.graph import (
    CallSiteKind,
    ExecutionGraph,
    ExecutionNode,
    GraphMetadata,
    PrimitiveHandler,
    StepKind,
    TypedCallSite,
)
from custos_workflow.runs import RunId, StepExecutionContext
from custos_workflow.runtime import (
    ChildWorkflowError,
    FakeWorkflowClient,
    FakeWorkflowRuntime,
    RunStatus,
    ScheduleWorkflowRequest,
)
from custos_workflow.steps.errors import (
    LoopExpansionError,
    SubOrchestrationSpawnError,
    SubWorkflowFailedError,
)
from custos_workflow.steps.sub_orchestration import (
    CHILD_STEP_WORKFLOW_NAME,
    ChildStepInput,
    ChildStepResult,
    SubOrchestrationManager,
)

_NOW = datetime(2026, 6, 1, 9, 0, 0, tzinfo=UTC)
_STEP_ID = "scan"


# ---------------------------------------------------------------------------
# Graph builders
# ---------------------------------------------------------------------------


def _for_each_call_site(cel: str, *, inputs_schema: dict[str, Any]) -> TypedCallSite:
    ast = parse(cel)
    return TypedCallSite(
        source=f"${{{{ {cel} }}}}",
        typed_ast=type_check(ast, SchemaBindings(inputs=inputs_schema)),
        kind=CallSiteKind.FOR_EACH,
        document_path="spec.steps[0].forEach",
    )


def _loop_graph(
    *,
    for_each_cel: str,
    inputs_schema: dict[str, Any],
    step_id: str = _STEP_ID,
    call_sites: dict[str, TypedCallSite] | None = None,
) -> ExecutionGraph:
    sites = {"forEach": _for_each_call_site(for_each_cel, inputs_schema=inputs_schema)}
    if call_sites:
        sites.update(call_sites)
    node = ExecutionNode(
        step_id=step_id,
        kind=StepKind.LET,
        primitive_handler=PrimitiveHandler.SUB_ORCHESTRATION,
        retry_policy=None,
        on_error_routes=(),
        call_sites=sites,
        step_source=LetStep.model_validate({"id": step_id, "let": {"noop": 0}}),
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


# ---------------------------------------------------------------------------
# Stub workflow context (for unit-driving ``run_loop`` directly)
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


_ARRAY_OF_OBJECTS = {
    "type": "object",
    "properties": {"items": {"type": "array", "items": {"type": "object"}}},
    "required": ["items"],
}


def _drive_to_when_all(gen: Generator[Any, Any, list[Any]]) -> Any:
    """Advance ``run_loop`` to its single ``when_all`` yield."""
    return next(gen)


# ---------------------------------------------------------------------------
# Expansion — happy spawn + ordered merge
# ---------------------------------------------------------------------------


def test_spawns_one_child_per_item_with_deterministic_ids() -> None:
    ctx = _StubContext()
    graph = _loop_graph(for_each_cel="inputs.items", inputs_schema=_ARRAY_OF_OBJECTS)
    step_ctx = _ctx(
        inputs={"items": [{"id": "a"}, {"id": "b"}, {"id": "c"}]},
        workflow_context=ctx,
    )

    gen = SubOrchestrationManager().run_loop(step_ctx, graph, _STEP_ID)
    token = _drive_to_when_all(gen)

    assert token[0] == "when_all"
    # One spawn per item, in input order, each under the child workflow name.
    assert [c["workflow"] for c in ctx.calls] == [CHILD_STEP_WORKFLOW_NAME] * 3
    assert [c["instance_id"] for c in ctx.calls] == [
        "parent-1/scan/a",
        "parent-1/scan/b",
        "parent-1/scan/c",
    ]
    # Each child input carries the per-iteration item + the loop body graph.
    decoded = [ChildStepInput.from_dict(c["input"]) for c in ctx.calls]
    assert [d.item for d in decoded] == [{"id": "a"}, {"id": "b"}, {"id": "c"}]
    assert all(d.step_id == _STEP_ID for d in decoded)
    assert all(d.loop_var == "item" for d in decoded)
    assert all(d.inputs == {"items": [{"id": "a"}, {"id": "b"}, {"id": "c"}]} for d in decoded)


def test_merges_child_outputs_in_input_order() -> None:
    ctx = _StubContext()
    graph = _loop_graph(for_each_cel="inputs.items", inputs_schema=_ARRAY_OF_OBJECTS)
    step_ctx = _ctx(inputs={"items": [{"id": "a"}, {"id": "b"}]}, workflow_context=ctx)

    gen = SubOrchestrationManager().run_loop(step_ctx, graph, _STEP_ID)
    _drive_to_when_all(gen)

    results = [
        ChildStepResult.succeeded({"rank": 1}).to_dict(),
        ChildStepResult.succeeded({"rank": 2}).to_dict(),
    ]
    with pytest.raises(StopIteration) as si:
        gen.send(results)
    assert si.value.value == [{"rank": 1}, {"rank": 2}]


def test_propagates_parent_outputs_to_children() -> None:
    ctx = _StubContext()
    graph = _loop_graph(for_each_cel="inputs.items", inputs_schema=_ARRAY_OF_OBJECTS)
    step_ctx = _ctx(
        inputs={"items": [{"id": "a"}]},
        workflow_context=ctx,
        outputs={"prior": {"bonus": 5}},
    )

    gen = SubOrchestrationManager().run_loop(step_ctx, graph, _STEP_ID)
    _drive_to_when_all(gen)

    decoded = ChildStepInput.from_dict(ctx.calls[0]["input"])
    assert decoded.parent_outputs == {"prior": {"bonus": 5}}


# ---------------------------------------------------------------------------
# Empty iterable — zero children, empty output list, no ``when_all``
# ---------------------------------------------------------------------------


def test_empty_iterable_returns_empty_list_without_yielding() -> None:
    ctx = _StubContext()
    graph = _loop_graph(for_each_cel="inputs.items", inputs_schema=_ARRAY_OF_OBJECTS)
    step_ctx = _ctx(inputs={"items": []}, workflow_context=ctx)

    gen = SubOrchestrationManager().run_loop(step_ctx, graph, _STEP_ID)
    with pytest.raises(StopIteration) as si:
        next(gen)
    assert si.value.value == []
    assert ctx.calls == []
    assert ctx.when_all_tasks is None


# ---------------------------------------------------------------------------
# Expansion errors → step.loop_expansion_error
# ---------------------------------------------------------------------------


def test_non_list_for_each_raises_loop_expansion_error() -> None:
    ctx = _StubContext()
    graph = _loop_graph(
        for_each_cel="inputs.count",
        inputs_schema={
            "type": "object",
            "properties": {"count": {"type": "integer"}},
            "required": ["count"],
        },
    )
    step_ctx = _ctx(inputs={"count": 5}, workflow_context=ctx)

    gen = SubOrchestrationManager().run_loop(step_ctx, graph, _STEP_ID)
    with pytest.raises(LoopExpansionError) as exc:
        next(gen)
    assert exc.value.kind == "step.loop_expansion_error"
    assert exc.value.step_id == _STEP_ID
    assert exc.value.cause_kind is None
    assert ctx.calls == []


def test_for_each_eval_error_raises_loop_expansion_error() -> None:
    ctx = _StubContext()
    # ``inputs.maybe`` is declared optional but absent at runtime, so the
    # CEL evaluator raises an unbound-name error mid-expansion.
    graph = _loop_graph(
        for_each_cel="inputs.maybe",
        inputs_schema={
            "type": "object",
            "properties": {"maybe": {"type": "array", "items": {"type": "object"}}},
            "required": [],
        },
    )
    step_ctx = _ctx(inputs={}, workflow_context=ctx)

    gen = SubOrchestrationManager().run_loop(step_ctx, graph, _STEP_ID)
    with pytest.raises(LoopExpansionError) as exc:
        next(gen)
    assert exc.value.kind == "step.loop_expansion_error"
    assert exc.value.cause_kind is not None
    assert exc.value.source == "${{ inputs.maybe }}"


def test_missing_for_each_call_site_raises_loop_expansion_error() -> None:
    ctx = _StubContext()
    graph = _loop_graph(for_each_cel="inputs.items", inputs_schema=_ARRAY_OF_OBJECTS)
    # Strip the forEach call site to exercise the defensive guard.
    node = graph.nodes[0]
    stripped = ExecutionNode(
        step_id=node.step_id,
        kind=node.kind,
        primitive_handler=node.primitive_handler,
        retry_policy=node.retry_policy,
        on_error_routes=node.on_error_routes,
        call_sites={},
        step_source=node.step_source,
    )
    graph = ExecutionGraph(
        nodes=(stripped,),
        edges=(),
        topological_order=(stripped.step_id,),
        metadata=graph.metadata,
    )
    step_ctx = _ctx(inputs={"items": [{"id": "a"}]}, workflow_context=ctx)

    gen = SubOrchestrationManager().run_loop(step_ctx, graph, _STEP_ID)
    with pytest.raises(LoopExpansionError) as exc:
        next(gen)
    assert exc.value.kind == "step.loop_expansion_error"


# ---------------------------------------------------------------------------
# Spawn-id failure → step.sub_orchestration_spawn_error
# ---------------------------------------------------------------------------


def test_malformed_child_id_raises_spawn_error() -> None:
    ctx = _StubContext()
    graph = _loop_graph(for_each_cel="inputs.items", inputs_schema=_ARRAY_OF_OBJECTS)
    # A parent run id containing the reserved separator makes every
    # deterministic child instance id malformed.
    step_ctx = _ctx(
        inputs={"items": [{"id": "a"}]},
        workflow_context=ctx,
        run_id="parent/oops",
    )

    gen = SubOrchestrationManager().run_loop(step_ctx, graph, _STEP_ID)
    with pytest.raises(SubOrchestrationSpawnError) as exc:
        next(gen)
    assert exc.value.kind == "step.sub_orchestration_spawn_error"
    assert exc.value.step_id == _STEP_ID
    assert exc.value.cause is not None


# ---------------------------------------------------------------------------
# Child failure → step.sub_workflow_failed (short-circuit)
# ---------------------------------------------------------------------------


def test_single_child_failure_short_circuits() -> None:
    ctx = _StubContext()
    graph = _loop_graph(for_each_cel="inputs.items", inputs_schema=_ARRAY_OF_OBJECTS)
    step_ctx = _ctx(
        inputs={"items": [{"id": "0"}, {"id": "1"}, {"id": "2"}]},
        workflow_context=ctx,
    )

    gen = SubOrchestrationManager().run_loop(step_ctx, graph, _STEP_ID)
    _drive_to_when_all(gen)

    results = [
        ChildStepResult.succeeded({"ok": True}).to_dict(),
        ChildStepResult.failed({"kind": "step.boom", "message": "bad"}).to_dict(),
        ChildStepResult.succeeded({"ok": True}).to_dict(),
    ]
    with pytest.raises(SubWorkflowFailedError) as exc:
        gen.send(results)
    assert exc.value.kind == "step.sub_workflow_failed"
    assert exc.value.child_instance_id == "parent-1/scan/1"
    assert exc.value.iteration_key == "1"
    assert exc.value.child_kind == "step.boom"


def test_abnormal_child_failure_surfaces_sub_workflow_failed() -> None:
    ctx = _StubContext()
    graph = _loop_graph(for_each_cel="inputs.items", inputs_schema=_ARRAY_OF_OBJECTS)
    step_ctx = _ctx(
        inputs={"items": [{"id": "0"}, {"id": "1"}]},
        workflow_context=ctx,
    )

    gen = SubOrchestrationManager().run_loop(step_ctx, graph, _STEP_ID)
    _drive_to_when_all(gen)

    with pytest.raises(SubWorkflowFailedError) as exc:
        gen.throw(ChildWorkflowError("parent-1/scan/1", "RuntimeError", "kaboom"))
    assert exc.value.kind == "step.sub_workflow_failed"
    assert exc.value.child_instance_id == "parent-1/scan/1"
    assert exc.value.iteration_key == "1"
    assert exc.value.child_kind == "RuntimeError"


@pytest.mark.parametrize("instance_id", [None, "parent-1/scan/unknown"])
def test_abnormal_failure_without_matching_iteration_key(instance_id: str | None) -> None:
    ctx = _StubContext()
    graph = _loop_graph(for_each_cel="inputs.items", inputs_schema=_ARRAY_OF_OBJECTS)
    step_ctx = _ctx(inputs={"items": [{"id": "0"}]}, workflow_context=ctx)

    gen = SubOrchestrationManager().run_loop(step_ctx, graph, _STEP_ID)
    _drive_to_when_all(gen)

    with pytest.raises(SubWorkflowFailedError) as exc:
        gen.throw(ChildWorkflowError(cast(str, instance_id), "RuntimeError", "boom"))
    assert exc.value.child_instance_id == instance_id
    assert exc.value.iteration_key is None


def test_unknown_step_id_raises_key_error() -> None:
    ctx = _StubContext()
    graph = _loop_graph(for_each_cel="inputs.items", inputs_schema=_ARRAY_OF_OBJECTS)
    step_ctx = _ctx(inputs={"items": [{"id": "a"}]}, workflow_context=ctx)

    gen = SubOrchestrationManager().run_loop(step_ctx, graph, "nope")
    with pytest.raises(KeyError):
        next(gen)


# ---------------------------------------------------------------------------
# End-to-end under the FakeWorkflowRuntime
# ---------------------------------------------------------------------------


def _echo_child(ctx: Any, raw: Any) -> Generator[Any, Any, dict[str, Any]]:
    child_input = ChildStepInput.from_dict(raw)
    if False:  # pragma: no cover - generator marker
        yield None
    item = cast(dict[str, Any], child_input.item)
    return ChildStepResult.succeeded({"n": item["n"]}).to_dict()


def _make_parent(graph: ExecutionGraph) -> Any:
    def run_parent(ctx: Any, raw: Any) -> Generator[Any, Any, list[Any]]:
        step_ctx = StepExecutionContext(
            run_id=cast(RunId, ctx.instance_id),
            workspace_id="ws-1",
            workflow_version_id="wf-v1",
            inputs=raw,
            workflow_context=ctx,
            outputs={},
            clock=DaprWorkflowClock(ctx),
        )
        result = yield from SubOrchestrationManager().run_loop(step_ctx, graph, _STEP_ID)
        return result

    return run_parent


@pytest.fixture
def runtime() -> FakeWorkflowRuntime:
    return FakeWorkflowRuntime(now=_NOW)


@pytest.fixture
def client(runtime: FakeWorkflowRuntime) -> FakeWorkflowClient:
    return runtime.client()


async def test_end_to_end_loop_fan_out(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    inputs_schema = {
        "type": "object",
        "properties": {
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {"id": {"type": "string"}, "n": {"type": "integer"}},
                    "required": ["id", "n"],
                },
            }
        },
        "required": ["items"],
    }
    graph = _loop_graph(for_each_cel="inputs.items", inputs_schema=inputs_schema)
    runtime.register_workflow(_echo_child, name=CHILD_STEP_WORKFLOW_NAME)
    runtime.register_workflow(_make_parent(graph), name="parent")

    inputs = {"items": [{"id": "a", "n": 1}, {"id": "b", "n": 2}, {"id": "c", "n": 3}]}
    returned = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input=inputs, instance_id="parent-1")
    )
    state = runtime.instance(returned)

    assert state.status == RunStatus.COMPLETED
    assert state.output == [{"n": 1}, {"n": 2}, {"n": 3}]
    assert state.spawned_children == [
        "parent-1/scan/a",
        "parent-1/scan/b",
        "parent-1/scan/c",
    ]


async def test_replay_reproduces_identical_child_set(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    inputs_schema = {
        "type": "object",
        "properties": {
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {"id": {"type": "string"}, "n": {"type": "integer"}},
                    "required": ["id", "n"],
                },
            }
        },
        "required": ["items"],
    }
    graph = _loop_graph(for_each_cel="inputs.items", inputs_schema=inputs_schema)
    runtime.register_workflow(_echo_child, name=CHILD_STEP_WORKFLOW_NAME)
    runtime.register_workflow(_make_parent(graph), name="parent")

    inputs = {"items": [{"id": "a", "n": 10}, {"id": "b", "n": 20}]}

    first = runtime.instance(
        await client.schedule_new_workflow(
            ScheduleWorkflowRequest(workflow="parent", input=inputs, instance_id="run-1")
        )
    )
    second = runtime.instance(
        await client.schedule_new_workflow(
            ScheduleWorkflowRequest(workflow="parent", input=inputs, instance_id="run-2")
        )
    )

    assert first.output == second.output == [{"n": 10}, {"n": 20}]
    assert first.spawned_children == ["run-1/scan/a", "run-1/scan/b"]
    assert second.spawned_children == ["run-2/scan/a", "run-2/scan/b"]

"""WF-IMPL-097 — sub-orchestration integration suite (production wiring).

The sub-orchestration primitives are exhaustively unit-tested against
the manager's own generators (``tests/steps/test_sub_orchestration_*``)
and the orchestrator's inline dispatch is proven with a stub manager
(``tests/runs/test_orchestrator_sub_orchestration``). This module closes
the loop by driving the **real** default :class:`SubOrchestrationManager`
through the production :func:`make_run_orchestrator` factory under
:class:`FakeWorkflowRuntime`, exactly as the FastAPI lifespan wires it,
exercising every ``step.*`` kind end-to-end:

* ``forEach`` loops — N items, an empty iterable, a ``where:`` pre-filter,
  and a child failure that short-circuits the run.
* ``workflow:`` sub-orchestration — a ``workflow:`` node with no child
  graph resolver wired fails the run with ``step.sub_orchestration_spawn_error``
  (the Catalog-backed resolver is wired separately).
* ``approval:`` gates — the always-ready durable timer wins the race when
  no decision is buffered, failing the run with ``step.approval_timeout``.
* Replay determinism — re-running the same input spawns a byte-identical
  child id set and binds identical outputs.
"""

from __future__ import annotations

import asyncio
from collections.abc import Generator
from datetime import UTC, datetime
from typing import Any, cast

from custos_cel import (
    BoolType,
    MapType,
    SchemaBindings,
    StringType,
    parse,
    type_check,
)

from custos_workflow.document import ApprovalStep, LetStep, WorkflowStep
from custos_workflow.graph import (
    CallSiteKind,
    ExecutionGraph,
    ExecutionNode,
    GraphMetadata,
    PrimitiveHandler,
    StepKind,
    TypedCallSite,
    to_json,
)
from custos_workflow.runs import (
    NoopStepHandler,
    RunInput,
    RunOutput,
    RunStatus,
    make_run_orchestrator,
)
from custos_workflow.runs.orchestrator import WORKFLOW_NAME
from custos_workflow.runtime import (
    FakeWorkflowRuntime,
    ScheduleWorkflowRequest,
)
from custos_workflow.runtime import RunStatus as RuntimeRunStatus
from custos_workflow.runtime.fake import FakeWorkflowFn
from custos_workflow.steps.sub_orchestration import (
    CHILD_STEP_WORKFLOW_NAME,
    ChildStepInput,
    ChildStepResult,
)

_NOW = datetime(2026, 6, 1, 9, 0, 0, tzinfo=UTC)
_STEP_ID = "scan"
_WORKFLOW_REF = "11111111-1111-1111-1111-111111111111"

_ARRAY_OF_OBJECTS = {
    "type": "object",
    "properties": {"items": {"type": "array", "items": {"type": "object"}}},
    "required": ["items"],
}


# ---------------------------------------------------------------------------
# Graph builders
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


def _for_each_call_site(cel: str, *, inputs_schema: dict[str, Any]) -> TypedCallSite:
    return TypedCallSite(
        source=f"${{{{ {cel} }}}}",
        typed_ast=type_check(parse(cel), SchemaBindings(inputs=inputs_schema)),
        kind=CallSiteKind.FOR_EACH,
        document_path="spec.steps[0].forEach",
    )


def _where_call_site(cel: str, *, inputs_schema: dict[str, Any]) -> TypedCallSite:
    bindings = SchemaBindings(
        inputs=inputs_schema,
        let={"item": MapType(key=StringType(), value=BoolType())},
    )
    return TypedCallSite(
        source=f"${{{{ {cel} }}}}",
        typed_ast=type_check(parse(cel), bindings),
        kind=CallSiteKind.WHERE,
        document_path="spec.steps[0].where",
    )


def _loop_graph(
    *,
    for_each_cel: str = "inputs.items",
    inputs_schema: dict[str, Any] | None = None,
    where_cel: str | None = None,
) -> ExecutionGraph:
    schema = inputs_schema or _ARRAY_OF_OBJECTS
    sites = {"forEach": _for_each_call_site(for_each_cel, inputs_schema=schema)}
    if where_cel is not None:
        sites["where"] = _where_call_site(where_cel, inputs_schema=schema)
    node = ExecutionNode(
        step_id=_STEP_ID,
        kind=StepKind.LET,
        primitive_handler=PrimitiveHandler.SUB_ORCHESTRATION,
        retry_policy=None,
        on_error_routes=(),
        call_sites=sites,
        step_source=LetStep.model_validate({"id": _STEP_ID, "let": {"noop": 0}}),
    )
    return _single_node_graph(node)


def _approval_graph(*, timeout: str = "PT24H") -> ExecutionGraph:
    node = ExecutionNode(
        step_id=_STEP_ID,
        kind=StepKind.APPROVAL,
        primitive_handler=PrimitiveHandler.SUB_ORCHESTRATION,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=ApprovalStep.model_validate(
            {"id": _STEP_ID, "approval": {"approvers": ["alice@example.com"], "timeout": timeout}}
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


def _run_input(graph: ExecutionGraph, *, inputs: dict[str, Any] | None = None) -> RunInput:
    return RunInput(
        workspace_id="ws-001",
        workflow_version_id="wfv-001",
        compiled_graph_json=to_json(graph),
        inputs=inputs or {},
        idempotency_key="idem-1",
    )


# ---------------------------------------------------------------------------
# Real child step workflows registered under CHILD_STEP_WORKFLOW_NAME
# ---------------------------------------------------------------------------


def _echo_child(ctx: Any, raw: Any) -> Generator[Any, Any, dict[str, Any]]:
    """A child step that echoes the per-iteration item's ``n`` back."""
    child_input = ChildStepInput.from_dict(raw)
    if False:  # pragma: no cover - generator marker
        yield None
    item = cast(dict[str, Any], child_input.item)
    return ChildStepResult.succeeded({"n": item["n"]}).to_dict()


def _failing_child(ctx: Any, raw: Any) -> Generator[Any, Any, dict[str, Any]]:
    """A child step that surfaces a terminal failure envelope."""
    if False:  # pragma: no cover - generator marker
        yield None
    return ChildStepResult.failed({"kind": "step.boom", "message": "child boom"}).to_dict()


# ---------------------------------------------------------------------------
# Driver — the production wiring: real default manager + orchestrator
# ---------------------------------------------------------------------------


def _drive(
    run_input: RunInput,
    *,
    child_step: FakeWorkflowFn | None = None,
    child_graph_resolver: Any = None,
    instance_id: str = "parent-1",
) -> tuple[FakeWorkflowRuntime, Any]:
    """Wire the real orchestrator + default manager and run one instance.

    Passing ``sub_orchestration_manager=None`` makes
    :func:`make_run_orchestrator` build the real default
    :class:`SubOrchestrationManager`, so this is the exact production
    dispatch path. The child step (loop body) is registered under
    :data:`CHILD_STEP_WORKFLOW_NAME` so loops fan out against a genuine
    child instance.
    """
    runtime = FakeWorkflowRuntime(now=_NOW)
    client = runtime.client()
    orchestrator = make_run_orchestrator(
        NoopStepHandler(),
        child_graph_resolver=child_graph_resolver,
    )
    runtime.register_workflow(cast(FakeWorkflowFn, orchestrator), name=WORKFLOW_NAME)
    if child_step is not None:
        runtime.register_workflow(child_step, name=CHILD_STEP_WORKFLOW_NAME)
    returned = asyncio.run(
        client.schedule_new_workflow(
            ScheduleWorkflowRequest(
                workflow=WORKFLOW_NAME, input=run_input, instance_id=instance_id
            )
        )
    )
    return runtime, runtime.instance(returned)


# ---------------------------------------------------------------------------
# forEach — real fan-out end-to-end
# ---------------------------------------------------------------------------


def test_loop_fans_out_one_child_per_item() -> None:
    graph = _loop_graph()
    _runtime, state = _drive(
        _run_input(
            graph,
            inputs={"items": [{"id": "a", "n": 1}, {"id": "b", "n": 2}, {"id": "c", "n": 3}]},
        ),
        child_step=_echo_child,
    )

    assert state.status == RuntimeRunStatus.COMPLETED
    assert isinstance(state.output, RunOutput)
    assert state.output.status == RunStatus.SUCCEEDED.value
    assert dict(state.output.outputs["scan"]) == {"results": [{"n": 1}, {"n": 2}, {"n": 3}]}
    assert state.spawned_children == [
        "parent-1/scan/a",
        "parent-1/scan/b",
        "parent-1/scan/c",
    ]


def test_loop_empty_iterable_binds_empty_results() -> None:
    graph = _loop_graph()
    _runtime, state = _drive(
        _run_input(graph, inputs={"items": []}),
        child_step=_echo_child,
    )

    assert state.status == RuntimeRunStatus.COMPLETED
    assert isinstance(state.output, RunOutput)
    assert state.output.status == RunStatus.SUCCEEDED.value
    assert dict(state.output.outputs["scan"]) == {"results": []}
    assert state.spawned_children == []


def test_loop_where_prefilter_spawns_only_kept_items() -> None:
    graph = _loop_graph(where_cel='let.item["keep"]')
    _runtime, state = _drive(
        _run_input(
            graph,
            inputs={
                "items": [
                    {"id": "a", "n": 1, "keep": True},
                    {"id": "b", "n": 2, "keep": False},
                    {"id": "c", "n": 3, "keep": True},
                ]
            },
        ),
        child_step=_echo_child,
    )

    assert state.status == RuntimeRunStatus.COMPLETED
    assert isinstance(state.output, RunOutput)
    assert dict(state.output.outputs["scan"]) == {"results": [{"n": 1}, {"n": 3}]}
    assert state.spawned_children == ["parent-1/scan/a", "parent-1/scan/c"]


def test_loop_child_failure_short_circuits_run() -> None:
    graph = _loop_graph()
    _runtime, state = _drive(
        _run_input(graph, inputs={"items": [{"id": "a", "n": 1}]}),
        child_step=_failing_child,
    )

    assert isinstance(state.output, RunOutput)
    assert state.output.status == RunStatus.FAILED.value
    assert state.output.failed_step == _STEP_ID
    assert state.output.failure_envelope is not None
    assert state.output.failure_envelope["kind"] == "step.sub_workflow_failed"


# ---------------------------------------------------------------------------
# workflow: — no resolver wired fails the run cleanly
# ---------------------------------------------------------------------------


def test_sub_workflow_without_resolver_fails_with_spawn_error() -> None:
    graph = _workflow_graph()
    _runtime, state = _drive(_run_input(graph), child_graph_resolver=None)

    assert isinstance(state.output, RunOutput)
    assert state.output.status == RunStatus.FAILED.value
    assert state.output.failed_step == _STEP_ID
    assert state.output.failure_envelope is not None
    assert state.output.failure_envelope["kind"] == "step.sub_orchestration_spawn_error"


# ---------------------------------------------------------------------------
# approval: — durable timer wins when no decision is buffered
# ---------------------------------------------------------------------------


def test_approval_times_out_when_no_decision_buffered() -> None:
    graph = _approval_graph()
    _runtime, state = _drive(_run_input(graph))

    assert isinstance(state.output, RunOutput)
    assert state.output.status == RunStatus.FAILED.value
    assert state.output.failed_step == _STEP_ID
    assert state.output.failure_envelope is not None
    assert state.output.failure_envelope["kind"] == "step.approval_timeout"


# ---------------------------------------------------------------------------
# Replay determinism — identical child set + outputs across re-runs
# ---------------------------------------------------------------------------


def test_loop_run_is_deterministic_across_reruns() -> None:
    graph = _loop_graph()
    inputs = {"items": [{"id": "a", "n": 1}, {"id": "b", "n": 2}, {"id": "c", "n": 3}]}

    runs = [_drive(_run_input(graph, inputs=inputs), child_step=_echo_child)[1] for _ in range(2)]

    first, second = runs
    assert first.spawned_children == second.spawned_children
    assert isinstance(first.output, RunOutput)
    assert isinstance(second.output, RunOutput)
    assert dict(first.output.outputs["scan"]) == dict(second.output.outputs["scan"])

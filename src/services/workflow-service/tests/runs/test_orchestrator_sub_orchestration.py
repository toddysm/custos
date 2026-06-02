"""WF-IMPL-093 — orchestrator inline dispatch of ``SUB_ORCHESTRATION`` nodes.

The Run Controller orchestrator (:func:`make_run_orchestrator`) drives
every :attr:`PrimitiveHandler.SUB_ORCHESTRATION` node through the
:class:`SubOrchestrationManager` inline (``yield from``), mirroring the
``wait:`` path. This module proves the wiring:

* ``forEach`` loops run end-to-end under :class:`FakeWorkflowRuntime`
  with the real manager + a stub child workflow; the ordered per-child
  output list binds under a single ``results`` key so the step's
  outputs stay a mapping.
* ``approval:`` and ``workflow:`` dispatch is verified with an injected
  stub manager (the manager's own generators are exhaustively covered
  in ``tests/steps/test_sub_orchestration_*``); we assert the
  discriminator selects the right method, threads the resolved child
  graph, and binds the returned mapping.
* A ``workflow:`` node with no ``child_graph_resolver`` wired fails the
  step with a structured ``step.sub_orchestration_spawn_error``
  (Catalog wiring is deferred to WF-IMPL-094).
* A :class:`StepCoordinatorError` raised by the manager short-circuits
  the run into a ``status="failed"`` :class:`RunOutput` carrying the
  manager's failure envelope.
"""

from __future__ import annotations

import asyncio
from collections.abc import Generator
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from custos_cel import (
    SchemaBindings,
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
    StepExecutionContext,
    make_run_orchestrator,
)
from custos_workflow.runs.orchestrator import WORKFLOW_NAME
from custos_workflow.runtime import (
    FakeWorkflowClient,
    FakeWorkflowRuntime,
    ScheduleWorkflowRequest,
)
from custos_workflow.runtime import RunStatus as RuntimeRunStatus
from custos_workflow.runtime.fake import FakeWorkflowFn
from custos_workflow.steps.errors import StepCoordinatorError, SubOrchestrationSpawnError
from custos_workflow.steps.sub_orchestration import (
    CHILD_STEP_WORKFLOW_NAME,
    ChildStepInput,
    ChildStepResult,
)

_NOW = datetime(2026, 6, 1, 9, 0, 0, tzinfo=UTC)
_STEP_ID = "scan"
_WORKFLOW_REF = "11111111-1111-1111-1111-111111111111"


# ---------------------------------------------------------------------------
# Graph builders
# ---------------------------------------------------------------------------

_ARRAY_OF_OBJECTS = {
    "type": "object",
    "properties": {"items": {"type": "array", "items": {"type": "object"}}},
    "required": ["items"],
}


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
    ast = parse(cel)
    return TypedCallSite(
        source=f"${{{{ {cel} }}}}",
        typed_ast=type_check(ast, SchemaBindings(inputs=inputs_schema)),
        kind=CallSiteKind.FOR_EACH,
        document_path="spec.steps[0].forEach",
    )


def _loop_graph(*, for_each_cel: str, inputs_schema: dict[str, Any]) -> ExecutionGraph:
    node = ExecutionNode(
        step_id=_STEP_ID,
        kind=StepKind.LET,
        primitive_handler=PrimitiveHandler.SUB_ORCHESTRATION,
        retry_policy=None,
        on_error_routes=(),
        call_sites={"forEach": _for_each_call_site(for_each_cel, inputs_schema=inputs_schema)},
        step_source=LetStep.model_validate({"id": _STEP_ID, "let": {"noop": 0}}),
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


def _workflow_graph(*, workflow_ref: str = _WORKFLOW_REF) -> ExecutionGraph:
    node = ExecutionNode(
        step_id=_STEP_ID,
        kind=StepKind.WORKFLOW,
        primitive_handler=PrimitiveHandler.SUB_ORCHESTRATION,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=WorkflowStep.model_validate(
            {"id": _STEP_ID, "workflow": workflow_ref, "with": None}
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
# Stub Sub-Orchestration Manager — records dispatch + emits canned results
# ---------------------------------------------------------------------------


class _StubManager:
    """Stand-in for :class:`SubOrchestrationManager`.

    Each method is a generator that yields nothing (so the orchestrator
    drives it to completion synchronously under the runtime) and returns
    a canned value — or raises a pre-seeded :class:`StepCoordinatorError`.
    Records the call arguments so tests can assert the discriminator.
    """

    def __init__(
        self,
        *,
        loop_result: list[Any] | None = None,
        approval_result: dict[str, Any] | None = None,
        sub_workflow_result: dict[str, dict[str, Any]] | None = None,
        raise_on_approval: StepCoordinatorError | None = None,
    ) -> None:
        self._loop_result = loop_result if loop_result is not None else []
        self._approval_result = approval_result if approval_result is not None else {}
        self._sub_workflow_result = sub_workflow_result if sub_workflow_result is not None else {}
        self._raise_on_approval = raise_on_approval
        self.run_loop_calls: list[str] = []
        self.run_approval_calls: list[str] = []
        self.run_sub_workflow_calls: list[tuple[str, ExecutionGraph]] = []

    def run_loop(
        self, ctx: StepExecutionContext, graph: ExecutionGraph, step_id: str
    ) -> Generator[Any, Any, list[Any]]:
        self.run_loop_calls.append(step_id)
        if False:  # pragma: no cover - generator marker
            yield None
        return list(self._loop_result)

    def run_approval(
        self, ctx: StepExecutionContext, graph: ExecutionGraph, step_id: str
    ) -> Generator[Any, Any, dict[str, Any]]:
        self.run_approval_calls.append(step_id)
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
        self.run_sub_workflow_calls.append((step_id, child_graph))
        if False:  # pragma: no cover - generator marker
            yield None
        return dict(self._sub_workflow_result)


# ---------------------------------------------------------------------------
# Drivers
# ---------------------------------------------------------------------------


def _register(runtime: FakeWorkflowRuntime, orchestrator: Any) -> None:
    runtime.register_workflow(cast(FakeWorkflowFn, orchestrator), name=WORKFLOW_NAME)


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
    sub_orchestration_manager: Any = None,
    child_graph_resolver: Any = None,
    instance_id: str = "parent-1",
) -> RunOutput:
    orchestrator = make_run_orchestrator(
        NoopStepHandler(),
        sub_orchestration_manager=sub_orchestration_manager,
        child_graph_resolver=child_graph_resolver,
    )
    _register(runtime, orchestrator)
    returned = asyncio.run(
        client.schedule_new_workflow(
            ScheduleWorkflowRequest(
                workflow=WORKFLOW_NAME, input=run_input, instance_id=instance_id
            )
        )
    )
    state = runtime.instance(returned)
    assert isinstance(state.output, RunOutput)
    return state.output


# ---------------------------------------------------------------------------
# forEach — real manager, end-to-end under the runtime
# ---------------------------------------------------------------------------


def _echo_child(ctx: Any, raw: Any) -> Generator[Any, Any, dict[str, Any]]:
    child_input = ChildStepInput.from_dict(raw)
    if False:  # pragma: no cover - generator marker
        yield None
    item = cast(dict[str, Any], child_input.item)
    return ChildStepResult.succeeded({"n": item["n"]}).to_dict()


async def test_for_each_binds_results_list_under_results_key(
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
    orchestrator = make_run_orchestrator(NoopStepHandler())
    _register(runtime, orchestrator)

    run_input = _run_input(
        graph,
        inputs={"items": [{"id": "a", "n": 1}, {"id": "b", "n": 2}, {"id": "c", "n": 3}]},
    )
    returned = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow=WORKFLOW_NAME, input=run_input, instance_id="parent-1")
    )
    state = runtime.instance(returned)

    assert state.status == RuntimeRunStatus.COMPLETED
    assert isinstance(state.output, RunOutput)
    assert state.output.status == RunStatus.SUCCEEDED.value
    # The ordered per-child output list binds under ``results`` so the
    # step's outputs stay a mapping.
    assert dict(state.output.outputs["scan"]) == {"results": [{"n": 1}, {"n": 2}, {"n": 3}]}
    assert state.spawned_children == [
        "parent-1/scan/a",
        "parent-1/scan/b",
        "parent-1/scan/c",
    ]


def test_for_each_empty_iterable_binds_empty_results(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    graph = _loop_graph(for_each_cel="inputs.items", inputs_schema=_ARRAY_OF_OBJECTS)
    stub = _StubManager(loop_result=[])
    output = _run(
        runtime,
        client,
        _run_input(graph, inputs={"items": []}),
        sub_orchestration_manager=stub,
    )

    assert stub.run_loop_calls == [_STEP_ID]
    assert output.status == RunStatus.SUCCEEDED.value
    assert dict(output.outputs["scan"]) == {"results": []}


# ---------------------------------------------------------------------------
# approval — stub manager, decision payload binds directly
# ---------------------------------------------------------------------------


def test_approval_binds_decision_mapping(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    graph = _approval_graph()
    stub = _StubManager(approval_result={"decision": "approved", "by": "alice@example.com"})
    output = _run(
        runtime,
        client,
        _run_input(graph),
        sub_orchestration_manager=stub,
    )

    assert stub.run_approval_calls == [_STEP_ID]
    assert stub.run_loop_calls == []
    assert output.status == RunStatus.SUCCEEDED.value
    assert dict(output.outputs["scan"]) == {
        "decision": "approved",
        "by": "alice@example.com",
    }


def test_approval_failure_short_circuits_run_as_failed(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    graph = _approval_graph()
    err = SubOrchestrationSpawnError(
        "gate timed out",
        run_id="parent-1",
        step_id=_STEP_ID,
    )
    stub = _StubManager(raise_on_approval=err)
    output = _run(
        runtime,
        client,
        _run_input(graph),
        sub_orchestration_manager=stub,
    )

    assert output.status == RunStatus.FAILED.value
    assert output.failed_step == _STEP_ID
    assert output.failure_envelope is not None
    assert output.failure_envelope["kind"] == "step.sub_orchestration_spawn_error"


# ---------------------------------------------------------------------------
# workflow: — child graph resolved via the injected resolver
# ---------------------------------------------------------------------------


def test_workflow_resolves_child_graph_and_binds_child_bag(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    graph = _workflow_graph(workflow_ref=_WORKFLOW_REF)
    child = _child_graph()
    resolved_refs: list[str] = []

    def _resolver(ref: str) -> ExecutionGraph:
        resolved_refs.append(ref)
        return child

    stub = _StubManager(sub_workflow_result={"greet": {"message": "hi"}})
    output = _run(
        runtime,
        client,
        _run_input(graph),
        sub_orchestration_manager=stub,
        child_graph_resolver=_resolver,
    )

    # The resolver is consulted with the step's workflow reference.
    assert resolved_refs == [_WORKFLOW_REF]
    # The resolved child graph is threaded into the manager.
    assert len(stub.run_sub_workflow_calls) == 1
    recorded_step, recorded_graph = stub.run_sub_workflow_calls[0]
    assert recorded_step == _STEP_ID
    assert recorded_graph is child
    assert output.status == RunStatus.SUCCEEDED.value
    assert dict(output.outputs["scan"]) == {"greet": {"message": "hi"}}


def test_workflow_without_resolver_fails_with_spawn_error(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    graph = _workflow_graph()
    stub = _StubManager()
    output = _run(
        runtime,
        client,
        _run_input(graph),
        sub_orchestration_manager=stub,
        child_graph_resolver=None,
    )

    assert stub.run_sub_workflow_calls == []
    assert output.status == RunStatus.FAILED.value
    assert output.failed_step == _STEP_ID
    assert output.failure_envelope is not None
    assert output.failure_envelope["kind"] == "step.sub_orchestration_spawn_error"
    assert "resolver" in output.failure_envelope["message"]

"""WF-IMPL-107 — orchestrator inline dispatch of ``RESUME_SUBSCRIPTION`` nodes.

The Run Controller orchestrator (:func:`make_run_orchestrator`) drives
every :attr:`PrimitiveHandler.RESUME_SUBSCRIPTION` (``waitFor:``) node
through an injected :class:`WaitForStepHandler` inline, mirroring the
``wait:`` / sub-orchestration paths. This module proves the wiring:

* A ``waitFor:`` node with a ``resume_handler`` wired resolves the
  ``eventKey`` and parks the run as ``status="waiting"`` — it no longer
  reaches the generic dispatcher's ``step.kind_not_implemented`` raise.
* The parked run carries the waiting step id + a log-safe reason derived
  from the resolved event key, and steps *after* the wait do not run.
* A ``waitFor:`` node whose ``eventKey`` resolution fails short-circuits
  the run into ``status="failed"`` with the handler's failure envelope.
* Without a ``resume_handler`` wired, the node falls through to the
  generic ``handler`` (the production ``StepCoordinator`` raises
  ``step.kind_not_implemented``); the durable register / suspend /
  resume wiring lands in WF-IMPL-108.
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest
from custos_cel import SchemaBindings, parse, type_check

from custos_workflow.document import LetStep, WaitForStep
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
    RunInput,
    RunOutput,
    StepExecutionContext,
    make_run_orchestrator,
)
from custos_workflow.runs.orchestrator import WORKFLOW_NAME
from custos_workflow.runs.step_handler import StepResult, StepSucceeded
from custos_workflow.runtime import (
    FakeWorkflowClient,
    FakeWorkflowRuntime,
    ScheduleWorkflowRequest,
)
from custos_workflow.runtime.fake import FakeWorkflowFn
from custos_workflow.steps.resume import (
    InMemoryResumeSubscriptionMirrorRepository,
    WaitForStepHandler,
)

_STEP_ID = "await-event"
_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"key": {"type": "string"}},
}


# ---------------------------------------------------------------------------
# Graph builders
# ---------------------------------------------------------------------------


def _metadata() -> GraphMetadata:
    return GraphMetadata(
        workflow_name="pipeline",
        workflow_workspace="ws-1",
        document_api_version="custos.dev/v1",
    )


def _call_site(cel: str, kind: CallSiteKind, document_path: str) -> TypedCallSite:
    return TypedCallSite(
        source=f"${{{{ {cel} }}}}",
        typed_ast=type_check(parse(cel), SchemaBindings(inputs=_SCHEMA)),
        kind=kind,
        document_path=document_path,
    )


def _wait_for_node(
    *,
    step_id: str = _STEP_ID,
    event_key_cel: str = "inputs.key",
) -> ExecutionNode:
    spec: dict[str, Any] = {"eventKey": f"${{{{ {event_key_cel} }}}}"}
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.WAIT_FOR,
        primitive_handler=PrimitiveHandler.RESUME_SUBSCRIPTION,
        retry_policy=None,
        on_error_routes=(),
        call_sites={
            "waitFor.eventKey": _call_site(
                event_key_cel,
                CallSiteKind.WAIT_FOR_EVENT_KEY,
                "spec.steps[0].waitFor.eventKey",
            )
        },
        step_source=WaitForStep.model_validate({"id": step_id, "waitFor": spec}),
    )


def _let_node(*, step_id: str = "downstream") -> ExecutionNode:
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.LET,
        primitive_handler=PrimitiveHandler.EXPRESSION_INLINE,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=LetStep.model_validate({"id": step_id, "let": {"x": "literal"}}),
    )


def _graph(*nodes: ExecutionNode) -> ExecutionGraph:
    return ExecutionGraph(
        nodes=tuple(nodes),
        edges=(),
        topological_order=tuple(n.step_id for n in nodes),
        metadata=_metadata(),
    )


def _run_input(graph: ExecutionGraph, *, inputs: dict[str, Any] | None = None) -> RunInput:
    return RunInput(
        workspace_id="ws-001",
        workflow_version_id="wfv-001",
        compiled_graph_json=to_json(graph),
        inputs=inputs or {},
        idempotency_key="idem-1",
    )


def _resume_handler() -> WaitForStepHandler:
    return WaitForStepHandler(InMemoryResumeSubscriptionMirrorRepository())


# ---------------------------------------------------------------------------
# Recording handler — proves the generic-dispatch fall-through
# ---------------------------------------------------------------------------


class _RecordingHandler:
    """Stand-in generic :class:`StepHandler` that records dispatch.

    Returns an empty :class:`StepSucceeded` for every node so the
    orchestrator advances; records the dispatched step ids so a test can
    assert a ``waitFor:`` node fell through to the generic dispatcher
    (i.e. the inline resume arm was *not* taken when no ``resume_handler``
    is wired).
    """

    def __init__(self) -> None:
        self.executed: list[str] = []

    def execute(self, ctx: StepExecutionContext, graph: ExecutionGraph, step_id: str) -> StepResult:
        self.executed.append(step_id)
        return StepSucceeded(outputs={})


# ---------------------------------------------------------------------------
# Drivers
# ---------------------------------------------------------------------------


def _register(runtime: FakeWorkflowRuntime, orchestrator: Any) -> None:
    runtime.register_workflow(cast(FakeWorkflowFn, orchestrator), name=WORKFLOW_NAME)


@pytest.fixture
def runtime() -> FakeWorkflowRuntime:
    return FakeWorkflowRuntime()


@pytest.fixture
def client(runtime: FakeWorkflowRuntime) -> FakeWorkflowClient:
    return runtime.client()


def _run(
    runtime: FakeWorkflowRuntime,
    client: FakeWorkflowClient,
    run_input: RunInput,
    *,
    handler: Any = None,
    resume_handler: WaitForStepHandler | None = None,
    instance_id: str = "run-1",
) -> RunOutput:
    orchestrator = make_run_orchestrator(
        handler if handler is not None else _RecordingHandler(),
        resume_handler=resume_handler,
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
# waitFor: dispatch — parks the run as waiting
# ---------------------------------------------------------------------------


def test_wait_for_node_parks_run_waiting(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    run_input = _run_input(_graph(_wait_for_node()), inputs={"key": "order-approved"})

    output = _run(runtime, client, run_input, resume_handler=_resume_handler())

    assert output.status == "waiting"
    assert output.waiting_step == _STEP_ID
    # Reason is derived from the resolved event key, not the raw CEL.
    assert output.waiting_reason == "waitFor:order-approved"


def test_wait_for_does_not_run_downstream_steps(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    # A let node ordered after the wait must NOT execute — the run parks
    # at the wait and returns immediately.
    graph = _graph(_wait_for_node(), _let_node(step_id="downstream"))
    run_input = _run_input(graph, inputs={"key": "order-approved"})

    output = _run(runtime, client, run_input, resume_handler=_resume_handler())

    assert output.status == "waiting"
    assert output.waiting_step == _STEP_ID
    assert "downstream" not in output.outputs


# ---------------------------------------------------------------------------
# waitFor: dispatch — resolution failure fails the run
# ---------------------------------------------------------------------------


def test_wait_for_event_key_resolution_failure_fails_run(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    # ``eventKey`` is required; an empty resolved value raises inside the
    # handler before its first effect, so the handler returns StepFailed
    # without yielding — the orchestrator surfaces it as status="failed".
    run_input = _run_input(_graph(_wait_for_node()), inputs={"key": ""})

    output = _run(runtime, client, run_input, resume_handler=_resume_handler())

    assert output.status == "failed"
    assert output.failed_step == _STEP_ID
    assert output.failure_envelope is not None


# ---------------------------------------------------------------------------
# waitFor: dispatch — no resume_handler wired falls through to the dispatcher
# ---------------------------------------------------------------------------


def test_wait_for_without_resume_handler_falls_through_to_dispatcher(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    # With no ``resume_handler`` wired, the inline resume arm is skipped
    # and the node flows through the generic ``handler`` (production:
    # StepCoordinator → step.kind_not_implemented; durable wiring lands
    # in WF-IMPL-108). The recording handler proves the fall-through.
    handler = _RecordingHandler()
    run_input = _run_input(_graph(_wait_for_node()), inputs={"key": "order-approved"})

    output = _run(runtime, client, run_input, handler=handler, resume_handler=None)

    assert output.status == "succeeded"
    assert handler.executed == [_STEP_ID]

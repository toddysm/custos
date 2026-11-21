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
import json
import logging
import runpy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from unittest.mock import Mock

import pytest
from custos_callctx.execution import ExecutionIdentity
from custos_cel import SchemaBindings, parse, type_check

from custos_workflow.bindings import ActivityTypeRegistry
from custos_workflow.compiler import RunMeta, compile
from custos_workflow.document import LetStep, WaitForStep, WorkflowDocument
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
from custos_workflow.runs.step_handler import NoopStepHandler, StepResult, StepSucceeded
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


@pytest.mark.parametrize("already_aware", [False, True])
def test_actual_sdk_wait_persists_deterministic_utc_mirror(
    already_aware: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Real SDK history reaches persist/replay with exact TTL and immutable pin, without RPC."""
    from dapr.ext.workflow import DaprWorkflowContext
    from dapr.ext.workflow._durabletask import task
    from dapr.ext.workflow._durabletask.internal import helpers
    from dapr.ext.workflow._durabletask.worker import (
        _OrchestrationExecutor,
        _Registry,
        _RuntimeOrchestrationContext,
    )

    smoke = runpy.run_path(str(Path(__file__).parents[5] / "tests/helm/smoke/public_workflow.py"))
    document = WorkflowDocument.model_validate(smoke["waiting_workflow_document"]())
    now = datetime(2026, 11, 21, 12, 0, tzinfo=UTC)
    identity = ExecutionIdentity("workspace-default", "synthetic-principal", "synthetic-binding")
    graph = compile(
        document,
        RunMeta("workspace-default", "synthetic-version", "private-cancellation-proof", "1", now),
        Mock(spec=ActivityTypeRegistry),
    )
    run_input = RunInput(
        workspace_id="workspace-default",
        workflow_version_id="synthetic-version",
        compiled_graph_json=to_json(graph),
        inputs={},
        delivery_protocol=3,
        execution_identity=identity,
    )
    orchestrator = make_run_orchestrator(NoopStepHandler(), resume_handler=_resume_handler())
    replay_observations: list[bool] = []

    def entry(inner: task.OrchestrationContext, payload: Any) -> Any:
        """Wrap real naive history time; the aware variant changes only the wrapper's awareness."""
        context = DaprWorkflowContext(inner)
        assert context.current_utc_datetime.tzinfo is None
        if already_aware:
            assert isinstance(inner, _RuntimeOrchestrationContext)
            inner.current_utc_datetime = inner.current_utc_datetime.replace(tzinfo=UTC)
        replay_observations.append(context.is_replaying)
        return orchestrator(context, payload)

    insecure = Mock(side_effect=AssertionError("SDK regression forbids RPC"))
    secure = Mock(side_effect=AssertionError("SDK regression forbids RPC"))
    monkeypatch.setattr("grpc.insecure_channel", insecure)
    monkeypatch.setattr("grpc.secure_channel", secure)
    registry = _Registry()  # type: ignore[no-untyped-call]
    registry.add_named_orchestrator(WORKFLOW_NAME, entry)
    executor = _OrchestrationExecutor(registry, logging.getLogger(__name__))
    run_id = "00000000-0000-5000-8000-000000000000"
    events = [
        helpers.new_workflow_started_event(now),
        helpers.new_execution_started_event(WORKFLOW_NAME, run_id, json.dumps(run_input.to_dict())),
    ]
    result = executor.execute(run_id, [], events)
    assert not any(action.HasField("completeWorkflow") for action in result.actions)
    scheduled = [action for action in result.actions if action.HasField("scheduleTask")]
    assert len(scheduled) == 1
    first = scheduled[0]
    body = json.loads(first.scheduleTask.input.value)
    assert body["operation"] == "persist"
    mirror = body["mirror"]
    registered = datetime.fromisoformat(mirror["registeredAt"])
    expires = datetime.fromisoformat(mirror["expiresAt"])
    assert registered == now and registered.tzinfo is not None
    assert (expires - registered).total_seconds() == 300
    assert mirror["executionIdentity"] == identity.to_dict()
    assert (mirror["runId"], mirror["stepId"], mirror["eventKey"]) == (
        run_id,
        "await-cancellation",
        "never-sent",
    )
    replay = executor.execute(
        run_id,
        [
            *events,
            helpers.new_task_scheduled_event(
                first.id, first.scheduleTask.name, first.scheduleTask.input.value
            ),
        ],
        [
            helpers.new_workflow_started_event(now),
            helpers.new_task_completed_event(first.id, json.dumps(mirror)),
        ],
    )
    assert not any(action.HasField("completeWorkflow") for action in replay.actions)
    registration = [action for action in replay.actions if action.HasField("scheduleTask")]
    assert len(registration) == 1
    request = json.loads(registration[0].scheduleTask.input.value)
    assert request["operation"] == "register"
    assert request["request"]["executionIdentity"] == identity.to_dict()
    assert request["request"]["ttl"] == "PT5M"
    assert replay_observations == [False, True]
    insecure.assert_not_called()
    secure.assert_not_called()


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


def test_wait_for_reason_sanitizes_control_characters(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    # The event key is user-derived; control characters must not leak
    # into the (log-able) waiting reason — they are replaced with ``?``.
    run_input = _run_input(_graph(_wait_for_node()), inputs={"key": "order\napproved\t!"})

    output = _run(runtime, client, run_input, resume_handler=_resume_handler())

    assert output.status == "waiting"
    assert output.waiting_reason == "waitFor:order?approved?!"


def test_wait_for_reason_caps_oversized_event_key(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    # An oversized event key is truncated so a hostile / huge key cannot
    # balloon telemetry when the waiting reason is emitted.
    key = "k" * 500
    run_input = _run_input(_graph(_wait_for_node()), inputs={"key": key})

    output = _run(runtime, client, run_input, resume_handler=_resume_handler())

    assert output.status == "waiting"
    assert output.waiting_reason == "waitFor:" + "k" * 200 + "…"


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

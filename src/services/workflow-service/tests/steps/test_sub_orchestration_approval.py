"""Tests for the Sub-Orchestration Manager ``approval:`` gate (WF-IMPL-092)."""

from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from custos_cel import DaprWorkflowClock, FixedClock

from custos_workflow.document import ApprovalStep, LetStep
from custos_workflow.graph import (
    ExecutionGraph,
    ExecutionNode,
    GraphMetadata,
    PrimitiveHandler,
    StepKind,
)
from custos_workflow.runs import RunId, StepExecutionContext
from custos_workflow.runtime import (
    FakeWorkflowClient,
    FakeWorkflowRuntime,
    RaiseRunEventRequest,
    RunStatus,
    ScheduleWorkflowRequest,
)
from custos_workflow.steps.errors import (
    ApprovalTimeoutError,
    SubOrchestrationSpawnError,
)
from custos_workflow.steps.sub_orchestration import SubOrchestrationManager

_NOW = datetime(2026, 6, 1, 9, 0, 0, tzinfo=UTC)
_STEP_ID = "gate"
_GATE_ID = f"parent-1/{_STEP_ID}/approval"


# ---------------------------------------------------------------------------
# Graph builders
# ---------------------------------------------------------------------------


def _approval_graph(
    *,
    timeout: str | None = "PT1H",
    approvers: list[str] | None = None,
    step_id: str = _STEP_ID,
) -> ExecutionGraph:
    approval: dict[str, Any] = {"approvers": approvers or ["alice"]}
    if timeout is not None:
        approval["timeout"] = timeout
    node = ExecutionNode(
        step_id=step_id,
        kind=StepKind.APPROVAL,
        primitive_handler=PrimitiveHandler.SUB_ORCHESTRATION,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=ApprovalStep.model_validate({"id": step_id, "approval": approval}),
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
# Stub workflow context (for unit-driving ``run_approval`` directly)
# ---------------------------------------------------------------------------


class _Token:
    """Opaque task token the stub hands back from event / timer calls."""

    def __init__(self, kind: str, result: Any = None) -> None:
        self.kind = kind
        self._result = result

    def get_result(self) -> Any:
        return self._result


class _StubContext:
    """Minimal :class:`WorkflowContext` that records the event/timer race."""

    instance_id = "parent-1"

    def __init__(self, *, event_result: Any = None) -> None:
        self.event_name: str | None = None
        self.timer_delta: Any = None
        self.when_any_tasks: list[Any] | None = None
        self.event = _Token("event", event_result)
        self.timer = _Token("timer")

    def wait_for_external_event(self, name: str) -> _Token:
        self.event_name = name
        return self.event

    def create_timer(self, fire_at: Any) -> _Token:
        self.timer_delta = fire_at
        return self.timer

    def when_any(self, tasks: Any) -> tuple[str, list[Any]]:
        self.when_any_tasks = list(tasks)
        return ("when_any", self.when_any_tasks)


def _ctx(
    *,
    workflow_context: Any,
    run_id: str = "parent-1",
    inputs: dict[str, Any] | None = None,
    outputs: dict[str, Any] | None = None,
) -> StepExecutionContext:
    return StepExecutionContext(
        run_id=cast(RunId, run_id),
        workspace_id="ws-1",
        workflow_version_id="wf-v1",
        inputs=inputs or {},
        workflow_context=workflow_context,
        outputs=outputs or {},
        clock=FixedClock(_NOW),
    )


def _drive_to_when_any(gen: Generator[Any, Any, dict[str, Any]]) -> Any:
    """Advance ``run_approval`` to its single ``when_any`` yield."""
    return next(gen)


# ---------------------------------------------------------------------------
# Signal-before-timeout → approval decision binds to outputs
# ---------------------------------------------------------------------------


def test_signal_wins_binds_decision_payload() -> None:
    ctx = _StubContext(event_result={"by": "alice", "decision": "approved"})
    graph = _approval_graph(timeout="PT2H")
    step_ctx = _ctx(workflow_context=ctx)

    gen = SubOrchestrationManager().run_approval(step_ctx, graph, _STEP_ID)
    yielded = _drive_to_when_any(gen)

    # The gate races the external event against the durable timer.
    assert yielded[0] == "when_any"
    assert ctx.when_any_tasks == [ctx.event, ctx.timer]
    assert ctx.event_name == _GATE_ID
    assert ctx.timer_delta == timedelta(hours=2)

    with pytest.raises(StopIteration) as si:
        gen.send(ctx.event)
    assert si.value.value == {"by": "alice", "decision": "approved"}


def test_reject_decision_also_binds_to_outputs() -> None:
    ctx = _StubContext(event_result={"by": "bob", "decision": "rejected"})
    graph = _approval_graph()
    step_ctx = _ctx(workflow_context=ctx)

    gen = SubOrchestrationManager().run_approval(step_ctx, graph, _STEP_ID)
    _drive_to_when_any(gen)

    with pytest.raises(StopIteration) as si:
        gen.send(ctx.event)
    # A reject is a decision, not a failure — its payload binds to outputs.
    assert si.value.value == {"by": "bob", "decision": "rejected"}


def test_signal_without_payload_binds_empty_outputs() -> None:
    ctx = _StubContext(event_result=None)
    graph = _approval_graph()
    step_ctx = _ctx(workflow_context=ctx)

    gen = SubOrchestrationManager().run_approval(step_ctx, graph, _STEP_ID)
    _drive_to_when_any(gen)

    with pytest.raises(StopIteration) as si:
        gen.send(ctx.event)
    assert si.value.value == {}


def test_default_timeout_is_pt24h() -> None:
    ctx = _StubContext()
    graph = _approval_graph(timeout=None)  # ApprovalSpec default PT24H applies
    step_ctx = _ctx(workflow_context=ctx)

    gen = SubOrchestrationManager().run_approval(step_ctx, graph, _STEP_ID)
    _drive_to_when_any(gen)

    assert ctx.timer_delta == timedelta(hours=24)


def test_configured_default_timeout_overrides_model_default() -> None:
    ctx = _StubContext()
    # The document leaves ``approval.timeout`` at the model default
    # (PT24H), so the platform-configured default applies instead.
    graph = _approval_graph(timeout=None)
    step_ctx = _ctx(workflow_context=ctx)

    manager = SubOrchestrationManager(approval_default_timeout=timedelta(hours=48))
    gen = manager.run_approval(step_ctx, graph, _STEP_ID)
    _drive_to_when_any(gen)

    assert ctx.timer_delta == timedelta(hours=48)


def test_configured_default_timeout_surfaces_iso_label_on_timeout() -> None:
    ctx = _StubContext()
    graph = _approval_graph(timeout=None)
    step_ctx = _ctx(workflow_context=ctx)

    manager = SubOrchestrationManager(approval_default_timeout=timedelta(hours=48))
    gen = manager.run_approval(step_ctx, graph, _STEP_ID)
    _drive_to_when_any(gen)

    with pytest.raises(ApprovalTimeoutError) as exc:
        gen.send(ctx.timer)
    # The audit envelope reflects the configured default rendered back
    # to a normalised ISO-8601 duration (48h → ``P2D``), not the
    # model-default literal.
    assert exc.value.timeout == "P2D"


@pytest.mark.parametrize(
    ("delta", "expected"),
    [
        (timedelta(hours=24), "P1D"),
        (timedelta(hours=23), "PT23H"),
        (timedelta(days=2), "P2D"),
        (timedelta(days=1, hours=2, minutes=3, seconds=4), "P1DT2H3M4S"),
        (timedelta(minutes=90), "PT1H30M"),
        (timedelta(seconds=45), "PT45S"),
        (timedelta(0), "PT0S"),
    ],
)
def test_format_iso8601_duration(delta: timedelta, expected: str) -> None:
    from custos_workflow.steps.sub_orchestration.manager import _format_iso8601_duration

    assert _format_iso8601_duration(delta) == expected


def test_explicit_document_timeout_wins_over_configured_default() -> None:
    ctx = _StubContext()
    graph = _approval_graph(timeout="PT2H")  # explicit override
    step_ctx = _ctx(workflow_context=ctx)

    manager = SubOrchestrationManager(approval_default_timeout=timedelta(hours=48))
    gen = manager.run_approval(step_ctx, graph, _STEP_ID)
    _drive_to_when_any(gen)

    # An explicit per-document timeout is honoured verbatim.
    assert ctx.timer_delta == timedelta(hours=2)


def test_model_default_iso_constant_matches_document_model() -> None:
    from custos_workflow.document.models import _DEFAULT_APPROVAL_TIMEOUT
    from custos_workflow.steps.sub_orchestration.manager import (
        _MODEL_DEFAULT_APPROVAL_TIMEOUT_ISO,
    )

    # The manager's substitution-trigger constant must track the
    # document model default, or the configured-default override would
    # silently stop firing.
    assert _MODEL_DEFAULT_APPROVAL_TIMEOUT_ISO == _DEFAULT_APPROVAL_TIMEOUT


# ---------------------------------------------------------------------------
# Timeout-before-signal → step.approval_timeout
# ---------------------------------------------------------------------------


def test_timeout_wins_raises_approval_timeout() -> None:
    ctx = _StubContext()
    graph = _approval_graph(timeout="PT3H")
    step_ctx = _ctx(workflow_context=ctx)

    gen = SubOrchestrationManager().run_approval(step_ctx, graph, _STEP_ID)
    _drive_to_when_any(gen)

    with pytest.raises(ApprovalTimeoutError) as exc:
        gen.send(ctx.timer)
    assert exc.value.kind == "step.approval_timeout"
    assert exc.value.step_id == _STEP_ID
    assert exc.value.child_instance_id == _GATE_ID
    assert exc.value.timeout == "PT3H"


# ---------------------------------------------------------------------------
# Spawn / shape guards
# ---------------------------------------------------------------------------


def test_malformed_run_id_raises_spawn_error() -> None:
    ctx = _StubContext()
    graph = _approval_graph()
    step_ctx = _ctx(workflow_context=ctx, run_id="parent/oops")

    gen = SubOrchestrationManager().run_approval(step_ctx, graph, _STEP_ID)
    with pytest.raises(SubOrchestrationSpawnError) as exc:
        next(gen)
    assert exc.value.kind == "step.sub_orchestration_spawn_error"
    assert exc.value.step_id == _STEP_ID
    assert exc.value.cause is not None
    assert ctx.event_name is None


def test_non_approval_step_raises_spawn_error() -> None:
    ctx = _StubContext()
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
    step_ctx = _ctx(workflow_context=ctx)

    gen = SubOrchestrationManager().run_approval(step_ctx, graph, _STEP_ID)
    with pytest.raises(SubOrchestrationSpawnError) as exc:
        next(gen)
    assert exc.value.kind == "step.sub_orchestration_spawn_error"
    assert ctx.event_name is None


def test_unknown_step_id_raises_key_error() -> None:
    ctx = _StubContext()
    graph = _approval_graph()
    step_ctx = _ctx(workflow_context=ctx)

    gen = SubOrchestrationManager().run_approval(step_ctx, graph, "nope")
    with pytest.raises(KeyError):
        next(gen)


# ---------------------------------------------------------------------------
# End-to-end under the FakeWorkflowRuntime
# ---------------------------------------------------------------------------


def _make_parent(graph: ExecutionGraph) -> Any:
    def run_parent(ctx: Any, raw: Any) -> Generator[Any, Any, dict[str, Any]]:
        # Suspend on a setup event so the test can buffer the approval
        # signal before the gate's ``when_any`` evaluates (mirrors the
        # runtime-primitives when_any tests).
        yield ctx.wait_for_external_event("go")
        step_ctx = StepExecutionContext(
            run_id=cast(RunId, ctx.instance_id),
            workspace_id="ws-1",
            workflow_version_id="wf-v1",
            inputs=raw,
            workflow_context=ctx,
            outputs={},
            clock=DaprWorkflowClock(ctx),
        )
        result = yield from SubOrchestrationManager().run_approval(step_ctx, graph, _STEP_ID)
        return result

    return run_parent


@pytest.fixture
def runtime() -> FakeWorkflowRuntime:
    return FakeWorkflowRuntime(now=_NOW)


@pytest.fixture
def client(runtime: FakeWorkflowRuntime) -> FakeWorkflowClient:
    return runtime.client()


async def test_end_to_end_signal_before_timeout(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    graph = _approval_graph(timeout="PT24H")
    runtime.register_workflow(_make_parent(graph), name="parent")

    returned = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input={}, instance_id="parent-1")
    )
    # Instance is parked on "go"; buffer the approval, then release it.
    await client.raise_workflow_event(
        RaiseRunEventRequest(
            instance_id="parent-1",
            event_name=_GATE_ID,
            data={"by": "alice", "decision": "approved"},
        )
    )
    await client.raise_workflow_event(
        RaiseRunEventRequest(instance_id="parent-1", event_name="go", data=None)
    )

    state = runtime.instance(returned)
    assert state.status == RunStatus.COMPLETED
    assert state.output == {"by": "alice", "decision": "approved"}


async def test_end_to_end_timeout_before_signal(
    runtime: FakeWorkflowRuntime, client: FakeWorkflowClient
) -> None:
    graph = _approval_graph(timeout="PT24H")
    runtime.register_workflow(_make_parent(graph), name="parent")

    returned = await client.schedule_new_workflow(
        ScheduleWorkflowRequest(workflow="parent", input={}, instance_id="parent-1")
    )
    # No approval buffered — releasing "go" runs the gate, where the
    # always-ready durable timer wins the race and times out.
    await client.raise_workflow_event(
        RaiseRunEventRequest(instance_id="parent-1", event_name="go", data=None)
    )

    state = runtime.instance(returned)
    assert state.status == RunStatus.FAILED
    assert state.failure_type == "ApprovalTimeoutError"

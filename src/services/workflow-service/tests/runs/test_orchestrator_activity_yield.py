"""Integration tests for the WF-IMPL-074 activity-task yield protocol
at the Run Controller orchestrator + :class:`FakeWorkflowRuntime`
seam.

The unit tests in
:mod:`tests.steps.test_activity_step_yield_protocol` pin the
contract of :meth:`ActivityStepHandler.iter_calls` and the
in-process :func:`drive_activity_generator` driver. This module
pins the *integration* contract:

* When the Run Controller orchestrator is constructed with
  ``activity_handler=...``, every
  :class:`~custos_workflow.graph.model.StepKind.ACTIVITY`
  node is dispatched via ``yield from
  activity_handler.iter_calls(...)`` \u2014 each bind / schedule call
  surfaces as a distinct yielded
  :data:`~custos_workflow.runtime.dapr_activities.ActivityCallToken`.
* :class:`FakeWorkflowRuntime` configured with
  ``activity_dispatcher=FakeDaprActivityDispatcher(...)`` resolves
  each yielded token, records an ``activity_call_resolved``
  history event, and threads the response back into the generator
  so the workflow instance terminates with the same
  :class:`RunOutput` the inline path would have produced.
* Backwards-compatibility: omitting ``activity_handler`` keeps the
  legacy :class:`StepCoordinator`-based path intact (existing
  tests already cover that surface; we add one regression check
  here to lock the default).
* Failure mode: when the orchestrator yields an activity-call
  token but the runtime is missing an ``activity_dispatcher``,
  the workflow instance fails fast with a structured error rather
  than hanging or silently coercing the token.
"""

from __future__ import annotations

import asyncio
import textwrap
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, cast

from custos_workflow.bindings import InMemoryActivityTypeRegistry
from custos_workflow.clients.activity_runtime import (
    ActivityResultEnvelope,
    FakeActivityRuntimeClient,
)
from custos_workflow.clients.connector import (
    BindForStepResponse,
    ConnectorContext,
    FakeConnectorClient,
)
from custos_workflow.compiler import RunMeta
from custos_workflow.compiler import compile as compile_workflow
from custos_workflow.document import WorkflowDocument
from custos_workflow.graph import to_json
from custos_workflow.graph.model import ExecutionGraph
from custos_workflow.runs import (
    RunInput,
    RunOutput,
    make_run_orchestrator,
)
from custos_workflow.runs.orchestrator import WORKFLOW_NAME
from custos_workflow.runtime import (
    FakeWorkflowClient,
    FakeWorkflowRuntime,
    RunStatus,
)
from custos_workflow.runtime._common import ScheduleWorkflowRequest
from custos_workflow.runtime.dapr_activities import (
    BindForStepCallToken,
    FakeDaprActivityDispatcher,
    ScheduleActivityCallToken,
)
from custos_workflow.runtime.fake import FakeWorkflowFn
from custos_workflow.steps import StepCoordinator
from custos_workflow.steps.activity_step import ActivityStepHandler

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


_FIXED_NOW = datetime(2026, 1, 1, tzinfo=UTC)


_SINGLE_ACTIVITY_DOC = """\
    apiVersion: custos.dev/v1
    kind: Workflow
    metadata: {name: pipeline, workspace: ws}
    spec:
      inputs:
        image: {type: string, default: 'alpine:3.19'}
      steps:
        - id: scan
          activity: security/scan@1
          connector: primary
          with:
            image: ${{ inputs.image }}
"""


def _registry() -> InMemoryActivityTypeRegistry:
    return InMemoryActivityTypeRegistry(
        {
            "security/scan@1": {
                "type": "object",
                "properties": {
                    "critical": {"type": "integer"},
                },
            },
        }
    )


def _run_meta() -> RunMeta:
    return RunMeta(
        workspace_id="ws",
        workflow_version_id="wfv-1",
        workflow_name="pipeline",
        workflow_version_label="v1",
        started_at_default=_FIXED_NOW,
    )


def _compile(doc_yaml: str) -> ExecutionGraph:
    import yaml

    payload = yaml.safe_load(textwrap.dedent(doc_yaml))
    doc = WorkflowDocument.model_validate(payload)
    return compile_workflow(doc, _run_meta(), _registry())


def _run_input(graph: ExecutionGraph, *, inputs: Mapping[str, Any] | None = None) -> RunInput:
    return RunInput(
        workspace_id="ws",
        workflow_version_id="wfv-1",
        compiled_graph_json=to_json(graph),
        inputs=inputs if inputs is not None else {"image": "alpine"},
        idempotency_key="idem-1",
    )


def _register(runtime: FakeWorkflowRuntime, orchestrator: Any) -> None:
    runtime.register_workflow(cast(FakeWorkflowFn, orchestrator), name=WORKFLOW_NAME)


def _bind_response(*slots: str) -> BindForStepResponse:
    expires = _FIXED_NOW.replace(hour=1)
    return BindForStepResponse(
        contexts={
            slot: ConnectorContext(
                slot_name=slot,
                handle=f"handle-{slot}",
                expires_at=expires,
                connector_kind="oci-registry",
            )
            for slot in slots
        },
    )


def _success(outputs: dict[str, Any], *, attempt: int = 1) -> ActivityResultEnvelope:
    return ActivityResultEnvelope(
        class_="success",
        outputs=outputs,
        error=None,
        attempt=attempt,
    )


# ---------------------------------------------------------------------------
# Orchestrator factory signature
# ---------------------------------------------------------------------------


class TestFactorySignature:
    def test_factory_accepts_activity_handler_kw(self) -> None:
        # ``activity_handler`` must be keyword-only and default to
        # ``None`` so existing call sites that pass only
        # ``handler`` (and optionally ``on_replay`` /
        # ``wait_handler``) compile unchanged \u2014 the FastAPI
        # lifespan wiring (WF-IMPL-079) opts in by passing the
        # handler explicitly.
        import inspect

        sig = inspect.signature(make_run_orchestrator)
        param = sig.parameters["activity_handler"]
        assert param.kind is param.KEYWORD_ONLY
        assert param.default is None


# ---------------------------------------------------------------------------
# Yield protocol \u2014 wired path
# ---------------------------------------------------------------------------


class TestActivityYieldProtocolWired:
    """``activity_handler`` is wired into the orchestrator AND a
    matching ``activity_dispatcher`` is wired into the runtime.

    Together this is the integration path the production Dapr
    worker (WF-IMPL-079) will install."""

    def test_workflow_completes_via_yield_protocol(self) -> None:
        graph = _compile(_SINGLE_ACTIVITY_DOC)
        activity_client = FakeActivityRuntimeClient(
            results=[_success({"critical": 0})],
        )
        connector_client = FakeConnectorClient(responses=[_bind_response("default")])
        activity_handler = ActivityStepHandler(
            activity_client=activity_client,
            connector_client=connector_client,
        )
        dispatcher = FakeDaprActivityDispatcher(
            activity_client=activity_client,
            connector_client=connector_client,
        )
        runtime = FakeWorkflowRuntime(now=_FIXED_NOW, activity_dispatcher=dispatcher)
        client = FakeWorkflowClient(runtime=runtime)

        # NOTE: the coordinator is still installed as the generic
        # handler so non-ACTIVITY kinds (which this doc doesn't
        # exercise) keep their dispatch arm \u2014 only ACTIVITY nodes
        # take the yield-protocol fast path.
        coordinator = StepCoordinator(activity_handler)
        orchestrator = make_run_orchestrator(
            coordinator,
            activity_handler=activity_handler,
        )
        _register(runtime, orchestrator)

        instance_id = asyncio.run(
            client.schedule_new_workflow(
                ScheduleWorkflowRequest(
                    workflow=WORKFLOW_NAME,
                    input=_run_input(graph, inputs={"image": "alpine:3.19"}),
                )
            )
        )
        state = runtime.instance(instance_id)
        assert state.status is RunStatus.COMPLETED
        assert isinstance(state.output, RunOutput)
        assert state.output.status == "succeeded"
        # The yield protocol does NOT bypass the handler's normal
        # output threading \u2014 the success envelope's outputs land
        # on the run's per-step bag verbatim.
        assert state.output.outputs == {"scan": {"critical": 0}}

    def test_runtime_history_records_resolved_tokens_in_order(self) -> None:
        graph = _compile(_SINGLE_ACTIVITY_DOC)
        activity_client = FakeActivityRuntimeClient(
            results=[_success({"critical": 0})],
        )
        connector_client = FakeConnectorClient(responses=[_bind_response("default")])
        activity_handler = ActivityStepHandler(
            activity_client=activity_client,
            connector_client=connector_client,
        )
        dispatcher = FakeDaprActivityDispatcher(
            activity_client=activity_client,
            connector_client=connector_client,
        )
        runtime = FakeWorkflowRuntime(now=_FIXED_NOW, activity_dispatcher=dispatcher)
        client = FakeWorkflowClient(runtime=runtime)

        coordinator = StepCoordinator(activity_handler)
        orchestrator = make_run_orchestrator(
            coordinator,
            activity_handler=activity_handler,
        )
        _register(runtime, orchestrator)

        instance_id = asyncio.run(
            client.schedule_new_workflow(
                ScheduleWorkflowRequest(
                    workflow=WORKFLOW_NAME,
                    input=_run_input(graph),
                )
            )
        )
        state = runtime.instance(instance_id)

        # Pull every ``activity_call_resolved`` event out of the
        # history in order. We expect bind \u2192 schedule per
        # attempt \u2014 with a successful first attempt that is one
        # of each.
        resolved = [
            ev.detail["token"] for ev in state.history if ev.kind == "activity_call_resolved"
        ]
        assert resolved == ["bind_for_step", "schedule_activity"]
        # And the dispatcher recorded one bind + one schedule on
        # the underlying fake clients \u2014 the yield protocol must
        # not lose calls.
        assert len(connector_client.calls) == 1
        assert len(activity_client.calls) == 1


# ---------------------------------------------------------------------------
# Backwards-compatibility \u2014 default (unwired) path
# ---------------------------------------------------------------------------


class TestActivityYieldProtocolUnwired:
    """``activity_handler`` is NOT passed to the orchestrator. The
    coordinator's :meth:`ActivityStepHandler.execute` adapter must
    drive the bind / schedule calls inline \u2014 no tokens are
    yielded, so no ``activity_dispatcher`` is needed and no
    ``activity_call_resolved`` history events appear."""

    def test_default_path_uses_inline_handler_execute(self) -> None:
        graph = _compile(_SINGLE_ACTIVITY_DOC)
        activity_client = FakeActivityRuntimeClient(
            results=[_success({"critical": 0})],
        )
        connector_client = FakeConnectorClient(responses=[_bind_response("default")])
        activity_handler = ActivityStepHandler(
            activity_client=activity_client,
            connector_client=connector_client,
        )
        runtime = FakeWorkflowRuntime(now=_FIXED_NOW)  # no dispatcher
        client = FakeWorkflowClient(runtime=runtime)

        coordinator = StepCoordinator(activity_handler)
        # No activity_handler kwarg \u2014 falls back to the inline
        # handler.execute path.
        orchestrator = make_run_orchestrator(coordinator)
        _register(runtime, orchestrator)

        instance_id = asyncio.run(
            client.schedule_new_workflow(
                ScheduleWorkflowRequest(
                    workflow=WORKFLOW_NAME,
                    input=_run_input(graph),
                )
            )
        )
        state = runtime.instance(instance_id)
        assert state.status is RunStatus.COMPLETED
        assert isinstance(state.output, RunOutput)
        assert state.output.status == "succeeded"
        # No yielded tokens \u2192 no history events for them.
        assert not any(
            ev.kind in ("activity_call_resolved", "activity_call_failed") for ev in state.history
        )


# ---------------------------------------------------------------------------
# Failure mode \u2014 yield protocol used but no dispatcher wired
# ---------------------------------------------------------------------------


class TestMissingDispatcher:
    def test_yielded_token_without_dispatcher_fails_instance(self) -> None:
        graph = _compile(_SINGLE_ACTIVITY_DOC)
        activity_client = FakeActivityRuntimeClient(
            results=[_success({"critical": 0})],
        )
        connector_client = FakeConnectorClient(responses=[_bind_response("default")])
        activity_handler = ActivityStepHandler(
            activity_client=activity_client,
            connector_client=connector_client,
        )
        runtime = FakeWorkflowRuntime(now=_FIXED_NOW)  # deliberately no dispatcher
        client = FakeWorkflowClient(runtime=runtime)

        coordinator = StepCoordinator(activity_handler)
        orchestrator = make_run_orchestrator(
            coordinator,
            activity_handler=activity_handler,  # yields tokens \u2026
        )
        _register(runtime, orchestrator)

        instance_id = asyncio.run(
            client.schedule_new_workflow(
                ScheduleWorkflowRequest(
                    workflow=WORKFLOW_NAME,
                    input=_run_input(graph),
                )
            )
        )
        state = runtime.instance(instance_id)
        # The token-bearing instance fails fast \u2014 silently
        # coercing the token would mask a real wiring bug in
        # production.
        assert state.status is RunStatus.FAILED
        assert state.failure_type == "MissingActivityDispatcherError"
        assert state.failure_message is not None
        assert "FakeDaprActivityDispatcher" in state.failure_message


# ---------------------------------------------------------------------------
# Token equivalence \u2014 the orchestrator yields the SAME tokens
# iter_calls would have yielded directly
# ---------------------------------------------------------------------------


class TestTokenYieldEquivalence:
    """A recording dispatcher that captures every token resolved
    proves the orchestrator's ``yield from`` re-yields the handler's
    tokens verbatim, in order, without any wrapping or
    re-shaping."""

    def test_orchestrator_yields_handler_tokens_verbatim(self) -> None:
        graph = _compile(_SINGLE_ACTIVITY_DOC)
        activity_client = FakeActivityRuntimeClient(
            results=[_success({"critical": 0})],
        )
        connector_client = FakeConnectorClient(responses=[_bind_response("default")])
        activity_handler = ActivityStepHandler(
            activity_client=activity_client,
            connector_client=connector_client,
        )

        captured: list[type] = []

        class _RecordingDispatcher(FakeDaprActivityDispatcher):
            def resolve(self, token: Any) -> Any:
                captured.append(type(token))
                return super().resolve(token)

        dispatcher = _RecordingDispatcher(
            activity_client=activity_client,
            connector_client=connector_client,
        )
        runtime = FakeWorkflowRuntime(now=_FIXED_NOW, activity_dispatcher=dispatcher)
        client = FakeWorkflowClient(runtime=runtime)

        coordinator = StepCoordinator(activity_handler)
        orchestrator = make_run_orchestrator(
            coordinator,
            activity_handler=activity_handler,
        )
        _register(runtime, orchestrator)

        asyncio.run(
            client.schedule_new_workflow(
                ScheduleWorkflowRequest(
                    workflow=WORKFLOW_NAME,
                    input=_run_input(graph),
                )
            )
        )

        assert captured == [BindForStepCallToken, ScheduleActivityCallToken]

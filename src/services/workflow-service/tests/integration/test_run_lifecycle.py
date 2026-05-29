"""WF-IMPL-045 — end-to-end Run Controller lifecycle integration tests.

Each test wires the **real** :class:`RunController`, the
**real** :func:`make_run_orchestrator`, and a
:class:`FakeWorkflowRuntime` through the shared
:mod:`tests.integration._harness` and exercises one of the five
lifecycle scenarios called out in the WF-IMPL-045 acceptance
criteria:

* ``start_run → step dispatch (Noop) → succeeded``
* ``start_run → cancel_run → cancelled``
* ``start_run → pause → resume → succeeded``
* ``start_run → orchestrator failure → failed``
* ``start_run → wait step (5s simulated) → succeeded``

The tests assert on three independent surfaces:

* **Store row** — ``InProcessRunStore.get_run`` reports the
  documented terminal :class:`RunStatus` after each lifecycle
  call returns.
* **Runtime instance** — ``runtime.instance(run_id).output`` is
  the :class:`RunOutput` the orchestrator produced (the fake
  drives the orchestrator inline, so the output is observable
  the moment :meth:`schedule_new_workflow` returns).
* **Lifecycle event tape** — every successful publish lands on
  the in-memory publisher in the documented order.

The cancel / pause / resume scenarios deliberately let the
orchestrator run to completion under the synchronous fake before
the lifecycle call: the Run Controller drives the **store**
transitions regardless of what the runtime is doing (its
``terminate_workflow`` / ``pause_workflow`` / ``resume_workflow``
calls are signalling side-effects), and the store-side state
machine is the AC surface for WF-IMPL-045.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from custos_workflow.graph.model import ExecutionGraph
from custos_workflow.runs import (
    LIFECYCLE_KIND_WORKFLOW_CANCELLED,
    LIFECYCLE_KIND_WORKFLOW_PAUSED,
    LIFECYCLE_KIND_WORKFLOW_RESUMED,
    LIFECYCLE_KIND_WORKFLOW_STARTED,
    RunOutput,
    RunStatus,
    StepExecutionContext,
    StepFailed,
    StepResult,
    StepSucceeded,
    derive_run_id,
)
from tests.integration._harness import (
    IDEMPOTENCY_KEY,
    WORKFLOW_VERSION_ID,
    WORKSPACE,
    compile_doc,
    make_harness,
)

# ---------------------------------------------------------------------------
# Documents
# ---------------------------------------------------------------------------


_LINEAR_DOC = """\
    apiVersion: custos.dev/v1
    kind: Workflow
    metadata: {name: pipeline, workspace: ws}
    spec:
      inputs:
        flag: {type: boolean, default: true}
      steps:
        - id: a
          let: {x: '${{ true }}'}
        - id: b
          needs: [a]
          let: {y: '${{ true }}'}
        - id: c
          needs: [b]
          let: {z: '${{ true }}'}
"""


_SINGLE_STEP_DOC = """\
    apiVersion: custos.dev/v1
    kind: Workflow
    metadata: {name: pipeline, workspace: ws}
    spec:
      inputs:
        flag: {type: boolean, default: true}
      steps:
        - id: only
          let: {x: '${{ true }}'}
"""


_WAIT_DOC = """\
    apiVersion: custos.dev/v1
    kind: Workflow
    metadata: {name: pipeline, workspace: ws}
    spec:
      inputs: {}
      steps:
        - id: hold
          wait: 'PT5S'
        - id: after
          needs: [hold]
          let: {ok: '${{ true }}'}
"""


# ---------------------------------------------------------------------------
# Handler doubles
# ---------------------------------------------------------------------------


@dataclass
class _FailingHandler:
    """Returns :class:`StepFailed` on the named step."""

    failing_step: str
    failed_message: str = "boom"

    def execute(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        step_id: str,
    ) -> StepResult:
        del ctx, graph
        if step_id == self.failing_step:
            return StepFailed(
                envelope={
                    "kind": "step.error",
                    "step_id": step_id,
                    "message": self.failed_message,
                },
            )
        return StepSucceeded(outputs={})


# ---------------------------------------------------------------------------
# start_run → step dispatch (Noop) → succeeded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestStartRunSucceeds:
    async def test_orchestrator_completes_under_noop_handler(self) -> None:
        h = make_harness(doc_yaml=_LINEAR_DOC)
        ref = await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"flag": True},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        assert ref.status is RunStatus.RUNNING

        # The fake drives the orchestrator inline during
        # ``schedule_new_workflow``, so the moment ``start_run``
        # returns the runtime instance is already terminal with
        # a ``succeeded`` ``RunOutput`` carrying the empty bag
        # the linear NoopStepHandler produced.
        state = h.runtime.instance(str(ref.run_id))
        assert isinstance(state.output, RunOutput)
        assert state.output.status == RunStatus.SUCCEEDED.value
        assert state.output.failed_step is None
        assert state.output.outputs == {"a": {}, "b": {}, "c": {}}

    async def test_lifecycle_event_started_published(self) -> None:
        h = make_harness(doc_yaml=_SINGLE_STEP_DOC)
        await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        events = h.publisher.events
        assert [e.kind for e in events] == [LIFECYCLE_KIND_WORKFLOW_STARTED]
        assert events[0].workspace_id == WORKSPACE
        assert events[0].workflow_version_id == WORKFLOW_VERSION_ID

    async def test_store_row_is_running_after_start(self) -> None:
        h = make_harness(doc_yaml=_SINGLE_STEP_DOC)
        ref = await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        record = await h.store.get_run(WORKSPACE, ref.run_id)
        assert record is not None
        # The Run Controller is responsible for queued -> running;
        # the queued -> succeeded reconciliation belongs to a future
        # task (the orchestrator's output lives on the runtime
        # instance, not on the store row).
        assert record.status is RunStatus.RUNNING

    async def test_workflow_client_received_exactly_one_schedule(self) -> None:
        h = make_harness(doc_yaml=_SINGLE_STEP_DOC)
        await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        assert len(h.workflow_client.schedule_requests) == 1
        request = h.workflow_client.schedule_requests[0]
        # The orchestrator was scheduled under the wire-stable name
        # and the instance id is the deterministic ``derive_run_id``
        # the controller minted from the idempotency key.
        assert request.instance_id == str(derive_run_id(WORKSPACE, IDEMPOTENCY_KEY))


# ---------------------------------------------------------------------------
# start_run → cancel_run → cancelled
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestStartThenCancel:
    async def test_cancel_drives_row_to_cancelled(self) -> None:
        h = make_harness(doc_yaml=_LINEAR_DOC)
        ref = await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"flag": True},
            idempotency_key=IDEMPOTENCY_KEY,
        )

        cancelled_ref = await h.controller.cancel_run(
            workspace_id=WORKSPACE,
            run_id=ref.run_id,
            reason="operator stop",
        )
        assert cancelled_ref.status is RunStatus.CANCELLED

        record = await h.store.get_run(WORKSPACE, ref.run_id)
        assert record is not None
        assert record.status is RunStatus.CANCELLED
        assert record.reason == "operator stop"

    async def test_cancel_emits_workflow_cancelled_after_started(self) -> None:
        h = make_harness(doc_yaml=_LINEAR_DOC)
        ref = await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"flag": True},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        await h.controller.cancel_run(
            workspace_id=WORKSPACE,
            run_id=ref.run_id,
            reason="operator stop",
        )
        events = h.publisher.events
        assert [e.kind for e in events] == [
            LIFECYCLE_KIND_WORKFLOW_STARTED,
            LIFECYCLE_KIND_WORKFLOW_CANCELLED,
        ]
        # ``reason`` round-trips on the cancelled event's ``extra``.
        assert dict(events[1].extra) == {"reason": "operator stop"}

    async def test_cancel_calls_terminate_workflow_once(self) -> None:
        h = make_harness(doc_yaml=_LINEAR_DOC)
        ref = await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"flag": True},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        await h.controller.cancel_run(
            workspace_id=WORKSPACE,
            run_id=ref.run_id,
            reason=None,
        )
        assert [r.instance_id for r in h.workflow_client.terminate_requests] == [
            str(ref.run_id),
        ]


# ---------------------------------------------------------------------------
# start_run → pause → resume → succeeded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestPauseResume:
    async def test_pause_then_resume_drives_row_through_paused_to_running(self) -> None:
        h = make_harness(doc_yaml=_LINEAR_DOC)
        ref = await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"flag": True},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        assert ref.status is RunStatus.RUNNING

        paused = await h.controller.pause_run(workspace_id=WORKSPACE, run_id=ref.run_id)
        assert paused.status is RunStatus.PAUSED

        resumed = await h.controller.resume_run(workspace_id=WORKSPACE, run_id=ref.run_id)
        assert resumed.status is RunStatus.RUNNING

    async def test_pause_then_resume_emits_paused_then_resumed(self) -> None:
        h = make_harness(doc_yaml=_LINEAR_DOC)
        ref = await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"flag": True},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        await h.controller.pause_run(workspace_id=WORKSPACE, run_id=ref.run_id)
        await h.controller.resume_run(workspace_id=WORKSPACE, run_id=ref.run_id)
        assert [e.kind for e in h.publisher.events] == [
            LIFECYCLE_KIND_WORKFLOW_STARTED,
            LIFECYCLE_KIND_WORKFLOW_PAUSED,
            LIFECYCLE_KIND_WORKFLOW_RESUMED,
        ]

    async def test_pause_and_resume_each_call_the_workflow_client_once(self) -> None:
        h = make_harness(doc_yaml=_LINEAR_DOC)
        ref = await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"flag": True},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        await h.controller.pause_run(workspace_id=WORKSPACE, run_id=ref.run_id)
        await h.controller.resume_run(workspace_id=WORKSPACE, run_id=ref.run_id)
        assert [r.instance_id for r in h.workflow_client.pause_requests] == [
            str(ref.run_id),
        ]
        assert [r.instance_id for r in h.workflow_client.resume_requests] == [
            str(ref.run_id),
        ]


# ---------------------------------------------------------------------------
# start_run → orchestrator failure → failed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestOrchestratorFailure:
    async def test_step_failed_terminates_run_output_as_failed(self) -> None:
        h = make_harness(
            doc_yaml=_LINEAR_DOC,
            handler=_FailingHandler(failing_step="b", failed_message="step b broke"),
        )
        ref = await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"flag": True},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        # ``start_run`` returned successfully (it doesn't depend on
        # the orchestrator's terminal status — the orchestrator runs
        # in a separate worker context). The failure is observable on
        # the runtime instance the fake drove inline.
        state = h.runtime.instance(str(ref.run_id))
        assert isinstance(state.output, RunOutput)
        assert state.output.status == RunStatus.FAILED.value
        assert state.output.failed_step == "b"
        assert state.output.failure_envelope is not None
        assert state.output.failure_envelope["message"] == "step b broke"

    async def test_step_failed_does_not_surface_workflow_failed_lifecycle_event(
        self,
    ) -> None:
        # WF-IMPL-041 publishes ``workflow.started`` on start; the
        # ``workflow.failed`` / ``workflow.succeeded`` lifecycle
        # events are reconciler-owned (a future task). The Run
        # Controller MUST NOT inflate the event tape from
        # ``start_run`` even when the orchestrator immediately
        # fails: ``workflow.started`` is still the only emitted
        # event because the row reached the ``running`` gate.
        h = make_harness(
            doc_yaml=_LINEAR_DOC,
            handler=_FailingHandler(failing_step="a"),
        )
        await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={"flag": True},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        assert [e.kind for e in h.publisher.events] == [
            LIFECYCLE_KIND_WORKFLOW_STARTED,
        ]


# ---------------------------------------------------------------------------
# start_run → wait step (5s simulated) → succeeded
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestWaitStep:
    async def test_wait_step_fires_simulated_timer_and_completes(self) -> None:
        # The fake auto-fires timers — the 5-second wait is
        # therefore *simulated* (no wall-clock sleep) and the
        # orchestrator completes with ``succeeded``.
        h = make_harness(doc_yaml=_WAIT_DOC)
        ref = await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        state = h.runtime.instance(str(ref.run_id))
        assert isinstance(state.output, RunOutput)
        assert state.output.status == RunStatus.SUCCEEDED.value
        # History bookends + exactly one ``timer_fired`` from the
        # wait step.
        kinds = [event.kind for event in state.history]
        assert kinds == ["started", "timer_fired", "completed"]

    async def test_wait_step_emits_started_lifecycle_event_only(self) -> None:
        h = make_harness(doc_yaml=_WAIT_DOC)
        await h.controller.start_run(
            workspace_id=WORKSPACE,
            workflow_version_id=WORKFLOW_VERSION_ID,
            inputs={},
            idempotency_key=IDEMPOTENCY_KEY,
        )
        # As in the failing-orchestrator scenario, the controller
        # publishes ``workflow.started`` once and stops — terminal
        # lifecycle events are reconciler-owned.
        assert [e.kind for e in h.publisher.events] == [
            LIFECYCLE_KIND_WORKFLOW_STARTED,
        ]


# ---------------------------------------------------------------------------
# Cross-scenario smoke: the harness round-trips a single graph compile
# ---------------------------------------------------------------------------


class TestHarnessSelfCheck:
    def test_compile_doc_yields_deterministic_topology(self) -> None:
        # Pin the harness's compile helper: any drift in the
        # compiler's frontier order (alphabetic on zero-in-degree
        # nodes) would invalidate the byte-equal-replays
        # assumption that the sibling test module relies on.
        graph: ExecutionGraph = compile_doc(_LINEAR_DOC)
        assert graph.topological_order == ("a", "b", "c")

    def test_two_compiles_of_same_doc_are_byte_equal(self) -> None:
        from custos_workflow.graph import to_json

        # Compiling the same document twice must yield byte-equal
        # JSON — the replay-safety test module reads this as a
        # given.
        a = to_json(compile_doc(_LINEAR_DOC))
        b = to_json(compile_doc(_LINEAR_DOC))
        assert a == b

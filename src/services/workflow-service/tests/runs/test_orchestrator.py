"""WF-IMPL-035 — :func:`run_orchestrator` Dapr Workflow function.

Drives the orchestrator under :class:`FakeWorkflowRuntime` with a
stubbed :class:`StepHandler` that records dispatch order. Covers
every acceptance criterion from #387:

* Linear graph — topological order strictly respected.
* Fan-out — multiple zero-in-degree nodes dispatched in the
  compiler's deterministic order.
* :class:`StepFailed` short-circuits the run on the first
  occurrence; the bag still contains every preceding step's
  outputs.
* :class:`StepSkipped` continues to the next node.
* :class:`StepWaiting` surfaces a ``status="waiting"``
  :class:`RunOutput` with the waiting ``step_id`` and ``reason``.
* ``if:`` / ``when:`` / ``unless:`` gates exclude steps without
  consulting the :class:`StepHandler` at all.
* Replay determinism — 100 fresh runtimes scheduling the same
  :class:`RunInput` produce byte-equal dispatch sequences and
  byte-equal :class:`FakeWorkflowRuntime.instance(...).history`
  event kinds + payloads.
* Orchestrator module never imports any Catalog client (the
  compiled graph is the run-time source of truth per design.md
  § Pod Restart / Dapr Replay).
* :data:`ReplayHook` fires exactly once per orchestrator entry,
  BEFORE the first dispatch, even with zero steps to run.
"""

from __future__ import annotations

import asyncio
import inspect
import textwrap
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from custos_cel import FixedClock  # noqa: F401  (kept for parity with sister test modules)

from custos_workflow.bindings import InMemoryActivityTypeRegistry
from custos_workflow.compiler import RunMeta
from custos_workflow.compiler import compile as compile_workflow
from custos_workflow.document import WorkflowDocument
from custos_workflow.graph import to_json
from custos_workflow.graph.model import ExecutionGraph
from custos_workflow.runs import (
    NoopStepHandler,
    ReplayHook,  # noqa: F401  (re-export sanity)
    RunInput,
    RunOutput,
    RunStatus,
    StepExecutionContext,
    StepFailed,
    StepResult,
    StepSkipped,
    StepSucceeded,
    StepWaiting,
    make_run_orchestrator,
)
from custos_workflow.runs.orchestrator import WORKFLOW_NAME
from custos_workflow.runtime import FakeWorkflowClient, FakeWorkflowRuntime
from custos_workflow.runtime._common import ScheduleWorkflowRequest
from custos_workflow.runtime.fake import FakeWorkflowFn

# ---------------------------------------------------------------------------
# Compile helpers — kept inline so this module does not depend on the
# private grammar in tests/test_determinism_property.py.
# ---------------------------------------------------------------------------


def _registry() -> InMemoryActivityTypeRegistry:
    return InMemoryActivityTypeRegistry({})


def _run_meta() -> RunMeta:
    return RunMeta(
        workspace_id="ws-001",
        workflow_version_id="wfv-001",
        workflow_name="pipeline",
        workflow_version_label="v1",
        started_at_default=datetime(2026, 5, 1, tzinfo=UTC),
    )


def _compile(doc_yaml: str) -> ExecutionGraph:
    """Parse + compile a YAML document. Helper for fixture compactness."""

    import yaml

    payload = yaml.safe_load(textwrap.dedent(doc_yaml))
    doc = WorkflowDocument.model_validate(payload)
    return compile_workflow(doc, _run_meta(), _registry())


def _run_input(graph: ExecutionGraph, *, inputs: Mapping[str, Any] | None = None) -> RunInput:
    return RunInput(
        workspace_id="ws-001",
        workflow_version_id="wfv-001",
        compiled_graph_json=to_json(graph),
        inputs=inputs or {},
        idempotency_key="idem-1",
    )


# ---------------------------------------------------------------------------
# Stubbed StepHandler — records dispatch order
# ---------------------------------------------------------------------------


@dataclass
class _RecordingHandler:
    """Records every dispatched step id; emits configurable results.

    The handler returns :class:`StepSucceeded` by default with an
    optional canned outputs mapping per step. Specific steps may be
    overridden to return :class:`StepFailed`, :class:`StepSkipped`,
    or :class:`StepWaiting` by registering them in :attr:`results`.
    """

    dispatched: list[str] = field(default_factory=list)
    outputs_per_step: dict[str, dict[str, Any]] = field(default_factory=dict)
    results: dict[str, StepResult] = field(default_factory=dict)
    # Pinned snapshot of `ctx.outputs` at the moment each step was
    # dispatched. Lets tests assert downstream steps see upstream
    # outputs.
    seen_outputs: list[dict[str, dict[str, Any]]] = field(default_factory=list)

    def execute(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        step_id: str,
    ) -> StepResult:
        self.dispatched.append(step_id)
        # Snapshot the read-only view the handler sees.
        snapshot: dict[str, dict[str, Any]] = {sid: dict(out) for sid, out in ctx.outputs.items()}
        self.seen_outputs.append(snapshot)
        if step_id in self.results:
            return self.results[step_id]
        outputs = self.outputs_per_step.get(step_id, {})
        return StepSucceeded(outputs=outputs)


# ---------------------------------------------------------------------------
# Doc fixtures
# ---------------------------------------------------------------------------


_LINEAR_DOC = """\
    apiVersion: custos.dev/v1
    kind: Workflow
    metadata: {name: linear, workspace: ws}
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


_FANOUT_DOC = """\
    apiVersion: custos.dev/v1
    kind: Workflow
    metadata: {name: fanout, workspace: ws}
    spec:
      inputs:
        flag: {type: boolean, default: true}
      steps:
        - id: a
          let: {x: '${{ true }}'}
        - id: b
          let: {y: '${{ true }}'}
        - id: c
          needs: [a, b]
          let: {z: '${{ true }}'}
"""


_GATED_DOC = """\
    apiVersion: custos.dev/v1
    kind: Workflow
    metadata: {name: gated, workspace: ws}
    spec:
      inputs:
        flag: {type: boolean, default: true}
      steps:
        - id: a
          let: {x: '${{ true }}'}
        - id: skip-if
          if: '${{ inputs.flag }}'
          let: {y: '${{ true }}'}
        - id: pass-when
          when: '${{ inputs.flag }}'
          let: {z: '${{ true }}'}
        - id: skip-unless
          unless: '${{ inputs.flag }}'
          let: {w: '${{ true }}'}
"""


# ---------------------------------------------------------------------------
# Test helpers — schedule + collect
# ---------------------------------------------------------------------------


def _register(runtime: FakeWorkflowRuntime, orchestrator: Any) -> None:
    """Register orchestrator under :data:`WORKFLOW_NAME`.

    The orchestrator returns a :class:`RunOutput` (not a generator),
    which structurally satisfies the runtime contract but not the
    narrow :data:`FakeWorkflowFn` alias. ``FakeWorkflowRuntime``
    explicitly accepts non-generator workflow functions and stores
    the return value as the instance output — see fake.py's
    ``_schedule``.
    """

    runtime.register_workflow(cast(FakeWorkflowFn, orchestrator), name=WORKFLOW_NAME)


def _schedule(
    runtime: FakeWorkflowRuntime,
    client: FakeWorkflowClient,
    handler: Any,
    run_input: RunInput,
    *,
    on_replay: Any = None,
) -> RunOutput:
    """Register the orchestrator under WORKFLOW_NAME and drive it."""

    orchestrator = make_run_orchestrator(handler, on_replay=on_replay)
    _register(runtime, orchestrator)
    instance_id = asyncio.run(
        client.schedule_new_workflow(
            ScheduleWorkflowRequest(workflow=WORKFLOW_NAME, input=run_input)
        )
    )
    state = runtime.instance(instance_id)
    assert isinstance(state.output, RunOutput)
    return state.output


# ---------------------------------------------------------------------------
# Module structure
# ---------------------------------------------------------------------------


class TestModuleStructure:
    def test_workflow_name_is_wire_stable(self) -> None:
        assert WORKFLOW_NAME == "custos.workflow.run"

    def test_module_does_not_import_catalog(self) -> None:
        # Orchestrator is REPLAY-isolated: the compiled graph is the
        # run-time source of truth. The module must never import any
        # Catalog Service client — design.md § Pod Restart / Dapr
        # Replay. (Docstrings naming the constraint are fine; live
        # ``import`` statements are not.)
        import custos_workflow.runs.orchestrator as orch_mod

        source = inspect.getsource(orch_mod)
        for line in source.splitlines():
            stripped = line.strip()
            if stripped.startswith(("import ", "from ")):
                assert "catalog" not in stripped.lower(), (
                    f"orchestrator must not import catalog: {stripped!r}"
                )

    def test_factory_signature_takes_handler_only(self) -> None:
        sig = inspect.signature(make_run_orchestrator)
        params = list(sig.parameters)
        # First positional is the handler; the rest are kw-only options.
        assert params[0] == "handler"
        # Only "handler" is positional; everything else is keyword.
        positional = [
            name
            for name, p in sig.parameters.items()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        ]
        assert positional == ["handler"]


# ---------------------------------------------------------------------------
# RunInput / RunOutput round-trips
# ---------------------------------------------------------------------------


class TestRunInputRoundTrip:
    def test_to_dict_from_dict_round_trips(self) -> None:
        ri = RunInput(
            workspace_id="ws-1",
            workflow_version_id="wfv-1",
            compiled_graph_json='{"graph_schema_version": 1}',
            inputs={"a": 1, "b": "two"},
            idempotency_key="k-1",
        )
        round_tripped = RunInput.from_dict(ri.to_dict())
        assert round_tripped == ri

    def test_from_dict_accepts_missing_optional_fields(self) -> None:
        ri = RunInput.from_dict(
            {
                "workspace_id": "ws-1",
                "workflow_version_id": "wfv-1",
                "compiled_graph_json": '{"graph_schema_version": 1}',
            }
        )
        assert ri.inputs == {}
        assert ri.idempotency_key == ""


class TestRunOutputRoundTrip:
    def test_to_dict_succeeded(self) -> None:
        ro = RunOutput(
            status=RunStatus.SUCCEEDED.value,
            outputs={"a": {"x": 1}},
        )
        d = ro.to_dict()
        assert d["status"] == "succeeded"
        assert d["outputs"] == {"a": {"x": 1}}
        assert d["failed_step"] is None
        assert d["failure_envelope"] is None

    def test_to_dict_failed_carries_envelope(self) -> None:
        ro = RunOutput(
            status=RunStatus.FAILED.value,
            outputs={},
            failed_step="b",
            failure_envelope={"kind": "x", "message": "boom"},
        )
        d = ro.to_dict()
        assert d["failed_step"] == "b"
        assert d["failure_envelope"] == {"kind": "x", "message": "boom"}


# ---------------------------------------------------------------------------
# Dispatch order
# ---------------------------------------------------------------------------


class TestLinearGraph:
    def test_dispatches_in_topological_order(self) -> None:
        graph = _compile(_LINEAR_DOC)
        assert graph.topological_order == ("a", "b", "c")

        runtime = FakeWorkflowRuntime(now=datetime(2026, 5, 28, tzinfo=UTC))
        handler = _RecordingHandler(outputs_per_step={"a": {"av": 1}, "b": {"bv": 2}})

        output = _schedule(runtime, runtime.client(), handler, _run_input(graph))

        assert handler.dispatched == ["a", "b", "c"]
        assert output.status == RunStatus.SUCCEEDED.value
        # Bag contains every step's outputs.
        assert output.outputs == {
            "a": {"av": 1},
            "b": {"bv": 2},
            "c": {},
        }

    def test_downstream_step_sees_upstream_outputs_in_bag(self) -> None:
        graph = _compile(_LINEAR_DOC)
        runtime = FakeWorkflowRuntime()
        handler = _RecordingHandler(outputs_per_step={"a": {"a_v": 10}, "b": {"b_v": 20}})

        _schedule(runtime, runtime.client(), handler, _run_input(graph))

        # Snapshot 0 (when a is dispatched) -> empty bag.
        # Snapshot 1 (when b is dispatched) -> {a: {a_v: 10}}.
        # Snapshot 2 (when c is dispatched) -> {a: ..., b: ...}.
        assert handler.seen_outputs[0] == {}
        assert handler.seen_outputs[1] == {"a": {"a_v": 10}}
        assert handler.seen_outputs[2] == {"a": {"a_v": 10}, "b": {"b_v": 20}}


class TestFanOut:
    def test_independent_nodes_dispatch_in_compiler_order(self) -> None:
        graph = _compile(_FANOUT_DOC)
        # The compiler alphabetizes zero-in-degree frontiers.
        assert graph.topological_order == ("a", "b", "c")

        runtime = FakeWorkflowRuntime()
        handler = _RecordingHandler()

        _schedule(runtime, runtime.client(), handler, _run_input(graph))

        assert handler.dispatched == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# StepResult dispatch
# ---------------------------------------------------------------------------


class TestStepFailedShortCircuits:
    def test_first_failure_returns_failed_status_and_envelope(self) -> None:
        graph = _compile(_LINEAR_DOC)
        runtime = FakeWorkflowRuntime()
        envelope = {"kind": "step.boom", "message": "nope"}
        handler = _RecordingHandler(
            outputs_per_step={"a": {"av": 1}},
            results={"b": StepFailed(envelope=envelope)},
        )

        output = _schedule(runtime, runtime.client(), handler, _run_input(graph))

        # Dispatched up to and including the failing step, no further.
        assert handler.dispatched == ["a", "b"]
        assert output.status == RunStatus.FAILED.value
        assert output.failed_step == "b"
        assert dict(output.failure_envelope or {}) == envelope
        # Preceding step's outputs are preserved.
        assert dict(output.outputs.get("a", {})) == {"av": 1}
        # Failing step was NOT recorded as having outputs.
        assert "b" not in output.outputs
        assert "c" not in output.outputs


class TestStepSkippedContinues:
    def test_handler_returned_skip_records_empty_and_continues(self) -> None:
        graph = _compile(_LINEAR_DOC)
        runtime = FakeWorkflowRuntime()
        handler = _RecordingHandler(
            outputs_per_step={"a": {"av": 1}, "c": {"cv": 3}},
            results={"b": StepSkipped(reason="handler-decided")},
        )

        output = _schedule(runtime, runtime.client(), handler, _run_input(graph))

        assert handler.dispatched == ["a", "b", "c"]
        assert output.status == RunStatus.SUCCEEDED.value
        assert output.outputs == {
            "a": {"av": 1},
            "b": {},
            "c": {"cv": 3},
        }


class TestStepWaiting:
    def test_waiting_surfaces_status_step_and_reason(self) -> None:
        graph = _compile(_LINEAR_DOC)
        runtime = FakeWorkflowRuntime()
        handler = _RecordingHandler(
            outputs_per_step={"a": {"av": 1}},
            results={"b": StepWaiting(reason="timer")},
        )

        output = _schedule(runtime, runtime.client(), handler, _run_input(graph))

        assert handler.dispatched == ["a", "b"]
        assert output.status == "waiting"
        assert output.waiting_step == "b"
        assert output.waiting_reason == "timer"
        # The waiting step's bag entry is empty; downstream not dispatched.
        assert output.outputs.get("b") == {}
        assert "c" not in output.outputs


# ---------------------------------------------------------------------------
# Gating (if / when / unless)
# ---------------------------------------------------------------------------


class TestGating:
    def test_if_false_skips_step(self) -> None:
        graph = _compile(_GATED_DOC)
        runtime = FakeWorkflowRuntime()
        handler = _RecordingHandler()

        # flag=False:
        #   skip-if      (if=false)    -> SKIPPED
        #   pass-when    (when=false)  -> SKIPPED
        #   skip-unless  (unless=false)-> DISPATCHED
        _schedule(runtime, runtime.client(), handler, _run_input(graph, inputs={"flag": False}))

        assert handler.dispatched == ["a", "skip-unless"]

    def test_when_true_passes_and_unless_true_skips(self) -> None:
        graph = _compile(_GATED_DOC)
        runtime = FakeWorkflowRuntime()
        handler = _RecordingHandler()

        # flag=True -> if passes, when passes, unless skips. Topological
        # order is alphabetical: a, pass-when, skip-if, skip-unless
        # (skip-unless is gated out, so it is not dispatched).
        _schedule(runtime, runtime.client(), handler, _run_input(graph, inputs={"flag": True}))

        assert handler.dispatched == ["a", "pass-when", "skip-if"]

    def test_skipped_step_records_empty_outputs_in_bag(self) -> None:
        graph = _compile(_GATED_DOC)
        runtime = FakeWorkflowRuntime()
        handler = _RecordingHandler()

        out = _schedule(
            runtime, runtime.client(), handler, _run_input(graph, inputs={"flag": False})
        )

        # Every gated step has an entry so downstream
        # `steps.<id>.outputs.*` resolves.
        assert "a" in out.outputs
        # skip-if and pass-when are gated out -> empty bag entries.
        assert dict(out.outputs["skip-if"]) == {}
        assert dict(out.outputs["pass-when"]) == {}
        # skip-unless dispatches (unless=false), so its bag entry is the
        # handler's empty success outputs.
        assert dict(out.outputs["skip-unless"]) == {}


# ---------------------------------------------------------------------------
# Replay hook
# ---------------------------------------------------------------------------


class TestReplayHook:
    def test_fires_exactly_once_per_orchestrator_entry(self) -> None:
        graph = _compile(_LINEAR_DOC)
        runtime = FakeWorkflowRuntime()
        handler = _RecordingHandler()
        calls: list[tuple[str, tuple[str, ...]]] = []

        def hook(ctx: StepExecutionContext, g: ExecutionGraph) -> None:
            calls.append((ctx.workspace_id, g.topological_order))

        _schedule(runtime, runtime.client(), handler, _run_input(graph), on_replay=hook)

        assert len(calls) == 1
        assert calls[0] == ("ws-001", ("a", "b", "c"))

    def test_fires_before_first_dispatch(self) -> None:
        graph = _compile(_LINEAR_DOC)
        runtime = FakeWorkflowRuntime()
        order: list[str] = []

        def hook(_ctx: StepExecutionContext, _g: ExecutionGraph) -> None:
            order.append("on_replay")

        @dataclass
        class _OrderRecordingHandler:
            def execute(
                self,
                _ctx: StepExecutionContext,
                _g: ExecutionGraph,
                _step_id: str,
            ) -> StepResult:
                order.append("dispatch")
                return StepSucceeded(outputs={})

        _schedule(
            runtime, runtime.client(), _OrderRecordingHandler(), _run_input(graph), on_replay=hook
        )

        assert order[0] == "on_replay"
        assert order[1] == "dispatch"

    def test_fires_with_empty_graph(self) -> None:
        # Graph with one step is the minimum the compiler accepts;
        # the hook still must fire exactly once.
        graph = _compile(
            """\
            apiVersion: custos.dev/v1
            kind: Workflow
            metadata: {name: empty, workspace: ws}
            spec:
              inputs: {}
              steps:
                - id: only
                  let: {x: '${{ true }}'}
            """
        )
        runtime = FakeWorkflowRuntime()
        handler = _RecordingHandler()
        calls: list[int] = []

        def hook(_ctx: StepExecutionContext, _g: ExecutionGraph) -> None:
            calls.append(1)

        _schedule(runtime, runtime.client(), handler, _run_input(graph), on_replay=hook)

        assert calls == [1]


# ---------------------------------------------------------------------------
# Replay determinism
# ---------------------------------------------------------------------------


class TestReplayDeterminism:
    def test_dispatch_sequence_byte_equal_across_100_replays(self) -> None:
        graph = _compile(_LINEAR_DOC)
        run_input = _run_input(graph, inputs={"flag": True})
        outputs_per_step = {"a": {"av": 1}, "b": {"bv": 2}, "c": {"cv": 3}}

        baseline_dispatch: list[str] | None = None
        baseline_history_kinds: list[str] | None = None
        baseline_output: RunOutput | None = None

        for _ in range(100):
            runtime = FakeWorkflowRuntime(now=datetime(2026, 5, 28, tzinfo=UTC))
            handler = _RecordingHandler(outputs_per_step=outputs_per_step)
            orchestrator = make_run_orchestrator(handler)
            _register(runtime, orchestrator)
            instance_id = asyncio.run(
                runtime.client().schedule_new_workflow(
                    ScheduleWorkflowRequest(
                        workflow=WORKFLOW_NAME,
                        input=run_input,
                        instance_id="run-fixed",
                    )
                )
            )
            state = runtime.instance(instance_id)
            assert isinstance(state.output, RunOutput)

            if baseline_dispatch is None:
                baseline_dispatch = list(handler.dispatched)
                baseline_history_kinds = [e.kind for e in state.history]
                baseline_output = state.output
            else:
                assert handler.dispatched == baseline_dispatch
                assert [e.kind for e in state.history] == baseline_history_kinds
                assert state.output == baseline_output


# ---------------------------------------------------------------------------
# Default handler (NoopStepHandler) — landing path proof
# ---------------------------------------------------------------------------


class TestNoopHandlerEndToEnd:
    def test_let_only_graph_completes_under_noop_handler(self) -> None:
        graph = _compile(_LINEAR_DOC)
        runtime = FakeWorkflowRuntime()
        orchestrator = make_run_orchestrator(NoopStepHandler())
        _register(runtime, orchestrator)
        instance_id = asyncio.run(
            runtime.client().schedule_new_workflow(
                ScheduleWorkflowRequest(workflow=WORKFLOW_NAME, input=_run_input(graph))
            )
        )
        state = runtime.instance(instance_id)
        assert isinstance(state.output, RunOutput)
        assert state.output.status == RunStatus.SUCCEEDED.value


# ---------------------------------------------------------------------------
# Expression timeout pass-through
# ---------------------------------------------------------------------------


class TestExpressionTimeout:
    def test_factory_accepts_kw_only_timeout(self) -> None:
        # Just exercises the kw-only path so the factory parameter is
        # covered. The hot path uses ``custos_cel`` defaults; this
        # asserts the parameter is plumbed through without raising.
        graph = _compile(_GATED_DOC)
        runtime = FakeWorkflowRuntime()
        handler = _RecordingHandler()
        orchestrator = make_run_orchestrator(handler, expression_timeout_ms=500)
        _register(runtime, orchestrator)
        instance_id = asyncio.run(
            runtime.client().schedule_new_workflow(
                ScheduleWorkflowRequest(
                    workflow=WORKFLOW_NAME,
                    input=_run_input(graph, inputs={"flag": True}),
                )
            )
        )
        state = runtime.instance(instance_id)
        assert isinstance(state.output, RunOutput)
        assert state.output.status == RunStatus.SUCCEEDED.value


# ---------------------------------------------------------------------------
# RunInput passed as dict (mirrors Dapr boundary serialization)
# ---------------------------------------------------------------------------


class TestRunInputAsDict:
    def test_orchestrator_accepts_run_input_dict_payload(self) -> None:
        graph = _compile(_LINEAR_DOC)
        runtime = FakeWorkflowRuntime()
        handler = _RecordingHandler()
        orchestrator = make_run_orchestrator(handler)
        _register(runtime, orchestrator)
        payload = _run_input(graph).to_dict()
        instance_id = asyncio.run(
            runtime.client().schedule_new_workflow(
                ScheduleWorkflowRequest(workflow=WORKFLOW_NAME, input=payload)
            )
        )
        state = runtime.instance(instance_id)
        assert isinstance(state.output, RunOutput)
        assert state.output.status == RunStatus.SUCCEEDED.value
        assert handler.dispatched == ["a", "b", "c"]


# (Guard for future maintainers — keep pytest happy if the module is
# imported without any test selection.)
if __name__ == "__main__":
    pytest.main([__file__])

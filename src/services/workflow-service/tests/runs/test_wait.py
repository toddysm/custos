"""WF-IMPL-036 — :class:`WaitStepHandler` (Run Controller's ``wait:`` driver).

``wait:`` is the one step kind the Run Controller orchestrator
handles directly per design.md § Workflow Schema: Step Kinds
Handled — ``Wait / sleep → Run Controller → Durable timer``.

Coverage:

* The ISO-8601 duration grammar accepted by
  :func:`parse_wait_duration` is the *same* one
  :class:`~custos_workflow.document.WaitStep` enforces at parse
  time. The two regex sources are pinned byte-equal here so a
  drift between them fails loudly.
* :func:`parse_wait_duration` accepts the documented shapes
  (``PT5S``, ``PT1H30M``, ``P1D``, ``P2W``, fractional seconds)
  and rejects the documented anti-shapes (CEL tokens, months,
  years, empty / zero / negative durations).
* :class:`WaitStepHandler.execute` opens exactly one
  :meth:`WorkflowContext.create_timer` per dispatch, yields its
  token, and returns :class:`StepSucceeded` with empty outputs.
* End-to-end: a ``wait:`` step compiled into the graph and
  driven through the orchestrator under
  :class:`FakeWorkflowRuntime` produces a ``timer_fired`` history
  event and a ``RunOutput(status="succeeded")``.
* Replay determinism: 100 fresh runtimes scheduling the same
  ``wait:`` workflow produce byte-equal histories.
* The defensive guard raises :class:`WaitDurationError` when the
  orchestrator hands the handler a node whose kind is not
  :attr:`StepKind.WAIT` or whose ``step_source`` is not a
  :class:`WaitStep` — both shapes are unreachable through the
  document model + compiler, so the assertion is defence in depth.
"""

from __future__ import annotations

import asyncio
import dataclasses
import inspect
import textwrap
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from custos_workflow.bindings import InMemoryActivityTypeRegistry
from custos_workflow.compiler import RunMeta
from custos_workflow.compiler import compile as compile_workflow
from custos_workflow.document import WaitStep, WorkflowDocument
from custos_workflow.graph import to_json
from custos_workflow.graph.model import ExecutionGraph, ExecutionNode, StepKind
from custos_workflow.runs import (
    NoopStepHandler,
    RunInput,
    RunOutput,
    WaitDurationError,
    WaitStepHandler,
    make_run_orchestrator,
    parse_wait_duration,
)
from custos_workflow.runs.orchestrator import WORKFLOW_NAME
from custos_workflow.runtime import FakeWorkflowClient, FakeWorkflowRuntime
from custos_workflow.runtime._common import ScheduleWorkflowRequest
from custos_workflow.runtime.fake import (
    FakeWorkflowContext,
    FakeWorkflowFn,
    _TimerTask,
)

# ---------------------------------------------------------------------------
# Compile helpers
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
    import yaml

    payload = yaml.safe_load(textwrap.dedent(doc_yaml))
    doc = WorkflowDocument.model_validate(payload)
    return compile_workflow(doc, _run_meta(), _registry())


def _wait_doc(wait_value: str, *, step_id: str = "pause") -> str:
    return f"""\
        apiVersion: custos.dev/v1
        kind: Workflow
        metadata: {{name: waiter, workspace: ws}}
        spec:
          inputs: {{}}
          steps:
            - id: {step_id}
              wait: {wait_value!r}
    """


def _run_input(graph: ExecutionGraph, *, inputs: Mapping[str, Any] | None = None) -> RunInput:
    return RunInput(
        workspace_id="ws-001",
        workflow_version_id="wfv-001",
        compiled_graph_json=to_json(graph),
        inputs=inputs or {},
        idempotency_key="idem-1",
    )


# ---------------------------------------------------------------------------
# Pattern parity (regex drift guard)
# ---------------------------------------------------------------------------


class TestGrammarParity:
    def test_runtime_and_document_patterns_are_byte_equal(self) -> None:
        # Two independently owned regex sources for the SAME grammar
        # — see wait.py module docstring. They must stay byte-equal
        # so the document model's accept set == the runtime's accept
        # set. A drift means a graph that publishes can be rejected
        # at run time (or vice-versa).
        from custos_workflow.document import models as doc_models
        from custos_workflow.runs import wait as wait_mod

        assert (
            wait_mod._ISO8601_DURATION_PATTERN.pattern
            == doc_models._ISO8601_DURATION_PATTERN.pattern
        )


# ---------------------------------------------------------------------------
# parse_wait_duration
# ---------------------------------------------------------------------------


class TestParseWaitDuration:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("PT5S", timedelta(seconds=5)),
            ("PT30M", timedelta(minutes=30)),
            ("PT1H", timedelta(hours=1)),
            ("PT1H30M", timedelta(hours=1, minutes=30)),
            ("PT1H30M15S", timedelta(hours=1, minutes=30, seconds=15)),
            ("PT0.5S", timedelta(milliseconds=500)),
            ("P1D", timedelta(days=1)),
            ("P7D", timedelta(days=7)),
            ("P2W", timedelta(weeks=2)),
            ("P1DT12H", timedelta(days=1, hours=12)),
        ],
    )
    def test_parses_documented_shapes(self, raw: str, expected: timedelta) -> None:
        assert parse_wait_duration("pause", raw) == expected

    @pytest.mark.parametrize(
        "raw",
        [
            # CEL token leaked through (document model rejects, runtime
            # double-checks).
            "${{ inputs.delay }}",
            # Bare numbers / wrong prefix.
            "5",
            "5s",
            "5S",
            # Months / years — calendar-dependent.
            "P1M",
            "P1Y",
            "P1Y2M",
            # Empty / structurally invalid.
            "",
            "P",
            "PT",
            # Mixed garbage.
            "PT5X",
            "PT--5S",
        ],
    )
    def test_rejects_non_grammar(self, raw: str) -> None:
        with pytest.raises(WaitDurationError) as exc_info:
            parse_wait_duration("pause", raw)
        err = exc_info.value
        assert err.step_id == "pause"
        assert err.duration == raw
        assert err.KIND == "compile.wait_duration"

    @pytest.mark.parametrize("raw", ["PT0S", "P0D", "P0W", "PT0H0M0S", "PT0.0S"])
    def test_rejects_non_positive_durations(self, raw: str) -> None:
        with pytest.raises(WaitDurationError) as exc_info:
            parse_wait_duration("pause", raw)
        assert "greater than zero" in exc_info.value.reason

    @pytest.mark.parametrize("raw", ["P", "PT"])
    def test_rejects_structurally_empty_durations(self, raw: str) -> None:
        # ``P`` / ``PT`` carry no components at all — a different
        # failure shape from explicit-zero (``PT0S``) and worth
        # surfacing distinctly in the audit envelope.
        with pytest.raises(WaitDurationError) as exc_info:
            parse_wait_duration("pause", raw)
        assert "at least one component" in exc_info.value.reason

    def test_error_envelope_round_trip(self) -> None:
        # ``compile.wait_duration`` is the kind tag the
        # observability-audit-service indexes against. The error's
        # to_dict() envelope MUST carry the step_id + duration so an
        # operator can triage which step blew up without re-fetching
        # the graph.
        err = WaitDurationError("pause", "PT-5S", "bad")
        envelope = err.to_dict()
        assert envelope["kind"] == "compile.wait_duration"
        assert envelope["step_id"] == "pause"
        assert envelope["duration"] == "PT-5S"
        assert envelope["reason"] == "bad"

    def test_is_value_error(self) -> None:
        # CompileError subclasses pin ValueError so existing
        # ``except ValueError:`` blocks (e.g. in retry-resolve) catch
        # them. Mirroring the rest of the compile.* taxonomy.
        assert issubclass(WaitDurationError, ValueError)


# ---------------------------------------------------------------------------
# WaitStepHandler unit
# ---------------------------------------------------------------------------


class TestWaitStepHandlerUnit:
    def test_yields_single_timer_token_and_returns_succeeded(self) -> None:
        graph = _compile(_wait_doc("PT5S"))
        node = graph.nodes[0]
        assert node.kind is StepKind.WAIT

        handler = WaitStepHandler()
        # Build a real FakeWorkflowContext so create_timer() returns
        # an honest _TimerTask token (rather than a Mock).
        ctx = FakeWorkflowContext(
            instance_id="inst-1",
            now=datetime(2026, 5, 1, tzinfo=UTC),
        )

        gen = handler.execute(ctx, node)
        token = next(gen)
        assert isinstance(token, _TimerTask)
        assert token.fire_at == datetime(2026, 5, 1, 0, 0, 5, tzinfo=UTC)

        # Driving past the yield returns StopIteration carrying the
        # StepSucceeded result with empty outputs.
        with pytest.raises(StopIteration) as stop_info:
            gen.send(None)
        assert stop_info.value.value.outputs == {}

    def test_rejects_non_wait_node(self) -> None:
        # Defensive guard: dispatching on a non-WAIT node must raise
        # rather than open a timer for an unrelated step kind.
        let_doc = """\
            apiVersion: custos.dev/v1
            kind: Workflow
            metadata: {name: compute, workspace: ws}
            spec:
              inputs: {}
              steps:
                - id: compute
                  let: {x: '${{ true }}'}
        """
        graph = _compile(let_doc)
        node = graph.nodes[0]
        assert node.kind is StepKind.LET

        handler = WaitStepHandler()
        ctx = FakeWorkflowContext(
            instance_id="inst-2",
            now=datetime(2026, 5, 1, tzinfo=UTC),
        )

        gen = handler.execute(ctx, node)
        with pytest.raises(WaitDurationError) as exc_info:
            next(gen)
        assert exc_info.value.step_id == "compute"
        assert "non-wait node" in exc_info.value.reason

    def test_rejects_node_with_non_wait_step_source(self) -> None:
        # Construct an ExecutionNode that *claims* WAIT kind but
        # carries a non-WaitStep step_source. This shape cannot be
        # produced by the compiler; the guard is defence in depth
        # for a schema-skewed graph.
        wait_graph = _compile(_wait_doc("PT5S"))
        wait_node = wait_graph.nodes[0]
        # Borrow a LET step_source.
        let_graph = _compile(
            """\
                apiVersion: custos.dev/v1
                kind: Workflow
                metadata: {name: c, workspace: ws}
                spec:
                  inputs: {}
                  steps:
                    - id: x
                      let: {y: '${{ true }}'}
            """
        )
        let_step_source = let_graph.nodes[0].step_source
        skewed = dataclasses.replace(wait_node, step_source=let_step_source)
        assert skewed.kind is StepKind.WAIT
        assert not isinstance(skewed.step_source, WaitStep)

        handler = WaitStepHandler()
        ctx = FakeWorkflowContext(
            instance_id="inst-3",
            now=datetime(2026, 5, 1, tzinfo=UTC),
        )
        gen = handler.execute(ctx, node=skewed)
        with pytest.raises(WaitDurationError) as exc_info:
            next(gen)
        assert "expected a WaitStep" in exc_info.value.reason


# ---------------------------------------------------------------------------
# Document-level WaitStep validation
# ---------------------------------------------------------------------------


class TestWaitStepDocumentModel:
    @pytest.mark.parametrize(
        "wait_value",
        ["PT5S", "PT1H30M", "P1D", "P2W", "PT0.5S"],
    )
    def test_accepts_documented_shapes(self, wait_value: str) -> None:
        graph = _compile(_wait_doc(wait_value))
        assert graph.nodes[0].kind is StepKind.WAIT
        step_source = graph.nodes[0].step_source
        assert isinstance(step_source, WaitStep)
        assert step_source.wait == wait_value

    @pytest.mark.parametrize(
        "wait_value",
        [
            # Calendar-dependent — rejected.
            "P1M",
            "P1Y",
            # Bare number — rejected.
            "5",
            # CEL token — rejected (wait: is a constant-only field).
            "${{ inputs.delay }}",
            # Structurally empty / zero — the regex permits these
            # because every component is optional, but a durable
            # timer requires a positive duration. Publish-time
            # validation must catch them so they cannot reach
            # runtime.
            "PT0S",
            "P0D",
            "P0W",
            "PT0H0M0S",
        ],
    )
    def test_rejects_non_grammar(self, wait_value: str) -> None:
        from pydantic import ValidationError

        # The document model rejects these at parse time, before
        # they reach the compiler / runtime.
        with pytest.raises(ValidationError):
            _compile(_wait_doc(wait_value))

    @pytest.mark.parametrize("wait_value", ["P", "PT"])
    def test_rejects_empty_grammar(self, wait_value: str) -> None:
        # ``P`` / ``PT`` violate the document model's ``min_length=2``
        # guard or its zero-component check, depending on which
        # check fires first. Either way the document refuses to
        # parse them.
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            _compile(_wait_doc(wait_value))


# ---------------------------------------------------------------------------
# StepKind / PrimitiveHandler enum membership
# ---------------------------------------------------------------------------


class TestEnumMembership:
    def test_step_kind_has_wait(self) -> None:
        assert StepKind.WAIT.value == "wait"

    def test_primitive_handler_has_run_controller_timer(self) -> None:
        from custos_workflow.graph.model import PrimitiveHandler

        assert PrimitiveHandler.RUN_CONTROLLER_TIMER.value == "run_controller_timer"

    def test_compiler_dispatch_maps_wait(self) -> None:
        graph = _compile(_wait_doc("PT5S"))
        node = graph.nodes[0]
        from custos_workflow.graph.model import PrimitiveHandler

        assert node.kind is StepKind.WAIT
        assert node.primitive_handler is PrimitiveHandler.RUN_CONTROLLER_TIMER


# ---------------------------------------------------------------------------
# End-to-end through the orchestrator
# ---------------------------------------------------------------------------


def _register(runtime: FakeWorkflowRuntime, orchestrator: Any) -> None:
    runtime.register_workflow(cast(FakeWorkflowFn, orchestrator), name=WORKFLOW_NAME)


def _schedule(
    runtime: FakeWorkflowRuntime,
    client: FakeWorkflowClient,
    run_input: RunInput,
) -> tuple[RunOutput, list[Any]]:
    handler = NoopStepHandler()
    orchestrator = make_run_orchestrator(handler)
    _register(runtime, orchestrator)
    instance_id = asyncio.run(
        client.schedule_new_workflow(
            ScheduleWorkflowRequest(workflow=WORKFLOW_NAME, input=run_input)
        )
    )
    state = runtime.instance(instance_id)
    assert isinstance(state.output, RunOutput)
    return state.output, list(state.history)


class TestOrchestratorEndToEnd:
    def test_single_wait_step_fires_timer_and_completes(self) -> None:
        graph = _compile(_wait_doc("PT5S"))
        runtime = FakeWorkflowRuntime()
        client = FakeWorkflowClient(runtime=runtime)
        out, history = _schedule(runtime, client, _run_input(graph))

        assert out.status == "succeeded"
        assert out.failed_step is None
        assert out.failure_envelope is None
        # The fake auto-fires timers; exactly one ``timer_fired``
        # event appears in history, sandwiched by ``started`` and
        # ``completed``.
        kinds = [event.kind for event in history]
        assert kinds == ["started", "timer_fired", "completed"]

    def test_wait_then_let_runs_let_after_timer(self) -> None:
        # A subsequent step depending on the wait runs only after
        # the timer fires; output bag records both with empty outputs
        # for the wait step.
        doc = """\
            apiVersion: custos.dev/v1
            kind: Workflow
            metadata: {name: w, workspace: ws}
            spec:
              inputs: {}
              steps:
                - id: pause
                  wait: 'PT2S'
                - id: after
                  needs: [pause]
                  let: {ok: '${{ true }}'}
        """
        graph = _compile(textwrap.dedent(doc))
        runtime = FakeWorkflowRuntime()
        client = FakeWorkflowClient(runtime=runtime)
        out, history = _schedule(runtime, client, _run_input(graph))

        assert out.status == "succeeded"
        kinds = [event.kind for event in history]
        # Started, timer_fired (from pause), completed.
        assert kinds == ["started", "timer_fired", "completed"]

    def test_two_sequential_wait_steps_fire_two_timers(self) -> None:
        doc = """\
            apiVersion: custos.dev/v1
            kind: Workflow
            metadata: {name: ww, workspace: ws}
            spec:
              inputs: {}
              steps:
                - id: first
                  wait: 'PT1S'
                - id: second
                  needs: [first]
                  wait: 'PT2S'
        """
        graph = _compile(textwrap.dedent(doc))
        runtime = FakeWorkflowRuntime()
        client = FakeWorkflowClient(runtime=runtime)
        out, history = _schedule(runtime, client, _run_input(graph))

        assert out.status == "succeeded"
        kinds = [event.kind for event in history]
        assert kinds == ["started", "timer_fired", "timer_fired", "completed"]


# ---------------------------------------------------------------------------
# Replay determinism
# ---------------------------------------------------------------------------


class TestReplayDeterminism:
    def test_byte_equal_histories_across_100_replays(self) -> None:
        graph = _compile(_wait_doc("PT5S"))
        run_input = _run_input(graph)

        snapshots: list[list[tuple[str, Any]]] = []
        for _ in range(100):
            runtime = FakeWorkflowRuntime()
            client = FakeWorkflowClient(runtime=runtime)
            _, history = _schedule(runtime, client, run_input)
            snapshots.append([(event.kind, event.detail) for event in history])

        # Every replay produces the same history.
        first = snapshots[0]
        for snap in snapshots[1:]:
            assert snap == first


# ---------------------------------------------------------------------------
# Orchestrator wiring surface
# ---------------------------------------------------------------------------


class TestOrchestratorWiring:
    def test_factory_accepts_custom_wait_handler(self) -> None:
        # Run Controller can inject a stubbed wait handler for
        # metric / trace decoration.
        graph = _compile(_wait_doc("PT5S"))
        dispatched: list[str] = []

        class _RecordingWait(WaitStepHandler):
            def execute(self, ctx: Any, node: ExecutionNode):  # type: ignore[no-untyped-def]
                dispatched.append(node.step_id)
                result = yield from super().execute(ctx, node)
                return result

        runtime = FakeWorkflowRuntime()
        client = FakeWorkflowClient(runtime=runtime)
        orchestrator = make_run_orchestrator(NoopStepHandler(), wait_handler=_RecordingWait())
        _register(runtime, orchestrator)
        asyncio.run(
            client.schedule_new_workflow(
                ScheduleWorkflowRequest(workflow=WORKFLOW_NAME, input=_run_input(graph))
            )
        )
        assert dispatched == ["pause"]

    def test_factory_signature_accepts_wait_handler_kwarg(self) -> None:
        sig = inspect.signature(make_run_orchestrator)
        assert "wait_handler" in sig.parameters
        assert sig.parameters["wait_handler"].kind is inspect.Parameter.KEYWORD_ONLY

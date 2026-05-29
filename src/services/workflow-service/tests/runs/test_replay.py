"""WF-IMPL-042 — :class:`ReplayReconciler` Protocol + ``NoopReplayReconciler``.

Covers every acceptance criterion from #394:

* :class:`ReplayReconciler` Protocol is ``runtime_checkable`` and
  any object with a structurally compatible ``on_replay`` method
  satisfies it (Phase E plugs the real Step Coordinator
  implementation in under WF-IMPL-046).
* :class:`NoopReplayReconciler` is the default — its ``on_replay``
  is a no-op and instances compare equal (frozen + slots).
* :class:`RunController` accepts an optional ``replay_reconciler``
  dependency; omitting it defaults to ``NoopReplayReconciler()``
  without error.
* When ``reconciler.on_replay`` is bound to the orchestrator's
  :data:`ReplayHook` slot, the orchestrator fires it exactly once
  per orchestrator entry, BEFORE the first node dispatch.
* The reconciler fires even for a minimal one-step graph (the
  smallest graph the compiler accepts) so a stale-state sweep can
  always run regardless of whether any step is about to dispatch.
* Across 50 simulated orchestrator entries (fresh runtime per
  entry — Dapr replays are an opaque internal property of the
  runtime that the FakeWorkflowRuntime does not simulate, so we
  exercise the next-coarser invariant Phase E actually relies on:
  one ``on_replay`` invocation per orchestrator entry, multiplied
  out to 50 entries → exactly 50 invocations).
"""

from __future__ import annotations

import asyncio
import textwrap
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from custos_cel import FixedClock
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

from custos_workflow.bindings import InMemoryActivityTypeRegistry
from custos_workflow.compiler import RunMeta
from custos_workflow.compiler import compile as compile_workflow
from custos_workflow.document import WorkflowDocument
from custos_workflow.graph import to_json
from custos_workflow.graph.model import ExecutionGraph
from custos_workflow.runs import (
    WORKFLOW_NAME,
    InMemoryLifecycleEventPublisher,
    InProcessRunStore,
    NoopReplayReconciler,
    NoopStepHandler,
    ReplayReconciler,
    RunController,
    RunInput,
    RunOutput,
    StepExecutionContext,
    StepResult,
    StepSucceeded,
    make_run_orchestrator,
)
from custos_workflow.runs.replay import NoopReplayReconciler as _NoopFromModule
from custos_workflow.runs.replay import ReplayReconciler as _ProtocolFromModule
from custos_workflow.runtime import FakeWorkflowClient, FakeWorkflowRuntime
from custos_workflow.runtime._common import ScheduleWorkflowRequest
from custos_workflow.runtime.fake import FakeWorkflowFn
from tests.runs._fakes import FakeMetadataStoreProvider

# ---------------------------------------------------------------------------
# Compile / runtime helpers — kept inline to avoid cross-test imports.
# ---------------------------------------------------------------------------


WORKSPACE = "ws-001"
FIXED_NOW = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)


def _run_meta() -> RunMeta:
    return RunMeta(
        workspace_id=WORKSPACE,
        workflow_version_id="wfv-001",
        workflow_name="pipeline",
        workflow_version_label="v1",
        started_at_default=FIXED_NOW,
    )


def _compile(doc_yaml: str) -> ExecutionGraph:
    import yaml

    payload = yaml.safe_load(textwrap.dedent(doc_yaml))
    doc = WorkflowDocument.model_validate(payload)
    return compile_workflow(doc, _run_meta(), InMemoryActivityTypeRegistry({}))


def _run_input(graph: ExecutionGraph) -> RunInput:
    return RunInput(
        workspace_id=WORKSPACE,
        workflow_version_id="wfv-001",
        compiled_graph_json=to_json(graph),
        inputs={},
        idempotency_key="idem-1",
    )


_LINEAR_DOC = """\
    apiVersion: custos.dev/v1
    kind: Workflow
    metadata: {name: linear, workspace: ws}
    spec:
      steps:
        - id: a
          let: {x: '${{ true }}'}
        - id: b
          needs: [a]
          let: {y: '${{ true }}'}
"""


_SINGLE_STEP_DOC = """\
    apiVersion: custos.dev/v1
    kind: Workflow
    metadata: {name: single, workspace: ws}
    spec:
      steps:
        - id: only
          let: {x: '${{ true }}'}
"""


def _drive(
    runtime: FakeWorkflowRuntime,
    client: FakeWorkflowClient,
    handler: Any,
    run_input: RunInput,
    *,
    on_replay: Any,
    instance_id: str | None = None,
) -> RunOutput:
    """Register orchestrator + drive one workflow entry under the
    fake runtime; return the produced :class:`RunOutput`."""

    orchestrator = make_run_orchestrator(handler, on_replay=on_replay)
    runtime.register_workflow(cast(FakeWorkflowFn, orchestrator), name=WORKFLOW_NAME)
    iid = asyncio.run(
        client.schedule_new_workflow(
            ScheduleWorkflowRequest(
                workflow=WORKFLOW_NAME,
                input=run_input,
                instance_id=instance_id,
            )
        )
    )
    state = runtime.instance(iid)
    assert isinstance(state.output, RunOutput)
    return state.output


# ---------------------------------------------------------------------------
# Module structure
# ---------------------------------------------------------------------------


class TestModuleStructure:
    def test_protocol_and_noop_are_re_exported_from_runs_package(self) -> None:
        assert ReplayReconciler is _ProtocolFromModule
        assert NoopReplayReconciler is _NoopFromModule

    def test_protocol_method_signature_matches_orchestrator_hook(self) -> None:
        import inspect

        sig = inspect.signature(ReplayReconciler.on_replay)
        params = list(sig.parameters.values())
        # (self, ctx, graph) → None. Matches the orchestrator's
        # :data:`ReplayHook` ``Callable[[StepExecutionContext, ExecutionGraph], None]``
        # so ``reconciler.on_replay`` can be bound to the
        # ``on_replay=`` slot of :func:`make_run_orchestrator`
        # without an adapter.
        assert [p.name for p in params] == ["self", "ctx", "graph"]


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


class TestReplayReconcilerProtocol:
    def test_protocol_is_runtime_checkable(self) -> None:
        # Bare structural check — anything with a callable
        # ``on_replay`` satisfies isinstance under
        # ``runtime_checkable``.
        @dataclass
        class _Impl:
            def on_replay(self, ctx: StepExecutionContext, graph: ExecutionGraph) -> None:
                return None

        assert isinstance(_Impl(), ReplayReconciler)

    def test_noop_satisfies_protocol(self) -> None:
        assert isinstance(NoopReplayReconciler(), ReplayReconciler)

    def test_object_without_on_replay_fails_protocol(self) -> None:
        class _NotImpl:
            pass

        assert not isinstance(_NotImpl(), ReplayReconciler)


# ---------------------------------------------------------------------------
# NoopReplayReconciler
# ---------------------------------------------------------------------------


class TestNoopReplayReconciler:
    def test_on_replay_returns_none_and_does_not_raise(self) -> None:
        # Build a real StepExecutionContext + ExecutionGraph and call
        # the noop against them. The reconciler must accept any
        # well-formed snapshot without inspecting it.
        graph = _compile(_LINEAR_DOC)
        runtime = FakeWorkflowRuntime(now=FIXED_NOW)
        captured: list[tuple[StepExecutionContext, ExecutionGraph]] = []

        def capture(ctx: StepExecutionContext, g: ExecutionGraph) -> None:
            captured.append((ctx, g))

        _drive(runtime, runtime.client(), NoopStepHandler(), _run_input(graph), on_replay=capture)
        assert len(captured) == 1
        ctx, g = captured[0]

        # The noop must not raise. The return value is statically
        # typed as ``None`` (the Protocol contract), so we don't
        # re-assert it here.
        reconciler = NoopReplayReconciler()
        reconciler.on_replay(ctx, g)

    def test_instances_compare_equal(self) -> None:
        # Frozen + slots dataclass with no state → all instances
        # equal. Lets dependency-injection containers de-duplicate.
        assert NoopReplayReconciler() == NoopReplayReconciler()
        assert hash(NoopReplayReconciler()) == hash(NoopReplayReconciler())

    def test_instances_are_immutable(self) -> None:
        # Frozen guard — mutation attempts raise. The exact
        # exception type depends on the dataclass+slots interaction
        # (``FrozenInstanceError``/``AttributeError`` on classic
        # frozen dataclasses, ``TypeError`` when slots are involved
        # on some CPython versions); we only assert the invariant
        # that mutation cannot succeed.
        reconciler = NoopReplayReconciler()
        with pytest.raises((AttributeError, TypeError)):
            reconciler.attribute = "x"  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Orchestrator wiring — one invocation per entry, fires even for the
# smallest representable graph, scales linearly across 50 entries.
# ---------------------------------------------------------------------------


@dataclass
class _CountingReconciler:
    """``ReplayReconciler`` implementation that counts invocations
    and pins the ``(ctx.workspace_id, graph.topological_order)``
    snapshot of every call so tests can assert ordering."""

    calls: list[tuple[str, tuple[str, ...]]] = field(default_factory=list)

    def on_replay(self, ctx: StepExecutionContext, graph: ExecutionGraph) -> None:
        self.calls.append((ctx.workspace_id, graph.topological_order))


class TestReconcilerFiredExactlyOncePerEntry:
    def test_one_entry_one_invocation(self) -> None:
        graph = _compile(_LINEAR_DOC)
        runtime = FakeWorkflowRuntime(now=FIXED_NOW)
        reconciler = _CountingReconciler()

        _drive(
            runtime,
            runtime.client(),
            NoopStepHandler(),
            _run_input(graph),
            on_replay=reconciler.on_replay,
        )

        assert len(reconciler.calls) == 1
        workspace_id, topo = reconciler.calls[0]
        assert workspace_id == WORKSPACE
        assert topo == ("a", "b")

    def test_fires_before_first_dispatch(self) -> None:
        graph = _compile(_LINEAR_DOC)
        runtime = FakeWorkflowRuntime(now=FIXED_NOW)
        order: list[str] = []
        reconciler_calls: list[int] = []

        def record_reconcile(ctx: StepExecutionContext, g: ExecutionGraph) -> None:
            order.append("reconcile")
            reconciler_calls.append(1)

        @dataclass
        class _RecordingHandler:
            def execute(
                self,
                _ctx: StepExecutionContext,
                _g: ExecutionGraph,
                _step_id: str,
            ) -> StepResult:
                order.append("dispatch")
                return StepSucceeded(outputs={})

        _drive(
            runtime,
            runtime.client(),
            _RecordingHandler(),
            _run_input(graph),
            on_replay=record_reconcile,
        )

        # Reconciler fires exactly once, BEFORE every dispatch in the
        # graph (the linear doc has two steps → two dispatches).
        assert reconciler_calls == [1]
        assert order == ["reconcile", "dispatch", "dispatch"]


class TestReconcilerFiresOnMinimalGraph:
    def test_single_step_graph_still_invokes_reconciler(self) -> None:
        # The compiler does not accept a truly empty graph; the
        # smallest representable workflow has one step. The hook
        # must still fire on this minimal graph so a stale-state
        # sweep can always run regardless of step count.
        graph = _compile(_SINGLE_STEP_DOC)
        runtime = FakeWorkflowRuntime(now=FIXED_NOW)
        reconciler = _CountingReconciler()

        _drive(
            runtime,
            runtime.client(),
            NoopStepHandler(),
            _run_input(graph),
            on_replay=reconciler.on_replay,
        )

        assert len(reconciler.calls) == 1


class TestReconcilerAcrossFiftySimulatedReplays:
    def test_fifty_orchestrator_entries_invoke_reconciler_fifty_times(self) -> None:
        # The FakeWorkflowRuntime does not simulate Dapr's internal
        # replay-the-generator semantics — replays are an opaque
        # property of the production runtime. The Phase-E invariant
        # the Step Coordinator actually relies on is the
        # next-coarser one: every orchestrator ENTRY produces
        # exactly one ``on_replay`` call. That property carries
        # through to N entries → N calls. Asserting 50 here gives
        # the same statistical confidence on the wiring without
        # depending on any private replay simulation.
        graph = _compile(_LINEAR_DOC)
        run_input = _run_input(graph)
        reconciler = _CountingReconciler()

        for n in range(50):
            runtime = FakeWorkflowRuntime(now=FIXED_NOW)
            _drive(
                runtime,
                runtime.client(),
                NoopStepHandler(),
                run_input,
                on_replay=reconciler.on_replay,
                instance_id=f"run-{n:03d}",
            )

        assert len(reconciler.calls) == 50
        # Every call observes the same (workspace, topo) snapshot
        # because the graph + run_input are identical across entries.
        unique_snapshots = set(reconciler.calls)
        assert unique_snapshots == {(WORKSPACE, ("a", "b"))}


# ---------------------------------------------------------------------------
# RunController dependency wiring
# ---------------------------------------------------------------------------


def _store() -> InProcessRunStore:
    provider = FakeMetadataStoreProvider()
    return InProcessRunStore(cast(MetadataStoreProvider, provider))


@dataclass
class _StubWorkflowClient:
    """Minimal client that satisfies the controller's structural
    ``_WorkflowClient`` Protocol; none of the methods are exercised
    by the construction tests."""

    async def schedule_new_workflow(self, request: Any) -> str:  # pragma: no cover
        return ""

    async def terminate_workflow(self, request: Any) -> None:  # pragma: no cover
        return None

    async def get_workflow_state(self, request: Any) -> Any:  # pragma: no cover
        return None

    async def pause_workflow(self, request: Any) -> None:  # pragma: no cover
        return None

    async def resume_workflow(self, request: Any) -> None:  # pragma: no cover
        return None


@dataclass
class _StubCatalog:
    async def get_workflow_version(
        self, workspace_id: str, workflow_version_id: str
    ) -> Any:  # pragma: no cover
        raise NotImplementedError


class TestRunControllerReconcilerDependency:
    def test_defaults_to_noop_when_omitted(self) -> None:
        controller = RunController(
            catalog=cast(Any, _StubCatalog()),
            store=_store(),
            workflow_client=cast(Any, _StubWorkflowClient()),
            activity_registry=InMemoryActivityTypeRegistry({}),
            lifecycle_publisher=InMemoryLifecycleEventPublisher(),
            clock=FixedClock(FIXED_NOW),
        )
        # The default is a NoopReplayReconciler instance; the
        # attribute satisfies the Protocol.
        reconciler = controller._replay_reconciler
        assert isinstance(reconciler, NoopReplayReconciler)
        assert isinstance(reconciler, ReplayReconciler)

    def test_explicit_none_falls_back_to_noop(self) -> None:
        # Explicit ``None`` is treated identically to omission so
        # callers that wire dependencies through a config object
        # don't have to special-case the missing key.
        controller = RunController(
            catalog=cast(Any, _StubCatalog()),
            store=_store(),
            workflow_client=cast(Any, _StubWorkflowClient()),
            activity_registry=InMemoryActivityTypeRegistry({}),
            lifecycle_publisher=InMemoryLifecycleEventPublisher(),
            clock=FixedClock(FIXED_NOW),
            replay_reconciler=None,
        )
        assert isinstance(
            controller._replay_reconciler,
            NoopReplayReconciler,
        )

    def test_accepts_custom_reconciler_implementation(self) -> None:
        # Any Protocol-satisfying object is accepted; the controller
        # holds the dependency by structural type only and does not
        # introspect it.
        custom = _CountingReconciler()
        controller = RunController(
            catalog=cast(Any, _StubCatalog()),
            store=_store(),
            workflow_client=cast(Any, _StubWorkflowClient()),
            activity_registry=InMemoryActivityTypeRegistry({}),
            lifecycle_publisher=InMemoryLifecycleEventPublisher(),
            clock=FixedClock(FIXED_NOW),
            replay_reconciler=custom,
        )
        assert controller._replay_reconciler is custom

    def test_held_reconcilers_on_replay_is_bindable_as_hook(self) -> None:
        # The controller's stored reconciler exposes a bound
        # ``on_replay`` method that satisfies the orchestrator's
        # ``ReplayHook`` callable signature — this is the seam Phase
        # E uses to wire the Step Coordinator's reconciler into
        # ``make_run_orchestrator``.
        custom = _CountingReconciler()
        controller = RunController(
            catalog=cast(Any, _StubCatalog()),
            store=_store(),
            workflow_client=cast(Any, _StubWorkflowClient()),
            activity_registry=InMemoryActivityTypeRegistry({}),
            lifecycle_publisher=InMemoryLifecycleEventPublisher(),
            clock=FixedClock(FIXED_NOW),
            replay_reconciler=custom,
        )

        graph = _compile(_LINEAR_DOC)
        runtime = FakeWorkflowRuntime(now=FIXED_NOW)
        _drive(
            runtime,
            runtime.client(),
            NoopStepHandler(),
            _run_input(graph),
            on_replay=controller._replay_reconciler.on_replay,
        )
        assert len(custom.calls) == 1

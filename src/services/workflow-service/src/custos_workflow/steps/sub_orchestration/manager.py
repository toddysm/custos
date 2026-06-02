"""Sub-Orchestration Manager — dynamic loop fan-out (WF-IMPL-089).

The Sub-Orchestration Manager (ADR-007) is the parent-side driver that
spawns and awaits the child Dapr Workflow instances WF-IMPL-088 ships.
This module lands its first operation: :meth:`SubOrchestrationManager.run_loop`,
the dynamic-loop (``forEach``) fan-out path.

What ``run_loop`` does
----------------------

For a ``forEach``-bearing step the manager:

1. Evaluates the ``forEach`` CEL call site against a
   :class:`custos_cel.BindingScope` built from the step's
   :class:`~custos_workflow.runs.StepExecutionContext` — the same
   ``inputs`` / ``steps`` / ``run`` / ``workflow`` / ``now`` roots the
   top-level orchestrator's gate evaluator uses — to obtain the
   iterable list of items.
2. Spawns **one child workflow instance per item**, each running the
   loop body (the same node) under :data:`CHILD_STEP_WORKFLOW_NAME`,
   with a deterministic instance id
   ``<parentRunId>/<stepId>/<iterationKey>`` (WF-IMPL-085) so Dapr
   replay reproduces an identical child set.
3. Awaits every child with a single ``when_all`` (WF-IMPL-084).
4. Merges the children's outputs into an **ordered list** that matches
   the input list order — the value that becomes
   ``steps.<stepId>.outputs`` in the parent's expression scope.

Result & failure contract
-------------------------

``run_loop`` is a **generator** (it ``yield``s the ``when_all`` task
token back to the runtime). On success it *returns* (via
``StopIteration.value``) the ordered ``list`` of per-child output
mappings. On failure it *raises* a
:class:`~custos_workflow.steps.errors.StepCoordinatorError` subclass so
the WF-IMPL-093 dispatch path can map it uniformly to a
``StepFailed`` envelope:

* :class:`~custos_workflow.steps.errors.LoopExpansionError`
  (``step.loop_expansion_error``) — the ``forEach`` expression failed
  to evaluate or did not yield a list, the optional ``where:`` predicate
  failed to evaluate or did not yield a bool, or the ``forEach`` list
  derived two equal iteration keys.
* :class:`~custos_workflow.steps.errors.SubOrchestrationSpawnError`
  (``step.sub_orchestration_spawn_error``) — a deterministic child
  instance id could not be derived (e.g. a malformed id component).
* :class:`~custos_workflow.steps.errors.SubWorkflowFailedError`
  (``step.sub_workflow_failed``) — a single awaited child ran and
  surfaced a terminal failure; the first such child (in spawn order)
  short-circuits the whole loop.

An empty iterable — or a ``forEach`` list that the optional ``where:``
pre-filter empties — is handled here: it spawns no children and returns
an empty list (without yielding a ``when_all`` over zero tasks).

The optional ``where:`` predicate is evaluated once per item with the
item overlaid into the CEL ``let`` scope under the loop variable
(:data:`DEFAULT_LOOP_VAR`); items the predicate rejects spawn no child.
A ``forEach`` list that derives two equal iteration keys is rejected
with ``step.loop_expansion_error`` (the deterministic child instance id
must be unique per iteration).

Replay determinism (WF-IMPL-095)
--------------------------------

Every child instance id this manager spawns is a pure function of the
parent's durable state — ``<parentRunId>/<stepId>/<iterationKey>``,
where ``iterationKey`` is derived from the ``forEach`` items (themselves
the output of the deterministic CEL evaluator) for loops, or a reserved
constant for ``workflow:`` / ``approval:`` children. Re-entering
``run_loop`` / ``run_sub_workflow`` / ``run_approval`` on a Dapr
Workflow **replay** therefore re-derives a *byte-equal* child id set, so
``call_child_workflow`` matches the recorded history and Dapr returns
the already-completed child's result instead of re-spawning it (no
duplicated side effects). The manager holds no mutable per-run state of
its own — it carries only its immutable fan-out / approval configuration
— which keeps that re-derivation independent of how many times the
orchestrator has been replayed. The property test
``tests/steps/test_sub_orchestration_determinism.py`` locks this
invariant under fuzzing.

Out of scope (later tasks)
--------------------------

The orchestrator dispatch that routes
``PrimitiveHandler.SUB_ORCHESTRATION`` nodes through this manager (and
stores the returned list under ``steps.<stepId>.outputs``) lands in
WF-IMPL-093.
"""

from __future__ import annotations

from collections.abc import Generator, Mapping
from datetime import timedelta
from types import MappingProxyType
from typing import Any, Final

import custos_cel
from custos_cel.errors import CelError
from custos_cel.scope import BindingScope, RunInfo, StepBinding, WorkflowInfo

from custos_workflow.document import ApprovalStep, WorkflowStep
from custos_workflow.graph.model import CallSiteKind, ExecutionGraph, ExecutionNode
from custos_workflow.graph.serialize import to_json
from custos_workflow.runs import WORKFLOW_NAME, RunInput, StepExecutionContext
from custos_workflow.runs.wait import parse_wait_duration
from custos_workflow.steps.errors import (
    ApprovalTimeoutError,
    LoopExpansionError,
    SubOrchestrationSpawnError,
    SubWorkflowFailedError,
)
from custos_workflow.steps.sub_orchestration.child import (
    CHILD_STEP_WORKFLOW_NAME,
    DEFAULT_LOOP_VAR,
    ChildStepInput,
    ChildStepResult,
)
from custos_workflow.steps.sub_orchestration.ids import (
    APPROVAL_ITERATION_KEY,
    WORKFLOW_ITERATION_KEY,
    ChildInstanceIdError,
    child_instance_id,
    iteration_key,
)
from custos_workflow.steps.with_inputs import WithInputResolver

__all__ = [
    "DEFAULT_APPROVAL_TIMEOUT",
    "DEFAULT_MAX_FANOUT_WIDTH",
    "SubOrchestrationManager",
]

#: Stateless ``with:`` evaluator shared across sub-workflow invocations
#: (the resolver holds no per-call state — see :class:`WithInputResolver`).
_WITH_RESOLVER: WithInputResolver = WithInputResolver()

#: Default upper bound on the number of children a single ``forEach``
#: fan-out may spawn (design.md § Configuration, ``WF_MAX_FANOUT_WIDTH``).
#: :func:`custos_workflow.providers.load_run_components` reads the env
#: override and threads it into the manager; a loop that would exceed
#: the cap is rejected with ``step.sub_orchestration_spawn_error``
#: *before* any child is spawned.
DEFAULT_MAX_FANOUT_WIDTH: Final[int] = 1000

#: Default approval-gate timeout applied when a node leaves
#: ``approval.timeout`` at the model default (design.md § Configuration,
#: ``WF_APPROVAL_DEFAULT_TIMEOUT``). An explicit per-document timeout
#: always wins.
DEFAULT_APPROVAL_TIMEOUT: Final[timedelta] = timedelta(hours=24)

#: The :class:`~custos_workflow.document.ApprovalStep` model default for
#: ``approval.timeout`` (``PT24H``). When a node carries exactly this
#: value the document did not override the platform default, so the
#: manager substitutes its configured
#: :attr:`~SubOrchestrationManager._approval_default_timeout`. Kept in
#: lock-step with ``document.models._DEFAULT_APPROVAL_TIMEOUT`` (parity
#: is asserted in the unit tests).
_MODEL_DEFAULT_APPROVAL_TIMEOUT_ISO: Final[str] = "PT24H"

#: Wire-name (and ``call_sites`` key) of the ``forEach`` slot. The
#: call-site collector keys typed call sites by ``CallSite.path``,
#: which for the ``forEach`` slot is the wire-name ``"forEach"`` (see
#: ``custos_workflow.callsites.collect._COMMON_SLOTS``).
_FOR_EACH_SLOT: str = "forEach"

#: Wire-name (and ``call_sites`` key) of the ``where`` slot — the
#: optional per-item predicate applied to the ``forEach`` list.
_WHERE_SLOT: str = "where"


class SubOrchestrationManager:
    """Parent-side driver for child-workflow fan-out (ADR-007).

    Effectively immutable — an instance carries only its fan-out /
    approval configuration (:meth:`__init__`) and holds no per-run
    state, so instances are interchangeable and may be reused across
    runs. Exposes :meth:`run_loop` for the ``forEach`` fan-out path,
    :meth:`run_sub_workflow` for the ``workflow:`` single-child
    invocation path, and :meth:`run_approval` for the ``approval:``
    human-in-the-loop gate.
    """

    def __init__(
        self,
        *,
        max_fanout_width: int = DEFAULT_MAX_FANOUT_WIDTH,
        approval_default_timeout: timedelta = DEFAULT_APPROVAL_TIMEOUT,
    ) -> None:
        """Bind the manager to its fan-out / approval configuration.

        :param max_fanout_width: Upper bound on the number of children
            a single ``forEach`` may spawn (design.md § Configuration,
            ``WF_MAX_FANOUT_WIDTH``). A loop that would exceed the cap
            is rejected with ``step.sub_orchestration_spawn_error``
            *before* any child is spawned. Must be a positive integer.
        :param approval_default_timeout: The gate timeout used when a
            node leaves ``approval.timeout`` at the model default
            (``PT24H``); an explicit per-document timeout always wins
            (design.md § Configuration, ``WF_APPROVAL_DEFAULT_TIMEOUT``).

        :raises ValueError: ``max_fanout_width`` is not a positive
            integer.
        """
        if max_fanout_width < 1:
            raise ValueError(
                f"max_fanout_width must be a positive integer, got {max_fanout_width!r}"
            )
        self._max_fanout_width: int = max_fanout_width
        self._approval_default_timeout: timedelta = approval_default_timeout

    def run_loop(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        step_id: str,
    ) -> Generator[Any, Any, list[Any]]:
        """Fan out the ``forEach`` loop for ``step_id`` and merge results.

        :param ctx: Per-dispatch context bundle. The handler reads
            :attr:`ctx.inputs` / :attr:`ctx.outputs` to build the CEL
            scope, :attr:`ctx.run_id` for the deterministic child
            instance ids, and :attr:`ctx.workflow_context` to spawn and
            await the children.
        :param graph: The compiled execution graph. The loop body is
            the node identified by ``step_id``; its serialized
            single-node graph is what each child runs.
        :param step_id: The id of the ``forEach``-bearing node.

        :returns: The ordered ``list`` of per-child output mappings
            (one entry per item, in input list order). Delivered as the
            generator's ``StopIteration.value``.

        :raises LoopExpansionError: The ``forEach`` (or ``where:``)
            expression failed to evaluate, ``forEach`` did not yield a
            list, ``where:`` did not yield a bool, or two items derived
            the same iteration key.
        :raises SubOrchestrationSpawnError: A deterministic child
            instance id could not be derived for an item, or the loop
            would spawn more children than the configured maximum
            fan-out width.
        :raises SubWorkflowFailedError: A single awaited child ran and
            surfaced a terminal failure (first failure short-circuits).
        :raises KeyError: ``step_id`` is not present in ``graph``.
        """
        node = _resolve_node(graph, step_id)
        run_id = str(ctx.run_id)

        items = self._expand_for_each(ctx, graph, node, step_id, run_id)
        items = self._apply_where(ctx, graph, node, step_id, run_id, items)

        # The loop body each child runs is a byte-stable single-node
        # graph carrying this node; identical for every item (only the
        # per-iteration ``item`` differs), so the child set is
        # reproducible under replay.
        child_graph_json = to_json(_child_graph(graph, node))

        # Derive every iteration key up front so a duplicate is rejected
        # *before* any child is spawned (a partial fan-out would leave
        # dangling durable child instances behind).
        keyed = self._key_items(step_id, run_id, items)

        # Enforce the configured fan-out width cap before spawning any
        # child, for the same reason: a loop that exceeds the cap must
        # fail cleanly rather than leave a partial set of durable child
        # instances behind (design.md § Configuration,
        # ``WF_MAX_FANOUT_WIDTH``).
        if len(keyed) > self._max_fanout_width:
            raise SubOrchestrationSpawnError(
                f"forEach loop on step {step_id!r} would spawn {len(keyed)} children, "
                f"exceeding the configured maximum fan-out width of {self._max_fanout_width}",
                run_id=run_id,
                step_id=step_id,
            )

        spawned: list[tuple[str, str, Any]] = []
        for key, item in keyed:
            try:
                instance_id = child_instance_id(run_id, step_id, key)
            except ChildInstanceIdError as exc:
                raise SubOrchestrationSpawnError(
                    f"failed to derive a child instance id for iteration "
                    f"{key!r} of step {step_id!r}: {exc}",
                    run_id=run_id,
                    step_id=step_id,
                    iteration_key=key,
                    cause=repr(exc),
                ) from exc
            child_input = ChildStepInput(
                workspace_id=ctx.workspace_id,
                workflow_version_id=ctx.workflow_version_id,
                compiled_graph_json=child_graph_json,
                step_id=step_id,
                inputs=ctx.inputs,
                parent_outputs=ctx.outputs,
                loop_var=DEFAULT_LOOP_VAR,
                item=item,
            )
            task = ctx.workflow_context.call_child_workflow(
                CHILD_STEP_WORKFLOW_NAME,
                input=child_input.to_dict(),
                instance_id=instance_id,
            )
            spawned.append((key, instance_id, task))

        # An empty iterable — or a ``where:`` filter that removed every
        # item — spawns no children and produces an empty output list;
        # this early return keeps the zero-child path from yielding a
        # ``when_all`` over no tasks.
        if not spawned:
            return []

        raw_results = yield from self._await_children(ctx, step_id, run_id, spawned)
        return self._merge(step_id, run_id, spawned, raw_results)

    def run_sub_workflow(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        step_id: str,
        child_graph: ExecutionGraph,
    ) -> Generator[Any, Any, dict[str, dict[str, Any]]]:
        """Invoke a ``workflow:`` sub-workflow as one child run and merge it.

        Spawns a single child Run of the compiled sub-workflow
        ``child_graph`` (resolved at compile time per REQ-025 from the
        step's fully-qualified ``workflow`` reference), passes the
        evaluated ``with:`` block through as the child's ``inputs.*``
        namespace, awaits the single child, and returns the child run's
        per-step output bag for the parent to bind under
        ``steps.<stepId>.outputs``.

        :param ctx: Per-dispatch context bundle. :attr:`ctx.inputs` /
            :attr:`ctx.outputs` build the CEL scope the ``with:`` block
            is evaluated against, :attr:`ctx.run_id` seeds the
            deterministic child instance id, and
            :attr:`ctx.workflow_context` spawns and awaits the child.
        :param graph: The compiled parent graph. ``step_id`` selects the
            ``workflow:`` node whose ``with:`` block is evaluated.
        :param step_id: The id of the ``workflow:`` node.
        :param child_graph: The compiled sub-workflow graph the child
            Run executes. The caller (orchestrator dispatch / wiring)
            resolves it from the node's fully-qualified reference.

        :returns: The child run's per-step output bag
            (``{childStepId: {outputs}}``). Delivered as the generator's
            ``StopIteration.value``.

        :raises SubOrchestrationSpawnError: ``step_id`` is not a
            ``workflow:`` node, or a deterministic child instance id
            could not be derived.
        :raises SubWorkflowFailedError: The child run did not succeed.
        :raises WithInputResolutionError: The ``with:`` block failed to
            evaluate.
        :raises KeyError: ``step_id`` is not present in ``graph``.
        """
        node = _resolve_node(graph, step_id)
        run_id = str(ctx.run_id)
        if not isinstance(node.step_source, WorkflowStep):
            # Defensive: the dispatcher only routes ``workflow:`` nodes
            # here, so a non-``WorkflowStep`` source is a programmer error.
            raise SubOrchestrationSpawnError(
                f"step {step_id!r} is not a workflow: invocation",
                run_id=run_id,
                step_id=step_id,
            )

        scope = self._build_scope(ctx, graph, run_id)
        child_inputs = _WITH_RESOLVER.resolve(node, scope, ctx.clock, run_id=run_id)

        try:
            instance_id = child_instance_id(run_id, step_id, WORKFLOW_ITERATION_KEY)
        except ChildInstanceIdError as exc:
            raise SubOrchestrationSpawnError(
                f"failed to derive a child instance id for the sub-workflow "
                f"invocation of step {step_id!r}: {exc}",
                run_id=run_id,
                step_id=step_id,
                iteration_key=WORKFLOW_ITERATION_KEY,
                cause=repr(exc),
            ) from exc

        run_input = RunInput(
            workspace_id=ctx.workspace_id,
            workflow_version_id=node.step_source.workflow,
            compiled_graph_json=to_json(child_graph),
            inputs=child_inputs,
        )
        task = ctx.workflow_context.call_child_workflow(
            WORKFLOW_NAME,
            input=run_input.to_dict(),
            instance_id=instance_id,
        )
        # Reuse the loop fan-in path with a single-element spawn list so
        # an abnormal child failure surfaces as ``step.sub_workflow_failed``
        # exactly as it does for a loop child.
        spawned = [(WORKFLOW_ITERATION_KEY, instance_id, task)]
        raw_results = yield from self._await_children(ctx, step_id, run_id, spawned)
        return self._merge_sub_workflow(step_id, run_id, instance_id, raw_results[0])

    def run_approval(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        step_id: str,
    ) -> Generator[Any, Any, dict[str, Any]]:
        """Drive an ``approval:`` human-in-the-loop gate to a decision.

        Opens a durable timer for the gate's ISO-8601 ``timeout:`` and
        a ``wait_for_external_event`` subscription for the approval
        signal, then races them with ``when_any``: whichever fires
        first resolves the gate. A delivered signal (approve **or**
        reject) binds its payload to the step's outputs; the timer
        winning raises ``step.approval_timeout``.

        The approval signal is delivered through the Trigger Service via
        ``RaiseExternalEvent`` (design.md § Sub-Orchestration) keyed on
        the deterministic gate address ``<parentRunId>/<stepId>/approval``,
        so it is subject to the same dedup / audit / idempotency
        machinery as every other external event. (Registering the
        resume subscription is the deferred Resume Subscription
        Manager's job, out of scope here.)

        :param ctx: Per-dispatch context bundle. :attr:`ctx.run_id`
            seeds the deterministic gate address and
            :attr:`ctx.workflow_context` opens the timer / event / race.
        :param graph: The compiled parent graph. ``step_id`` selects the
            ``approval:`` node.
        :param step_id: The id of the ``approval:`` node.

        :returns: The approval decision payload as the step's outputs
            mapping (an empty mapping when the signal carried no body).
            Delivered as the generator's ``StopIteration.value``.

        :raises SubOrchestrationSpawnError: ``step_id`` is not an
            ``approval:`` node, or the deterministic gate address could
            not be derived.
        :raises ApprovalTimeoutError: The durable timer fired before an
            approval signal arrived.
        :raises KeyError: ``step_id`` is not present in ``graph``.
        """
        node = _resolve_node(graph, step_id)
        run_id = str(ctx.run_id)
        if not isinstance(node.step_source, ApprovalStep):
            # Defensive: the dispatcher only routes ``approval:`` nodes
            # here, so a non-``ApprovalStep`` source is a programmer error.
            raise SubOrchestrationSpawnError(
                f"step {step_id!r} is not an approval: gate",
                run_id=run_id,
                step_id=step_id,
            )

        spec = node.step_source.approval
        try:
            gate_id = child_instance_id(run_id, step_id, APPROVAL_ITERATION_KEY)
        except ChildInstanceIdError as exc:
            raise SubOrchestrationSpawnError(
                f"failed to derive a gate address for the approval gate of step {step_id!r}: {exc}",
                run_id=run_id,
                step_id=step_id,
                iteration_key=APPROVAL_ITERATION_KEY,
                cause=repr(exc),
            ) from exc

        # The timeout is validated to a positive ISO-8601 duration at
        # document-parse time; re-parsing here is defence in depth and
        # keeps the durable timer payload byte-stable across replay.
        # When the node left ``approval.timeout`` at the model default
        # (``PT24H``) the platform-configured default applies instead;
        # any explicit per-document timeout (a different value) wins.
        if spec.timeout == _MODEL_DEFAULT_APPROVAL_TIMEOUT_ISO:
            timeout = self._approval_default_timeout
            timeout_label = _format_iso8601_duration(timeout)
        else:
            timeout = parse_wait_duration(step_id, spec.timeout)
            timeout_label = spec.timeout
        wf = ctx.workflow_context
        event = wf.wait_for_external_event(gate_id)
        timer = wf.create_timer(timeout)
        winner = yield wf.when_any([event, timer])
        if winner is timer:
            raise ApprovalTimeoutError(
                f"approval gate {step_id!r} timed out after {timeout_label!r} "
                "without an approval signal",
                run_id=run_id,
                step_id=step_id,
                child_instance_id=gate_id,
                timeout=timeout_label,
            )
        payload = event.get_result()
        return dict(payload) if isinstance(payload, Mapping) else {}

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _expand_for_each(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        node: ExecutionNode,
        step_id: str,
        run_id: str,
    ) -> list[Any]:
        """Evaluate the ``forEach`` call site to a list of items."""
        call_site = node.call_sites.get(_FOR_EACH_SLOT)
        if call_site is None or call_site.kind is not CallSiteKind.FOR_EACH:
            # Defensive: the dispatcher only routes ``forEach``-bearing
            # steps here, and the document model guarantees the slot is a
            # complete ``${{ ... }}`` token (so a call site always
            # exists). A missing slot is a programmer error.
            raise LoopExpansionError(
                f"step {step_id!r} has no forEach call site to expand",
                run_id=run_id,
                step_id=step_id,
            )

        scope = self._build_scope(ctx, graph, run_id)
        try:
            raw = custos_cel.evaluate(call_site.typed_ast, scope, ctx.clock)
        except CelError as exc:
            raise LoopExpansionError(
                f"failed to evaluate forEach on step {step_id!r}: {exc}",
                run_id=run_id,
                step_id=step_id,
                cause_kind=exc.kind,
                source=call_site.source,
            ) from exc

        if not isinstance(raw, list):
            raise LoopExpansionError(
                f"forEach on step {step_id!r} evaluated to a {type(raw).__name__}, expected a list",
                run_id=run_id,
                step_id=step_id,
                source=call_site.source,
            )
        return raw

    def _apply_where(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        node: ExecutionNode,
        step_id: str,
        run_id: str,
        items: list[Any],
    ) -> list[Any]:
        """Apply the optional ``where:`` pre-filter to ``items``.

        When the node carries a ``where`` call site, the predicate is
        evaluated once per item with the item overlaid into the CEL
        ``let`` scope under :data:`DEFAULT_LOOP_VAR` (so a predicate can
        read e.g. ``let.item.enabled``). Items the predicate rejects are
        dropped and spawn no child. When the node has no ``where`` slot
        the list is returned unchanged.
        """
        call_site = node.call_sites.get(_WHERE_SLOT)
        if call_site is None:
            return items
        if call_site.kind is not CallSiteKind.WHERE:
            # Defensive: the collector keys ``where`` call sites under the
            # ``"where"`` slot with ``CallSiteKind.WHERE``; a mismatch is a
            # programmer error rather than a workflow-author error.
            raise LoopExpansionError(
                f"step {step_id!r} has a non-where call site in the where slot",
                run_id=run_id,
                step_id=step_id,
            )

        base_scope = self._build_scope(ctx, graph, run_id)
        kept: list[Any] = []
        for item in items:
            scope = base_scope.with_let(**{DEFAULT_LOOP_VAR: item})
            try:
                verdict = custos_cel.evaluate(call_site.typed_ast, scope, ctx.clock)
            except CelError as exc:
                raise LoopExpansionError(
                    f"failed to evaluate where on step {step_id!r}: {exc}",
                    run_id=run_id,
                    step_id=step_id,
                    cause_kind=exc.kind,
                    source=call_site.source,
                ) from exc
            if not isinstance(verdict, bool):
                raise LoopExpansionError(
                    f"where on step {step_id!r} evaluated to a "
                    f"{type(verdict).__name__}, expected a bool",
                    run_id=run_id,
                    step_id=step_id,
                    source=call_site.source,
                )
            if verdict:
                kept.append(item)
        return kept

    def _key_items(
        self,
        step_id: str,
        run_id: str,
        items: list[Any],
    ) -> list[tuple[str, Any]]:
        """Pair each item with its deterministic iteration key.

        Two items deriving the same iteration key would collide on the
        deterministic child instance id ``<runId>/<stepId>/<key>``, so
        the whole loop is rejected rather than silently dropping or
        overwriting a child.
        """
        keyed: list[tuple[str, Any]] = []
        seen_keys: set[str] = set()
        for index, item in enumerate(items):
            key = iteration_key(item, index)
            if key in seen_keys:
                raise LoopExpansionError(
                    f"forEach on step {step_id!r} produced a duplicate "
                    f"iteration key {key!r}; loop iteration keys must be unique",
                    run_id=run_id,
                    step_id=step_id,
                    colliding_key=key,
                )
            seen_keys.add(key)
            keyed.append((key, item))
        return keyed

    def _build_scope(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        run_id: str,
    ) -> BindingScope:
        """Build the CEL scope for loop ``forEach`` / ``where:`` evaluation.

        Exposes the same ``inputs`` / ``steps`` / ``run`` / ``workflow``
        / ``now`` roots the top-level orchestrator's gate evaluator uses,
        so a loop expression sees exactly the parent's expression scope.
        """
        return BindingScope(
            run=RunInfo(id=run_id, workspace=ctx.workspace_id),
            workflow=WorkflowInfo(
                name=graph.metadata.workflow_name,
                version=ctx.workflow_version_id,
            ),
            now=ctx.clock.now,
            inputs=ctx.inputs,
            steps=MappingProxyType(
                {sid: StepBinding(out, sealed=True) for sid, out in ctx.outputs.items()}
            ),
        )

    def _await_children(
        self,
        ctx: StepExecutionContext,
        step_id: str,
        run_id: str,
        spawned: list[tuple[str, str, Any]],
    ) -> Generator[Any, Any, list[Any]]:
        """Await all spawned children via a single ``when_all``."""
        from custos_workflow.runtime import ChildWorkflowError

        tasks = [task for _key, _instance_id, task in spawned]
        try:
            results = yield ctx.workflow_context.when_all(tasks)
        except ChildWorkflowError as exc:
            # A child instance failed *abnormally* (an unhandled
            # exception inside the child workflow, not a normal
            # ``status="failed"`` return). Surface the proximate child
            # as a terminal sub-workflow failure.
            key = _iteration_key_for_instance(spawned, exc.instance_id)
            raise SubWorkflowFailedError(
                f"child {exc.instance_id!r} of step {step_id!r} failed: {exc}",
                run_id=run_id,
                step_id=step_id,
                child_instance_id=exc.instance_id,
                iteration_key=key,
                child_kind=exc.error_type,
            ) from exc
        return list(results)

    def _merge(
        self,
        step_id: str,
        run_id: str,
        spawned: list[tuple[str, str, Any]],
        raw_results: list[Any],
    ) -> list[Any]:
        """Merge ordered child results, short-circuiting on first failure."""
        merged: list[Any] = []
        for (key, instance_id, _task), raw_result in zip(spawned, raw_results, strict=True):
            child_result = ChildStepResult.from_dict(raw_result)
            if child_result.status == "failed":
                envelope = child_result.failure_envelope or {}
                raise SubWorkflowFailedError(
                    f"child {instance_id!r} of step {step_id!r} returned a terminal failure",
                    run_id=run_id,
                    step_id=step_id,
                    child_instance_id=instance_id,
                    iteration_key=key,
                    child_kind=_envelope_kind(envelope),
                )
            merged.append(dict(child_result.outputs))
        return merged

    def _merge_sub_workflow(
        self,
        step_id: str,
        run_id: str,
        instance_id: str,
        raw_result: Any,
    ) -> dict[str, dict[str, Any]]:
        """Normalise the child run output, failing on a non-success status.

        A sub-workflow child returns a :class:`RunOutput` envelope
        (rendered via ``RunOutput.to_dict``). Any status other than
        ``"succeeded"`` (a terminal failure, or the transitional
        ``"waiting"`` sentinel) propagates to the parent step as
        ``step.sub_workflow_failed``; on success the child run's
        per-step output bag is returned for the parent to bind under
        ``steps.<stepId>.outputs``.
        """
        envelope: Mapping[str, Any] = raw_result
        if envelope.get("status") != "succeeded":
            failure = envelope.get("failure_envelope") or {}
            raise SubWorkflowFailedError(
                f"sub-workflow child {instance_id!r} of step {step_id!r} "
                f"returned status {envelope.get('status')!r}",
                run_id=run_id,
                step_id=step_id,
                child_instance_id=instance_id,
                iteration_key=WORKFLOW_ITERATION_KEY,
                child_kind=_envelope_kind(failure),
            )
        outputs: Mapping[str, Mapping[str, Any]] = envelope.get("outputs") or {}
        return {sid: dict(out) for sid, out in outputs.items()}


# ---------------------------------------------------------------------------
# Module-private helpers
# ---------------------------------------------------------------------------


def _format_iso8601_duration(delta: timedelta) -> str:
    """Render a :class:`~datetime.timedelta` as an ISO-8601 duration.

    Produces the canonical ``P[nD][T[nH][nM][nS]]`` form, normalising
    whole-day spans into the date component (e.g. ``timedelta(hours=24)``
    → ``"P1D"``, ``timedelta(hours=23)`` → ``"PT23H"``). Used for the
    audit-facing ``timeout`` field of an
    :class:`~custos_workflow.steps.errors.ApprovalTimeoutError` when the
    gate timeout came from the platform-configured default rather than a
    document literal. Sub-second precision is dropped (the configured
    default is always whole seconds; see
    :func:`custos_workflow.providers._resolve_approval_default_timeout`,
    which rejects fractional values at the env boundary).
    """
    total_seconds = int(delta.total_seconds())
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    date_part = f"{days}D" if days else ""
    time_part = ""
    if hours:
        time_part += f"{hours}H"
    if minutes:
        time_part += f"{minutes}M"
    if seconds:
        time_part += f"{seconds}S"
    if time_part:
        time_part = "T" + time_part
    body = date_part + time_part
    return f"P{body}" if body else "PT0S"


def _resolve_node(graph: ExecutionGraph, step_id: str) -> ExecutionNode:
    """Return the node for ``step_id`` or raise :class:`KeyError`."""
    for node in graph.nodes:
        if node.step_id == step_id:
            return node
    raise KeyError(step_id)


def _child_graph(graph: ExecutionGraph, node: ExecutionNode) -> ExecutionGraph:
    """Build the byte-stable single-node graph the children run.

    The child runs exactly one node — the loop body — so it needs no
    edges and a trivial topological order. ``forEach`` / ``where:`` call
    sites on the node are inert in the child (the body handler dispatches
    on the structural kind and never re-expands the loop), so the node is
    carried through unchanged; the per-iteration item flows through
    :attr:`ChildStepInput.item` instead.
    """
    return ExecutionGraph(
        nodes=(node,),
        edges=(),
        topological_order=(node.step_id,),
        metadata=graph.metadata,
    )


def _envelope_kind(envelope: Mapping[str, Any]) -> str | None:
    """Extract the ``kind`` from a child failure envelope, if present."""
    kind = envelope.get("kind")
    return str(kind) if kind is not None else None


def _iteration_key_for_instance(
    spawned: list[tuple[str, str, Any]],
    instance_id: str | None,
) -> str | None:
    """Map a failed child's instance id back to its iteration key."""
    if instance_id is None:
        return None
    for key, candidate_id, _task in spawned:
        if candidate_id == instance_id:
            return key
    return None

"""``let:`` step handler for the Step Coordinator (WF-IMPL-052).

A ``let:`` step is the workflow author's way to compute a small,
pure data binding without scheduling an activity:

.. code-block:: yaml

    - id: derive
      let:
        critical_count: ${{ steps.scan.outputs.findings.size() }}
        is_critical: ${{ let.critical_count > 0 }}
        labels: ['critical', 'auto-derived']

The Run Controller's WF-IMPL-035 orchestrator dispatches every
:class:`~custos_workflow.graph.model.StepKind.LET` node through the
shared :class:`~custos_workflow.runs.StepHandler` Protocol. This
module is the dedicated handler that *actually* evaluates the
``let:`` block — it supersedes the placeholder branch in
:class:`~custos_workflow.runs.NoopStepHandler`, which returned an
empty :class:`StepSucceeded` regardless of the step's bindings.

Semantics (mirrored from ``design.md`` § ``let:`` Primitive):

* Each binding is evaluated **once**, in declared (insertion)
  order.
* Within the same block, later bindings observe earlier ones as
  ``let.<name>``. Once the block has been evaluated, downstream
  steps observe the same names under
  ``steps.<this_step_id>.outputs.<name>`` — the resolved bindings
  become the step's outputs.
* Non-string values pass through unchanged (numbers, bools, lists,
  dicts, ``None``) — the document model already accepts them as
  plain data.
* String values whose entire body is a single ``${{ ... }}``
  placeholder are evaluated by :func:`custos_cel.evaluate` against
  the per-binding :class:`BindingScope`. The raw CEL value is
  retained — preserving the value's CEL type so e.g.
  ``${{ steps.x.outputs.count }}`` lands as an ``int`` in the
  step's output mapping.
* Strings that contain no placeholder pass through as literal
  strings.
* Mixed-content strings (interleaved literals and placeholders) are
  rejected at compile time by
  :func:`custos_workflow.callsites.collect._collect_let_bindings`,
  so they can never reach this handler.

Any :class:`custos_cel.CelError` (parse / type / unbound-name /
timeout / evaluation) maps to a
``step.with_input_resolution_error`` envelope — by design ``let:``
errors share the taxonomy with ``with:`` errors because they're
semantically identical (CEL evaluation failure inside a step body).

The handler is **pure**: it performs no I/O, holds no state, and
receives every external dependency through its arguments. Two
``execute()`` calls under the same :class:`~custos_cel.FixedClock`
produce byte-equal outputs — the replay-determinism guarantee the
Dapr Workflow model requires.

Acceptance criteria (mirrored from #423):

* ``let.a + let.b`` reads ``a`` then ``b`` from the same-step
  overlay, not from ``steps.*.outputs``.
* Two :meth:`execute` calls under the same
  :class:`~custos_cel.FixedClock` produce byte-equal
  :class:`StepSucceeded.outputs`.
* 100 % coverage on this module.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

import custos_cel
from custos_cel.errors import CelError
from custos_cel.scope import BindingScope, RunInfo, StepBinding, WorkflowInfo

from custos_workflow.document.models import LetStep
from custos_workflow.graph.model import CallSiteKind, ExecutionGraph, ExecutionNode, StepKind
from custos_workflow.runs.step_handler import (
    StepExecutionContext,
    StepFailed,
    StepResult,
    StepSucceeded,
)
from custos_workflow.steps.errors import WithInputResolutionError

if TYPE_CHECKING:
    pass

__all__ = ["LetStepHandler"]


class LetStepHandler:
    """:class:`~custos_workflow.runs.StepHandler` for ``StepKind.LET``.

    Stateless dataclass-style class — instances are interchangeable
    and may be reused across runs. The single public method,
    :meth:`execute`, matches the
    :class:`~custos_workflow.runs.StepHandler` Protocol so the
    Step Coordinator dispatcher (WF-IMPL-055) can drop it in
    behind the same surface as
    :class:`~custos_workflow.runs.NoopStepHandler`.
    """

    def execute(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        step_id: str,
    ) -> StepResult:
        """Evaluate every binding in ``step_id``'s ``let:`` block.

        :param ctx: Per-dispatch context bundle. The handler reads
            :attr:`ctx.outputs` to seed ``steps.<id>.outputs``
            references, :attr:`ctx.run_id` / :attr:`ctx.workspace_id`
            for ``run.*`` references, and :attr:`ctx.clock` for both
            the BindingScope's ``now()`` callable and the CEL
            evaluator's deadline source.
        :param graph: The compiled execution graph. The handler reads
            :attr:`graph.metadata` for ``workflow.*`` references and
            looks up the target node via the
            :attr:`~custos_workflow.graph.model.ExecutionNode.step_id`.
        :param step_id: The id of the node to execute.

        :returns: :class:`StepSucceeded` carrying the resolved
            bindings as an immutable :class:`MappingProxyType`, OR
            :class:`StepFailed` carrying a
            ``step.with_input_resolution_error`` envelope when any
            binding's CEL expression raises.

        :raises KeyError: If ``step_id`` is not in ``graph.nodes``.
            Mirrors :class:`~custos_workflow.runs.NoopStepHandler`'s
            behaviour for unknown step ids.
        :raises NotImplementedError: If the resolved node is not a
            :class:`~custos_workflow.graph.model.StepKind.LET` step.
            Defensive guard — the dispatcher (WF-IMPL-055) is
            responsible for routing each kind to its dedicated
            handler, and a misrouted step is a programmer error.
        """
        node = _resolve_node(graph, step_id)
        if node.kind is not StepKind.LET:
            raise NotImplementedError(
                f"LetStepHandler.execute received {node.kind.value!r} step "
                f"{step_id!r}; only StepKind.LET is supported"
            )
        let_step = node.step_source
        if not isinstance(let_step, LetStep):  # pragma: no cover - defensive
            raise NotImplementedError(
                f"LetStepHandler.execute: node {step_id!r} has step_source "
                f"of type {type(let_step).__name__}, expected LetStep"
            )

        # Pre-build the parts of the BindingScope that are constant
        # across every binding in this block. The orchestrator
        # already deep-copies ctx.outputs so we can use it by
        # reference here without risk of mutation leakage.
        run_info = RunInfo(id=str(ctx.run_id), workspace=ctx.workspace_id)
        workflow_info = WorkflowInfo(
            name=graph.metadata.workflow_name,
            # ``document_api_version`` is the closest stable workflow
            # version identifier exposed on the compiled graph. The
            # real ``workflowVersionId`` UUID is carried on
            # ``RunInput`` (and so on the workflow context's input),
            # but the StepHandler Protocol does not surface it today;
            # WF-IMPL-055 may widen the surface so this falls back to
            # the published version pin. For now this is good enough
            # for ``workflow.name`` / ``workflow.version`` references.
            version=graph.metadata.document_api_version,
        )
        steps_view: Mapping[str, StepBinding] = MappingProxyType(
            {sid: StepBinding(out, sealed=True) for sid, out in ctx.outputs.items()}
        )

        overlay: dict[str, Any] = {}
        for name, value in let_step.let.items():
            try:
                overlay[name] = _resolve_binding(
                    node=node,
                    name=name,
                    value=value,
                    run_info=run_info,
                    workflow_info=workflow_info,
                    steps_view=steps_view,
                    overlay=overlay,
                    ctx=ctx,
                )
            except CelError as exc:
                # ``let:`` errors share the
                # ``step.with_input_resolution_error`` taxonomy with
                # ``with:`` errors — both are CEL-evaluation failures
                # inside a step body, and downstream audit consumers
                # already dispatch on that kind.
                err = WithInputResolutionError(
                    f"failed to evaluate let:{name!r} on step {step_id!r}: {exc}",
                    run_id=str(ctx.run_id),
                    step_id=step_id,
                    attempt=None,
                    binding_name=name,
                    cause_kind=exc.kind,
                    source=_source_for(node, name),
                )
                return StepFailed(envelope=MappingProxyType(err.to_dict()))

        return StepSucceeded(outputs=MappingProxyType(overlay))


# ---------------------------------------------------------------------------
# Module-private helpers
# ---------------------------------------------------------------------------


def _resolve_node(graph: ExecutionGraph, step_id: str) -> ExecutionNode:
    """Return the node for ``step_id`` or raise :class:`KeyError`.

    Mirrors the lookup pattern :class:`NoopStepHandler` uses today
    so dispatch behaviour stays uniform across handlers.
    """
    for node in graph.nodes:
        if node.step_id == step_id:
            return node
    raise KeyError(step_id)


def _resolve_binding(
    *,
    node: ExecutionNode,
    name: str,
    value: Any,
    run_info: RunInfo,
    workflow_info: WorkflowInfo,
    steps_view: Mapping[str, StepBinding],
    overlay: Mapping[str, Any],
    ctx: StepExecutionContext,
) -> Any:
    """Evaluate one ``let:`` binding against the current overlay."""
    # Non-string values are pure data (numbers, bools, dicts,
    # lists, None) — the document model already accepts them.
    if not isinstance(value, str):
        return value

    slot_label = f"let.{name}"
    call_site = node.call_sites.get(slot_label)
    if call_site is None:
        # Plain literal string (no ``${{ ... }}`` placeholder). The
        # compile-time collector deliberately omits a call site for
        # these; the value passes through as a literal.
        return value

    # Defensive guard against a slot-label collision (impossible
    # today — the collector always emits ``LET`` kind here — but
    # cheap to assert and useful if a future kind reuses the
    # ``let.`` prefix).
    if call_site.kind is not CallSiteKind.LET:  # pragma: no cover - defensive
        raise NotImplementedError(
            f"LetStepHandler: slot {slot_label!r} on step {node.step_id!r} "
            f"has unexpected kind {call_site.kind.value!r}"
        )

    scope = BindingScope(
        run=run_info,
        workflow=workflow_info,
        now=ctx.clock.now,
        # ``inputs`` is not surfaced through the StepHandler
        # Protocol today; the dispatcher (WF-IMPL-055) may widen
        # the bundle later. For now, ``let:`` expressions that
        # reference ``inputs.*`` raise an UnboundNameError, which
        # the caller catches and surfaces as
        # ``step.with_input_resolution_error``.
        inputs={},
        steps=steps_view,
        let=overlay,
    )
    return custos_cel.evaluate(call_site.typed_ast, scope, ctx.clock)


def _source_for(node: ExecutionNode, binding_name: str) -> str | None:
    """Return the original CEL source for a ``let:`` binding, if any."""
    call_site = node.call_sites.get(f"let.{binding_name}")
    return call_site.source if call_site is not None else None

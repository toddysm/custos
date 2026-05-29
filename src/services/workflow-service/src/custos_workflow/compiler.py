"""Definition Compiler driver (WF-IMPL-021).

The public :func:`compile` entry point orchestrates the full
parse → type-check → topology → typed-AST caching pipeline,
producing the :class:`~custos_workflow.graph.ExecutionGraph` the Run
Controller persists on ``Run.compiledGraph`` at ``StartRun`` time.

The pipeline mirrors design.md § Internal Structure (Definition
Compiler row):

1. **Collect call sites** — :func:`collect_call_sites` walks the
   document and returns one untyped AST per
   ``${{ ... }}`` placeholder.
2. **Derive bindings** — :func:`derive_bindings` produces the
   :class:`~custos_cel.SchemaBindings` each step sees, including the
   permissive object schema for sub-workflow outputs (the
   :func:`derive_bindings` function emits a structured warning when
   it does so).
3. **Validate step references** — :func:`validate_step_refs`
   pre-flights the untyped call-site ASTs so forward / unknown /
   self ``steps.X.outputs.*`` references surface as
   :class:`TopologyCompileError`. Without this pass the type checker
   would reject those references as ``expression.unbound_name``
   first — the per-step bindings only expose prior steps — and the
   caller would see a structurally wrong "name not bound"
   diagnostic.
4. **Type-check** — every untyped call site is lifted to a
   :class:`~custos_workflow.graph.TypedCallSite` via
   :func:`custos_cel.type_check`. Errors **accumulate across the
   stage** so callers see every type problem at once rather than
   fix-then-recompile.
5. **Topology** — explicit ``needs:`` edges and implicit
   ``${{ steps.X.outputs.* }}`` edges are collected, **deduplicated
   by ``(from_step, to_step)`` pair** (explicit needs wins so the
   compiled edge carries the author's intent), cycles are detected,
   and a stable topological order is produced.
6. **Retry + on-error resolution** — per-step policies are translated
   from the document model into the wire-stable resolved shapes the
   :class:`~custos_workflow.graph.ExecutionNode` carries. The full
   precedence overlay and route taxonomy land in WF-IMPL-022 /
   WF-IMPL-023; today the resolvers are deliberately conservative
   pass-throughs so the compiled graph remains well-formed.
7. **Assemble graph** — the
   :class:`~custos_workflow.graph.ExecutionGraph` is built from
   :class:`~custos_workflow.graph.ExecutionNode` instances in
   topological order plus the canonicalised edge tuple.

A more detailed structured error taxonomy (``CompileError`` →
``ParseError`` / ``BindingsError`` / ``TypeCheckError`` /
``TopologyError`` / ``UnknownActivityError`` with stable ``kind``
strings) lands in WF-IMPL-024; the subclass set in this module is the
minimum shape that lets callers branch on the *stage* that failed.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from custos_cel import CelError
from custos_cel import type_check as cel_type_check

from custos_workflow.bindings import (
    ActivityTypeNotFoundError,
    derive_bindings,
)
from custos_workflow.callsites import (
    CallSiteParseError,
    collect_call_sites,
)
from custos_workflow.document import (
    ActivityStep,
    LetStep,
    OnErrorArm,
    RetryPolicy,
    Step,
    WorkflowDocument,
    WorkflowStep,
)
from custos_workflow.graph import (
    BackoffStrategyTag,
    Edge,
    ExecutionGraph,
    ExecutionNode,
    GraphMetadata,
    JitterStrategyTag,
    OnErrorActionTag,
    OnErrorRoute,
    PrimitiveHandler,
    ResolvedBackoffPolicy,
    ResolvedRetryPolicy,
    StepKind,
    TopologyError,
    TypedCallSite,
    collect_data_dependencies,
    collect_explicit_edges,
    detect_cycles,
    topological_sort,
    validate_step_refs,
)

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime

    from custos_cel import SchemaBindings

    from custos_workflow.bindings import ActivityTypeRegistry
    from custos_workflow.callsites import CallSite


__all__ = [
    "BindingsCompileError",
    "CallSiteCompileError",
    "CompileError",
    "RunMeta",
    "TopologyCompileError",
    "TypeCheckCompileError",
    "TypeCheckFailure",
    "compile",
]


# ---------------------------------------------------------------------------
# Public data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RunMeta:
    """Run-scoped metadata fed into the compile pipeline.

    Concrete clocks and identity resolution are the Run Controller's
    concern at execute time; the compiler only needs the typing
    defaults so the type checker can resolve identifiers like
    ``workflow.name`` and the ``now()`` built-in.

    Attributes:
        workspace_id: The tenant-scoped workspace owning the run.
        workflow_version_id: The Catalog
            ``WorkflowVersion`` UUID being compiled.
        workflow_name: Denormalised workflow name (used for
            ``workflow.name`` in CEL expressions).
        workflow_version_label: Human-readable workflow version label
            (used for ``workflow.version`` in CEL expressions).
        started_at_default: The default ``now()`` timestamp used by
            the type checker. The runtime supplies a real clock at
            evaluation time; this only feeds typing defaults.
    """

    workspace_id: str
    workflow_version_id: str
    workflow_name: str
    workflow_version_label: str
    started_at_default: datetime


@dataclass(frozen=True, slots=True)
class TypeCheckFailure:
    """One per-call-site type-check failure.

    Carried inside :class:`TypeCheckCompileError`'s ``errors`` list so
    the caller can render every diagnostic at once.

    Attributes:
        step_id: The step id whose call site failed.
        path: The collector-assigned dict-key path inside the step
            (e.g. ``"if"``, ``"let.severity"``, ``"with.image"``).
        source: The original ``${{ ... }}`` token from the document.
        message: The :class:`custos_cel.CelError` message verbatim.
        kind: The :class:`custos_cel.CelError.kind` string (e.g.
            ``"expression.type_error"`` / ``"expression.unbound_name"``)
            for callers that want to bucket diagnostics.
    """

    step_id: str
    path: str
    source: str
    message: str
    kind: str


# ---------------------------------------------------------------------------
# Error taxonomy
# ---------------------------------------------------------------------------


class CompileError(ValueError):
    """Top-level compiler failure.

    WF-IMPL-024 will replace this hierarchy with the structured
    workflow-service error taxonomy (stable ``kind`` strings,
    locator envelopes). Until then the subclasses below pin only
    the failing pipeline stage so callers can branch without
    parsing messages.
    """


class CallSiteCompileError(CompileError):
    """Stage 1 failure — at least one ``${{ ... }}`` did not parse."""


class BindingsCompileError(CompileError):
    """Stage 2 failure — schema bindings could not be derived.

    The most common cause today is an
    :class:`~custos_workflow.bindings.ActivityTypeNotFoundError` for
    an activity ref that the registry does not know.
    """


class TypeCheckCompileError(CompileError):
    """Stage 3 failure — at least one call site did not type-check.

    Carries the per-call-site error list on :attr:`errors` so callers
    can render every diagnostic at once rather than fix-then-recompile.
    """

    def __init__(self, errors: list[TypeCheckFailure]) -> None:
        self.errors: list[TypeCheckFailure] = errors
        lines = [f"  - step {f.step_id!r} at {f.path!r} ({f.kind}): {f.message}" for f in errors]
        super().__init__(
            f"compile: type-check stage produced {len(errors)} error(s):\n" + "\n".join(lines)
        )


class TopologyCompileError(CompileError):
    """Stage 4 failure — explicit/implicit edges or topology rejected.

    Wraps :class:`~custos_workflow.graph.TopologyError` raised by the
    edge collectors, cycle detector, or topological sorter. Cycle
    diagnostics include the offending step ids in declaration order.
    """


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def compile(
    document: WorkflowDocument,
    run_meta: RunMeta,
    registry: ActivityTypeRegistry,
    *,
    logger: logging.Logger | None = None,
) -> ExecutionGraph:
    """Compile a parsed :class:`WorkflowDocument` to an :class:`ExecutionGraph`.

    Args:
        document: A pre-parsed :class:`WorkflowDocument`. Wire-level
            schema validation has already happened in
            :func:`~custos_workflow.document.parse_document`; this
            entry point does not re-validate the wire shape.
        run_meta: Run-scoped metadata. Reserved for future stages
            that need run identity at compile time (e.g. workspace
            scope checks on activity refs); the current pipeline
            only carries it through so callers do not have to
            refactor when WF-IMPL-022+ start consuming it.
        registry: Read-only catalog of activity types — used by
            :func:`derive_bindings` to resolve each
            :class:`ActivityStep`'s output schema for type-checking
            downstream ``steps.X.outputs.*`` access.
        logger: Optional logger forwarded to :func:`derive_bindings`
            for the structured ``binding.unresolved_sub_workflow``
            warning the permissive sub-workflow stub emits. Defaults
            to the module-level logger.

    Returns:
        A frozen :class:`ExecutionGraph` ready to be persisted on
        ``Run.compiledGraph``.

    Raises:
        CallSiteCompileError: any ``${{ ... }}`` failed parsing.
        BindingsCompileError: an activity ref was not in the
            registry.
        TypeCheckCompileError: at least one call site failed type
            checking. The exception carries every per-call-site
            failure so the caller can render a full report.
        TopologyCompileError: cycle detected, forward reference,
            unknown step id, or duplicate step id in the graph.
    """
    log = logger if logger is not None else logging.getLogger(__name__)

    # ``run_meta`` is reserved for future stages (WF-IMPL-022+); the
    # parameter stays on the public surface today so callers do not
    # have to refactor once it lights up.
    _ = run_meta

    # ---- Stage 1: collect untyped call sites --------------------------
    try:
        untyped_by_step = collect_call_sites(document)
    except CallSiteParseError as exc:
        raise CallSiteCompileError(
            f"compile: failed to parse call site at step {exc.step_id!r}/{exc.path!r}: {exc}",
        ) from exc

    # ---- Stage 2: derive per-step schema bindings ---------------------
    try:
        bindings_by_step = derive_bindings(document, registry, logger=log)
    except ActivityTypeNotFoundError as exc:
        raise BindingsCompileError(
            f"compile: activity type {exc.activity_ref!r} is not "
            "registered (cannot derive its output schema for "
            "type-checking)",
        ) from exc

    # ---- Stage 2.5: surface graph-shape step refs as topology errors --
    # Without this pre-pass, a CEL reference like
    # ``steps.later_step.outputs.x`` (forward), ``steps.ghost.outputs.x``
    # (unknown), or a step's own ``steps.self.outputs.x`` (self) would
    # surface as ``expression.unbound_name`` type-check failures
    # because :func:`derive_bindings` only exposes prior steps. Those
    # diagnostics are structurally wrong — they are graph-shape
    # problems, not type problems — so we validate first and translate
    # to :class:`TopologyCompileError` before the type checker runs.
    try:
        validate_step_refs(
            document,
            (
                (step_id, site.position.document_path, site.parsed_ast)
                for step_id, sites in untyped_by_step.items()
                for site in sites
            ),
        )
    except TopologyError as exc:
        raise TopologyCompileError(
            f"compile: topology stage rejected the graph: {exc}",
        ) from exc

    # ---- Stage 3: type-check call sites -------------------------------
    typed_by_step, node_call_sites, failures = _type_check_all(
        untyped_by_step,
        bindings_by_step,
    )
    if failures:
        raise TypeCheckCompileError(failures)

    # ---- Stage 4: build graph topology --------------------------------
    try:
        explicit_edges = collect_explicit_edges(document)
        implicit_edges = collect_data_dependencies(document, typed_by_step)
        # Dedupe ``(from_step, to_step)`` pairs across the explicit
        # and implicit edge lists. When the author both writes
        # ``needs: [X]`` and references ``steps.X.outputs.*``, the
        # explicit edge wins because it carries the author's intent
        # — downstream schedulers process the prerequisite once,
        # tagged as EXPLICIT_NEEDS rather than DATA_DEPENDENCY.
        edge_by_pair: dict[tuple[str, str], Edge] = {}
        for edge in explicit_edges:
            edge_by_pair[(edge.from_step, edge.to_step)] = edge
        for edge in implicit_edges:
            edge_by_pair.setdefault((edge.from_step, edge.to_step), edge)
        all_edges: list[Edge] = list(edge_by_pair.values())
        cycles = detect_cycles(all_edges)
        if cycles:
            cyc_repr = "; ".join(" -> ".join(c) for c in cycles)
            raise TopologyCompileError(
                f"compile: graph contains cycle(s): {cyc_repr}",
            )
        step_ids = [s.id for s in document.spec.steps]
        topological_order = topological_sort(step_ids, all_edges)
    except TopologyError as exc:
        raise TopologyCompileError(
            f"compile: topology stage rejected the graph: {exc}",
        ) from exc

    # ---- Stage 5+6: assemble ExecutionGraph ---------------------------
    by_id: dict[str, Step] = {s.id: s for s in document.spec.steps}
    nodes = tuple(_build_node(by_id[sid], node_call_sites[sid]) for sid in topological_order)
    return ExecutionGraph(
        nodes=nodes,
        edges=tuple(all_edges),
        topological_order=topological_order,
        metadata=GraphMetadata(
            workflow_name=document.metadata.name,
            workflow_workspace=document.metadata.workspace,
            document_api_version=document.api_version,
        ),
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _type_check_all(
    untyped_by_step: Mapping[str, list[CallSite]],
    bindings_by_step: Mapping[str, SchemaBindings],
) -> tuple[
    dict[str, list[TypedCallSite]],
    dict[str, dict[str, TypedCallSite]],
    list[TypeCheckFailure],
]:
    """Type-check every call site, accumulating per-stage errors.

    Returns three parallel views over the same typed call sites:

    - ``typed_by_step`` — ``{step_id: [TypedCallSite, ...]}`` used by
      :func:`collect_data_dependencies` (which only needs the AST and
      the ``document_path`` breadcrumb).
    - ``node_call_sites`` — ``{step_id: {path: TypedCallSite}}`` used
      to populate :attr:`ExecutionNode.call_sites`. The inner key is
      the collector-assigned :attr:`CallSite.path` so per-step access
      is stable across compiles.
    - ``failures`` — every :class:`CelError` raised by the type
      checker, tagged with locator metadata. If non-empty, the
      caller raises :class:`TypeCheckCompileError`.
    """
    typed_by_step: dict[str, list[TypedCallSite]] = {}
    node_call_sites: dict[str, dict[str, TypedCallSite]] = {}
    failures: list[TypeCheckFailure] = []
    for step_id, sites in untyped_by_step.items():
        typed_list: list[TypedCallSite] = []
        typed_map: dict[str, TypedCallSite] = {}
        bindings = bindings_by_step[step_id]
        for site in sites:
            try:
                typed_ast = cel_type_check(site.parsed_ast, bindings)
            except CelError as exc:
                failures.append(
                    TypeCheckFailure(
                        step_id=step_id,
                        path=site.path,
                        source=site.source,
                        message=str(exc),
                        kind=exc.kind,
                    ),
                )
                continue
            tcs = TypedCallSite(
                source=site.source,
                typed_ast=typed_ast,
                kind=site.kind,
                document_path=site.position.document_path,
            )
            typed_list.append(tcs)
            typed_map[site.path] = tcs
        typed_by_step[step_id] = typed_list
        node_call_sites[step_id] = typed_map
    return typed_by_step, node_call_sites, failures


_STEP_DISPATCH: dict[type[Step], tuple[StepKind, PrimitiveHandler]] = {
    ActivityStep: (StepKind.ACTIVITY, PrimitiveHandler.ACTIVITY_RUNTIME),
    LetStep: (StepKind.LET, PrimitiveHandler.EXPRESSION_INLINE),
    WorkflowStep: (StepKind.WORKFLOW, PrimitiveHandler.SUB_ORCHESTRATION),
}


def _build_node(
    step: Step,
    typed_call_sites: dict[str, TypedCallSite],
) -> ExecutionNode:
    """Assemble one :class:`ExecutionNode` from a source step + its typed sites."""
    step_kind, handler = _STEP_DISPATCH[type(step)]
    retry_policy = _resolve_retry_policy(step.retry) if step.retry else None
    on_error_routes = tuple(_resolve_on_error_route(a) for a in step.on_error or ())
    return ExecutionNode(
        step_id=step.id,
        kind=step_kind,
        primitive_handler=handler,
        retry_policy=retry_policy,
        on_error_routes=on_error_routes,
        call_sites=typed_call_sites,
        step_source=step,
    )


# ---------------------------------------------------------------------------
# WF-IMPL-022 / WF-IMPL-023 stubs
# ---------------------------------------------------------------------------
#
# Both resolvers below are deliberately conservative pass-throughs.
# The structured precedence overlay (per-match → step →
# ``spec.defaults`` → platform overlay) and the full on_error route
# taxonomy (implicit policy synthesis, cancelled short-circuit,
# disallowed-kind rejection) land in WF-IMPL-022 and WF-IMPL-023
# respectively. Until then the helpers fill any field the document
# omits with safe defaults so the compiled :class:`ExecutionGraph`
# remains well-formed at this milestone.

#: Platform-default backoff curve fed in for any field the
#: document does not pin. Replaced wholesale by WF-IMPL-022's
#: real precedence overlay.
_DEFAULT_BACKOFF = ResolvedBackoffPolicy(
    strategy=BackoffStrategyTag.EXPONENTIAL,
    initial_delay_ms=100,
    max_delay_ms=30_000,
    multiplier=2.0,
)


def _resolve_retry_policy(policy: RetryPolicy) -> ResolvedRetryPolicy:
    """STUB resolver for WF-IMPL-022.

    Passes ``max_attempts``, ``jitter`` and ``respect_retry_after``
    through from the document where set; substitutes the
    :data:`_DEFAULT_BACKOFF` curve unconditionally (parsing
    duration strings like ``"100ms"`` and merging the per-match →
    step → defaults precedence chain are WF-IMPL-022's job).
    """
    return ResolvedRetryPolicy(
        max_attempts=policy.max_attempts if policy.max_attempts is not None else 3,
        backoff=_DEFAULT_BACKOFF,
        jitter=(
            JitterStrategyTag(policy.jitter.value)
            if policy.jitter is not None
            else JitterStrategyTag.FULL
        ),
        respect_retry_after=(
            policy.respect_retry_after if policy.respect_retry_after is not None else True
        ),
    )


def _resolve_on_error_route(arm: OnErrorArm) -> OnErrorRoute:
    """STUB resolver for WF-IMPL-023.

    Maps one :class:`OnErrorArm` 1:1 onto an
    :class:`OnErrorRoute`. The implicit-policy synthesis,
    cancelled short-circuit, and disallowed-kind rejection
    (design.md § Implicit on_error policy) land in WF-IMPL-023.
    """
    return OnErrorRoute(
        action=OnErrorActionTag(arm.do.value),
        code=arm.match.code,
        code_prefix=arm.match.code_prefix,
        cls=arm.match.cls,
        retry=_resolve_retry_policy(arm.retry) if arm.retry is not None else None,
    )

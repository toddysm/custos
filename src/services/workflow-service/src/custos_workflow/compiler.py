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

The structured error taxonomy lives in
:mod:`custos_workflow.errors` (WF-IMPL-024, issue #358). This module
re-exports the legacy stage-named subclasses
(``CallSiteCompileError`` / ``BindingsCompileError`` /
``TypeCheckCompileError`` / ``TopologyCompileError`` /
``RetryPolicyCompileError``) which are now thin specializations of
the canonical structured classes (:class:`CompileParseError` /
:class:`CompileTypeError` / :class:`CompileTopologyError` /
:class:`CompileRetryPolicyError`) and carry the locked
``compile.*`` ``kind`` strings.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

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
    Step,
    WorkflowDocument,
    WorkflowStep,
)
from custos_workflow.errors import (
    CompileError,
    CompileParseError,
    CompileRetryPolicyError,
    CompileTopologyError,
    CompileTypeError,
)
from custos_workflow.graph import (
    Edge,
    ExecutionGraph,
    ExecutionNode,
    GraphMetadata,
    PrimitiveHandler,
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
from custos_workflow.on_error import compile_on_error
from custos_workflow.retry import (
    RetryResolutionError,
    resolve_step_retry,
)

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime

    from custos_cel import SchemaBindings

    from custos_workflow.bindings import ActivityTypeRegistry
    from custos_workflow.callsites import CallSite
    from custos_workflow.document import Defaults


__all__ = [
    "BindingsCompileError",
    "CallSiteCompileError",
    "CompileError",
    "CompileParseError",
    "CompileRetryPolicyError",
    "CompileTopologyError",
    "CompileTypeError",
    "RetryPolicyCompileError",
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


class CallSiteCompileError(CompileParseError):
    """Stage 1 failure — at least one ``${{ ... }}`` did not parse.

    Specialisation of :class:`CompileParseError` (``kind =
    "compile.parse_error"``) used by the compiler's call-site
    collection stage. Callers can either ``except
    CompileParseError:`` (canonical, prefer this) or ``except
    CallSiteCompileError:`` (legacy, kept for backwards
    compatibility).
    """


class BindingsCompileError(CompileError, ValueError):
    """Stage 2 failure — schema bindings could not be derived.

    The most common cause today is an
    :class:`~custos_workflow.bindings.ActivityTypeNotFoundError` for
    an activity ref that the registry does not know. The bindings
    stage is not one of the four canonical compile-time failures
    (parse / type / topology / retry-policy) so it carries its own
    ``compile.bindings_error`` ``kind`` string; ``isinstance(...,
    CompileError)`` still holds.
    """

    KIND: Final[str] = "compile.bindings_error"  # type: ignore[misc]


class TypeCheckCompileError(CompileTypeError):
    """Stage 3 failure — at least one call site did not type-check.

    Aggregating specialisation of :class:`CompileTypeError`. The
    canonical class carries a single ``step_id`` /
    ``call_site_path`` per the WF-IMPL-024 taxonomy; this subclass
    additionally exposes :attr:`errors` (the full per-call-site
    failure list) so callers can render every diagnostic at once
    rather than fix-then-recompile. The first failure's
    ``step_id`` / ``call_site_path`` are surfaced through the
    canonical fields so audit consumers do not need to special-case
    the aggregator.
    """

    def __init__(self, errors: list[TypeCheckFailure]) -> None:
        self.errors: list[TypeCheckFailure] = errors
        lines = [f"  - step {f.step_id!r} at {f.path!r} ({f.kind}): {f.message}" for f in errors]
        message = f"compile: type-check stage produced {len(errors)} error(s):\n" + "\n".join(lines)
        primary = errors[0] if errors else None
        super().__init__(
            message,
            step_id=primary.step_id if primary is not None else None,
            call_site_path=primary.path if primary is not None else None,
        )

    def _extra_fields(self) -> dict[str, Any]:
        extras = super()._extra_fields()
        extras["errors"] = [
            {
                "step_id": f.step_id,
                "path": f.path,
                "source": f.source,
                "message": f.message,
                "kind": f.kind,
            }
            for f in self.errors
        ]
        return extras


class TopologyCompileError(CompileTopologyError):
    """Stage 4 failure — explicit/implicit edges or topology rejected.

    Specialisation of :class:`CompileTopologyError` (``kind =
    "compile.topology_error"``). Wraps
    :class:`~custos_workflow.graph.TopologyError` raised by the
    edge collectors, cycle detector, or topological sorter. Cycle
    diagnostics include the offending step ids in declaration
    order on the canonical :attr:`cycle` field.
    """


class RetryPolicyCompileError(CompileRetryPolicyError):
    """Stage 5 failure — retry-policy overlay produced an invalid policy.

    Specialisation of :class:`CompileRetryPolicyError` (``kind =
    "compile.retry_policy_error"``). Wraps
    :class:`~custos_workflow.retry.RetryResolutionError` raised by
    :func:`~custos_workflow.retry.resolve_step_retry` /
    :func:`~custos_workflow.retry.resolve_arm_retry` when a layered
    retry policy contains a malformed ISO-8601 duration, a backoff
    with ``maxDelay < initialDelay``, or an ``on_error[]`` arm
    whose inline ``maxAttempts:`` shorthand disagrees with its
    structured ``retry: { maxAttempts: ... }`` value. The Catalog
    publish-time validator should catch every one of these, so
    seeing this at compile time means a document slipped past
    validation.
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
        RetryPolicyCompileError: a layered retry policy is invalid
            (malformed duration, ``maxDelay < initialDelay``, or
            conflicting ``maxAttempts:`` shorthand vs structured).
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
        # ``CallSiteParseError.__cause__`` carries the original
        # :class:`custos_cel.CelError` raised by the parser. Forward
        # it as the structured ``cause`` so
        # ``CompileParseError.to_dict()["cause"]`` preserves the
        # underlying ``kind`` / ``message`` for audit correlation
        # (the canonical contract documented in
        # :mod:`custos_workflow.errors`).
        cel_cause = exc.__cause__ if isinstance(exc.__cause__, CelError) else None
        raise CallSiteCompileError(
            f"compile: failed to parse call site at step {exc.step_id!r}/{exc.path!r}: {exc}",
            step_id=exc.step_id,
            call_site_path=exc.path,
            cause=cel_cause,
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
                cycle=tuple(cycles[0]),
            )
        step_ids = [s.id for s in document.spec.steps]
        topological_order = topological_sort(step_ids, all_edges)
    except TopologyError as exc:
        raise TopologyCompileError(
            f"compile: topology stage rejected the graph: {exc}",
        ) from exc

    # ---- Stage 5+6: assemble ExecutionGraph ---------------------------
    by_id: dict[str, Step] = {s.id: s for s in document.spec.steps}
    spec_defaults = document.spec.defaults
    try:
        nodes = tuple(
            _build_node(by_id[sid], node_call_sites[sid], spec_defaults)
            for sid in topological_order
        )
    except RetryResolutionError as exc:
        raise RetryPolicyCompileError(
            f"compile: retry-policy resolver rejected the graph: {exc}",
            reason=str(exc),
        ) from exc
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
    spec_defaults: Defaults | None,
) -> ExecutionNode:
    """Assemble one :class:`ExecutionNode` from a source step + its typed sites.

    The step-level :class:`ResolvedRetryPolicy` is computed once
    here (layers: ``step.retry`` → ``spec.defaults.retry`` →
    platform defaults — see :func:`resolve_step_retry`) and then
    handed to :func:`compile_on_error`, which both folds the
    per-arm overlay for any declared ``on_error`` arms and
    synthesises the implicit fallback routes documented in
    design.md § Implicit ``on_error`` policy.

    A non-activity step keeps ``retry_policy=None`` and is rejected
    by :func:`compile_on_error` if it carries a ``retry:`` or
    ``on_error:`` block (design.md § Retry Policy → § Where
    ``retry:`` may appear).
    """
    step_kind, handler = _STEP_DISPATCH[type(step)]
    retry_policy: ResolvedRetryPolicy | None
    if isinstance(step, ActivityStep):
        retry_policy = resolve_step_retry(step.retry, spec_defaults)
    else:
        retry_policy = None
    on_error_routes = compile_on_error(step, retry_policy)
    return ExecutionNode(
        step_id=step.id,
        kind=step_kind,
        primitive_handler=handler,
        retry_policy=retry_policy,
        on_error_routes=on_error_routes,
        call_sites=typed_call_sites,
        step_source=step,
    )

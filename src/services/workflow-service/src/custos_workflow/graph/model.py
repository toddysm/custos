"""Frozen dataclasses for the :class:`ExecutionGraph` data model.

Every dataclass is ``@dataclass(frozen=True, slots=True)`` so the
compiled graph is structurally immutable — Dapr Workflow replay relies
on the graph being byte-identical across pod restarts, and frozen
instances prevent accidental mutation during replay (design.md §
Replay-safe Immutability).

Equality is structural (dataclass-generated ``__eq__``) so
``from_json(to_json(g)) == g`` is a meaningful round-trip property.
``tuple`` is used everywhere instead of ``list`` so the structures are
hashable and deeply ``==``-comparable without surprise; ``Mapping``
fields use :class:`types.MappingProxyType` snapshots constructed in
``__post_init__``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from custos_cel import TypedAST

    from custos_workflow.document import Step


# ---------------------------------------------------------------------------
# Enum tags
# ---------------------------------------------------------------------------


class StepKind(StrEnum):
    """The structural step kind, mirroring the YAML keyword.

    Today's :class:`~custos_workflow.document.Step` discriminated
    union covers exactly these five kinds. The design lists more
    primitives (``parallel`` / ``waitFor``) that the wire schema
    does not yet expose; they will land as additional members here
    without disturbing the JSON envelope because :class:`StrEnum`
    values are forward-compatible.

    :attr:`WAIT` is the one kind the Run Controller orchestrator
    handles inline (no Step Coordinator dispatch): it issues a
    Dapr durable timer for the ISO-8601 duration carried on
    :attr:`~custos_workflow.document.WaitStep.wait`.

    :attr:`APPROVAL` is a human-in-the-loop gate (ADR-007). The
    Sub-Orchestration Manager spawns one child workflow instance
    that awaits an approval signal via ``wait_for_external_event``
    with a durable timeout timer — so it maps to
    :attr:`PrimitiveHandler.SUB_ORCHESTRATION`, the same handler as
    :attr:`WORKFLOW` and any ``forEach``-bearing loop step.
    """

    ACTIVITY = "activity"
    LET = "let"
    WORKFLOW = "workflow"
    WAIT = "wait"
    APPROVAL = "approval"


class PrimitiveHandler(StrEnum):
    """Maps a step kind to the Step Coordinator handler that drives it.

    Mirrors the *Handler* column of design.md § Workflow Schema: Step
    Kinds Handled. The Step Coordinator dispatches strictly off this
    tag — we resolve it at compile time so each step's handler is
    durable, not re-derived on every replay.

    :attr:`RUN_CONTROLLER_TIMER` is the sentinel for the one kind
    (:attr:`StepKind.WAIT`) that the Run Controller orchestrator
    handles directly via :meth:`~dapr.ext.workflow.DaprWorkflowContext.create_timer`;
    no Step Coordinator handler is invoked for these nodes.
    """

    ACTIVITY_RUNTIME = "activity_runtime"
    EXPRESSION_INLINE = "expression_inline"
    SUB_ORCHESTRATION = "sub_orchestration"
    RUN_CONTROLLER_TIMER = "run_controller_timer"


class EdgeKind(StrEnum):
    """Why an edge exists in the graph.

    - ``EXPLICIT_NEEDS`` — declared by a future ``needs:`` field
      (reserved; the v1 wire schema has none, but the slot keeps the
      enum closed for WF-IMPL-019 to fill).
    - ``DATA_DEPENDENCY`` — derived from a ``${{ steps.X.outputs.* }}``
      reference inside a CEL expression.
    - ``CONTROL_FLOW`` — derived from sequential ordering in
      ``spec.steps`` (the fallback when no other edge type fires).
    """

    EXPLICIT_NEEDS = "explicit_needs"
    DATA_DEPENDENCY = "data_dependency"
    CONTROL_FLOW = "control_flow"


class CallSiteKind(StrEnum):
    """Where in the step a CEL expression slot lives.

    Mirrors the set the design exposes as expression slots. The
    Definition Compiler (WF-IMPL-020) attaches one
    :class:`TypedCallSite` per slot per step.
    """

    IF = "if"
    WHEN = "when"
    UNLESS = "unless"
    WITH = "with"
    FOR_EACH = "for"
    WHERE = "where"
    LET = "let"
    #: Reserved for compiler-internal use (e.g. a synthetic placeholder
    #: used while wiring partial graphs in tests). Not emitted by the
    #: real call-site collector.
    PLACEHOLDER = "placeholder"


class BackoffStrategyTag(StrEnum):
    """Compiled backoff strategy. Mirrors the document enum."""

    CONSTANT = "constant"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"


class JitterStrategyTag(StrEnum):
    """Compiled jitter strategy. Mirrors the document enum."""

    NONE = "none"
    FULL = "full"
    EQUAL = "equal"
    DECORRELATED = "decorrelated"


class OnErrorActionTag(StrEnum):
    """Compiled on-error action. Mirrors the document enum."""

    SKIP = "skip"
    RETRY = "retry"
    FAIL = "fail"


# ---------------------------------------------------------------------------
# Retry / on-error structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ResolvedBackoffPolicy:
    """The fully-resolved backoff portion of a step's retry policy.

    ``initial_delay_ms`` and ``max_delay_ms`` are integer milliseconds
    (not ISO-8601 strings) so the runtime never re-parses durations
    inside the Step Coordinator hot path.
    """

    strategy: BackoffStrategyTag
    initial_delay_ms: int
    max_delay_ms: int
    multiplier: float


@dataclass(frozen=True, slots=True)
class ResolvedRetryPolicy:
    """The fully-resolved retry policy that the Step Coordinator applies.

    "Resolved" means defaults, step-level overrides, and per-match
    ``on_error[].retry`` overrides have all been merged into a single
    flat policy — that resolution happens in WF-IMPL-022, but the
    shape it writes is fixed here so the JSON envelope is stable
    across the implementation gap.
    """

    max_attempts: int
    backoff: ResolvedBackoffPolicy
    jitter: JitterStrategyTag
    respect_retry_after: bool


@dataclass(frozen=True, slots=True)
class OnErrorRoute:
    """One compiled ``on_error`` arm.

    Match fields mirror the document model
    (:class:`~custos_workflow.document.OnErrorMatch`): exactly one of
    ``code`` / ``code_prefix`` / ``cls`` is non-``None``. The Step
    Coordinator picks the first matching route in declaration order
    (design.md § Implicit on_error policy).

    A ``RETRY`` action carries an inline :class:`ResolvedRetryPolicy`
    if and only if the arm overrides the prevailing one; otherwise
    :attr:`retry` is ``None`` and the node's own :attr:`retry_policy`
    applies.
    """

    action: OnErrorActionTag
    code: str | None = None
    code_prefix: str | None = None
    cls: str | None = None
    retry: ResolvedRetryPolicy | None = None


# ---------------------------------------------------------------------------
# Typed call sites
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TypedCallSite:
    """One compiled CEL expression occurrence inside a step.

    Carries both the original source string (for diagnostics) and the
    fully type-checked AST (for evaluation). The Step Coordinator
    feeds :attr:`typed_ast` directly to ``custos_cel.evaluate`` — it
    never re-parses :attr:`source`.

    Attributes:
        source: The original CEL expression text — includes the
            ``${{ ... }}`` wrapper as it appears in the document.
        typed_ast: The fully type-checked AST produced by
            :func:`custos_cel.type_check`. Round-trips through the
            shared ``custos_cel.to_json`` envelope so it can be
            serialized inside the graph blob.
        kind: Which slot inside the step holds this call site
            (``if`` / ``when`` / ``with`` / …). Drives diagnostics
            and the runtime dispatcher.
        document_path: Dotted breadcrumb pointing back at the source
            location in the original document (e.g.
            ``spec.steps[2].when``). Used by error messages emitted
            after compilation; the on-disk JSON keeps the breadcrumb
            so cross-pod logs reference identical paths.
    """

    source: str
    typed_ast: TypedAST
    kind: CallSiteKind
    document_path: str


# ---------------------------------------------------------------------------
# Nodes and edges
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ExecutionNode:
    """One compiled step in the graph.

    Attributes:
        step_id: The step id from the source document. Unique within
            the graph (enforced by
            :meth:`WorkflowSpec._step_ids_unique` at parse time).
        kind: Structural step kind.
        primitive_handler: Resolved handler tag — what the Step
            Coordinator dispatches to.
        retry_policy: The effective retry policy for this step
            (``None`` if no retry — the implicit fail-permanent rule
            applies). Resolution lands in WF-IMPL-022.
        on_error_routes: Compiled on-error arms in declaration order.
            Empty for steps that use the implicit policy
            (design.md § Implicit on_error policy). Compiled in
            WF-IMPL-023.
        call_sites: ``{slot_label: TypedCallSite}``. ``slot_label`` is
            stable for single-slot kinds (e.g. ``"if"``, ``"with"``)
            and uses a sub-key for multi-slot kinds (e.g. ``"let.x"``
            for the ``x`` binding inside a let step). The compile-time
            collector (WF-IMPL-020) owns the labelling scheme; this
            dataclass just stores the resulting mapping.
        step_source: The original :class:`~custos_workflow.document.Step`
            instance. Round-tripping the pydantic model uses
            ``model_dump(by_alias=True)`` so wire field names survive
            (e.g. ``forEach`` not ``for_each``).
    """

    step_id: str
    kind: StepKind
    primitive_handler: PrimitiveHandler
    retry_policy: ResolvedRetryPolicy | None
    on_error_routes: tuple[OnErrorRoute, ...]
    call_sites: Mapping[str, TypedCallSite]
    step_source: Step

    def __post_init__(self) -> None:
        # Freeze the call_sites mapping so callers cannot mutate the
        # dict after the node is constructed. The wrapping is
        # transparent — ``dict(node.call_sites)`` still works.
        if not isinstance(self.call_sites, MappingProxyType):
            # ``object.__setattr__`` is the standard escape hatch for
            # frozen dataclasses in ``__post_init__``.
            object.__setattr__(
                self,
                "call_sites",
                MappingProxyType(dict(self.call_sites)),
            )


@dataclass(frozen=True, slots=True)
class Edge:
    """One compiled dependency edge in the graph."""

    from_step: str
    to_step: str
    kind: EdgeKind


# ---------------------------------------------------------------------------
# Graph + metadata
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class GraphMetadata:
    """Identification + provenance for the compiled graph.

    These fields are denormalized off the source
    :class:`~custos_workflow.document.WorkflowDocument` so a logged
    graph blob is self-describing without a Catalog roundtrip.
    """

    workflow_name: str
    workflow_workspace: str | None
    document_api_version: str


@dataclass(frozen=True, slots=True)
class ExecutionGraph:
    """The compiled execution plan persisted on ``Run.compiledGraph``.

    Attributes:
        nodes: Compiled steps. Tuple order matches the topological
            order today (WF-IMPL-018 ships in v1 with
            ``topological_order == tuple(n.step_id for n in nodes)``);
            WF-IMPL-019 will populate the dedicated
            :attr:`topological_order` tuple so the two can diverge if
            the topology builder ever reorders nodes for serialization
            stability.
        edges: Compiled dependency edges. Stored in
            ``(from_step, to_step, kind)`` lexicographic order by
            :func:`to_json` so the serializer output is deterministic
            even when the topology builder hands us an order that
            depends on dict-iteration order.
        topological_order: A defensive copy of step ids in a valid
            execution order. Duplicated from :attr:`nodes` so
            callers that only need the order do not have to walk
            the full node list.
        metadata: Identification + provenance.
    """

    nodes: tuple[ExecutionNode, ...]
    edges: tuple[Edge, ...]
    topological_order: tuple[str, ...]
    metadata: GraphMetadata = field()

    def __post_init__(self) -> None:
        # Cheap structural sanity check — the topology builder owns
        # the heavy validation (WF-IMPL-019). Here we only assert the
        # invariants the *data model itself* guarantees.
        node_ids = {n.step_id for n in self.nodes}
        order_set = set(self.topological_order)
        if node_ids != order_set:
            raise ValueError(
                "ExecutionGraph: topological_order must reference the same step ids as nodes",
            )
        if len(self.topological_order) != len(self.nodes):
            raise ValueError(
                "ExecutionGraph: topological_order has duplicate entries",
            )
        for edge in self.edges:
            if edge.from_step not in node_ids:
                raise ValueError(
                    f"ExecutionGraph: edge from {edge.from_step!r} references an unknown step",
                )
            if edge.to_step not in node_ids:
                raise ValueError(
                    f"ExecutionGraph: edge to {edge.to_step!r} references an unknown step",
                )
        # Edges are canonicalised at construction time so two graphs
        # that differ only in edge order compare equal AND serialize
        # byte-identical (design.md § Replay-safe Immutability). The
        # JSON serializer relies on this invariant rather than
        # re-sorting on every call.
        sorted_edges = tuple(
            sorted(self.edges, key=lambda e: (e.from_step, e.to_step, e.kind.value))
        )
        if sorted_edges != self.edges:
            object.__setattr__(self, "edges", sorted_edges)

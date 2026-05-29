"""Topology builder for the Definition Compiler (WF-IMPL-019).

The Step Coordinator drives execution in topological order. Replay
correctness demands that two runs of the same compiled
:class:`~custos_workflow.graph.ExecutionGraph` see identical step
ordering, so the sort below is *stable*: when multiple steps are
ready (zero in-degree) we pop them in alphabetical id order, never
in input-list order.

The topology layer combines two edge sources:

* **Explicit** — author-declared ``needs:`` arrays on each step.
* **Implicit** — data dependencies derived from CEL references of
  the form ``steps.<other_id>.outputs.*`` anywhere inside an
  ``if`` / ``when`` / ``unless`` / ``with`` / ``forEach`` / ``where`` /
  ``let`` / ``${{ }}`` placeholder. The call sites and their typed
  ASTs come from the call-site collector (WF-IMPL-020).

The two passes are deliberately independent so the driver
(WF-IMPL-021) can interleave them with the call-site collector
without circular imports.

Errors raised here are *placeholder* — the final error taxonomy
arrives with WF-IMPL-024. Until then they are subclasses of
:class:`TopologyError` so a single ``except`` clause in the driver
catches every shape.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from heapq import heappop, heappush
from typing import TYPE_CHECKING

from custos_cel.ast import Ident, Member, Node

from custos_workflow.graph.model import Edge, EdgeKind

if TYPE_CHECKING:
    from custos_workflow.document.models import WorkflowDocument
    from custos_workflow.graph.model import TypedCallSite


__all__ = [
    "TopologyError",
    "collect_data_dependencies",
    "collect_explicit_edges",
    "detect_cycles",
    "topological_sort",
    "validate_step_refs",
]


class TopologyError(ValueError):
    """Placeholder error for topology-layer failures.

    Replaced by the structured taxonomy in WF-IMPL-024. The driver
    catches this base so future subclasses widen, not narrow, the
    contract.
    """


def collect_explicit_edges(doc: WorkflowDocument) -> list[Edge]:
    """Return the explicit edges declared via ``step.needs:``.

    Validates that every referenced step exists in the document.
    Self-references and forward references (depending on a step
    declared later in document order) are rejected — both are caught
    by ``_StepCommon`` validators for self-refs and by this function
    for forward refs.

    Edges are not deduplicated here. Callers usually combine this
    list with :func:`collect_data_dependencies` and then rely on
    :func:`topological_sort` (which collapses duplicates by index
    construction) or build the final ``ExecutionGraph`` which
    canonicalises edges in ``__post_init__``.
    """
    steps = doc.spec.steps
    index: dict[str, int] = {step.id: i for i, step in enumerate(steps)}
    edges: list[Edge] = []
    for i, step in enumerate(steps):
        if step.needs is None:
            continue
        for dep in step.needs:
            try:
                dep_idx = index[dep]
            except KeyError as exc:
                raise TopologyError(
                    f"step {step.id!r}: needs references unknown step {dep!r}",
                ) from exc
            if dep_idx >= i:
                # ``dep_idx == i`` is already blocked by the
                # ``_StepCommon`` self-ref validator; keep the check
                # here as well so the topology layer's contract is
                # self-contained.
                raise TopologyError(
                    f"step {step.id!r}: needs entry {dep!r} is a forward "
                    "reference (must appear earlier in document order)",
                )
            edges.append(Edge(from_step=dep, to_step=step.id, kind=EdgeKind.EXPLICIT_NEEDS))
    return edges


def _iter_subnodes(node: Node) -> Iterable[Node]:
    """Yield ``node`` and all of its descendants (depth-first, pre-order).

    The traversal walks every dataclass field that contains a
    :class:`Node` or a tuple of nodes. We do this generically (via
    ``getattr``) rather than dispatching per node class so the walker
    stays robust if ``custos_cel`` grows new node kinds.
    """
    yield node
    # ``Member`` and ``Index`` expose ``target`` / ``index`` directly;
    # ``Call`` and the literal collections expose tuples; ``Binary`` /
    # ``Unary`` / ``Conditional`` expose discrete children. Walking
    # ``__dataclass_fields__`` would also work but ``dir()``-based
    # introspection is too broad. We list the field names known to
    # ``custos_cel`` instead.
    for field_name in (
        "target",
        "index",
        "operand",
        "left",
        "right",
        "cond",
        "then_branch",
        "else_branch",
    ):
        child = getattr(node, field_name, None)
        if isinstance(child, Node):
            yield from _iter_subnodes(child)
    args = getattr(node, "args", None)
    if isinstance(args, tuple):
        for arg in args:
            if isinstance(arg, Node):
                yield from _iter_subnodes(arg)
    elements = getattr(node, "elements", None)
    if isinstance(elements, tuple):
        for el in elements:
            if isinstance(el, Node):
                yield from _iter_subnodes(el)
    entries = getattr(node, "entries", None)
    if isinstance(entries, tuple):
        for k, v in entries:
            if isinstance(k, Node):
                yield from _iter_subnodes(k)
            if isinstance(v, Node):
                yield from _iter_subnodes(v)


def _step_ref_target(node: Node) -> str | None:
    """If ``node`` is a ``steps.<id>.outputs`` Member access, return ``<id>``.

    The shape we match is::

        Member(target=Member(target=Ident('steps'), name=<id>),
               name='outputs')

    Any other configuration of ``steps`` access (e.g. ``steps`` by
    itself, or ``steps[expr]`` with a dynamic key) is treated as
    *not* a static reference and returns ``None``. The type checker
    (WF-IMPL-022) rejects dynamic ``steps`` access separately.
    """
    if not isinstance(node, Member) or node.name != "outputs":
        return None
    inner = node.target
    if not isinstance(inner, Member):
        return None
    if not isinstance(inner.target, Ident) or inner.target.name != "steps":
        return None
    return inner.name


def collect_data_dependencies(
    doc: WorkflowDocument,
    call_sites: Mapping[str, Sequence[TypedCallSite]],
) -> list[Edge]:
    """Return implicit edges derived from CEL ``steps.<id>.outputs.*`` refs.

    Parameters
    ----------
    doc
        The workflow document — used for the document-order index so
        forward references can be flagged.
    call_sites
        A mapping from step id to the typed call sites WF-IMPL-020
        gathers for that step (one per CEL slot: ``if``, ``when``,
        ``unless``, ``with``, ``forEach``, ``where``, ``let``,
        ``${{ }}`` placeholders).

    Self-references (``step X`` references ``steps.X.outputs.*``) and
    forward references (a step references a step that appears later
    in document order) are :class:`TopologyError`. References to
    steps that do not exist in the document are also rejected.
    """
    steps = doc.spec.steps
    index: dict[str, int] = {step.id: i for i, step in enumerate(steps)}
    edges: list[Edge] = []
    # Use an ordered set keyed on the (from, to) pair so we emit at
    # most one DATA_DEPENDENCY edge per producer/consumer combination,
    # even when the same reference appears in multiple call sites or
    # several times inside the same expression.
    seen: dict[tuple[str, str], None] = {}

    for step_id, sites in call_sites.items():
        if step_id not in index:
            raise TopologyError(
                f"call_sites references unknown step {step_id!r}",
            )
        consumer_idx = index[step_id]
        for site in sites:
            for node in _iter_subnodes(site.typed_ast):
                producer = _step_ref_target(node)
                if producer is None:
                    continue
                if producer == step_id:
                    raise TopologyError(
                        f"step {step_id!r}: CEL expression at {site.document_path!r} "
                        "references the step's own outputs",
                    )
                if producer not in index:
                    raise TopologyError(
                        f"step {step_id!r}: CEL expression at {site.document_path!r} "
                        f"references unknown step {producer!r}",
                    )
                if index[producer] >= consumer_idx:
                    raise TopologyError(
                        f"step {step_id!r}: CEL expression at {site.document_path!r} "
                        f"references step {producer!r} declared later in document order",
                    )
                key = (producer, step_id)
                if key in seen:
                    continue
                seen[key] = None
                edges.append(
                    Edge(from_step=producer, to_step=step_id, kind=EdgeKind.DATA_DEPENDENCY),
                )
    return edges


def validate_step_refs(
    doc: WorkflowDocument,
    refs: Iterable[tuple[str, str, Node]],
) -> None:
    """Surface forward / unknown / self ``steps.X.outputs`` refs as topology errors.

    The Definition Compiler driver (WF-IMPL-021) runs this pre-flight
    before the type-check stage so that graph-shape problems
    (referencing a later step, an unknown step, or the consuming
    step's own outputs) surface as :class:`TopologyError` instead of
    as ``expression.unbound_name`` type-check failures.

    Without this pass the type checker would reject those references
    first — the per-step :class:`~custos_cel.SchemaBindings`
    deliberately only exposes prior steps — and the caller would see
    a "name not bound" diagnostic instead of the structurally
    correct "step appears later in document order".

    Parameters
    ----------
    doc
        The workflow document — used for the document-order index.
    refs
        Iterable of ``(consumer_step_id, document_path, ast)`` triples.
        The ``ast`` is the parsed CEL :class:`~custos_cel.ast.Node`
        for the call site (typed or untyped — the walker only looks
        at the syntactic shape, not the ``cel_type`` annotations).

    Raises
    ------
    TopologyError
        For self-references, references to unknown step ids, and
        forward references (a step referencing a step that appears
        later in document order). The same conditions that
        :func:`collect_data_dependencies` raises after type-check.
    """
    steps = doc.spec.steps
    index: dict[str, int] = {step.id: i for i, step in enumerate(steps)}
    for step_id, doc_path, ast in refs:
        if step_id not in index:
            raise TopologyError(
                f"validate_step_refs references unknown step {step_id!r}",
            )
        consumer_idx = index[step_id]
        for node in _iter_subnodes(ast):
            producer = _step_ref_target(node)
            if producer is None:
                continue
            if producer == step_id:
                raise TopologyError(
                    f"step {step_id!r}: CEL expression at {doc_path!r} "
                    "references the step's own outputs",
                )
            if producer not in index:
                raise TopologyError(
                    f"step {step_id!r}: CEL expression at {doc_path!r} "
                    f"references unknown step {producer!r}",
                )
            if index[producer] >= consumer_idx:
                raise TopologyError(
                    f"step {step_id!r}: CEL expression at {doc_path!r} "
                    f"references step {producer!r} declared later in document order",
                )


def detect_cycles(edges: Iterable[Edge]) -> list[list[str]]:
    """Return cycles in the dependency graph.

    Each returned cycle is a list of step ids. Self-loops surface as
    single-element lists. Strongly-connected components of size
    greater than one surface as the full list of member nodes,
    sorted alphabetically so the diagnostic message is deterministic.

    Implemented as iterative Tarjan's SCC over the edge list so we
    avoid Python's recursion limit on deep DAGs.
    """
    successors: dict[str, list[str]] = defaultdict(list)
    nodes: set[str] = set()
    for edge in edges:
        successors[edge.from_step].append(edge.to_step)
        nodes.add(edge.from_step)
        nodes.add(edge.to_step)

    index_of: dict[str, int] = {}
    lowlink: dict[str, int] = {}
    on_stack: dict[str, bool] = {}
    stack: list[str] = []
    counter = 0
    cycles: list[list[str]] = []

    # Iterative Tarjan: each "work item" is (node, successor_iter).
    work: list[tuple[str, list[str], int]] = []

    for start in sorted(nodes):
        if start in index_of:
            continue
        work.append((start, successors.get(start, []), 0))
        index_of[start] = counter
        lowlink[start] = counter
        counter += 1
        stack.append(start)
        on_stack[start] = True

        while work:
            node, succs, i = work[-1]
            if i < len(succs):
                work[-1] = (node, succs, i + 1)
                neighbour = succs[i]
                if neighbour not in index_of:
                    index_of[neighbour] = counter
                    lowlink[neighbour] = counter
                    counter += 1
                    stack.append(neighbour)
                    on_stack[neighbour] = True
                    work.append((neighbour, successors.get(neighbour, []), 0))
                elif on_stack.get(neighbour, False):
                    lowlink[node] = min(lowlink[node], index_of[neighbour])
            else:
                if lowlink[node] == index_of[node]:
                    component: list[str] = []
                    while True:
                        member = stack.pop()
                        on_stack[member] = False
                        component.append(member)
                        if member == node:
                            break
                    is_self_loop = len(component) == 1 and component[0] in successors.get(
                        component[0], []
                    )
                    if len(component) > 1 or is_self_loop:
                        cycles.append(sorted(component))
                work.pop()
                if work:
                    parent, _, _ = work[-1]
                    lowlink[parent] = min(lowlink[parent], lowlink[node])

    cycles.sort()
    return cycles


def topological_sort(
    step_ids: Sequence[str],
    edges: Iterable[Edge],
) -> tuple[str, ...]:
    """Return a stable topological order over ``step_ids``.

    The sort is Kahn's algorithm with an alphabetical tiebreak on
    step ids — when multiple nodes have zero in-degree at the same
    time we pop them in lexicographic order. This guarantees that
    two runs of the same document produce identical orderings even
    if the input edge list arrives in a different sequence (Dapr
    Workflow replay relies on this).

    Raises :class:`TopologyError` when a cycle is present; the
    message lists the first cycle returned by :func:`detect_cycles`.
    ``step_ids`` may include nodes with no edges — they are kept and
    sorted alphabetically among the no-dependency frontier.
    """
    id_set = set(step_ids)
    if len(id_set) != len(step_ids):
        raise TopologyError(
            "topological_sort: duplicate ids in step_ids input",
        )

    indegree: dict[str, int] = dict.fromkeys(step_ids, 0)
    successors: dict[str, list[str]] = defaultdict(list)
    # Deduplicate (from, to) pairs so a node that appears as both an
    # explicit and implicit predecessor is only counted once in
    # in-degrees. This makes the sort agree with the canonicalised
    # edge set on ``ExecutionGraph``.
    seen: set[tuple[str, str]] = set()
    edge_list = list(edges)
    for edge in edge_list:
        if edge.from_step not in id_set:
            raise TopologyError(
                f"topological_sort: edge from unknown step {edge.from_step!r}",
            )
        if edge.to_step not in id_set:
            raise TopologyError(
                f"topological_sort: edge to unknown step {edge.to_step!r}",
            )
        pair = (edge.from_step, edge.to_step)
        if pair in seen:
            continue
        seen.add(pair)
        successors[edge.from_step].append(edge.to_step)
        indegree[edge.to_step] += 1

    frontier: list[str] = [sid for sid, deg in indegree.items() if deg == 0]
    heap: list[str] = []
    for sid in frontier:
        heappush(heap, sid)

    ordered: list[str] = []
    while heap:
        sid = heappop(heap)
        ordered.append(sid)
        for nxt in successors.get(sid, ()):
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                heappush(heap, nxt)

    if len(ordered) != len(step_ids):
        cycles = detect_cycles(edge_list)
        if cycles:
            cycle_repr = " -> ".join(cycles[0])
            raise TopologyError(
                f"topological_sort: cycle detected: {cycle_repr}",
            )
        raise TopologyError(  # pragma: no cover - belt-and-braces
            "topological_sort: unreachable steps without an explicit cycle",
        )

    return tuple(ordered)

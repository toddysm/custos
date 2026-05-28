"""Tests for :mod:`custos_workflow.graph.topology` (WF-IMPL-019)."""

from __future__ import annotations

import random
from collections.abc import Sequence
from typing import Any

import pytest
from custos_cel import SchemaBindings, parse, type_check

from custos_workflow.document import WorkflowDocument
from custos_workflow.graph import (
    CallSiteKind,
    Edge,
    EdgeKind,
    TopologyError,
    TypedCallSite,
    collect_data_dependencies,
    collect_explicit_edges,
    detect_cycles,
    topological_sort,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


_INPUTS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "target": {"type": "string"},
        "threshold": {"type": "integer"},
    },
}


def _doc(steps: Sequence[dict[str, Any]]) -> WorkflowDocument:
    return WorkflowDocument.model_validate(
        {
            "apiVersion": "custos.dev/v1",
            "kind": "Workflow",
            "metadata": {"name": "pipeline"},
            "spec": {"steps": list(steps)},
        }
    )


def _outputs_schema(properties: dict[str, Any]) -> dict[str, Any]:
    return {"type": "object", "properties": properties}


def _call_site(
    source: str,
    *,
    kind: CallSiteKind = CallSiteKind.LET,
    document_path: str = "spec.steps[0].let.value",
    prior_steps: Sequence[tuple[str, dict[str, Any]]] = (),
) -> TypedCallSite:
    raw = source.removeprefix("${{ ").removesuffix(" }}")
    ast = type_check(
        parse(raw),
        SchemaBindings(inputs=_INPUTS_SCHEMA, prior_steps=tuple(prior_steps)),
    )
    return TypedCallSite(
        source=source,
        typed_ast=ast,
        kind=kind,
        document_path=document_path,
    )


# ---------------------------------------------------------------------------
# collect_explicit_edges
# ---------------------------------------------------------------------------


class TestCollectExplicitEdges:
    def test_chain_of_needs(self) -> None:
        doc = _doc(
            [
                {"id": "a", "activity": "ns/scan@1"},
                {"id": "b", "activity": "ns/scan@1", "needs": ["a"]},
                {"id": "c", "activity": "ns/scan@1", "needs": ["b"]},
            ]
        )
        edges = collect_explicit_edges(doc)
        assert edges == [
            Edge(from_step="a", to_step="b", kind=EdgeKind.EXPLICIT_NEEDS),
            Edge(from_step="b", to_step="c", kind=EdgeKind.EXPLICIT_NEEDS),
        ]

    def test_no_needs_returns_empty(self) -> None:
        doc = _doc(
            [
                {"id": "a", "activity": "ns/scan@1"},
                {"id": "b", "activity": "ns/scan@1"},
            ]
        )
        assert collect_explicit_edges(doc) == []

    def test_multiple_needs_on_one_step(self) -> None:
        doc = _doc(
            [
                {"id": "a", "activity": "ns/scan@1"},
                {"id": "b", "activity": "ns/scan@1"},
                {"id": "c", "activity": "ns/scan@1", "needs": ["a", "b"]},
            ]
        )
        assert collect_explicit_edges(doc) == [
            Edge(from_step="a", to_step="c", kind=EdgeKind.EXPLICIT_NEEDS),
            Edge(from_step="b", to_step="c", kind=EdgeKind.EXPLICIT_NEEDS),
        ]

    def test_forward_reference_rejected(self) -> None:
        # ``b`` declared first, depending on ``c`` which appears later
        # in document order: forward reference.
        doc = _doc(
            [
                {"id": "a", "activity": "ns/scan@1"},
                {"id": "b", "activity": "ns/scan@1", "needs": ["c"]},
                {"id": "c", "activity": "ns/scan@1"},
            ]
        )
        with pytest.raises(TopologyError, match="forward reference"):
            collect_explicit_edges(doc)

    def test_unknown_step_id_rejected(self) -> None:
        doc = _doc(
            [
                {"id": "a", "activity": "ns/scan@1"},
                {"id": "b", "activity": "ns/scan@1", "needs": ["ghost"]},
            ]
        )
        with pytest.raises(TopologyError, match="unknown step"):
            collect_explicit_edges(doc)


# ---------------------------------------------------------------------------
# collect_data_dependencies
# ---------------------------------------------------------------------------


class TestCollectDataDependencies:
    def test_implicit_from_cel_member_chain(self) -> None:
        doc = _doc(
            [
                {"id": "scan", "activity": "ns/scan@1"},
                {
                    "id": "derive",
                    "let": {
                        "verdict": "${{ steps.scan.outputs.critical > inputs.threshold }}",
                    },
                },
            ]
        )
        prior = (("scan", _outputs_schema({"critical": {"type": "integer"}})),)
        call_sites = {
            "derive": [
                _call_site(
                    "${{ steps.scan.outputs.critical > inputs.threshold }}",
                    kind=CallSiteKind.LET,
                    document_path="spec.steps[1].let.verdict",
                    prior_steps=prior,
                ),
            ],
        }
        edges = collect_data_dependencies(doc, call_sites)
        assert edges == [
            Edge(from_step="scan", to_step="derive", kind=EdgeKind.DATA_DEPENDENCY),
        ]

    def test_deduplicates_repeated_reference(self) -> None:
        doc = _doc(
            [
                {"id": "scan", "activity": "ns/scan@1"},
                {
                    "id": "derive",
                    "let": {
                        "x": "${{ steps.scan.outputs.critical + steps.scan.outputs.critical }}",
                    },
                },
            ]
        )
        prior = (("scan", _outputs_schema({"critical": {"type": "integer"}})),)
        call_sites = {
            "derive": [
                _call_site(
                    "${{ steps.scan.outputs.critical + steps.scan.outputs.critical }}",
                    prior_steps=prior,
                )
            ],
        }
        edges = collect_data_dependencies(doc, call_sites)
        assert len(edges) == 1
        assert edges[0].from_step == "scan"

    def test_no_steps_reference_returns_empty(self) -> None:
        doc = _doc(
            [
                {"id": "a", "activity": "ns/scan@1"},
                {"id": "b", "let": {"x": "${{ inputs.threshold + 1 }}"}},
            ]
        )
        call_sites = {
            "b": [_call_site("${{ inputs.threshold + 1 }}")],
        }
        assert collect_data_dependencies(doc, call_sites) == []

    def test_self_reference_rejected(self) -> None:
        doc = _doc(
            [
                {"id": "scan", "activity": "ns/scan@1"},
                {
                    "id": "derive",
                    "let": {"x": "${{ steps.derive.outputs.foo }}"},
                },
            ]
        )
        prior = (("derive", _outputs_schema({"foo": {"type": "string"}})),)
        call_sites = {
            "derive": [_call_site("${{ steps.derive.outputs.foo }}", prior_steps=prior)],
        }
        with pytest.raises(TopologyError, match="own outputs"):
            collect_data_dependencies(doc, call_sites)

    def test_forward_reference_rejected(self) -> None:
        # Step ``a`` (index 0) references ``b`` (index 1) — forward.
        doc = _doc(
            [
                {"id": "a", "let": {"x": "${{ steps.b.outputs.foo }}"}},
                {"id": "b", "activity": "ns/scan@1"},
            ]
        )
        # ``b`` declared *after* ``a``, but to typecheck the
        # expression we still need a prior-steps schema for ``b``.
        prior = (("b", _outputs_schema({"foo": {"type": "string"}})),)
        call_sites = {
            "a": [_call_site("${{ steps.b.outputs.foo }}", prior_steps=prior)],
        }
        with pytest.raises(TopologyError, match="declared later"):
            collect_data_dependencies(doc, call_sites)

    def test_unknown_producer_rejected(self) -> None:
        doc = _doc(
            [
                {"id": "derive", "let": {"x": "${{ steps.ghost.outputs.foo }}"}},
            ]
        )
        prior = (("ghost", _outputs_schema({"foo": {"type": "string"}})),)
        call_sites = {
            "derive": [_call_site("${{ steps.ghost.outputs.foo }}", prior_steps=prior)],
        }
        with pytest.raises(TopologyError, match="unknown step 'ghost'"):
            collect_data_dependencies(doc, call_sites)

    def test_call_sites_unknown_step_rejected(self) -> None:
        doc = _doc(
            [
                {"id": "a", "activity": "ns/scan@1"},
            ]
        )
        call_sites = {
            "ghost": [_call_site("${{ inputs.threshold }}")],
        }
        with pytest.raises(TopologyError, match="call_sites references unknown"):
            collect_data_dependencies(doc, call_sites)

    def test_dynamic_steps_index_not_treated_as_static_ref(self) -> None:
        # ``steps`` accessed without a Member chain (e.g. as a bare
        # identifier inside a different construct) must not produce
        # an edge. We construct a CEL expression that mentions
        # ``inputs.target`` and ``inputs.threshold`` to ensure the
        # walker descends into Binary children without crashing.
        doc = _doc(
            [
                {"id": "a", "activity": "ns/scan@1"},
                {"id": "b", "let": {"x": "${{ inputs.target + 'x' }}"}},
            ]
        )
        call_sites = {
            "b": [_call_site("${{ inputs.target + 'x' }}")],
        }
        assert collect_data_dependencies(doc, call_sites) == []

    def test_walker_descends_into_calls_lists_and_maps(self) -> None:
        # Exercises Call.args / ListLit.elements / MapLit.entries
        # traversal: the ``steps.scan.outputs.x`` reference is buried
        # inside a list-literal element passed to ``size()`` and a
        # map-literal value. The walker must reach it.
        prior = (("scan", _outputs_schema({"x": {"type": "integer"}})),)
        doc = _doc(
            [
                {"id": "scan", "activity": "ns/scan@1"},
                {
                    "id": "derive",
                    "let": {
                        "n": (
                            "${{ size([steps.scan.outputs.x]) + {'k': steps.scan.outputs.x}['k'] }}"
                        ),
                    },
                },
            ]
        )
        call_sites = {
            "derive": [
                _call_site(
                    ("${{ size([steps.scan.outputs.x]) + {'k': steps.scan.outputs.x}['k'] }}"),
                    prior_steps=prior,
                ),
            ],
        }
        # Both references collapse into a single DATA_DEPENDENCY edge
        # thanks to the dedup table inside the collector.
        assert collect_data_dependencies(doc, call_sites) == [
            Edge(from_step="scan", to_step="derive", kind=EdgeKind.DATA_DEPENDENCY),
        ]

    def test_inputs_outputs_not_treated_as_step_ref(self) -> None:
        # ``inputs.outputs`` happens to match the outer ``.outputs``
        # member, but the intermediate target is the ``inputs`` ident
        # (not a Member), so it must NOT produce an edge.
        doc = _doc(
            [
                {
                    "id": "a",
                    "let": {"x": "${{ inputs.outputs }}"},
                },
            ]
        )
        # Type-checking ``inputs.outputs`` requires an ``outputs`` key
        # under the inputs schema, so we build the typed call site
        # against a one-shot bindings shape rather than the module's
        # default fixture.
        bindings = SchemaBindings(
            inputs={
                "type": "object",
                "properties": {"outputs": {"type": "string"}},
            },
            prior_steps=(),
        )
        ast = type_check(parse("inputs.outputs"), bindings)
        call_sites = {
            "a": [
                TypedCallSite(
                    source="${{ inputs.outputs }}",
                    typed_ast=ast,
                    kind=CallSiteKind.LET,
                    document_path="spec.steps[0].let.x",
                )
            ],
        }
        assert collect_data_dependencies(doc, call_sites) == []

    def test_two_level_member_outputs_not_treated_as_step_ref(self) -> None:
        # ``a.b.outputs`` has the outer ``.outputs`` member and an
        # intermediate Member, but the intermediate's target is an
        # Ident other than ``steps`` — rejection path on line 170.
        doc = _doc(
            [
                {"id": "a", "let": {"x": "${{ inputs.target }}"}},
            ]
        )
        # Build the awkward AST manually: Member(name='outputs',
        # target=Member(name='b', target=Ident('inputs'))). This
        # only type-checks against an inputs schema whose ``inputs``
        # has a nested ``b.outputs`` shape; rather than carve such a
        # schema, we construct the TypedCallSite directly.
        bindings = SchemaBindings(
            inputs={
                "type": "object",
                "properties": {
                    "b": {
                        "type": "object",
                        "properties": {"outputs": {"type": "string"}},
                    },
                },
            },
            prior_steps=(),
        )
        ast = type_check(parse("inputs.b.outputs"), bindings)
        call_sites = {
            "a": [
                TypedCallSite(
                    source="${{ inputs.b.outputs }}",
                    typed_ast=ast,
                    kind=CallSiteKind.LET,
                    document_path="spec.steps[0].let.x",
                )
            ],
        }
        assert collect_data_dependencies(doc, call_sites) == []


# ---------------------------------------------------------------------------
# detect_cycles
# ---------------------------------------------------------------------------


class TestDetectCycles:
    def test_acyclic_graph_returns_empty(self) -> None:
        edges = [
            Edge(from_step="a", to_step="b", kind=EdgeKind.EXPLICIT_NEEDS),
            Edge(from_step="b", to_step="c", kind=EdgeKind.EXPLICIT_NEEDS),
        ]
        assert detect_cycles(edges) == []

    def test_self_loop_detected(self) -> None:
        edges = [
            Edge(from_step="a", to_step="a", kind=EdgeKind.DATA_DEPENDENCY),
        ]
        assert detect_cycles(edges) == [["a"]]

    def test_two_cycle_detected(self) -> None:
        edges = [
            Edge(from_step="a", to_step="b", kind=EdgeKind.DATA_DEPENDENCY),
            Edge(from_step="b", to_step="a", kind=EdgeKind.DATA_DEPENDENCY),
        ]
        assert detect_cycles(edges) == [["a", "b"]]

    def test_three_cycle_detected(self) -> None:
        edges = [
            Edge(from_step="a", to_step="b", kind=EdgeKind.DATA_DEPENDENCY),
            Edge(from_step="b", to_step="c", kind=EdgeKind.DATA_DEPENDENCY),
            Edge(from_step="c", to_step="a", kind=EdgeKind.DATA_DEPENDENCY),
        ]
        assert detect_cycles(edges) == [["a", "b", "c"]]

    def test_multiple_independent_cycles(self) -> None:
        edges = [
            Edge(from_step="a", to_step="b", kind=EdgeKind.DATA_DEPENDENCY),
            Edge(from_step="b", to_step="a", kind=EdgeKind.DATA_DEPENDENCY),
            Edge(from_step="c", to_step="d", kind=EdgeKind.DATA_DEPENDENCY),
            Edge(from_step="d", to_step="c", kind=EdgeKind.DATA_DEPENDENCY),
        ]
        cycles = detect_cycles(edges)
        assert cycles == [["a", "b"], ["c", "d"]]


# ---------------------------------------------------------------------------
# topological_sort
# ---------------------------------------------------------------------------


class TestTopologicalSort:
    def test_linear_chain(self) -> None:
        ids = ("c", "b", "a")  # input order is irrelevant
        edges = [
            Edge(from_step="a", to_step="b", kind=EdgeKind.EXPLICIT_NEEDS),
            Edge(from_step="b", to_step="c", kind=EdgeKind.EXPLICIT_NEEDS),
        ]
        assert topological_sort(ids, edges) == ("a", "b", "c")

    def test_alphabetical_tiebreak_on_frontier(self) -> None:
        # Three independent steps; tiebreak is alphabetical id.
        assert topological_sort(("z", "a", "m"), []) == ("a", "m", "z")

    def test_dedups_edge_pairs_for_indegree(self) -> None:
        # The same (a -> b) appearing as both explicit and implicit
        # should not double-count b's in-degree, otherwise the sort
        # would never visit b.
        edges = [
            Edge(from_step="a", to_step="b", kind=EdgeKind.EXPLICIT_NEEDS),
            Edge(from_step="a", to_step="b", kind=EdgeKind.DATA_DEPENDENCY),
        ]
        assert topological_sort(("a", "b"), edges) == ("a", "b")

    def test_cycle_raises(self) -> None:
        edges = [
            Edge(from_step="a", to_step="b", kind=EdgeKind.DATA_DEPENDENCY),
            Edge(from_step="b", to_step="a", kind=EdgeKind.DATA_DEPENDENCY),
        ]
        with pytest.raises(TopologyError, match="cycle detected"):
            topological_sort(("a", "b"), edges)

    def test_unknown_from_rejected(self) -> None:
        edges = [
            Edge(from_step="ghost", to_step="b", kind=EdgeKind.EXPLICIT_NEEDS),
        ]
        with pytest.raises(TopologyError, match="from unknown step"):
            topological_sort(("a", "b"), edges)

    def test_unknown_to_rejected(self) -> None:
        edges = [
            Edge(from_step="a", to_step="ghost", kind=EdgeKind.EXPLICIT_NEEDS),
        ]
        with pytest.raises(TopologyError, match="to unknown step"):
            topological_sort(("a", "b"), edges)

    def test_duplicate_step_ids_rejected(self) -> None:
        with pytest.raises(TopologyError, match="duplicate ids"):
            topological_sort(("a", "a"), [])

    def test_stable_across_input_permutations(self) -> None:
        # Diamond: a -> b, a -> c, b -> d, c -> d. The unique
        # topological order respecting alphabetical tiebreak is
        # (a, b, c, d) regardless of edge input order.
        ids = ("a", "b", "c", "d")
        base_edges = [
            Edge(from_step="a", to_step="b", kind=EdgeKind.EXPLICIT_NEEDS),
            Edge(from_step="a", to_step="c", kind=EdgeKind.EXPLICIT_NEEDS),
            Edge(from_step="b", to_step="d", kind=EdgeKind.EXPLICIT_NEEDS),
            Edge(from_step="c", to_step="d", kind=EdgeKind.EXPLICIT_NEEDS),
        ]
        rng = random.Random(0)
        expected = topological_sort(ids, base_edges)
        for _ in range(100):
            shuffled = list(base_edges)
            rng.shuffle(shuffled)
            assert topological_sort(ids, shuffled) == expected

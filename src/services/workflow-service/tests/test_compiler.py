"""Tests for :mod:`custos_workflow.compiler` (WF-IMPL-021)."""

from __future__ import annotations

import logging
import textwrap
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import pytest

from custos_workflow.bindings import InMemoryActivityTypeRegistry
from custos_workflow.compiler import (
    BindingsCompileError,
    CallSiteCompileError,
    CompileError,
    RunMeta,
    TopologyCompileError,
    TypeCheckCompileError,
)
from custos_workflow.compiler import (
    compile as compile_workflow,
)
from custos_workflow.document import (
    ActivityStep,
    LetStep,
    WorkflowDocument,
    WorkflowStep,
    parse_document,
)
from custos_workflow.graph import (
    CallSiteKind,
    Edge,
    EdgeKind,
    JitterStrategyTag,
    OnErrorActionTag,
    PrimitiveHandler,
    StepKind,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _registry() -> InMemoryActivityTypeRegistry:
    return InMemoryActivityTypeRegistry(
        {
            "security/scan@1": {
                "type": "object",
                "properties": {
                    "critical": {"type": "integer"},
                    "findings": {"type": "array", "items": {"type": "string"}},
                },
            },
            "ops/notify@1": {
                "type": "object",
                "properties": {"sent": {"type": "boolean"}},
            },
        }
    )


def _run_meta() -> RunMeta:
    return RunMeta(
        workspace_id="ws-001",
        workflow_version_id="wfv-001",
        workflow_name="pipeline",
        workflow_version_label="v1",
        started_at_default=datetime(2026, 5, 1, tzinfo=UTC),
    )


def _doc(steps: Sequence[dict[str, Any]], *, name: str = "pipeline") -> WorkflowDocument:
    return WorkflowDocument.model_validate(
        {
            "apiVersion": "custos.dev/v1",
            "kind": "Workflow",
            "metadata": {"name": name, "workspace": "security"},
            "spec": {
                "inputs": {
                    "target": {"type": "string", "required": True},
                    "threshold": {"type": "integer", "default": 10},
                },
                "steps": list(steps),
            },
        }
    )


_HAPPY_DOC = textwrap.dedent(
    """\
    apiVersion: custos.dev/v1
    kind: Workflow
    metadata:
      name: pipeline
      workspace: security
    spec:
      inputs:
        target:
          type: string
          required: true
        threshold:
          type: integer
          default: 10
      steps:
        - id: scan
          activity: security/scan@1
          connector: primary
          with:
            image: ${{ inputs.target }}
        - id: derive
          let:
            severity: ${{ steps.scan.outputs.critical }}
            verdict: ${{ steps.scan.outputs.critical > inputs.threshold }}
        - id: promote
          workflow: security/promote@1
          needs:
            - derive
          with:
            verdict: ${{ steps.derive.outputs.verdict }}
        - id: notify
          activity: ops/notify@1
          connector: primary
          if: ${{ steps.derive.outputs.verdict }}
          with:
            channel: ${{ inputs.target }}
    """
)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestRunMeta:
    def test_dataclass_frozen(self) -> None:
        meta = _run_meta()
        with pytest.raises(AttributeError):
            meta.workspace_id = "ws-other"  # type: ignore[misc]


class TestCompileHappyPath:
    def test_returns_execution_graph(self) -> None:
        doc = parse_document(_HAPPY_DOC)
        graph = compile_workflow(doc, _run_meta(), _registry())
        assert len(graph.nodes) == 4
        assert graph.metadata.workflow_name == "pipeline"
        assert graph.metadata.workflow_workspace == "security"
        assert graph.metadata.document_api_version == "custos.dev/v1"

    def test_topological_order_respects_dependencies(self) -> None:
        # ``scan`` must precede ``derive`` (data dep on
        # ``steps.scan.outputs``); ``promote`` declares an explicit
        # ``needs: [derive]``; ``notify`` reads
        # ``steps.derive.outputs.verdict`` from its ``if`` slot.
        doc = parse_document(_HAPPY_DOC)
        graph = compile_workflow(doc, _run_meta(), _registry())
        order = list(graph.topological_order)
        assert order.index("scan") < order.index("derive")
        assert order.index("derive") < order.index("promote")
        assert order.index("derive") < order.index("notify")

    def test_nodes_iterate_in_topological_order(self) -> None:
        doc = parse_document(_HAPPY_DOC)
        graph = compile_workflow(doc, _run_meta(), _registry())
        assert tuple(n.step_id for n in graph.nodes) == graph.topological_order

    def test_edges_are_canonical_sorted(self) -> None:
        doc = parse_document(_HAPPY_DOC)
        graph = compile_workflow(doc, _run_meta(), _registry())
        as_keys = [(e.from_step, e.to_step, e.kind.value) for e in graph.edges]
        assert as_keys == sorted(as_keys)

    def test_expected_edges_present(self) -> None:
        doc = parse_document(_HAPPY_DOC)
        graph = compile_workflow(doc, _run_meta(), _registry())
        kinds = {(e.from_step, e.to_step): e.kind for e in graph.edges}
        # ``promote`` declares an EXPLICIT_NEEDS edge from ``derive``.
        assert kinds[("derive", "promote")] is EdgeKind.EXPLICIT_NEEDS
        # ``derive``'s let bindings consume ``scan.outputs.*`` → DATA_DEPENDENCY.
        assert kinds[("scan", "derive")] is EdgeKind.DATA_DEPENDENCY
        # ``notify.if`` consumes ``derive.outputs.verdict`` → DATA_DEPENDENCY.
        assert kinds[("derive", "notify")] is EdgeKind.DATA_DEPENDENCY

    def test_step_kinds_and_handlers(self) -> None:
        doc = parse_document(_HAPPY_DOC)
        graph = compile_workflow(doc, _run_meta(), _registry())
        by_id = {n.step_id: n for n in graph.nodes}
        assert by_id["scan"].kind is StepKind.ACTIVITY
        assert by_id["scan"].primitive_handler is PrimitiveHandler.ACTIVITY_RUNTIME
        assert by_id["derive"].kind is StepKind.LET
        assert by_id["derive"].primitive_handler is PrimitiveHandler.EXPRESSION_INLINE
        assert by_id["promote"].kind is StepKind.WORKFLOW
        assert by_id["promote"].primitive_handler is PrimitiveHandler.SUB_ORCHESTRATION
        assert by_id["notify"].kind is StepKind.ACTIVITY

    def test_call_sites_keyed_by_collector_path(self) -> None:
        doc = parse_document(_HAPPY_DOC)
        graph = compile_workflow(doc, _run_meta(), _registry())
        by_id = {n.step_id: n for n in graph.nodes}
        # ``scan.with.image`` is a single-placeholder string, keyed
        # by collector at ``with.image``.
        assert "with.image" in by_id["scan"].call_sites
        assert by_id["scan"].call_sites["with.image"].kind is CallSiteKind.WITH
        # ``derive`` has two let bindings, keyed under ``let.<name>``.
        assert set(by_id["derive"].call_sites) == {"let.severity", "let.verdict"}
        for cs in by_id["derive"].call_sites.values():
            assert cs.kind is CallSiteKind.LET
        # ``notify`` has both an ``if`` and a ``with.channel`` site.
        assert set(by_id["notify"].call_sites) == {"if", "with.channel"}
        assert by_id["notify"].call_sites["if"].kind is CallSiteKind.IF

    def test_step_source_round_trips(self) -> None:
        doc = parse_document(_HAPPY_DOC)
        graph = compile_workflow(doc, _run_meta(), _registry())
        by_id = {n.step_id: n for n in graph.nodes}
        assert isinstance(by_id["scan"].step_source, ActivityStep)
        assert isinstance(by_id["derive"].step_source, LetStep)
        assert isinstance(by_id["promote"].step_source, WorkflowStep)
        # The original ActivityStep should round-trip its wire fields.
        scan_dump = by_id["scan"].step_source.model_dump(by_alias=True, exclude_none=True)
        assert scan_dump["activity"] == "security/scan@1"
        assert scan_dump["with"] == {"image": "${{ inputs.target }}"}

    def test_call_site_typed_ast_is_not_none(self) -> None:
        doc = parse_document(_HAPPY_DOC)
        graph = compile_workflow(doc, _run_meta(), _registry())
        by_id = {n.step_id: n for n in graph.nodes}
        # ``custos_cel.type_check`` annotates every node with a
        # ``cel_type``; if the compiler shipped untyped ASTs, the
        # root node's ``cel_type`` would be ``None``.
        for node in graph.nodes:
            for cs in node.call_sites.values():
                assert cs.typed_ast.cel_type is not None, (
                    f"step {node.step_id} call site {cs.document_path}"
                )
        # silence unused-variable lint for the ``by_id`` smoke.
        assert by_id


# ---------------------------------------------------------------------------
# Sub-workflow permissive-warning path
# ---------------------------------------------------------------------------


class TestSubWorkflowPermissivePath:
    def test_sub_workflow_outputs_typecheck_against_permissive_schema(self) -> None:
        # ``promote`` is a WorkflowStep — the bindings layer falls
        # back to ``{"type": "object"}`` for its outputs schema, so
        # any field access (``steps.promote.outputs.<anything>``)
        # must type-check.
        doc = parse_document(_HAPPY_DOC)
        graph = compile_workflow(doc, _run_meta(), _registry())
        # The compile succeeds — that is the assertion.
        assert any(n.step_id == "promote" for n in graph.nodes)

    def test_warning_emitted_through_provided_logger(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        doc = parse_document(_HAPPY_DOC)
        logger = logging.getLogger("compiler.test.subwf")
        with caplog.at_level(logging.WARNING, logger=logger.name):
            compile_workflow(doc, _run_meta(), _registry(), logger=logger)
        # The bindings layer logs ``binding.unresolved_sub_workflow``;
        # the compiler hands its logger down so callers can route
        # the warning into their own observability pipeline.
        assert any("binding.unresolved_sub_workflow" in rec.message for rec in caplog.records)


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


class TestCallSiteErrors:
    def test_unterminated_placeholder_raises_call_site_error(self) -> None:
        doc = _doc(
            [
                {
                    "id": "scan",
                    "activity": "security/scan@1",
                    "connector": "primary",
                    "with": {"image": "${{ unterminated"},
                },
            ]
        )
        with pytest.raises(CallSiteCompileError) as ei:
            compile_workflow(doc, _run_meta(), _registry())
        # Stage-1 errors are a CompileError subclass so callers can
        # branch on the family if they only care about "did the
        # pipeline fail?".
        assert isinstance(ei.value, CompileError)
        assert "scan" in str(ei.value)
        assert "with.image" in str(ei.value)


class TestBindingsErrors:
    def test_unknown_activity_raises_bindings_error(self) -> None:
        doc = _doc(
            [
                {
                    "id": "scan",
                    "activity": "missing/activity@9",
                    "connector": "primary",
                },
            ]
        )
        with pytest.raises(BindingsCompileError) as ei:
            compile_workflow(doc, _run_meta(), _registry())
        assert "missing/activity@9" in str(ei.value)
        assert isinstance(ei.value, CompileError)


class TestTypeCheckErrors:
    def test_unbound_name_in_if_raises(self) -> None:
        doc = _doc(
            [
                {
                    "id": "scan",
                    "activity": "security/scan@1",
                    "connector": "primary",
                    "if": "${{ inputs.nope }}",
                },
            ]
        )
        with pytest.raises(TypeCheckCompileError) as ei:
            compile_workflow(doc, _run_meta(), _registry())
        assert len(ei.value.errors) == 1
        failure = ei.value.errors[0]
        assert failure.step_id == "scan"
        assert failure.path == "if"

    def test_multiple_failures_accumulate_into_single_error(self) -> None:
        # Two distinct type errors in two different steps; the
        # compiler must surface *both* in one raise instead of
        # bailing after the first.
        doc = _doc(
            [
                {
                    "id": "scan",
                    "activity": "security/scan@1",
                    "connector": "primary",
                    "if": "${{ inputs.does_not_exist }}",
                },
                {
                    "id": "filter",
                    "activity": "security/scan@1",
                    "connector": "primary",
                    "when": "${{ inputs.also_missing }}",
                },
            ]
        )
        with pytest.raises(TypeCheckCompileError) as ei:
            compile_workflow(doc, _run_meta(), _registry())
        steps_with_failures = {f.step_id for f in ei.value.errors}
        assert steps_with_failures == {"scan", "filter"}
        # Each failure carries the original CelError kind so callers
        # can bucket the diagnostics.
        for failure in ei.value.errors:
            assert failure.kind.startswith("expression.")


class TestTopologyErrors:
    def test_cycle_via_needs_raises_topology_error(self) -> None:
        # Forward references in ``needs:`` are caught by
        # ``collect_explicit_edges`` before cycle detection runs,
        # so we declare a backwards cycle using two distinct
        # steps with ``needs:`` referencing each other across
        # document order. The first step pointing at a later one
        # is a forward reference (rejected); the second pointing
        # backward is fine; introducing a cycle requires both
        # entries — the existing collector flags the forward ref
        # as the topology error.
        doc = _doc(
            [
                {"id": "a", "activity": "security/scan@1", "connector": "p", "needs": ["b"]},
                {"id": "b", "activity": "security/scan@1", "connector": "p"},
            ]
        )
        with pytest.raises(TopologyCompileError) as ei:
            compile_workflow(doc, _run_meta(), _registry())
        assert "a" in str(ei.value) or "b" in str(ei.value)
        assert isinstance(ei.value, CompileError)

    def test_forward_cel_step_ref_raises_topology_not_typecheck(self) -> None:
        # ``steps.later.outputs.x`` from an earlier step is a graph
        # shape problem, NOT a type problem. The pre-flight
        # ``validate_step_refs`` stage surfaces it as
        # TopologyCompileError before the type checker runs (where it
        # would otherwise show up as expression.unbound_name because
        # derive_bindings only exposes prior steps).
        doc = _doc(
            [
                {
                    "id": "early",
                    "activity": "security/scan@1",
                    "connector": "primary",
                    "if": "${{ steps.late.outputs.critical }}",
                },
                {
                    "id": "late",
                    "activity": "security/scan@1",
                    "connector": "primary",
                },
            ]
        )
        with pytest.raises(TopologyCompileError) as ei:
            compile_workflow(doc, _run_meta(), _registry())
        # The diagnostic must point at the actual offender, not at
        # the type checker's misleading "unbound name" framing.
        assert "later in document order" in str(ei.value)
        assert "late" in str(ei.value)

    def test_unknown_cel_step_ref_raises_topology_not_typecheck(self) -> None:
        # ``steps.ghost.outputs.x`` — same reasoning as the forward
        # case: graph-shape problem, must surface as topology.
        doc = _doc(
            [
                {
                    "id": "scan",
                    "activity": "security/scan@1",
                    "connector": "primary",
                    "if": "${{ steps.ghost.outputs.critical }}",
                },
            ]
        )
        with pytest.raises(TopologyCompileError) as ei:
            compile_workflow(doc, _run_meta(), _registry())
        assert "ghost" in str(ei.value)

    def test_self_cel_step_ref_raises_topology_not_typecheck(self) -> None:
        # ``steps.self.outputs.x`` from inside ``self`` is also a
        # graph-shape problem.
        doc = _doc(
            [
                {
                    "id": "scan",
                    "activity": "security/scan@1",
                    "connector": "primary",
                    "if": "${{ steps.scan.outputs.critical }}",
                },
            ]
        )
        with pytest.raises(TopologyCompileError) as ei:
            compile_workflow(doc, _run_meta(), _registry())
        assert "own outputs" in str(ei.value)


class TestEdgeDeduplication:
    def test_explicit_needs_plus_cel_ref_produces_single_edge(self) -> None:
        # When the author writes BOTH ``needs: [scan]`` and a CEL
        # reference to ``steps.scan.outputs.*`` on the same consumer,
        # the compiled graph must contain ONE edge — not two —
        # tagged as EXPLICIT_NEEDS (the author's intent).
        doc = _doc(
            [
                {"id": "scan", "activity": "security/scan@1", "connector": "primary"},
                {
                    "id": "consumer",
                    "activity": "security/scan@1",
                    "connector": "primary",
                    "needs": ["scan"],
                    "with": {"image": "${{ steps.scan.outputs.findings[0] }}"},
                },
            ]
        )
        graph = compile_workflow(doc, _run_meta(), _registry())
        pair_edges = [e for e in graph.edges if (e.from_step, e.to_step) == ("scan", "consumer")]
        assert len(pair_edges) == 1
        assert pair_edges[0].kind is EdgeKind.EXPLICIT_NEEDS


# ---------------------------------------------------------------------------
# Stubbed retry / on_error resolvers (WF-IMPL-022 / WF-IMPL-023)
# ---------------------------------------------------------------------------


class TestRetryAndOnErrorResolvers:
    def test_retry_policy_filled_with_defaults(self) -> None:
        # ``maxAttempts: 5`` should pass through; every other field is
        # taken from the platform-default curve until WF-IMPL-022
        # tightens the resolver.
        doc = _doc(
            [
                {
                    "id": "scan",
                    "activity": "security/scan@1",
                    "connector": "primary",
                    "retry": {"maxAttempts": 5},
                },
            ]
        )
        graph = compile_workflow(doc, _run_meta(), _registry())
        policy = graph.nodes[0].retry_policy
        assert policy is not None
        assert policy.max_attempts == 5
        # Defaults from the stub:
        assert policy.backoff.initial_delay_ms == 100
        assert policy.backoff.max_delay_ms == 30_000
        assert policy.respect_retry_after is True
        assert policy.jitter is JitterStrategyTag.FULL

    def test_step_without_retry_has_none_policy(self) -> None:
        doc = _doc(
            [
                {"id": "scan", "activity": "security/scan@1", "connector": "primary"},
            ]
        )
        graph = compile_workflow(doc, _run_meta(), _registry())
        assert graph.nodes[0].retry_policy is None

    def test_on_error_arm_passes_through_to_route(self) -> None:
        doc = _doc(
            [
                {
                    "id": "scan",
                    "activity": "security/scan@1",
                    "connector": "primary",
                    "on_error": [
                        {
                            "match": {"code": "E_TIMEOUT"},
                            "do": "retry",
                            "maxAttempts": 2,
                            "retry": {"maxAttempts": 7},
                        },
                        {"match": {"codePrefix": "E_RATE_"}, "do": "skip"},
                    ],
                },
            ]
        )
        graph = compile_workflow(doc, _run_meta(), _registry())
        routes = graph.nodes[0].on_error_routes
        assert len(routes) == 2
        assert routes[0].action is OnErrorActionTag.RETRY
        assert routes[0].code == "E_TIMEOUT"
        assert routes[0].retry is not None
        # ``retry:`` block wins over the shorthand ``maxAttempts:`` at
        # this stub — WF-IMPL-023 will sort out the merge precedence.
        assert routes[0].retry.max_attempts == 7
        assert routes[1].action is OnErrorActionTag.SKIP
        assert routes[1].code_prefix == "E_RATE_"
        assert routes[1].retry is None


# ---------------------------------------------------------------------------
# Edge consumers
# ---------------------------------------------------------------------------


class TestEdgeAndNodeInvariants:
    def test_empty_step_list_rejected_by_document(self) -> None:
        # ``WorkflowSpec`` rejects an empty step list at parse time;
        # the compiler never sees it.
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            _doc([])

    def test_graph_round_trips_via_serializer(self) -> None:
        # The byte-stable JSON envelope (WF-IMPL-018) must accept the
        # compiler's output without losing any field. This guards
        # against the compiler producing nodes the serializer cannot
        # round-trip.
        from custos_workflow.graph import from_json, to_json

        doc = parse_document(_HAPPY_DOC)
        graph = compile_workflow(doc, _run_meta(), _registry())
        rebuilt = from_json(to_json(graph))
        assert rebuilt.topological_order == graph.topological_order
        assert rebuilt.edges == graph.edges
        assert tuple(n.step_id for n in rebuilt.nodes) == tuple(n.step_id for n in graph.nodes)

    def test_edge_tuple_matches_edge_dataclass(self) -> None:
        # Sanity: edges in the compiled graph are real Edge dataclasses
        # with EdgeKind enum members (so consumers can ``is``-compare
        # against EdgeKind.* rather than parsing strings).
        doc = parse_document(_HAPPY_DOC)
        graph = compile_workflow(doc, _run_meta(), _registry())
        assert all(isinstance(e, Edge) for e in graph.edges)
        assert all(isinstance(e.kind, EdgeKind) for e in graph.edges)

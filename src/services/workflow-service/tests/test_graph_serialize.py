"""Tests for :mod:`custos_workflow.graph.serialize`."""

from __future__ import annotations

import json

import pytest
from custos_cel import SchemaBindings, parse, type_check

from custos_workflow.document import ActivityStep, LetStep, WorkflowStep
from custos_workflow.graph import (
    GRAPH_SCHEMA_VERSION,
    BackoffStrategyTag,
    CallSiteKind,
    Edge,
    EdgeKind,
    ExecutionGraph,
    ExecutionNode,
    GraphMetadata,
    GraphSerializationError,
    JitterStrategyTag,
    OnErrorActionTag,
    OnErrorRoute,
    PrimitiveHandler,
    ResolvedBackoffPolicy,
    ResolvedRetryPolicy,
    StepKind,
    TypedCallSite,
    from_json,
    to_json,
)
from custos_workflow.graph.serialize import _STEP_TYPE_ADAPTER

_INPUTS_SCHEMA = {
    "type": "object",
    "properties": {
        "target": {"type": "string"},
        "threshold": {"type": "integer"},
    },
}


def _typed_ast(source: str, prior_steps=()):  # type: ignore[no-untyped-def]
    ast = parse(source)
    return type_check(ast, SchemaBindings(inputs=_INPUTS_SCHEMA, prior_steps=prior_steps))


def _retry_policy() -> ResolvedRetryPolicy:
    return ResolvedRetryPolicy(
        max_attempts=5,
        backoff=ResolvedBackoffPolicy(
            strategy=BackoffStrategyTag.EXPONENTIAL,
            initial_delay_ms=250,
            max_delay_ms=30_000,
            multiplier=2.0,
        ),
        jitter=JitterStrategyTag.FULL,
        respect_retry_after=True,
    )


def _graph() -> ExecutionGraph:
    scan_step = ActivityStep.model_validate(
        {"id": "scan", "activity": "security/scan@1", "connector": "primary"}
    )
    derive_step = LetStep.model_validate(
        {
            "id": "derive",
            "let": {"verdict": "${{ steps.scan.outputs.critical > inputs.threshold }}"},
        }
    )
    promote_step = WorkflowStep.model_validate({"id": "promote", "workflow": "security/promote@1"})

    prior_scan_outputs = {
        "type": "object",
        "properties": {"critical": {"type": "integer"}},
    }

    scan_node = ExecutionNode(
        step_id="scan",
        kind=StepKind.ACTIVITY,
        primitive_handler=PrimitiveHandler.ACTIVITY_RUNTIME,
        retry_policy=_retry_policy(),
        on_error_routes=(
            OnErrorRoute(
                action=OnErrorActionTag.RETRY,
                code="rate.limited",
                retry=_retry_policy(),
            ),
            OnErrorRoute(
                action=OnErrorActionTag.FAIL,
                cls="permanent",
            ),
        ),
        call_sites={
            "with.target": TypedCallSite(
                source="${{ inputs.target }}",
                typed_ast=_typed_ast("inputs.target"),
                kind=CallSiteKind.WITH,
                document_path="spec.steps[0].with.target",
            ),
        },
        step_source=scan_step,
    )
    derive_node = ExecutionNode(
        step_id="derive",
        kind=StepKind.LET,
        primitive_handler=PrimitiveHandler.EXPRESSION_INLINE,
        retry_policy=None,
        on_error_routes=(),
        call_sites={
            "let.verdict": TypedCallSite(
                source="${{ steps.scan.outputs.critical > inputs.threshold }}",
                typed_ast=_typed_ast(
                    "steps.scan.outputs.critical > inputs.threshold",
                    prior_steps=(("scan", prior_scan_outputs),),
                ),
                kind=CallSiteKind.LET,
                document_path="spec.steps[1].let.verdict",
            ),
        },
        step_source=derive_step,
    )
    promote_node = ExecutionNode(
        step_id="promote",
        kind=StepKind.WORKFLOW,
        primitive_handler=PrimitiveHandler.SUB_ORCHESTRATION,
        retry_policy=None,
        on_error_routes=(),
        call_sites={},
        step_source=promote_step,
    )

    return ExecutionGraph(
        nodes=(scan_node, derive_node, promote_node),
        edges=(
            Edge(from_step="scan", to_step="derive", kind=EdgeKind.DATA_DEPENDENCY),
            Edge(from_step="derive", to_step="promote", kind=EdgeKind.CONTROL_FLOW),
        ),
        topological_order=("scan", "derive", "promote"),
        metadata=GraphMetadata(
            workflow_name="pipeline",
            workflow_workspace="security",
            document_api_version="custos.dev/v1",
        ),
    )


class TestRoundTrip:
    def test_round_trip_equals_original(self) -> None:
        graph = _graph()
        rebuilt = from_json(to_json(graph))
        assert rebuilt == graph

    def test_to_json_is_byte_stable(self) -> None:
        graph = _graph()
        assert to_json(graph) == to_json(graph)

    def test_typed_ast_round_trips_byte_equal(self) -> None:
        # The inner typed_ast envelopes must serialize byte-equal
        # across two passes; this is what the design's replay-safe
        # immutability contract relies on.
        graph = _graph()
        text1 = to_json(graph)
        rebuilt = from_json(text1)
        text2 = to_json(rebuilt)
        assert text1 == text2

    def test_envelope_carries_schema_versions(self) -> None:
        graph = _graph()
        envelope = json.loads(to_json(graph))
        assert envelope["graph_schema_version"] == GRAPH_SCHEMA_VERSION
        assert isinstance(envelope["ast_schema_version"], int)

    def test_step_source_uses_wire_field_names(self) -> None:
        # ForEach is the canonical example of a wire alias. We use
        # an activity step with forEach to lock the by_alias=True
        # behaviour of the serializer.
        step = ActivityStep.model_validate(
            {
                "id": "fanout",
                "activity": "security/scan@1",
                "connector": "primary",
                "forEach": "${{ inputs.targets }}",
            }
        )
        node = ExecutionNode(
            step_id="fanout",
            kind=StepKind.ACTIVITY,
            primitive_handler=PrimitiveHandler.ACTIVITY_RUNTIME,
            retry_policy=None,
            on_error_routes=(),
            call_sites={},
            step_source=step,
        )
        graph = ExecutionGraph(
            nodes=(node,),
            edges=(),
            topological_order=("fanout",),
            metadata=GraphMetadata(
                workflow_name="pipeline",
                workflow_workspace=None,
                document_api_version="custos.dev/v1",
            ),
        )
        envelope = json.loads(to_json(graph))
        step_payload = envelope["nodes"][0]["step_source"]
        assert "forEach" in step_payload
        assert "for_each" not in step_payload
        # Round-trip preserves the alias.
        assert from_json(to_json(graph)) == graph


class TestEdgeOrdering:
    def test_edges_are_sorted_in_output(self) -> None:
        # Build the same graph twice but feed edges in different
        # orders; the serialized output must be byte-identical.
        scan = ActivityStep.model_validate(
            {"id": "scan", "activity": "security/scan@1", "connector": "primary"}
        )
        a = ExecutionNode(
            step_id="a",
            kind=StepKind.ACTIVITY,
            primitive_handler=PrimitiveHandler.ACTIVITY_RUNTIME,
            retry_policy=None,
            on_error_routes=(),
            call_sites={},
            step_source=scan.model_copy(update={"id": "a"}),
        )
        b = ExecutionNode(
            step_id="b",
            kind=StepKind.ACTIVITY,
            primitive_handler=PrimitiveHandler.ACTIVITY_RUNTIME,
            retry_policy=None,
            on_error_routes=(),
            call_sites={},
            step_source=scan.model_copy(update={"id": "b"}),
        )
        edges_one = (
            Edge(from_step="b", to_step="a", kind=EdgeKind.CONTROL_FLOW),
            Edge(from_step="a", to_step="b", kind=EdgeKind.CONTROL_FLOW),
        )
        edges_two = tuple(reversed(edges_one))
        meta = GraphMetadata(
            workflow_name="pipeline",
            workflow_workspace=None,
            document_api_version="custos.dev/v1",
        )
        g1 = ExecutionGraph(
            nodes=(a, b),
            edges=edges_one,
            topological_order=("a", "b"),
            metadata=meta,
        )
        g2 = ExecutionGraph(
            nodes=(a, b),
            edges=edges_two,
            topological_order=("a", "b"),
            metadata=meta,
        )
        assert to_json(g1) == to_json(g2)


class TestSchemaVersionGuard:
    def test_unknown_graph_schema_version_rejected(self) -> None:
        envelope = json.loads(to_json(_graph()))
        envelope["graph_schema_version"] = 999
        with pytest.raises(GraphSerializationError, match="graph_schema_version"):
            from_json(json.dumps(envelope))

    def test_unknown_ast_schema_version_rejected(self) -> None:
        envelope = json.loads(to_json(_graph()))
        envelope["ast_schema_version"] = 999
        with pytest.raises(GraphSerializationError, match="ast_schema_version"):
            from_json(json.dumps(envelope))

    def test_invalid_json_rejected(self) -> None:
        with pytest.raises(GraphSerializationError, match="JSON"):
            from_json("{not json")

    def test_non_mapping_envelope_rejected(self) -> None:
        with pytest.raises(GraphSerializationError, match="envelope"):
            from_json("[1, 2, 3]")


class TestDecoderShapeErrors:
    def test_step_source_failing_validation_is_wrapped(self) -> None:
        envelope = json.loads(to_json(_graph()))
        envelope["nodes"][0]["step_source"]["activity"] = "not-a-valid-ref"
        with pytest.raises(GraphSerializationError, match="Step union"):
            from_json(json.dumps(envelope))

    def test_nodes_must_be_list(self) -> None:
        envelope = json.loads(to_json(_graph()))
        envelope["nodes"] = {}
        with pytest.raises(GraphSerializationError, match="nodes"):
            from_json(json.dumps(envelope))

    def test_edges_must_be_list(self) -> None:
        envelope = json.loads(to_json(_graph()))
        envelope["edges"] = {}
        with pytest.raises(GraphSerializationError, match="edges"):
            from_json(json.dumps(envelope))

    def test_topological_order_must_be_list(self) -> None:
        envelope = json.loads(to_json(_graph()))
        envelope["topological_order"] = {}
        with pytest.raises(GraphSerializationError, match="topological_order"):
            from_json(json.dumps(envelope))

    def test_on_error_routes_must_be_list(self) -> None:
        envelope = json.loads(to_json(_graph()))
        envelope["nodes"][0]["on_error_routes"] = {}
        with pytest.raises(GraphSerializationError, match="on_error_routes"):
            from_json(json.dumps(envelope))

    def test_post_init_failure_wrapped_in_graph_error(self) -> None:
        envelope = json.loads(to_json(_graph()))
        envelope["topological_order"] = ["ghost"]
        with pytest.raises(GraphSerializationError, match="topological_order"):
            from_json(json.dumps(envelope))

    def test_typed_ast_payload_corruption_wrapped(self) -> None:
        # custos_cel.from_dict raises bare ValueError on a malformed
        # envelope (wrong schema version, missing 'root', unknown
        # node kind). The serializer must wrap that into
        # GraphSerializationError so callers only catch one type.
        envelope = json.loads(to_json(_graph()))
        envelope["nodes"][0]["call_sites"]["with.target"]["typed_ast"] = {
            "schema_version": 999,
            "root": {},
        }
        with pytest.raises(GraphSerializationError, match="typed_ast"):
            from_json(json.dumps(envelope))

    def test_typed_ast_payload_non_mapping_wrapped(self) -> None:
        envelope = json.loads(to_json(_graph()))
        envelope["nodes"][0]["call_sites"]["with.target"]["typed_ast"] = 42
        with pytest.raises(GraphSerializationError, match="typed_ast"):
            from_json(json.dumps(envelope))


class TestEncoderBranches:
    def test_optional_workflow_workspace_omitted_when_none(self) -> None:
        node = ExecutionNode(
            step_id="scan",
            kind=StepKind.ACTIVITY,
            primitive_handler=PrimitiveHandler.ACTIVITY_RUNTIME,
            retry_policy=None,
            on_error_routes=(),
            call_sites={},
            step_source=ActivityStep.model_validate(
                {"id": "scan", "activity": "security/scan@1", "connector": "primary"}
            ),
        )
        graph = ExecutionGraph(
            nodes=(node,),
            edges=(),
            topological_order=("scan",),
            metadata=GraphMetadata(
                workflow_name="pipeline",
                workflow_workspace=None,
                document_api_version="custos.dev/v1",
            ),
        )
        envelope = json.loads(to_json(graph))
        assert "workflow_workspace" not in envelope["metadata"]
        # Round-trip preserves the absence.
        rebuilt = from_json(to_json(graph))
        assert rebuilt.metadata.workflow_workspace is None

    def test_on_error_route_match_fields_round_trip(self) -> None:
        # Cover all three match shapes (code, code_prefix, class) +
        # the retry override branch.
        routes = (
            OnErrorRoute(action=OnErrorActionTag.SKIP, code="x.y"),
            OnErrorRoute(action=OnErrorActionTag.SKIP, code_prefix="x."),
            OnErrorRoute(action=OnErrorActionTag.SKIP, cls="transient"),
            OnErrorRoute(action=OnErrorActionTag.RETRY, code="z.z", retry=_retry_policy()),
        )
        node = ExecutionNode(
            step_id="scan",
            kind=StepKind.ACTIVITY,
            primitive_handler=PrimitiveHandler.ACTIVITY_RUNTIME,
            retry_policy=None,
            on_error_routes=routes,
            call_sites={},
            step_source=ActivityStep.model_validate(
                {"id": "scan", "activity": "security/scan@1", "connector": "primary"}
            ),
        )
        graph = ExecutionGraph(
            nodes=(node,),
            edges=(),
            topological_order=("scan",),
            metadata=GraphMetadata(
                workflow_name="pipeline",
                workflow_workspace=None,
                document_api_version="custos.dev/v1",
            ),
        )
        rebuilt = from_json(to_json(graph))
        assert rebuilt.nodes[0].on_error_routes == routes


def test_step_type_adapter_exposed() -> None:
    # Sanity: the module-level TypeAdapter is the one bound to the
    # document Step union (cheap import-time check).
    assert _STEP_TYPE_ADAPTER is not None

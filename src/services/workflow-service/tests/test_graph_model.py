"""Tests for :mod:`custos_workflow.graph.model`."""

from __future__ import annotations

import dataclasses
from types import MappingProxyType

import pytest
from custos_cel import SchemaBindings, parse, type_check

from custos_workflow.document import ActivityStep
from custos_workflow.graph import (
    CallSiteKind,
    Edge,
    EdgeKind,
    ExecutionGraph,
    ExecutionNode,
    GraphMetadata,
    PrimitiveHandler,
    StepKind,
    TypedCallSite,
)

_INPUTS_SCHEMA = {
    "type": "object",
    "properties": {"target": {"type": "string"}},
    "required": ["target"],
}


def _typed_ast(source: str):  # type: ignore[no-untyped-def]
    ast = parse(source)
    return type_check(ast, SchemaBindings(inputs=_INPUTS_SCHEMA))


def _simple_step() -> ActivityStep:
    return ActivityStep.model_validate(
        {"id": "scan", "activity": "security/scan@1", "connector": "primary"}
    )


def _node(step_id: str = "scan") -> ExecutionNode:
    cs = TypedCallSite(
        source="${{ inputs.target }}",
        typed_ast=_typed_ast("inputs.target"),
        kind=CallSiteKind.WITH,
        document_path=f"spec.steps[?(@.id=={step_id!r})].with.target",
    )
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.ACTIVITY,
        primitive_handler=PrimitiveHandler.ACTIVITY_RUNTIME,
        retry_policy=None,
        on_error_routes=(),
        call_sites={"with.target": cs},
        step_source=_simple_step(),
    )


class TestExecutionNode:
    def test_call_sites_become_immutable_mapping(self) -> None:
        node = _node()
        assert isinstance(node.call_sites, MappingProxyType)
        # The wrapping mapping cannot be mutated.
        with pytest.raises(TypeError):
            node.call_sites["new"] = node.call_sites["with.target"]  # type: ignore[index]

    def test_node_is_frozen(self) -> None:
        node = _node()
        with pytest.raises(dataclasses.FrozenInstanceError):
            node.step_id = "other"  # type: ignore[misc]

    def test_already_proxied_mapping_is_preserved(self) -> None:
        # When the caller already passes a MappingProxyType, the
        # __post_init__ short-circuits and reuses it verbatim.
        cs = TypedCallSite(
            source="${{ inputs.target }}",
            typed_ast=_typed_ast("inputs.target"),
            kind=CallSiteKind.WITH,
            document_path="spec.steps[0].with.target",
        )
        proxy = MappingProxyType({"with.target": cs})
        node = ExecutionNode(
            step_id="scan",
            kind=StepKind.ACTIVITY,
            primitive_handler=PrimitiveHandler.ACTIVITY_RUNTIME,
            retry_policy=None,
            on_error_routes=(),
            call_sites=proxy,
            step_source=_simple_step(),
        )
        assert node.call_sites is proxy


class TestExecutionGraphPostInit:
    def test_topological_order_must_match_nodes(self) -> None:
        node = _node()
        with pytest.raises(ValueError, match="topological_order"):
            ExecutionGraph(
                nodes=(node,),
                edges=(),
                topological_order=("other",),
                metadata=GraphMetadata(
                    workflow_name="pipeline",
                    workflow_workspace="security",
                    document_api_version="custos.dev/v1",
                ),
            )

    def test_duplicate_topological_entry_is_rejected(self) -> None:
        node = _node()
        with pytest.raises(ValueError, match="duplicate"):
            ExecutionGraph(
                nodes=(node,),
                edges=(),
                topological_order=("scan", "scan"),
                metadata=GraphMetadata(
                    workflow_name="pipeline",
                    workflow_workspace=None,
                    document_api_version="custos.dev/v1",
                ),
            )

    def test_edge_must_reference_known_steps(self) -> None:
        node = _node()
        with pytest.raises(ValueError, match="from"):
            ExecutionGraph(
                nodes=(node,),
                edges=(Edge(from_step="ghost", to_step="scan", kind=EdgeKind.CONTROL_FLOW),),
                topological_order=("scan",),
                metadata=GraphMetadata(
                    workflow_name="pipeline",
                    workflow_workspace=None,
                    document_api_version="custos.dev/v1",
                ),
            )

    def test_edge_to_unknown_step_rejected(self) -> None:
        node = _node()
        with pytest.raises(ValueError, match="to"):
            ExecutionGraph(
                nodes=(node,),
                edges=(Edge(from_step="scan", to_step="ghost", kind=EdgeKind.CONTROL_FLOW),),
                topological_order=("scan",),
                metadata=GraphMetadata(
                    workflow_name="pipeline",
                    workflow_workspace=None,
                    document_api_version="custos.dev/v1",
                ),
            )

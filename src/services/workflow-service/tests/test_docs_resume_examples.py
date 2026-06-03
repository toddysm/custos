"""Smoke test for the resume-subscriptions developer doc (WF-IMPL-112).

The doc at ``docs/developers/workflow-resume-subscriptions.md`` ships
three worked ``waitFor:`` YAML examples (§ *Worked examples*).
Acceptance criterion: *every* fenced ``yaml`` block in the doc must be
copy-paste-runnable through ``compile()``. This test is the
verification.

For each ``yaml`` block we:

1. Parse the literal YAML source with ``parse_document``.
2. Compile it with ``compile(document, run_meta, registry)``.
3. Assert the resulting graph matches the shape the doc text describes
   (the ``waitFor:`` node compiles to a ``RESUME_SUBSCRIPTION``
   primitive; Example 3's topology is ``prepare -> await-shipment``).

The blocks are parametrized so silent drift (rename a step id, drop a
``needs:`` entry, break the ``${{ }}`` token) trips loudly with the
offending block index in the failure message.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path

import pytest

from custos_workflow.bindings import InMemoryActivityTypeRegistry
from custos_workflow.compiler import RunMeta
from custos_workflow.compiler import (
    compile as compile_workflow,
)
from custos_workflow.document import parse_document
from custos_workflow.graph import EdgeKind, PrimitiveHandler

# ---------------------------------------------------------------------------
# Doc location + fixtures shared by every example
# ---------------------------------------------------------------------------

_DOC_PATH = (
    Path(__file__).resolve().parent.parent.parent.parent.parent
    / "docs"
    / "developers"
    / "workflow-resume-subscriptions.md"
)

_YAML_FENCE_RE = re.compile(r"```yaml\n(?P<body>.*?)\n```", re.DOTALL)


def _registry() -> InMemoryActivityTypeRegistry:
    """The doc's ``waitFor:`` examples declare no activities, so an
    empty registry suffices."""
    return InMemoryActivityTypeRegistry({})


def _run_meta() -> RunMeta:
    return RunMeta(
        workspace_id="ws-001",
        workflow_version_id="wfv-001",
        workflow_name="resume-docs",
        workflow_version_label="v1",
        started_at_default=datetime(2026, 6, 5, tzinfo=UTC),
    )


def _fenced_yaml_blocks() -> list[tuple[int, str]]:
    assert _DOC_PATH.is_file(), _DOC_PATH
    doc = _DOC_PATH.read_text(encoding="utf-8")
    return [(i, m.group("body")) for i, m in enumerate(_YAML_FENCE_RE.finditer(doc))]


_BLOCKS = _fenced_yaml_blocks()


def test_doc_contains_expected_yaml_blocks() -> None:
    """The doc must ship the three worked ``waitFor:`` examples."""
    assert len(_BLOCKS) == 3, f"expected 3 ```yaml blocks in {_DOC_PATH}, found {len(_BLOCKS)}"


@pytest.mark.parametrize(("index", "source"), _BLOCKS)
def test_every_doc_yaml_block_parses_and_compiles(index: int, source: str) -> None:
    """Each ```yaml fenced block must round-trip through ``compile()``."""
    doc = parse_document(source)
    graph = compile_workflow(doc, _run_meta(), _registry())
    assert graph.nodes, f"block #{index}: compiled graph has no nodes"
    assert len(graph.topological_order) == len(graph.nodes)
    # Every example carries exactly one waitFor step, compiled to the
    # resume-subscription primitive.
    resume_nodes = [
        n for n in graph.nodes if n.primitive_handler is PrimitiveHandler.RESUME_SUBSCRIPTION
    ]
    assert len(resume_nodes) == 1, f"block #{index}: expected exactly one waitFor node"


def test_minimal_example_is_a_single_waitfor_node() -> None:
    """§ Worked examples, Example 1 — minimal ``waitFor:``."""
    _, source = _BLOCKS[0]
    graph = compile_workflow(parse_document(source), _run_meta(), _registry())
    assert graph.topological_order == ("await-approval",)
    node = graph.nodes[0]
    assert node.step_id == "await-approval"
    assert node.primitive_handler is PrimitiveHandler.RESUME_SUBSCRIPTION


def test_after_let_example_topology_is_prepare_then_await() -> None:
    """§ Worked examples, Example 3 — ``waitFor:`` after a ``let:``."""
    _, source = _BLOCKS[2]
    graph = compile_workflow(parse_document(source), _run_meta(), _registry())
    assert graph.topological_order == ("prepare", "await-shipment")
    edge = next(e for e in graph.edges if e.to_step == "await-shipment")
    assert edge.from_step == "prepare"
    assert edge.kind is EdgeKind.EXPLICIT_NEEDS

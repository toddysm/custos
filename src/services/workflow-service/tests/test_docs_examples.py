"""Smoke test for the developer-facing workflow-compilation doc (WF-IMPL-028).

The doc at ``docs/developers/workflow-compilation.md`` advertises
worked YAML examples in §§ 6 (Retry-policy resolution) and 7
(Worked examples). Acceptance criterion: *every* fenced ``yaml``
block in the doc must be copy-paste-runnable through ``compile()``.
This test is the verification.

For each ``yaml`` block we:

1. Parse the literal YAML source with ``parse_document``.
2. Compile it with ``compile(document, run_meta, registry)`` against
   the same fixture registry every example expects.
3. Assert the resulting graph matches the shape the doc text
   describes (node count, topological order, edge kinds, resolved
   retry layering on the retry-policy example).

Additionally, the test parametrizes every fenced ``yaml`` block so
silent drift (rename a step id, swap an activity ref, drop a
``needs:`` entry) trips loudly with the offending block index in
the failure message.
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
from custos_workflow.graph import EdgeKind, OnErrorActionTag

# ---------------------------------------------------------------------------
# Doc location + fixture shared by every example
# ---------------------------------------------------------------------------

_DOC_PATH = (
    Path(__file__).resolve().parent.parent.parent.parent.parent
    / "docs"
    / "developers"
    / "workflow-compilation.md"
)

_YAML_FENCE_RE = re.compile(r"```yaml\n(?P<body>.*?)\n```", re.DOTALL)


def _registry() -> InMemoryActivityTypeRegistry:
    """The fixture registry every doc example expects.

    The doc names two activity types — ``security/scan@1`` and
    ``ops/notify@1`` — so the registry must publish output schemas
    for both. ``security/scan@1`` produces ``critical: integer``
    because Example 2 reads ``steps.scan.outputs.critical``; the
    type-checker rejects the reference otherwise.
    """
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
        started_at_default=datetime(2026, 5, 29, tzinfo=UTC),
    )


# ---------------------------------------------------------------------------
# Doc-block extraction. We collect every ```yaml fence and key it by
# its zero-based occurrence index in document order so failures
# pinpoint the offending block precisely.
# ---------------------------------------------------------------------------


def _fenced_yaml_blocks() -> list[tuple[int, str]]:
    assert _DOC_PATH.is_file(), _DOC_PATH
    doc = _DOC_PATH.read_text(encoding="utf-8")
    return [(i, m.group("body")) for i, m in enumerate(_YAML_FENCE_RE.finditer(doc))]


_BLOCKS = _fenced_yaml_blocks()


def test_doc_contains_expected_yaml_blocks() -> None:
    """The doc must ship the four canonical YAML examples.

    One retry-policy worked example (§ 6) plus the three worked
    examples in § 7 (linear chain, implicit data dep, fanout). This
    is the structural guard against accidentally dropping a fenced
    block.
    """
    assert len(_BLOCKS) == 4, f"expected 4 ```yaml blocks in {_DOC_PATH}, found {len(_BLOCKS)}"


# ---------------------------------------------------------------------------
# Parameterized smoke: every block must parse + compile without
# raising. This is the literal copy-paste-runnable acceptance
# criterion from issue #362.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("index", "source"), _BLOCKS)
def test_every_doc_yaml_block_parses_and_compiles(index: int, source: str) -> None:
    """Each ```yaml fenced block must round-trip through ``compile()``.

    Catches silent drift: rename a step id, rename an activity ref,
    drop a ``needs:`` entry, change a YAML key — any of those will
    fail ``parse_document`` or ``compile``.
    """
    doc = parse_document(source)
    graph = compile_workflow(doc, _run_meta(), _registry())
    # Sanity: the graph must carry at least one node and a
    # topological order matching the node count.
    assert graph.nodes, f"block #{index}: compiled graph has no nodes"
    assert len(graph.topological_order) == len(graph.nodes)


# ---------------------------------------------------------------------------
# § 6 — Retry-policy resolution. The worked example must produce
# the resolved policy advertised in the doc table.
# ---------------------------------------------------------------------------


def test_retry_layers_example_resolves_per_match_overrides() -> None:
    """§ 6 — first ``yaml`` block: retry-layers worked example.

    The doc claims:

    * ``registry.rate_limited`` arm: ``maxAttempts=10``,
      ``backoff.initialDelay=PT5S``, ``backoff.maxDelay=PT10M``.
    * ``class: retryable`` arm: ``maxAttempts=3``,
      ``backoff.initialDelay=PT1S`` (inherited from
      ``spec.defaults``), ``backoff.maxDelay=PT5M`` (inherited).
    """
    _, source = _BLOCKS[0]
    doc = parse_document(source)
    graph = compile_workflow(doc, _run_meta(), _registry())

    scan = next(n for n in graph.nodes if n.step_id == "scan")

    # The on_error compiler ALWAYS prepends a ``cancelled → fail``
    # short-circuit and appends the implicit
    # ``retryable → retry`` / ``permanent → fail`` fallback routes
    # (design.md § Implicit on_error policy). Pick the two
    # user-declared retry arms out of the synthesized list by
    # their distinctive match fields.
    rate_limited = next(r for r in scan.on_error_routes if r.code_prefix == "registry.rate_limited")
    # The user-declared ``class: retryable`` arm comes BEFORE the
    # implicit fallback arm of the same class — both end up in the
    # route table after the on-error compiler appends the implicit
    # fallbacks. The first occurrence is the user's.
    retryable = next(r for r in scan.on_error_routes if r.cls == "retryable")

    assert rate_limited.action == OnErrorActionTag.RETRY
    assert retryable.action == OnErrorActionTag.RETRY
    assert rate_limited.retry is not None
    assert retryable.retry is not None

    # Per-match arm wins on every field it sets.
    assert rate_limited.retry.max_attempts == 10
    assert rate_limited.retry.backoff.initial_delay_ms == 5_000
    assert rate_limited.retry.backoff.max_delay_ms == 600_000

    # Inline ``maxAttempts: 3`` shorthand overrides; backoff falls
    # through to ``spec.defaults.retry`` (PT1S / PT5M).
    assert retryable.retry.max_attempts == 3
    assert retryable.retry.backoff.initial_delay_ms == 1_000
    assert retryable.retry.backoff.max_delay_ms == 300_000


# ---------------------------------------------------------------------------
# § 7 Example 1 — linear chain with explicit ``needs:``.
# ---------------------------------------------------------------------------


def test_linear_chain_example_topology_is_scan_gate_notify() -> None:
    """§ 7 Example 1 — second ``yaml`` block.

    Three steps, two explicit edges, deterministic topological
    order ``scan → gate → notify``; every edge carries
    ``EdgeKind.explicit_needs``.
    """
    _, source = _BLOCKS[1]
    doc = parse_document(source)
    graph = compile_workflow(doc, _run_meta(), _registry())

    assert graph.topological_order == ("scan", "gate", "notify")
    assert len(graph.edges) == 2
    assert all(e.kind == EdgeKind.EXPLICIT_NEEDS for e in graph.edges), (
        f"expected only EXPLICIT_NEEDS edges; got {[e.kind for e in graph.edges]}"
    )
    by_pair = {(e.from_step, e.to_step) for e in graph.edges}
    assert by_pair == {("scan", "gate"), ("gate", "notify")}


# ---------------------------------------------------------------------------
# § 7 Example 2 — implicit data dependency via ``steps.X.outputs``.
# ---------------------------------------------------------------------------


def test_implicit_data_dep_example_infers_scan_to_summarize() -> None:
    """§ 7 Example 2 — third ``yaml`` block.

    Two steps, no ``needs:``, but a ``steps.scan.outputs.critical``
    reference inside ``summarize.let.critical`` forces the compiler
    to emit one ``Edge(kind=implicit_data)`` from ``scan`` to
    ``summarize``.
    """
    _, source = _BLOCKS[2]
    doc = parse_document(source)
    graph = compile_workflow(doc, _run_meta(), _registry())

    assert graph.topological_order == ("scan", "summarize")
    assert len(graph.edges) == 1
    edge = graph.edges[0]
    assert (edge.from_step, edge.to_step) == ("scan", "summarize")
    assert edge.kind == EdgeKind.DATA_DEPENDENCY


# ---------------------------------------------------------------------------
# § 7 Example 3 — ``forEach:`` fan-out over a list-typed input.
# ---------------------------------------------------------------------------


def test_for_each_fanout_example_compiles_to_single_node() -> None:
    """§ 7 Example 3 — fourth ``yaml`` block.

    ``forEach`` is a runtime fan-out modifier; at compile time
    the graph carries one node whose ``forEach`` slot type-checked
    against ``inputs.targets: list<string>``.
    """
    _, source = _BLOCKS[3]
    doc = parse_document(source)
    graph = compile_workflow(doc, _run_meta(), _registry())

    assert graph.topological_order == ("scan-all",)
    assert len(graph.edges) == 0
    scan_all = graph.nodes[0]
    assert scan_all.step_id == "scan-all"
    # The ``forEach`` slot is collected as a call site under the
    # stable label key the compiler assigns.
    assert any("forEach" in label or "for_each" in label for label in scan_all.call_sites), (
        f"expected a forEach call site on scan-all; got {sorted(scan_all.call_sites)}"
    )

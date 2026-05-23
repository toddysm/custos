"""Tests for :mod:`custos_catalog.normalize` (CS-IMPL-006)."""

from __future__ import annotations

import json
from typing import Any

import yaml

from custos_catalog.normalize import (
    NormalizedTemplate,
    NormalizedWorkflow,
    RefResolutionSlot,
    canonical_hash,
    canonical_json,
    normalize_template,
    normalize_workflow,
)


def _wf(steps: list[dict[str, Any]], **extra: Any) -> dict[str, Any]:
    spec: dict[str, Any] = {"steps": steps}
    spec.update(extra)
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "Workflow",
        "metadata": {"name": "wf"},
        "spec": spec,
    }


# ---------------------------------------------------------------------------
# Canonicalization
# ---------------------------------------------------------------------------


def test_normalize_workflow_sorts_keys_at_every_level() -> None:
    doc = _wf(
        [
            {"id": "s", "activity": "ns/t@1", "with": {"z": 1, "a": 2}},
        ],
        triggers=[{"type": "x", "connector": "c"}],
    )
    nw = normalize_workflow(doc)
    rendered = canonical_json(nw.document)
    # Sorted dict keys at the document root...
    assert rendered.startswith('{"apiVersion":"custos.dev/v1","kind":"Workflow"')
    # ...and inside `with`.
    with_obj = nw.document["spec"]["steps"][0]["with"]
    assert list(with_obj.keys()) == ["a", "z"]


def test_normalize_workflow_preserves_step_order() -> None:
    doc = _wf(
        [
            {"id": "second", "activity": "ns/b@1"},
            {"id": "first", "activity": "ns/a@1"},
        ],
    )
    nw = normalize_workflow(doc)
    ids = [step["id"] for step in nw.document["spec"]["steps"]]
    assert ids == ["second", "first"]  # list order is semantic, not sorted


def test_canonical_hash_is_key_order_independent() -> None:
    a = _wf([{"activity": "ns/t@1", "id": "s"}])
    b = _wf([{"id": "s", "activity": "ns/t@1"}])
    assert canonical_hash(normalize_workflow(a).document) == canonical_hash(
        normalize_workflow(b).document,
    )


def test_canonical_hash_yaml_json_parity() -> None:
    yaml_doc = yaml.safe_load(
        """
        apiVersion: custos.dev/v1
        kind: Workflow
        metadata: {name: wf}
        spec:
          steps:
            - id: s
              activity: ns/t@1
        """,
    )
    json_doc = json.loads(json.dumps(yaml_doc))
    h_yaml = canonical_hash(normalize_workflow(yaml_doc).document)
    h_json = canonical_hash(normalize_workflow(json_doc).document)
    assert h_yaml == h_json


def test_canonical_hash_is_stable_across_calls() -> None:
    doc = _wf([{"id": "s", "activity": "ns/t@1"}])
    nw = normalize_workflow(doc)
    h1 = canonical_hash(nw.document)
    h2 = canonical_hash(nw.document)
    assert h1 == h2 and len(h1) == 64  # sha256 hex


def test_canonical_hash_changes_on_value_change() -> None:
    a = canonical_hash(normalize_workflow(_wf([{"id": "s", "activity": "ns/t@1"}])).document)
    b = canonical_hash(normalize_workflow(_wf([{"id": "s", "activity": "ns/t@2"}])).document)
    assert a != b


def test_normalize_workflow_tolerates_mixed_type_keys() -> None:
    # YAML happily parses ``1: x`` as an integer key. Such documents
    # cannot pass the JSON Schema gate, but the normalizer must stay
    # total — emitting them as a ``TypeError`` would short-circuit the
    # downstream CEL/resolver gates instead of letting them surface
    # structured errors. See review comment on CS-IMPL-006.
    doc = {
        "apiVersion": "custos.dev/v1",
        "kind": "Workflow",
        "metadata": {"name": "wf"},
        "spec": {"weird": {1: "int-key", "a": "str-key"}, "steps": []},
    }
    nw = normalize_workflow(doc)  # must not raise
    # Hashing must also stay total even with mixed-type keys.
    digest = canonical_hash(nw.document)
    assert len(digest) == 64
    # And it must be byte-stable across calls.
    assert digest == canonical_hash(nw.document)


# ---------------------------------------------------------------------------
# Slot discovery
# ---------------------------------------------------------------------------


def test_normalize_workflow_emits_activity_slot() -> None:
    doc = _wf([{"id": "s", "activity": "custos.builtin/vuln-scan@2"}])
    nw = normalize_workflow(doc)
    assert nw.slots == (
        RefResolutionSlot(
            kind="activity",
            path=("spec", "steps", 0, "activity"),
            original_ref="custos.builtin/vuln-scan@2",
        ),
    )


def test_normalize_workflow_emits_connector_and_connectors_slots() -> None:
    doc = _wf(
        [
            {
                "id": "s1",
                "activity": "ns/a@1",
                "connector": "prod-registry",
            },
            {
                "id": "s2",
                "activity": "ns/b@1",
                "connectors": {"source": "src-reg", "destination": "dst-reg"},
            },
        ],
        triggers=[{"type": "x", "connector": "trigger-conn"}],
    )
    nw = normalize_workflow(doc)
    kinds = [s.kind for s in nw.slots]
    paths = [s.path for s in nw.slots]
    refs = [s.original_ref for s in nw.slots]
    # Triggers come first (lower index), then steps in order. Within
    # `connectors:` the slots come in sorted-key order for determinism.
    assert kinds == [
        "connector_instance",
        "activity",
        "connector_instance",
        "activity",
        "connector_instance",
        "connector_instance",
    ]
    assert ("spec", "triggers", 0, "connector") in paths
    assert ("spec", "steps", 0, "connector") in paths
    assert ("spec", "steps", 1, "connectors", "destination") in paths
    assert ("spec", "steps", 1, "connectors", "source") in paths
    assert "prod-registry" in refs
    assert "src-reg" in refs and "dst-reg" in refs and "trigger-conn" in refs


def test_normalize_workflow_skips_expression_refs() -> None:
    """${{...}} interpolations are runtime-resolved, not slot-emitted."""
    doc = _wf(
        [
            {
                "id": "s",
                "activity": "${{ placeholders.scanActivity }}",
                "connector": "${{ placeholders.connectorRef }}",
            },
        ],
    )
    nw = normalize_workflow(doc)
    assert nw.slots == ()


def test_normalize_workflow_emits_subworkflow_slot() -> None:
    doc = _wf([{"id": "call", "workflow": "default/child-wf@1"}])
    nw = normalize_workflow(doc)
    assert len(nw.slots) == 1
    assert nw.slots[0].kind == "subworkflow"
    assert nw.slots[0].original_ref == "default/child-wf@1"


def test_normalize_workflow_handles_missing_spec_gracefully() -> None:
    """A doc missing `spec` is degenerate but must not raise here.

    The schema validator is the gate that rejects malformed inputs; the
    normalizer is a pure data-shaping function and treats missing keys
    as zero slots.
    """
    nw = normalize_workflow({"apiVersion": "custos.dev/v1", "kind": "Workflow"})
    assert isinstance(nw, NormalizedWorkflow)
    assert nw.slots == ()


# ---------------------------------------------------------------------------
# Template
# ---------------------------------------------------------------------------


def test_normalize_template_canonicalizes_inner_workflow() -> None:
    doc = {
        "apiVersion": "custos.dev/v1",
        "kind": "WorkflowTemplate",
        "metadata": {"name": "t"},
        "spec": {
            "placeholders": [
                {
                    "type": "connectorRef",
                    "name": "c",
                    "connectorType": "oci-registry",
                },
            ],
            "workflow": {
                "steps": [
                    {
                        "with": {"z": 1, "a": 2},
                        "id": "s",
                        "activity": "${{ placeholders.scanActivity }}",
                    },
                ],
            },
        },
    }
    nt = normalize_template(doc)
    inner_with = nt.document["spec"]["workflow"]["steps"][0]["with"]
    assert list(inner_with.keys()) == ["a", "z"]


def test_normalize_template_emits_no_slots_for_placeholder_bound_refs() -> None:
    """Placeholder-interpolated refs do not emit slots."""
    doc = {
        "apiVersion": "custos.dev/v1",
        "kind": "WorkflowTemplate",
        "metadata": {"name": "t"},
        "spec": {
            "placeholders": [
                {"name": "c", "type": "connectorRef", "connectorType": "x"},
            ],
            "workflow": {
                "steps": [
                    {
                        "id": "s",
                        "activity": "${{ placeholders.scanActivity }}",
                        "connector": "${{ placeholders.c }}",
                    },
                ],
            },
        },
    }
    nt = normalize_template(doc)
    assert nt.slots == ()


def test_normalize_template_emits_slot_for_concrete_inner_ref() -> None:
    """A concrete ref inside a template still emits a slot (uncommon but valid)."""
    doc = {
        "apiVersion": "custos.dev/v1",
        "kind": "WorkflowTemplate",
        "metadata": {"name": "t"},
        "spec": {
            "placeholders": [
                {"name": "c", "type": "connectorRef", "connectorType": "x"},
            ],
            "workflow": {
                "steps": [
                    {
                        "id": "s",
                        "activity": "custos.builtin/noop@1",
                        "connector": "${{ placeholders.c }}",
                    },
                ],
            },
        },
    }
    nt = normalize_template(doc)
    assert isinstance(nt, NormalizedTemplate)
    assert len(nt.slots) == 1
    assert nt.slots[0].kind == "activity"
    assert nt.slots[0].path == ("spec", "workflow", "steps", 0, "activity")


# ---------------------------------------------------------------------------
# Canonical JSON
# ---------------------------------------------------------------------------


def test_canonical_json_uses_tight_separators() -> None:
    assert canonical_json({"a": 1, "b": [1, 2]}) == '{"a":1,"b":[1,2]}'


def test_canonical_json_preserves_unicode() -> None:
    out = canonical_json({"name": "café"})
    assert "café" in out  # ensure_ascii=False

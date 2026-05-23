"""Tests for :mod:`custos_catalog.cel_validate` (CS-IMPL-007)."""

from __future__ import annotations

from typing import Any

import pytest

from custos_catalog.cel_validate import (
    CelNameBindingError,
    CelSyntaxError,
    discover_template_slots,
    discover_workflow_slots,
    validate_expressions,
    validate_template_expressions,
)
from custos_catalog.normalize import normalize_template, normalize_workflow


def _wf(
    steps: list[dict[str, Any]],
    *,
    inputs: dict[str, Any] | None = None,
    triggers: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    spec: dict[str, Any] = {"steps": steps}
    if inputs is not None:
        spec["inputs"] = inputs
    if triggers is not None:
        spec["triggers"] = triggers
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "Workflow",
        "metadata": {"name": "wf"},
        "spec": spec,
    }


# ---------------------------------------------------------------------------
# Slot discovery
# ---------------------------------------------------------------------------


def test_discover_workflow_slots_collects_every_expression_position() -> None:
    doc = _wf(
        [
            {
                "id": "first",
                "let": {"x": "${{ inputs.foo }}"},
            },
            {
                "id": "second",
                "if": "${{ let.x > 0 }}",
                "forEach": "${{ steps.first.outputs.items }}",
                "where": "${{ item.kind == 'ok' }}",
                "activity": "ns/t@1",
                "connector": "${{ inputs.connectorName }}",
                "with": {"a": "${{ item.value }}", "b": 42},
            },
        ],
        inputs={"foo": {"type": "integer"}, "connectorName": {"type": "string"}},
        triggers=[{"type": "x", "connector": "${{ inputs.connectorName }}"}],
    )
    norm = normalize_workflow(doc)
    slots = discover_workflow_slots(norm)
    paths = sorted(s.path for s in slots)
    assert paths == [
        "spec.steps[0].let.x",
        "spec.steps[1].connector",
        "spec.steps[1].forEach",
        "spec.steps[1].if",
        "spec.steps[1].where",
        "spec.steps[1].with.a",
        "spec.triggers[0].connector",
    ]


def test_discover_workflow_slots_ignores_literal_with_values() -> None:
    doc = _wf([{"id": "s", "activity": "ns/t@1", "with": {"x": 42, "y": "literal"}}])
    norm = normalize_workflow(doc)
    assert discover_workflow_slots(norm) == []


# ---------------------------------------------------------------------------
# Positive parse + bind
# ---------------------------------------------------------------------------


def test_validate_expressions_accepts_clean_workflow() -> None:
    doc = _wf(
        [
            {
                "id": "compute",
                "let": {"x": "${{ inputs.value + 1 }}"},
            },
            {
                "id": "use",
                "if": "${{ steps.compute.outputs.x > 0 }}",
                "activity": "ns/t@1",
                "with": {"v": "${{ steps.compute.outputs.x }}"},
            },
        ],
        inputs={"value": {"type": "integer"}},
    )
    validate_expressions(normalize_workflow(doc))


def test_validate_expressions_accepts_run_and_workflow_roots() -> None:
    doc = _wf(
        [
            {
                "id": "s",
                "activity": "ns/t@1",
                "with": {
                    "rid": "${{ run.id }}",
                    "name": "${{ workflow.name }}",
                },
            },
        ],
    )
    validate_expressions(normalize_workflow(doc))


def test_validate_expressions_allows_bracketed_step_ids() -> None:
    doc = _wf(
        [
            {"id": "scan-source", "activity": "ns/a@1"},
            {
                "id": "report",
                "if": '${{ steps["scan-source"].outputs.ok }}',
                "activity": "ns/b@1",
            },
        ],
    )
    validate_expressions(normalize_workflow(doc))


def test_validate_expressions_accepts_let_referencing_earlier_let() -> None:
    """Within one step, a `let` entry can reference an earlier one."""
    doc = _wf(
        [
            {
                "id": "compute",
                "let": {
                    "a": "${{ inputs.x }}",
                    # `b` comes after `a` in sorted-key order.
                    "b": "${{ let.a + 1 }}",
                },
            },
        ],
        inputs={"x": {"type": "integer"}},
    )
    validate_expressions(normalize_workflow(doc))


def test_validate_expressions_item_in_scope_under_foreach() -> None:
    doc = _wf(
        [
            {"id": "src", "activity": "ns/a@1"},
            {
                "id": "fan",
                "forEach": "${{ steps.src.outputs.items }}",
                "where": "${{ item.kind == 'ok' }}",
                "activity": "ns/b@1",
                "with": {"v": "${{ item.value }}"},
            },
        ],
    )
    validate_expressions(normalize_workflow(doc))


# ---------------------------------------------------------------------------
# Syntax errors
# ---------------------------------------------------------------------------


def test_validate_expressions_raises_on_syntax_error() -> None:
    doc = _wf(
        [
            {
                "id": "bad",
                "if": "${{ 1 + }}",  # incomplete binary
                "activity": "ns/t@1",
            },
        ],
    )
    with pytest.raises(CelSyntaxError) as exc:
        validate_expressions(normalize_workflow(doc))
    assert any(i.path == "spec.steps[0].if" for i in exc.value.issues)


def test_validate_expressions_raises_on_multiple_syntax_errors() -> None:
    doc = _wf(
        [
            {"id": "a", "if": "${{ ( }}", "activity": "ns/t@1"},
            {"id": "b", "if": "${{ ] }}", "activity": "ns/t@1"},
        ],
    )
    with pytest.raises(CelSyntaxError) as exc:
        validate_expressions(normalize_workflow(doc))
    paths = {i.path for i in exc.value.issues}
    assert paths == {"spec.steps[0].if", "spec.steps[1].if"}


# ---------------------------------------------------------------------------
# Name-binding errors
# ---------------------------------------------------------------------------


def test_validate_expressions_rejects_unknown_root() -> None:
    doc = _wf(
        [
            {"id": "s", "if": "${{ os.environ }}", "activity": "ns/t@1"},
        ],
    )
    with pytest.raises(CelNameBindingError) as exc:
        validate_expressions(normalize_workflow(doc))
    assert any("os" in i.message for i in exc.value.issues)


def test_validate_expressions_rejects_undefined_step() -> None:
    doc = _wf(
        [
            {
                "id": "s",
                "if": "${{ steps.missing.outputs.x }}",
                "activity": "ns/t@1",
            },
        ],
    )
    with pytest.raises(CelNameBindingError) as exc:
        validate_expressions(normalize_workflow(doc))
    assert any("missing" in i.message for i in exc.value.issues)


def test_validate_expressions_rejects_forward_step_ref() -> None:
    """A step cannot reference a later step (or itself) via `steps.*`."""
    doc = _wf(
        [
            {
                "id": "first",
                "if": "${{ steps.second.outputs.x }}",
                "activity": "ns/t@1",
            },
            {"id": "second", "activity": "ns/t@1"},
        ],
    )
    with pytest.raises(CelNameBindingError):
        validate_expressions(normalize_workflow(doc))


def test_validate_expressions_rejects_undeclared_input() -> None:
    doc = _wf(
        [
            {"id": "s", "if": "${{ inputs.undeclared }}", "activity": "ns/t@1"},
        ],
        inputs={"declared": {"type": "string"}},
    )
    with pytest.raises(CelNameBindingError) as exc:
        validate_expressions(normalize_workflow(doc))
    assert any("undeclared" in i.message for i in exc.value.issues)


def test_validate_expressions_hyphen_step_dot_form_emits_hint() -> None:
    """`steps.foo-bar` parses as subtraction; hint must mention brackets."""
    doc = _wf(
        [
            {"id": "alpha", "activity": "ns/t@1"},
            {
                "id": "use",
                "if": "${{ steps.alpha-beta.outputs.x }}",
                "activity": "ns/t@1",
            },
        ],
    )
    with pytest.raises(CelNameBindingError) as exc:
        validate_expressions(normalize_workflow(doc))
    # Either the `alpha-beta` second-level check fires (it would
    # not match `alpha`) or the bare `beta` root check fires —
    # either way at least one issue should mention brackets or the
    # missing root.
    msgs = " ".join(i.message for i in exc.value.issues)
    assert "bracket" in msgs or "unknown" in msgs


def test_validate_expressions_rejects_item_outside_foreach() -> None:
    doc = _wf(
        [
            {
                "id": "s",
                "if": "${{ item.value }}",  # no forEach in this step
                "activity": "ns/t@1",
            },
        ],
    )
    with pytest.raises(CelNameBindingError) as exc:
        validate_expressions(normalize_workflow(doc))
    assert any("item" in i.message for i in exc.value.issues)


def test_validate_expressions_let_cannot_see_later_let() -> None:
    """A `let` entry can NOT reference a later-declared `let`."""
    doc = _wf(
        [
            {
                "id": "s",
                "let": {
                    # `a` comes first in sorted order but references `z`
                    "a": "${{ let.z }}",
                    "z": "${{ 1 }}",
                },
            },
        ],
    )
    with pytest.raises(CelNameBindingError) as exc:
        validate_expressions(normalize_workflow(doc))
    assert any("let" in i.message and "z" in i.message for i in exc.value.issues)


def test_validate_expressions_collects_multiple_binding_errors() -> None:
    doc = _wf(
        [
            {
                "id": "s",
                "if": "${{ inputs.bad }}",
                "activity": "ns/t@1",
                "with": {"v": "${{ steps.absent.outputs.x }}"},
            },
        ],
    )
    with pytest.raises(CelNameBindingError) as exc:
        validate_expressions(normalize_workflow(doc))
    assert len(exc.value.issues) >= 2


# ---------------------------------------------------------------------------
# Template
# ---------------------------------------------------------------------------


def test_validate_template_expressions_accepts_placeholder_refs() -> None:
    doc = {
        "apiVersion": "custos.dev/v1",
        "kind": "WorkflowTemplate",
        "metadata": {"name": "t"},
        "spec": {
            "placeholders": [
                {"name": "registryConnector", "type": "connectorRef", "connectorType": "oci"},
                {"name": "scanActivity", "type": "activityRef", "activityType": "scan"},
            ],
            "workflow": {
                "steps": [
                    {
                        "id": "scan",
                        "activity": "${{ placeholders.scanActivity }}",
                        "connector": "${{ placeholders.registryConnector }}",
                    },
                ],
            },
        },
    }
    validate_template_expressions(normalize_template(doc))


def test_validate_template_expressions_rejects_undeclared_placeholder() -> None:
    doc = {
        "apiVersion": "custos.dev/v1",
        "kind": "WorkflowTemplate",
        "metadata": {"name": "t"},
        "spec": {
            "placeholders": [
                {"name": "declared", "type": "string"},
            ],
            "workflow": {
                "steps": [
                    {
                        "id": "s",
                        "activity": "ns/t@1",
                        "with": {"v": "${{ placeholders.undeclared }}"},
                    },
                ],
            },
        },
    }
    with pytest.raises(CelNameBindingError) as exc:
        validate_template_expressions(normalize_template(doc))
    assert any("undeclared" in i.message for i in exc.value.issues)


def test_discover_template_slots_includes_inner_workflow_paths() -> None:
    doc = {
        "apiVersion": "custos.dev/v1",
        "kind": "WorkflowTemplate",
        "metadata": {"name": "t"},
        "spec": {
            "placeholders": [{"name": "x", "type": "string"}],
            "workflow": {
                "steps": [
                    {
                        "id": "s",
                        "activity": "ns/t@1",
                        "with": {"v": "${{ placeholders.x }}"},
                    },
                ],
            },
        },
    }
    slots = discover_template_slots(normalize_template(doc))
    assert [s.path for s in slots] == ["spec.workflow.steps[0].with.v"]

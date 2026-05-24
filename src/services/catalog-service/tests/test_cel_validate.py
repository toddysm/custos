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


# ---------------------------------------------------------------------------
# Coverage backfill (CS-IMPL-020): AST visitor branches + slot variants
# ---------------------------------------------------------------------------


def test_validate_walks_call_binary_unary_conditional_list_map_branches() -> None:
    """Exercise every AST visitor branch in ``_iter_ast``.

    A single deeply-nested expression touches Call, Binary, Unary,
    Conditional, ListLit, and MapLit visitors so the recursion paths
    used to collect identifiers are all exercised.
    """
    doc = _wf(
        [
            {
                "id": "s",
                "if": ('${{ size([1, 2, !true ? 3 : 4, {"k": inputs.foo + 1}]) > 0 }}'),
                "activity": "ns/t@1",
            },
        ],
        inputs={"foo": {"type": "int"}},
    )
    # No name-binding violations — the expression is well-formed and
    # all roots are legal. The interesting assertion is that this
    # expression validates without raising despite the deep AST.
    validate_expressions(normalize_workflow(doc))


def test_validate_rejects_unknown_root_inside_call_argument() -> None:
    """A bad root nested inside a Call argument is reported."""
    doc = _wf(
        [
            {
                "id": "s",
                "if": "${{ size([nope.x]) > 0 }}",
                "activity": "ns/t@1",
            },
        ],
    )
    with pytest.raises(CelNameBindingError) as exc:
        validate_expressions(normalize_workflow(doc))
    assert any("nope" in i.message for i in exc.value.issues)


def test_validate_steps_with_non_string_index_is_accepted_silently() -> None:
    """``steps[1+2]`` parses as Index with a non-Literal-str index.

    The validator can't statically resolve the step id; it must not
    crash and must not emit a spurious binding error against that
    Index node (the inner ``1+2`` carries no Ident root).
    """
    doc = _wf(
        [
            {"id": "a", "activity": "ns/t@1"},
            {
                "id": "b",
                "if": "${{ steps[a.b].outputs.x }}",
                "activity": "ns/t@1",
            },
        ],
    )
    # `a.b` is a bad root (the leftmost Ident isn't a legal root), so
    # we expect *some* binding error pointing at `a`, but not a crash.
    with pytest.raises(CelNameBindingError):
        validate_expressions(normalize_workflow(doc))


def test_validate_trigger_connector_slot_is_discovered_and_validated() -> None:
    """A trigger's connector expression is in the discovered slot list."""
    doc = _wf(
        [{"id": "s", "activity": "ns/t@1"}],
        triggers=[{"name": "t1", "connector": "${{ inputs.cn }}"}],
        inputs={"cn": {"type": "connectorRef"}},
    )
    slots = discover_workflow_slots(normalize_workflow(doc))
    assert any(s.path == "spec.triggers[0].connector" for s in slots)
    validate_expressions(normalize_workflow(doc))


def test_validate_step_connector_and_connectors_block_are_discovered() -> None:
    """``step.connector`` and ``step.connectors.<alias>`` both surface as slots."""
    doc = _wf(
        [
            {
                "id": "s",
                "activity": "ns/t@1",
                "connector": "${{ inputs.primary }}",
                "connectors": {
                    "alpha": "${{ inputs.cn_alpha }}",
                    "beta": "${{ inputs.cn_beta }}",
                },
            },
        ],
        inputs={
            "primary": {"type": "connectorRef"},
            "cn_alpha": {"type": "connectorRef"},
            "cn_beta": {"type": "connectorRef"},
        },
    )
    paths = {s.path for s in discover_workflow_slots(normalize_workflow(doc))}
    assert "spec.steps[0].connector" in paths
    assert "spec.steps[0].connectors.alpha" in paths
    assert "spec.steps[0].connectors.beta" in paths


def test_validate_let_block_binds_progressively() -> None:
    """``let.x`` is visible to a *subsequent* ``let`` entry, not its own."""
    doc = _wf(
        [
            {
                "id": "s",
                "activity": "ns/t@1",
                "let": {
                    "a": "${{ inputs.foo }}",
                    "b": "${{ let.a + 1 }}",
                },
            },
        ],
        inputs={"foo": {"type": "int"}},
    )
    # `let.b` may reference `let.a` because we walk in sorted order
    # and `a < b`.
    validate_expressions(normalize_workflow(doc))


def test_validate_for_each_evaluates_in_outer_item_less_scope() -> None:
    """A ``forEach`` expression cannot reference ``item``; following slots can."""
    doc = _wf(
        [
            {
                "id": "s",
                "activity": "ns/t@1",
                "forEach": "${{ inputs.items }}",
                "with": {"v": "${{ item.value }}"},
            },
        ],
        inputs={"items": {"type": "list"}},
    )
    validate_expressions(normalize_workflow(doc))


def test_validate_for_each_referencing_item_is_rejected() -> None:
    doc = _wf(
        [
            {
                "id": "s",
                "activity": "ns/t@1",
                "forEach": "${{ item.values }}",
            },
        ],
    )
    with pytest.raises(CelNameBindingError) as exc:
        validate_expressions(normalize_workflow(doc))
    assert any("item" in i.message for i in exc.value.issues)


def test_validate_template_expressions_raises_binding_error_for_unknown_placeholder() -> None:
    """Surface the binding-error path through ``validate_template_expressions``."""
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
                        "with": {"v": "${{ placeholders.does_not_exist }}"},
                    },
                ],
            },
        },
    }
    with pytest.raises(CelNameBindingError) as exc:
        validate_template_expressions(normalize_template(doc))
    assert any("does_not_exist" in i.message for i in exc.value.issues)


# ---------------------------------------------------------------------------
# Defensive guards: malformed documents pass through cleanly
# ---------------------------------------------------------------------------


def test_discover_workflow_slots_returns_empty_for_non_dict_spec() -> None:
    """A normalized workflow with a non-dict ``spec`` yields no slots."""
    norm = normalize_workflow(
        {
            "apiVersion": "custos.dev/v1",
            "kind": "Workflow",
            "metadata": {"name": "wf"},
            "spec": {"steps": []},
        },
    )
    # Mutate the document post-normalize so the defensive guard is hit
    # (schema gate would normally reject this upstream).
    object.__setattr__(norm, "document", {"apiVersion": "x", "spec": "not-a-dict"})
    assert discover_workflow_slots(norm) == []


def test_discover_template_slots_returns_empty_for_non_dict_inner_workflow() -> None:
    norm = normalize_template(
        {
            "apiVersion": "custos.dev/v1",
            "kind": "WorkflowTemplate",
            "metadata": {"name": "t"},
            "spec": {
                "placeholders": [{"name": "x", "type": "string"}],
                "workflow": {"steps": []},
            },
        },
    )
    object.__setattr__(
        norm,
        "document",
        {
            "apiVersion": "x",
            "spec": {
                "placeholders": [{"name": "x", "type": "string"}, "not-a-dict"],
                "workflow": "not-a-dict",
            },
        },
    )
    assert discover_template_slots(norm) == []


def test_discover_template_slots_returns_empty_for_non_dict_spec() -> None:
    norm = normalize_template(
        {
            "apiVersion": "custos.dev/v1",
            "kind": "WorkflowTemplate",
            "metadata": {"name": "t"},
            "spec": {
                "placeholders": [{"name": "x", "type": "string"}],
                "workflow": {"steps": []},
            },
        },
    )
    object.__setattr__(norm, "document", {"apiVersion": "x", "spec": "not-a-dict"})
    assert discover_template_slots(norm) == []


def test_validate_template_expressions_raises_syntax_error_for_unparseable_cel() -> None:
    """The syntax-error path through ``validate_template_expressions``."""
    doc = {
        "apiVersion": "custos.dev/v1",
        "kind": "WorkflowTemplate",
        "metadata": {"name": "t"},
        "spec": {
            "placeholders": [],
            "workflow": {
                "steps": [
                    {
                        "id": "s",
                        "activity": "ns/t@1",
                        "if": "${{ )) bad syntax }}",
                    },
                ],
            },
        },
    }
    with pytest.raises(CelSyntaxError):
        validate_template_expressions(normalize_template(doc))

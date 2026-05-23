"""Tests for :mod:`custos_catalog.template_engine` (CS-IMPL-013)."""

from __future__ import annotations

from typing import Any

import pytest

from custos_catalog.template_engine import (
    TemplateRenderError,
    render,
)


def _template(workflow_body: dict[str, Any]) -> dict[str, Any]:
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "WorkflowTemplate",
        "metadata": {"name": "my-tmpl", "workspace": "ws-1"},
        "spec": {
            "placeholders": [],
            "workflow": workflow_body,
        },
    }


def test_render_substitutes_string_placeholder() -> None:
    template_doc = _template({"inputs": {"topic": "${{ placeholders.topic }}"}})
    out = render(template_doc, {"topic": "hello"}, target_workflow_name="wf-a")
    assert out["spec"] == {"inputs": {"topic": "hello"}}


def test_render_preserves_integer_binding_type() -> None:
    template_doc = _template({"settings": {"retries": "${{ placeholders.retries }}"}})
    out = render(template_doc, {"retries": 5}, target_workflow_name="wf-a")
    assert out["spec"]["settings"]["retries"] == 5
    assert isinstance(out["spec"]["settings"]["retries"], int)


def test_render_preserves_boolean_binding_type() -> None:
    template_doc = _template({"settings": {"strict": "${{ placeholders.strict }}"}})
    out = render(template_doc, {"strict": True}, target_workflow_name="wf-a")
    assert out["spec"]["settings"]["strict"] is True


def test_render_preserves_number_binding_type() -> None:
    template_doc = _template({"settings": {"ratio": "${{ placeholders.ratio }}"}})
    out = render(template_doc, {"ratio": 0.25}, target_workflow_name="wf-a")
    assert out["spec"]["settings"]["ratio"] == 0.25
    assert isinstance(out["spec"]["settings"]["ratio"], float)


def test_render_preserves_json_binding_structure() -> None:
    template_doc = _template({"settings": {"opts": "${{ placeholders.opts }}"}})
    out = render(
        template_doc,
        {"opts": {"a": [1, 2, 3], "b": {"nested": True}}},
        target_workflow_name="wf-a",
    )
    assert out["spec"]["settings"]["opts"] == {"a": [1, 2, 3], "b": {"nested": True}}


def test_render_substitutes_activity_ref_placeholder() -> None:
    template_doc = _template(
        {
            "steps": [
                {
                    "id": "scan",
                    "activity": "${{ placeholders.scanActivity }}",
                },
            ],
        },
    )
    out = render(
        template_doc,
        {"scanActivity": "custos.builtin/vuln-scan@2.0.0"},
        target_workflow_name="wf-a",
    )
    assert out["spec"]["steps"][0]["activity"] == "custos.builtin/vuln-scan@2.0.0"


def test_render_accepts_whitespace_inside_token() -> None:
    template_doc = _template({"x": "${{   placeholders.topic   }}"})
    out = render(template_doc, {"topic": "v"}, target_workflow_name="wf-a")
    assert out["spec"]["x"] == "v"


def test_render_accepts_whitespace_around_token() -> None:
    template_doc = _template({"x": "  ${{ placeholders.topic }}  "})
    out = render(template_doc, {"topic": "v"}, target_workflow_name="wf-a")
    assert out["spec"]["x"] == "v"


def test_render_passes_through_non_placeholder_cel_expressions() -> None:
    # Workflow-level CEL expressions (``inputs.*``, ``steps.*``) must
    # not be touched; only ``placeholders.*`` tokens are substituted.
    template_doc = _template(
        {
            "steps": [
                {
                    "id": "scan",
                    "activity": "${{ placeholders.act }}",
                    "with": {"target": "${{ inputs.target }}"},
                },
                {
                    "id": "use",
                    "with": {"in": "${{ steps.scan.outputs.result }}"},
                },
            ],
        },
    )
    out = render(template_doc, {"act": "x@1"}, target_workflow_name="wf-a")
    assert out["spec"]["steps"][0]["activity"] == "x@1"
    assert out["spec"]["steps"][0]["with"]["target"] == "${{ inputs.target }}"
    assert out["spec"]["steps"][1]["with"]["in"] == "${{ steps.scan.outputs.result }}"


def test_render_emits_workflow_envelope() -> None:
    template_doc = _template({"steps": [{"id": "noop"}]})
    out = render(template_doc, {}, target_workflow_name="my-wf")
    assert out["apiVersion"] == "custos.dev/v1"
    assert out["kind"] == "Workflow"
    assert out["metadata"]["name"] == "my-wf"
    assert out["metadata"]["workspace"] == "ws-1"
    assert out["spec"] == {"steps": [{"id": "noop"}]}


def test_render_overrides_target_name_over_template_metadata_name() -> None:
    template_doc = _template({"steps": [{"id": "noop"}]})
    out = render(template_doc, {}, target_workflow_name="explicit-name")
    assert out["metadata"]["name"] == "explicit-name"


def test_render_omits_workspace_when_template_lacks_one() -> None:
    template_doc = {
        "apiVersion": "custos.dev/v1",
        "kind": "WorkflowTemplate",
        "metadata": {"name": "x"},
        "spec": {"placeholders": [], "workflow": {"steps": []}},
    }
    out = render(template_doc, {}, target_workflow_name="wf-a")
    assert "workspace" not in out["metadata"]


def test_render_unbound_placeholder_raises() -> None:
    template_doc = _template({"x": "${{ placeholders.missing }}"})
    with pytest.raises(TemplateRenderError) as exc:
        render(template_doc, {}, target_workflow_name="wf-a")
    issues = exc.value.issues
    assert len(issues) == 1
    assert issues[0].code == "unbound_placeholder"
    assert "missing" in issues[0].message
    assert issues[0].path.endswith("/x")


def test_render_collects_multiple_unbound_placeholders() -> None:
    template_doc = _template(
        {
            "a": "${{ placeholders.x }}",
            "b": "${{ placeholders.y }}",
            "c": "${{ placeholders.z }}",
        },
    )
    with pytest.raises(TemplateRenderError) as exc:
        render(template_doc, {"x": "ok"}, target_workflow_name="wf-a")
    codes = {i.code for i in exc.value.issues}
    assert codes == {"unbound_placeholder"}
    assert len(exc.value.issues) == 2


def test_render_embedded_token_rejected() -> None:
    template_doc = _template({"x": "prefix-${{ placeholders.foo }}-suffix"})
    with pytest.raises(TemplateRenderError) as exc:
        render(template_doc, {"foo": "v"}, target_workflow_name="wf-a")
    assert exc.value.issues[0].code == "embedded_placeholder"
    assert "foo" in exc.value.issues[0].message


def test_render_walks_lists_and_records_index_in_path() -> None:
    template_doc = _template(
        {
            "steps": [
                {"id": "a"},
                {"activity": "${{ placeholders.act }}"},
            ],
        },
    )
    out = render(template_doc, {"act": "x@1"}, target_workflow_name="wf-a")
    assert out["spec"]["steps"][1]["activity"] == "x@1"


def test_render_unbound_path_contains_step_index() -> None:
    template_doc = _template(
        {
            "steps": [
                {"id": "a"},
                {"activity": "${{ placeholders.act }}"},
            ],
        },
    )
    with pytest.raises(TemplateRenderError) as exc:
        render(template_doc, {}, target_workflow_name="wf-a")
    assert "steps/1/activity" in exc.value.issues[0].path


def test_render_does_not_substitute_non_string_scalars() -> None:
    template_doc = _template(
        {
            "settings": {
                "retries": 3,
                "strict": True,
                "ratio": 0.5,
                "null": None,
            },
        },
    )
    out = render(template_doc, {}, target_workflow_name="wf-a")
    assert out["spec"]["settings"] == {
        "retries": 3,
        "strict": True,
        "ratio": 0.5,
        "null": None,
    }


def test_render_drops_placeholders_block_from_output() -> None:
    template_doc = {
        "apiVersion": "custos.dev/v1",
        "kind": "WorkflowTemplate",
        "metadata": {"name": "t", "workspace": "ws-1"},
        "spec": {
            "placeholders": [{"name": "x", "type": "string"}],
            "workflow": {"x": "${{ placeholders.x }}"},
        },
    }
    out = render(template_doc, {"x": "v"}, target_workflow_name="wf-a")
    # Output is the *Workflow* envelope — no spec.placeholders.
    assert "placeholders" not in out["spec"]
    assert out["spec"]["x"] == "v"

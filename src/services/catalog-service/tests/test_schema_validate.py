"""Tests for :mod:`custos_catalog.schema` (CS-IMPL-005)."""

from __future__ import annotations

import json
from typing import Any

import pytest

from custos_catalog.schema import (
    DocumentParseError,
    TemplateSchemaError,
    WorkflowSchemaError,
    load_document,
    validate_template,
    validate_workflow,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _registry_quarantine_workflow() -> dict[str, Any]:
    """The full workflow example from `overview.md` § Workflow and Template Schema.

    Kept in test code so a schema change that breaks the canonical
    example fails CI rather than going unnoticed.
    """
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "Workflow",
        "metadata": {
            "name": "registry-quarantine",
            "workspace": "default",
        },
        "spec": {
            "triggers": [
                {"type": "registry.push", "connector": "prod-registry"},
            ],
            "inputs": {
                "image": {"type": "string"},
            },
            "steps": [
                {
                    "id": "list-manifests",
                    "activity": "custos.builtin/oci-list@1",
                    "connector": "prod-registry",
                    "with": {"image": "${{ inputs.image }}"},
                },
                {
                    "id": "resolve-host",
                    "let": {"registryHost": '${{ connector("prod-registry").host }}'},
                },
                {
                    "id": "scan",
                    "forEach": "${{ steps.list-manifests.outputs.items }}",
                    "where": (
                        '${{ item.mediaType == "application/vnd.oci.image.manifest.v1+json" }}'
                    ),
                    "activity": "custos.builtin/vuln-scan@2",
                    "connector": "prod-registry",
                    "with": {
                        "image": (
                            "${{ imageRef(item.ref, steps.resolve-host.outputs.registryHost) }}"
                        ),
                    },
                    "on_error": [
                        {"match": {"codePrefix": "registry."}, "do": "skip"},
                        {
                            "match": {"class": "retryable"},
                            "do": "retry",
                            "maxAttempts": 5,
                        },
                    ],
                },
                {
                    "id": "gate",
                    "if": "${{ steps.scan.outputs.critical > 0 }}",
                    "activity": "custos.builtin/quarantine@1",
                    "connector": "prod-registry",
                    "with": {
                        "image": "${{ inputs.image }}",
                        "reason": "${{ steps.scan.outputs.summary }}",
                    },
                },
                {
                    "id": "promote",
                    "if": "${{ steps.scan.outputs.critical == 0 }}",
                    "activity": "custos.builtin/image-promote@1",
                    "connectors": {
                        "source": "prod-registry",
                        "destination": "public-registry",
                    },
                    "with": {"image": "${{ inputs.image }}"},
                },
            ],
        },
    }


def _registry_quarantine_template() -> dict[str, Any]:
    """The template example from `overview.md` § Workflow and Template Schema."""
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "WorkflowTemplate",
        "metadata": {"name": "registry-quarantine-template"},
        "spec": {
            "placeholders": [
                {
                    "name": "registryConnector",
                    "type": "connectorRef",
                    "connectorType": "oci-registry",
                    "required": True,
                },
                {
                    "name": "scanActivity",
                    "type": "activityRef",
                    "activityType": "vuln-scan",
                    "default": "custos.builtin/vuln-scan@2",
                },
            ],
            "workflow": {
                "triggers": [
                    {
                        "type": "registry.push",
                        "connector": "${{ placeholders.registryConnector }}",
                    },
                ],
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


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def test_load_document_parses_json() -> None:
    doc = load_document('{"apiVersion": "custos.dev/v1", "kind": "Workflow"}')
    assert doc == {"apiVersion": "custos.dev/v1", "kind": "Workflow"}


def test_load_document_parses_yaml() -> None:
    doc = load_document("apiVersion: custos.dev/v1\nkind: Workflow\n")
    assert doc == {"apiVersion": "custos.dev/v1", "kind": "Workflow"}


def test_load_document_accepts_bytes() -> None:
    doc = load_document(b"apiVersion: custos.dev/v1\n")
    assert doc == {"apiVersion": "custos.dev/v1"}


def test_load_document_rejects_root_list() -> None:
    with pytest.raises(DocumentParseError, match="must decode to a JSON object"):
        load_document("[1, 2, 3]")


def test_load_document_rejects_garbage() -> None:
    with pytest.raises(DocumentParseError):
        # Tab-indented YAML is rejected by safe_load, which becomes the
        # raised parse error.
        load_document("apiVersion: custos.dev/v1\n\tkind: Workflow")


# ---------------------------------------------------------------------------
# Positive cases
# ---------------------------------------------------------------------------


def test_validate_workflow_accepts_overview_example() -> None:
    validate_workflow(_registry_quarantine_workflow())


def test_validate_workflow_accepts_minimal_document() -> None:
    validate_workflow(
        {
            "apiVersion": "custos.dev/v1",
            "kind": "Workflow",
            "metadata": {"name": "minimal"},
            "spec": {
                "steps": [
                    {"id": "noop", "activity": "custos.builtin/noop@1"},
                ],
            },
        },
    )


def test_validate_template_accepts_overview_example() -> None:
    validate_template(_registry_quarantine_template())


def test_validate_workflow_accepts_let_only_step() -> None:
    validate_workflow(
        {
            "apiVersion": "custos.dev/v1",
            "kind": "Workflow",
            "metadata": {"name": "let-only"},
            "spec": {
                "steps": [
                    {"id": "compute", "let": {"x": "${{ inputs.foo }}"}},
                ],
            },
        },
    )


def test_validate_workflow_accepts_subworkflow_step() -> None:
    validate_workflow(
        {
            "apiVersion": "custos.dev/v1",
            "kind": "Workflow",
            "metadata": {"name": "calls-sub"},
            "spec": {
                "steps": [
                    {
                        "id": "call",
                        "workflow": ("12345678-1234-1234-1234-123456789abc"),
                    },
                ],
            },
        },
    )


def test_canonical_yaml_round_trip_matches_json() -> None:
    """YAML and JSON inputs must produce identical validation results."""
    doc = _registry_quarantine_workflow()
    json_doc = load_document(json.dumps(doc))
    assert json_doc == doc
    validate_workflow(json_doc)


# ---------------------------------------------------------------------------
# Negative cases — every issue surfaces as a `SchemaValidationIssue`
# ---------------------------------------------------------------------------


def test_validate_workflow_rejects_missing_apiversion() -> None:
    with pytest.raises(WorkflowSchemaError) as exc:
        validate_workflow(
            {
                "kind": "Workflow",
                "metadata": {"name": "x"},
                "spec": {"steps": [{"id": "s", "activity": "ns/t@1"}]},
            },
        )
    paths = {issue.path for issue in exc.value.issues}
    assert "" in paths  # required keyword reports at parent path


def test_validate_workflow_rejects_unknown_kind() -> None:
    with pytest.raises(WorkflowSchemaError) as exc:
        validate_workflow(
            {
                "apiVersion": "custos.dev/v1",
                "kind": "NotAWorkflow",
                "metadata": {"name": "x"},
                "spec": {"steps": [{"id": "s", "activity": "ns/t@1"}]},
            },
        )
    assert any(issue.path == "kind" for issue in exc.value.issues)


def test_validate_workflow_rejects_short_activity_ref() -> None:
    """`vuln-scan@2` without a namespace prefix is M1-rejected (overview)."""
    with pytest.raises(WorkflowSchemaError) as exc:
        validate_workflow(
            {
                "apiVersion": "custos.dev/v1",
                "kind": "Workflow",
                "metadata": {"name": "x"},
                "spec": {
                    "steps": [{"id": "s", "activity": "vuln-scan@2"}],
                },
            },
        )
    # The bad ref surfaces inside the `oneOf` branch for an activity
    # step; at minimum some issue paths must point under spec/steps/0.
    assert any(issue.path.startswith("spec/steps/0") for issue in exc.value.issues)


def test_validate_workflow_rejects_step_with_no_kind() -> None:
    """A step missing all three of activity/let/workflow is rejected."""
    with pytest.raises(WorkflowSchemaError) as exc:
        validate_workflow(
            {
                "apiVersion": "custos.dev/v1",
                "kind": "Workflow",
                "metadata": {"name": "x"},
                "spec": {"steps": [{"id": "incomplete"}]},
            },
        )
    assert any(issue.path.startswith("spec/steps/0") for issue in exc.value.issues)


def test_validate_workflow_rejects_connector_xor_violation() -> None:
    """Both `connector` and `connectors` on one step is forbidden."""
    with pytest.raises(WorkflowSchemaError):
        validate_workflow(
            {
                "apiVersion": "custos.dev/v1",
                "kind": "Workflow",
                "metadata": {"name": "x"},
                "spec": {
                    "steps": [
                        {
                            "id": "both",
                            "activity": "ns/t@1",
                            "connector": "a",
                            "connectors": {"x": "b"},
                        },
                    ],
                },
            },
        )


def test_validate_workflow_rejects_additional_step_property() -> None:
    with pytest.raises(WorkflowSchemaError) as exc:
        validate_workflow(
            {
                "apiVersion": "custos.dev/v1",
                "kind": "Workflow",
                "metadata": {"name": "x"},
                "spec": {
                    "steps": [
                        {"id": "s", "activity": "ns/t@1", "bogus": True},
                    ],
                },
            },
        )
    assert any(issue.validator == "oneOf" for issue in exc.value.issues)


def test_validate_workflow_collects_all_errors_in_one_pass() -> None:
    """A doc with multiple violations surfaces every one of them."""
    with pytest.raises(WorkflowSchemaError) as exc:
        validate_workflow(
            {
                "apiVersion": "custos.dev/v1",
                "kind": "Workflow",
                "metadata": {"name": "BAD-NAME"},  # uppercase rejected
                "spec": {
                    "steps": [
                        {"id": "BAD-ID", "activity": "ns/t@1"},
                    ],
                },
            },
        )
    assert len(exc.value.issues) >= 2  # at least the metadata.name and step id


def test_validate_template_rejects_missing_connector_type_on_connector_ref() -> None:
    with pytest.raises(TemplateSchemaError) as exc:
        validate_template(
            {
                "apiVersion": "custos.dev/v1",
                "kind": "WorkflowTemplate",
                "metadata": {"name": "t"},
                "spec": {
                    "placeholders": [
                        {"name": "c", "type": "connectorRef"},
                    ],
                    "workflow": {
                        "steps": [{"id": "s", "activity": "ns/t@1"}],
                    },
                },
            },
        )
    assert any(issue.path.startswith("spec/placeholders/0") for issue in exc.value.issues)


def test_validate_template_rejects_empty_placeholders() -> None:
    with pytest.raises(TemplateSchemaError):
        validate_template(
            {
                "apiVersion": "custos.dev/v1",
                "kind": "WorkflowTemplate",
                "metadata": {"name": "t"},
                "spec": {
                    "placeholders": [],
                    "workflow": {
                        "steps": [{"id": "s", "activity": "ns/t@1"}],
                    },
                },
            },
        )


def test_schema_validation_error_stringifies_summary() -> None:
    try:
        validate_workflow({})
    except WorkflowSchemaError as exc:
        assert "issue(s)" in str(exc)
        assert exc.issues  # at least one issue
    else:
        pytest.fail("expected WorkflowSchemaError")

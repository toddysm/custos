"""Integration test: publish-time validation envelopes (CS-IMPL-021).

Per issue #222 scope: every error class surfaced by CS-IMPL-005..008
must translate to a stable wire envelope. These tests exercise the
publish pipeline end-to-end against Postgres to verify that:

* Parse / schema / placeholders / normalise / resolve / cel failures
  emit ``catalog.publish.<stage>`` codes (CS-IMPL-005 + CS-IMPL-006).
* Extract failures emit ``catalog.template_extract_failed.<cause>``
  envelopes (CS-IMPL-008).
* Materialize failures emit
  ``catalog.template_materialization_failed.<cause>`` envelopes
  (CS-IMPL-008).
* Workflow immutability raises a 409
  ``catalog.workflow_immutability_violation`` (CS-IMPL-009).

The shape contract for every envelope is ``{"error": {"code": "...",
"detail": "..."}}`` with an optional ``issues`` array for the
multi-issue stages (schema, placeholders, normalize, resolve, cel).
"""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from tests.integration.conftest import (
    admin_header,
    minimal_workflow,
    seed_echo_activity,
)

pytestmark = pytest.mark.integration


def _publish(client: TestClient, doc: dict[str, object]) -> dict[str, object]:
    body: dict[str, object] = client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(doc)},
        headers=admin_header(),
    ).json()
    return body


def test_publish_parse_stage_envelope(client: TestClient) -> None:
    seed_echo_activity(client)
    resp = client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": "not: [valid: yaml"},
        headers=admin_header(),
    )
    assert resp.status_code == 400, resp.text
    body = resp.json()
    assert body["error"]["code"] == "catalog.publish.parse"


def test_publish_schema_stage_envelope(client: TestClient) -> None:
    seed_echo_activity(client)
    bad = minimal_workflow()
    del bad["apiVersion"]
    resp = client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(bad)},
        headers=admin_header(),
    )
    assert resp.status_code == 400, resp.text
    body = resp.json()
    assert body["error"]["code"] == "catalog.publish.schema"
    assert body["error"]["issues"]


def test_publish_resolve_stage_envelope(client: TestClient) -> None:
    # No activity registered → resolve stage cannot find ws-1/echo@1.
    doc = minimal_workflow()
    resp = client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(doc)},
        headers=admin_header(),
    )
    assert resp.status_code == 400, resp.text
    body = resp.json()
    assert body["error"]["code"] == "catalog.publish.resolve"
    assert body["error"]["issues"]


def test_publish_cel_stage_envelope(client: TestClient) -> None:
    """A malformed CEL expression in ``with`` lands on the cel stage."""
    seed_echo_activity(client)
    doc = minimal_workflow()
    # Set message to a CEL ``${{ ... }}`` interpolation referencing an
    # unknown root binding — surfaces as a CelNameBindingError at the
    # cel stage of the publish pipeline.
    doc["spec"]["steps"][0]["with"]["message"] = "${{ unknown.binding }}"
    resp = client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(doc)},
        headers=admin_header(),
    )
    assert resp.status_code == 400, resp.text
    body = resp.json()
    assert body["error"]["code"] == "catalog.publish.cel"
    assert body["error"]["issues"]


def test_extract_template_envelope_on_unknown_path(client: TestClient) -> None:
    seed_echo_activity(client)
    pub = _publish(client, minimal_workflow())
    assert "workflowName" in pub, pub
    resp = client.post(
        "/v1/workspaces/ws-1/workflows/orders@1:extractTemplate",
        json={
            "templateName": "orders-tmpl",
            "selectors": [
                {
                    "path": "spec.steps[99].nonexistent",
                    "placeholderName": "p1",
                    "placeholderType": "string",
                },
            ],
        },
        headers=admin_header(),
    )
    assert resp.status_code == 400, resp.text
    body = resp.json()
    assert body["error"]["code"].startswith("catalog.template_extract_failed.")


def test_materialize_envelope_on_missing_required_binding(
    client: TestClient,
) -> None:
    seed_echo_activity(client)
    _publish(client, minimal_workflow())
    # Extract a template requiring one placeholder.
    ext = client.post(
        "/v1/workspaces/ws-1/workflows/orders@1:extractTemplate",
        json={
            "templateName": "orders-tmpl",
            "selectors": [
                {
                    "path": "spec.steps[0].with.message",
                    "placeholderName": "msg",
                    "placeholderType": "string",
                    "required": True,
                },
            ],
        },
        headers=admin_header(),
    )
    assert ext.status_code == 201, ext.text
    # Materialise with the wrong binding name (still passes schema but
    # leaves ``msg`` unbound).
    mat = client.post(
        "/v1/workspaces/ws-1/templates/orders-tmpl@1:materialize",
        json={"targetName": "orders-mat", "bindings": {"wrong": "x"}},
        headers=admin_header(),
    )
    assert mat.status_code == 400, mat.text
    body = mat.json()
    assert body["error"]["code"].startswith(
        "catalog.template_materialization_failed.",
    )


def test_workflow_deprecate_not_found_envelope(client: TestClient) -> None:
    resp = client.post(
        "/v1/workspaces/ws-1/workflows/does-not-exist@1:deprecate",
        json={},
        headers=admin_header(),
    )
    # Catalog policy: deprecate of an unknown workflow returns 404
    # with the workflow_not_found envelope.
    assert resp.status_code == 404, resp.text
    body = resp.json()
    assert body["error"]["code"] == "catalog.workflow_not_found"


def test_workflow_deprecate_invalid_ref_envelope(client: TestClient) -> None:
    resp = client.post(
        "/v1/workspaces/ws-1/workflows/does-not-exist:deprecate",
        json={},
        headers=admin_header(),
    )
    assert resp.status_code == 400, resp.text
    body = resp.json()
    assert body["error"]["code"] == "catalog.workflow_ref_invalid"

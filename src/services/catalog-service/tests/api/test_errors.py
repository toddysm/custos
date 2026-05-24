"""Direct tests for the error-envelope handlers (CS-IMPL-017)."""

from __future__ import annotations

import json
from typing import Any

from fastapi.testclient import TestClient

from tests.api.conftest import (
    FakeCatalogStore,
    FakeDefinitionStore,
    FakeMetadataStore,
    admin_header,
    callctx_header,
    minimal_activity_manifest,
    minimal_connector_manifest,
    minimal_template,
    minimal_workflow,
    seed_builtin_echo,
)

# ---------------------------------------------------------------------------
# Generic envelope shape
# ---------------------------------------------------------------------------


def _assert_envelope(payload: dict[str, Any], code: str) -> dict[str, Any]:
    """Assert ``payload`` matches ``{"error": {"code", "detail", "issues"?}}``."""
    assert "error" in payload, payload
    err: dict[str, Any] = payload["error"]
    assert err["code"] == code, err
    assert isinstance(err["detail"], str)
    return err


# ---------------------------------------------------------------------------
# 422 — request validation
# ---------------------------------------------------------------------------


def test_request_validation_error_emits_422_envelope(client: TestClient) -> None:
    # Activity-types LIST requires both `namespace` and `type` query params.
    resp = client.get(
        "/v1/workspaces/ws-1/activity-types",
        headers=admin_header(),
    )
    assert resp.status_code == 422
    err = _assert_envelope(resp.json(), "catalog.request_invalid")
    assert err["issues"]


# ---------------------------------------------------------------------------
# 400 — manifest envelope failure (issues array)
# ---------------------------------------------------------------------------


def test_activity_manifest_invalid_emits_envelope_with_issues(
    client: TestClient,
) -> None:
    bad = minimal_activity_manifest()
    bad["apiVersion"] = "wrong"
    resp = client.post(
        "/v1/workspaces/ws-1/activity-types",
        json={"manifest": bad},
        headers=admin_header(),
    )
    assert resp.status_code == 400
    err = _assert_envelope(resp.json(), "catalog.activity_manifest_invalid")
    assert err["issues"]


def test_connector_manifest_invalid_emits_envelope_with_issues(
    client: TestClient,
) -> None:
    bad = minimal_connector_manifest()
    bad["apiVersion"] = "wrong"
    resp = client.post(
        "/v1/catalog/connector-types",
        json={"manifest": bad},
        headers=admin_header(),
    )
    assert resp.status_code == 400
    err = _assert_envelope(resp.json(), "catalog.connector_manifest_invalid")
    assert err["issues"]


def test_workflow_publish_validation_emits_envelope_with_issues(
    client: TestClient,
) -> None:
    bad = minimal_workflow()
    bad["spec"]["steps"] = []  # workflows must have at least one step
    resp = client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(bad)},
        headers=admin_header(),
    )
    assert resp.status_code == 400
    body = resp.json()
    assert body["error"]["code"].startswith("catalog.publish."), body
    assert body["error"]["issues"]


def test_template_publish_validation_emits_envelope_with_issues(
    client: TestClient,
) -> None:
    bad = minimal_template()
    bad["spec"]["steps"] = []
    resp = client.post(
        "/v1/workspaces/ws-1/templates",
        json={"definition": json.dumps(bad)},
        headers=admin_header(),
    )
    assert resp.status_code == 400
    body = resp.json()
    assert body["error"]["code"].startswith("catalog.publish."), body
    assert body["error"]["issues"]


# ---------------------------------------------------------------------------
# 404 — not-found envelopes for every manager class
# ---------------------------------------------------------------------------


def test_workflow_not_found_envelope(client: TestClient) -> None:
    resp = client.get(
        "/v1/workspaces/ws-1/workflows/missing@1",
        headers=admin_header(),
    )
    assert resp.status_code == 404
    _assert_envelope(resp.json(), "catalog.workflow_not_found")


def test_template_not_found_envelope(client: TestClient) -> None:
    resp = client.get(
        "/v1/workspaces/ws-1/templates/missing@1",
        headers=admin_header(),
    )
    assert resp.status_code == 404
    _assert_envelope(resp.json(), "catalog.template_not_found")


def test_activity_type_not_found_envelope(client: TestClient) -> None:
    resp = client.get(
        "/v1/workspaces/ws-1/activity-types/ws-1/missing@1.0.0",
        headers=admin_header(),
    )
    assert resp.status_code == 404
    _assert_envelope(resp.json(), "catalog.activity_type_not_found")


def test_connector_type_not_found_envelope(client: TestClient) -> None:
    resp = client.get(
        "/v1/catalog/connector-types/missing@1.0.0",
        headers=admin_header(),
    )
    assert resp.status_code == 404
    _assert_envelope(resp.json(), "catalog.connector_type_not_found")


# ---------------------------------------------------------------------------
# 409 — conflict envelopes
# ---------------------------------------------------------------------------


def test_activity_registry_conflict_emits_envelope_with_digests(
    client: TestClient,
) -> None:
    manifest = minimal_activity_manifest()
    client.post(
        "/v1/workspaces/ws-1/activity-types",
        json={"manifest": manifest},
        headers=admin_header(),
    )
    # Re-register the same triple with a modified manifest body so the
    # digest differs (this is the conflict the registry guards against).
    second = minimal_activity_manifest()
    second["spec"]["runtime"]["digest"] = "sha256:zzz"
    resp = client.post(
        "/v1/workspaces/ws-1/activity-types",
        json={"manifest": second},
        headers=admin_header(),
    )
    assert resp.status_code == 409
    err = _assert_envelope(resp.json(), "catalog.activity_type_digest_conflict")
    assert err["issues"]


def test_connector_registry_conflict_emits_envelope_with_digests(
    client: TestClient,
) -> None:
    manifest = minimal_connector_manifest()
    client.post(
        "/v1/catalog/connector-types",
        json={"manifest": manifest},
        headers=admin_header(),
    )
    second = minimal_connector_manifest()
    # Anything that survives normalisation but changes the digest works.
    second["spec"]["secretSchema"] = {"type": "object", "properties": {"x": {"type": "string"}}}
    resp = client.post(
        "/v1/catalog/connector-types",
        json={"manifest": second},
        headers=admin_header(),
    )
    assert resp.status_code == 409
    err = _assert_envelope(resp.json(), "catalog.connector_type_digest_conflict")
    assert err["issues"]


# ---------------------------------------------------------------------------
# 403 — namespace forbidden envelope (subclass dispatch)
# ---------------------------------------------------------------------------


def test_activity_namespace_forbidden_envelope(client: TestClient) -> None:
    resp = client.post(
        "/v1/workspaces/ws-1/activity-types",
        json={"manifest": minimal_activity_manifest(namespace="custos.builtin")},
        headers=admin_header(),
    )
    assert resp.status_code == 403
    _assert_envelope(resp.json(), "catalog.activity_namespace_forbidden")


# ---------------------------------------------------------------------------
# Call-context shim envelopes (workspace mismatch + permission denied)
# ---------------------------------------------------------------------------


def test_workspace_mismatch_403_envelope(client: TestClient) -> None:
    resp = client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(minimal_workflow())},
        headers=callctx_header(workspace_id="ws-other", permissions=["catalog:workflows:write"]),
    )
    assert resp.status_code == 403
    _assert_envelope(resp.json(), "catalog.workspace_mismatch")


def test_permission_denied_403_envelope(client: TestClient) -> None:
    resp = client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(minimal_workflow())},
        headers=callctx_header(workspace_id="ws-1", permissions=[]),
    )
    assert resp.status_code == 403
    _assert_envelope(resp.json(), "permission_denied")


# ---------------------------------------------------------------------------
# Materialization / Extraction wrappers re-raise inner manager codes
# ---------------------------------------------------------------------------


def test_extract_with_bad_selector_emits_envelope(
    client: TestClient,
    stores: tuple[FakeDefinitionStore, FakeCatalogStore, FakeMetadataStore],
) -> None:
    _, catalog_store, _ = stores
    seed_builtin_echo(catalog_store)
    client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(minimal_workflow())},
        headers=admin_header(),
    )
    # Path that doesn't resolve in the workflow body — should fail
    # extraction with a structured envelope.
    resp = client.post(
        "/v1/workspaces/ws-1/workflows/orders@1:extractTemplate",
        json={
            "selectors": [
                {
                    "path": "spec.does.not.exist",
                    "placeholderName": "x",
                    "placeholderType": "string",
                },
            ],
            "templateName": "broken-tmpl",
        },
        headers=admin_header(),
    )
    assert resp.status_code in (400, 422)
    body = resp.json()
    assert "error" in body

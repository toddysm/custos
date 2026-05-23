"""Connector-type REST route tests (CS-IMPL-017 — connector_types.py)."""

from __future__ import annotations

from fastapi.testclient import TestClient

from tests.api.conftest import (
    FakeCatalogStore,
    FakeDefinitionStore,
    admin_header,
    callctx_header,
    minimal_connector_manifest,
)


def _register(client: TestClient, **overrides: str) -> dict[str, str]:
    manifest = minimal_connector_manifest(**overrides)
    resp = client.post(
        "/v1/catalog/connector-types",
        json={"manifest": manifest},
        headers=admin_header(),
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


# ---------------------------------------------------------------------------
# Register
# ---------------------------------------------------------------------------


def test_register_returns_ref(client: TestClient) -> None:
    body = _register(client)
    assert body["type"] == "oci-registry"
    assert body["version"] == "1.0.0"
    assert body["digest"].startswith("sha256:")


def test_register_requires_write_permission(client: TestClient) -> None:
    resp = client.post(
        "/v1/catalog/connector-types",
        json={"manifest": minimal_connector_manifest()},
        headers=callctx_header(workspace_id="ws-1", permissions=["catalog:connector-types:read"]),
    )
    assert resp.status_code == 403


def test_register_manifest_envelope_failure_emits_envelope(client: TestClient) -> None:
    bad = minimal_connector_manifest()
    del bad["apiVersion"]
    resp = client.post(
        "/v1/catalog/connector-types",
        json={"manifest": bad},
        headers=admin_header(),
    )
    assert resp.status_code == 400
    body = resp.json()
    assert body["error"]["code"] == "catalog.connector_manifest_invalid"
    assert body["error"]["issues"]


# ---------------------------------------------------------------------------
# List
# ---------------------------------------------------------------------------


def test_list_returns_versions(client: TestClient) -> None:
    _register(client, version="1.0.0")
    _register(client, version="2.0.0")

    resp = client.get(
        "/v1/catalog/connector-types",
        params={"type": "oci-registry"},
        headers=admin_header(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["items"]) == 2


def test_list_requires_type_query_param(client: TestClient) -> None:
    resp = client.get(
        "/v1/catalog/connector-types",
        headers=admin_header(),
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Get
# ---------------------------------------------------------------------------


def test_get_returns_version_body(client: TestClient) -> None:
    _register(client)
    resp = client.get(
        "/v1/catalog/connector-types/oci-registry@1.0.0",
        headers=admin_header(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["type"] == "oci-registry"
    assert body["version"] == "1.0.0"
    assert body["normalizedManifest"]["kind"] == "ConnectorManifest"


def test_get_404_when_absent(client: TestClient) -> None:
    resp = client.get(
        "/v1/catalog/connector-types/missing@1.0.0",
        headers=admin_header(),
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "catalog.connector_type_not_found"


def test_get_400_on_malformed_ref(client: TestClient) -> None:
    resp = client.get(
        "/v1/catalog/connector-types/bogus",
        headers=admin_header(),
    )
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Deprecate
# ---------------------------------------------------------------------------


def test_deprecate_returns_status_ok(
    client: TestClient,
    stores: tuple[FakeDefinitionStore, FakeCatalogStore],
) -> None:
    _, catalog_store = stores
    _register(client)

    resp = client.post(
        "/v1/catalog/connector-types/oci-registry@1.0.0:deprecate",
        json={"reason": "obsolete"},
        headers=admin_header(),
    )
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}
    assert catalog_store.connector_deprecated == {"oci-registry": True}


def test_deprecate_404_when_absent(client: TestClient) -> None:
    resp = client.post(
        "/v1/catalog/connector-types/missing@1.0.0:deprecate",
        json={},
        headers=admin_header(),
    )
    assert resp.status_code == 404

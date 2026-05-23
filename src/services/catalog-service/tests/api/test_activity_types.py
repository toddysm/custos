"""Activity-type REST route tests (CS-IMPL-017 — activity_types.py)."""

from __future__ import annotations

from fastapi.testclient import TestClient

from tests.api.conftest import (
    FakeCatalogStore,
    FakeDefinitionStore,
    admin_header,
    callctx_header,
    minimal_activity_manifest,
)


def _register(client: TestClient, **overrides: str) -> dict[str, str]:
    manifest = minimal_activity_manifest(**overrides)
    resp = client.post(
        "/v1/workspaces/ws-1/activity-types",
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
    assert body == {
        "namespace": "ws-1",
        "type": "fetch-orders",
        "version": "1.0.0",
        "digest": body["digest"],
    }
    assert body["digest"].startswith("sha256:")


def test_register_rejects_workspace_mismatch(client: TestClient) -> None:
    resp = client.post(
        "/v1/workspaces/ws-1/activity-types",
        json={"manifest": minimal_activity_manifest()},
        headers=callctx_header(
            workspace_id="ws-other",
            permissions=["catalog:activity-types:write"],
        ),
    )
    assert resp.status_code == 403


def test_register_manifest_envelope_failure_emits_envelope(client: TestClient) -> None:
    bad = minimal_activity_manifest()
    del bad["apiVersion"]
    resp = client.post(
        "/v1/workspaces/ws-1/activity-types",
        json={"manifest": bad},
        headers=admin_header(),
    )
    assert resp.status_code == 400
    body = resp.json()
    assert body["error"]["code"] == "catalog.activity_manifest_invalid"
    assert body["error"]["issues"]


def test_register_namespace_forbidden_emits_403(client: TestClient) -> None:
    # ws-1 cannot publish into a reserved 'custos.builtin' namespace.
    resp = client.post(
        "/v1/workspaces/ws-1/activity-types",
        json={"manifest": minimal_activity_manifest(namespace="custos.builtin")},
        headers=admin_header(),
    )
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "catalog.activity_namespace_forbidden"


# ---------------------------------------------------------------------------
# List
# ---------------------------------------------------------------------------


def test_list_returns_versions(client: TestClient) -> None:
    _register(client, version="1.0.0")
    _register(client, version="1.1.0")

    resp = client.get(
        "/v1/workspaces/ws-1/activity-types",
        params={"namespace": "ws-1", "type": "fetch-orders"},
        headers=admin_header(),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert len(body["items"]) == 2
    # Design contract: list returns refs only — no normalizedManifest /
    # parentDeprecated / publishedAt leak. Keeps payload small for
    # authoring UIs that fan out across many (namespace, type) pairs.
    for item in body["items"]:
        assert set(item.keys()) == {"namespace", "type", "version", "digest"}


def test_list_requires_namespace_and_type(client: TestClient) -> None:
    resp = client.get(
        "/v1/workspaces/ws-1/activity-types",
        headers=admin_header(),
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Get
# ---------------------------------------------------------------------------


def test_get_returns_version_body(client: TestClient) -> None:
    _register(client)

    resp = client.get(
        "/v1/workspaces/ws-1/activity-types/ws-1/fetch-orders@1.0.0",
        headers=admin_header(),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["namespace"] == "ws-1"
    assert body["type"] == "fetch-orders"
    assert body["version"] == "1.0.0"
    assert body["normalizedManifest"]["kind"] == "ActivityManifest"


def test_get_404_when_absent(client: TestClient) -> None:
    resp = client.get(
        "/v1/workspaces/ws-1/activity-types/ws-1/missing@1.0.0",
        headers=admin_header(),
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "catalog.activity_type_not_found"


def test_get_400_on_malformed_ref(client: TestClient) -> None:
    resp = client.get(
        "/v1/workspaces/ws-1/activity-types/bogus",
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
        "/v1/workspaces/ws-1/activity-types/ws-1/fetch-orders@1.0.0:deprecate",
        json={"reason": "obsolete"},
        headers=admin_header(),
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"status": "ok"}
    assert catalog_store.activity_deprecated == {("ws-1", "fetch-orders"): True}


def test_deprecate_404_when_absent(client: TestClient) -> None:
    resp = client.post(
        "/v1/workspaces/ws-1/activity-types/ws-1/missing@1.0.0:deprecate",
        json={},
        headers=admin_header(),
    )
    assert resp.status_code == 404

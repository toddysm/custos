"""Internal RPC route tests (CS-IMPL-018)."""

from __future__ import annotations

import json

from fastapi.testclient import TestClient

from tests.api.conftest import (
    FakeCatalogStore,
    FakeDefinitionStore,
    admin_header,
    callctx_header,
    minimal_connector_manifest,
    minimal_workflow,
    seed_builtin_echo,
)

# ---------------------------------------------------------------------------
# GET /rpc/v1/workflow-versions/{id}
# ---------------------------------------------------------------------------


def test_rpc_get_workflow_version_returns_row(
    client: TestClient,
    stores: tuple[FakeDefinitionStore, FakeCatalogStore],
) -> None:
    _, catalog_store = stores
    seed_builtin_echo(catalog_store)
    client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(minimal_workflow())},
        headers=admin_header(),
    )

    resp = client.get(
        "/rpc/v1/workflow-versions/ws-1/orders@1",
        headers=callctx_header(
            workspace_id="ws-1",
            principal_id="workflow-service",
            permissions=["catalog:rpc:read"],
        ),
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["workspaceId"] == "ws-1"
    assert body["workflowName"] == "orders"
    assert body["version"] == 1
    assert body["document"]["kind"] == "Workflow"


def test_rpc_get_workflow_version_requires_permission(
    client: TestClient,
    stores: tuple[FakeDefinitionStore, FakeCatalogStore],
) -> None:
    _, catalog_store = stores
    seed_builtin_echo(catalog_store)
    client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(minimal_workflow())},
        headers=admin_header(),
    )

    resp = client.get(
        "/rpc/v1/workflow-versions/ws-1/orders@1",
        headers=callctx_header(workspace_id="ws-1", permissions=[]),
    )
    assert resp.status_code == 403


def test_rpc_get_workflow_version_400_on_malformed_id(client: TestClient) -> None:
    resp = client.get(
        "/rpc/v1/workflow-versions/no-slash",
        headers=callctx_header(
            workspace_id="ws-1", permissions=["catalog:rpc:read"]
        ),
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "catalog.workflow_version_id_invalid"


def test_rpc_get_workflow_version_404_when_missing(client: TestClient) -> None:
    resp = client.get(
        "/rpc/v1/workflow-versions/ws-1/missing@1",
        headers=callctx_header(
            workspace_id="ws-1", permissions=["catalog:rpc:read"]
        ),
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /rpc/v1/connector-types/{ref}
# ---------------------------------------------------------------------------


def test_rpc_resolve_connector_type_returns_row(
    client: TestClient,
) -> None:
    client.post(
        "/v1/catalog/connector-types",
        json={"manifest": minimal_connector_manifest()},
        headers=admin_header(),
    )

    resp = client.get(
        "/rpc/v1/connector-types/oci-registry@1.0.0",
        headers=callctx_header(
            workspace_id="ws-1",
            principal_id="workflow-service",
            permissions=["catalog:rpc:read"],
        ),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["type"] == "oci-registry"
    assert body["version"] == "1.0.0"
    assert body["normalizedManifest"]["kind"] == "ConnectorManifest"


def test_rpc_resolve_connector_type_400_on_malformed(client: TestClient) -> None:
    resp = client.get(
        "/rpc/v1/connector-types/bogus",
        headers=callctx_header(
            workspace_id="ws-1", permissions=["catalog:rpc:read"]
        ),
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "catalog.connector_type_ref_invalid"


def test_rpc_resolve_connector_type_404_when_missing(client: TestClient) -> None:
    resp = client.get(
        "/rpc/v1/connector-types/missing@1.0.0",
        headers=callctx_header(
            workspace_id="ws-1", permissions=["catalog:rpc:read"]
        ),
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "catalog.connector_type_not_found"

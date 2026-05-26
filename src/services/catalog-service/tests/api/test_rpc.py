"""Internal RPC route tests (CS-IMPL-018)."""

from __future__ import annotations

import json

from fastapi.testclient import TestClient

from tests.api.conftest import (
    FakeCatalogStore,
    FakeDefinitionStore,
    FakeMetadataStore,
    admin_header,
    callctx_header,
    minimal_connector_manifest,
    minimal_workflow,
    seed_builtin_echo,
)

IMAGE_REF = (
    "ghcr.io/custos/connector-oci-registry@sha256:"
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

# ---------------------------------------------------------------------------
# GET /rpc/v1/workflow-versions/{id}
# ---------------------------------------------------------------------------


def test_rpc_get_workflow_version_returns_row(
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
    stores: tuple[FakeDefinitionStore, FakeCatalogStore, FakeMetadataStore],
) -> None:
    _, catalog_store, _ = stores
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
        headers=callctx_header(workspace_id="ws-1", permissions=["catalog:rpc:read"]),
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "catalog.workflow_version_id_invalid"


def test_rpc_get_workflow_version_404_when_missing(client: TestClient) -> None:
    resp = client.get(
        "/rpc/v1/workflow-versions/ws-1/missing@1",
        headers=callctx_header(workspace_id="ws-1", permissions=["catalog:rpc:read"]),
    )
    assert resp.status_code == 404


def test_rpc_get_workflow_version_rejects_cross_workspace_without_explicit_permission(
    client: TestClient,
    stores: tuple[FakeDefinitionStore, FakeCatalogStore, FakeMetadataStore],
) -> None:
    """``catalog:rpc:read`` alone does not allow crossing the tenant boundary.

    A caller whose context lives in ``ws-2`` cannot resolve a workflow
    that belongs to ``ws-1`` just by crafting the id; the gateway must
    additionally grant ``catalog:rpc:cross-workspace-read``.
    """
    _, catalog_store, _ = stores
    seed_builtin_echo(catalog_store)
    client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(minimal_workflow())},
        headers=admin_header(),
    )

    resp = client.get(
        "/rpc/v1/workflow-versions/ws-1/orders@1",
        headers=callctx_header(
            workspace_id="ws-2",
            principal_id="rogue-rpc-caller",
            permissions=["catalog:rpc:read"],
        ),
    )
    assert resp.status_code == 403
    body = resp.json()
    assert body["error"]["code"] == "catalog.workspace_mismatch"
    assert "catalog:rpc:cross-workspace-read" in body["error"]["detail"]


def test_rpc_get_workflow_version_allows_cross_workspace_with_explicit_permission(
    client: TestClient,
    stores: tuple[FakeDefinitionStore, FakeCatalogStore, FakeMetadataStore],
) -> None:
    """Internal services with the cross-workspace grant can still fan out.

    The workflow runtime and activity dispatcher both operate under a
    system workspace while reading tenant workflows; the gateway
    issues them ``catalog:rpc:cross-workspace-read`` to authorise this
    explicitly.
    """
    _, catalog_store, _ = stores
    seed_builtin_echo(catalog_store)
    client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(minimal_workflow())},
        headers=admin_header(),
    )

    resp = client.get(
        "/rpc/v1/workflow-versions/ws-1/orders@1",
        headers=callctx_header(
            workspace_id="ws-system",
            principal_id="workflow-runtime",
            permissions=[
                "catalog:rpc:read",
                "catalog:rpc:cross-workspace-read",
            ],
        ),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["workspaceId"] == "ws-1"
    assert body["workflowName"] == "orders"
    assert body["version"] == 1


# ---------------------------------------------------------------------------
# GET /rpc/v1/connector-types/{ref}
# ---------------------------------------------------------------------------


def test_rpc_resolve_connector_type_returns_row(
    client: TestClient,
) -> None:
    client.post(
        "/v1/catalog/connector-types",
        json={"imageRef": IMAGE_REF, "manifest": minimal_connector_manifest()},
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
        headers=callctx_header(workspace_id="ws-1", permissions=["catalog:rpc:read"]),
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "catalog.connector_type_ref_invalid"


def test_rpc_resolve_connector_type_404_when_missing(client: TestClient) -> None:
    resp = client.get(
        "/rpc/v1/connector-types/missing@1.0.0",
        headers=callctx_header(workspace_id="ws-1", permissions=["catalog:rpc:read"]),
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "catalog.connector_type_not_found"

"""Workflow REST route tests (CS-IMPL-017 — workflows.py)."""

from __future__ import annotations

import json

from fastapi.testclient import TestClient

from tests.api.conftest import (
    FakeCatalogStore,
    FakeDefinitionStore,
    admin_header,
    callctx_header,
    minimal_workflow,
    seed_builtin_echo,
)

# ---------------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------------


def test_publish_returns_201_and_version_ref(
    client: TestClient,
    stores: tuple[FakeDefinitionStore, FakeCatalogStore],
) -> None:
    _, catalog_store = stores
    seed_builtin_echo(catalog_store)

    resp = client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(minimal_workflow())},
        headers=admin_header(),
    )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body == {"workspaceId": "ws-1", "workflowName": "orders", "version": 1}


def test_publish_accepts_pre_parsed_object_body(
    client: TestClient,
    stores: tuple[FakeDefinitionStore, FakeCatalogStore],
) -> None:
    _, catalog_store = stores
    seed_builtin_echo(catalog_store)

    resp = client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": minimal_workflow()},
        headers=admin_header(),
    )

    assert resp.status_code == 201, resp.text


def test_publish_requires_workspace_write_permission(client: TestClient) -> None:
    resp = client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(minimal_workflow())},
        headers=callctx_header(
            workspace_id="ws-1",
            permissions=["catalog:workflows:read"],
        ),
    )

    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "permission_denied"


def test_publish_rejects_workspace_mismatch(client: TestClient) -> None:
    resp = client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(minimal_workflow())},
        headers=callctx_header(
            workspace_id="ws-other",
            permissions=["catalog:workflows:write"],
        ),
    )

    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "catalog.workspace_mismatch"


def test_publish_emits_envelope_on_schema_failure(
    client: TestClient,
    stores: tuple[FakeDefinitionStore, FakeCatalogStore],
) -> None:
    _, catalog_store = stores
    seed_builtin_echo(catalog_store)

    # Drop the required apiVersion.
    doc = minimal_workflow()
    del doc["apiVersion"]
    resp = client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(doc)},
        headers=admin_header(),
    )

    assert resp.status_code == 400
    body = resp.json()
    assert body["error"]["code"].startswith("catalog.publish.")
    assert body["error"]["issues"]


# ---------------------------------------------------------------------------
# List
# ---------------------------------------------------------------------------


def test_list_versions_returns_published_rows(
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
        "/v1/workspaces/ws-1/workflows/orders",
        headers=admin_header(),
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["nextCursor"] is None
    assert len(body["items"]) == 1
    item = body["items"][0]
    assert item["workspaceId"] == "ws-1"
    assert item["workflowName"] == "orders"
    assert item["version"] == 1
    assert item["document"]["kind"] == "Workflow"
    assert item["parentDeprecated"] is False


def test_list_versions_returns_empty_for_unknown_workflow(client: TestClient) -> None:
    resp = client.get(
        "/v1/workspaces/ws-1/workflows/missing",
        headers=admin_header(),
    )
    assert resp.status_code == 200
    assert resp.json() == {"items": [], "nextCursor": None}


def test_list_versions_rejects_out_of_range_limit(client: TestClient) -> None:
    # ``limit`` must satisfy ``1 <= limit <= 1000``; FastAPI rejects everything
    # else at the API boundary with a 422 before the handler runs.
    for bad in (0, -1, 1001, 99999):
        resp = client.get(
            f"/v1/workspaces/ws-1/workflows/orders?limit={bad}",
            headers=admin_header(),
        )
        assert resp.status_code == 422, (bad, resp.text)


# ---------------------------------------------------------------------------
# Get-by-ref
# ---------------------------------------------------------------------------


def test_get_by_ref_returns_version_body(
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
        "/v1/workspaces/ws-1/workflows/orders@1",
        headers=admin_header(),
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["version"] == 1
    assert body["workflowName"] == "orders"


def test_get_by_ref_404_when_version_missing(client: TestClient) -> None:
    resp = client.get(
        "/v1/workspaces/ws-1/workflows/orders@99",
        headers=admin_header(),
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "catalog.workflow_not_found"


def test_segment_without_at_sign_is_treated_as_list_by_name(client: TestClient) -> None:
    """Bare segments dispatch to the list handler — no error.

    The unified GET matcher branches on the presence of ``@``;
    ``/workflows/not-a-ref`` is a legal list-by-name request that
    returns an empty page when no such workflow exists.
    """
    resp = client.get(
        "/v1/workspaces/ws-1/workflows/not-a-ref",
        headers=admin_header(),
    )
    assert resp.status_code == 200
    assert resp.json() == {"items": [], "nextCursor": None}


# ---------------------------------------------------------------------------
# Get-by-id (workspaceless)
# ---------------------------------------------------------------------------


def test_get_by_id_returns_version_body(
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
        "/v1/workflows/ws-1/orders@1",
        headers=admin_header(),
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["workspaceId"] == "ws-1"
    assert body["workflowName"] == "orders"
    assert body["version"] == 1


def test_get_by_id_400_on_malformed_id(client: TestClient) -> None:
    resp = client.get(
        "/v1/workflows/no-slash",
        headers=admin_header(),
    )
    assert resp.status_code == 400


def test_get_by_id_rejects_cross_workspace_read(
    client: TestClient,
    stores: tuple[FakeDefinitionStore, FakeCatalogStore],
) -> None:
    """A tenant principal cannot read other workspaces' workflows by id.

    The workspaceless route only carries a permission gate
    (``catalog:workflows:read``); without an explicit workspace check
    against the call context, a principal holding that permission in
    workspace ``ws-1`` could craft an id pointing at ``ws-2`` and read
    across the tenant boundary. Internal callers that legitimately
    need cross-workspace reads must use the ``/rpc/v1/`` surface
    gated on ``catalog:rpc:read``.
    """
    _, catalog_store = stores
    seed_builtin_echo(catalog_store)
    # Publish a workflow in ws-2 as ws-2's admin.
    resp = client.post(
        "/v1/workspaces/ws-2/workflows",
        json={"definition": json.dumps(minimal_workflow(ws="ws-2"))},
        headers=admin_header(ws="ws-2"),
    )
    assert resp.status_code == 201, resp.text

    # Caller belongs to ws-1, has read permission in ws-1, tries to
    # read ws-2's workflow.
    resp = client.get(
        "/v1/workflows/ws-2/orders@1",
        headers=admin_header(ws="ws-1"),
    )
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "catalog.workspace_mismatch"


def test_get_by_id_400_when_name_contains_slash(client: TestClient) -> None:
    """Workflow names cannot contain ``/`` — three-slash IDs are rejected.

    The documented id shape is ``<workspaceId>/<workflowName>@<version>``;
    accepting ``ws-1/a/b@1`` would silently route a malformed id to the
    manager with ``name="a/b"``. ``_REF_RE`` now excludes ``/`` from
    the name group so the parse fails with a 400.
    """
    resp = client.get(
        "/v1/workflows/ws-1/a/b@1",
        headers=admin_header(),
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "catalog.workflow_ref_invalid"


# ---------------------------------------------------------------------------
# Deprecate
# ---------------------------------------------------------------------------


def test_deprecate_returns_status_ok(
    client: TestClient,
    stores: tuple[FakeDefinitionStore, FakeCatalogStore],
) -> None:
    definition_store, catalog_store = stores
    seed_builtin_echo(catalog_store)
    client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(minimal_workflow())},
        headers=admin_header(),
    )

    resp = client.post(
        "/v1/workspaces/ws-1/workflows/orders@1:deprecate",
        json={"reason": "superseded"},
        headers=admin_header(),
    )

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"status": "ok"}
    assert definition_store.workflow_deprecated == {("ws-1", "orders"): True}


def test_deprecate_empty_body_is_accepted(
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

    resp = client.post(
        "/v1/workspaces/ws-1/workflows/orders@1:deprecate",
        json={},
        headers=admin_header(),
    )
    assert resp.status_code == 200


def test_deprecate_404_when_workflow_absent(client: TestClient) -> None:
    resp = client.post(
        "/v1/workspaces/ws-1/workflows/missing@1:deprecate",
        json={},
        headers=admin_header(),
    )
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "catalog.workflow_not_found"


# ---------------------------------------------------------------------------
# Extract template (covered in test_templates.py via end-to-end)
# ---------------------------------------------------------------------------

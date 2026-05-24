"""HTTP-level tests for the tenant + workspace endpoints (AS-IMPL-005, #240)."""

from __future__ import annotations

from datetime import UTC, datetime

from custos_spl.ids import TenantId, WorkspaceId
from custos_spl.interfaces.auth_store import Tenant, Workspace
from fastapi.testclient import TestClient

from custos_auth.audit import EVENT_TENANT_CREATED, EVENT_WORKSPACE_CREATED
from tests._fakes import FakeAuthAdapter, FakeMetadataAdapter
from tests.conftest import callctx_header

# ---------------------------------------------------------------------------
# POST /v1/tenants
# ---------------------------------------------------------------------------


def test_create_tenant_returns_201_and_emits_audit(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
    fake_metadata_store: FakeMetadataAdapter,
) -> None:
    resp = client.post(
        "/v1/tenants",
        headers=callctx_header(permissions=["platform.admin"]),
        json={"tenant_id": "t1", "display_name": "Acme"},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["tenant_id"] == "t1"
    assert body["display_name"] == "Acme"
    assert body["disabled_at"] is None
    # Persisted in the fake store
    assert "t1" in fake_auth_store.tenants
    # Audit emitted against the platform sentinel workspace
    assert len(fake_metadata_store.append_audit_calls) == 1
    ws_id, event = fake_metadata_store.append_audit_calls[0]
    assert ws_id == "__platform__"
    assert event.event_type == EVENT_TENANT_CREATED


def test_create_tenant_requires_platform_admin(client: TestClient) -> None:
    resp = client.post(
        "/v1/tenants",
        headers=callctx_header(permissions=["tenant.admin"]),
        json={"tenant_id": "t1", "display_name": "Acme"},
    )
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "permission_denied"


def test_create_tenant_rejects_duplicate(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
) -> None:
    fake_auth_store.tenants["t1"] = Tenant(
        tenant_id=TenantId("t1"),
        display_name="Existing",
        disabled_at=None,
        created_at=datetime.now(UTC),
    )
    resp = client.post(
        "/v1/tenants",
        headers=callctx_header(permissions=["platform.admin"]),
        json={"tenant_id": "t1", "display_name": "Other"},
    )
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "conflict"


def test_create_tenant_rejects_invalid_body(client: TestClient) -> None:
    resp = client.post(
        "/v1/tenants",
        headers=callctx_header(permissions=["platform.admin"]),
        json={"tenant_id": "", "display_name": "Acme"},
    )
    assert resp.status_code == 422
    body = resp.json()
    assert body["error"]["code"] == "request_validation_failed"
    assert body["error"]["issues"]


# ---------------------------------------------------------------------------
# GET /v1/tenants
# ---------------------------------------------------------------------------


def test_list_tenants_as_platform_admin_returns_all(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    now = datetime.now(UTC)
    for tid in ("t1", "t2"):
        fake_auth_store.tenants[tid] = Tenant(
            tenant_id=TenantId(tid),
            display_name=tid.upper(),
            disabled_at=None,
            created_at=now,
        )
    resp = client.get(
        "/v1/tenants",
        headers=callctx_header(permissions=["platform.admin"]),
    )
    assert resp.status_code == 200
    ids = {t["tenant_id"] for t in resp.json()["tenants"]}
    assert ids == {"t1", "t2"}


def test_list_tenants_as_tenant_admin_returns_only_own_tenant(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    now = datetime.now(UTC)
    for tid in ("t1", "t2"):
        fake_auth_store.tenants[tid] = Tenant(
            tenant_id=TenantId(tid),
            display_name=tid.upper(),
            disabled_at=None,
            created_at=now,
        )
    resp = client.get(
        "/v1/tenants",
        headers=callctx_header(tenant_id="t1", permissions=["tenant.admin"]),
    )
    assert resp.status_code == 200
    ids = [t["tenant_id"] for t in resp.json()["tenants"]]
    assert ids == ["t1"]


def test_list_tenants_as_tenant_admin_without_tenant_returns_empty(
    client: TestClient,
) -> None:
    resp = client.get(
        "/v1/tenants",
        headers=callctx_header(permissions=["tenant.admin"]),
    )
    assert resp.status_code == 200
    assert resp.json() == {"tenants": []}


def test_list_tenants_requires_admin_permission(client: TestClient) -> None:
    resp = client.get(
        "/v1/tenants",
        headers=callctx_header(permissions=["random.perm"]),
    )
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# POST /v1/tenants/{id}/workspaces
# ---------------------------------------------------------------------------


def _seed_tenant(store: FakeAuthAdapter, tenant_id: str, *, disabled: bool = False) -> None:
    now = datetime.now(UTC)
    store.tenants[tenant_id] = Tenant(
        tenant_id=TenantId(tenant_id),
        display_name=tenant_id.upper(),
        disabled_at=now if disabled else None,
        created_at=now,
    )


def test_create_workspace_as_platform_admin(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
    fake_metadata_store: FakeMetadataAdapter,
) -> None:
    _seed_tenant(fake_auth_store, "t1")
    resp = client.post(
        "/v1/tenants/t1/workspaces",
        headers=callctx_header(permissions=["platform.admin"]),
        json={"workspace_id": "ws-1", "display_name": "Default"},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["workspace_id"] == "ws-1"
    assert body["tenant_id"] == "t1"
    assert "ws-1" in fake_auth_store.workspaces
    ws_id, event = fake_metadata_store.append_audit_calls[0]
    assert ws_id == "ws-1"
    assert event.event_type == EVENT_WORKSPACE_CREATED


def test_create_workspace_as_tenant_admin_in_own_tenant(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
) -> None:
    _seed_tenant(fake_auth_store, "t1")
    resp = client.post(
        "/v1/tenants/t1/workspaces",
        headers=callctx_header(tenant_id="t1", permissions=["tenant.admin"]),
        json={"workspace_id": "ws-1", "display_name": "Default"},
    )
    assert resp.status_code == 201


def test_create_workspace_as_tenant_admin_cross_tenant_returns_404(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
) -> None:
    _seed_tenant(fake_auth_store, "t-other")
    resp = client.post(
        "/v1/tenants/t-other/workspaces",
        headers=callctx_header(tenant_id="t1", permissions=["tenant.admin"]),
        json={"workspace_id": "ws-1", "display_name": "Default"},
    )
    assert resp.status_code == 404


def test_create_workspace_on_nonexistent_tenant_returns_404(
    client: TestClient,
) -> None:
    resp = client.post(
        "/v1/tenants/ghost/workspaces",
        headers=callctx_header(permissions=["platform.admin"]),
        json={"workspace_id": "ws-1", "display_name": "Default"},
    )
    assert resp.status_code == 404


def test_create_workspace_on_disabled_tenant_returns_400(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
) -> None:
    _seed_tenant(fake_auth_store, "t1", disabled=True)
    resp = client.post(
        "/v1/tenants/t1/workspaces",
        headers=callctx_header(permissions=["platform.admin"]),
        json={"workspace_id": "ws-1", "display_name": "Default"},
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_request"


def test_create_workspace_duplicate_returns_409(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
) -> None:
    _seed_tenant(fake_auth_store, "t1")
    fake_auth_store.workspaces["ws-1"] = Workspace(
        workspace_id=WorkspaceId("ws-1"),
        tenant_id=TenantId("t1"),
        display_name="Existing",
        disabled_at=None,
        created_at=datetime.now(UTC),
    )
    resp = client.post(
        "/v1/tenants/t1/workspaces",
        headers=callctx_header(permissions=["platform.admin"]),
        json={"workspace_id": "ws-1", "display_name": "New"},
    )
    assert resp.status_code == 409


def test_create_workspace_requires_admin_permission(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_tenant(fake_auth_store, "t1")
    resp = client.post(
        "/v1/tenants/t1/workspaces",
        headers=callctx_header(permissions=["random.perm"]),
        json={"workspace_id": "ws-1", "display_name": "Default"},
    )
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# GET /v1/workspaces
# ---------------------------------------------------------------------------


def _seed_workspace(store: FakeAuthAdapter, ws: str, tenant: str) -> None:
    now = datetime.now(UTC)
    store.workspaces[ws] = Workspace(
        workspace_id=WorkspaceId(ws),
        tenant_id=TenantId(tenant),
        display_name=ws.upper(),
        disabled_at=None,
        created_at=now,
    )


def test_list_workspaces_as_platform_admin_sees_all(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_tenant(fake_auth_store, "t1")
    _seed_tenant(fake_auth_store, "t2")
    _seed_workspace(fake_auth_store, "ws-1", "t1")
    _seed_workspace(fake_auth_store, "ws-2", "t2")
    resp = client.get(
        "/v1/workspaces",
        headers=callctx_header(permissions=["platform.admin"]),
    )
    assert resp.status_code == 200
    ids = {w["workspace_id"] for w in resp.json()["workspaces"]}
    assert ids == {"ws-1", "ws-2"}


def test_list_workspaces_as_tenant_admin_scoped_to_tenant(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_tenant(fake_auth_store, "t1")
    _seed_tenant(fake_auth_store, "t2")
    _seed_workspace(fake_auth_store, "ws-1", "t1")
    _seed_workspace(fake_auth_store, "ws-2", "t2")
    resp = client.get(
        "/v1/workspaces",
        headers=callctx_header(tenant_id="t1", permissions=["tenant.admin"]),
    )
    ids = [w["workspace_id"] for w in resp.json()["workspaces"]]
    assert ids == ["ws-1"]


def test_list_workspaces_as_tenant_admin_without_tenant_returns_empty(
    client: TestClient,
) -> None:
    resp = client.get(
        "/v1/workspaces",
        headers=callctx_header(permissions=["tenant.admin"]),
    )
    assert resp.json() == {"workspaces": []}


def test_list_workspaces_as_regular_user_returns_only_pinned_workspace(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_tenant(fake_auth_store, "t1")
    _seed_workspace(fake_auth_store, "ws-1", "t1")
    _seed_workspace(fake_auth_store, "ws-2", "t1")
    resp = client.get(
        "/v1/workspaces",
        headers=callctx_header(workspace_id="ws-1"),
    )
    ids = [w["workspace_id"] for w in resp.json()["workspaces"]]
    assert ids == ["ws-1"]


def test_list_workspaces_as_regular_user_no_workspace_returns_empty(
    client: TestClient,
) -> None:
    resp = client.get(
        "/v1/workspaces",
        headers=callctx_header(),
    )
    assert resp.json() == {"workspaces": []}


# ---------------------------------------------------------------------------
# GET /v1/workspaces/{workspace_id}
# ---------------------------------------------------------------------------


def test_get_workspace_as_platform_admin_returns_200(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_workspace(fake_auth_store, "ws-1", "t1")
    resp = client.get(
        "/v1/workspaces/ws-1",
        headers=callctx_header(permissions=["platform.admin"]),
    )
    assert resp.status_code == 200
    assert resp.json()["workspace_id"] == "ws-1"


def test_get_workspace_unknown_returns_404(client: TestClient) -> None:
    resp = client.get(
        "/v1/workspaces/ghost",
        headers=callctx_header(permissions=["platform.admin"]),
    )
    assert resp.status_code == 404


def test_get_workspace_cross_tenant_collapses_to_404(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_workspace(fake_auth_store, "ws-other", "t-other")
    resp = client.get(
        "/v1/workspaces/ws-other",
        headers=callctx_header(tenant_id="t1", permissions=["tenant.admin"]),
    )
    assert resp.status_code == 404


def test_get_workspace_as_tenant_admin_same_tenant_returns_200(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_workspace(fake_auth_store, "ws-1", "t1")
    resp = client.get(
        "/v1/workspaces/ws-1",
        headers=callctx_header(tenant_id="t1", permissions=["tenant.admin"]),
    )
    assert resp.status_code == 200


def test_get_workspace_as_member_returns_200(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_workspace(fake_auth_store, "ws-1", "t1")
    resp = client.get(
        "/v1/workspaces/ws-1",
        headers=callctx_header(workspace_id="ws-1"),
    )
    assert resp.status_code == 200


def test_get_workspace_non_member_returns_404(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_workspace(fake_auth_store, "ws-1", "t1")
    resp = client.get(
        "/v1/workspaces/ws-1",
        headers=callctx_header(workspace_id="ws-other"),
    )
    assert resp.status_code == 404

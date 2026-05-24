"""HTTP-level tests for principal + service-account endpoints (AS-IMPL-006, #241)."""

from __future__ import annotations

from datetime import UTC, datetime

from custos_spl.ids import PrincipalId, TenantId, WorkspaceId
from custos_spl.interfaces.auth_store import ServiceAccount, User
from fastapi.testclient import TestClient

from custos_auth.audit import EVENT_PRINCIPAL_CREATED, EVENT_PRINCIPAL_DISABLED
from tests._fakes import FakeAuthAdapter, FakeMetadataAdapter
from tests.conftest import callctx_header


def _seed_user(store: FakeAuthAdapter, principal_id: str, tenant_id: str) -> None:
    now = datetime.now(UTC)
    store.principals[principal_id] = User(
        kind="user",
        principal_id=PrincipalId(principal_id),
        tenant_id=TenantId(tenant_id),
        display_name=principal_id,
        email=f"{principal_id}@example.com",
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )


def _seed_service_account(store: FakeAuthAdapter, principal_id: str, workspace_id: str) -> None:
    now = datetime.now(UTC)
    store.principals[principal_id] = ServiceAccount(
        kind="serviceAccount",
        principal_id=PrincipalId(principal_id),
        workspace_id=WorkspaceId(workspace_id),
        display_name=principal_id,
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )


# ---------------------------------------------------------------------------
# GET /v1/principals/me
# ---------------------------------------------------------------------------


def test_get_me_returns_caller_principal(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_user(fake_auth_store, "user-1", "t1")
    resp = client.get(
        "/v1/principals/me",
        headers=callctx_header(principal_id="user-1"),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "user"
    assert body["principal_id"] == "user-1"
    assert body["tenant_id"] == "t1"


def test_get_me_returns_service_account(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    resp = client.get(
        "/v1/principals/me",
        headers=callctx_header(principal_id="sa-1"),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "serviceAccount"
    assert body["principal_id"] == "sa-1"
    assert body["workspace_id"] == "ws-1"


def test_get_me_returns_404_when_principal_missing(client: TestClient) -> None:
    resp = client.get(
        "/v1/principals/me",
        headers=callctx_header(principal_id="ghost"),
    )
    assert resp.status_code == 404


def test_get_me_requires_callctx_header(client: TestClient) -> None:
    resp = client.get("/v1/principals/me")
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# POST /v1/principals/{id}/disable
# ---------------------------------------------------------------------------


def test_disable_principal_as_platform_admin_returns_204(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
    fake_metadata_store: FakeMetadataAdapter,
) -> None:
    _seed_user(fake_auth_store, "user-target", "t1")
    resp = client.post(
        "/v1/principals/user-target/disable",
        headers=callctx_header(principal_id="admin", permissions=["platform.admin"]),
        json={"reason": "left-the-company"},
    )
    assert resp.status_code == 204
    target = fake_auth_store.principals["user-target"]
    assert target.disabled_at is not None
    assert target.disabled_reason == "left-the-company"
    assert fake_auth_store.disable_principal_calls == [("user-target", "admin", "left-the-company")]
    ws_id, event = fake_metadata_store.append_audit_calls[0]
    assert event.event_type == EVENT_PRINCIPAL_DISABLED
    # Users live at tenant scope → platform sentinel
    assert ws_id == "__platform__"


def test_disable_service_account_audits_under_workspace(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
    fake_metadata_store: FakeMetadataAdapter,
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    resp = client.post(
        "/v1/principals/sa-1/disable",
        headers=callctx_header(principal_id="admin", permissions=["platform.admin"]),
        json={"reason": "rotation"},
    )
    assert resp.status_code == 204
    ws_id, _event = fake_metadata_store.append_audit_calls[0]
    assert ws_id == "ws-1"


def test_disable_principal_404_when_missing(client: TestClient) -> None:
    resp = client.post(
        "/v1/principals/ghost/disable",
        headers=callctx_header(permissions=["platform.admin"]),
        json={"reason": "x"},
    )
    assert resp.status_code == 404


def test_disable_principal_cross_tenant_collapses_to_404(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_user(fake_auth_store, "user-target", "t-other")
    resp = client.post(
        "/v1/principals/user-target/disable",
        headers=callctx_header(tenant_id="t1", permissions=["tenant.admin"]),
        json={"reason": "x"},
    )
    assert resp.status_code == 404


def test_disable_service_account_cross_workspace_collapses_to_404(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-other")
    resp = client.post(
        "/v1/principals/sa-1/disable",
        headers=callctx_header(workspace_id="ws-1", permissions=["tenant.admin"]),
        json={"reason": "x"},
    )
    assert resp.status_code == 404


def test_disable_principal_requires_admin_permission(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_user(fake_auth_store, "user-target", "t1")
    resp = client.post(
        "/v1/principals/user-target/disable",
        headers=callctx_header(permissions=["unrelated.perm"]),
        json={"reason": "x"},
    )
    assert resp.status_code == 403


def test_disable_principal_rejects_empty_reason(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_user(fake_auth_store, "user-target", "t1")
    resp = client.post(
        "/v1/principals/user-target/disable",
        headers=callctx_header(permissions=["platform.admin"]),
        json={"reason": ""},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST /v1/service-accounts
# ---------------------------------------------------------------------------


def test_create_service_account_returns_201_and_persists(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
    fake_metadata_store: FakeMetadataAdapter,
) -> None:
    resp = client.post(
        "/v1/service-accounts",
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
        json={"principal_id": "sa-1", "display_name": "ci-runner"},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["kind"] == "serviceAccount"
    assert body["principal_id"] == "sa-1"
    assert body["workspace_id"] == "ws-1"
    assert "sa-1" in fake_auth_store.principals
    ws_id, event = fake_metadata_store.append_audit_calls[0]
    assert ws_id == "ws-1"
    assert event.event_type == EVENT_PRINCIPAL_CREATED
    assert event.payload["kind"] == "serviceAccount"


def test_create_service_account_requires_workspace_in_context(
    client: TestClient,
) -> None:
    resp = client.post(
        "/v1/service-accounts",
        headers=callctx_header(permissions=["admin:service-account"]),
        json={"principal_id": "sa-1", "display_name": "ci-runner"},
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_request"


def test_create_service_account_rejects_duplicate(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    resp = client.post(
        "/v1/service-accounts",
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
        json={"principal_id": "sa-1", "display_name": "other"},
    )
    assert resp.status_code == 409


def test_create_service_account_requires_permission(client: TestClient) -> None:
    resp = client.post(
        "/v1/service-accounts",
        headers=callctx_header(workspace_id="ws-1", permissions=["random.perm"]),
        json={"principal_id": "sa-1", "display_name": "ci"},
    )
    assert resp.status_code == 403


def test_create_service_account_rejects_invalid_body(client: TestClient) -> None:
    resp = client.post(
        "/v1/service-accounts",
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
        json={"principal_id": "", "display_name": "x"},
    )
    assert resp.status_code == 422

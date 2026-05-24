"""Tests for the Phase D role-binding HTTP surface (AS-IMPL-010).

Covers:

* POST happy path (201 + audit + binding-changed event).
* POST 400 ``invalid_role_scope`` when binding a tenant-only role
  through the workspace endpoint.
* POST 400 ``invalid_role_scope`` when binding a role the registry
  has never heard of.
* POST 403 when the caller lacks ``admin:role-binding``.
* POST 404 when the workspace does not exist.
* POST 404 when the workspace is in a different tenant
  (existence-hiding semantics).
* POST 404 when the caller has ``admin:role-binding`` but no
  ``tenant_id`` on the call-context (deny-by-default).
* DELETE happy path (204 + audit + binding-changed event).
* DELETE 404 when the binding does not exist at this workspace.
* DELETE 404 when the binding exists at a different workspace
  (anti-leak).
* Exactly-once binding-changed semantics: the event is published
  once per successful mutation, and not at all when the mutation
  short-circuits with a 4xx.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from custos_spl.ids import TenantId, WorkspaceId
from custos_spl.interfaces.auth_store import Tenant, Workspace
from fastapi import FastAPI
from fastapi.testclient import TestClient

from custos_auth import create_app
from custos_auth.audit import (
    EVENT_ROLE_BINDING_GRANTED,
    EVENT_ROLE_BINDING_REVOKED,
)
from custos_auth.binding_events import RecordingBindingChangedPublisher
from custos_auth.providers import Providers
from custos_auth.settings import load_settings
from tests._fakes import FakeAuthAdapter, FakeMetadataAdapter
from tests.conftest import callctx_header

_DEFAULT_ENV = {
    "CUSTOS_AUTH_STORE_DSN": "postgresql://u:p@h:5432/custos_auth",
    "CUSTOS_AUTH_METADATA_STORE_DSN": "postgresql://u:p@h:5432/custos_meta",
}

WORKSPACE = "ws-1"
TENANT = "tenant-1"
OTHER_WORKSPACE = "ws-2"
OTHER_TENANT = "tenant-2"


@pytest.fixture
def auth_store() -> FakeAuthAdapter:
    store = FakeAuthAdapter()
    store.tenants[TENANT] = Tenant(
        tenant_id=TenantId(TENANT),
        display_name="t1",
        disabled_at=None,
        created_at=datetime.now(UTC),
    )
    store.tenants[OTHER_TENANT] = Tenant(
        tenant_id=TenantId(OTHER_TENANT),
        display_name="t2",
        disabled_at=None,
        created_at=datetime.now(UTC),
    )
    store.workspaces[WORKSPACE] = Workspace(
        workspace_id=WorkspaceId(WORKSPACE),
        tenant_id=TenantId(TENANT),
        display_name="w1",
        disabled_at=None,
        created_at=datetime.now(UTC),
    )
    store.workspaces[OTHER_WORKSPACE] = Workspace(
        workspace_id=WorkspaceId(OTHER_WORKSPACE),
        tenant_id=TenantId(OTHER_TENANT),
        display_name="w2",
        disabled_at=None,
        created_at=datetime.now(UTC),
    )
    return store


@pytest.fixture
def metadata_store() -> FakeMetadataAdapter:
    return FakeMetadataAdapter()


@pytest.fixture
def publisher() -> RecordingBindingChangedPublisher:
    return RecordingBindingChangedPublisher()


@pytest.fixture
def app_for_bindings(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
    publisher: RecordingBindingChangedPublisher,
) -> FastAPI:
    providers = Providers(
        auth_store=auth_store,  # type: ignore[arg-type]
        metadata_store=metadata_store,  # type: ignore[arg-type]
        binding_changed_publisher=publisher,
    )
    return create_app(
        settings=load_settings(_DEFAULT_ENV),
        providers=providers,
    )


@pytest.fixture
def bindings_client(app_for_bindings: FastAPI) -> Iterator[TestClient]:
    with TestClient(app_for_bindings) as c:
        yield c


def _admin_header(*, tenant: str | None = TENANT) -> dict[str, str]:
    return callctx_header(
        principal_id="admin",
        tenant_id=tenant,
        workspace_id=WORKSPACE,
        permissions=["admin:role-binding"],
    )


# ---------------------------------------------------------------------------
# POST happy path
# ---------------------------------------------------------------------------


def test_post_grants_workspace_viewer_role(
    bindings_client: TestClient,
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
    publisher: RecordingBindingChangedPublisher,
) -> None:
    response = bindings_client.post(
        f"/v1/workspaces/{WORKSPACE}/role-bindings",
        headers=_admin_header(),
        json={"principal_id": "user-1", "role_id": "role:workspace.viewer"},
    )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["principal_id"] == "user-1"
    assert body["role_id"] == "role:workspace.viewer"
    assert body["scope_kind"] == "workspace"
    assert body["scope_id"] == WORKSPACE
    assert body["bound_by"] == "admin"
    binding_id = body["binding_id"]

    # Binding committed to the store.
    assert binding_id in auth_store.role_bindings

    # Audit event emitted under the workspace bucket.
    audits = [a for a in metadata_store.append_audit_calls if a[0] == WORKSPACE]
    assert any(a[1].event_type == EVENT_ROLE_BINDING_GRANTED for a in audits)

    # Binding-changed event published exactly once.
    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.action == "granted"
    assert event.principal_id == "user-1"
    assert event.role_id == "role:workspace.viewer"
    assert event.binding_id == binding_id


# ---------------------------------------------------------------------------
# POST scope-rule + unknown-role rejections
# ---------------------------------------------------------------------------


def test_post_rejects_tenant_only_role_at_workspace_scope(
    bindings_client: TestClient,
    auth_store: FakeAuthAdapter,
    publisher: RecordingBindingChangedPublisher,
) -> None:
    response = bindings_client.post(
        f"/v1/workspaces/{WORKSPACE}/role-bindings",
        headers=_admin_header(),
        json={"principal_id": "user-1", "role_id": "role:tenant.admin"},
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_role_scope"
    # No binding committed, no event published.
    assert auth_store.role_bindings == {}
    assert publisher.published == []


def test_post_rejects_unknown_role(
    bindings_client: TestClient,
    auth_store: FakeAuthAdapter,
    publisher: RecordingBindingChangedPublisher,
) -> None:
    response = bindings_client.post(
        f"/v1/workspaces/{WORKSPACE}/role-bindings",
        headers=_admin_header(),
        json={"principal_id": "user-1", "role_id": "role:custom.unknown"},
    )
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_role_scope"
    assert auth_store.role_bindings == {}
    assert publisher.published == []


# ---------------------------------------------------------------------------
# POST permission + tenant scoping
# ---------------------------------------------------------------------------


def test_post_requires_admin_role_binding_permission(
    bindings_client: TestClient,
    auth_store: FakeAuthAdapter,
    publisher: RecordingBindingChangedPublisher,
) -> None:
    # Caller carries an authenticated callctx but no admin:role-binding.
    headers = callctx_header(
        principal_id="someone",
        tenant_id=TENANT,
        workspace_id=WORKSPACE,
        permissions=[],
    )
    response = bindings_client.post(
        f"/v1/workspaces/{WORKSPACE}/role-bindings",
        headers=headers,
        json={"principal_id": "user-1", "role_id": "role:workspace.viewer"},
    )
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "permission_denied"
    assert auth_store.role_bindings == {}
    assert publisher.published == []


def test_post_returns_404_on_unknown_workspace(
    bindings_client: TestClient,
    publisher: RecordingBindingChangedPublisher,
) -> None:
    response = bindings_client.post(
        "/v1/workspaces/does-not-exist/role-bindings",
        headers=_admin_header(),
        json={"principal_id": "user-1", "role_id": "role:workspace.viewer"},
    )
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"
    assert publisher.published == []


def test_post_returns_404_on_cross_tenant_workspace(
    bindings_client: TestClient,
    publisher: RecordingBindingChangedPublisher,
) -> None:
    # Caller is in TENANT but targets OTHER_WORKSPACE (in OTHER_TENANT).
    headers = callctx_header(
        principal_id="admin",
        tenant_id=TENANT,
        workspace_id=WORKSPACE,
        permissions=["admin:role-binding"],
    )
    response = bindings_client.post(
        f"/v1/workspaces/{OTHER_WORKSPACE}/role-bindings",
        headers=headers,
        json={"principal_id": "user-1", "role_id": "role:workspace.viewer"},
    )
    assert response.status_code == 404
    # Existence-hiding: collapse to "not_found" rather than 403.
    assert response.json()["error"]["code"] == "not_found"
    assert publisher.published == []


def test_post_returns_404_when_caller_has_no_tenant(
    bindings_client: TestClient,
    auth_store: FakeAuthAdapter,
    publisher: RecordingBindingChangedPublisher,
) -> None:
    """A tenant-less ``admin:role-binding`` caller must not be able to
    target any workspace. Without an explicit tenant on the
    call-context we have no way to authorise the cross-tenant check,
    so the resolver denies by default (404 existence-hiding) unless
    the caller is ``platform.admin``.
    """
    # No tenant_id on the callctx — only the bare admin:role-binding
    # permission. This is the case the original guard was bypassing.
    headers = callctx_header(
        principal_id="rogue-admin",
        permissions=["admin:role-binding"],
    )
    response = bindings_client.post(
        f"/v1/workspaces/{WORKSPACE}/role-bindings",
        headers=headers,
        json={"principal_id": "user-1", "role_id": "role:workspace.viewer"},
    )
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"
    assert auth_store.role_bindings == {}
    assert publisher.published == []


# ---------------------------------------------------------------------------
# DELETE happy path + anti-leak
# ---------------------------------------------------------------------------


def _grant(bindings_client: TestClient, *, workspace: str = WORKSPACE) -> str:
    response = bindings_client.post(
        f"/v1/workspaces/{workspace}/role-bindings",
        headers=_admin_header(),
        json={"principal_id": "user-1", "role_id": "role:workspace.viewer"},
    )
    assert response.status_code == 201, response.text
    return str(response.json()["binding_id"])


def test_delete_revokes_binding(
    bindings_client: TestClient,
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
    publisher: RecordingBindingChangedPublisher,
) -> None:
    binding_id = _grant(bindings_client)
    publisher.published.clear()
    metadata_store.append_audit_calls.clear()

    response = bindings_client.delete(
        f"/v1/workspaces/{WORKSPACE}/role-bindings/{binding_id}",
        headers=_admin_header(),
    )
    assert response.status_code == 204

    assert binding_id not in auth_store.role_bindings
    assert auth_store.delete_role_binding_calls[-1][0] == binding_id

    # Revoked audit event + binding-changed publish exactly once.
    types = [a[1].event_type for a in metadata_store.append_audit_calls]
    assert EVENT_ROLE_BINDING_REVOKED in types
    assert len(publisher.published) == 1
    assert publisher.published[0].action == "revoked"
    assert publisher.published[0].binding_id == binding_id


def test_delete_returns_404_for_unknown_binding(
    bindings_client: TestClient,
    publisher: RecordingBindingChangedPublisher,
) -> None:
    response = bindings_client.delete(
        f"/v1/workspaces/{WORKSPACE}/role-bindings/does-not-exist",
        headers=_admin_header(),
    )
    assert response.status_code == 404
    assert publisher.published == []


def test_delete_returns_404_when_binding_belongs_to_other_workspace(
    bindings_client: TestClient,
    auth_store: FakeAuthAdapter,
    publisher: RecordingBindingChangedPublisher,
) -> None:
    # Create the binding under WORKSPACE, then try to delete it through
    # OTHER_WORKSPACE — must collapse to 404 (anti-leak), and must not
    # delete the binding nor publish an event.
    binding_id = _grant(bindings_client)
    publisher.published.clear()

    # Use a platform.admin caller so the cross-tenant 404 on the
    # workspace check itself doesn't mask the binding-mismatch 404 we
    # actually want to assert.
    headers = callctx_header(
        principal_id="root",
        permissions=["admin:role-binding", "platform.admin"],
    )
    response = bindings_client.delete(
        f"/v1/workspaces/{OTHER_WORKSPACE}/role-bindings/{binding_id}",
        headers=headers,
    )
    assert response.status_code == 404
    assert binding_id in auth_store.role_bindings
    assert publisher.published == []


def test_delete_idempotent_returns_404_on_repeat(
    bindings_client: TestClient,
    publisher: RecordingBindingChangedPublisher,
) -> None:
    binding_id = _grant(bindings_client)
    response = bindings_client.delete(
        f"/v1/workspaces/{WORKSPACE}/role-bindings/{binding_id}",
        headers=_admin_header(),
    )
    assert response.status_code == 204
    # Second delete: the binding is already gone ⇒ 404.
    publisher.published.clear()
    response = bindings_client.delete(
        f"/v1/workspaces/{WORKSPACE}/role-bindings/{binding_id}",
        headers=_admin_header(),
    )
    assert response.status_code == 404
    assert publisher.published == []


# ---------------------------------------------------------------------------
# Exactly-once event semantics
# ---------------------------------------------------------------------------


def test_event_not_published_when_rejected_at_validation(
    bindings_client: TestClient,
    publisher: RecordingBindingChangedPublisher,
) -> None:
    # Pydantic-level validation failure (missing required field).
    response = bindings_client.post(
        f"/v1/workspaces/{WORKSPACE}/role-bindings",
        headers=_admin_header(),
        json={"principal_id": "user-1"},  # role_id missing
    )
    assert response.status_code == 422
    assert publisher.published == []

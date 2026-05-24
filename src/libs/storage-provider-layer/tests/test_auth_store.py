"""Tests for AuthStoreProvider Protocol and its data shapes."""

from __future__ import annotations

import inspect
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from custos_spl.ids import (
    PrincipalId,
    RoleBindingId,
    RoleId,
    ServiceTokenId,
    TenantId,
    WorkspaceId,
)
from custos_spl.interfaces import (
    AuthStoreProvider,
    GlobalScope,
    OidcIdentity,
    Permission,
    Principal,
    Role,
    RoleBinding,
    ServiceAccount,
    ServiceToken,
    Tenant,
    TenantScope,
    User,
    Workspace,
    WorkspaceScope,
)


def _now() -> datetime:
    return datetime(2026, 1, 1, tzinfo=UTC)


# ----- Data shape immutability -----


def test_tenant_is_frozen() -> None:
    t = Tenant(
        tenant_id=TenantId("t-1"),
        display_name="Acme",
        disabled_at=None,
        created_at=_now(),
    )
    with pytest.raises(FrozenInstanceError):
        t.display_name = "Other"  # type: ignore[misc]


def test_workspace_is_frozen() -> None:
    w = Workspace(
        workspace_id=WorkspaceId("ws-1"),
        tenant_id=TenantId("t-1"),
        display_name="Default",
        disabled_at=None,
        created_at=_now(),
    )
    with pytest.raises(FrozenInstanceError):
        w.display_name = "Other"  # type: ignore[misc]


def test_user_is_frozen_and_carries_kind_literal() -> None:
    u = User(
        kind="user",
        principal_id=PrincipalId("p-1"),
        tenant_id=TenantId("t-1"),
        display_name="Alice",
        email="alice@example.com",
        disabled_at=None,
        disabled_reason=None,
        created_at=_now(),
    )
    assert u.kind == "user"
    with pytest.raises(FrozenInstanceError):
        u.email = None  # type: ignore[misc]


def test_service_account_is_frozen_and_carries_kind_literal() -> None:
    sa = ServiceAccount(
        kind="serviceAccount",
        principal_id=PrincipalId("p-2"),
        workspace_id=WorkspaceId("ws-1"),
        display_name="ci-bot",
        disabled_at=None,
        disabled_reason=None,
        created_at=_now(),
    )
    assert sa.kind == "serviceAccount"
    with pytest.raises(FrozenInstanceError):
        sa.display_name = "other"  # type: ignore[misc]


def test_oidc_identity_is_frozen() -> None:
    oi = OidcIdentity(
        issuer="https://accounts.example.com",
        subject="sub-1",
        user_id=PrincipalId("p-1"),
        bound_at=_now(),
    )
    with pytest.raises(FrozenInstanceError):
        oi.user_id = PrincipalId("p-9")  # type: ignore[misc]


def test_service_token_is_frozen() -> None:
    st = ServiceToken(
        token_id=ServiceTokenId("st-1"),
        service_account_id=PrincipalId("p-2"),
        hash="sha256:abcd",
        issued_at=_now(),
        expires_at=_now(),
        revoked_at=None,
        revoked_by=None,
        revoked_reason=None,
    )
    with pytest.raises(FrozenInstanceError):
        st.hash = "sha256:other"  # type: ignore[misc]


def test_role_carries_immutable_permission_tuple() -> None:
    r = Role(
        role_id=RoleId("r-1"),
        name="workspace-admin",
        description="Full control of a workspace.",
        permission_names=("workspace.read", "workspace.write"),
    )
    assert r.permission_names == ("workspace.read", "workspace.write")
    with pytest.raises(FrozenInstanceError):
        r.name = "other"  # type: ignore[misc]


def test_role_binding_is_frozen() -> None:
    rb = RoleBinding(
        binding_id=RoleBindingId("rb-1"),
        principal_id=PrincipalId("p-1"),
        role_id=RoleId("r-1"),
        scope=WorkspaceScope(workspace_id=WorkspaceId("ws-1")),
        bound_at=_now(),
        bound_by=PrincipalId("p-0"),
    )
    with pytest.raises(FrozenInstanceError):
        rb.role_id = RoleId("r-9")  # type: ignore[misc]


# ----- Discriminated unions -----


def test_principal_union_admits_both_kinds() -> None:
    user: Principal = User(
        kind="user",
        principal_id=PrincipalId("p-1"),
        tenant_id=TenantId("t-1"),
        display_name="Alice",
        email=None,
        disabled_at=None,
        disabled_reason=None,
        created_at=_now(),
    )
    sa: Principal = ServiceAccount(
        kind="serviceAccount",
        principal_id=PrincipalId("p-2"),
        workspace_id=WorkspaceId("ws-1"),
        display_name="bot",
        disabled_at=None,
        disabled_reason=None,
        created_at=_now(),
    )
    assert isinstance(user, User)
    assert isinstance(sa, ServiceAccount)


def test_role_binding_scope_union_has_three_variants() -> None:
    ws = WorkspaceScope(workspace_id=WorkspaceId("ws-1"))
    ts = TenantScope(tenant_id=TenantId("t-1"))
    gs = GlobalScope()
    assert isinstance(ws, WorkspaceScope)
    assert isinstance(ts, TenantScope)
    assert isinstance(gs, GlobalScope)


# ----- Protocol shape -----


def test_protocol_declares_required_schema_revision() -> None:
    assert AuthStoreProvider.SCHEMA_REVISION == 1


REQUIRED_METHODS = [
    # Tenants
    "put_tenant",
    "get_tenant",
    "list_tenants",
    # Workspaces
    "put_workspace",
    "get_workspace",
    "list_workspaces",
    # Principals
    "put_principal",
    "get_principal",
    "list_principals",
    "disable_principal",
    # OIDC
    "put_oidc_identity",
    "get_oidc_identity",
    "list_oidc_identities_for_user",
    # Service tokens
    "put_service_token",
    "get_service_token_by_hash",
    "get_service_token",
    "revoke_service_token",
    "list_service_tokens_for_service_account",
    "delete_expired_service_tokens",
    # Permissions + roles
    "upsert_permission",
    "list_permissions",
    "put_role",
    "get_role",
    "list_roles",
    # Role bindings
    "put_role_binding",
    "delete_role_binding",
    "list_role_bindings_for_principal",
    "list_role_bindings_for_scope",
    # Transactions
    "with_transaction",
]


@pytest.mark.parametrize("method", REQUIRED_METHODS)
def test_protocol_exposes_method(method: str) -> None:
    assert hasattr(AuthStoreProvider, method)


@pytest.mark.parametrize("method", REQUIRED_METHODS)
def test_protocol_methods_are_async(method: str) -> None:
    fn = getattr(AuthStoreProvider, method)
    assert inspect.iscoroutinefunction(fn), f"{method} must be async"


# ----- Workspace-scoping exemption -----
#
# AuthStoreProvider is exempt from the workspace-scoping middleware that
# wraps the four workspace-scoped interfaces — its entities define
# workspaces (`Workspace`) and span tenancy levels (`Tenant`,
# `RoleBinding` with `GlobalScope`). The exemption is a documentation /
# integration concern (Auth Service is the sole caller and enforces its
# own authorization); it is not a constraint expressible at the Protocol
# shape level. `get_workspace(workspace_id)` and `put_workspace(...)`
# legitimately take a `WorkspaceId` as identity, which is not the same
# as "this method is scoped to that workspace".


# ----- Permission row -----


def test_permission_is_frozen() -> None:
    p = Permission(name="run.read", description="Read run history.")
    with pytest.raises(FrozenInstanceError):
        p.name = "other"  # type: ignore[misc]


# ----- runtime_checkable conformance -----


class _MinimalAuthStore:
    """Just enough of the Protocol to satisfy isinstance() at runtime."""

    SCHEMA_REVISION = 1

    async def put_tenant(self, *a: object, **kw: object) -> None: ...
    async def get_tenant(self, *a: object, **kw: object) -> None: ...
    async def list_tenants(self, *a: object, **kw: object) -> None: ...
    async def put_workspace(self, *a: object, **kw: object) -> None: ...
    async def get_workspace(self, *a: object, **kw: object) -> None: ...
    async def list_workspaces(self, *a: object, **kw: object) -> None: ...
    async def put_principal(self, *a: object, **kw: object) -> None: ...
    async def get_principal(self, *a: object, **kw: object) -> None: ...
    async def list_principals(self, *a: object, **kw: object) -> None: ...
    async def disable_principal(self, *a: object, **kw: object) -> None: ...
    async def put_oidc_identity(self, *a: object, **kw: object) -> None: ...
    async def get_oidc_identity(self, *a: object, **kw: object) -> None: ...
    async def list_oidc_identities_for_user(self, *a: object, **kw: object) -> None: ...
    async def put_service_token(self, *a: object, **kw: object) -> None: ...
    async def get_service_token_by_hash(self, *a: object, **kw: object) -> None: ...
    async def get_service_token(self, *a: object, **kw: object) -> None: ...
    async def revoke_service_token(self, *a: object, **kw: object) -> None: ...
    async def list_service_tokens_for_service_account(
        self, *a: object, **kw: object
    ) -> None: ...
    async def delete_expired_service_tokens(self, *a: object, **kw: object) -> None: ...
    async def upsert_permission(self, *a: object, **kw: object) -> None: ...
    async def list_permissions(self, *a: object, **kw: object) -> None: ...
    async def put_role(self, *a: object, **kw: object) -> None: ...
    async def get_role(self, *a: object, **kw: object) -> None: ...
    async def list_roles(self, *a: object, **kw: object) -> None: ...
    async def put_role_binding(self, *a: object, **kw: object) -> None: ...
    async def delete_role_binding(self, *a: object, **kw: object) -> None: ...
    async def list_role_bindings_for_principal(self, *a: object, **kw: object) -> None: ...
    async def list_role_bindings_for_scope(self, *a: object, **kw: object) -> None: ...
    async def with_transaction(self, *a: object, **kw: object) -> None: ...


def test_runtime_checkable_recognizes_duck_typed_impl() -> None:
    assert isinstance(_MinimalAuthStore(), AuthStoreProvider)


def test_runtime_checkable_rejects_partial_impl() -> None:
    class Partial:
        async def put_tenant(self, *a: object, **kw: object) -> None: ...

    assert not isinstance(Partial(), AuthStoreProvider)

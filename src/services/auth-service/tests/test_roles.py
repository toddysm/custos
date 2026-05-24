"""Tests for the Phase D built-in role table (AS-IMPL-009).

Covers:

* The six v1 built-ins are present with their canonical IDs and
  allowed-scope tags.
* :func:`seed_builtin_roles` upserts every role and is idempotent
  across re-invocation.
* :func:`is_scope_allowed` honours the scope-rule registry for every
  known role and falls back to permissive for unknown roles.
* :func:`scope_kind` returns the canonical tag for every concrete
  ``RoleBindingScope`` discriminator.
"""

from __future__ import annotations

from custos_spl.ids import RoleId, TenantId, WorkspaceId
from custos_spl.interfaces.auth_store import (
    GlobalScope,
    TenantScope,
    WorkspaceScope,
)

from custos_auth.roles import (
    BUILTIN_ROLES,
    BUILTIN_ROLES_BY_ID,
    ROLE_ALLOWED_SCOPES,
    ROLE_PLATFORM_ADMIN,
    ROLE_TENANT_ADMIN,
    ROLE_WORKSPACE_ADMIN,
    ROLE_WORKSPACE_AUTHOR,
    ROLE_WORKSPACE_OPERATOR,
    ROLE_WORKSPACE_VIEWER,
    is_scope_allowed,
    scope_kind,
    seed_builtin_roles,
)
from tests._fakes import FakeAuthAdapter

# ---------------------------------------------------------------------------
# Built-in registry shape
# ---------------------------------------------------------------------------


def test_builtin_roles_canonical_ids() -> None:
    ids = [role.role_id for role in BUILTIN_ROLES]
    assert ids == [
        ROLE_WORKSPACE_VIEWER,
        ROLE_WORKSPACE_AUTHOR,
        ROLE_WORKSPACE_OPERATOR,
        ROLE_WORKSPACE_ADMIN,
        ROLE_TENANT_ADMIN,
        ROLE_PLATFORM_ADMIN,
    ]


def test_builtin_roles_by_id_covers_every_role() -> None:
    assert set(BUILTIN_ROLES_BY_ID.keys()) == {r.role_id for r in BUILTIN_ROLES}


def test_workspace_roles_inherit_perms() -> None:
    viewer = BUILTIN_ROLES_BY_ID[ROLE_WORKSPACE_VIEWER]
    author = BUILTIN_ROLES_BY_ID[ROLE_WORKSPACE_AUTHOR]
    operator = BUILTIN_ROLES_BY_ID[ROLE_WORKSPACE_OPERATOR]
    admin = BUILTIN_ROLES_BY_ID[ROLE_WORKSPACE_ADMIN]
    # Each tier is a superset of the previous.
    assert set(viewer.permission_names) <= set(author.permission_names)
    assert set(author.permission_names) <= set(operator.permission_names)
    assert set(operator.permission_names) <= set(admin.permission_names)
    # workspace.admin includes the two auth-service perms.
    assert "admin:role-binding" in admin.permission_names
    assert "admin:service-account" in admin.permission_names


def test_platform_admin_has_empty_permission_tuple() -> None:
    role = BUILTIN_ROLES_BY_ID[ROLE_PLATFORM_ADMIN]
    # Empty tuple — the Phase E authorize engine short-circuits this
    # role to a blanket allow rather than spelling out every permission.
    assert role.permission_names == ()


def test_role_allowed_scopes_registry() -> None:
    assert ROLE_ALLOWED_SCOPES[ROLE_WORKSPACE_VIEWER] == frozenset({"workspace"})
    assert ROLE_ALLOWED_SCOPES[ROLE_WORKSPACE_ADMIN] == frozenset({"workspace"})
    assert ROLE_ALLOWED_SCOPES[ROLE_TENANT_ADMIN] == frozenset({"tenant"})
    assert ROLE_ALLOWED_SCOPES[ROLE_PLATFORM_ADMIN] == frozenset({"platform"})


# ---------------------------------------------------------------------------
# Seeder
# ---------------------------------------------------------------------------


async def test_seed_builtin_roles_upserts_every_role() -> None:
    store = FakeAuthAdapter()
    await seed_builtin_roles(store)  # type: ignore[arg-type]
    assert set(store.roles.keys()) == {str(r.role_id) for r in BUILTIN_ROLES}


async def test_seed_builtin_roles_idempotent() -> None:
    store = FakeAuthAdapter()
    await seed_builtin_roles(store)  # type: ignore[arg-type]
    snapshot = dict(store.roles)
    await seed_builtin_roles(store)  # type: ignore[arg-type]
    # Re-running over a populated store produces an identical view.
    assert store.roles == snapshot


# ---------------------------------------------------------------------------
# scope_kind + is_scope_allowed
# ---------------------------------------------------------------------------


def test_scope_kind_for_each_concrete_scope() -> None:
    assert scope_kind(WorkspaceScope(workspace_id=WorkspaceId("w"))) == "workspace"
    assert scope_kind(TenantScope(tenant_id=TenantId("t"))) == "tenant"
    assert scope_kind(GlobalScope()) == "platform"


def test_is_scope_allowed_for_workspace_role() -> None:
    ws = WorkspaceScope(workspace_id=WorkspaceId("w"))
    tenant = TenantScope(tenant_id=TenantId("t"))
    assert is_scope_allowed(ROLE_WORKSPACE_VIEWER, ws) is True
    assert is_scope_allowed(ROLE_WORKSPACE_VIEWER, tenant) is False


def test_is_scope_allowed_for_tenant_role() -> None:
    ws = WorkspaceScope(workspace_id=WorkspaceId("w"))
    tenant = TenantScope(tenant_id=TenantId("t"))
    assert is_scope_allowed(ROLE_TENANT_ADMIN, ws) is False
    assert is_scope_allowed(ROLE_TENANT_ADMIN, tenant) is True


def test_is_scope_allowed_for_platform_role() -> None:
    ws = WorkspaceScope(workspace_id=WorkspaceId("w"))
    platform = GlobalScope()
    assert is_scope_allowed(ROLE_PLATFORM_ADMIN, ws) is False
    assert is_scope_allowed(ROLE_PLATFORM_ADMIN, platform) is True


def test_is_scope_allowed_unknown_role_returns_true() -> None:
    # Custom roles (M2+) are not in the registry; the scope check
    # falls back to permissive — the custom-role surface owns its own
    # scope rules and adds them when it lands.
    unknown = RoleId("role:custom.example")
    assert is_scope_allowed(unknown, WorkspaceScope(workspace_id=WorkspaceId("w"))) is True
    assert is_scope_allowed(unknown, GlobalScope()) is True

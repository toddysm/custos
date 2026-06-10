"""Built-in v1 roles and scope-rule registry.

Phase D / AS-IMPL-009. Six roles are seeded at every service startup:

* ``role:workspace.viewer``   — workspace-scope read-only.
* ``role:workspace.author``   — viewer + workflow/template/run authoring.
* ``role:workspace.operator`` — author + connector/trigger admin.
* ``role:workspace.admin``    — operator + role-binding + service-account admin.
* ``role:tenant.admin``       — workspace admin + tenant-level role-binding.
* ``role:platform.admin``     — platform short-circuit (empty permission tuple
                                — the Phase E authorize engine treats this role
                                as "allow every permission at every scope").

The permission tuples here mirror the table in
``design/components/auth-service/design.md`` § Permission and Role Model;
:func:`seed_builtin_roles` calls
``AuthStoreProvider.put_role`` for each (idempotent — keyed on
``role_id``), so role updates land by editing this module and shipping
a new auth-service release.

The :data:`ROLE_ALLOWED_SCOPES` registry encodes the "scope rules"
section of the same design doc: ``role:workspace.*`` may only bind at
``WorkspaceScope``, ``role:tenant.admin`` only at ``TenantScope``, and
``role:platform.admin`` only at ``GlobalScope``. Bind requests that
violate this mapping return 400 ``invalid_role_scope`` (see
``custos_auth.api.routes.role_bindings``).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Literal

from custos_spl.ids import RoleId
from custos_spl.interfaces.auth_store import (
    GlobalScope,
    Role,
    RoleBindingScope,
    TenantScope,
    WorkspaceScope,
)

if TYPE_CHECKING:
    from custos_spl import AuthStoreProvider


#: Canonical scope-kind tag. Matches the discriminated-union kind on
#: the SPL :class:`RoleBindingScope` (``WorkspaceScope`` /
#: ``TenantScope`` / ``GlobalScope``). Encoded as a ``Literal`` so the
#: scope-rule registry below is statically checked.
ScopeKind = Literal["workspace", "tenant", "platform"]


@dataclass(frozen=True, slots=True)
class BuiltInRole:
    """Hard-coded built-in role definition.

    Carries the SPL ``Role`` shape plus the allowed-scope-kinds
    metadata that the role-binding scope check consults. Projection to
    the SPL dataclass is via :meth:`to_spl`.
    """

    role_id: RoleId
    name: str
    description: str
    permission_names: tuple[str, ...]
    allowed_scopes: frozenset[ScopeKind]

    def to_spl(self) -> Role:
        return Role(
            role_id=self.role_id,
            name=self.name,
            description=self.description,
            permission_names=self.permission_names,
        )


# ---------------------------------------------------------------------------
# Canonical role IDs
# ---------------------------------------------------------------------------


ROLE_WORKSPACE_VIEWER: Final[RoleId] = RoleId("role:workspace.viewer")
ROLE_WORKSPACE_AUTHOR: Final[RoleId] = RoleId("role:workspace.author")
ROLE_WORKSPACE_OPERATOR: Final[RoleId] = RoleId("role:workspace.operator")
ROLE_WORKSPACE_ADMIN: Final[RoleId] = RoleId("role:workspace.admin")
ROLE_TENANT_ADMIN: Final[RoleId] = RoleId("role:tenant.admin")
ROLE_PLATFORM_ADMIN: Final[RoleId] = RoleId("role:platform.admin")


# ---------------------------------------------------------------------------
# Permission bundles (extracted as constants so each role definition
# reads as a single composition; mypy --strict catches typos).
# ---------------------------------------------------------------------------


_VIEWER_PERMS: Final[tuple[str, ...]] = (
    "catalog:workflows:read",
    "catalog:templates:read",
    "catalog:activity-types:read",
    "catalog:connector-types:read",
    "connector:read",
    "audit:read",
    "run:read",
    "logs:read",
    "metrics:read",
)

_AUTHOR_PERMS: Final[tuple[str, ...]] = (
    *_VIEWER_PERMS,
    "catalog:workflows:write",
    "catalog:templates:write",
    "catalog:activity-types:write",
    "catalog:connector-types:write",
    "workflow:execute",
    "run:cancel",
)

_OPERATOR_PERMS: Final[tuple[str, ...]] = (
    *_AUTHOR_PERMS,
    "admin:connector",
    "trigger:subscriptions:read",
    "trigger:subscriptions:write",
    "trigger:subscriptions:delete",
    "trigger:subscriptions:fire",
)

_WORKSPACE_ADMIN_PERMS: Final[tuple[str, ...]] = (
    *_OPERATOR_PERMS,
    "admin:role-binding",
    "admin:service-account",
)

_TENANT_ADMIN_PERMS: Final[tuple[str, ...]] = (
    "admin:workspace",
    "admin:role-binding",
)


# ---------------------------------------------------------------------------
# Built-in role table
# ---------------------------------------------------------------------------


BUILTIN_ROLES: Final[tuple[BuiltInRole, ...]] = (
    BuiltInRole(
        role_id=ROLE_WORKSPACE_VIEWER,
        name="workspace.viewer",
        description="Read-only access to a workspace.",
        permission_names=_VIEWER_PERMS,
        allowed_scopes=frozenset({"workspace"}),
    ),
    BuiltInRole(
        role_id=ROLE_WORKSPACE_AUTHOR,
        name="workspace.author",
        description="Viewer + author / execute workflows and templates.",
        permission_names=_AUTHOR_PERMS,
        allowed_scopes=frozenset({"workspace"}),
    ),
    BuiltInRole(
        role_id=ROLE_WORKSPACE_OPERATOR,
        name="workspace.operator",
        description="Author + administer connectors and triggers.",
        permission_names=_OPERATOR_PERMS,
        allowed_scopes=frozenset({"workspace"}),
    ),
    BuiltInRole(
        role_id=ROLE_WORKSPACE_ADMIN,
        name="workspace.admin",
        description="Operator + manage role bindings and service accounts.",
        permission_names=_WORKSPACE_ADMIN_PERMS,
        allowed_scopes=frozenset({"workspace"}),
    ),
    BuiltInRole(
        role_id=ROLE_TENANT_ADMIN,
        name="tenant.admin",
        description="Manage workspaces and tenant-level role bindings.",
        permission_names=_TENANT_ADMIN_PERMS,
        allowed_scopes=frozenset({"tenant"}),
    ),
    BuiltInRole(
        role_id=ROLE_PLATFORM_ADMIN,
        name="platform.admin",
        # Empty permission tuple — the authorize engine (Phase E /
        # AS-IMPL-011) short-circuits any binding of this role to a
        # blanket allow regardless of requested permission/scope.
        # Keeping the tuple empty here means the role advertises no
        # *explicit* permissions through GET /v1/roles, matching the
        # design's "implicit superuser" framing.
        description=(
            "Platform-wide superuser. Short-circuits the authorization "
            "engine to allow every permission at every scope."
        ),
        permission_names=(),
        allowed_scopes=frozenset({"platform"}),
    ),
)


#: Quick-lookup table keyed by :class:`RoleId`.
BUILTIN_ROLES_BY_ID: Final[dict[RoleId, BuiltInRole]] = {
    role.role_id: role for role in BUILTIN_ROLES
}


#: Allowed-scope-kinds registry consulted by the role-binding handlers
#: when enforcing scope rules. Lookup miss ⇒ the role is unknown to
#: this build of auth-service (e.g. a custom role from M2+) and the
#: caller falls back to "no constraint" — the future custom-role
#: surface owns its own scope rules.
ROLE_ALLOWED_SCOPES: Final[dict[RoleId, frozenset[ScopeKind]]] = {
    role.role_id: role.allowed_scopes for role in BUILTIN_ROLES
}


# ---------------------------------------------------------------------------
# Scope-kind helpers
# ---------------------------------------------------------------------------


def scope_kind(scope: RoleBindingScope) -> ScopeKind:
    """Return the canonical :data:`ScopeKind` tag for a SPL scope."""
    if isinstance(scope, WorkspaceScope):
        return "workspace"
    if isinstance(scope, TenantScope):
        return "tenant"
    assert isinstance(scope, GlobalScope)
    return "platform"


def is_scope_allowed(role_id: RoleId, scope: RoleBindingScope) -> bool:
    """Check that ``role_id`` may bind at ``scope``.

    Unknown role IDs (not in :data:`ROLE_ALLOWED_SCOPES`) return
    ``True`` — see the registry docstring above.
    """
    allowed = ROLE_ALLOWED_SCOPES.get(role_id)
    if allowed is None:
        return True
    return scope_kind(scope) in allowed


# ---------------------------------------------------------------------------
# Seeder
# ---------------------------------------------------------------------------


async def seed_builtin_roles(auth_store: AuthStoreProvider) -> None:
    """Upsert every built-in role into the SPL ``Role`` table.

    Idempotent — ``put_role`` is keyed on ``role_id`` so repeated
    startups just refresh descriptions and permission tuples without
    duplicating rows. Callers that need to read back the seeded set
    after the upsert should call ``auth_store.list_roles()``.
    """
    for role in BUILTIN_ROLES:
        await auth_store.put_role(role.to_spl())


__all__ = [
    "BUILTIN_ROLES",
    "BUILTIN_ROLES_BY_ID",
    "ROLE_ALLOWED_SCOPES",
    "ROLE_PLATFORM_ADMIN",
    "ROLE_TENANT_ADMIN",
    "ROLE_WORKSPACE_ADMIN",
    "ROLE_WORKSPACE_AUTHOR",
    "ROLE_WORKSPACE_OPERATOR",
    "ROLE_WORKSPACE_VIEWER",
    "BuiltInRole",
    "ScopeKind",
    "is_scope_allowed",
    "scope_kind",
    "seed_builtin_roles",
]

"""AuthStoreProvider — identity, tenancy, RBAC persistence for Auth Service.

Owns the entity families described in
`design/components/storage-provider-layer/design.md` § AuthStoreProvider:

1. Tenancy + workspace records (`Tenant`, `Workspace`)
2. Principals — `User` + `ServiceAccount` discriminated union
3. OIDC identity bindings (write-once `(issuer, subject)`)
4. Service tokens (hashed; plaintext never persisted)
5. Permissions + roles
6. Role bindings (`scope ∈ {workspaceId, tenantId, "*"}`)
7. Intra-provider transactions (`with_transaction`)

This interface is **exempt** from the workspace-scoping middleware that
wraps the four workspace-scoped interfaces — its entities define
workspaces and span tenancy levels by design. Auth Service is the sole
caller and is responsible for authorization on every method invocation
before reaching the adapter.

Immutability:
  - `OidcIdentity` rows are write-once on `(issuer, subject)`; rebinding
    requires explicit delete + re-put with audit trail.
  - `ServiceToken.hash` is immutable; revocation sets `revoked_at`,
    never deletes (sweeper removes expired rows separately).

Audit:
  All mutating methods participate in the audit outbox via the same
  transaction-handle contract as `MetadataStoreProvider`; audit rows are
  appended via `MetadataStoreProvider.append_audit` using that shared
  transaction handle.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar, Literal, Protocol, TypeVar, runtime_checkable

from custos_spl.ids import (
    PrincipalId,
    RoleBindingId,
    RoleId,
    ServiceTokenId,
    TenantId,
    WorkspaceId,
)
from custos_spl.interfaces.metadata_store import TransactionHandle

T = TypeVar("T")


# ============================================================================
# Section 1 — Tenant + Workspace
# ============================================================================


@dataclass(frozen=True, slots=True)
class Tenant:
    """A tenancy boundary.

    Tenants own workspaces. Cross-tenant data access is not expressible
    through any other SPL interface — once a row carries `workspace_id`,
    its tenant is fixed via `Workspace.tenant_id`.
    """

    tenant_id: TenantId
    display_name: str
    disabled_at: datetime | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class TenantFilter:
    """Optional predicates for `list_tenants`."""

    include_disabled: bool = False


@dataclass(frozen=True, slots=True)
class Workspace:
    """A workspace row.

    `tenant_id` is a required FK — every workspace lives inside exactly
    one tenant. Workspace rows themselves are exempt from the
    workspace-scoping middleware (they describe workspaces rather than
    living inside one).
    """

    workspace_id: WorkspaceId
    tenant_id: TenantId
    display_name: str
    disabled_at: datetime | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class WorkspaceFilter:
    """Optional predicates for `list_workspaces`."""

    tenant_id: TenantId | None = None
    include_disabled: bool = False


# ============================================================================
# Section 2 — Principals (discriminated union)
# ============================================================================


@dataclass(frozen=True, slots=True)
class User:
    """A human principal.

    The `kind` field is fixed to `"user"` to support the
    `Principal = User | ServiceAccount` discriminated union — callers
    can `match principal.kind` to narrow the type.
    """

    kind: Literal["user"]
    principal_id: PrincipalId
    tenant_id: TenantId
    display_name: str
    email: str | None
    disabled_at: datetime | None
    disabled_reason: str | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class ServiceAccount:
    """A non-human principal owned by a workspace.

    Issued by Auth Service and bound to a workspace. Verification path
    is `ServiceToken.hash → service_account_id → ServiceAccount`. The
    `kind` field discriminates the union.
    """

    kind: Literal["serviceAccount"]
    principal_id: PrincipalId
    workspace_id: WorkspaceId
    display_name: str
    disabled_at: datetime | None
    disabled_reason: str | None
    created_at: datetime


Principal = User | ServiceAccount


@dataclass(frozen=True, slots=True)
class PrincipalFilter:
    """Optional predicates for `list_principals`."""

    tenant_id: TenantId | None = None
    workspace_id: WorkspaceId | None = None
    kind: Literal["user", "serviceAccount"] | None = None
    include_disabled: bool = False


# ============================================================================
# Section 3 — OIDC identity bindings
# ============================================================================


@dataclass(frozen=True, slots=True)
class OidcIdentity:
    """Write-once `(issuer, subject) → user_id` binding.

    Used by Auth Service's verifier path to resolve an inbound OIDC token
    to an internal `PrincipalId`. Re-binding requires explicit delete +
    re-put with audit trail — `put_oidc_identity` on an existing key
    raises `ImmutableViolation`.
    """

    issuer: str
    subject: str
    user_id: PrincipalId
    bound_at: datetime


# ============================================================================
# Section 4 — Service tokens
# ============================================================================


@dataclass(frozen=True, slots=True)
class ServiceToken:
    """A hashed service-account credential row.

    Plaintext is **never** persisted; Auth Service hashes before calling
    `put_service_token`. `hash` is immutable; revocation sets
    `revoked_at`. Adapter MUST index on `hash` (verifier hot path).
    """

    token_id: ServiceTokenId
    service_account_id: PrincipalId
    hash: str
    issued_at: datetime
    expires_at: datetime
    revoked_at: datetime | None
    revoked_by: PrincipalId | None
    revoked_reason: str | None


# ============================================================================
# Section 5 — Permissions + roles
# ============================================================================


@dataclass(frozen=True, slots=True)
class Permission:
    """A declared permission name.

    Upserted at platform startup for every permission declared by a
    service. `(name)` is the primary key.
    """

    name: str
    description: str


@dataclass(frozen=True, slots=True)
class Role:
    """A named bundle of permissions.

    v1 roles are seeded at startup; the put surface is reserved for M2+
    custom roles. Identity is `role_id`; `name` is human-readable.
    """

    role_id: RoleId
    name: str
    description: str
    permission_names: tuple[str, ...]


# ============================================================================
# Section 6 — Role bindings + scope union
# ============================================================================


@dataclass(frozen=True, slots=True)
class WorkspaceScope:
    """Binding limited to one workspace."""

    workspace_id: WorkspaceId


@dataclass(frozen=True, slots=True)
class TenantScope:
    """Binding limited to one tenant (all workspaces in that tenant)."""

    tenant_id: TenantId


@dataclass(frozen=True, slots=True)
class GlobalScope:
    """Platform-wide binding (the `"*"` scope).

    A separate dataclass — rather than a sentinel string — so the
    discriminated `RoleBindingScope` union matches by type rather than
    by value.
    """


RoleBindingScope = WorkspaceScope | TenantScope | GlobalScope


@dataclass(frozen=True, slots=True)
class RoleBinding:
    """Assigns a `Role` to a `Principal` at a `RoleBindingScope`.

    Adapter MUST index on `(principal_id, scope)` — this is the
    authorization hot path. `list_role_bindings_for_principal` reads
    that index for every authorize() call.
    """

    binding_id: RoleBindingId
    principal_id: PrincipalId
    role_id: RoleId
    scope: RoleBindingScope
    bound_at: datetime
    bound_by: PrincipalId


@dataclass(frozen=True, slots=True)
class RoleBindingFilter:
    """Optional predicates for `list_role_bindings_for_scope`."""

    role_id: RoleId | None = None
    principal_id: PrincipalId | None = None


# ============================================================================
# Protocol
# ============================================================================


@runtime_checkable
class AuthStoreProvider(Protocol):
    """Identity, tenancy, and RBAC persistence — exempt from workspace scoping.

    Sole caller: Auth Service. Authorization is enforced by the caller
    before any method here runs; the adapter applies no scoping rules
    of its own.

    Failure surface (canonical):
      - `ImmutableViolation` — re-binding `(issuer, subject)` via
        `put_oidc_identity`; mutating `ServiceToken.hash`.
      - `BackendUnavailable` — transient backend failure.

    Authorization semantics live in Auth Service; this interface is a
    storage contract only.
    """

    SCHEMA_REVISION: ClassVar[int] = 1

    # --- Tenants --------------------------------------------------------

    async def put_tenant(self, tenant: Tenant) -> None:
        """Insert or update a tenant.

        Tenant identity is `tenant_id`; updating `display_name` or the
        disabled state is allowed.
        """
        ...

    async def get_tenant(self, tenant_id: TenantId) -> Tenant | None:
        """Read a tenant by ID. Returns `None` if absent."""
        ...

    async def list_tenants(self, filter: TenantFilter) -> tuple[Tenant, ...]:
        """List tenants matching `filter`.

        Bounded small (operator-managed); returns the full set rather
        than a page.
        """
        ...

    # --- Workspaces -----------------------------------------------------

    async def put_workspace(self, workspace: Workspace) -> None:
        """Insert or update a workspace. `tenant_id` is a required FK."""
        ...

    async def get_workspace(self, workspace_id: WorkspaceId) -> Workspace | None:
        """Read a workspace by ID. Returns `None` if absent."""
        ...

    async def list_workspaces(self, filter: WorkspaceFilter) -> tuple[Workspace, ...]:
        """List workspaces matching `filter`.

        Bounded by tenant size; returns the full set rather than a page.
        """
        ...

    # --- Principals -----------------------------------------------------

    async def put_principal(self, principal: Principal) -> None:
        """Insert or update a principal (User or ServiceAccount).

        Discriminated on `principal.kind`. Adapters MUST persist the
        kind-specific fields to the matching table (or column set).
        """
        ...

    async def get_principal(self, principal_id: PrincipalId) -> Principal | None:
        """Read a principal by ID, returning the matching union variant.

        `None` if absent. Adapter is responsible for reading the row's
        kind and returning the correct dataclass.
        """
        ...

    async def list_principals(self, filter: PrincipalFilter) -> tuple[Principal, ...]:
        """List principals matching `filter`.

        Typically scoped to a tenant or workspace via the filter; full
        platform-wide scans are reserved for admin tooling.
        """
        ...

    async def disable_principal(
        self,
        principal_id: PrincipalId,
        actor: PrincipalId,
        reason: str,
    ) -> None:
        """Soft-disable a principal.

        Sets `disabled_at` and `disabled_reason`; preserves audit trail
        — the row is never deleted. `actor` and `reason` are persisted
        for after-the-fact review.
        """
        ...

    # --- OIDC identities ------------------------------------------------

    async def put_oidc_identity(
        self,
        issuer: str,
        subject: str,
        user_id: PrincipalId,
    ) -> None:
        """Bind an OIDC `(issuer, subject)` to an internal user.

        Write-once on `(issuer, subject)`. Raises `ImmutableViolation`
        if already bound (even to the same `user_id`). Rebinding is an
        explicit delete + re-put workflow handled by Auth Service.
        """
        ...

    async def get_oidc_identity(
        self,
        issuer: str,
        subject: str,
    ) -> PrincipalId | None:
        """Resolve an OIDC `(issuer, subject)` to a user.

        Verifier hot path; `None` means "unknown OIDC identity, treat
        as unauthenticated". Adapter MUST index on `(issuer, subject)`.
        """
        ...

    async def list_oidc_identities_for_user(
        self,
        user_id: PrincipalId,
    ) -> tuple[OidcIdentity, ...]:
        """List all OIDC identities bound to a user.

        Bounded small (one user has at most a handful of issuers);
        returns the full set.
        """
        ...

    # --- Service tokens -------------------------------------------------

    async def put_service_token(self, token: ServiceToken) -> None:
        """Insert a service token row.

        `hash` is the only credential stored. Identity is `token_id`;
        rotating a token is a new row + revoke-old, not an in-place
        update of `hash`.
        """
        ...

    async def get_service_token_by_hash(self, hash: str) -> ServiceToken | None:
        """Verifier hot path. `None` if no row matches (treat as invalid).

        Adapter MUST index on `hash`. Callers are responsible for
        checking `revoked_at` and `expires_at` against the current
        time — this method does not filter on either.
        """
        ...

    async def get_service_token(self, token_id: ServiceTokenId) -> ServiceToken | None:
        """Look up a service token by its operator-facing identifier.

        Used by the revoke path to (a) check that the caller's
        workspace owns the SA that holds the token, (b) detect the
        already-revoked idempotency case, and (c) extract the hash
        to publish on the ``custos.auth.token-revoked`` event. The
        primary key is `token_id`; returns `None` if no row matches.
        Includes revoked rows.
        """
        ...

    async def revoke_service_token(
        self,
        token_id: ServiceTokenId,
        actor: PrincipalId,
        reason: str,
    ) -> None:
        """Mark a token revoked.

        Sets `revoked_at`, `revoked_by`, `revoked_reason`; never
        deletes the row. Sweeper handles physical removal of expired
        rows separately.
        """
        ...

    async def list_service_tokens_for_service_account(
        self,
        service_account_id: PrincipalId,
    ) -> tuple[ServiceToken, ...]:
        """List all tokens for a service account.

        Bounded small (a SA holds at most a handful of live tokens);
        returns the full set including revoked rows so callers can
        render rotation history.
        """
        ...

    async def list_expired_service_tokens(
        self,
        before: datetime,
    ) -> tuple[ServiceToken, ...]:
        """List token rows whose `expires_at < before`.

        Sweeper-only read. Returns every row that the matching
        :meth:`delete_expired_service_tokens` call would remove,
        including rows already marked revoked, so the sweeper can
        emit a `token.expired` audit row and publish a cache-
        eviction event **before** the row is physically deleted.
        Auth Service callers must invoke this with the same
        `before` value they then pass to
        :meth:`delete_expired_service_tokens`; otherwise the audit
        row set and the deleted row set will drift.
        """
        ...

    async def delete_expired_service_tokens(self, before: datetime) -> int:
        """Sweeper-only physical delete.

        Removes service-token rows whose `expires_at < before`. Returns
        the number of rows removed. Operated by a platform sweeper; no
        single workspace owns the call.
        """
        ...

    # --- Permissions + roles --------------------------------------------

    async def upsert_permission(self, permission: Permission) -> None:
        """Insert or update a permission.

        Called at platform startup for every declared permission. `name`
        is the primary key; re-upserting with an updated description is
        the supported way to evolve copy.
        """
        ...

    async def list_permissions(self) -> tuple[Permission, ...]:
        """List all declared permissions.

        Bounded small (one row per declared permission); returns the
        full set.
        """
        ...

    async def put_role(self, role: Role) -> None:
        """Insert or update a role.

        v1 roles are seeded at startup. The put surface is reserved for
        M2+ custom roles; adapters MAY refuse mutation of seeded roles
        if the operator pins them.
        """
        ...

    async def get_role(self, role_id: RoleId) -> Role | None:
        """Read a role by ID. Returns `None` if absent."""
        ...

    async def list_roles(self) -> tuple[Role, ...]:
        """List all roles. Bounded small."""
        ...

    # --- Role bindings --------------------------------------------------

    async def put_role_binding(self, binding: RoleBinding) -> None:
        """Insert a role binding.

        Adapter MUST index on `(principal_id, scope)` — this is the
        authorization hot path. The `scope` discriminated union is
        stored as `(scope_kind, scope_id?)` at the adapter's discretion.
        """
        ...

    async def delete_role_binding(
        self,
        binding_id: RoleBindingId,
        actor: PrincipalId,
        reason: str,
    ) -> None:
        """Delete a role binding.

        Physical delete (bindings are revocation-by-removal). `actor`
        and `reason` are captured in the audit trail via the
        transaction handle, not on the binding row itself.
        """
        ...

    async def list_role_bindings_for_principal(
        self,
        principal_id: PrincipalId,
        scopes: tuple[RoleBindingScope, ...],
    ) -> tuple[RoleBinding, ...]:
        """List a principal's bindings at the supplied scopes.

        Used by `authorize`: the caller assembles the relevant scope
        set (the request's workspace + its tenant + global) and reads
        the bindings in one round-trip. Returns the full set; bounded
        by the number of distinct roles assigned to the principal.
        """
        ...

    async def list_role_bindings_for_scope(
        self,
        scope: RoleBindingScope,
        filter: RoleBindingFilter,
    ) -> tuple[RoleBinding, ...]:
        """Admin view: all bindings at a given scope.

        Bounded by workspace/tenant size; returns the full set rather
        than a page (admin tooling expects to see everything).
        """
        ...

    # --- Transactions ---------------------------------------------------

    async def with_transaction(
        self,
        body: Callable[[TransactionHandle], Awaitable[T]],
    ) -> T:
        """Run `body` inside a single adapter transaction.

        The `TransactionHandle` is opaque and scoped to the adapter
        transaction opened by this method. It may be passed to other
        SPL provider interfaces only when they are implemented by the
        same adapter/transaction domain; in particular, `append_audit`
        on the matching `MetadataStoreProvider` accepts this handle so
        audit rows commit atomically with the mutation that produced
        them. Passing the handle to a provider backed by a different
        adapter transaction raises `InvalidTransactionHandle`.
        """
        ...


__all__ = [
    "AuthStoreProvider",
    "GlobalScope",
    "OidcIdentity",
    "Permission",
    "Principal",
    "PrincipalFilter",
    "Role",
    "RoleBinding",
    "RoleBindingFilter",
    "RoleBindingScope",
    "ServiceAccount",
    "ServiceToken",
    "Tenant",
    "TenantFilter",
    "TenantScope",
    "User",
    "Workspace",
    "WorkspaceFilter",
    "WorkspaceScope",
]

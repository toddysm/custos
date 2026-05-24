"""In-memory fakes for the SPL provider Protocols used by auth-service tests."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from collections.abc import Set as AbstractSet
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, TypeVar

from custos_spl.errors import ImmutableViolation
from custos_spl.ids import PrincipalId, RoleBindingId, RoleId, ServiceTokenId, TenantId, WorkspaceId
from custos_spl.interfaces.auth_store import (
    GlobalScope,
    Permission,
    Principal,
    PrincipalFilter,
    Role,
    RoleBinding,
    RoleBindingFilter,
    RoleBindingScope,
    ServiceAccount,
    ServiceToken,
    Tenant,
    TenantFilter,
    TenantScope,
    User,
    Workspace,
    WorkspaceFilter,
    WorkspaceScope,
)
from custos_spl.interfaces.metadata_store import TransactionHandle
from custos_spl.middleware.transactions import bind_handle

T = TypeVar("T")


class _FakeAuthTransactionHandle(TransactionHandle):
    """Adapter-side handle subclass for the in-memory ``with_transaction``."""


class FakeAuthAdapter:
    """A minimal in-memory ``AuthStoreProvider`` for auth-service tests.

    Implements the ``MigrationCapable`` surface (used by the schema-
    revision startup gate tests) plus the CRUD surface that the Phase
    C HTTP routes exercise. The real Postgres adapter surface is
    covered by the SPL conformance suite in
    ``src/libs/storage-provider-layer/tests/test_auth_store.py`` and
    by ``src/libs/custos-postgres/tests/test_integration.py``; this
    fake intentionally keeps the storage model deliberately simple
    (plain dicts, no concurrency, no real transactions) so tests stay
    focused on the route + audit logic.
    """

    SCHEMA_REVISION = 1

    def __init__(self, *, applied_revisions: AbstractSet[int] | None = None) -> None:
        self._applied: set[int] = set({1} if applied_revisions is None else applied_revisions)
        self.refresh_calls = 0
        # In-memory entity stores
        self.tenants: dict[str, Tenant] = {}
        self.workspaces: dict[str, Workspace] = {}
        self.principals: dict[str, Principal] = {}
        self.oidc_identities: dict[tuple[str, str], str] = {}
        self.permissions: dict[str, Permission] = {}
        self.roles: dict[str, Role] = {}
        self.role_bindings: dict[str, RoleBinding] = {}
        self.service_tokens: dict[str, ServiceToken] = {}
        # Call recorders for tests that want to assert on argument shape
        self.disable_principal_calls: list[tuple[str, str, str]] = []
        self.delete_role_binding_calls: list[tuple[str, str, str]] = []
        self.revoke_service_token_calls: list[tuple[str, str, str]] = []

    @property
    def declared_revisions(self) -> Mapping[str, AbstractSet[int]]:
        return MappingProxyType(
            {"AuthStoreProvider": frozenset(self._applied)},
        )

    async def apply_pending(self) -> list[str]:  # pragma: no cover - not exercised
        return []

    async def refresh_declared(self) -> None:
        self.refresh_calls += 1

    def set_applied(self, revisions: AbstractSet[int]) -> None:
        self._applied = set(revisions)

    # ------------------------------------------------------------------
    # Tenants
    # ------------------------------------------------------------------

    async def put_tenant(self, tenant: Tenant) -> None:
        self.tenants[str(tenant.tenant_id)] = tenant

    async def get_tenant(self, tenant_id: TenantId) -> Tenant | None:
        return self.tenants.get(str(tenant_id))

    async def list_tenants(self, filter: TenantFilter) -> tuple[Tenant, ...]:
        rows = list(self.tenants.values())
        if not filter.include_disabled:
            rows = [t for t in rows if t.disabled_at is None]
        return tuple(rows)

    # ------------------------------------------------------------------
    # Workspaces
    # ------------------------------------------------------------------

    async def put_workspace(self, workspace: Workspace) -> None:
        self.workspaces[str(workspace.workspace_id)] = workspace

    async def get_workspace(self, workspace_id: WorkspaceId) -> Workspace | None:
        return self.workspaces.get(str(workspace_id))

    async def list_workspaces(self, filter: WorkspaceFilter) -> tuple[Workspace, ...]:
        rows = list(self.workspaces.values())
        if filter.tenant_id is not None:
            rows = [w for w in rows if w.tenant_id == filter.tenant_id]
        if not filter.include_disabled:
            rows = [w for w in rows if w.disabled_at is None]
        return tuple(rows)

    # ------------------------------------------------------------------
    # Principals
    # ------------------------------------------------------------------

    async def put_principal(self, principal: Principal) -> None:
        self.principals[str(principal.principal_id)] = principal

    async def get_principal(self, principal_id: PrincipalId) -> Principal | None:
        return self.principals.get(str(principal_id))

    async def list_principals(self, filter: PrincipalFilter) -> tuple[Principal, ...]:
        rows = list(self.principals.values())
        if filter.kind is not None:
            rows = [p for p in rows if p.kind == filter.kind]
        if filter.tenant_id is not None:
            rows = [p for p in rows if isinstance(p, User) and p.tenant_id == filter.tenant_id]
        if filter.workspace_id is not None:
            rows = [
                p
                for p in rows
                if isinstance(p, ServiceAccount) and p.workspace_id == filter.workspace_id
            ]
        if not filter.include_disabled:
            rows = [p for p in rows if p.disabled_at is None]
        return tuple(rows)

    async def disable_principal(
        self,
        principal_id: PrincipalId,
        actor: PrincipalId,
        reason: str,
    ) -> None:
        self.disable_principal_calls.append((str(principal_id), str(actor), reason))
        existing = self.principals.get(str(principal_id))
        if existing is None:
            return
        now = datetime.now(UTC)
        # Rebuild the frozen dataclass with the disabled fields populated.
        if isinstance(existing, User):
            self.principals[str(principal_id)] = User(
                kind="user",
                principal_id=existing.principal_id,
                tenant_id=existing.tenant_id,
                display_name=existing.display_name,
                email=existing.email,
                disabled_at=now,
                disabled_reason=reason,
                created_at=existing.created_at,
            )
        else:
            assert isinstance(existing, ServiceAccount)
            self.principals[str(principal_id)] = ServiceAccount(
                kind="serviceAccount",
                principal_id=existing.principal_id,
                workspace_id=existing.workspace_id,
                display_name=existing.display_name,
                disabled_at=now,
                disabled_reason=reason,
                created_at=existing.created_at,
            )

    # ------------------------------------------------------------------
    # OIDC identities (write-once on (issuer, subject))
    # ------------------------------------------------------------------

    async def put_oidc_identity(
        self,
        issuer: str,
        subject: str,
        user_id: PrincipalId,
    ) -> None:
        key = (issuer, subject)
        if key in self.oidc_identities:
            raise ImmutableViolation(f"OIDC identity ({issuer!r}, {subject!r}) is already bound")
        self.oidc_identities[key] = str(user_id)

    async def get_oidc_identity(
        self,
        issuer: str,
        subject: str,
    ) -> PrincipalId | None:
        raw = self.oidc_identities.get((issuer, subject))
        if raw is None:
            return None
        return PrincipalId(raw)

    # ------------------------------------------------------------------
    # Permissions (Phase D / AS-IMPL-008)
    # ------------------------------------------------------------------

    async def upsert_permission(self, permission: Permission) -> None:
        self.permissions[permission.name] = permission

    async def list_permissions(self) -> tuple[Permission, ...]:
        return tuple(self.permissions.values())

    # ------------------------------------------------------------------
    # Roles (Phase D / AS-IMPL-009)
    # ------------------------------------------------------------------

    async def put_role(self, role: Role) -> None:
        self.roles[str(role.role_id)] = role

    async def get_role(self, role_id: RoleId) -> Role | None:
        return self.roles.get(str(role_id))

    async def list_roles(self) -> tuple[Role, ...]:
        return tuple(self.roles.values())

    # ------------------------------------------------------------------
    # Role bindings (Phase D / AS-IMPL-010)
    # ------------------------------------------------------------------

    async def put_role_binding(self, binding: RoleBinding) -> None:
        self.role_bindings[str(binding.binding_id)] = binding

    async def delete_role_binding(
        self,
        binding_id: RoleBindingId,
        actor: PrincipalId,
        reason: str,
    ) -> None:
        self.delete_role_binding_calls.append((str(binding_id), str(actor), reason))
        # Match the SPL Postgres adapter: silently no-op on missing
        # — the calling handler is responsible for the 404 check.
        self.role_bindings.pop(str(binding_id), None)

    async def list_role_bindings_for_principal(
        self,
        principal_id: PrincipalId,
        scopes: tuple[RoleBindingScope, ...],
    ) -> tuple[RoleBinding, ...]:
        return tuple(
            b
            for b in self.role_bindings.values()
            if b.principal_id == principal_id and b.scope in scopes
        )

    async def list_role_bindings_for_scope(
        self,
        scope: RoleBindingScope,
        filter: RoleBindingFilter,
    ) -> tuple[RoleBinding, ...]:
        def _scope_eq(left: RoleBindingScope, right: RoleBindingScope) -> bool:
            if isinstance(left, WorkspaceScope) and isinstance(right, WorkspaceScope):
                return left.workspace_id == right.workspace_id
            if isinstance(left, TenantScope) and isinstance(right, TenantScope):
                return left.tenant_id == right.tenant_id
            return isinstance(left, GlobalScope) and isinstance(right, GlobalScope)

        rows = [b for b in self.role_bindings.values() if _scope_eq(b.scope, scope)]
        if filter.role_id is not None:
            rows = [b for b in rows if b.role_id == filter.role_id]
        if filter.principal_id is not None:
            rows = [b for b in rows if b.principal_id == filter.principal_id]
        return tuple(rows)

    # ------------------------------------------------------------------
    # Service tokens (Phase F / AS-IMPL-013)
    # ------------------------------------------------------------------

    async def put_service_token(self, token: ServiceToken) -> None:
        self.service_tokens[str(token.token_id)] = token

    async def get_service_token_by_hash(self, hash: str) -> ServiceToken | None:
        for token in self.service_tokens.values():
            if token.hash == hash:
                return token
        return None

    async def get_service_token(self, token_id: ServiceTokenId) -> ServiceToken | None:
        return self.service_tokens.get(str(token_id))

    async def revoke_service_token(
        self,
        token_id: ServiceTokenId,
        actor: PrincipalId,
        reason: str,
    ) -> None:
        self.revoke_service_token_calls.append((str(token_id), str(actor), reason))
        existing = self.service_tokens.get(str(token_id))
        if existing is None:
            return
        now = datetime.now(UTC)
        self.service_tokens[str(token_id)] = ServiceToken(
            token_id=existing.token_id,
            service_account_id=existing.service_account_id,
            hash=existing.hash,
            issued_at=existing.issued_at,
            expires_at=existing.expires_at,
            revoked_at=now,
            revoked_by=actor,
            revoked_reason=reason,
        )

    async def list_service_tokens_for_service_account(
        self,
        service_account_id: PrincipalId,
    ) -> tuple[ServiceToken, ...]:
        return tuple(
            t for t in self.service_tokens.values() if t.service_account_id == service_account_id
        )

    async def list_expired_service_tokens(self, before: datetime) -> tuple[ServiceToken, ...]:
        return tuple(t for t in self.service_tokens.values() if t.expires_at < before)

    async def delete_expired_service_tokens(self, before: datetime) -> int:
        victims = [tid for tid, t in self.service_tokens.items() if t.expires_at < before]
        for tid in victims:
            del self.service_tokens[tid]
        return len(victims)

    # ------------------------------------------------------------------
    # Transactions
    # ------------------------------------------------------------------

    async def with_transaction(
        self,
        body: Callable[[TransactionHandle], Awaitable[T]],
    ) -> T:
        """In-memory ``with_transaction`` that auto-commits.

        The fake has no real backing store, so no rollback semantics
        are simulated — Phase D handlers commit binding + audit
        sequentially anyway (the SPL ``with_transaction`` is intra-
        provider only). The handle is registered through the standard
        SPL machinery so any future tests that mis-use cross-provider
        handles will surface the canonical :class:`InvalidTransactionHandle`.
        """
        handle = _FakeAuthTransactionHandle()
        bind_handle(handle, self)
        return await body(handle)


class FakeMetadataAdapter:
    """In-memory ``MetadataStoreProvider`` mirror used by auth-service wiring tests.

    Covers the migration surface plus a minimal ``append_audit`` hook so
    auth-service audit modules can be unit-tested without depending on
    Postgres. The full Protocol surface (runs, steps, triggers, cursors,
    idempotency, leases, audit reads) is exercised by the conformance
    suite under ``src/libs/custos-postgres/tests/test_integration.py``.
    """

    SCHEMA_REVISION = 4

    def __init__(
        self,
        *,
        applied_revisions: AbstractSet[int] | None = None,
        append_audit_should_fail: bool = False,
    ) -> None:
        self._applied: set[int] = set(
            {1, 2, 3, 4} if applied_revisions is None else applied_revisions,
        )
        self.refresh_calls = 0
        self.append_audit_calls: list[tuple[str, Any]] = []
        self.append_audit_should_fail = append_audit_should_fail

    @property
    def declared_revisions(self) -> Mapping[str, AbstractSet[int]]:
        return MappingProxyType(
            {"MetadataStoreProvider": frozenset(self._applied)},
        )

    async def apply_pending(self) -> list[str]:  # pragma: no cover - not exercised
        return []

    async def refresh_declared(self) -> None:
        self.refresh_calls += 1

    def set_applied(self, revisions: AbstractSet[int]) -> None:
        self._applied = set(revisions)

    async def append_audit(
        self,
        workspace_id: object,
        event: object,
        tx: object = None,
    ) -> None:
        if self.append_audit_should_fail:
            raise RuntimeError("simulated audit-outbox failure")
        self.append_audit_calls.append((str(workspace_id), event))


__all__ = [
    "FakeAuthAdapter",
    "FakeMetadataAdapter",
]

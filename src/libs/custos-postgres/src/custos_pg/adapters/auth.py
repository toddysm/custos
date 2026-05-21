"""Postgres adapter for `AuthStoreProvider`.

Implements identity, tenancy, and RBAC persistence.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import asyncpg

from custos_pg.pool import LazyPool
from custos_spl.errors import ImmutableViolation
from custos_spl.ids import PrincipalId, TenantId, WorkspaceId
from custos_spl.interfaces.auth_store import (
    AuthStoreProvider,
    OidcIdentity,
    Principal,
    PrincipalFilter,
    ServiceAccount,
    Tenant,
    TenantFilter,
    User,
    Workspace,
    WorkspaceFilter,
)
from custos_spl.migrations.runner import MigrationCapable

from custos_pg.migrations.auth import AUTH_REV1


class PgAuthAdapter(MigrationCapable):
    """Postgres implementation of AuthStoreProvider."""

    SCHEMA_REVISION = 1
    INTERFACE_NAME = "AuthStoreProvider"

    def __init__(
        self,
        *,
        pool: asyncpg.Pool | None = None,
        lazy: LazyPool | None = None,
    ) -> None:
        if (pool is None and lazy is None) or (pool is not None and lazy is not None):
            raise ValueError("__init__ requires either pool= or lazy=, not both")
        self._pool = pool
        self._lazy = lazy
        self._applied_revisions: set[int] = set()

    async def _pool_ref(self) -> asyncpg.Pool:
        """Acquire the connection pool (lazy or eager)."""
        if self._pool is not None:
            return self._pool
        return await self._lazy()

    @property
    def declared_revisions(self) -> dict[str, frozenset[int]]:
        """Revisions applied by this adapter."""
        return {self.INTERFACE_NAME: frozenset(self._applied_revisions)}

    async def refresh_declared(self) -> None:
        """Refresh declared revisions from the migration ledger."""
        from custos_pg.adapters.metadata import read_declared

        pool = await self._pool_ref()
        declared = await read_declared(pool, (self.INTERFACE_NAME,))
        self._applied_revisions = set(declared.get(self.INTERFACE_NAME, set()))

    async def apply_pending(self) -> list[str]:
        """Apply pending migrations."""
        from custos_pg.adapters.metadata import ensure_ledger, record_revision

        summaries: list[str] = []
        pool = await self._pool_ref()
        async with pool.acquire() as conn, conn.transaction():
            await ensure_ledger(conn)
            applied = await conn.fetch(
                "SELECT revision FROM custos_meta.adapter_revisions WHERE interface_name = $1",
                self.INTERFACE_NAME,
            )
            already = {int(r["revision"]) for r in applied}
            for rev in (AUTH_REV1,):
                if rev.number in already:
                    continue
                for stmt in rev.statements:
                    await conn.execute(stmt)
                await record_revision(conn, self.INTERFACE_NAME, rev.number)
                summaries.append(
                    f"applied {self.INTERFACE_NAME} rev{rev.number} ({len(rev.statements)} statements)"
                )
        await self.refresh_declared()
        return summaries

    # ----- Tenants -----

    async def put_tenant(self, tenant: Tenant) -> None:
        """Insert or update a tenant."""
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO auth.tenant (tenant_id, display_name, disabled_at, created_at)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (tenant_id) DO UPDATE SET
                    display_name = $2,
                    disabled_at = $3
                """,
                tenant.tenant_id,
                tenant.display_name,
                tenant.disabled_at,
                tenant.created_at,
            )

    async def get_tenant(self, tenant_id: TenantId) -> Tenant | None:
        """Read a tenant by ID."""
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT tenant_id, display_name, disabled_at, created_at
                FROM auth.tenant
                WHERE tenant_id = $1
                """,
                tenant_id,
            )
            if row is None:
                return None
            return Tenant(
                tenant_id=row["tenant_id"],
                display_name=row["display_name"],
                disabled_at=row["disabled_at"],
                created_at=row["created_at"],
            )

    async def list_tenants(self, filter: TenantFilter) -> tuple[Tenant, ...]:
        """List tenants matching filter."""
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            query = "SELECT tenant_id, display_name, disabled_at, created_at FROM auth.tenant"
            params: list[Any] = []

            if not filter.include_disabled:
                query += " WHERE disabled_at IS NULL"

            query += " ORDER BY created_at DESC"

            rows = await conn.fetch(query, *params)
            return tuple(
                Tenant(
                    tenant_id=row["tenant_id"],
                    display_name=row["display_name"],
                    disabled_at=row["disabled_at"],
                    created_at=row["created_at"],
                )
                for row in rows
            )

    # ----- Workspaces -----

    async def put_workspace(self, workspace: Workspace) -> None:
        """Insert or update a workspace."""
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO auth.workspace (workspace_id, tenant_id, display_name, disabled_at, created_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (workspace_id) DO UPDATE SET
                    display_name = $3,
                    disabled_at = $4
                """,
                workspace.workspace_id,
                workspace.tenant_id,
                workspace.display_name,
                workspace.disabled_at,
                workspace.created_at,
            )

    async def get_workspace(self, workspace_id: WorkspaceId) -> Workspace | None:
        """Read a workspace by ID."""
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT workspace_id, tenant_id, display_name, disabled_at, created_at
                FROM auth.workspace
                WHERE workspace_id = $1
                """,
                workspace_id,
            )
            if row is None:
                return None
            return Workspace(
                workspace_id=row["workspace_id"],
                tenant_id=row["tenant_id"],
                display_name=row["display_name"],
                disabled_at=row["disabled_at"],
                created_at=row["created_at"],
            )

    async def list_workspaces(self, filter: WorkspaceFilter) -> tuple[Workspace, ...]:
        """List workspaces matching filter."""
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            query = """
                SELECT workspace_id, tenant_id, display_name, disabled_at, created_at
                FROM auth.workspace
            """
            params: list[Any] = []
            conditions = []

            if filter.tenant_id is not None:
                conditions.append(f"tenant_id = ${len(params) + 1}")
                params.append(filter.tenant_id)

            if not filter.include_disabled:
                conditions.append("disabled_at IS NULL")

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            query += " ORDER BY created_at DESC"

            rows = await conn.fetch(query, *params)
            return tuple(
                Workspace(
                    workspace_id=row["workspace_id"],
                    tenant_id=row["tenant_id"],
                    display_name=row["display_name"],
                    disabled_at=row["disabled_at"],
                    created_at=row["created_at"],
                )
                for row in rows
            )

    # ----- Principals -----

    async def put_principal(self, principal: Principal) -> None:
        """Insert or update a principal (User or ServiceAccount)."""
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            if isinstance(principal, User):
                await conn.execute(
                    """
                    INSERT INTO auth.principal (
                        principal_id, kind, tenant_id, workspace_id,
                        display_name, email, disabled_at, disabled_reason, created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (principal_id) DO UPDATE SET
                        kind = $2,
                        tenant_id = $3,
                        workspace_id = $4,
                        display_name = $5,
                        email = $6,
                        disabled_at = $7,
                        disabled_reason = $8
                    """,
                    principal.principal_id,
                    "user",
                    principal.tenant_id,
                    None,
                    principal.display_name,
                    principal.email,
                    principal.disabled_at,
                    principal.disabled_reason,
                    principal.created_at,
                )
            elif isinstance(principal, ServiceAccount):
                await conn.execute(
                    """
                    INSERT INTO auth.principal (
                        principal_id, kind, tenant_id, workspace_id,
                        display_name, email, disabled_at, disabled_reason, created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (principal_id) DO UPDATE SET
                        kind = $2,
                        tenant_id = $3,
                        workspace_id = $4,
                        display_name = $5,
                        email = $6,
                        disabled_at = $7,
                        disabled_reason = $8
                    """,
                    principal.principal_id,
                    "serviceAccount",
                    None,
                    principal.workspace_id,
                    principal.display_name,
                    None,
                    principal.disabled_at,
                    principal.disabled_reason,
                    principal.created_at,
                )
            else:
                raise TypeError(
                    f"principal must be User or ServiceAccount, got {type(principal).__name__}"
                )

    async def get_principal(self, principal_id: PrincipalId) -> Principal | None:
        """Read a principal by ID, returning the matching union variant."""
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT principal_id, kind, tenant_id, workspace_id,
                       display_name, email, disabled_at, disabled_reason, created_at
                FROM auth.principal
                WHERE principal_id = $1
                """,
                principal_id,
            )
            if row is None:
                return None
            return self._reconstruct_principal(row)

    async def list_principals(self, filter: PrincipalFilter) -> tuple[Principal, ...]:
        """List principals matching filter."""
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            query = """
                SELECT principal_id, kind, tenant_id, workspace_id,
                       display_name, email, disabled_at, disabled_reason, created_at
                FROM auth.principal
            """
            params: list[Any] = []
            conditions = []

            if filter.tenant_id is not None:
                conditions.append(f"tenant_id = ${len(params) + 1}")
                params.append(filter.tenant_id)

            if filter.workspace_id is not None:
                conditions.append(f"workspace_id = ${len(params) + 1}")
                params.append(filter.workspace_id)

            if filter.kind is not None:
                conditions.append(f"kind = ${len(params) + 1}")
                params.append(filter.kind)

            if not filter.include_disabled:
                conditions.append("disabled_at IS NULL")

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            query += " ORDER BY created_at DESC"

            rows = await conn.fetch(query, *params)
            return tuple(self._reconstruct_principal(row) for row in rows)

    async def disable_principal(
        self, principal_id: PrincipalId, actor: PrincipalId, reason: str
    ) -> None:
        """Soft-disable a principal.

        Sets disabled_at and disabled_reason on the principal row. The actor
        and reason are intended for audit trail recording via the transaction
        handle (SPL-130h); until transaction support lands, audit trail
        emission is deferred and the actor parameter is presently unused.
        """
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE auth.principal
                SET disabled_at = now(), disabled_reason = $2
                WHERE principal_id = $1
                """,
                principal_id,
                reason,
            )

    def _reconstruct_principal(self, row: dict) -> Principal:
        """Reconstruct Principal union from database row."""
        if row["kind"] == "user":
            return User(
                kind="user",
                principal_id=row["principal_id"],
                tenant_id=row["tenant_id"],
                display_name=row["display_name"],
                email=row["email"],
                disabled_at=row["disabled_at"],
                disabled_reason=row["disabled_reason"],
                created_at=row["created_at"],
            )
        elif row["kind"] == "serviceAccount":
            return ServiceAccount(
                kind="serviceAccount",
                principal_id=row["principal_id"],
                workspace_id=row["workspace_id"],
                display_name=row["display_name"],
                disabled_at=row["disabled_at"],
                disabled_reason=row["disabled_reason"],
                created_at=row["created_at"],
            )
        else:
            raise ValueError(
                f"Unknown principal kind: {row['kind']!r} (expected 'user' or 'serviceAccount')"
            )


    # ----- OIDC identities -----

    async def put_oidc_identity(
        self,
        issuer: str,
        subject: str,
        user_id: PrincipalId,
    ) -> None:
        """Bind an OIDC (issuer, subject) to an internal user.

        Write-once on (issuer, subject). Raises ImmutableViolation if already
        bound (even to the same user_id).
        """
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            try:
                await conn.execute(
                    """
                    INSERT INTO auth.oidc_identity (issuer, subject, user_id, bound_at)
                    VALUES ($1, $2, $3, $4)
                    """,
                    issuer,
                    subject,
                    user_id,
                    datetime.now(UTC),
                )
            except asyncpg.UniqueViolationError as exc:
                raise ImmutableViolation(
                    f"OIDC identity ({issuer}, {subject}) already bound"
                ) from exc

    async def get_oidc_identity(
        self,
        issuer: str,
        subject: str,
    ) -> PrincipalId | None:
        """Resolve an OIDC (issuer, subject) to a user.

        Verifier hot path; None means "unknown OIDC identity, treat as
        unauthenticated". Indexed on (issuer, subject).
        """
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT user_id FROM auth.oidc_identity
                WHERE issuer = $1 AND subject = $2
                """,
                issuer,
                subject,
            )
            if row is None:
                return None
            return PrincipalId(row["user_id"])

    async def list_oidc_identities_for_user(
        self,
        user_id: PrincipalId,
    ) -> tuple[OidcIdentity, ...]:
        """List all OIDC identities bound to a user."""
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT issuer, subject, user_id, bound_at
                FROM auth.oidc_identity
                WHERE user_id = $1
                ORDER BY bound_at DESC
                """,
                user_id,
            )
            return tuple(
                OidcIdentity(
                    issuer=row["issuer"],
                    subject=row["subject"],
                    user_id=PrincipalId(row["user_id"]),
                    bound_at=row["bound_at"],
                )
                for row in rows
            )


    async def put_service_token(self, token: object) -> None:
        """Not yet implemented (SPL-130e)."""
        raise NotImplementedError("Service tokens: SPL-130e")

    async def get_service_token_by_hash(self, hash: object) -> object | None:
        """Not yet implemented (SPL-130e)."""
        raise NotImplementedError("Service tokens: SPL-130e")

    async def revoke_service_token(
        self, token_id: object, actor: object, reason: object
    ) -> None:
        """Not yet implemented (SPL-130e)."""
        raise NotImplementedError("Service tokens: SPL-130e")

    async def list_service_tokens_for_service_account(
        self, service_account_id: object
    ) -> list:
        """Not yet implemented (SPL-130e)."""
        raise NotImplementedError("Service tokens: SPL-130e")

    async def delete_expired_service_tokens(self, before: object) -> None:
        """Not yet implemented (SPL-130e)."""
        raise NotImplementedError("Service tokens: SPL-130e")

    async def upsert_permission(self, permission: object) -> None:
        """Not yet implemented (SPL-130f)."""
        raise NotImplementedError("Permissions: SPL-130f")

    async def list_permissions(self) -> list:
        """Not yet implemented (SPL-130f)."""
        raise NotImplementedError("Permissions: SPL-130f")

    async def put_role(self, role: object) -> None:
        """Not yet implemented (SPL-130f)."""
        raise NotImplementedError("Roles: SPL-130f")

    async def get_role(self, role_id: object) -> object | None:
        """Not yet implemented (SPL-130f)."""
        raise NotImplementedError("Roles: SPL-130f")

    async def list_roles(self) -> list:
        """Not yet implemented (SPL-130f)."""
        raise NotImplementedError("Roles: SPL-130f")

    async def put_role_binding(self, binding: object) -> None:
        """Not yet implemented (SPL-130g)."""
        raise NotImplementedError("Role bindings: SPL-130g")

    async def delete_role_binding(
        self, binding_id: object, actor: object, reason: object
    ) -> None:
        """Not yet implemented (SPL-130g)."""
        raise NotImplementedError("Role bindings: SPL-130g")

    async def list_role_bindings_for_principal(
        self, principal_id: object, scopes: object
    ) -> list:
        """Not yet implemented (SPL-130g)."""
        raise NotImplementedError("Role bindings: SPL-130g")

    async def list_role_bindings_for_scope(
        self, scope: object, filter: object | None = None
    ) -> list:
        """Not yet implemented (SPL-130g)."""
        raise NotImplementedError("Role bindings: SPL-130g")

    async def with_transaction(self, callback: object) -> None:
        """Not yet implemented (SPL-130h)."""
        raise NotImplementedError("Transactions: SPL-130h")


def make_adapter() -> PgAuthAdapter:
    """Factory: create an AuthStoreProvider adapter from environment DSN."""
    from custos_pg.pool import read_dsn_from_env

    dsn = read_dsn_from_env()
    return PgAuthAdapter(lazy=LazyPool(dsn))

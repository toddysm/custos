"""Postgres adapter for `AuthStoreProvider`.

Implements identity, tenancy, and RBAC persistence.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import asyncpg

from custos_pg.pool import LazyPool
from custos_spl.ids import TenantId, WorkspaceId
from custos_spl.interfaces.auth_store import (
    AuthStoreProvider,
    Tenant,
    TenantFilter,
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

    async def list_tenants(self, filter: TenantFilter | None = None) -> list[Tenant]:
        """List tenants with optional filters."""
        if filter is None:
            filter = TenantFilter()

        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            query = "SELECT tenant_id, display_name, disabled_at, created_at FROM auth.tenant"
            params: list[Any] = []

            if not filter.include_disabled:
                query += " WHERE disabled_at IS NULL"

            query += " ORDER BY created_at DESC"

            rows = await conn.fetch(query, *params)
            return [
                Tenant(
                    tenant_id=row["tenant_id"],
                    display_name=row["display_name"],
                    disabled_at=row["disabled_at"],
                    created_at=row["created_at"],
                )
                for row in rows
            ]

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

    async def list_workspaces(
        self, filter: WorkspaceFilter | None = None
    ) -> list[Workspace]:
        """List workspaces with optional filters."""
        if filter is None:
            filter = WorkspaceFilter()

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
            return [
                Workspace(
                    workspace_id=row["workspace_id"],
                    tenant_id=row["tenant_id"],
                    display_name=row["display_name"],
                    disabled_at=row["disabled_at"],
                    created_at=row["created_at"],
                )
                for row in rows
            ]

    # ----- Unimplemented (SPL-130c and later) -----

    async def put_principal(self, principal: object) -> None:
        """Not yet implemented (SPL-130c)."""
        raise NotImplementedError("Principals: SPL-130c")

    async def get_principal(self, principal_id: object) -> None:
        """Not yet implemented (SPL-130c)."""
        raise NotImplementedError("Principals: SPL-130c")

    async def list_principals(self, filter: object | None = None) -> list:
        """Not yet implemented (SPL-130c)."""
        raise NotImplementedError("Principals: SPL-130c")

    async def disable_principal(
        self, principal_id: object, actor: object, reason: object
    ) -> None:
        """Not yet implemented (SPL-130c)."""
        raise NotImplementedError("Principals: SPL-130c")

    async def put_oidc_identity(
        self, issuer: object, subject: object, user_id: object
    ) -> None:
        """Not yet implemented (SPL-130d)."""
        raise NotImplementedError("OIDC identities: SPL-130d")

    async def get_oidc_identity(
        self, issuer: object, subject: object
    ) -> object | None:
        """Not yet implemented (SPL-130d)."""
        raise NotImplementedError("OIDC identities: SPL-130d")

    async def list_oidc_identities_for_user(self, user_id: object) -> list:
        """Not yet implemented (SPL-130d)."""
        raise NotImplementedError("OIDC identities: SPL-130d")

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

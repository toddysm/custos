"""Postgres adapter for `AuthStoreProvider`.

Implements identity, tenancy, and RBAC persistence.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, TypeVar

import asyncpg

from custos_pg.pool import LazyPool
from custos_spl.errors import ImmutableViolation
from custos_spl.middleware.transactions import bind_handle
from custos_spl.ids import PrincipalId, RoleBindingId, RoleId, ServiceTokenId, TenantId, WorkspaceId
from custos_spl.interfaces.auth_store import (
    AuthStoreProvider,
    GlobalScope,
    OidcIdentity,
    Permission,
    Principal,
    PrincipalFilter,
    Role,
    RoleBinding,
    RoleBindingFilter,
    RoleBindingScope,
    ServiceAccount,
    ServiceToken,
    TenantScope,
    Tenant,
    TenantFilter,
    TransactionHandle,
    User,
    Workspace,
    WorkspaceFilter,
    WorkspaceScope,
)
from custos_spl.migrations.runner import MigrationCapable

from custos_pg.migrations.auth import AUTH_REV1


T = TypeVar("T")


class PgAuthTransactionHandle(TransactionHandle):
    """Concrete handle pinning a single asyncpg connection inside `with_transaction`.

    The connection has an active `BEGIN`; tx-aware methods that
    receive this handle issue their statements on the pinned
    connection so they share atomicity with the rest of the callback.

    The handle is one-shot: `with_transaction` marks it `closed` in a
    `finally` block once the callback returns (success or failure).
    A caller that retains a reference and passes it back later gets
    `InvalidTransactionHandle` from tx-aware methods instead of
    silently running on a connection that has been returned to the
    pool — `check_handle` alone only verifies provider ownership, not
    liveness of the underlying transaction.
    """

    __slots__ = ("_closed", "_conn")

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn
        self._closed = False

    @property
    def conn(self) -> asyncpg.Connection:
        return self._conn

    @property
    def closed(self) -> bool:
        return self._closed

    def _mark_closed(self) -> None:
        self._closed = True


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
                    VALUES ($1, $2, $3, now())
                    """,
                    issuer,
                    subject,
                    user_id,
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


    # ----- Service tokens -----

    async def put_service_token(self, token: ServiceToken) -> None:
        """Insert or update a service token row.

        Only hash is persisted; plaintext is never stored. token_id is the
        identity; rotating is new row + revoke-old, not in-place hash update.
        Hash is immutable: if a row with this token_id already exists with a
        different hash, raises ImmutableViolation.
        """
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            try:
                await conn.execute(
                    """
                    INSERT INTO auth.service_token (
                        token_id, service_account_id, hash,
                        issued_at, expires_at, revoked_at, revoked_by, revoked_reason
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    """,
                    token.token_id,
                    token.service_account_id,
                    token.hash,
                    token.issued_at,
                    token.expires_at,
                    token.revoked_at,
                    token.revoked_by,
                    token.revoked_reason,
                )
            except asyncpg.UniqueViolationError:
                existing = await conn.fetchrow(
                    "SELECT hash FROM auth.service_token WHERE token_id = $1",
                    token.token_id,
                )
                if existing and existing["hash"] != token.hash:
                    raise ImmutableViolation(
                        f"service_token hash for {token.token_id} is immutable"
                    )
                await conn.execute(
                    """
                    UPDATE auth.service_token
                    SET service_account_id = $2,
                        issued_at = $3,
                        expires_at = $4,
                        revoked_at = $5,
                        revoked_by = $6,
                        revoked_reason = $7
                    WHERE token_id = $1
                    """,
                    token.token_id,
                    token.service_account_id,
                    token.issued_at,
                    token.expires_at,
                    token.revoked_at,
                    token.revoked_by,
                    token.revoked_reason,
                )

    async def get_service_token_by_hash(self, hash: str) -> ServiceToken | None:
        """Resolve a hashed token to its row.

        Verifier hot path, returns None if no match (treat as invalid).
        Indexed on hash. Callers check revoked_at/expires_at; this method
        returns all rows, revoked or expired.
        """
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT token_id, service_account_id, hash,
                       issued_at, expires_at, revoked_at, revoked_by, revoked_reason
                FROM auth.service_token
                WHERE hash = $1
                """,
                hash,
            )
            if row is None:
                return None
            return ServiceToken(
                token_id=ServiceTokenId(row["token_id"]),
                service_account_id=PrincipalId(row["service_account_id"]),
                hash=row["hash"],
                issued_at=row["issued_at"],
                expires_at=row["expires_at"],
                revoked_at=row["revoked_at"],
                revoked_by=PrincipalId(row["revoked_by"]) if row["revoked_by"] else None,
                revoked_reason=row["revoked_reason"],
            )

    async def revoke_service_token(
        self, token_id: ServiceTokenId, actor: PrincipalId, reason: str
    ) -> None:
        """Mark a token revoked.

        Sets revoked_at, revoked_by, revoked_reason; never deletes row.
        Sweeper handles physical removal separately.
        """
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE auth.service_token
                SET revoked_at = now(), revoked_by = $2, revoked_reason = $3
                WHERE token_id = $1
                """,
                token_id,
                actor,
                reason,
            )

    async def list_service_tokens_for_service_account(
        self,
        service_account_id: PrincipalId,
    ) -> tuple[ServiceToken, ...]:
        """List all tokens for a service account.

        Bounded small; includes revoked rows so callers can render rotation history.
        Returns the full set, not paginated.
        """
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT token_id, service_account_id, hash,
                       issued_at, expires_at, revoked_at, revoked_by, revoked_reason
                FROM auth.service_token
                WHERE service_account_id = $1
                ORDER BY issued_at DESC
                """,
                service_account_id,
            )
            return tuple(
                ServiceToken(
                    token_id=ServiceTokenId(row["token_id"]),
                    service_account_id=PrincipalId(row["service_account_id"]),
                    hash=row["hash"],
                    issued_at=row["issued_at"],
                    expires_at=row["expires_at"],
                    revoked_at=row["revoked_at"],
                    revoked_by=PrincipalId(row["revoked_by"]) if row["revoked_by"] else None,
                    revoked_reason=row["revoked_reason"],
                )
                for row in rows
            )

    async def delete_expired_service_tokens(self, before: datetime) -> int:
        """Physical delete of expired tokens.

        Sweeper-only operation. Removes rows where expires_at < before.
        Returns the number of rows deleted.
        """
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            result = await conn.execute(
                """
                DELETE FROM auth.service_token
                WHERE expires_at < $1
                """,
                before,
            )
            # Extract count from result string like "DELETE N"
            return int(result.split()[-1])


    # ----- Permissions -----

    async def upsert_permission(self, permission: Permission) -> None:
        """Insert or update a permission by name.

        Called at platform startup for each declared permission. name is
        the primary key; re-upserting updates description.
        """
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO auth.permission (name, description)
                VALUES ($1, $2)
                ON CONFLICT (name) DO UPDATE SET description = EXCLUDED.description
                """,
                permission.name,
                permission.description,
            )

    async def list_permissions(self) -> tuple[Permission, ...]:
        """List all declared permissions.

        Bounded small; returns the full set without pagination.
        """
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT name, description FROM auth.permission ORDER BY name"
            )
            return tuple(
                Permission(name=row["name"], description=row["description"])
                for row in rows
            )

    # ----- Roles -----

    async def put_role(self, role: Role) -> None:
        """Insert or update a role by role_id.

        role_id is the primary key; upsert updates name, description, and
        permission_names.
        """
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO auth.role (role_id, name, description, permission_names)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (role_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    permission_names = EXCLUDED.permission_names
                """,
                role.role_id,
                role.name,
                role.description,
                list(role.permission_names),
            )

    async def get_role(self, role_id: RoleId) -> Role | None:
        """Read a role by ID. Returns None if absent."""
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT role_id, name, description, permission_names
                FROM auth.role
                WHERE role_id = $1
                """,
                role_id,
            )
            if row is None:
                return None
            return Role(
                role_id=RoleId(row["role_id"]),
                name=row["name"],
                description=row["description"],
                permission_names=tuple(row["permission_names"]),
            )

    async def list_roles(self) -> tuple[Role, ...]:
        """List all roles. Bounded small."""
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT role_id, name, description, permission_names
                FROM auth.role
                ORDER BY role_id
                """
            )
            return tuple(
                Role(
                    role_id=RoleId(row["role_id"]),
                    name=row["name"],
                    description=row["description"],
                    permission_names=tuple(row["permission_names"]),
                )
                for row in rows
            )

    # ----- Role bindings -----

    def _scope_to_json(self, scope: RoleBindingScope) -> dict[str, Any]:
        """Serialize RoleBindingScope discriminated union to JSON.

        Uses 'type' as discriminator key per schema documentation.
        """
        if isinstance(scope, WorkspaceScope):
            return {"type": "workspace", "workspace_id": scope.workspace_id}
        elif isinstance(scope, TenantScope):
            return {"type": "tenant", "tenant_id": scope.tenant_id}
        elif isinstance(scope, GlobalScope):
            return {"type": "global"}
        else:
            raise TypeError(f"unknown scope type: {type(scope)}")

    def _json_to_scope(self, data: dict[str, Any]) -> RoleBindingScope:
        """Deserialize JSON to RoleBindingScope discriminated union.

        Accepts both 'type' and 'kind' keys for backward compatibility.
        """
        kind = data.get("type") or data.get("kind")
        if kind == "workspace":
            return WorkspaceScope(workspace_id=data["workspace_id"])
        elif kind == "tenant":
            return TenantScope(tenant_id=data["tenant_id"])
        elif kind == "global":
            return GlobalScope()
        else:
            raise ValueError(f"unknown scope kind: {kind}")

    async def put_role_binding(self, binding: RoleBinding) -> None:
        """Insert or update a role binding.

        Upsert by binding_id so callers can safely retry. The (principal_id,
        scope) index is the authorization hot path.
        """
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO auth.role_binding (
                    binding_id, principal_id, role_id, scope, bound_at, bound_by
                ) VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (binding_id) DO UPDATE SET
                    principal_id = EXCLUDED.principal_id,
                    role_id = EXCLUDED.role_id,
                    scope = EXCLUDED.scope,
                    bound_at = EXCLUDED.bound_at,
                    bound_by = EXCLUDED.bound_by
                """,
                binding.binding_id,
                binding.principal_id,
                binding.role_id,
                self._scope_to_json(binding.scope),
                binding.bound_at,
                binding.bound_by,
            )

    async def delete_role_binding(
        self, binding_id: RoleBindingId, actor: PrincipalId, reason: str
    ) -> None:
        """Delete a role binding.

        Physical delete (revocation-by-removal). actor and reason are
        captured in audit trail via transaction handle, not on row.
        """
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM auth.role_binding WHERE binding_id = $1",
                binding_id,
            )

    async def list_role_bindings_for_principal(
        self,
        principal_id: PrincipalId,
        scopes: tuple[RoleBindingScope, ...],
    ) -> tuple[RoleBinding, ...]:
        """List a principal's bindings at the supplied scopes.

        Authorization hot path: assembles relevant scope set (workspace
        + tenant + global) and reads bindings in one round-trip.
        """
        if not scopes:
            return ()

        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            scope_jsons = [self._scope_to_json(s) for s in scopes]
            rows = await conn.fetch(
                """
                SELECT binding_id, principal_id, role_id, scope, bound_at, bound_by
                FROM auth.role_binding
                WHERE principal_id = $1 AND scope = ANY($2)
                ORDER BY bound_at DESC
                """,
                principal_id,
                scope_jsons,
            )
            return tuple(
                RoleBinding(
                    binding_id=RoleBindingId(row["binding_id"]),
                    principal_id=PrincipalId(row["principal_id"]),
                    role_id=RoleId(row["role_id"]),
                    scope=self._json_to_scope(row["scope"]),
                    bound_at=row["bound_at"],
                    bound_by=PrincipalId(row["bound_by"]),
                )
                for row in rows
            )

    async def list_role_bindings_for_scope(
        self,
        scope: RoleBindingScope,
        filter: RoleBindingFilter | None = None,
    ) -> tuple[RoleBinding, ...]:
        """Admin view: all bindings at a given scope.

        Bounded by workspace/tenant size; returns full set without
        pagination.
        """
        pool = await self._pool_ref()
        async with pool.acquire() as conn:
            scope_json = self._scope_to_json(scope)
            query = """
                SELECT binding_id, principal_id, role_id, scope, bound_at, bound_by
                FROM auth.role_binding
                WHERE scope = $1
            """
            params: list[Any] = [scope_json]

            if filter:
                if filter.role_id is not None:
                    query += " AND role_id = $2"
                    params.append(filter.role_id)
                if filter.principal_id is not None:
                    idx = len(params) + 1
                    query += f" AND principal_id = ${idx}"
                    params.append(filter.principal_id)

            query += " ORDER BY bound_at DESC"
            rows = await conn.fetch(query, *params)

            return tuple(
                RoleBinding(
                    binding_id=RoleBindingId(row["binding_id"]),
                    principal_id=PrincipalId(row["principal_id"]),
                    role_id=RoleId(row["role_id"]),
                    scope=self._json_to_scope(row["scope"]),
                    bound_at=row["bound_at"],
                    bound_by=PrincipalId(row["bound_by"]),
                )
                for row in rows
            )

    async def with_transaction(self, fn: Callable[[TransactionHandle], Awaitable[T]]) -> T:
        """Execute a callback within an atomic transaction.

        Acquires a connection, opens a transaction, creates a transaction
        handle, invokes the callback, and ensures the handle is marked
        closed before the transaction ends. Callers can use the handle
        in tx-aware methods to share atomicity.
        """
        pool = await self._pool_ref()
        async with pool.acquire() as conn, conn.transaction():
            handle = PgAuthTransactionHandle(conn)
            bind_handle(handle, self)
            try:
                return await fn(handle)
            finally:
                # Mark the handle dead the moment control leaves the
                # callback. The asyncpg `async with` block then ends
                # the transaction and returns the connection to the
                # pool, after which any retained reference must not be
                # usable via `_check_tx_handle`.
                handle._mark_closed()


def make_adapter() -> PgAuthAdapter:
    """Factory: create an AuthStoreProvider adapter from environment DSN."""
    from custos_pg.pool import read_dsn_from_env

    dsn = read_dsn_from_env()
    return PgAuthAdapter(lazy=LazyPool(dsn))

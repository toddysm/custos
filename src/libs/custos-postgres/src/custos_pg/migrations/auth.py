"""Revision-1 DDL for `AuthStoreProvider`.

Schema: tenancy, principals, OIDC identity bindings, service tokens, RBAC.

Tenancy & workspaces:
  - `auth.tenant` — tenancy boundary. Owns workspaces.
  - `auth.workspace` — workspace row with FK to tenant_id.
    These tables are NOT scoped to workspace (they define workspaces).

Principals:
  - `auth.principal` — discriminated union: kind ∈ {user, serviceAccount}.
    Users are tenant-scoped; ServiceAccounts are workspace-scoped.

OIDC identities:
  - `auth.oidc_identity` — write-once on (issuer, subject). Unique constraint
    enforces the contract; re-binding requires explicit delete + re-put.

Service tokens:
  - `auth.service_token` — hashed credentials (plaintext never persisted).
    Indexed on hash for the verifier hot-path. Soft-revoke via revoked_at.

Permissions & roles:
  - `auth.permission` — declared permission names. Upserted at startup.
  - `auth.role` — role definition with bundled permission_names (ARRAY).

Role bindings:
  - `auth.role_binding` — assigns role to principal at a scope.
    Indexed on (principal_id, scope) for the authorization hot-path
    (used on every authorize() call). Scope is a JSONB discriminated union:
    {type: "workspace", workspace_id: "..."},
    {type: "tenant", tenant_id: "..."},
    {type: "global"}
    Adapter accepts both "type" and "kind" keys for backward compatibility.
"""

from __future__ import annotations

from custos_pg.migrations import Revision

AUTH_REV1 = Revision(
    number=1,
    statements=(
        "CREATE SCHEMA IF NOT EXISTS auth",
        # ----- Tenancy -----
        """
        CREATE TABLE IF NOT EXISTS auth.tenant (
            tenant_id    TEXT        NOT NULL,
            display_name TEXT        NOT NULL,
            disabled_at  TIMESTAMPTZ,
            created_at   TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (tenant_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS auth.workspace (
            workspace_id TEXT        NOT NULL,
            tenant_id    TEXT        NOT NULL,
            display_name TEXT        NOT NULL,
            disabled_at  TIMESTAMPTZ,
            created_at   TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (workspace_id),
            FOREIGN KEY (tenant_id)
                REFERENCES auth.tenant (tenant_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS workspace_by_tenant
            ON auth.workspace (tenant_id)
        """,
        # ----- Principals -----
        """
        CREATE TABLE IF NOT EXISTS auth.principal (
            principal_id    TEXT        NOT NULL,
            kind            TEXT        NOT NULL,
            tenant_id       TEXT,
            workspace_id    TEXT,
            display_name    TEXT        NOT NULL,
            email           TEXT,
            disabled_at     TIMESTAMPTZ,
            disabled_reason TEXT,
            created_at      TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (principal_id),
            FOREIGN KEY (tenant_id)
                REFERENCES auth.tenant (tenant_id),
            FOREIGN KEY (workspace_id)
                REFERENCES auth.workspace (workspace_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS principal_by_tenant_kind
            ON auth.principal (tenant_id, kind)
        """,
        """
        CREATE INDEX IF NOT EXISTS principal_by_workspace_kind
            ON auth.principal (workspace_id, kind)
        """,
        # ----- OIDC identities -----
        """
        CREATE TABLE IF NOT EXISTS auth.oidc_identity (
            issuer   TEXT        NOT NULL,
            subject  TEXT        NOT NULL,
            user_id  TEXT        NOT NULL,
            bound_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (issuer, subject),
            FOREIGN KEY (user_id)
                REFERENCES auth.principal (principal_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS oidc_identity_by_user
            ON auth.oidc_identity (user_id)
        """,
        # ----- Service tokens -----
        """
        CREATE TABLE IF NOT EXISTS auth.service_token (
            token_id           TEXT        NOT NULL,
            service_account_id TEXT        NOT NULL,
            hash               TEXT        NOT NULL,
            issued_at          TIMESTAMPTZ NOT NULL,
            expires_at         TIMESTAMPTZ NOT NULL,
            revoked_at         TIMESTAMPTZ,
            revoked_by         TEXT,
            revoked_reason     TEXT,
            PRIMARY KEY (token_id),
            FOREIGN KEY (service_account_id)
                REFERENCES auth.principal (principal_id),
            FOREIGN KEY (revoked_by)
                REFERENCES auth.principal (principal_id)
        )
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS service_token_by_hash
            ON auth.service_token (hash)
        """,
        """
        CREATE INDEX IF NOT EXISTS service_token_by_service_account
            ON auth.service_token (service_account_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS service_token_by_expires_at
            ON auth.service_token (expires_at)
        """,
        # ----- Permissions & Roles -----
        """
        CREATE TABLE IF NOT EXISTS auth.permission (
            name        TEXT NOT NULL,
            description TEXT NOT NULL,
            PRIMARY KEY (name)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS auth.role (
            role_id      TEXT   NOT NULL,
            name         TEXT   NOT NULL,
            description  TEXT   NOT NULL,
            permission_names TEXT[] NOT NULL,
            PRIMARY KEY (role_id)
        )
        """,
        # ----- Role bindings -----
        """
        CREATE TABLE IF NOT EXISTS auth.role_binding (
            binding_id   TEXT        NOT NULL,
            principal_id TEXT        NOT NULL,
            role_id      TEXT        NOT NULL,
            scope        JSONB       NOT NULL,
            bound_at     TIMESTAMPTZ NOT NULL,
            bound_by     TEXT        NOT NULL,
            PRIMARY KEY (binding_id),
            FOREIGN KEY (principal_id)
                REFERENCES auth.principal (principal_id),
            FOREIGN KEY (role_id)
                REFERENCES auth.role (role_id),
            FOREIGN KEY (bound_by)
                REFERENCES auth.principal (principal_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS role_binding_by_principal_scope
            ON auth.role_binding (principal_id, scope)
        """,
        """
        CREATE INDEX IF NOT EXISTS role_binding_by_role
            ON auth.role_binding (role_id)
        """,
    ),
)


__all__ = ["AUTH_REV1"]

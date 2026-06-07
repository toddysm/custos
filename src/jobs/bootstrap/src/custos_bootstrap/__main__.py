"""``custos-bootstrap`` — idempotent post-install platform seeder.

Seeds a freshly migrated Custos database into a usable platform by reusing the
canonical auth domain (the auth-service permission registry + built-in role
table) so permissions and roles have a single source of truth.

Database connection
-------------------
The SPL Postgres adapters read the libpq DSN from ``CUSTOS_PG_DSN``. When run as
the Helm hook the Job inherits the CloudNativePG application secret via
``envFrom`` (keys ``host`` / ``port`` / ``dbname`` / ``username`` / ``password``
/ ``uri``); this module resolves ``CUSTOS_PG_DSN`` from the first available of
``CUSTOS_PG_DSN`` → ``DATABASE_URL`` → ``uri`` and exports it before building
the adapter.

Admin binding
-------------
The admin principal id is read from ``CUSTOS_ADMIN_PRINCIPAL_ID``. The Helm hook
injects it from an optional secret, so when it is absent the seeder still seeds
permissions/roles/tenant/workspace and exits ``0`` (the platform is usable; an
operator can bind an admin later).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import cast
from uuid import uuid4

from custos_auth.permission_registry import seed_permissions_and_validate_roles
from custos_auth.roles import BUILTIN_ROLES, ROLE_PLATFORM_ADMIN, seed_builtin_roles
from custos_spl import AuthStoreProvider
from custos_spl.ids import (
    PrincipalId,
    RoleBindingId,
    TenantId,
    WorkspaceId,
)
from custos_spl.interfaces.auth_store import (
    GlobalScope,
    RoleBinding,
    Tenant,
    User,
    Workspace,
)

_LOGGER = logging.getLogger("custos_bootstrap")

#: Env var the SPL Postgres adapters read for their libpq DSN.
DSN_ENV_VAR = "CUSTOS_PG_DSN"

#: Fallback env vars, in priority order, carrying a ready-made connection URL.
_DSN_FALLBACK_VARS = ("DATABASE_URL", "uri")

#: Env var carrying the principal id to grant platform-admin to.
ADMIN_PRINCIPAL_ENV = "CUSTOS_ADMIN_PRINCIPAL_ID"

#: Canonical default tenant / workspace seeded for a usable single-tenant
#: install. They are plain string ids (not UUIDs) so they are stable and
#: human-recognisable across installs.
DEFAULT_TENANT_ID = TenantId("tenant-default")
DEFAULT_WORKSPACE_ID = WorkspaceId("workspace-default")

#: Actor recorded as ``bound_by`` on the admin binding the seeder creates.
_BOOTSTRAP_ACTOR = PrincipalId("custos-bootstrap")


def resolve_dsn(env: Mapping[str, str]) -> str | None:
    """Return the libpq DSN to use, or ``None`` if it cannot be resolved.

    An explicit ``CUSTOS_PG_DSN`` always wins; otherwise the first non-empty
    fallback connection URL (``DATABASE_URL`` then ``uri``) is used.
    """
    explicit = env.get(DSN_ENV_VAR)
    if explicit:
        return explicit
    for name in _DSN_FALLBACK_VARS:
        value = env.get(name)
        if value:
            return value
    return None


async def _ensure_tenant(auth_store: AuthStoreProvider, now: datetime) -> None:
    if await auth_store.get_tenant(DEFAULT_TENANT_ID) is not None:
        return
    await auth_store.put_tenant(
        Tenant(
            tenant_id=DEFAULT_TENANT_ID,
            display_name="Default Tenant",
            disabled_at=None,
            created_at=now,
        )
    )


async def _ensure_workspace(auth_store: AuthStoreProvider, now: datetime) -> None:
    if await auth_store.get_workspace(DEFAULT_WORKSPACE_ID) is not None:
        return
    await auth_store.put_workspace(
        Workspace(
            workspace_id=DEFAULT_WORKSPACE_ID,
            tenant_id=DEFAULT_TENANT_ID,
            display_name="Default Workspace",
            disabled_at=None,
            created_at=now,
        )
    )


async def _ensure_admin(
    auth_store: AuthStoreProvider, admin_principal_id: str, now: datetime
) -> None:
    principal_id = PrincipalId(admin_principal_id)

    if await auth_store.get_principal(principal_id) is None:
        await auth_store.put_principal(
            User(
                kind="user",
                principal_id=principal_id,
                tenant_id=DEFAULT_TENANT_ID,
                display_name="Bootstrap Admin",
                email=None,
                disabled_at=None,
                disabled_reason=None,
                created_at=now,
            )
        )

    # Grant platform-admin (global, implicit superuser) once. Guard on the
    # existing global bindings so a re-run — or a binding an operator added
    # manually — does not create a duplicate grant.
    existing = await auth_store.list_role_bindings_for_principal(principal_id, (GlobalScope(),))
    if any(binding.role_id == ROLE_PLATFORM_ADMIN for binding in existing):
        return
    await auth_store.put_role_binding(
        RoleBinding(
            binding_id=RoleBindingId(str(uuid4())),
            principal_id=principal_id,
            role_id=ROLE_PLATFORM_ADMIN,
            scope=GlobalScope(),
            bound_at=now,
            bound_by=_BOOTSTRAP_ACTOR,
        )
    )


async def seed_platform(
    auth_store: AuthStoreProvider,
    *,
    admin_principal_id: str | None,
    now: datetime,
) -> None:
    """Seed permissions, roles, the default tenant/workspace, and the admin.

    Idempotent: permissions/roles are upserted by name/id, the tenant and
    workspace are created only when absent, and the admin binding is created
    only when the principal has no platform-admin grant yet.
    """
    roles_spl = [role.to_spl() for role in BUILTIN_ROLES]
    await seed_permissions_and_validate_roles(auth_store, paths=[], roles=roles_spl)
    await seed_builtin_roles(auth_store)

    await _ensure_tenant(auth_store, now)
    await _ensure_workspace(auth_store, now)

    if admin_principal_id:
        await _ensure_admin(auth_store, admin_principal_id, now)
        _LOGGER.info("bootstrap seeded admin binding principal=%s", admin_principal_id)
    else:
        _LOGGER.warning(
            "%s not set; skipping admin principal + platform-admin binding "
            "(permissions, roles, default tenant/workspace still seeded)",
            ADMIN_PRINCIPAL_ENV,
        )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="custos-bootstrap",
        description=(
            "Custos bootstrap Job — idempotently seeds permissions, roles, the "
            "default tenant/workspace, and the admin binding (post-install hook)."
        ),
    )
    parser.add_argument(
        "--admin-principal-id",
        default=None,
        help=(
            "principal id to grant platform-admin; overrides the "
            f"{ADMIN_PRINCIPAL_ENV} environment variable."
        ),
    )
    return parser


async def _run(args: argparse.Namespace) -> int:
    dsn = resolve_dsn(os.environ)
    if dsn is None:
        print(
            f"error: no Postgres DSN available; set {DSN_ENV_VAR} (or provide a "
            "DATABASE_URL/uri) to a libpq connection string such as "
            "'postgresql://user:pw@host:5432/custos'.",
            file=sys.stderr,
        )
        return 1
    os.environ[DSN_ENV_VAR] = dsn

    from custos_pg.adapters.auth import make_adapter

    admin_principal_id = args.admin_principal_id or os.environ.get(ADMIN_PRINCIPAL_ENV)
    admin_principal_id = admin_principal_id.strip() if admin_principal_id else None

    # `make_adapter()` returns a concrete `PgAuthAdapter`; it satisfies the
    # `AuthStoreProvider` protocol at runtime (the only static mismatch is a
    # ``SCHEMA_REVISION`` ClassVar-vs-instance-var technicality), so cast.
    auth_store = cast(AuthStoreProvider, make_adapter())
    await seed_platform(
        auth_store,
        admin_principal_id=admin_principal_id or None,
        now=datetime.now(UTC),
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Resolve the DSN and seed the platform; return the process exit code."""
    logging.basicConfig(level=logging.INFO)
    args = _build_parser().parse_args(argv)
    return asyncio.run(_run(args))


if __name__ == "__main__":  # pragma: no cover - exercised via the console script
    sys.exit(main())

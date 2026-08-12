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
The initial platform admin is pre-bound by their OIDC identity so they can
authenticate immediately after install. The operator supplies the issuer and
subject via ``CUSTOS_BOOTSTRAP_ADMIN_OIDC_ISSUER`` /
``CUSTOS_BOOTSTRAP_ADMIN_OIDC_SUBJECT`` (the Helm hook injects them from an
optional secret). The seeder provisions an internal user, links the
``(issuer, subject)`` OIDC identity to it, and grants it platform-admin. When
either value is absent the seeder still seeds permissions/roles/tenant/workspace
and exits ``0`` (the platform is usable; an operator can bind an admin later).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import cast
from uuid import NAMESPACE_URL, uuid4, uuid5

from custos_auth.oidc_identity import (
    OidcIdentityAlreadyBound,
    find_user_by_oidc,
    link_oidc_identity,
)
from custos_auth.permission_registry import seed_permissions_and_validate_roles
from custos_auth.roles import BUILTIN_ROLES, ROLE_PLATFORM_ADMIN, seed_builtin_roles
from custos_auth.settings import ENV_PERMISSIONS_PATHS
from custos_auth.tokens import hash_token, looks_like_custos_token
from custos_spl import AuthStoreProvider
from custos_spl.ids import (
    PrincipalId,
    RoleBindingId,
    ServiceTokenId,
    TenantId,
    WorkspaceId,
)
from custos_spl.interfaces.auth_store import (
    GlobalScope,
    RoleBinding,
    ServiceAccount,
    ServiceToken,
    Tenant,
    User,
    Workspace,
)

_LOGGER = logging.getLogger("custos_bootstrap")

#: Env var the SPL Postgres adapters read for their libpq DSN.
DSN_ENV_VAR = "CUSTOS_PG_DSN"

#: Fallback env vars, in priority order, carrying a ready-made connection URL.
_DSN_FALLBACK_VARS = ("DATABASE_URL", "uri")

#: Env vars carrying the initial admin's OIDC issuer + subject to pre-bind.
ADMIN_OIDC_ISSUER_ENV = "CUSTOS_BOOTSTRAP_ADMIN_OIDC_ISSUER"
ADMIN_OIDC_SUBJECT_ENV = "CUSTOS_BOOTSTRAP_ADMIN_OIDC_SUBJECT"

ADMIN_TOKEN_MODE_ENV = "CUSTOS_BOOTSTRAP_ADMIN_TOKEN_MODE"
ADMIN_TOKEN_ENV = "CUSTOS_BOOTSTRAP_ADMIN_TOKEN"
ADMIN_TOKEN_PRINCIPAL_ID_ENV = "CUSTOS_BOOTSTRAP_ADMIN_PRINCIPAL_ID"
ADMIN_TOKEN_WORKSPACE_ID_ENV = "CUSTOS_BOOTSTRAP_ADMIN_WORKSPACE_ID"
ADMIN_TOKEN_TTL_SECONDS_ENV = "CUSTOS_BOOTSTRAP_ADMIN_TOKEN_TTL_SECONDS"

DEFAULT_ADMIN_TOKEN_PRINCIPAL_ID = PrincipalId("custos-bootstrap-admin")
DEFAULT_ADMIN_TOKEN_TTL_SECONDS = 90 * 24 * 60 * 60
MAX_ADMIN_TOKEN_TTL_SECONDS = 10 * 365 * 24 * 60 * 60

#: Canonical default tenant / workspace seeded for a usable single-tenant
#: install. They are plain string ids (not UUIDs) so they are stable and
#: human-recognisable across installs.
DEFAULT_TENANT_ID = TenantId("tenant-default")
DEFAULT_WORKSPACE_ID = WorkspaceId("workspace-default")

#: Actor recorded as ``bound_by`` on the admin binding the seeder creates.
_BOOTSTRAP_ACTOR = PrincipalId("custos-bootstrap")


class BootstrapAdminMode(StrEnum):
    """Allowed first-admin credential state transitions."""

    DISABLED = "disabled"
    INIT = "init"
    RECOVER = "recover"


@dataclass(frozen=True, slots=True)
class BootstrapAdminTokenConfig:
    """Validated non-secret settings plus the Secret-projected token."""

    mode: BootstrapAdminMode
    token: str | None
    principal_id: PrincipalId = DEFAULT_ADMIN_TOKEN_PRINCIPAL_ID
    workspace_id: WorkspaceId = DEFAULT_WORKSPACE_ID
    ttl_seconds: int = DEFAULT_ADMIN_TOKEN_TTL_SECONDS


def resolve_bootstrap_admin_token_config(env: Mapping[str, str]) -> BootstrapAdminTokenConfig:
    """Parse and validate the bootstrap-admin token environment contract."""
    raw_mode = env.get(ADMIN_TOKEN_MODE_ENV, BootstrapAdminMode.DISABLED.value).strip().lower()
    try:
        mode = BootstrapAdminMode(raw_mode)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in BootstrapAdminMode)
        raise ValueError(f"{ADMIN_TOKEN_MODE_ENV} must be one of: {allowed}") from exc

    token = env.get(ADMIN_TOKEN_ENV)
    token = token.strip() if token else None
    principal_raw = env.get(
        ADMIN_TOKEN_PRINCIPAL_ID_ENV, str(DEFAULT_ADMIN_TOKEN_PRINCIPAL_ID)
    ).strip()
    workspace_raw = env.get(ADMIN_TOKEN_WORKSPACE_ID_ENV, str(DEFAULT_WORKSPACE_ID)).strip()
    if not principal_raw:
        raise ValueError(f"{ADMIN_TOKEN_PRINCIPAL_ID_ENV} must not be empty")
    if not workspace_raw:
        raise ValueError(f"{ADMIN_TOKEN_WORKSPACE_ID_ENV} must not be empty")

    raw_ttl = env.get(ADMIN_TOKEN_TTL_SECONDS_ENV, str(DEFAULT_ADMIN_TOKEN_TTL_SECONDS)).strip()
    try:
        ttl_seconds = int(raw_ttl)
    except ValueError as exc:
        raise ValueError(f"{ADMIN_TOKEN_TTL_SECONDS_ENV} must be an integer") from exc
    if not 1 <= ttl_seconds <= MAX_ADMIN_TOKEN_TTL_SECONDS:
        raise ValueError(
            f"{ADMIN_TOKEN_TTL_SECONDS_ENV} must be between 1 and {MAX_ADMIN_TOKEN_TTL_SECONDS}"
        )

    if mode is BootstrapAdminMode.DISABLED:
        if token is not None:
            raise ValueError(
                f"{ADMIN_TOKEN_ENV} must be absent when {ADMIN_TOKEN_MODE_ENV}=disabled"
            )
    elif token is None:
        raise ValueError(f"{ADMIN_TOKEN_ENV} is required when {ADMIN_TOKEN_MODE_ENV}={mode.value}")
    elif not looks_like_custos_token(token):
        raise ValueError(f"{ADMIN_TOKEN_ENV} is not a canonical Custos service token")

    return BootstrapAdminTokenConfig(
        mode=mode,
        token=token,
        principal_id=PrincipalId(principal_raw),
        workspace_id=WorkspaceId(workspace_raw),
        ttl_seconds=ttl_seconds,
    )


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


def resolve_permission_paths(env: Mapping[str, str]) -> list[str]:
    """Return the per-service ``permissions.yaml`` paths to seed from.

    Reads ``CUSTOS_AUTH_PERMISSIONS_PATHS`` — the same colon-separated env var
    the auth-service runtime consumes — and splits it the same way as
    ``custos_auth.settings._parse_paths`` (trim entries, drop empties). An empty
    or unset value yields an empty list, which makes
    :func:`seed_permissions_and_validate_roles` fall back to the bundled
    platform-M1 aggregate embedded in the ``custos_auth`` wheel (the dev/test
    path). When set, the listed files are the canonical per-service
    declarations baked into the bootstrap image, so the seeder and the
    auth-service pod aggregate the identical registry surface.
    """
    raw = env.get(ENV_PERMISSIONS_PATHS, "").strip()
    return [part.strip() for part in raw.split(":") if part.strip()]


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


def _admin_principal_id(issuer: str, subject: str) -> PrincipalId:
    """Return a stable internal principal id derived from the OIDC identity.

    Deterministic (``uuid5`` over ``issuer|subject``) so repeated runs converge
    on the same user without a lookup, and distinct admins never collide.
    """
    return PrincipalId(f"user-{uuid5(NAMESPACE_URL, f'{issuer}|{subject}')}")


async def _ensure_admin(
    auth_store: AuthStoreProvider, *, issuer: str, subject: str, now: datetime
) -> None:
    # Reuse the principal already bound to this OIDC identity when present so a
    # re-run never provisions a second user for the same admin.
    existing_user = await find_user_by_oidc(auth_store, issuer=issuer, subject=subject)
    if existing_user is not None:
        principal_id = PrincipalId(existing_user)
    else:
        principal_id = _admin_principal_id(issuer, subject)
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
        # Pre-bind the OIDC identity so the admin can authenticate immediately.
        # ``link_oidc_identity`` is write-once; treat an existing binding (from a
        # prior partial run) as success. Audit is skipped (no metadata store).
        with contextlib.suppress(OidcIdentityAlreadyBound):
            await link_oidc_identity(
                auth_store,
                None,
                user_id=principal_id,
                issuer=issuer,
                subject=subject,
                actor=_BOOTSTRAP_ACTOR,
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


async def _ensure_platform_admin_binding(
    auth_store: AuthStoreProvider, principal_id: PrincipalId, now: datetime
) -> None:
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


async def _apply_bootstrap_admin_token(
    auth_store: AuthStoreProvider,
    config: BootstrapAdminTokenConfig,
    now: datetime,
) -> None:
    if config.mode is BootstrapAdminMode.DISABLED:
        return
    assert config.token is not None

    workspace = await auth_store.get_workspace(config.workspace_id)
    if workspace is None:
        raise ValueError(f"bootstrap admin workspace '{config.workspace_id}' does not exist")

    principal = await auth_store.get_principal(config.principal_id)
    if config.mode is BootstrapAdminMode.INIT:
        if principal is not None:
            raise ValueError(
                f"bootstrap admin principal '{config.principal_id}' already exists; "
                "use recover to replace its credential"
            )
        await auth_store.put_principal(
            ServiceAccount(
                kind="serviceAccount",
                principal_id=config.principal_id,
                workspace_id=config.workspace_id,
                display_name="Custos Bootstrap Administrator",
                disabled_at=None,
                disabled_reason=None,
                created_at=now,
            )
        )
    else:
        if not isinstance(principal, ServiceAccount):
            raise ValueError(
                f"bootstrap admin principal '{config.principal_id}' is not a service account"
            )
        if principal.workspace_id != config.workspace_id:
            raise ValueError(
                f"bootstrap admin principal '{config.principal_id}' belongs to workspace "
                f"'{principal.workspace_id}', not '{config.workspace_id}'"
            )
        for existing_token in await auth_store.list_service_tokens_for_service_account(
            config.principal_id
        ):
            if existing_token.revoked_at is None:
                await auth_store.revoke_service_token(
                    existing_token.token_id,
                    _BOOTSTRAP_ACTOR,
                    "bootstrap admin credential recovery",
                )

    await _ensure_platform_admin_binding(auth_store, config.principal_id, now)
    await auth_store.put_service_token(
        ServiceToken(
            token_id=ServiceTokenId(str(uuid4())),
            service_account_id=config.principal_id,
            hash=hash_token(config.token),
            issued_at=now,
            expires_at=now + timedelta(seconds=config.ttl_seconds),
            revoked_at=None,
            revoked_by=None,
            revoked_reason=None,
        )
    )
    _LOGGER.info("bootstrap.admin-token.%s principal=%s", config.mode.value, config.principal_id)


async def seed_platform(
    auth_store: AuthStoreProvider,
    *,
    admin_oidc_issuer: str | None,
    admin_oidc_subject: str | None,
    now: datetime,
    permission_paths: Sequence[str] = (),
    bootstrap_admin_token: BootstrapAdminTokenConfig | None = None,
) -> None:
    """Seed permissions, roles, the default tenant/workspace, and the admin.

    Idempotent: permissions/roles are upserted by name/id, the tenant and
    workspace are created only when absent, and the admin user / OIDC identity /
    platform-admin grant are created only when missing.

    ``permission_paths`` carries the per-service ``permissions.yaml`` files to
    aggregate (from :func:`resolve_permission_paths`). An empty sequence falls
    back to the bundled platform-M1 aggregate, keeping dev/test runs working
    without any registry files on disk.
    """
    roles_spl = [role.to_spl() for role in BUILTIN_ROLES]
    await seed_permissions_and_validate_roles(auth_store, paths=permission_paths, roles=roles_spl)
    await seed_builtin_roles(auth_store)

    await _ensure_tenant(auth_store, now)
    await _ensure_workspace(auth_store, now)

    if bootstrap_admin_token is not None:
        await _apply_bootstrap_admin_token(auth_store, bootstrap_admin_token, now)

    if admin_oidc_issuer and admin_oidc_subject:
        await _ensure_admin(
            auth_store, issuer=admin_oidc_issuer, subject=admin_oidc_subject, now=now
        )
        _LOGGER.info(
            "bootstrap linked admin OIDC identity issuer=%s subject=%s",
            admin_oidc_issuer,
            admin_oidc_subject,
        )
    else:
        _LOGGER.warning(
            "%s / %s not both set; skipping admin user + OIDC link + "
            "platform-admin binding (permissions, roles, default "
            "tenant/workspace still seeded)",
            ADMIN_OIDC_ISSUER_ENV,
            ADMIN_OIDC_SUBJECT_ENV,
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
        "--admin-oidc-issuer",
        default=None,
        help=(
            "OIDC issuer URL of the initial admin to pre-bind; overrides the "
            f"{ADMIN_OIDC_ISSUER_ENV} environment variable."
        ),
    )
    parser.add_argument(
        "--admin-oidc-subject",
        default=None,
        help=(
            "OIDC subject (``sub`` claim) of the initial admin to pre-bind; "
            f"overrides the {ADMIN_OIDC_SUBJECT_ENV} environment variable."
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

    issuer = args.admin_oidc_issuer or os.environ.get(ADMIN_OIDC_ISSUER_ENV)
    issuer = issuer.strip() if issuer else None
    subject = args.admin_oidc_subject or os.environ.get(ADMIN_OIDC_SUBJECT_ENV)
    subject = subject.strip() if subject else None

    # `make_adapter()` returns a concrete `PgAuthAdapter`; it satisfies the
    # `AuthStoreProvider` protocol at runtime (the only static mismatch is a
    # ``SCHEMA_REVISION`` ClassVar-vs-instance-var technicality), so cast.
    auth_store = cast(AuthStoreProvider, make_adapter())
    try:
        await seed_platform(
            auth_store,
            admin_oidc_issuer=issuer or None,
            admin_oidc_subject=subject or None,
            now=datetime.now(UTC),
            permission_paths=resolve_permission_paths(os.environ),
            bootstrap_admin_token=resolve_bootstrap_admin_token_config(os.environ),
        )
    except ValueError as exc:
        _LOGGER.error("bootstrap.admin-token.rejected: %s", exc)
        return 2
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Resolve the DSN and seed the platform; return the process exit code."""
    logging.basicConfig(level=logging.INFO)
    args = _build_parser().parse_args(argv)
    return asyncio.run(_run(args))


if __name__ == "__main__":  # pragma: no cover - exercised via the console script
    sys.exit(main())

"""Custos bootstrap Job (``custos-bootstrap``).

The idempotent ``post-install`` / ``post-upgrade`` Helm hook that seeds a freshly
migrated Custos database into a *usable* platform. It runs after the migration
hook (DEPLOY-IMPL-004) and before the platform is handed to operators, reusing
the canonical auth domain so there is a single source of truth for permissions
and roles:

* upserts every component's declared permission (the bundled auth-service
  registry),
* seeds the canonical built-in roles,
* creates the default tenant (``tenant-default``) and workspace
  (``workspace-default``),
* provisions the initial admin, links their OIDC identity, and grants them the
  platform-admin role.

Every step is an upsert keyed on the entity's identity, so re-running the hook
(e.g. on ``helm upgrade``) converges without duplicating rows.

Design: ``design/architecture/reference-deployment.md`` § Bootstrap job.
"""

from __future__ import annotations

from custos_bootstrap.__main__ import (
    ADMIN_OIDC_ISSUER_ENV,
    ADMIN_OIDC_SUBJECT_ENV,
    ADMIN_TOKEN_ENV,
    ADMIN_TOKEN_MODE_ENV,
    DEFAULT_TENANT_ID,
    DEFAULT_WORKSPACE_ID,
    DSN_ENV_VAR,
    BootstrapAdminMode,
    BootstrapAdminTokenConfig,
    main,
    resolve_bootstrap_admin_token_config,
    resolve_dsn,
    resolve_permission_paths,
    seed_platform,
)

__all__ = [
    "ADMIN_OIDC_ISSUER_ENV",
    "ADMIN_OIDC_SUBJECT_ENV",
    "ADMIN_TOKEN_ENV",
    "ADMIN_TOKEN_MODE_ENV",
    "DEFAULT_TENANT_ID",
    "DEFAULT_WORKSPACE_ID",
    "DSN_ENV_VAR",
    "BootstrapAdminMode",
    "BootstrapAdminTokenConfig",
    "main",
    "resolve_bootstrap_admin_token_config",
    "resolve_dsn",
    "resolve_permission_paths",
    "seed_platform",
]
__version__ = "0.1.0"

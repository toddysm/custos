"""Custos bootstrap Job (``custos-bootstrap``).

The idempotent ``post-install`` Helm hook that seeds a freshly migrated Custos
database into a *usable* platform. It runs after the migration hook
(DEPLOY-IMPL-004) and before the platform is handed to operators, reusing the
canonical auth domain so there is a single source of truth for permissions and
roles:

* upserts every component's declared permission (the bundled auth-service
  registry),
* seeds the canonical built-in roles,
* creates the default tenant (``tenant-default``) and workspace
  (``workspace-default``),
* binds the configured admin principal to the platform-admin role.

Every step is an upsert keyed on the entity's identity, so re-running the hook
(e.g. on ``helm upgrade``) converges without duplicating rows.

Design: ``design/architecture/reference-deployment.md`` § Bootstrap job.
"""

from __future__ import annotations

from custos_bootstrap.__main__ import (
    ADMIN_PRINCIPAL_ENV,
    DEFAULT_TENANT_ID,
    DEFAULT_WORKSPACE_ID,
    DSN_ENV_VAR,
    main,
    resolve_dsn,
    seed_platform,
)

__all__ = [
    "ADMIN_PRINCIPAL_ENV",
    "DEFAULT_TENANT_ID",
    "DEFAULT_WORKSPACE_ID",
    "DSN_ENV_VAR",
    "main",
    "resolve_dsn",
    "seed_platform",
]
__version__ = "0.1.0"

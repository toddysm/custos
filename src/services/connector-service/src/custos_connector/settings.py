"""Runtime configuration parsed from environment variables (CONN-IMPL-003).

Connector Service is configured exclusively through the ``CONN_*`` env vars
documented in ``design/components/connector-service/design.md`` and projected
by the Helm subchart at ``deploy/helm/charts/connector-service/templates/``.

This module is deliberately stdlib-only so it can be imported by both the
ASGI app factory and lightweight test fixtures without dragging in FastAPI
or asyncpg.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final

#: Required. DSN that resolves the ``CatalogStoreProvider`` adapter.
#: Connector Service writes connector-type versions and reads them back via
#: this provider (the same Postgres database catalog-service reads from).
ENV_CATALOG_STORE: Final[str] = "CONN_CATALOG_STORE"

#: Required. DSN that resolves the ``MetadataStoreProvider`` adapter.
#: Backs ``ConnectorInstance`` rows, ``ConnectorCursor`` rows, the audit
#: outbox, and the lease primitive for the pull-loop.
ENV_METADATA_STORE: Final[str] = "CONN_METADATA_STORE"

#: Required. URL of the in-cluster Catalog Service (used by Phase J routes
#: that resolve activity-type references during bind).
ENV_CATALOG_ENDPOINT: Final[str] = "CONN_CATALOG_ENDPOINT"

#: Required in production. Empty switches the service to the dev-shim
#: call-context middleware (CONN-IMPL-004), which refuses to start when
#: ``ENVIRONMENT=production``.
ENV_AUTHZ_ENDPOINT: Final[str] = "CONN_AUTHZ_ENDPOINT"

#: Optional. OCI referrers discovery timeout in milliseconds (reached by
#: the OCI sample plugin in CONN-IMPL-032).
ENV_OCI_REFERRERS_TIMEOUT_MS: Final[str] = "CONN_OCI_REFERRERS_TIMEOUT_MS"

#: Optional. Maximum publish body size in MiB (mirrors catalog-service for
#: parity; reached by Phase J publish-style routes).
ENV_PUBLISH_MAX_BODY_MB: Final[str] = "CONN_PUBLISH_MAX_BODY_MB"

#: Optional. Default lease TTL in seconds requested from the Lease Manager
#: when a plugin does not specify a ``leaseHint`` (CONN-IMPL-017).
ENV_SIDECAR_DEFAULT_TTL: Final[str] = "CONN_SIDECAR_DEFAULT_TTL"

#: Optional. Per-instance concurrent-lease cap (CONN-IMPL-017 default = 16).
ENV_LEASE_MAX_CONCURRENT: Final[str] = "CONN_LEASE_MAX_CONCURRENT"

#: Optional. Minimum pull-loop tick interval in seconds (CONN-IMPL-023; the
#: design pins ``>= 10`` so we refuse to honour smaller values).
ENV_PULL_LOOP_MIN_INTERVAL_SEC: Final[str] = "CONN_PULL_LOOP_MIN_INTERVAL_SEC"

#: Optional. PKI issuer for the secret-bridge sidecar mTLS material
#: (CONN-IMPL-020).
ENV_SIDECAR_MTLS_ISSUER: Final[str] = "CONN_SIDECAR_MTLS_ISSUER"

#: Operational env tag. The call-context dev shim refuses to run when this
#: is ``production`` (case-insensitive).
ENV_ENVIRONMENT: Final[str] = "ENVIRONMENT"

DEFAULT_OCI_REFERRERS_TIMEOUT_MS: Final[int] = 5000
DEFAULT_PUBLISH_MAX_BODY_MB: Final[int] = 4
DEFAULT_SIDECAR_DEFAULT_TTL: Final[int] = 600
DEFAULT_LEASE_MAX_CONCURRENT: Final[int] = 16
DEFAULT_PULL_LOOP_MIN_INTERVAL_SEC: Final[int] = 10

#: Minimum pull-loop interval the design allows (CONN-IMPL-023). Set
#: ``CONN_PULL_LOOP_MIN_INTERVAL_SEC`` above this; smaller values are rejected.
PULL_LOOP_HARD_FLOOR_SEC: Final[int] = 10


class SettingsError(RuntimeError):
    """Raised when the environment is missing a required setting or carries a malformed value."""


@dataclass(frozen=True, slots=True)
class Settings:
    """Parsed and validated connector-service configuration."""

    catalog_store_dsn: str
    metadata_store_dsn: str
    catalog_endpoint: str
    authz_endpoint: str  # empty string means "dev shim active"
    oci_referrers_timeout_ms: int
    publish_max_body_mb: int
    sidecar_default_ttl_sec: int
    lease_max_concurrent: int
    pull_loop_min_interval_sec: int
    sidecar_mtls_issuer: str | None
    environment: str

    @property
    def use_callctx_dev_shim(self) -> bool:
        """True when the dev-shim call-context middleware should be wired."""
        return self.authz_endpoint == ""

    @property
    def is_production(self) -> bool:
        return self.environment.lower() == "production"


def _require(name: str, env: dict[str, str]) -> str:
    value = env.get(name, "")
    if not value:
        raise SettingsError(
            f"{name} is required and must be set to a non-empty value "
            f"(see design/components/connector-service/design.md § Configuration)"
        )
    return value


def _opt_int(name: str, env: dict[str, str], default: int, *, minimum: int | None = None) -> int:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise SettingsError(f"{name} must be an integer (got {raw!r})") from exc
    if minimum is not None and value < minimum:
        raise SettingsError(
            f"{name} must be >= {minimum} (got {value}); the design pins this floor"
        )
    return value


def load_settings(env: dict[str, str] | None = None) -> Settings:
    """Parse a :class:`Settings` from the supplied env mapping (default ``os.environ``).

    ``CONN_AUTHZ_ENDPOINT`` is required in production but accepted as empty
    here so local development and tests can opt into the dev-shim
    call-context middleware. The shim itself refuses to start when
    :meth:`Settings.is_production` is true; see CONN-IMPL-004.
    """
    src: dict[str, str] = dict(os.environ if env is None else env)
    mtls_issuer = src.get(ENV_SIDECAR_MTLS_ISSUER, "").strip() or None
    return Settings(
        catalog_store_dsn=_require(ENV_CATALOG_STORE, src),
        metadata_store_dsn=_require(ENV_METADATA_STORE, src),
        catalog_endpoint=_require(ENV_CATALOG_ENDPOINT, src),
        authz_endpoint=src.get(ENV_AUTHZ_ENDPOINT, "").strip(),
        oci_referrers_timeout_ms=_opt_int(
            ENV_OCI_REFERRERS_TIMEOUT_MS, src, DEFAULT_OCI_REFERRERS_TIMEOUT_MS
        ),
        publish_max_body_mb=_opt_int(ENV_PUBLISH_MAX_BODY_MB, src, DEFAULT_PUBLISH_MAX_BODY_MB),
        sidecar_default_ttl_sec=_opt_int(ENV_SIDECAR_DEFAULT_TTL, src, DEFAULT_SIDECAR_DEFAULT_TTL),
        lease_max_concurrent=_opt_int(ENV_LEASE_MAX_CONCURRENT, src, DEFAULT_LEASE_MAX_CONCURRENT),
        pull_loop_min_interval_sec=_opt_int(
            ENV_PULL_LOOP_MIN_INTERVAL_SEC,
            src,
            DEFAULT_PULL_LOOP_MIN_INTERVAL_SEC,
            minimum=PULL_LOOP_HARD_FLOOR_SEC,
        ),
        sidecar_mtls_issuer=mtls_issuer,
        environment=src.get(ENV_ENVIRONMENT, "development").strip() or "development",
    )


__all__ = [
    "DEFAULT_LEASE_MAX_CONCURRENT",
    "DEFAULT_OCI_REFERRERS_TIMEOUT_MS",
    "DEFAULT_PUBLISH_MAX_BODY_MB",
    "DEFAULT_PULL_LOOP_MIN_INTERVAL_SEC",
    "DEFAULT_SIDECAR_DEFAULT_TTL",
    "ENV_AUTHZ_ENDPOINT",
    "ENV_CATALOG_ENDPOINT",
    "ENV_CATALOG_STORE",
    "ENV_ENVIRONMENT",
    "ENV_LEASE_MAX_CONCURRENT",
    "ENV_METADATA_STORE",
    "ENV_OCI_REFERRERS_TIMEOUT_MS",
    "ENV_PUBLISH_MAX_BODY_MB",
    "ENV_PULL_LOOP_MIN_INTERVAL_SEC",
    "ENV_SIDECAR_DEFAULT_TTL",
    "ENV_SIDECAR_MTLS_ISSUER",
    "PULL_LOOP_HARD_FLOOR_SEC",
    "Settings",
    "SettingsError",
    "load_settings",
]

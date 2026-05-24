"""Runtime configuration parsed from environment variables.

Catalog Service is configured exclusively through the `CAT_*` env vars
documented in ``design/components/catalog-service/design.md`` § Configuration
and projected by the Helm subchart at
``deploy/helm/charts/catalog-service/templates/``.

This module is deliberately stdlib-only so it can be imported by both the
ASGI app factory and lightweight test fixtures without dragging in FastAPI
or asyncpg.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final

#: Required. DSN that resolves the ``DefinitionStoreProvider`` adapter.
#: V1 only supports Postgres adapters from ``custos-postgres``; the value
#: is therefore a libpq DSN such as
#: ``postgresql://user:pass@host:5432/custos_definition``.
ENV_DEFINITION_STORE: Final[str] = "CAT_DEFINITION_STORE"

#: Required. DSN that resolves the ``CatalogStoreProvider`` adapter.
ENV_CATALOG_STORE: Final[str] = "CAT_CATALOG_STORE"

#: Required. DSN that resolves the ``MetadataStoreProvider`` adapter.
#: catalog-service writes audit-trail events to the SPL outbox via this
#: adapter (CS-IMPL-019).
ENV_METADATA_STORE: Final[str] = "CAT_METADATA_STORE"

#: Required. URL of the in-cluster Connector Service.
ENV_CONNECTOR_ENDPOINT: Final[str] = "CAT_CONNECTOR_ENDPOINT"

#: Required in production. Empty switches the service to the dev-shim
#: call-context middleware (CS-IMPL-004), which refuses to start when
#: ``ENVIRONMENT=production``. See
#: ``design/components/catalog-service/design.md`` § Configuration.
ENV_AUTHZ_ENDPOINT: Final[str] = "CAT_AUTHZ_ENDPOINT"

#: Optional. Documented default 4 MiB; reached by CS-IMPL-017 publish path.
ENV_PUBLISH_MAX_BODY_MB: Final[str] = "CAT_PUBLISH_MAX_BODY_MB"

#: Optional. Documented default 500 ms; reached by CS-IMPL-007 CEL validator.
ENV_CEL_PARSE_TIMEOUT_MS: Final[str] = "CAT_CEL_PARSE_TIMEOUT_MS"

#: Optional namespace default; reached by CS-IMPL-005 schema validator.
ENV_DEFAULT_NAMESPACE_TIER_VENDOR: Final[str] = "CAT_DEFAULT_NAMESPACE_TIER_VENDOR"

#: Operational env tag. The call-context dev shim refuses to run when this
#: is ``production`` (case-insensitive).
ENV_ENVIRONMENT: Final[str] = "ENVIRONMENT"

DEFAULT_PUBLISH_MAX_BODY_MB: Final[int] = 4
DEFAULT_CEL_PARSE_TIMEOUT_MS: Final[int] = 500


class SettingsError(RuntimeError):
    """Raised when the environment is missing a required setting or carries a malformed value."""


@dataclass(frozen=True, slots=True)
class Settings:
    """Parsed and validated catalog-service configuration."""

    definition_store_dsn: str
    catalog_store_dsn: str
    metadata_store_dsn: str
    connector_endpoint: str
    authz_endpoint: str  # empty string means "dev shim active"
    publish_max_body_mb: int
    cel_parse_timeout_ms: int
    default_namespace_tier_vendor: str | None
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
            f"(see design/components/catalog-service/design.md § Configuration)"
        )
    return value


def _opt_int(name: str, env: dict[str, str], default: int) -> int:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise SettingsError(f"{name} must be an integer (got {raw!r})") from exc


def load_settings(env: dict[str, str] | None = None) -> Settings:
    """Parse a :class:`Settings` from the supplied env mapping (default ``os.environ``).

    ``CAT_AUTHZ_ENDPOINT`` is required in production but accepted as empty
    here so local development and tests can opt into the dev-shim
    call-context middleware. The shim itself refuses to start when
    :meth:`Settings.is_production` is true; see CS-IMPL-004.
    """
    src: dict[str, str] = dict(os.environ if env is None else env)
    namespace_tier = src.get(ENV_DEFAULT_NAMESPACE_TIER_VENDOR, "").strip()
    return Settings(
        definition_store_dsn=_require(ENV_DEFINITION_STORE, src),
        catalog_store_dsn=_require(ENV_CATALOG_STORE, src),
        metadata_store_dsn=_require(ENV_METADATA_STORE, src),
        connector_endpoint=_require(ENV_CONNECTOR_ENDPOINT, src),
        authz_endpoint=src.get(ENV_AUTHZ_ENDPOINT, "").strip(),
        publish_max_body_mb=_opt_int(ENV_PUBLISH_MAX_BODY_MB, src, DEFAULT_PUBLISH_MAX_BODY_MB),
        cel_parse_timeout_ms=_opt_int(ENV_CEL_PARSE_TIMEOUT_MS, src, DEFAULT_CEL_PARSE_TIMEOUT_MS),
        default_namespace_tier_vendor=namespace_tier or None,
        environment=src.get(ENV_ENVIRONMENT, "development").strip() or "development",
    )


__all__ = [
    "DEFAULT_CEL_PARSE_TIMEOUT_MS",
    "DEFAULT_PUBLISH_MAX_BODY_MB",
    "ENV_AUTHZ_ENDPOINT",
    "ENV_CATALOG_STORE",
    "ENV_CEL_PARSE_TIMEOUT_MS",
    "ENV_CONNECTOR_ENDPOINT",
    "ENV_DEFAULT_NAMESPACE_TIER_VENDOR",
    "ENV_DEFINITION_STORE",
    "ENV_ENVIRONMENT",
    "ENV_METADATA_STORE",
    "ENV_PUBLISH_MAX_BODY_MB",
    "Settings",
    "SettingsError",
    "load_settings",
]

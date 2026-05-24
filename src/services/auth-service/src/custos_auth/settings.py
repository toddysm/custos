"""Runtime configuration parsed from environment variables.

Auth Service is configured through the ``CUSTOS_AUTH_*`` env vars documented
in ``design/components/auth-service/design.md`` § Configuration and projected
by the Helm subchart at ``deploy/helm/charts/auth-service/templates/``.

This module is deliberately stdlib-only so it can be imported by both the
ASGI app factory and lightweight test fixtures without dragging in FastAPI
or asyncpg.

AS-IMPL-004 introduces the two SPL-DSN env vars
(``CUSTOS_AUTH_STORE_DSN`` / ``CUSTOS_AUTH_METADATA_STORE_DSN``) needed to
construct the Postgres adapters for ``AuthStoreProvider`` and
``MetadataStoreProvider`` (the audit-outbox writer). The remaining auth
config — OIDC issuers, call-context signing key, cache TTLs, etc. — lands
in subsequent AS-IMPL-* phases.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final

#: Required. DSN that resolves the ``AuthStoreProvider`` adapter.
#: V1 only supports Postgres adapters from ``custos-postgres``; the value
#: is therefore a libpq DSN such as
#: ``postgresql://user:pass@host:5432/custos_auth``.
ENV_AUTH_STORE_DSN: Final[str] = "CUSTOS_AUTH_STORE_DSN"

#: Required. DSN that resolves the ``MetadataStoreProvider`` adapter.
#: auth-service writes audit-trail events (``authz.decision``, ``token.*``,
#: ``principal.*``, ``role-binding.*``, ``oidc.identity-linked``, …) to the
#: SPL outbox via this adapter — see
#: ``design/components/auth-service/design.md`` § Audit events.
ENV_METADATA_STORE_DSN: Final[str] = "CUSTOS_AUTH_METADATA_STORE_DSN"

#: Operational env tag. Reserved for future use by middleware that needs
#: to differentiate development from production behavior (mirrors the
#: ``ENVIRONMENT`` convention catalog-service uses).
ENV_ENVIRONMENT: Final[str] = "ENVIRONMENT"


class SettingsError(RuntimeError):
    """Raised when the environment is missing a required setting or carries a malformed value."""


@dataclass(frozen=True, slots=True)
class Settings:
    """Parsed and validated auth-service configuration."""

    auth_store_dsn: str
    metadata_store_dsn: str
    environment: str

    @property
    def is_production(self) -> bool:
        return self.environment.lower() == "production"


def _require(name: str, env: dict[str, str]) -> str:
    value = env.get(name, "")
    if not value:
        raise SettingsError(
            f"{name} is required and must be set to a non-empty value "
            f"(see design/components/auth-service/design.md § Configuration)"
        )
    return value


def load_settings(env: dict[str, str] | None = None) -> Settings:
    """Parse a :class:`Settings` from the supplied env mapping (default ``os.environ``)."""
    src: dict[str, str] = dict(os.environ if env is None else env)
    return Settings(
        auth_store_dsn=_require(ENV_AUTH_STORE_DSN, src),
        metadata_store_dsn=_require(ENV_METADATA_STORE_DSN, src),
        environment=src.get(ENV_ENVIRONMENT, "development").strip() or "development",
    )


__all__ = [
    "ENV_AUTH_STORE_DSN",
    "ENV_ENVIRONMENT",
    "ENV_METADATA_STORE_DSN",
    "Settings",
    "SettingsError",
    "load_settings",
]

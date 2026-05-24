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
``MetadataStoreProvider`` (the audit-outbox writer). AS-IMPL-005/006/007
add the ``CUSTOS_AUTH_CALLCTX_VERIFIER_URL`` env var that gates the
call-context dev shim (empty = dev shim active; non-empty = production
verifier — wired in Phase G). Remaining auth config — OIDC issuers,
call-context signing key, cache TTLs, etc. — lands in subsequent
AS-IMPL-* phases.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final

from custos_auth.authz_cache import DEFAULT_AUTHZ_CACHE_TTL_SECONDS

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

#: Operational env tag. ``"production"`` (case-insensitive) flips the
#: call-context dev-shim guard from "warn" to "refuse to start" so a
#: production deployment with an unconfigured verifier crash-loops at
#: startup rather than silently accepting unsigned headers.
ENV_ENVIRONMENT: Final[str] = "ENVIRONMENT"

#: Optional. JWKS URL the call-context middleware consults to verify
#: signed call-contexts coming back to auth-service. Empty enables the
#: **dev shim** that accepts an unsigned JSON header (see
#: ``custos_auth.middleware.callctx`` for the wire format). Phase G
#: (AS-IMPL-018) ships auth-service's own JWKS endpoint and Phase G
#: (AS-IMPL-019) ships the verifier helper that consumes it; setting
#: this env var before those phases land surfaces a
#: ``NotImplementedError`` on the first protected request, which is the
#: intentional fail-loud signal that operators must roll back to the
#: dev shim until the verifier is in place.
ENV_CALLCTX_VERIFIER_URL: Final[str] = "CUSTOS_AUTH_CALLCTX_VERIFIER_URL"

#: Optional. Colon-separated list of filesystem paths to
#: ``permissions.yaml`` files that the Phase D registry loader ingests
#: at startup (AS-IMPL-008). Empty falls back to the bundled
#: platform-M1 registry shipped with the package
#: (``custos_auth._data.permissions``), which declares every permission
#: referenced by the six built-in roles. Each path independently
#: contributes ``(name, description, declared_by)`` rows; multi-
#: declarer names are merged with a ``|`` separator on
#: ``declared_by``. The loader refuses to start the service when any
#: built-in role references a name that no path declared.
ENV_PERMISSIONS_PATHS: Final[str] = "CUSTOS_AUTH_PERMISSIONS_PATHS"

#: Optional. Time-to-live (seconds) for entries in the per-replica
#: authz decision cache (AS-IMPL-012). Default 60 seconds matches the
#: ``design/components/auth-service/design.md`` § "Cache Invalidation
#: Bus" table. Setting the value to ``0`` puts the cache in **bypass
#: mode** — every authorize call performs a full binding resolution
#: against the auth store and the cache neither reads nor writes —
#: which is the AS-IMPL-012 acceptance-criterion knob for diagnostic
#: and side-by-side comparison scenarios. Negative values are
#: rejected with :class:`SettingsError`. Non-integer values are
#: rejected with :class:`SettingsError`.
ENV_AUTHZ_CACHE_TTL: Final[str] = "CUSTOS_AUTH_AUTHZ_CACHE_TTL"

#: Optional. Default lifetime (seconds) for newly minted service
#: tokens (AS-IMPL-013, REQ-035). Matches the design doc §
#: Configuration default ``90d`` (= 7_776_000 seconds). Operators
#: override per platform via this env var; clients additionally
#: override per mint by passing ``ttl_seconds`` on the request body.
#: Must be a positive integer — a service token with zero or
#: negative TTL is meaningless (and the audit trail would record an
#: already-expired token), so :class:`SettingsError` is raised on
#: anything non-positive.
ENV_SERVICE_TOKEN_TTL_DEFAULT: Final[str] = "CUSTOS_AUTH_SERVICE_TOKEN_TTL_DEFAULT"

#: Default lifetime for service tokens when no env override is set.
#: 90 days in seconds.
DEFAULT_SERVICE_TOKEN_TTL_SECONDS: Final[int] = 90 * 24 * 60 * 60


class SettingsError(RuntimeError):
    """Raised when the environment is missing a required setting or carries a malformed value."""


@dataclass(frozen=True, slots=True)
class Settings:
    """Parsed and validated auth-service configuration."""

    auth_store_dsn: str
    metadata_store_dsn: str
    environment: str
    callctx_verifier_url: str
    permissions_paths: tuple[str, ...]
    authz_cache_ttl_seconds: int
    service_token_ttl_default_seconds: int

    @property
    def is_production(self) -> bool:
        return self.environment.lower() == "production"

    @property
    def callctx_dev_shim_active(self) -> bool:
        """True when no verifier URL is configured (dev mode)."""
        return self.callctx_verifier_url == ""

    @property
    def authz_cache_enabled(self) -> bool:
        """True when the authz decision cache is configured to store entries."""
        return self.authz_cache_ttl_seconds > 0


def _require(name: str, env: dict[str, str]) -> str:
    value = env.get(name, "")
    if not value:
        raise SettingsError(
            f"{name} is required and must be set to a non-empty value "
            f"(see design/components/auth-service/design.md § Configuration)"
        )
    return value


def _parse_paths(raw: str) -> tuple[str, ...]:
    """Split a colon-separated path list, trim entries, and drop empties."""
    return tuple(part.strip() for part in raw.split(":") if part.strip())


def _parse_authz_cache_ttl(raw: str) -> int:
    """Parse the ``CUSTOS_AUTH_AUTHZ_CACHE_TTL`` env value.

    Empty string falls back to
    :data:`custos_auth.authz_cache.DEFAULT_AUTHZ_CACHE_TTL_SECONDS`.
    A literal ``0`` is preserved (and enables the bypass-mode
    acceptance criterion). Negative values raise
    :class:`SettingsError` because a negative TTL is not a meaningful
    configuration. Non-integer values raise :class:`SettingsError`.
    """
    if raw == "":
        return DEFAULT_AUTHZ_CACHE_TTL_SECONDS
    try:
        value = int(raw)
    except ValueError as exc:
        raise SettingsError(
            f"{ENV_AUTHZ_CACHE_TTL} must be a non-negative integer (got {raw!r})"
        ) from exc
    if value < 0:
        raise SettingsError(
            f"{ENV_AUTHZ_CACHE_TTL} must be non-negative (got {value}); use 0 to disable the cache."
        )
    return value


def _parse_service_token_ttl_default(raw: str) -> int:
    """Parse the ``CUSTOS_AUTH_SERVICE_TOKEN_TTL_DEFAULT`` env value.

    Empty string falls back to
    :data:`DEFAULT_SERVICE_TOKEN_TTL_SECONDS` (90 days). Values
    ≤ 0 are rejected because a service token with zero or negative
    TTL would be born expired. Non-integer values are also
    rejected.
    """
    if raw == "":
        return DEFAULT_SERVICE_TOKEN_TTL_SECONDS
    try:
        value = int(raw)
    except ValueError as exc:
        raise SettingsError(
            f"{ENV_SERVICE_TOKEN_TTL_DEFAULT} must be a positive integer (got {raw!r})"
        ) from exc
    if value <= 0:
        raise SettingsError(
            f"{ENV_SERVICE_TOKEN_TTL_DEFAULT} must be a positive integer (got {value}); "
            "a service token's default TTL must be strictly positive."
        )
    return value


def load_settings(env: dict[str, str] | None = None) -> Settings:
    """Parse a :class:`Settings` from the supplied env mapping (default ``os.environ``)."""
    src: dict[str, str] = dict(os.environ if env is None else env)
    return Settings(
        auth_store_dsn=_require(ENV_AUTH_STORE_DSN, src),
        metadata_store_dsn=_require(ENV_METADATA_STORE_DSN, src),
        environment=src.get(ENV_ENVIRONMENT, "development").strip() or "development",
        callctx_verifier_url=src.get(ENV_CALLCTX_VERIFIER_URL, "").strip(),
        permissions_paths=_parse_paths(src.get(ENV_PERMISSIONS_PATHS, "").strip()),
        authz_cache_ttl_seconds=_parse_authz_cache_ttl(
            src.get(ENV_AUTHZ_CACHE_TTL, "").strip(),
        ),
        service_token_ttl_default_seconds=_parse_service_token_ttl_default(
            src.get(ENV_SERVICE_TOKEN_TTL_DEFAULT, "").strip(),
        ),
    )


__all__ = [
    "DEFAULT_SERVICE_TOKEN_TTL_SECONDS",
    "ENV_AUTHZ_CACHE_TTL",
    "ENV_AUTH_STORE_DSN",
    "ENV_CALLCTX_VERIFIER_URL",
    "ENV_ENVIRONMENT",
    "ENV_METADATA_STORE_DSN",
    "ENV_PERMISSIONS_PATHS",
    "ENV_SERVICE_TOKEN_TTL_DEFAULT",
    "Settings",
    "SettingsError",
    "load_settings",
]

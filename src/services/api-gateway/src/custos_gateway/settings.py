"""Runtime configuration parsed from environment variables.

The API Gateway is configured exclusively through the ``CUSTOS_GATEWAY_*`` env
vars documented in ``design/components/api-gateway/design.md`` § Configuration,
plus the Dapr sidecar coordinates (``DAPR_HTTP_HOST`` / ``DAPR_HTTP_PORT``) used
to reach downstream components and the Auth Service.

This module is deliberately stdlib-only so it can be imported by both the ASGI
app factory and lightweight test fixtures without dragging in FastAPI, httpx, or
the Dapr SDK.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Final

# --- § Configuration knobs (design.md § Configuration) -----------------------

#: Optional. TLS listen address. Default ``:8443``.
ENV_LISTEN_ADDR: Final[str] = "CUSTOS_GATEWAY_LISTEN_ADDR"

#: Required. Dapr secret reference for the TLS certificate.
ENV_TLS_CERT_REF: Final[str] = "CUSTOS_GATEWAY_TLS_CERT_REF"

#: Required. Dapr secret reference for the TLS private key.
ENV_TLS_KEY_REF: Final[str] = "CUSTOS_GATEWAY_TLS_KEY_REF"

#: Required. JSON list of allowed CORS origins for the UI. No wildcard.
ENV_CORS_ALLOWED_ORIGINS: Final[str] = "CUSTOS_GATEWAY_CORS_ALLOWED_ORIGINS"

#: Optional. Global default request-body size cap (bytes). Default ``1048576``.
ENV_BODY_MAX_BYTES_DEFAULT: Final[str] = "CUSTOS_GATEWAY_BODY_MAX_BYTES_DEFAULT"

#: Optional. Body size cap override for workflow/template publish. Default ``5242880``.
ENV_BODY_MAX_BYTES_PUBLISH: Final[str] = "CUSTOS_GATEWAY_BODY_MAX_BYTES_PUBLISH"

#: Optional. Per-principal write rate (rps). Default ``20``.
ENV_RATE_LIMIT_PRINCIPAL_WRITES_RPS: Final[str] = "CUSTOS_GATEWAY_RATE_LIMIT_PRINCIPAL_WRITES_RPS"

#: Optional. Per-principal write burst. Default ``40``.
ENV_RATE_LIMIT_PRINCIPAL_WRITES_BURST: Final[str] = (
    "CUSTOS_GATEWAY_RATE_LIMIT_PRINCIPAL_WRITES_BURST"
)

#: Optional. Per-workspace write rate (rps). Default ``200``.
ENV_RATE_LIMIT_WORKSPACE_WRITES_RPS: Final[str] = "CUSTOS_GATEWAY_RATE_LIMIT_WORKSPACE_WRITES_RPS"

#: Optional. Per-workspace write burst. Default ``400``.
ENV_RATE_LIMIT_WORKSPACE_WRITES_BURST: Final[str] = (
    "CUSTOS_GATEWAY_RATE_LIMIT_WORKSPACE_WRITES_BURST"
)

#: Optional. Idempotency-record TTL. Default ``24h``.
ENV_IDEMPOTENCY_TTL: Final[str] = "CUSTOS_GATEWAY_IDEMPOTENCY_TTL"

#: Optional. Device-code session TTL. Default ``15m``.
ENV_DEVICE_CODE_TTL: Final[str] = "CUSTOS_GATEWAY_DEVICE_CODE_TTL"

#: Optional. Device-code poll-interval hint returned to the CLI. Default ``5s``.
ENV_DEVICE_CODE_POLL_INTERVAL: Final[str] = "CUSTOS_GATEWAY_DEVICE_CODE_POLL_INTERVAL"

#: Required only when the device-code flow is enabled. Issuer alias (from
#: ``CUSTOS_AUTH_OIDC_ISSUERS``) for the device-code landing page.
ENV_OIDC_DEFAULT_ISSUER: Final[str] = "CUSTOS_GATEWAY_OIDC_DEFAULT_ISSUER"

#: Optional. Initial backoff between startup permission-check retries when the
#: Auth Service is transiently unreachable (seconds). Default ``1``.
ENV_STARTUP_PERMISSION_CHECK_INITIAL_BACKOFF: Final[str] = (
    "CUSTOS_GATEWAY_STARTUP_PERMISSION_CHECK_INITIAL_BACKOFF_SECONDS"
)

#: Optional. Maximum backoff between startup permission-check retries (seconds).
#: Default ``30``.
ENV_STARTUP_PERMISSION_CHECK_MAX_BACKOFF: Final[str] = (
    "CUSTOS_GATEWAY_STARTUP_PERMISSION_CHECK_MAX_BACKOFF_SECONDS"
)

# --- Dapr sidecar coordinates -------------------------------------------------

#: Optional. Dapr sidecar HTTP host. Default ``127.0.0.1``.
ENV_DAPR_HTTP_HOST: Final[str] = "DAPR_HTTP_HOST"

#: Optional. Dapr sidecar HTTP port. Default ``3500``.
ENV_DAPR_HTTP_PORT: Final[str] = "DAPR_HTTP_PORT"

# --- Operational --------------------------------------------------------------

#: Operational environment tag. Default ``development``.
ENV_ENVIRONMENT: Final[str] = "ENVIRONMENT"

# --- Defaults (design.md § Configuration) ------------------------------------

DEFAULT_LISTEN_ADDR: Final[str] = ":8443"
DEFAULT_BODY_MAX_BYTES_DEFAULT: Final[int] = 1_048_576
DEFAULT_BODY_MAX_BYTES_PUBLISH: Final[int] = 5_242_880
DEFAULT_RATE_LIMIT_PRINCIPAL_WRITES_RPS: Final[int] = 20
DEFAULT_RATE_LIMIT_PRINCIPAL_WRITES_BURST: Final[int] = 40
DEFAULT_RATE_LIMIT_WORKSPACE_WRITES_RPS: Final[int] = 200
DEFAULT_RATE_LIMIT_WORKSPACE_WRITES_BURST: Final[int] = 400
DEFAULT_IDEMPOTENCY_TTL_SECONDS: Final[int] = 24 * 60 * 60
DEFAULT_DEVICE_CODE_TTL_SECONDS: Final[int] = 15 * 60
DEFAULT_DEVICE_CODE_POLL_INTERVAL_SECONDS: Final[int] = 5
DEFAULT_STARTUP_PERMISSION_CHECK_INITIAL_BACKOFF_SECONDS: Final[int] = 1
DEFAULT_STARTUP_PERMISSION_CHECK_MAX_BACKOFF_SECONDS: Final[int] = 30
DEFAULT_DAPR_HTTP_HOST: Final[str] = "127.0.0.1"
DEFAULT_DAPR_HTTP_PORT: Final[int] = 3500
DEFAULT_ENVIRONMENT: Final[str] = "development"

_DURATION_RE: Final[re.Pattern[str]] = re.compile(r"^(?P<value>\d+)(?P<unit>[smhd]?)$")
_DURATION_UNIT_SECONDS: Final[dict[str, int]] = {
    "": 1,
    "s": 1,
    "m": 60,
    "h": 60 * 60,
    "d": 24 * 60 * 60,
}


class SettingsError(RuntimeError):
    """Raised when the environment is missing a required setting or carries a malformed value."""


@dataclass(frozen=True, slots=True)
class Settings:
    """Parsed and validated API Gateway configuration."""

    listen_addr: str
    tls_cert_ref: str
    tls_key_ref: str
    cors_allowed_origins: tuple[str, ...]
    body_max_bytes_default: int
    body_max_bytes_publish: int
    rate_limit_principal_writes_rps: int
    rate_limit_principal_writes_burst: int
    rate_limit_workspace_writes_rps: int
    rate_limit_workspace_writes_burst: int
    idempotency_ttl_seconds: int
    device_code_ttl_seconds: int
    device_code_poll_interval_seconds: int
    startup_permission_check_initial_backoff_seconds: int
    startup_permission_check_max_backoff_seconds: int
    oidc_default_issuer: str  # empty string means "device-code flow disabled"
    dapr_http_host: str
    dapr_http_port: int
    environment: str

    @property
    def is_production(self) -> bool:
        return self.environment.lower() == "production"

    @property
    def device_code_enabled(self) -> bool:
        """True when a default OIDC issuer is configured for the device-code flow."""
        return self.oidc_default_issuer != ""


def _require(name: str, env: dict[str, str]) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise SettingsError(
            f"{name} is required and must be set to a non-empty value "
            f"(see design/components/api-gateway/design.md § Configuration)"
        )
    return value


def _opt_str(name: str, env: dict[str, str], default: str) -> str:
    return env.get(name, "").strip() or default


def _opt_positive_int(name: str, env: dict[str, str], default: int) -> int:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise SettingsError(f"{name} must be a positive integer (got {raw!r})") from exc
    if value <= 0:
        raise SettingsError(f"{name} must be a positive integer (got {raw!r})")
    return value


def _opt_duration_seconds(name: str, env: dict[str, str], default: int) -> int:
    """Parse a duration like ``24h`` / ``15m`` / ``5s`` / ``3600`` into seconds."""
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    match = _DURATION_RE.match(raw.strip())
    if match is None:
        raise SettingsError(
            f"{name} must be a duration like '24h', '15m', '5s', or a bare second "
            f"count (got {raw!r})"
        )
    value = int(match.group("value")) * _DURATION_UNIT_SECONDS[match.group("unit")]
    if value <= 0:
        raise SettingsError(f"{name} must be a positive duration (got {raw!r})")
    return value


def _require_cors_origins(name: str, env: dict[str, str]) -> tuple[str, ...]:
    raw = _require(name, env)
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SettingsError(f"{name} must be a JSON array of origin strings (got {raw!r})") from exc
    if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
        raise SettingsError(f"{name} must be a JSON array of origin strings (got {raw!r})")
    origins = tuple(item.strip() for item in parsed if item.strip())
    if not origins:
        raise SettingsError(f"{name} must contain at least one origin")
    if any(origin == "*" for origin in origins):
        raise SettingsError(f"{name} must not contain the '*' wildcard origin")
    return origins


def load_settings(env: dict[str, str] | None = None) -> Settings:
    """Parse a :class:`Settings` from the supplied env mapping (default ``os.environ``).

    Raises :class:`SettingsError` when a required variable is missing or any
    value is malformed.
    """
    src: dict[str, str] = dict(os.environ if env is None else env)
    return Settings(
        listen_addr=_opt_str(ENV_LISTEN_ADDR, src, DEFAULT_LISTEN_ADDR),
        tls_cert_ref=_require(ENV_TLS_CERT_REF, src),
        tls_key_ref=_require(ENV_TLS_KEY_REF, src),
        cors_allowed_origins=_require_cors_origins(ENV_CORS_ALLOWED_ORIGINS, src),
        body_max_bytes_default=_opt_positive_int(
            ENV_BODY_MAX_BYTES_DEFAULT, src, DEFAULT_BODY_MAX_BYTES_DEFAULT
        ),
        body_max_bytes_publish=_opt_positive_int(
            ENV_BODY_MAX_BYTES_PUBLISH, src, DEFAULT_BODY_MAX_BYTES_PUBLISH
        ),
        rate_limit_principal_writes_rps=_opt_positive_int(
            ENV_RATE_LIMIT_PRINCIPAL_WRITES_RPS, src, DEFAULT_RATE_LIMIT_PRINCIPAL_WRITES_RPS
        ),
        rate_limit_principal_writes_burst=_opt_positive_int(
            ENV_RATE_LIMIT_PRINCIPAL_WRITES_BURST, src, DEFAULT_RATE_LIMIT_PRINCIPAL_WRITES_BURST
        ),
        rate_limit_workspace_writes_rps=_opt_positive_int(
            ENV_RATE_LIMIT_WORKSPACE_WRITES_RPS, src, DEFAULT_RATE_LIMIT_WORKSPACE_WRITES_RPS
        ),
        rate_limit_workspace_writes_burst=_opt_positive_int(
            ENV_RATE_LIMIT_WORKSPACE_WRITES_BURST, src, DEFAULT_RATE_LIMIT_WORKSPACE_WRITES_BURST
        ),
        idempotency_ttl_seconds=_opt_duration_seconds(
            ENV_IDEMPOTENCY_TTL, src, DEFAULT_IDEMPOTENCY_TTL_SECONDS
        ),
        device_code_ttl_seconds=_opt_duration_seconds(
            ENV_DEVICE_CODE_TTL, src, DEFAULT_DEVICE_CODE_TTL_SECONDS
        ),
        device_code_poll_interval_seconds=_opt_duration_seconds(
            ENV_DEVICE_CODE_POLL_INTERVAL, src, DEFAULT_DEVICE_CODE_POLL_INTERVAL_SECONDS
        ),
        startup_permission_check_initial_backoff_seconds=_opt_positive_int(
            ENV_STARTUP_PERMISSION_CHECK_INITIAL_BACKOFF,
            src,
            DEFAULT_STARTUP_PERMISSION_CHECK_INITIAL_BACKOFF_SECONDS,
        ),
        startup_permission_check_max_backoff_seconds=_opt_positive_int(
            ENV_STARTUP_PERMISSION_CHECK_MAX_BACKOFF,
            src,
            DEFAULT_STARTUP_PERMISSION_CHECK_MAX_BACKOFF_SECONDS,
        ),
        oidc_default_issuer=_opt_str(ENV_OIDC_DEFAULT_ISSUER, src, ""),
        dapr_http_host=_opt_str(ENV_DAPR_HTTP_HOST, src, DEFAULT_DAPR_HTTP_HOST),
        dapr_http_port=_opt_positive_int(ENV_DAPR_HTTP_PORT, src, DEFAULT_DAPR_HTTP_PORT),
        environment=_opt_str(ENV_ENVIRONMENT, src, DEFAULT_ENVIRONMENT),
    )


__all__ = [
    "DEFAULT_BODY_MAX_BYTES_DEFAULT",
    "DEFAULT_BODY_MAX_BYTES_PUBLISH",
    "DEFAULT_DAPR_HTTP_HOST",
    "DEFAULT_DAPR_HTTP_PORT",
    "DEFAULT_DEVICE_CODE_POLL_INTERVAL_SECONDS",
    "DEFAULT_DEVICE_CODE_TTL_SECONDS",
    "DEFAULT_ENVIRONMENT",
    "DEFAULT_IDEMPOTENCY_TTL_SECONDS",
    "DEFAULT_LISTEN_ADDR",
    "DEFAULT_RATE_LIMIT_PRINCIPAL_WRITES_BURST",
    "DEFAULT_RATE_LIMIT_PRINCIPAL_WRITES_RPS",
    "DEFAULT_RATE_LIMIT_WORKSPACE_WRITES_BURST",
    "DEFAULT_RATE_LIMIT_WORKSPACE_WRITES_RPS",
    "DEFAULT_STARTUP_PERMISSION_CHECK_INITIAL_BACKOFF_SECONDS",
    "DEFAULT_STARTUP_PERMISSION_CHECK_MAX_BACKOFF_SECONDS",
    "ENV_CORS_ALLOWED_ORIGINS",
    "ENV_TLS_CERT_REF",
    "ENV_TLS_KEY_REF",
    "Settings",
    "SettingsError",
    "load_settings",
]

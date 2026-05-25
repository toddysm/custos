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

#: Optional. Time-to-live (seconds) for entries in the per-replica
#: authn cache that backs :func:`custos_auth.authn.verify_token`
#: (AS-IMPL-014). Default 30 seconds matches the design's § Cache
#: Invalidation Bus table. Setting the value to ``0`` puts the cache
#: in **bypass mode** — every verify call performs a full SPL
#: lookup. Negative or non-integer values raise
#: :class:`SettingsError`.
ENV_AUTHN_CACHE_TTL: Final[str] = "CUSTOS_AUTH_AUTHN_CACHE_TTL"

#: Default lifetime for authn cache entries when the env override is
#: unset. The design fixes this at 30 s so a revoke is observable
#: across replicas within one TTL window even if the eviction event
#: gets lost in the pub/sub transport.
DEFAULT_AUTHN_CACHE_TTL_SECONDS: Final[int] = 30

#: Optional. Interval (seconds) at which the background sweeper
#: scans for service tokens whose ``expires_at`` has elapsed,
#: emits ``token.expired``, publishes a cache-eviction event, and
#: physically deletes the row (AS-IMPL-016). Setting the value to
#: ``0`` disables the sweeper entirely (useful for tests and for
#: single-purpose replicas that should never run the housekeeping
#: loop). Negative or non-integer values raise
#: :class:`SettingsError`.
ENV_TOKEN_SWEEPER_INTERVAL: Final[str] = "CUSTOS_AUTH_TOKEN_SWEEPER_INTERVAL_SECONDS"

#: Default interval for the token sweeper when the env override is
#: unset. Five minutes balances audit-row latency against SPL load;
#: an operator who needs the sweep to land within a minute can
#: override the env var.
DEFAULT_TOKEN_SWEEPER_INTERVAL_SECONDS: Final[int] = 300

#: Dapr Secrets reference (logical name) under which the EdDSA
#: call-context signing key PEM is stored. During app lifespan startup,
#: auth-service consults this setting to decide whether to fetch the
#: initial signing key from Dapr Secrets or fall back to generating an
#: ephemeral key when no reference is configured. Phase G AS-IMPL-018
#: wires the signer/JWKS into the app lifespan, including the
#: "missing key ref crash-loops production" guard and rotation
#: scheduler integration.
ENV_CALL_CONTEXT_KEY_REF: Final[str] = "CUSTOS_AUTH_CALL_CONTEXT_KEY_REF"

#: Optional. Name of the Dapr secret-store component the resolver
#: consults to fetch :data:`ENV_CALL_CONTEXT_KEY_REF`. Empty falls
#: back to :data:`DEFAULT_CALL_CONTEXT_SECRET_STORE`, which matches
#: the default component name shipped by the Helm umbrella chart.
ENV_CALL_CONTEXT_SECRET_STORE: Final[str] = "CUSTOS_AUTH_CALL_CONTEXT_SECRET_STORE"

#: Default Dapr secret-store component name.
DEFAULT_CALL_CONTEXT_SECRET_STORE: Final[str] = "custos-secrets"

#: Optional. JWT ``aud`` claim stamped into every call-context token
#: minted by :class:`custos_auth.callctx_signer.CallContextSigner`.
#: Receivers must be configured with the same value. Empty falls
#: back to
#: :data:`custos_auth.callctx_signer.DEFAULT_AUDIENCE`
#: (``custos.internal``).
ENV_CALL_CONTEXT_AUDIENCE: Final[str] = "CUSTOS_AUTH_CALL_CONTEXT_AUDIENCE"

#: Optional. Default lifetime (seconds) applied to minted call-context
#: tokens when the caller does not pass an explicit override. Default
#: :data:`DEFAULT_CALL_CONTEXT_TTL_SECONDS` (5 min) matches the
#: design's "Internal vs External Auth — Trust Model" section. Must be
#: a strictly positive integer; non-integer or non-positive values
#: raise :class:`SettingsError`.
ENV_CALL_CONTEXT_TTL_SECONDS: Final[str] = "CUSTOS_AUTH_CALL_CONTEXT_TTL_SECONDS"

#: Default call-context JWT lifetime (5 min). Mirrors
#: :data:`custos_auth.callctx_signer.DEFAULT_TTL_SECONDS`; carried
#: in settings so the runtime knob lives next to its peers.
DEFAULT_CALL_CONTEXT_TTL_SECONDS: Final[int] = 300

#: Default JWT ``aud`` claim. Mirrors
#: :data:`custos_auth.callctx_signer.DEFAULT_AUDIENCE`; duplicated
#: here so the settings module remains stdlib-only and does not
#: have to import the signer just to know the default.
DEFAULT_CALL_CONTEXT_AUDIENCE: Final[str] = "custos.internal"

#: Optional. Interval (seconds) between call-context signing-key
#: rotations (AS-IMPL-018). Default
#: :data:`DEFAULT_CALL_CONTEXT_KEY_ROTATION_SECONDS` (7 days) mirrors
#: the design's ``CUSTOS_AUTH_CALL_CONTEXT_KEY_ROTATION`` default.
#: Setting the value to ``0`` disables the in-process rotation
#: loop entirely — operators must then manage rotation externally
#: (e.g. via a Kubernetes CronJob that swaps the Dapr secret).
#: Negative or non-integer values raise :class:`SettingsError`.
ENV_CALL_CONTEXT_KEY_ROTATION: Final[str] = "CUSTOS_AUTH_CALL_CONTEXT_KEY_ROTATION"

#: Default rotation interval (7 days, in seconds). Mirrors
#: :data:`custos_auth.callctx_keyring.DEFAULT_ROTATION_PERIOD_SECONDS`;
#: carried in settings so the runtime knob lives next to its peers.
DEFAULT_CALL_CONTEXT_KEY_ROTATION_SECONDS: Final[int] = 7 * 24 * 60 * 60

#: Optional boolean. Gates the OIDC verifier code paths shipped in
#: Phase H (AS-IMPL-020 — AS-IMPL-023). M1 deployments ship with the
#: OIDC routes mounted but disabled: ``POST /v1/auth/login/oidc/callback``
#: returns ``503 oidc_not_enabled`` until the operator flips this flag
#: *and* Phase H lands. Phase I (AS-IMPL-024) only stubs the route
#: behind this flag — the actual verifier wiring follows in Phase H.
#: Accepts the conventional truthy/falsy strings (``true`` / ``false``
#: / ``1`` / ``0`` / ``yes`` / ``no``, case-insensitive). Empty falls
#: back to ``False``. Other values raise :class:`SettingsError`.
ENV_OIDC_ENABLED: Final[str] = "CUSTOS_AUTH_OIDC_ENABLED"

#: Default OIDC feature-flag state. ``False`` keeps the OIDC callback
#: route in stub mode until Phase H ships.
DEFAULT_OIDC_ENABLED: Final[bool] = False

#: Optional. JSON document carrying the OIDC issuer config consumed
#: by Phase H (AS-IMPL-020). Empty falls back to an empty issuer
#: list — the M1 default deployment shape. See
#: :mod:`custos_auth.oidc.config` for the schema. Malformed JSON or
#: schema violations raise :class:`SettingsError` at startup; the
#: auth-service refuses to come up so operators see the problem at
#: deploy time rather than at first user login.
ENV_OIDC_ISSUERS: Final[str] = "CUSTOS_AUTH_OIDC_ISSUERS"


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
    authn_cache_ttl_seconds: int
    token_sweeper_interval_seconds: int
    call_context_key_ref: str
    call_context_secret_store: str
    call_context_audience: str
    call_context_ttl_seconds: int
    call_context_key_rotation_seconds: int
    oidc_enabled: bool
    oidc_issuers_raw: str

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

    @property
    def authn_cache_enabled(self) -> bool:
        """True when the authn cache is configured to store entries."""
        return self.authn_cache_ttl_seconds > 0

    @property
    def token_sweeper_enabled(self) -> bool:
        """True when the token sweeper is configured to run.

        ``CUSTOS_AUTH_TOKEN_SWEEPER_INTERVAL_SECONDS=0`` disables
        sweep work entirely. The current lifespan flow may still
        create the background task, but the sweeper loop returns
        immediately when disabled instead of performing any periodic
        cleanup.
        """
        return self.token_sweeper_interval_seconds > 0


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


def _parse_authn_cache_ttl(raw: str) -> int:
    """Parse the ``CUSTOS_AUTH_AUTHN_CACHE_TTL`` env value.

    Empty string falls back to
    :data:`DEFAULT_AUTHN_CACHE_TTL_SECONDS`. ``0`` enables the
    bypass-mode acceptance criterion (every verify hits the store).
    Negative or non-integer values raise :class:`SettingsError`.
    """
    if raw == "":
        return DEFAULT_AUTHN_CACHE_TTL_SECONDS
    try:
        value = int(raw)
    except ValueError as exc:
        raise SettingsError(
            f"{ENV_AUTHN_CACHE_TTL} must be a non-negative integer (got {raw!r})"
        ) from exc
    if value < 0:
        raise SettingsError(
            f"{ENV_AUTHN_CACHE_TTL} must be non-negative (got {value}); use 0 to disable the cache."
        )
    return value


def _parse_token_sweeper_interval(raw: str) -> int:
    """Parse the ``CUSTOS_AUTH_TOKEN_SWEEPER_INTERVAL_SECONDS`` env value.

    Empty string falls back to
    :data:`DEFAULT_TOKEN_SWEEPER_INTERVAL_SECONDS`. ``0`` disables
    the sweeper. Negative or non-integer values raise
    :class:`SettingsError`.
    """
    if raw == "":
        return DEFAULT_TOKEN_SWEEPER_INTERVAL_SECONDS
    try:
        value = int(raw)
    except ValueError as exc:
        raise SettingsError(
            f"{ENV_TOKEN_SWEEPER_INTERVAL} must be a non-negative integer (got {raw!r})"
        ) from exc
    if value < 0:
        raise SettingsError(
            f"{ENV_TOKEN_SWEEPER_INTERVAL} must be non-negative (got {value}); "
            "use 0 to disable the sweeper."
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


def _parse_call_context_ttl(raw: str) -> int:
    """Parse the ``CUSTOS_AUTH_CALL_CONTEXT_TTL_SECONDS`` env value.

    Empty string falls back to
    :data:`DEFAULT_CALL_CONTEXT_TTL_SECONDS` (5 minutes). Zero or
    negative TTLs would mint already-expired tokens, so the parser
    rejects anything non-positive. Non-integer values are rejected
    too.
    """
    if raw == "":
        return DEFAULT_CALL_CONTEXT_TTL_SECONDS
    try:
        value = int(raw)
    except ValueError as exc:
        raise SettingsError(
            f"{ENV_CALL_CONTEXT_TTL_SECONDS} must be a positive integer (got {raw!r})"
        ) from exc
    if value <= 0:
        raise SettingsError(
            f"{ENV_CALL_CONTEXT_TTL_SECONDS} must be a positive integer (got {value}); "
            "minting a call-context token with a zero or negative TTL "
            "would be born expired."
        )
    return value


def _parse_call_context_key_rotation(raw: str) -> int:
    """Parse the ``CUSTOS_AUTH_CALL_CONTEXT_KEY_ROTATION`` env value.

    Empty string falls back to
    :data:`DEFAULT_CALL_CONTEXT_KEY_ROTATION_SECONDS` (7 days). ``0``
    disables the in-process rotation loop (operator manages rotation
    externally). Negative or non-integer values raise
    :class:`SettingsError`.
    """
    if raw == "":
        return DEFAULT_CALL_CONTEXT_KEY_ROTATION_SECONDS
    try:
        value = int(raw)
    except ValueError as exc:
        raise SettingsError(
            f"{ENV_CALL_CONTEXT_KEY_ROTATION} must be a non-negative integer (got {raw!r})"
        ) from exc
    if value < 0:
        raise SettingsError(
            f"{ENV_CALL_CONTEXT_KEY_ROTATION} must be non-negative (got {value}); "
            "use 0 to disable in-process rotation."
        )
    return value


_BOOL_TRUE_VALUES: Final[frozenset[str]] = frozenset({"true", "1", "yes", "on"})
_BOOL_FALSE_VALUES: Final[frozenset[str]] = frozenset({"false", "0", "no", "off"})


def _parse_oidc_enabled(raw: str) -> bool:
    """Parse the ``CUSTOS_AUTH_OIDC_ENABLED`` env value.

    Empty string falls back to :data:`DEFAULT_OIDC_ENABLED` (``False``).
    Accepted truthy values: ``true``, ``1``, ``yes``, ``on``
    (case-insensitive). Accepted falsy values: ``false``, ``0``,
    ``no``, ``off``. Any other value raises :class:`SettingsError`.
    """
    if raw == "":
        return DEFAULT_OIDC_ENABLED
    folded = raw.lower()
    if folded in _BOOL_TRUE_VALUES:
        return True
    if folded in _BOOL_FALSE_VALUES:
        return False
    raise SettingsError(
        f"{ENV_OIDC_ENABLED} must be a boolean "
        f"(one of {{{', '.join(sorted(_BOOL_TRUE_VALUES | _BOOL_FALSE_VALUES))}}}); "
        f"got {raw!r}"
    )


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
        authn_cache_ttl_seconds=_parse_authn_cache_ttl(
            src.get(ENV_AUTHN_CACHE_TTL, "").strip(),
        ),
        token_sweeper_interval_seconds=_parse_token_sweeper_interval(
            src.get(ENV_TOKEN_SWEEPER_INTERVAL, "").strip(),
        ),
        call_context_key_ref=src.get(ENV_CALL_CONTEXT_KEY_REF, "").strip(),
        call_context_secret_store=(
            src.get(ENV_CALL_CONTEXT_SECRET_STORE, "").strip() or DEFAULT_CALL_CONTEXT_SECRET_STORE
        ),
        call_context_audience=(
            src.get(ENV_CALL_CONTEXT_AUDIENCE, "").strip() or DEFAULT_CALL_CONTEXT_AUDIENCE
        ),
        call_context_ttl_seconds=_parse_call_context_ttl(
            src.get(ENV_CALL_CONTEXT_TTL_SECONDS, "").strip(),
        ),
        call_context_key_rotation_seconds=_parse_call_context_key_rotation(
            src.get(ENV_CALL_CONTEXT_KEY_ROTATION, "").strip(),
        ),
        oidc_enabled=_parse_oidc_enabled(
            src.get(ENV_OIDC_ENABLED, "").strip(),
        ),
        oidc_issuers_raw=src.get(ENV_OIDC_ISSUERS, ""),
    )


__all__ = [
    "DEFAULT_AUTHN_CACHE_TTL_SECONDS",
    "DEFAULT_CALL_CONTEXT_AUDIENCE",
    "DEFAULT_CALL_CONTEXT_KEY_ROTATION_SECONDS",
    "DEFAULT_CALL_CONTEXT_SECRET_STORE",
    "DEFAULT_CALL_CONTEXT_TTL_SECONDS",
    "DEFAULT_OIDC_ENABLED",
    "DEFAULT_SERVICE_TOKEN_TTL_SECONDS",
    "DEFAULT_TOKEN_SWEEPER_INTERVAL_SECONDS",
    "ENV_AUTHN_CACHE_TTL",
    "ENV_AUTHZ_CACHE_TTL",
    "ENV_AUTH_STORE_DSN",
    "ENV_CALLCTX_VERIFIER_URL",
    "ENV_CALL_CONTEXT_AUDIENCE",
    "ENV_CALL_CONTEXT_KEY_REF",
    "ENV_CALL_CONTEXT_KEY_ROTATION",
    "ENV_CALL_CONTEXT_SECRET_STORE",
    "ENV_CALL_CONTEXT_TTL_SECONDS",
    "ENV_ENVIRONMENT",
    "ENV_METADATA_STORE_DSN",
    "ENV_OIDC_ENABLED",
    "ENV_OIDC_ISSUERS",
    "ENV_PERMISSIONS_PATHS",
    "ENV_SERVICE_TOKEN_TTL_DEFAULT",
    "ENV_TOKEN_SWEEPER_INTERVAL",
    "Settings",
    "SettingsError",
    "load_settings",
]

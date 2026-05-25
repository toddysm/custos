"""Tests for :mod:`custos_auth.settings` (AS-IMPL-004)."""

from __future__ import annotations

import pytest

from custos_auth.authz_cache import DEFAULT_AUTHZ_CACHE_TTL_SECONDS
from custos_auth.settings import (
    DEFAULT_AUTHN_CACHE_TTL_SECONDS,
    DEFAULT_CALL_CONTEXT_AUDIENCE,
    DEFAULT_CALL_CONTEXT_KEY_ROTATION_SECONDS,
    DEFAULT_CALL_CONTEXT_SECRET_STORE,
    DEFAULT_CALL_CONTEXT_TTL_SECONDS,
    DEFAULT_SERVICE_TOKEN_TTL_SECONDS,
    DEFAULT_TOKEN_SWEEPER_INTERVAL_SECONDS,
    ENV_AUTH_STORE_DSN,
    ENV_AUTHN_CACHE_TTL,
    ENV_AUTHZ_CACHE_TTL,
    ENV_CALL_CONTEXT_AUDIENCE,
    ENV_CALL_CONTEXT_KEY_REF,
    ENV_CALL_CONTEXT_KEY_ROTATION,
    ENV_CALL_CONTEXT_SECRET_STORE,
    ENV_CALL_CONTEXT_TTL_SECONDS,
    ENV_INTERNAL_IDENTITY_MODE,
    ENV_METADATA_STORE_DSN,
    ENV_OIDC_ENABLED,
    ENV_SERVICE_TOKEN_TTL_DEFAULT,
    ENV_TOKEN_SWEEPER_INTERVAL,
    INTERNAL_IDENTITY_MODE_JWT,
    INTERNAL_IDENTITY_MODE_SPIFFE,
    Settings,
    SettingsError,
    load_settings,
)


def _required_env(**extra: str) -> dict[str, str]:
    """Return a minimal env with the required DSNs plus any overrides."""
    env = {
        ENV_AUTH_STORE_DSN: "postgresql://u:p@h:5432/custos_auth",
        ENV_METADATA_STORE_DSN: "postgresql://u:p@h:5432/custos_meta",
    }
    env.update(extra)
    return env


def test_load_settings_returns_dataclass_with_required_dsns() -> None:
    settings = load_settings(
        {
            ENV_AUTH_STORE_DSN: "postgresql://u:p@h:5432/custos_auth",
            ENV_METADATA_STORE_DSN: "postgresql://u:p@h:5432/custos_meta",
        },
    )
    assert isinstance(settings, Settings)
    assert settings.auth_store_dsn == "postgresql://u:p@h:5432/custos_auth"
    assert settings.metadata_store_dsn == "postgresql://u:p@h:5432/custos_meta"
    assert settings.environment == "development"
    assert settings.is_production is False


def test_load_settings_marks_production_environment() -> None:
    settings = load_settings(
        {
            ENV_AUTH_STORE_DSN: "postgresql://u:p@h:5432/custos_auth",
            ENV_METADATA_STORE_DSN: "postgresql://u:p@h:5432/custos_meta",
            "ENVIRONMENT": "Production",
        },
    )
    assert settings.is_production is True


def test_load_settings_rejects_missing_auth_store_dsn() -> None:
    with pytest.raises(SettingsError, match=ENV_AUTH_STORE_DSN):
        load_settings(
            {ENV_METADATA_STORE_DSN: "postgresql://u:p@h:5432/custos_meta"},
        )


def test_load_settings_rejects_missing_metadata_store_dsn() -> None:
    with pytest.raises(SettingsError, match=ENV_METADATA_STORE_DSN):
        load_settings(
            {ENV_AUTH_STORE_DSN: "postgresql://u:p@h:5432/custos_auth"},
        )


def test_load_settings_rejects_empty_dsn_values() -> None:
    with pytest.raises(SettingsError):
        load_settings(
            {
                ENV_AUTH_STORE_DSN: "",
                ENV_METADATA_STORE_DSN: "postgresql://u:p@h:5432/custos_meta",
            },
        )


def test_settings_is_frozen() -> None:
    settings = load_settings(
        {
            ENV_AUTH_STORE_DSN: "postgresql://u:p@h:5432/custos_auth",
            ENV_METADATA_STORE_DSN: "postgresql://u:p@h:5432/custos_meta",
        },
    )
    with pytest.raises((AttributeError, TypeError)):
        settings.auth_store_dsn = "other"  # type: ignore[misc]


def test_authz_cache_ttl_defaults_to_60_seconds() -> None:
    # Default tracks the design's "Authz (decision) … 60s" entry and is
    # single-sourced from ``custos_auth.authz_cache``.
    settings = load_settings(_required_env())
    assert settings.authz_cache_ttl_seconds == DEFAULT_AUTHZ_CACHE_TTL_SECONDS
    assert settings.authz_cache_enabled is True


def test_authz_cache_ttl_zero_disables_cache() -> None:
    # AS-IMPL-012 acceptance criterion: 0 puts the cache in bypass mode.
    settings = load_settings(_required_env(**{ENV_AUTHZ_CACHE_TTL: "0"}))
    assert settings.authz_cache_ttl_seconds == 0
    assert settings.authz_cache_enabled is False


def test_authz_cache_ttl_positive_override_is_respected() -> None:
    settings = load_settings(_required_env(**{ENV_AUTHZ_CACHE_TTL: "30"}))
    assert settings.authz_cache_ttl_seconds == 30


def test_authz_cache_ttl_rejects_negative_value() -> None:
    with pytest.raises(SettingsError, match="non-negative"):
        load_settings(_required_env(**{ENV_AUTHZ_CACHE_TTL: "-1"}))


def test_authz_cache_ttl_rejects_non_integer_value() -> None:
    with pytest.raises(SettingsError, match=ENV_AUTHZ_CACHE_TTL):
        load_settings(_required_env(**{ENV_AUTHZ_CACHE_TTL: "not-a-number"}))


# ---------------------------------------------------------------------------
# CUSTOS_AUTH_SERVICE_TOKEN_TTL_DEFAULT (AS-IMPL-013)
# ---------------------------------------------------------------------------


def test_service_token_ttl_default_is_90_days_when_unset() -> None:
    settings = load_settings(_required_env())
    # 90 days == 90 * 24 * 60 * 60 = 7_776_000 seconds. Asserting
    # against the constant keeps the test resilient to a future
    # default-tuning change while still pinning the named contract.
    assert settings.service_token_ttl_default_seconds == DEFAULT_SERVICE_TOKEN_TTL_SECONDS
    assert DEFAULT_SERVICE_TOKEN_TTL_SECONDS == 90 * 24 * 60 * 60


def test_service_token_ttl_default_positive_override_is_respected() -> None:
    settings = load_settings(_required_env(**{ENV_SERVICE_TOKEN_TTL_DEFAULT: "3600"}))
    assert settings.service_token_ttl_default_seconds == 3600


def test_service_token_ttl_default_empty_string_falls_back_to_default() -> None:
    settings = load_settings(_required_env(**{ENV_SERVICE_TOKEN_TTL_DEFAULT: ""}))
    assert settings.service_token_ttl_default_seconds == DEFAULT_SERVICE_TOKEN_TTL_SECONDS


def test_service_token_ttl_default_rejects_zero() -> None:
    # A zero default would mint tokens that are already expired the
    # instant they leave the API; that's never a legitimate config,
    # so reject at boot rather than silently breaking minting.
    with pytest.raises(SettingsError, match=ENV_SERVICE_TOKEN_TTL_DEFAULT):
        load_settings(_required_env(**{ENV_SERVICE_TOKEN_TTL_DEFAULT: "0"}))


def test_service_token_ttl_default_rejects_negative() -> None:
    with pytest.raises(SettingsError, match=ENV_SERVICE_TOKEN_TTL_DEFAULT):
        load_settings(_required_env(**{ENV_SERVICE_TOKEN_TTL_DEFAULT: "-1"}))


def test_service_token_ttl_default_rejects_non_integer_value() -> None:
    with pytest.raises(SettingsError, match=ENV_SERVICE_TOKEN_TTL_DEFAULT):
        load_settings(_required_env(**{ENV_SERVICE_TOKEN_TTL_DEFAULT: "not-a-number"}))


# ---------------------------------------------------------------------------
# CUSTOS_AUTH_AUTHN_CACHE_TTL (AS-IMPL-014)
# ---------------------------------------------------------------------------


def test_authn_cache_ttl_defaults_to_30_seconds() -> None:
    # Default tracks the design's "Authn (token) … 30s" entry.
    settings = load_settings(_required_env())
    assert settings.authn_cache_ttl_seconds == DEFAULT_AUTHN_CACHE_TTL_SECONDS
    assert DEFAULT_AUTHN_CACHE_TTL_SECONDS == 30
    assert settings.authn_cache_enabled is True


def test_authn_cache_ttl_zero_puts_cache_in_bypass_mode() -> None:
    # AS-IMPL-014 acceptance criterion: 0 disables the cache so
    # operators can run a forced-bypass smoke test in production
    # without redeploying.
    settings = load_settings(_required_env(**{ENV_AUTHN_CACHE_TTL: "0"}))
    assert settings.authn_cache_ttl_seconds == 0
    assert settings.authn_cache_enabled is False


def test_authn_cache_ttl_positive_override_is_respected() -> None:
    settings = load_settings(_required_env(**{ENV_AUTHN_CACHE_TTL: "5"}))
    assert settings.authn_cache_ttl_seconds == 5


def test_authn_cache_ttl_empty_string_falls_back_to_default() -> None:
    settings = load_settings(_required_env(**{ENV_AUTHN_CACHE_TTL: ""}))
    assert settings.authn_cache_ttl_seconds == DEFAULT_AUTHN_CACHE_TTL_SECONDS


def test_authn_cache_ttl_rejects_negative_value() -> None:
    with pytest.raises(SettingsError, match="non-negative"):
        load_settings(_required_env(**{ENV_AUTHN_CACHE_TTL: "-1"}))


def test_authn_cache_ttl_rejects_non_integer_value() -> None:
    with pytest.raises(SettingsError, match=ENV_AUTHN_CACHE_TTL):
        load_settings(_required_env(**{ENV_AUTHN_CACHE_TTL: "not-a-number"}))


# ---------------------------------------------------------------------------
# CUSTOS_AUTH_TOKEN_SWEEPER_INTERVAL_SECONDS (AS-IMPL-016)
# ---------------------------------------------------------------------------


def test_token_sweeper_interval_defaults_to_300_seconds() -> None:
    # Design's "sweeper sweeps every ~5 min" recommendation.
    settings = load_settings(_required_env())
    assert settings.token_sweeper_interval_seconds == DEFAULT_TOKEN_SWEEPER_INTERVAL_SECONDS
    assert DEFAULT_TOKEN_SWEEPER_INTERVAL_SECONDS == 300
    assert settings.token_sweeper_enabled is True


def test_token_sweeper_interval_zero_disables_the_sweeper() -> None:
    # Operators can disable the sweeper for tests, a degraded
    # cluster, or an external janitor without redeploying.
    settings = load_settings(_required_env(**{ENV_TOKEN_SWEEPER_INTERVAL: "0"}))
    assert settings.token_sweeper_interval_seconds == 0
    assert settings.token_sweeper_enabled is False


def test_token_sweeper_interval_positive_override_is_respected() -> None:
    settings = load_settings(_required_env(**{ENV_TOKEN_SWEEPER_INTERVAL: "60"}))
    assert settings.token_sweeper_interval_seconds == 60


def test_token_sweeper_interval_empty_string_falls_back_to_default() -> None:
    settings = load_settings(_required_env(**{ENV_TOKEN_SWEEPER_INTERVAL: ""}))
    assert settings.token_sweeper_interval_seconds == DEFAULT_TOKEN_SWEEPER_INTERVAL_SECONDS


def test_token_sweeper_interval_rejects_negative_value() -> None:
    with pytest.raises(SettingsError, match="non-negative"):
        load_settings(_required_env(**{ENV_TOKEN_SWEEPER_INTERVAL: "-1"}))


def test_token_sweeper_interval_rejects_non_integer_value() -> None:
    with pytest.raises(SettingsError, match=ENV_TOKEN_SWEEPER_INTERVAL):
        load_settings(_required_env(**{ENV_TOKEN_SWEEPER_INTERVAL: "not-a-number"}))


# ---------------------------------------------------------------------------
# Phase G — call-context signer settings (AS-IMPL-017)
# ---------------------------------------------------------------------------


def test_call_context_settings_default_when_env_unset() -> None:
    settings = load_settings(_required_env())
    assert settings.call_context_key_ref == ""
    assert settings.call_context_secret_store == DEFAULT_CALL_CONTEXT_SECRET_STORE
    assert settings.call_context_audience == DEFAULT_CALL_CONTEXT_AUDIENCE
    assert settings.call_context_ttl_seconds == DEFAULT_CALL_CONTEXT_TTL_SECONDS
    assert settings.call_context_key_rotation_seconds == DEFAULT_CALL_CONTEXT_KEY_ROTATION_SECONDS


def test_call_context_key_ref_is_carried_when_provided() -> None:
    settings = load_settings(
        _required_env(**{ENV_CALL_CONTEXT_KEY_REF: "call-context-key"}),
    )
    assert settings.call_context_key_ref == "call-context-key"


def test_call_context_secret_store_override() -> None:
    settings = load_settings(
        _required_env(**{ENV_CALL_CONTEXT_SECRET_STORE: "vault"}),
    )
    assert settings.call_context_secret_store == "vault"


def test_call_context_audience_override() -> None:
    settings = load_settings(
        _required_env(**{ENV_CALL_CONTEXT_AUDIENCE: "custos.test"}),
    )
    assert settings.call_context_audience == "custos.test"


def test_call_context_ttl_override() -> None:
    settings = load_settings(
        _required_env(**{ENV_CALL_CONTEXT_TTL_SECONDS: "60"}),
    )
    assert settings.call_context_ttl_seconds == 60


def test_call_context_ttl_rejects_zero() -> None:
    with pytest.raises(SettingsError, match="positive integer"):
        load_settings(_required_env(**{ENV_CALL_CONTEXT_TTL_SECONDS: "0"}))


def test_call_context_ttl_rejects_negative() -> None:
    with pytest.raises(SettingsError, match="positive integer"):
        load_settings(_required_env(**{ENV_CALL_CONTEXT_TTL_SECONDS: "-1"}))


def test_call_context_ttl_rejects_non_integer() -> None:
    with pytest.raises(SettingsError, match=ENV_CALL_CONTEXT_TTL_SECONDS):
        load_settings(_required_env(**{ENV_CALL_CONTEXT_TTL_SECONDS: "not-a-number"}))


def test_call_context_key_rotation_override() -> None:
    settings = load_settings(
        _required_env(**{ENV_CALL_CONTEXT_KEY_ROTATION: "3600"}),
    )
    assert settings.call_context_key_rotation_seconds == 3600


def test_call_context_key_rotation_accepts_zero_to_disable() -> None:
    settings = load_settings(
        _required_env(**{ENV_CALL_CONTEXT_KEY_ROTATION: "0"}),
    )
    assert settings.call_context_key_rotation_seconds == 0


def test_call_context_key_rotation_rejects_negative() -> None:
    with pytest.raises(SettingsError, match="non-negative"):
        load_settings(_required_env(**{ENV_CALL_CONTEXT_KEY_ROTATION: "-1"}))


def test_call_context_key_rotation_rejects_non_integer() -> None:
    with pytest.raises(SettingsError, match=ENV_CALL_CONTEXT_KEY_ROTATION):
        load_settings(_required_env(**{ENV_CALL_CONTEXT_KEY_ROTATION: "not-a-number"}))


def test_oidc_enabled_defaults_to_false() -> None:
    # Phase I (AS-IMPL-024) lands the OIDC callback as a stub; the
    # feature flag stays False so M1 deployments ship with the
    # endpoint mounted but disabled until Phase H wires the verifier.
    settings = load_settings(_required_env())
    assert settings.oidc_enabled is False


@pytest.mark.parametrize("raw", ["true", "True", "TRUE", "1", "yes", "on"])
def test_oidc_enabled_accepts_truthy_strings(raw: str) -> None:
    settings = load_settings(_required_env(**{ENV_OIDC_ENABLED: raw}))
    assert settings.oidc_enabled is True


@pytest.mark.parametrize("raw", ["false", "False", "FALSE", "0", "no", "off"])
def test_oidc_enabled_accepts_falsy_strings(raw: str) -> None:
    settings = load_settings(_required_env(**{ENV_OIDC_ENABLED: raw}))
    assert settings.oidc_enabled is False


def test_oidc_enabled_rejects_unknown_value() -> None:
    with pytest.raises(SettingsError, match=ENV_OIDC_ENABLED):
        load_settings(_required_env(**{ENV_OIDC_ENABLED: "maybe"}))


# ---------------------------------------------------------------------------
# AS-IMPL-031: CUSTOS_AUTH_INTERNAL_IDENTITY_MODE
# ---------------------------------------------------------------------------


def test_internal_identity_mode_defaults_to_jwt() -> None:
    """M1 deployments ship without the env var set; the parser must
    default to the signed-JWT path (the only fully wired verifier)."""
    settings = load_settings(_required_env())
    assert settings.internal_identity_mode == INTERNAL_IDENTITY_MODE_JWT


@pytest.mark.parametrize("raw", ["jwt", "JWT", "Jwt"])
def test_internal_identity_mode_accepts_jwt_case_insensitively(raw: str) -> None:
    settings = load_settings(_required_env(**{ENV_INTERNAL_IDENTITY_MODE: raw}))
    assert settings.internal_identity_mode == INTERNAL_IDENTITY_MODE_JWT


def test_internal_identity_mode_empty_string_falls_back_to_jwt() -> None:
    settings = load_settings(_required_env(**{ENV_INTERNAL_IDENTITY_MODE: ""}))
    assert settings.internal_identity_mode == INTERNAL_IDENTITY_MODE_JWT


def test_internal_identity_mode_spiffe_refuses_to_boot() -> None:
    """AS-IMPL-031: the SPIFFE/SPIRE verifier is the M3 target and is
    not implemented yet. Selecting ``spiffe`` must fail fast at
    settings parse so the operator sees a clear pointer to the
    cutover plan, rather than booting in a half-wired state."""
    with pytest.raises(SettingsError) as excinfo:
        load_settings(
            _required_env(**{ENV_INTERNAL_IDENTITY_MODE: INTERNAL_IDENTITY_MODE_SPIFFE}),
        )
    msg = str(excinfo.value)
    assert ENV_INTERNAL_IDENTITY_MODE in msg
    assert INTERNAL_IDENTITY_MODE_SPIFFE in msg
    assert "AS-IMPL-031" in msg
    assert "spiffe-cutover-plan" in msg


def test_internal_identity_mode_spiffe_case_insensitive_still_refuses() -> None:
    """The case-fold happens before the not-implemented check, so
    upper-case ``SPIFFE`` produces the same not-implemented error
    (not the unknown-value error)."""
    with pytest.raises(SettingsError, match="AS-IMPL-031"):
        load_settings(_required_env(**{ENV_INTERNAL_IDENTITY_MODE: "SPIFFE"}))


def test_internal_identity_mode_rejects_unknown_value() -> None:
    with pytest.raises(SettingsError) as excinfo:
        load_settings(_required_env(**{ENV_INTERNAL_IDENTITY_MODE: "mtls"}))
    msg = str(excinfo.value)
    assert ENV_INTERNAL_IDENTITY_MODE in msg
    # The error must enumerate the legal values so the operator can
    # self-correct without reading the source.
    assert INTERNAL_IDENTITY_MODE_JWT in msg
    assert INTERNAL_IDENTITY_MODE_SPIFFE in msg

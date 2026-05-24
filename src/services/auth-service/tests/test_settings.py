"""Tests for :mod:`custos_auth.settings` (AS-IMPL-004)."""

from __future__ import annotations

import pytest

from custos_auth.settings import (
    ENV_AUTH_STORE_DSN,
    ENV_AUTHZ_CACHE_TTL,
    ENV_METADATA_STORE_DSN,
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
    # Default tracks the design's "Authz (decision) … 60s" entry.
    settings = load_settings(_required_env())
    assert settings.authz_cache_ttl_seconds == 60
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

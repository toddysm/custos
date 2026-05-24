"""Tests for :mod:`custos_auth.settings` (AS-IMPL-004)."""

from __future__ import annotations

import pytest

from custos_auth.settings import (
    ENV_AUTH_STORE_DSN,
    ENV_METADATA_STORE_DSN,
    Settings,
    SettingsError,
    load_settings,
)


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

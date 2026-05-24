"""Tests for :mod:`custos_catalog.settings`."""

from __future__ import annotations

import pytest

from custos_catalog.settings import (
    DEFAULT_CEL_PARSE_TIMEOUT_MS,
    DEFAULT_PUBLISH_MAX_BODY_MB,
    SettingsError,
    load_settings,
)

_REQUIRED = {
    "CAT_DEFINITION_STORE": "postgresql://u:p@h:5432/def",
    "CAT_CATALOG_STORE": "postgresql://u:p@h:5432/cat",
    "CAT_METADATA_STORE": "postgresql://u:p@h:5432/meta",
    "CAT_CONNECTOR_ENDPOINT": "http://connector-service:8080",
}


def test_load_settings_populates_all_required_dsns_and_endpoint() -> None:
    cfg = load_settings(_REQUIRED)
    assert cfg.definition_store_dsn == _REQUIRED["CAT_DEFINITION_STORE"]
    assert cfg.catalog_store_dsn == _REQUIRED["CAT_CATALOG_STORE"]
    assert cfg.metadata_store_dsn == _REQUIRED["CAT_METADATA_STORE"]
    assert cfg.connector_endpoint == _REQUIRED["CAT_CONNECTOR_ENDPOINT"]


def test_authz_endpoint_empty_triggers_dev_shim() -> None:
    cfg = load_settings(_REQUIRED)
    assert cfg.authz_endpoint == ""
    assert cfg.use_callctx_dev_shim is True


def test_authz_endpoint_set_disables_dev_shim() -> None:
    cfg = load_settings({**_REQUIRED, "CAT_AUTHZ_ENDPOINT": "http://auth-service:8080"})
    assert cfg.use_callctx_dev_shim is False


def test_defaults_for_optional_numeric_settings() -> None:
    cfg = load_settings(_REQUIRED)
    assert cfg.publish_max_body_mb == DEFAULT_PUBLISH_MAX_BODY_MB
    assert cfg.cel_parse_timeout_ms == DEFAULT_CEL_PARSE_TIMEOUT_MS


def test_optional_numeric_settings_parse_int() -> None:
    cfg = load_settings(
        {
            **_REQUIRED,
            "CAT_PUBLISH_MAX_BODY_MB": "16",
            "CAT_CEL_PARSE_TIMEOUT_MS": "250",
        },
    )
    assert cfg.publish_max_body_mb == 16
    assert cfg.cel_parse_timeout_ms == 250


def test_non_int_numeric_setting_raises() -> None:
    with pytest.raises(SettingsError, match="must be an integer"):
        load_settings({**_REQUIRED, "CAT_PUBLISH_MAX_BODY_MB": "huge"})


def test_default_namespace_tier_vendor_is_optional() -> None:
    assert load_settings(_REQUIRED).default_namespace_tier_vendor is None
    cfg = load_settings({**_REQUIRED, "CAT_DEFAULT_NAMESPACE_TIER_VENDOR": "custos"})
    assert cfg.default_namespace_tier_vendor == "custos"


def test_missing_required_var_raises_with_actionable_message() -> None:
    incomplete = {k: v for k, v in _REQUIRED.items() if k != "CAT_DEFINITION_STORE"}
    with pytest.raises(SettingsError, match="CAT_DEFINITION_STORE is required"):
        load_settings(incomplete)


def test_environment_defaults_to_development() -> None:
    assert load_settings(_REQUIRED).environment == "development"
    assert load_settings(_REQUIRED).is_production is False


def test_environment_production_is_detected_case_insensitively() -> None:
    cfg = load_settings({**_REQUIRED, "ENVIRONMENT": "Production"})
    assert cfg.is_production is True

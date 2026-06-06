"""Tests for :mod:`custos_gateway.settings` (AGW-IMPL-002)."""

from __future__ import annotations

import pytest

from custos_gateway.settings import (
    DEFAULT_BODY_MAX_BYTES_DEFAULT,
    DEFAULT_BODY_MAX_BYTES_PUBLISH,
    DEFAULT_DAPR_HTTP_HOST,
    DEFAULT_DAPR_HTTP_PORT,
    DEFAULT_DEVICE_CODE_POLL_INTERVAL_SECONDS,
    DEFAULT_DEVICE_CODE_TTL_SECONDS,
    DEFAULT_ENVIRONMENT,
    DEFAULT_IDEMPOTENCY_TTL_SECONDS,
    DEFAULT_LISTEN_ADDR,
    DEFAULT_RATE_LIMIT_PRINCIPAL_WRITES_BURST,
    DEFAULT_RATE_LIMIT_PRINCIPAL_WRITES_RPS,
    DEFAULT_RATE_LIMIT_WORKSPACE_WRITES_BURST,
    DEFAULT_RATE_LIMIT_WORKSPACE_WRITES_RPS,
    SettingsError,
    load_settings,
)

from .conftest import minimal_gateway_env


def test_defaults_match_design_configuration_table() -> None:
    settings = load_settings(minimal_gateway_env())

    assert settings.listen_addr == DEFAULT_LISTEN_ADDR
    assert settings.body_max_bytes_default == DEFAULT_BODY_MAX_BYTES_DEFAULT
    assert settings.body_max_bytes_publish == DEFAULT_BODY_MAX_BYTES_PUBLISH
    assert settings.rate_limit_principal_writes_rps == DEFAULT_RATE_LIMIT_PRINCIPAL_WRITES_RPS
    assert settings.rate_limit_principal_writes_burst == DEFAULT_RATE_LIMIT_PRINCIPAL_WRITES_BURST
    assert settings.rate_limit_workspace_writes_rps == DEFAULT_RATE_LIMIT_WORKSPACE_WRITES_RPS
    assert settings.rate_limit_workspace_writes_burst == DEFAULT_RATE_LIMIT_WORKSPACE_WRITES_BURST
    assert settings.idempotency_ttl_seconds == DEFAULT_IDEMPOTENCY_TTL_SECONDS
    assert settings.device_code_ttl_seconds == DEFAULT_DEVICE_CODE_TTL_SECONDS
    assert settings.device_code_poll_interval_seconds == DEFAULT_DEVICE_CODE_POLL_INTERVAL_SECONDS
    assert settings.dapr_http_host == DEFAULT_DAPR_HTTP_HOST
    assert settings.dapr_http_port == DEFAULT_DAPR_HTTP_PORT
    assert settings.environment == DEFAULT_ENVIRONMENT
    assert settings.oidc_default_issuer == ""
    assert settings.device_code_enabled is False
    assert settings.is_production is False


def test_parses_required_and_overridden_values() -> None:
    env = minimal_gateway_env() | {
        "CUSTOS_GATEWAY_LISTEN_ADDR": ":9443",
        "CUSTOS_GATEWAY_BODY_MAX_BYTES_DEFAULT": "2048",
        "CUSTOS_GATEWAY_IDEMPOTENCY_TTL": "1h",
        "CUSTOS_GATEWAY_DEVICE_CODE_TTL": "30m",
        "CUSTOS_GATEWAY_DEVICE_CODE_POLL_INTERVAL": "10s",
        "CUSTOS_GATEWAY_OIDC_DEFAULT_ISSUER": "github",
        "DAPR_HTTP_HOST": "10.0.0.1",
        "DAPR_HTTP_PORT": "3600",
        "ENVIRONMENT": "production",
    }

    settings = load_settings(env)

    assert settings.tls_cert_ref == "secretref://tls/cert"
    assert settings.tls_key_ref == "secretref://tls/key"
    assert settings.cors_allowed_origins == ("https://ui.custos.example",)
    assert settings.listen_addr == ":9443"
    assert settings.body_max_bytes_default == 2048
    assert settings.idempotency_ttl_seconds == 3600
    assert settings.device_code_ttl_seconds == 30 * 60
    assert settings.device_code_poll_interval_seconds == 10
    assert settings.oidc_default_issuer == "github"
    assert settings.device_code_enabled is True
    assert settings.dapr_http_host == "10.0.0.1"
    assert settings.dapr_http_port == 3600
    assert settings.is_production is True


def test_bare_second_duration_is_accepted() -> None:
    env = minimal_gateway_env() | {"CUSTOS_GATEWAY_IDEMPOTENCY_TTL": "3600"}
    assert load_settings(env).idempotency_ttl_seconds == 3600


@pytest.mark.parametrize(
    "missing",
    [
        "CUSTOS_GATEWAY_TLS_CERT_REF",
        "CUSTOS_GATEWAY_TLS_KEY_REF",
        "CUSTOS_GATEWAY_CORS_ALLOWED_ORIGINS",
    ],
)
def test_missing_required_var_raises(missing: str) -> None:
    env = minimal_gateway_env()
    del env[missing]
    with pytest.raises(SettingsError, match=missing):
        load_settings(env)


def test_cors_must_be_json_array() -> None:
    env = minimal_gateway_env() | {"CUSTOS_GATEWAY_CORS_ALLOWED_ORIGINS": "not-json"}
    with pytest.raises(SettingsError, match="JSON array"):
        load_settings(env)


def test_cors_rejects_non_string_items() -> None:
    env = minimal_gateway_env() | {"CUSTOS_GATEWAY_CORS_ALLOWED_ORIGINS": "[1, 2]"}
    with pytest.raises(SettingsError, match="JSON array"):
        load_settings(env)


def test_cors_rejects_empty_list() -> None:
    env = minimal_gateway_env() | {"CUSTOS_GATEWAY_CORS_ALLOWED_ORIGINS": "[]"}
    with pytest.raises(SettingsError, match="at least one origin"):
        load_settings(env)


def test_cors_rejects_wildcard() -> None:
    env = minimal_gateway_env() | {"CUSTOS_GATEWAY_CORS_ALLOWED_ORIGINS": '["*"]'}
    with pytest.raises(SettingsError, match="wildcard"):
        load_settings(env)


def test_invalid_int_raises() -> None:
    env = minimal_gateway_env() | {"CUSTOS_GATEWAY_BODY_MAX_BYTES_DEFAULT": "abc"}
    with pytest.raises(SettingsError, match="positive integer"):
        load_settings(env)


def test_non_positive_int_raises() -> None:
    env = minimal_gateway_env() | {"CUSTOS_GATEWAY_RATE_LIMIT_PRINCIPAL_WRITES_RPS": "0"}
    with pytest.raises(SettingsError, match="positive integer"):
        load_settings(env)


def test_malformed_duration_raises() -> None:
    env = minimal_gateway_env() | {"CUSTOS_GATEWAY_IDEMPOTENCY_TTL": "5x"}
    with pytest.raises(SettingsError, match="duration"):
        load_settings(env)


def test_zero_duration_raises() -> None:
    env = minimal_gateway_env() | {"CUSTOS_GATEWAY_DEVICE_CODE_TTL": "0h"}
    with pytest.raises(SettingsError, match="positive duration"):
        load_settings(env)

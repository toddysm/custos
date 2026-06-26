"""Tests for :mod:`custos_connector.settings` (CONN-IMPL-003)."""

from __future__ import annotations

import pytest

from custos_connector.settings import (
    DEFAULT_HEALTH_CACHE_TTL_S,
    DEFAULT_LEASE_MAX_CONCURRENT,
    DEFAULT_OCI_REFERRERS_TIMEOUT_MS,
    DEFAULT_PUBLISH_MAX_BODY_MB,
    DEFAULT_PULL_LOOP_MIN_INTERVAL_SEC,
    DEFAULT_SIDECAR_DEFAULT_TTL,
    SettingsError,
    load_settings,
)

_MIN_ENV = {
    "CONN_CATALOG_STORE": "postgresql://u:p@h:5432/cat",
    "CONN_METADATA_STORE": "postgresql://u:p@h:5432/meta",
    "CONN_CATALOG_ENDPOINT": "http://catalog-service:8080",
}


def test_load_settings_applies_documented_defaults() -> None:
    s = load_settings(_MIN_ENV)
    assert s.catalog_store_dsn == "postgresql://u:p@h:5432/cat"
    assert s.metadata_store_dsn == "postgresql://u:p@h:5432/meta"
    assert s.catalog_endpoint == "http://catalog-service:8080"
    assert s.authz_endpoint == ""  # empty -> dev shim
    assert s.oci_referrers_timeout_ms == DEFAULT_OCI_REFERRERS_TIMEOUT_MS
    assert s.publish_max_body_mb == DEFAULT_PUBLISH_MAX_BODY_MB
    assert s.sidecar_default_ttl_sec == DEFAULT_SIDECAR_DEFAULT_TTL
    assert s.lease_max_concurrent == DEFAULT_LEASE_MAX_CONCURRENT
    assert s.pull_loop_min_interval_sec == DEFAULT_PULL_LOOP_MIN_INTERVAL_SEC
    assert s.health_cache_ttl_s == DEFAULT_HEALTH_CACHE_TTL_S
    assert s.sidecar_mtls_issuer is None
    assert s.environment == "development"
    assert s.use_callctx_dev_shim is True
    assert s.is_production is False


def test_use_callctx_dev_shim_flips_off_when_authz_endpoint_set() -> None:
    s = load_settings({**_MIN_ENV, "CONN_AUTHZ_ENDPOINT": "http://auth-service:8080"})
    assert s.use_callctx_dev_shim is False


def test_environment_production_case_insensitive() -> None:
    s = load_settings({**_MIN_ENV, "ENVIRONMENT": "Production"})
    assert s.is_production is True


@pytest.mark.parametrize(
    "missing",
    ["CONN_CATALOG_STORE", "CONN_METADATA_STORE", "CONN_CATALOG_ENDPOINT"],
)
def test_required_envs_raise(missing: str) -> None:
    env = {k: v for k, v in _MIN_ENV.items() if k != missing}
    with pytest.raises(SettingsError, match=missing):
        load_settings(env)


def test_int_envs_reject_non_integer() -> None:
    with pytest.raises(SettingsError, match="CONN_PUBLISH_MAX_BODY_MB"):
        load_settings({**_MIN_ENV, "CONN_PUBLISH_MAX_BODY_MB": "not-a-number"})


def test_pull_loop_floor_is_enforced() -> None:
    """The design pins ``>=10s`` for the pull-loop minimum interval."""
    with pytest.raises(SettingsError, match=">= 10"):
        load_settings({**_MIN_ENV, "CONN_PULL_LOOP_MIN_INTERVAL_SEC": "5"})


def test_pull_loop_floor_value_is_allowed() -> None:
    s = load_settings({**_MIN_ENV, "CONN_PULL_LOOP_MIN_INTERVAL_SEC": "10"})
    assert s.pull_loop_min_interval_sec == 10


def test_sidecar_mtls_issuer_blank_string_becomes_none() -> None:
    s = load_settings({**_MIN_ENV, "CONN_SIDECAR_MTLS_ISSUER": "   "})
    assert s.sidecar_mtls_issuer is None


def test_sidecar_mtls_issuer_propagates_when_set() -> None:
    s = load_settings({**_MIN_ENV, "CONN_SIDECAR_MTLS_ISSUER": "custos-internal-ca"})
    assert s.sidecar_mtls_issuer == "custos-internal-ca"


def test_health_cache_ttl_override() -> None:
    s = load_settings({**_MIN_ENV, "CONN_HEALTH_CACHE_TTL_S": "15"})
    assert s.health_cache_ttl_s == 15


def test_dapr_secret_store_defaults_to_chart_component_name() -> None:
    s = load_settings(_MIN_ENV)
    assert s.dapr_secret_store == "custos-secretstore"


def test_dapr_secret_store_override() -> None:
    s = load_settings({**_MIN_ENV, "CONN_DAPR_SECRET_STORE": "vault-store"})
    assert s.dapr_secret_store == "vault-store"


def test_dapr_secret_store_blank_falls_back_to_default() -> None:
    s = load_settings({**_MIN_ENV, "CONN_DAPR_SECRET_STORE": "   "})
    assert s.dapr_secret_store == "custos-secretstore"

"""Tests for SPL provider wiring from settings (OBS-IMPL-004).

Pin the identifier -> adapter resolution, the fail-fast on an unrecognised
identifier, the noop-without-URL contract, deferred-pool construction (no
socket), and clean lifespan shutdown.
"""

from __future__ import annotations

from dataclasses import replace

import pytest
from custos_loki.adapters import LokiLogQueryAdapter
from custos_pg.adapters.metadata import PgMetadataAdapter
from custos_prometheus.adapters import NoopMetricsAdapter, PrometheusMetricsAdapter
from custos_spl.errors import QueryUnsupported
from custos_spl.interfaces.log_query import LogQueryProvider
from custos_spl.interfaces.metadata_store import MetadataStoreProvider
from custos_spl.interfaces.metrics_query import MetricsQueryProvider

from custos_obs.providers import (
    NoopLogQueryAdapter,
    ProviderConfigError,
    Providers,
    aclose_providers,
    build_log_query_provider,
    build_metadata_store,
    build_metrics_query_provider,
    load_providers,
)
from custos_obs.settings import Settings, load_settings

_BACKEND_ENV = {
    "CUSTOS_LOG_QUERY_PROVIDER": "loki",
    "CUSTOS_LOKI_URL": "http://loki:3100",
    "CUSTOS_METRICS_QUERY_PROVIDER": "prometheus",
    "CUSTOS_PROMETHEUS_URL": "http://prometheus:9090",
    "CUSTOS_OBS_METADATA_STORE_DSN": "postgresql://obs:obs@postgres:5432/obs",
}

_NOOP_ENV = {
    "CUSTOS_LOG_QUERY_PROVIDER": "noop",
    "CUSTOS_LOGS_EXTERNAL_URL": "https://logs.example.com",
    "CUSTOS_METRICS_QUERY_PROVIDER": "noop",
    "CUSTOS_METRICS_EXTERNAL_URL": "https://metrics.example.com",
    "CUSTOS_OBS_METADATA_STORE_DSN": "postgresql://obs:obs@postgres:5432/obs",
}


def _backend_settings() -> Settings:
    return load_settings(_BACKEND_ENV)


def _noop_settings() -> Settings:
    return load_settings(_NOOP_ENV)


# --- Log query provider ------------------------------------------------------


def test_loki_identifier_builds_loki_adapter() -> None:
    provider = build_log_query_provider(_backend_settings())
    assert isinstance(provider, LokiLogQueryAdapter)
    assert provider.base_url == "http://loki:3100"
    assert isinstance(provider, LogQueryProvider)


def test_noop_log_identifier_builds_noop_adapter() -> None:
    provider = build_log_query_provider(_noop_settings())
    assert isinstance(provider, NoopLogQueryAdapter)
    assert isinstance(provider, LogQueryProvider)


def test_unknown_log_identifier_is_rejected() -> None:
    settings = replace(_noop_settings(), log_query_provider="splunk")
    with pytest.raises(ProviderConfigError, match="unknown LogQueryProvider identifier 'splunk'"):
        build_log_query_provider(settings)


# --- Metrics query provider --------------------------------------------------


def test_prometheus_identifier_builds_prometheus_adapter() -> None:
    provider = build_metrics_query_provider(_backend_settings())
    assert isinstance(provider, PrometheusMetricsAdapter)
    assert provider.base_url == "http://prometheus:9090"
    assert isinstance(provider, MetricsQueryProvider)


def test_noop_metrics_identifier_builds_noop_adapter() -> None:
    provider = build_metrics_query_provider(_noop_settings())
    assert isinstance(provider, NoopMetricsAdapter)
    assert isinstance(provider, MetricsQueryProvider)


def test_unknown_metrics_identifier_is_rejected() -> None:
    settings = replace(_noop_settings(), metrics_query_provider="datadog")
    with pytest.raises(
        ProviderConfigError, match="unknown MetricsQueryProvider identifier 'datadog'"
    ):
        build_metrics_query_provider(settings)


# --- Metadata store ----------------------------------------------------------


def test_metadata_store_is_deferred_pg_adapter() -> None:
    adapter = build_metadata_store(_backend_settings())
    assert isinstance(adapter, PgMetadataAdapter)
    assert isinstance(adapter, MetadataStoreProvider)


# --- load_providers ----------------------------------------------------------


def test_load_providers_wires_all_three_backends() -> None:
    providers = load_providers(_backend_settings())
    assert isinstance(providers, Providers)
    assert isinstance(providers.metadata_store, PgMetadataAdapter)
    assert isinstance(providers.log_query, LokiLogQueryAdapter)
    assert isinstance(providers.metrics_query, PrometheusMetricsAdapter)


def test_load_providers_wires_noop_without_backend_urls() -> None:
    providers = load_providers(_noop_settings())
    assert isinstance(providers.log_query, NoopLogQueryAdapter)
    assert isinstance(providers.metrics_query, NoopMetricsAdapter)


# --- aclose_providers --------------------------------------------------------


async def test_aclose_providers_is_a_noop() -> None:
    # At this phase no provider holds a persistent resource (per-request HTTP;
    # the pg pool is reclaimed at process exit), so closing must not raise.
    providers = load_providers(_backend_settings())
    await aclose_providers(providers)


# --- NoopLogQueryAdapter behaviour ------------------------------------------


async def test_noop_log_adapter_query_run_logs_raises() -> None:
    adapter = NoopLogQueryAdapter()
    with pytest.raises(QueryUnsupported, match="logs not configured"):
        await adapter.query_run_logs("ws", "run", filter=None)  # type: ignore[arg-type]


async def test_noop_log_adapter_query_step_logs_raises() -> None:
    adapter = NoopLogQueryAdapter()
    with pytest.raises(QueryUnsupported, match="logs not configured"):
        await adapter.query_step_logs("ws", "run", "step", filter=None)  # type: ignore[arg-type]


def test_noop_log_adapter_tail_run_logs_raises() -> None:
    adapter = NoopLogQueryAdapter()
    with pytest.raises(QueryUnsupported, match="logs not configured"):
        adapter.tail_run_logs("ws", "run")  # type: ignore[arg-type]

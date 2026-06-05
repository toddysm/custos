"""Unit tests for the Observability and Audit Service settings loader (OBS-IMPL-002).

Every conditional-requirement branch and the documented defaults are pinned
here so misconfigured provider/backend combinations fail fast at startup with a
clear, named error.
"""

from __future__ import annotations

import pytest

from custos_obs.settings import (
    DEFAULT_ALERT_RULES_CONFIGMAP,
    DEFAULT_AUDIT_OUTBOX_DRAIN_MODE,
    DEFAULT_AUDIT_OUTBOX_LAG_THRESHOLD,
    DEFAULT_AUDIT_OUTBOX_POLL_INTERVAL_S,
    DEFAULT_AUDIT_OUTBOX_RETENTION_MARGIN_S,
    DEFAULT_AUDIT_RETENTION_DAYS,
    DEFAULT_AUDIT_RETENTION_SWEEP_INTERVAL_S,
    DEFAULT_ENVIRONMENT,
    DEFAULT_LOG_QUERY_PROVIDER,
    DEFAULT_METRICS_QUERY_PROVIDER,
    DEFAULT_OTEL_COLLECTOR_CONFIGMAP,
    DEFAULT_OTEL_EXPORTERS_CONFIGMAP,
    DEFAULT_SMTP_PORT,
    Settings,
    SettingsError,
    load_settings,
)


def _base_env(**overrides: str) -> dict[str, str]:
    """A minimal env that loads cleanly with the in-cluster defaults."""
    env = {
        "CUSTOS_LOKI_URL": "http://loki:3100",
        "CUSTOS_PROMETHEUS_URL": "http://prometheus:9090",
        "CUSTOS_OBS_METADATA_STORE_DSN": "postgresql://obs:obs@postgres:5432/obs",
    }
    env.update(overrides)
    return env


# --- Happy path / defaults ---------------------------------------------------


def test_defaults_match_design_table() -> None:
    settings = load_settings(_base_env())
    # Pin both the loaded value and the literal design-table default so an
    # accidental change to either the constant or the loader is caught.
    assert settings.log_query_provider == DEFAULT_LOG_QUERY_PROVIDER == "loki"
    assert settings.metrics_query_provider == DEFAULT_METRICS_QUERY_PROVIDER == "prometheus"
    assert settings.audit_retention_days == DEFAULT_AUDIT_RETENTION_DAYS == 90
    assert (
        settings.audit_retention_sweep_interval_s
        == DEFAULT_AUDIT_RETENTION_SWEEP_INTERVAL_S
        == 3_600
    )
    assert settings.audit_outbox_drain_mode == DEFAULT_AUDIT_OUTBOX_DRAIN_MODE == "listen"
    assert settings.audit_outbox_poll_interval_s == DEFAULT_AUDIT_OUTBOX_POLL_INTERVAL_S == 5
    assert (
        settings.audit_outbox_retention_margin_s
        == DEFAULT_AUDIT_OUTBOX_RETENTION_MARGIN_S
        == 86_400
    )
    assert settings.audit_outbox_lag_threshold == DEFAULT_AUDIT_OUTBOX_LAG_THRESHOLD == 1_000
    assert settings.otel_collector_configmap == DEFAULT_OTEL_COLLECTOR_CONFIGMAP
    assert settings.otel_collector_configmap == "custos-otel-collector-config"
    assert settings.otel_exporters_configmap == DEFAULT_OTEL_EXPORTERS_CONFIGMAP
    assert settings.otel_exporters_configmap == "custos-otel-exporters"
    assert settings.alert_rules_configmap == DEFAULT_ALERT_RULES_CONFIGMAP == "custos-alert-rules"
    assert settings.alert_webhook_urls == ()
    assert settings.smtp_port == DEFAULT_SMTP_PORT == 587
    assert settings.smtp_configured is False
    assert settings.environment == DEFAULT_ENVIRONMENT == "development"
    assert settings.is_production is False


def test_settings_is_frozen() -> None:
    settings = load_settings(_base_env())
    with pytest.raises((AttributeError, TypeError)):
        settings.audit_retention_days = 30  # type: ignore[misc]


# --- Log provider conditional branches ---------------------------------------


def test_loki_provider_requires_loki_url() -> None:
    env = _base_env()
    del env["CUSTOS_LOKI_URL"]
    with pytest.raises(SettingsError, match="CUSTOS_LOKI_URL is required"):
        load_settings(env)


def test_loki_provider_sets_loki_url_and_no_external() -> None:
    settings = load_settings(_base_env())
    assert settings.loki_url == "http://loki:3100"
    assert settings.logs_external_url is None


def test_noop_log_provider_requires_external_url() -> None:
    env = _base_env(CUSTOS_LOG_QUERY_PROVIDER="noop")
    with pytest.raises(SettingsError, match="CUSTOS_LOGS_EXTERNAL_URL is required"):
        load_settings(env)


def test_noop_log_provider_with_external_url() -> None:
    settings = load_settings(
        _base_env(
            CUSTOS_LOG_QUERY_PROVIDER="noop",
            CUSTOS_LOGS_EXTERNAL_URL="https://logs.example.com",
        )
    )
    assert settings.log_query_provider == "noop"
    assert settings.logs_external_url == "https://logs.example.com"
    assert settings.loki_url is None


def test_opensearch_log_provider_is_rejected_in_m1() -> None:
    with pytest.raises(SettingsError, match="CUSTOS_LOG_QUERY_PROVIDER must be one of"):
        load_settings(_base_env(CUSTOS_LOG_QUERY_PROVIDER="opensearch"))


# --- Metadata store DSN ------------------------------------------------------


def test_metadata_store_dsn_is_loaded() -> None:
    settings = load_settings(_base_env())
    assert settings.metadata_store_dsn == "postgresql://obs:obs@postgres:5432/obs"


def test_metadata_store_dsn_is_required() -> None:
    env = _base_env()
    del env["CUSTOS_OBS_METADATA_STORE_DSN"]
    with pytest.raises(SettingsError, match="CUSTOS_OBS_METADATA_STORE_DSN is required"):
        load_settings(env)


# --- Metrics provider conditional branches -----------------------------------


def test_prometheus_provider_requires_prometheus_url() -> None:
    env = _base_env()
    del env["CUSTOS_PROMETHEUS_URL"]
    with pytest.raises(SettingsError, match="CUSTOS_PROMETHEUS_URL is required"):
        load_settings(env)


def test_noop_metrics_provider_requires_external_url() -> None:
    env = _base_env(CUSTOS_METRICS_QUERY_PROVIDER="noop")
    with pytest.raises(SettingsError, match="CUSTOS_METRICS_EXTERNAL_URL is required"):
        load_settings(env)


def test_noop_metrics_provider_with_external_url() -> None:
    settings = load_settings(
        _base_env(
            CUSTOS_METRICS_QUERY_PROVIDER="noop",
            CUSTOS_METRICS_EXTERNAL_URL="https://metrics.example.com",
        )
    )
    assert settings.metrics_query_provider == "noop"
    assert settings.metrics_external_url == "https://metrics.example.com"
    assert settings.prometheus_url is None


def test_unknown_metrics_provider_is_rejected() -> None:
    with pytest.raises(SettingsError, match="CUSTOS_METRICS_QUERY_PROVIDER must be one of"):
        load_settings(_base_env(CUSTOS_METRICS_QUERY_PROVIDER="datadog"))


# --- Drain mode --------------------------------------------------------------


def test_poll_drain_mode_accepted() -> None:
    settings = load_settings(_base_env(CUSTOS_AUDIT_OUTBOX_DRAIN_MODE="poll"))
    assert settings.audit_outbox_drain_mode == "poll"


def test_invalid_drain_mode_rejected() -> None:
    with pytest.raises(SettingsError, match="CUSTOS_AUDIT_OUTBOX_DRAIN_MODE must be one of"):
        load_settings(_base_env(CUSTOS_AUDIT_OUTBOX_DRAIN_MODE="stream"))


# --- Integer knobs -----------------------------------------------------------


def test_audit_retention_days_override() -> None:
    settings = load_settings(_base_env(CUSTOS_AUDIT_RETENTION_DAYS="365"))
    assert settings.audit_retention_days == 365


def test_audit_retention_sweep_interval_override() -> None:
    settings = load_settings(_base_env(CUSTOS_AUDIT_RETENTION_SWEEP_INTERVAL_S="900"))
    assert settings.audit_retention_sweep_interval_s == 900


@pytest.mark.parametrize("bad", ["0", "-5", "abc", "1.5"])
def test_audit_retention_sweep_interval_rejects_non_positive_or_garbage(bad: str) -> None:
    with pytest.raises(
        SettingsError,
        match="CUSTOS_AUDIT_RETENTION_SWEEP_INTERVAL_S must be a positive integer",
    ):
        load_settings(_base_env(CUSTOS_AUDIT_RETENTION_SWEEP_INTERVAL_S=bad))


@pytest.mark.parametrize("bad", ["0", "-5", "abc", "1.5"])
def test_audit_retention_days_rejects_non_positive_or_garbage(bad: str) -> None:
    with pytest.raises(
        SettingsError, match="CUSTOS_AUDIT_RETENTION_DAYS must be a positive integer"
    ):
        load_settings(_base_env(CUSTOS_AUDIT_RETENTION_DAYS=bad))


def test_poll_interval_and_retention_margin_override() -> None:
    settings = load_settings(
        _base_env(
            CUSTOS_AUDIT_OUTBOX_POLL_INTERVAL_S="15",
            CUSTOS_AUDIT_OUTBOX_RETENTION_MARGIN="3600",
        )
    )
    assert settings.audit_outbox_poll_interval_s == 15
    assert settings.audit_outbox_retention_margin_s == 3600


def test_outbox_lag_threshold_override() -> None:
    settings = load_settings(_base_env(CUSTOS_AUDIT_OUTBOX_LAG_THRESHOLD="250"))
    assert settings.audit_outbox_lag_threshold == 250


@pytest.mark.parametrize("bad", ["0", "-5", "abc", "1.5"])
def test_outbox_lag_threshold_rejects_non_positive_or_garbage(bad: str) -> None:
    with pytest.raises(
        SettingsError, match="CUSTOS_AUDIT_OUTBOX_LAG_THRESHOLD must be a positive integer"
    ):
        load_settings(_base_env(CUSTOS_AUDIT_OUTBOX_LAG_THRESHOLD=bad))


# --- ConfigMap / webhook / SMTP knobs ----------------------------------------


def test_configmap_overrides() -> None:
    settings = load_settings(
        _base_env(
            CUSTOS_OTEL_COLLECTOR_CONFIGMAP="my-collector",
            CUSTOS_OTEL_EXPORTERS_CONFIGMAP="my-exporters",
            CUSTOS_ALERT_RULES_CONFIGMAP="my-rules",
        )
    )
    assert settings.otel_collector_configmap == "my-collector"
    assert settings.otel_exporters_configmap == "my-exporters"
    assert settings.alert_rules_configmap == "my-rules"


def test_alert_webhook_urls_are_split_and_trimmed() -> None:
    settings = load_settings(
        _base_env(CUSTOS_ALERT_WEBHOOK_URLS=" https://a.example , https://b.example ,, ")
    )
    assert settings.alert_webhook_urls == ("https://a.example", "https://b.example")


def test_smtp_configured_reports_host_presence() -> None:
    settings = load_settings(
        _base_env(
            CUSTOS_SMTP_HOST="smtp.example.com",
            CUSTOS_SMTP_PORT="2525",
            CUSTOS_SMTP_USERNAME="mailer",
            CUSTOS_SMTP_PASSWORD="secret",
            CUSTOS_SMTP_FROM="alerts@example.com",
        )
    )
    assert settings.smtp_configured is True
    assert settings.smtp_host == "smtp.example.com"
    assert settings.smtp_port == 2525
    assert settings.smtp_username == "mailer"
    assert settings.smtp_password == "secret"
    assert settings.smtp_from == "alerts@example.com"


def test_environment_production_flag() -> None:
    settings = load_settings(_base_env(ENVIRONMENT="Production"))
    assert settings.is_production is True


def test_load_settings_reads_os_environ_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CUSTOS_LOG_QUERY_PROVIDER", "noop")
    monkeypatch.setenv("CUSTOS_LOGS_EXTERNAL_URL", "https://logs.example.com")
    monkeypatch.setenv("CUSTOS_METRICS_QUERY_PROVIDER", "noop")
    monkeypatch.setenv("CUSTOS_METRICS_EXTERNAL_URL", "https://metrics.example.com")
    monkeypatch.setenv("CUSTOS_OBS_METADATA_STORE_DSN", "postgresql://obs/obs")
    settings = load_settings()
    assert isinstance(settings, Settings)
    assert settings.log_query_provider == "noop"
    assert settings.metrics_query_provider == "noop"
    assert settings.metadata_store_dsn == "postgresql://obs/obs"

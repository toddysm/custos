"""Tests for the typed ``TRIGGER_*`` settings loader (TS-IMPL-004)."""

from __future__ import annotations

import pytest

from custos_trigger.settings import (
    DEFAULT_DEDUP_TTL_SECONDS,
    DEFAULT_DISPATCH_MAX_RETRIES,
    DEFAULT_ENVIRONMENT,
    DEFAULT_NORMALIZED_TOPIC,
    DEFAULT_POLLER_DEFAULT_INTERVAL_SECONDS,
    DEFAULT_PUBSUB_COMPONENT,
    DEFAULT_RESUME_DEFAULT_TTL_SECONDS,
    DEFAULT_SCHEDULER_LEADER_LEASE_SECONDS,
    DEFAULT_WORKFLOW_EVENTS_TOPIC,
    Settings,
    SettingsError,
    load_settings,
)

#: The minimal env that satisfies every required knob.
_REQUIRED_ENV: dict[str, str] = {
    "TRIGGER_WEBHOOK_BASE_URL": "https://hooks.example.test",
    "TRIGGER_WF_ENDPOINT": "http://workflow-service:8080",
    "TRIGGER_CONNECTOR_ENDPOINT": "http://connector-service:8080",
    "TRIGGER_METADATA_STORE": "postgresql://u:p@db:5432/custos_trigger",
}


def _env(**overrides: str) -> dict[str, str]:
    return {**_REQUIRED_ENV, **overrides}


def test_defaults_applied_when_only_required_set() -> None:
    settings = load_settings(_env())

    assert isinstance(settings, Settings)
    assert settings.webhook_base_url == "https://hooks.example.test"
    assert settings.dedup_ttl_seconds == DEFAULT_DEDUP_TTL_SECONDS
    assert settings.poller_default_interval_seconds == DEFAULT_POLLER_DEFAULT_INTERVAL_SECONDS
    assert settings.resume_default_ttl_seconds == DEFAULT_RESUME_DEFAULT_TTL_SECONDS
    assert settings.dispatch_max_retries == DEFAULT_DISPATCH_MAX_RETRIES
    assert settings.scheduler_leader_lease_seconds == DEFAULT_SCHEDULER_LEADER_LEASE_SECONDS
    assert settings.pubsub_component == DEFAULT_PUBSUB_COMPONENT
    assert settings.normalized_topic == DEFAULT_NORMALIZED_TOPIC
    assert settings.workflow_events_topic == DEFAULT_WORKFLOW_EVENTS_TOPIC
    assert settings.environment == DEFAULT_ENVIRONMENT
    assert settings.authz_endpoint == ""
    assert settings.use_callctx_dev_shim is True
    assert settings.is_production is False


@pytest.mark.parametrize(
    ("env_var", "raw", "attr", "expected"),
    [
        ("TRIGGER_DEDUP_TTL_SECONDS", "120", "dedup_ttl_seconds", 120),
        ("TRIGGER_POLLER_DEFAULT_INTERVAL_SECONDS", "15", "poller_default_interval_seconds", 15),
        ("TRIGGER_RESUME_DEFAULT_TTL_SECONDS", "3600", "resume_default_ttl_seconds", 3600),
        ("TRIGGER_DISPATCH_MAX_RETRIES", "9", "dispatch_max_retries", 9),
        ("TRIGGER_SCHEDULER_LEADER_LEASE_SECONDS", "45", "scheduler_leader_lease_seconds", 45),
        ("TRIGGER_PUBSUB_COMPONENT", "my-pubsub", "pubsub_component", "my-pubsub"),
        ("TRIGGER_NORMALIZED_TOPIC", "custom.normalized", "normalized_topic", "custom.normalized"),
        (
            "TRIGGER_WORKFLOW_EVENTS_TOPIC",
            "custom.wf.events",
            "workflow_events_topic",
            "custom.wf.events",
        ),
        ("ENVIRONMENT", "production", "environment", "production"),
        (
            "TRIGGER_AUTHZ_ENDPOINT",
            "http://auth-service:8080",
            "authz_endpoint",
            "http://auth-service:8080",
        ),
    ],
)
def test_each_knob_is_overridable(env_var: str, raw: str, attr: str, expected: object) -> None:
    settings = load_settings(_env(**{env_var: raw}))
    assert getattr(settings, attr) == expected


@pytest.mark.parametrize(
    "env_var",
    [
        "TRIGGER_WEBHOOK_BASE_URL",
        "TRIGGER_WF_ENDPOINT",
        "TRIGGER_CONNECTOR_ENDPOINT",
        "TRIGGER_METADATA_STORE",
    ],
)
def test_missing_required_knob_raises(env_var: str) -> None:
    env = _env()
    del env[env_var]
    with pytest.raises(SettingsError, match=env_var):
        load_settings(env)


@pytest.mark.parametrize(
    "env_var",
    [
        "TRIGGER_WEBHOOK_BASE_URL",
        "TRIGGER_WF_ENDPOINT",
        "TRIGGER_CONNECTOR_ENDPOINT",
        "TRIGGER_METADATA_STORE",
    ],
)
def test_blank_required_knob_raises(env_var: str) -> None:
    with pytest.raises(SettingsError, match=env_var):
        load_settings(_env(**{env_var: "   "}))


def test_non_integer_knob_raises() -> None:
    with pytest.raises(SettingsError, match="TRIGGER_DEDUP_TTL_SECONDS"):
        load_settings(_env(TRIGGER_DEDUP_TTL_SECONDS="not-a-number"))


def test_negative_integer_knob_raises() -> None:
    with pytest.raises(SettingsError, match="TRIGGER_DISPATCH_MAX_RETRIES"):
        load_settings(_env(TRIGGER_DISPATCH_MAX_RETRIES="-1"))


def test_blank_optional_int_falls_back_to_default() -> None:
    settings = load_settings(_env(TRIGGER_DEDUP_TTL_SECONDS="  "))
    assert settings.dedup_ttl_seconds == DEFAULT_DEDUP_TTL_SECONDS


def test_blank_optional_string_falls_back_to_default() -> None:
    settings = load_settings(_env(TRIGGER_PUBSUB_COMPONENT="  ", ENVIRONMENT="  "))
    assert settings.pubsub_component == DEFAULT_PUBSUB_COMPONENT
    assert settings.environment == DEFAULT_ENVIRONMENT


def test_production_with_authz_endpoint_disables_dev_shim() -> None:
    settings = load_settings(
        _env(ENVIRONMENT="Production", TRIGGER_AUTHZ_ENDPOINT="http://auth-service:8080")
    )
    assert settings.is_production is True
    assert settings.use_callctx_dev_shim is False


def test_settings_is_frozen() -> None:
    settings = load_settings(_env())
    with pytest.raises(AttributeError):
        settings.dedup_ttl_seconds = 1  # type: ignore[misc]


def test_load_settings_reads_os_environ_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in _REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)
    settings = load_settings()
    assert settings.webhook_base_url == _REQUIRED_ENV["TRIGGER_WEBHOOK_BASE_URL"]

"""Receiver validation and deterministic schedule calculation contracts."""

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from custos_trigger.models import SubscriptionCreate
from custos_trigger.source_config import SourceConfig, SourceConfigurationError, next_schedule_fire

_NOW = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    ("kind", "config"),
    [
        ("manual", None),
        ("internal", None),
        ("scheduled", {"cron": "*/5 * * * *", "timezone": "America/New_York"}),
        ("scheduled", {"intervalSeconds": 1}),
        ("webhook", {"connectorInstanceId": "registry"}),
        ("vendor-push", {"connectorInstanceId": "registry"}),
        ("pull", {"connectorInstanceId": "registry", "pollIntervalSeconds": 10}),
    ],
)
def test_valid_source_combinations(kind: str, config: dict[str, object] | None) -> None:
    """Source configuration is additive for manual/internal and explicit for receivers."""
    model = SubscriptionCreate.model_validate(
        {"sourceType": kind, "workflowId": "wf", "sourceConfig": config}
    )
    assert model.source_type.value == kind


@pytest.mark.parametrize(
    ("kind", "config"),
    [
        ("scheduled", None),
        ("webhook", None),
        ("vendor-push", None),
        ("pull", None),
        ("manual", {}),
        ("internal", {}),
        ("scheduled", {}),
        ("scheduled", {"cron": "* * * * *", "intervalSeconds": 30}),
        ("scheduled", {"intervalSeconds": 30, "timezone": "UTC"}),
        ("scheduled", {"cron": "* * * * *", "connectorInstanceId": "registry"}),
        ("scheduled", {"intervalSeconds": 30, "pollIntervalSeconds": 10}),
        ("webhook", {}),
        ("webhook", {"connectorInstanceId": "registry", "pollIntervalSeconds": 10}),
        ("vendor-push", {"connectorInstanceId": "registry", "cron": "* * * * *"}),
        ("pull", {"connectorInstanceId": "registry", "intervalSeconds": 30}),
        ("pull", {"connectorInstanceId": "registry", "timezone": "UTC"}),
    ],
)
def test_incompatible_source_combinations(kind: str, config: dict[str, object] | None) -> None:
    """Reject missing configuration and fields belonging to a different receiver."""
    with pytest.raises(ValidationError):
        SubscriptionCreate.model_validate(
            {"sourceType": kind, "workflowId": "wf", "sourceConfig": config}
        )


@pytest.mark.parametrize(
    "config",
    [
        {"cron": "* * * * * *"},
        {"cron": "invalid"},
        {"cron": "60 * * * *"},
        {"cron": "R * * * *"},
        {"cron": "H * * * *"},
        {"cron": "H(0-10) * * * *"},
        {"timezone": "Not/A_Zone"},
        {"timezone": "/etc/passwd"},
        {"timezone": ""},
        {"intervalSeconds": 0},
        {"intervalSeconds": -1},
        {"intervalSeconds": True},
        {"intervalSeconds": 1.0},
        {"intervalSeconds": "30"},
        {"pollIntervalSeconds": 9},
        {"pollIntervalSeconds": False},
        {"connectorInstanceId": "  "},
        {"unexpected": True},
    ],
)
def test_invalid_source_values(config: dict[str, object]) -> None:
    """Validate values strictly rather than coercing invalid operational settings."""
    with pytest.raises(ValidationError):
        SourceConfig.model_validate(config)


def test_interval_is_anchored_and_coalesces_missed_ticks() -> None:
    """Late calculation advances to the original UTC grid, never now-plus-interval."""
    config = SourceConfig(interval_seconds=30)
    assert next_schedule_fire(config, anchor=_NOW, after=_NOW) == _NOW + timedelta(seconds=30)
    assert next_schedule_fire(config, anchor=_NOW, after=_NOW + timedelta(seconds=95)) == (
        _NOW + timedelta(seconds=120)
    )


def test_cron_skips_nonexistent_and_includes_both_repeated_instants() -> None:
    """Daylight-saving transitions use valid local matches, compared as UTC instants."""
    fall = SourceConfig(cron="0 1 * * *", timezone="America/New_York")
    first = next_schedule_fire(fall, anchor=_NOW, after=datetime(2026, 11, 1, 4, 59, tzinfo=UTC))
    assert first == datetime(2026, 11, 1, 5, tzinfo=UTC)
    assert next_schedule_fire(fall, anchor=_NOW, after=first) == datetime(
        2026, 11, 1, 6, tzinfo=UTC
    )
    spring = SourceConfig(cron="30 2 * * *", timezone="America/New_York")
    assert next_schedule_fire(
        spring, anchor=_NOW, after=datetime(2026, 3, 8, 6, 59, tzinfo=UTC)
    ) == datetime(2026, 3, 9, 6, 30, tzinfo=UTC)


def test_cron_handles_half_hour_daylight_saving_changes() -> None:
    """The policy is based on instants, not an assumed one-hour offset change."""
    fall = SourceConfig(cron="45 1 * * *", timezone="Australia/Lord_Howe")
    first = next_schedule_fire(fall, anchor=_NOW, after=datetime(2026, 4, 4, 14, 44, tzinfo=UTC))
    assert first == datetime(2026, 4, 4, 14, 45, tzinfo=UTC)
    assert next_schedule_fire(fall, anchor=_NOW, after=first) == datetime(
        2026, 4, 4, 15, 15, tzinfo=UTC
    )
    assert next_schedule_fire(
        SourceConfig(cron="15 2 * * *", timezone="Australia/Lord_Howe"),
        anchor=_NOW,
        after=datetime(2026, 10, 3, 15, 29, tzinfo=UTC),
    ) == datetime(2026, 10, 4, 15, 15, tzinfo=UTC)


@pytest.mark.parametrize(
    "config",
    [SourceConfig(), SourceConfig(interval_seconds=10**30), SourceConfig(cron="0 0 30 2 *")],
)
def test_unrepresentable_schedule_fails_explicitly(config: SourceConfig) -> None:
    """Impossible dates and arithmetic overflow must not create success-shaped schedules."""
    with pytest.raises(SourceConfigurationError):
        next_schedule_fire(config, anchor=_NOW, after=_NOW)


def test_schedule_rejects_naive_timestamps() -> None:
    """Scheduling never guesses the timezone of an unqualified timestamp."""
    with pytest.raises(SourceConfigurationError, match="timezone-aware"):
        next_schedule_fire(
            SourceConfig(interval_seconds=30), anchor=_NOW, after=datetime(2026, 1, 1)
        )

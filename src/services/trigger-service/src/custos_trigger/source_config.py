"""Typed receiver configuration and deterministic initial schedule computation."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import Self
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import CroniterBadDateError, croniter
from custos_spl.errors import BackendUnavailable
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.connector_instance_store import ConnectorReferenceReader
from custos_spl.interfaces.trigger_source import (
    MIN_POLL_INTERVAL_SECONDS,
    DeliveryMode,
    ResumeSource,
)
from pydantic import Field, StrictInt, model_validator

from custos_trigger._wire import WireModel
from custos_trigger.errors import TriggerError, TriggerErrorKind


class SourceConfigurationError(ValueError):
    """A receiver's source-specific fields do not form a supported configuration."""


class SourceConfig(WireModel):
    """Optional fields whose allowed combinations are determined by sourceType."""

    connector_instance_id: str | None = Field(default=None, min_length=1)
    cron: str | None = Field(default=None, min_length=1, max_length=256)
    timezone: str | None = None
    interval_seconds: StrictInt | None = Field(default=None, gt=0)
    poll_interval_seconds: StrictInt | None = Field(default=None, ge=MIN_POLL_INTERVAL_SECONDS)

    @model_validator(mode="after")
    def validate_values(self) -> Self:
        """Reject invalid expressions, zones and blank connector identifiers."""
        if self.connector_instance_id is not None and not self.connector_instance_id.strip():
            raise SourceConfigurationError("connectorInstanceId must not be blank")
        if self.cron is not None and (
            len(self.cron.split()) != 5
            or re.search(r"(?<![A-Z])[HR](?:\b|\()", self.cron.upper()) is not None
            or not croniter.is_valid(self.cron)
        ):
            raise SourceConfigurationError("cron must be a valid five-field expression")
        if self.timezone is not None:
            try:
                ZoneInfo(self.timezone)
            except (ZoneInfoNotFoundError, ValueError) as exc:
                raise SourceConfigurationError("timezone must be a valid IANA zone") from exc
        return self


def validate_source_config(
    source_type: str, config: SourceConfig | None, *, required: bool = True
) -> None:
    """Validate the source tag and fields without coercing legacy/manual requests."""
    if source_type in ("manual", "internal"):
        if config is not None:
            raise SourceConfigurationError(
                f"{source_type} subscriptions do not accept sourceConfig"
            )
        return
    if config is None:
        if required:
            raise SourceConfigurationError(f"{source_type} subscriptions require sourceConfig")
        return
    if source_type == "scheduled":
        if config.connector_instance_id is not None or config.poll_interval_seconds is not None:
            raise SourceConfigurationError(
                "scheduled sources cannot reference a connector or poller"
            )
        if (config.cron is None) == (config.interval_seconds is None):
            raise SourceConfigurationError(
                "scheduled sources require exactly one of cron or intervalSeconds"
            )
        if config.interval_seconds is not None and config.timezone is not None:
            raise SourceConfigurationError("timezone is only supported for cron schedules")
        return
    if source_type not in ("webhook", "vendor-push", "pull"):
        raise SourceConfigurationError("unsupported sourceType")
    if config.connector_instance_id is None:
        raise SourceConfigurationError("connector-backed sources require connectorInstanceId")
    if (
        config.cron is not None
        or config.interval_seconds is not None
        or config.timezone is not None
    ):
        raise SourceConfigurationError("connector-backed sources cannot contain schedule fields")
    if source_type != "pull" and config.poll_interval_seconds is not None:
        raise SourceConfigurationError("pollIntervalSeconds is only supported for pull sources")


class ResumeSourceWire(WireModel):
    """CamelCase wire model for the shared immutable external-resume association."""

    connector_instance_id: str = Field(min_length=1)
    delivery_modes: tuple[DeliveryMode, ...]
    poll_interval_seconds: StrictInt | None = None

    @model_validator(mode="after")
    def validate_association(self) -> Self:
        """Apply shared source invariants and canonicalize delivery-mode order."""
        association = self.to_source()
        self.delivery_modes = association.delivery_modes
        return self

    def to_source(self) -> ResumeSource:
        """Return the hashable association used for persistence and replay comparisons."""
        return ResumeSource(
            self.connector_instance_id, self.delivery_modes, self.poll_interval_seconds
        )


def next_schedule_fire(config: SourceConfig, *, anchor: datetime, after: datetime) -> datetime:
    """Compute the first eligible UTC instant after the cutoff.

    Intervals remain anchored to subscription creation. Cron uses the declared
    zone, includes both repeated-hour instants, and skips parser-adjusted times
    that do not match the expression (nonexistent local times).
    """
    if anchor.utcoffset() is None or after.utcoffset() is None:
        raise SourceConfigurationError("schedule timestamps must be timezone-aware")
    anchor, after = anchor.astimezone(UTC), after.astimezone(UTC)
    if config.interval_seconds is not None:
        try:
            interval = timedelta(seconds=config.interval_seconds)
            periods = max(0, (after - anchor) // interval) + 1
            return anchor + periods * interval
        except OverflowError as exc:
            raise SourceConfigurationError(
                "interval exceeds the supported timestamp range"
            ) from exc
    if config.cron is None:
        raise SourceConfigurationError("schedule has no cron or interval")
    zone = ZoneInfo(config.timezone or "UTC")
    try:
        cursor = croniter(config.cron, after.astimezone(zone))
        for _ in range(50):
            candidate = cursor.get_next(datetime)
            # Aware match() also accepts the parser's spring-gap adjustment.
            # Match wall-clock fields instead, then compare actual UTC instants.
            if candidate.astimezone(UTC) > after and croniter.match(
                config.cron, candidate.replace(tzinfo=None)
            ):
                return candidate.astimezone(UTC)
    except (CroniterBadDateError, OverflowError, ValueError) as exc:
        raise SourceConfigurationError(
            "cron has no eligible occurrence within its search budget"
        ) from exc
    raise SourceConfigurationError(
        "cron has no eligible local-time occurrence within its search budget"
    )


async def validate_connector_reference(
    reader: ConnectorReferenceReader | None, workspace_id: str, connector_instance_id: str
) -> None:
    """Validate only existence in the trusted workspace, without loading credentials.

    Listener capabilities and operator-enabled state remain Connector Service
    responsibilities; this check does not activate or resolve a connector.
    """
    if reader is None:
        raise TriggerError(
            TriggerErrorKind.SOURCE_UNAVAILABLE,
            "connector reference validation is not configured for this metadata backend",
        )
    try:
        exists = await reader.has_connector_instance(
            WorkspaceId(workspace_id), ConnectorInstanceId(connector_instance_id)
        )
    except BackendUnavailable as exc:
        raise TriggerError(
            TriggerErrorKind.SOURCE_UNAVAILABLE, "connector reference validation is unavailable"
        ) from exc
    if not exists:
        raise TriggerError(
            TriggerErrorKind.SOURCE_NOT_FOUND, "connector reference was not found in this workspace"
        )

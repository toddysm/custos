"""Shared source identity and representable duration contracts for resume registrations."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final, Literal

DeliveryMode = Literal["push", "pull"]
MIN_POLL_INTERVAL_SECONDS: Final[int] = 10
CONNECTOR_EXPRESSION_PATTERN: Final[str] = r"^\s*\$\{\{[\s\S]*\}\}\s*$"
_DURATION_PATTERN = re.compile(
    r"P(?:(?P<weeks>\d+)W|(?:(?P<days>\d+)D)?"
    r"(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?"
    r"(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?)"
)


def validate_connector_identifier(value: str) -> None:
    """Require a nonblank literal identifier, including when expression wrappers are padded."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("connectorInstanceId must be a non-empty string")
    if re.fullmatch(CONNECTOR_EXPRESSION_PATTERN, value):
        raise ValueError("connectorInstanceId must be a constant, not an expression")


def parse_resume_duration(value: str) -> timedelta:
    """Return a positive, representable ISO duration at Python datetime precision.

    Actual deadline overflow remains a runtime check because execution time is
    unknown at publication. Fractional digits round exactly to even microseconds.
    Impossible durations and values rounding to zero never become a default TTL.
    """
    match = _DURATION_PATTERN.fullmatch(value) if isinstance(value, str) else None
    if match is None:
        raise ValueError("duration must be a valid ISO-8601 duration")
    parts = match.groupdict()
    try:
        whole_seconds, _, fraction = (parts["seconds"] or "0").partition(".")
        microseconds = int((fraction + "000000")[:6])
        remainder = fraction[6:]
        # Round ties to even without converting the whole seconds to floating point.
        if remainder and (
            int(remainder[0]) > 5
            or (
                int(remainder[0]) == 5
                and (any(int(digit) != 0 for digit in remainder[1:]) or microseconds % 2 == 1)
            )
        ):
            microseconds += 1
        duration = timedelta(
            weeks=int(parts["weeks"] or 0),
            days=int(parts["days"] or 0),
            hours=int(parts["hours"] or 0),
            minutes=int(parts["minutes"] or 0),
            seconds=int(whole_seconds),
            microseconds=microseconds,
        )
    except (OverflowError, ValueError) as exc:
        raise ValueError("duration exceeds the supported range") from exc
    if duration <= timedelta(0):
        raise ValueError("duration must be positive at microsecond precision")
    if duration > datetime.max - datetime.min:
        raise ValueError("duration exceeds the supported datetime range")
    return duration


@dataclass(frozen=True, slots=True)
class ResumeSource:
    """Canonical source identity shared by Workflow and Trigger wire adapters.

    Workspace is supplied separately from the trusted run context, never by
    this source declaration. Delivery modes are an unordered, nonempty subset;
    serialization orders them consistently for replay comparisons.
    """

    connector_instance_id: str
    delivery_modes: tuple[DeliveryMode, ...]
    poll_interval_seconds: int | None = None

    def __post_init__(self) -> None:
        """Validate identity/modes and canonicalize a hashable replay-safe association."""
        validate_connector_identifier(self.connector_instance_id)
        if (
            not isinstance(self.delivery_modes, (list, tuple))
            or not self.delivery_modes
            or any(mode not in ("push", "pull") for mode in self.delivery_modes)
        ):
            raise ValueError("deliveryModes must be a non-empty subset of push and pull")
        if len(set(self.delivery_modes)) != len(self.delivery_modes):
            raise ValueError("deliveryModes must not contain duplicates")
        if self.poll_interval_seconds is not None:
            if (
                type(self.poll_interval_seconds) is not int
                or self.poll_interval_seconds < MIN_POLL_INTERVAL_SECONDS
            ):
                raise ValueError("pollIntervalSeconds must be an integer of at least 10")
            if "pull" not in self.delivery_modes:
                raise ValueError("pollIntervalSeconds requires pull delivery")
        object.__setattr__(self, "delivery_modes", tuple(sorted(self.delivery_modes)))

    def to_wire(self) -> dict[str, object]:
        """Serialize only declared source fields using the version-1 camelCase contract."""
        result: dict[str, object] = {
            "connectorInstanceId": self.connector_instance_id,
            "deliveryModes": list(self.delivery_modes),
        }
        if self.poll_interval_seconds is not None:
            result["pollIntervalSeconds"] = self.poll_interval_seconds
        return result

    @classmethod
    def from_wire(cls, value: Mapping[str, object]) -> ResumeSource:
        """Decode a stored/wire association without coercing identifiers or interval types."""
        if not isinstance(value, Mapping):
            raise ValueError("source must be an object")
        unknown = set(value) - {"connectorInstanceId", "deliveryModes", "pollIntervalSeconds"}
        if unknown:
            raise ValueError(f"unsupported source fields: {', '.join(sorted(unknown))}")
        identifier = value.get("connectorInstanceId")
        modes = value.get("deliveryModes")
        interval = value.get("pollIntervalSeconds")
        if not isinstance(identifier, str) or not isinstance(modes, (list, tuple)):
            raise ValueError("source requires connectorInstanceId and deliveryModes")
        parsed_modes: list[DeliveryMode] = []
        for mode in modes:
            if mode not in ("push", "pull"):
                raise ValueError("deliveryModes only supports push and pull")
            parsed_modes.append("push" if mode == "push" else "pull")
        if interval is not None and type(interval) is not int:
            raise ValueError("pollIntervalSeconds must be an integer")
        return cls(identifier, tuple(parsed_modes), interval)


def validate_resume_context(
    workspace_id: str | None, source: ResumeSource | None, selector: str | None
) -> None:
    """Keep legacy waits unscoped and require selective, tenant-scoped external waits."""
    if source is None:
        if workspace_id is not None:
            raise ValueError("workspaceId requires a source association")
        return
    if not isinstance(source, ResumeSource):
        raise ValueError("source must be a validated ResumeSource association")
    if not isinstance(workspace_id, str) or not workspace_id.strip() or workspace_id == "_resume":
        raise ValueError("external source requires a non-reserved workspaceId")
    if not isinstance(selector, str) or not selector.strip():
        raise ValueError("external source requires a non-empty selector")


__all__ = [
    "CONNECTOR_EXPRESSION_PATTERN",
    "MIN_POLL_INTERVAL_SECONDS",
    "DeliveryMode",
    "ResumeSource",
    "parse_resume_duration",
    "validate_connector_identifier",
    "validate_resume_context",
]

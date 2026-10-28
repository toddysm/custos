"""Immutable source associations persisted with external resume registrations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, Literal

DeliveryMode = Literal["push", "pull"]
MIN_POLL_INTERVAL_SECONDS: Final[int] = 10


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
        if (
            not isinstance(self.connector_instance_id, str)
            or not self.connector_instance_id.strip()
        ):
            raise ValueError("connectorInstanceId must be a non-empty string")
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


__all__ = ["MIN_POLL_INTERVAL_SECONDS", "DeliveryMode", "ResumeSource", "validate_resume_context"]

"""Wire models for the inbound RPC surface (ARM-IMPL-018).

The Workflow Service invokes ARM over Dapr Service-Invocation with a
camelCase JSON envelope (design § Internal RPC). These Pydantic models parse
that envelope, reject malformed requests with a ``422`` at the HTTP boundary,
and translate it into the Scheduler's internal :class:`ScheduleRequest`.

Two deliberate boundary decisions:

* **Workspace from the call context, not the body.** The verified
  ``x-custos-callctx`` document is the source of truth for the tenant; the
  request body never carries ``workspaceId`` so a caller cannot schedule into
  a workspace it is not authorized for.
* **Connector contexts carry no secrets on the wire.** The wire form exposes
  only credential-free handles (``slotName``/``handle``/``connectorKind``);
  credential material reaches the activity through the sidecar token flow, not
  ``ScheduleActivity``. The translation therefore builds
  :class:`~custos_arm.secrets.ConnectorContext` instances with empty
  ``secrets`` and the handle as the refreshable ``lease_id``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from custos_arm.contract import StepRef
from custos_arm.scheduler import ScheduleRequest
from custos_arm.secrets import ConnectorContext

__all__ = [
    "CancelActivityWire",
    "ConnectorContextWire",
    "ScheduleActivityWire",
]


class ConnectorContextWire(BaseModel):
    """One pre-resolved connector slot as it appears on the wire.

    Mirrors the Workflow Service's ``BindForStep`` output: a credential-free
    handle the activity later exchanges for short-lived credentials through
    the sidecar. ``expiresAt`` is accepted for forward compatibility but is
    not consumed by the initial schedule.
    """

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    slot_name: str = Field(alias="slotName", min_length=1)
    handle: str = Field(min_length=1)
    connector_kind: str = Field(alias="connectorKind", min_length=1)
    expires_at: datetime | None = Field(default=None, alias="expiresAt")

    def to_connector_context(self) -> ConnectorContext:
        """Build the internal :class:`ConnectorContext` for this slot."""
        return ConnectorContext(
            slot_name=self.slot_name,
            connector_type=self.connector_kind,
            connector_instance_id=self.handle,
            secrets={},
            lease_id=self.handle,
        )


class ScheduleActivityWire(BaseModel):
    """The ``ScheduleActivity`` request envelope (design § Internal RPC)."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    run_id: str = Field(alias="runId", min_length=1)
    step_id: str = Field(alias="stepId", min_length=1)
    attempt: int = Field(ge=1)
    activity_ref: str = Field(alias="activityRef", min_length=1)
    inputs: dict[str, Any] = Field(default_factory=dict)
    connector_contexts: dict[str, ConnectorContextWire] = Field(
        default_factory=dict, alias="connectorContexts"
    )
    deadline: datetime | None = None

    def idempotency_key(self) -> str:
        """The canonical ``runId|stepId|attempt`` dedup key for this request."""
        return f"{self.run_id}|{self.step_id}|{self.attempt}"

    def to_schedule_request(self, *, workspace_id: str) -> ScheduleRequest:
        """Translate the wire envelope into the Scheduler's input.

        :param workspace_id: The tenant taken from the verified call context;
            never read from the request body.
        """
        step = StepRef.model_validate(
            {"runId": self.run_id, "stepId": self.step_id, "attempt": self.attempt}
        )
        contexts = tuple(value.to_connector_context() for value in self.connector_contexts.values())
        return ScheduleRequest(
            workspace_id=workspace_id,
            step=step,
            activity_ref=self.activity_ref,
            inputs=self.inputs,
            connector_contexts=contexts,
            step_deadline=self.deadline,
        )


class CancelActivityWire(BaseModel):
    """The ``CancelActivity`` request envelope (design § Internal RPC).

    Carries only the run/step coordinates; cancellation is idempotent and the
    workspace is taken from the verified call context.
    """

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    run_id: str = Field(alias="runId", min_length=1)
    step_id: str = Field(alias="stepId", min_length=1)

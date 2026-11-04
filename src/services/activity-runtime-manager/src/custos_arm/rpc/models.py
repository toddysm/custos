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
* **Versioned bindings preserve their opaque structure.** Legacy handles retain
  their existing translation. Version-two descriptors carry actual instance
  metadata without a fabricated lease or expiry. Their opaque context is treated
  as private and materialized under the slot's secret directory, not in inputs.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from urllib.parse import urlsplit

from custos_callctx.execution import ExecutionIdentity
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from custos_arm.contract import ConnectorRef, StepRef
from custos_arm.scheduler import ScheduleRequest
from custos_arm.secrets import ConnectorContext

__all__ = [
    "CancelActivityWire",
    "ConnectorContextWire",
    "ScheduleActivityWire",
]


def _endpoint_host(endpoint: str) -> str:
    """Extract public authority, rejecting credential-bearing or ambiguous endpoint forms."""
    try:
        parts = urlsplit(endpoint if "://" in endpoint else "//" + endpoint)
        if (
            not parts.hostname
            or parts.username is not None
            or parts.password is not None
            or parts.query
            or parts.fragment
            or any(character.isspace() or ord(character) < 32 for character in endpoint)
        ):
            raise ValueError
        _ = parts.port
        return parts.netloc
    except ValueError:
        raise ValueError("connector endpoint must have a credential-free authority") from None


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

    @model_validator(mode="before")
    @classmethod
    def reject_versioned_context(cls, value: Any) -> Any:
        """Do not downgrade malformed or unknown versioned descriptors into legacy handles."""
        if isinstance(value, dict) and {"schemaVersion", "schema_version"} & value.keys():
            raise ValueError("versioned bindings require the canonical descriptor")
        return value

    def to_connector_context(self) -> ConnectorContext:
        """Build the internal :class:`ConnectorContext` for this slot."""
        return ConnectorContext(
            slot_name=self.slot_name,
            connector_type=self.connector_kind,
            connector_instance_id=self.handle,
            secrets={},
            lease_id=self.handle,
        )


class BoundConnectorContextWire(BaseModel):
    """Canonical binding metadata; opaque data is not reinterpreted as a lease identifier."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")
    schema_version: Literal["2"] = Field(alias="schemaVersion")
    slot_name: str = Field(alias="slotName", min_length=1)
    connector_instance_id: str = Field(alias="connectorInstanceId", min_length=1)
    connector_kind: str = Field(alias="connectorKind", min_length=1)
    capabilities: list[str]
    context: dict[str, Any] = Field(repr=False)

    @field_validator("context")
    @classmethod
    def validate_context(cls, value: dict[str, Any]) -> dict[str, Any]:
        """Require the actual Connector shape without inventing credential or expiry fields."""
        if (
            set(value) != {"endpoint", "tokenTypeHint", "handle", "extras"}
            or not isinstance(value["endpoint"], str)
            or not value["endpoint"]
            or not isinstance(value["handle"], dict)
            or not isinstance(value["extras"], dict)
            or (value["tokenTypeHint"] is not None and not isinstance(value["tokenTypeHint"], str))
        ):
            raise ValueError("invalid canonical connector context")
        _endpoint_host(value["endpoint"])
        return value

    def to_connector_ref(self) -> ConnectorRef:
        """Expose only endpoint authority and registered family, never opaque binding data."""
        endpoint = self.context["endpoint"]
        return ConnectorRef(
            host=_endpoint_host(endpoint),
            endpoint=endpoint,
            type=self.connector_kind,
        )

    def to_connector_context(self) -> ConnectorContext:
        """Preserve real instance identity and opaque binding; no synthetic refresh lease exists."""
        return ConnectorContext(
            slot_name=self.slot_name,
            connector_type=self.connector_kind,
            connector_instance_id=self.connector_instance_id,
            binding=self.context,
            capabilities=tuple(self.capabilities),
        )


class ScheduleActivityWire(BaseModel):
    """The ``ScheduleActivity`` request envelope (design § Internal RPC)."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    run_id: str = Field(alias="runId", min_length=1)
    step_id: str = Field(alias="stepId", min_length=1)
    attempt: int = Field(ge=1)
    activity_ref: str = Field(alias="activityRef", min_length=1)
    inputs: dict[str, Any] = Field(default_factory=dict)
    connector_contexts: dict[str, ConnectorContextWire | BoundConnectorContextWire] = Field(
        default_factory=dict, alias="connectorContexts"
    )
    deadline: datetime | None = None
    execution_identity: dict[str, str] | None = Field(default=None, alias="executionIdentity")

    @field_validator("execution_identity")
    @classmethod
    def validate_execution_identity(cls, value: dict[str, str] | None) -> dict[str, str] | None:
        """Accept only a credential-free execution reference at the HTTP boundary."""
        if value is not None:
            ExecutionIdentity.from_dict(value)
        return value

    @field_validator("connector_contexts")
    @classmethod
    def validate_slot_keys(
        cls, value: dict[str, ConnectorContextWire | BoundConnectorContextWire]
    ) -> dict[str, ConnectorContextWire | BoundConnectorContextWire]:
        """Reject conflicting slot names before invoking the scheduler."""
        if any(name != context.slot_name for name, context in value.items()):
            raise ValueError("connector slot key does not match the binding")
        return value

    def idempotency_key(self) -> str:
        """The canonical ``runId|stepId|attempt`` dedup key for this request."""
        return f"{self.run_id}|{self.step_id}|{self.attempt}"

    def to_schedule_request(
        self, *, workspace_id: str, principal_id: str | None = None
    ) -> ScheduleRequest:
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
            principal_id=principal_id,
            execution_identity=(
                ExecutionIdentity.from_dict(self.execution_identity)
                if self.execution_identity is not None
                else None
            ),
            step=step,
            activity_ref=self.activity_ref,
            inputs=self.inputs,
            connector_contexts=contexts,
            connectors={
                name: value.to_connector_ref()
                for name, value in self.connector_contexts.items()
                if isinstance(value, BoundConnectorContextWire)
            },
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

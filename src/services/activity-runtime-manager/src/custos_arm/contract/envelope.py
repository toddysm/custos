"""The `inputs.json` / `ctx.json` / `outputs.json` envelopes (Activity Contract v1).

The orchestrator never speaks to activity code in-process: it writes these
JSON envelopes to a known filesystem location, starts the sandbox, and
reads the outputs envelope back when the activity exits.

The ``inputs`` and ``outputs`` payloads themselves are carried as free-form
JSON (shaped by each activity's declared JSON Schema, validated separately);
these models pin only the **envelope** structure.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import Field, model_validator

from custos_arm.contract._base import ContractModel
from custos_arm.contract.errors import ErrorEnvelope
from custos_arm.contract.types import ArtifactRef, ConnectorRef

#: The contract / schema version this implementation speaks.
CONTRACT_VERSION: str = "1"
SCHEMA_VERSION: str = "1"


class ActivitySpec(ContractModel):
    """The ``(type, version)`` coordinates of the activity being executed."""

    type: str = Field(..., min_length=1)
    version: str = Field(..., min_length=1)


class StepRef(ContractModel):
    """The orchestrator step coordinates for a single attempt."""

    run_id: str = Field(..., alias="runId", min_length=1)
    step_id: str = Field(..., alias="stepId", min_length=1)
    attempt: int = Field(..., ge=1)


class InputsEnvelope(ContractModel):
    """The ``/custos/in/inputs.json`` envelope ARM writes for the activity."""

    schema_version: str = Field(default=SCHEMA_VERSION, alias="schemaVersion")
    contract_version: str = Field(default=CONTRACT_VERSION, alias="contractVersion")
    activity: ActivitySpec
    step: StepRef
    inputs: dict[str, Any] = Field(default_factory=dict)


class CtxEnvelope(ContractModel):
    """The ``/custos/in/ctx.json`` execution context ARM writes for the activity.

    Carries the step coordinates, the activity identity, the connector
    handles (credential-free — secrets are mounted separately under
    ``/custos/in/secrets/``), and the attempt deadline.
    """

    schema_version: str = Field(default=SCHEMA_VERSION, alias="schemaVersion")
    contract_version: str = Field(default=CONTRACT_VERSION, alias="contractVersion")
    run_id: str = Field(..., alias="runId", min_length=1)
    step_id: str = Field(..., alias="stepId", min_length=1)
    attempt: int = Field(..., ge=1)
    workspace_id: str = Field(..., alias="workspaceId", min_length=1)
    activity: ActivitySpec
    connectors: dict[str, ConnectorRef] = Field(default_factory=dict)
    deadline: datetime


class OutputsEnvelope(ContractModel):
    """The ``/custos/out/outputs.json`` envelope the activity writes.

    On success ``outputs`` carries the structured result and ``produced``
    (ARM-synthesized during finalization) enumerates uploaded artifacts. On
    failure ``error`` carries the structured failure and ``outputs`` is
    empty.
    """

    schema_version: str = Field(default=SCHEMA_VERSION, alias="schemaVersion")
    contract_version: str = Field(default=CONTRACT_VERSION, alias="contractVersion")
    status: Literal["success", "failure"]
    outputs: dict[str, Any] = Field(default_factory=dict)
    error: ErrorEnvelope | None = None
    #: ARM-synthesized; never written by the activity.
    produced: list[ArtifactRef] | None = None

    @model_validator(mode="after")
    def _check_status_consistency(self) -> OutputsEnvelope:
        if self.status == "failure":
            if self.error is None:
                raise ValueError("outputs.json with status='failure' requires an 'error' envelope")
        elif self.error is not None:
            raise ValueError(
                "outputs.json with status='success' must not carry an 'error' envelope"
            )
        return self


__all__ = [
    "CONTRACT_VERSION",
    "SCHEMA_VERSION",
    "ActivitySpec",
    "CtxEnvelope",
    "InputsEnvelope",
    "OutputsEnvelope",
    "StepRef",
]

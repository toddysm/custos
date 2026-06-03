"""Activity Manifest v1 models.

The manifest is the contract document for an activity: what inputs it
accepts, what outputs it produces, what connectors it needs, what runtime it
requires, what resources it wants, and what version of the contract it speaks
(design § Activity Manifest v1). v1 supports ``runtime.kind: oci-container``
only; ``http`` / ``wasm`` are reserved for later milestones.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from custos_arm.contract import Duration, ErrorClass
from custos_arm.manifest._base import (
    ManifestModel,
    is_capability_token,
    parse_semver,
)


class IsolationTier(StrEnum):
    """The sandbox isolation tiers, weakest to strongest."""

    PROCESS = "process"
    VM = "vm"
    MICROVM = "microvm"


class Determinism(StrEnum):
    """Whether an activity is pure (cacheable) or side-effecting."""

    PURE = "pure"
    SIDE_EFFECTING = "side-effecting"


class Idempotency(StrEnum):
    """Whether ARM may skip re-execution for an already-succeeded input."""

    BY_INPUT_HASH = "by-input-hash"
    NONE = "none"


class Metadata(ManifestModel):
    """The activity's identity and human-facing description."""

    type: str = Field(..., min_length=1)
    version: str = Field(..., min_length=1)
    namespace: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)
    labels: dict[str, str] = Field(default_factory=dict)
    owner: str = Field(..., min_length=1)

    @field_validator("version")
    @classmethod
    def _check_version(cls, value: str) -> str:
        parse_semver(value)
        return value


class Isolation(ManifestModel):
    """Sandbox isolation lower bound plus an optional concrete hint."""

    min_tier: IsolationTier | None = Field(default=None, alias="minTier")
    preferred: str | None = None


class Runtime(ManifestModel):
    """How the activity is packaged and isolated. v1: oci-container only."""

    kind: Literal["oci-container"]
    image: str = Field(..., min_length=1)
    digest: str = Field(..., min_length=1)
    isolation: Isolation | None = None


class InputsSpec(ManifestModel):
    """The activity's input JSON Schema (Draft 2020-12)."""

    json_schema: dict[str, Any] = Field(..., alias="schema")


class ArtifactSpec(ManifestModel):
    """A declared file output written to ``/custos/out/artifacts/<name>``."""

    name: str = Field(..., min_length=1)
    media_type: str = Field(..., alias="mediaType", min_length=1)
    required: bool


class OutputsSpec(ManifestModel):
    """The activity's output JSON Schema plus declared file artifacts."""

    json_schema: dict[str, Any] = Field(..., alias="schema")
    artifacts: list[ArtifactSpec] = Field(default_factory=list)


class ConnectorSpec(ManifestModel):
    """A connector slot the activity needs the workflow to bind."""

    name: str = Field(..., min_length=1)
    type: str = Field(..., min_length=1)
    required: bool
    capabilities: list[str] = Field(default_factory=list)

    @field_validator("capabilities")
    @classmethod
    def _check_capabilities(cls, value: list[str]) -> list[str]:
        for token in value:
            if not is_capability_token(token):
                raise ValueError(
                    f"capability {token!r} must be a dot-namespaced lowercase token "
                    "(e.g. 'oci.pull'); bare tokens and 'event.*' verbs are not allowed"
                )
        return value


class ResourceQuota(ManifestModel):
    """An optional request/limit pair for cpu or memory."""

    request: str | None = None
    limit: str | None = None


class EphemeralStorage(ManifestModel):
    """An optional ephemeral-storage limit."""

    limit: str | None = None


class Resources(ManifestModel):
    """Layered resource defaults; only ``timeout`` is required."""

    cpu: ResourceQuota | None = None
    memory: ResourceQuota | None = None
    ephemeral_storage: EphemeralStorage | None = Field(default=None, alias="ephemeralStorage")
    timeout: Duration


class ErrorSpec(ManifestModel):
    """A documented error code the activity may emit."""

    code: str = Field(..., min_length=1)
    error_class: ErrorClass = Field(..., alias="class")


class Spec(ManifestModel):
    """The activity's behavioral contract."""

    contract_version: str = Field(..., alias="contractVersion")
    runtime: Runtime
    inputs: InputsSpec
    outputs: OutputsSpec
    connectors: list[ConnectorSpec] = Field(default_factory=list)
    resources: Resources
    errors: list[ErrorSpec] = Field(default_factory=list)
    determinism: Determinism = Determinism.SIDE_EFFECTING
    idempotency: Idempotency = Idempotency.NONE

    @field_validator("contract_version")
    @classmethod
    def _check_contract_version(cls, value: str) -> str:
        if value != "1":
            raise ValueError(f"spec.contractVersion {value!r} is not supported; v1 requires '1'")
        return value


class ActivityManifest(ManifestModel):
    """A complete, parsed Activity Manifest v1 document."""

    api_version: Literal["custos.dev/v1"] = Field(..., alias="apiVersion")
    kind: Literal["ActivityManifest"]
    metadata: Metadata
    spec: Spec

    @model_validator(mode="after")
    def _check_artifact_refs_unique(self) -> ActivityManifest:
        names = [artifact.name for artifact in self.spec.outputs.artifacts]
        if len(names) != len(set(names)):
            raise ValueError("spec.outputs.artifacts[].name values must be unique")
        return self

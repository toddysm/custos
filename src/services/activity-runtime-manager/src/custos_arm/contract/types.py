"""Platform types for the Activity Contract v1 (design § Platform Types).

Activity authors compose inputs/outputs from these platform-defined types
so activities interoperate without each one reinventing the wheel.

Note that envelope ``inputs`` / ``outputs`` blocks are carried as free-form
JSON (``dict[str, Any]``); these models are the typed surface activity
authors and ARM construct/validate against, not a parser imposed on the
free-form payload.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import AfterValidator, Field

from custos_arm.contract._base import ContractModel, is_iso8601_duration


def _check_duration(value: str) -> str:
    if not is_iso8601_duration(value):
        raise ValueError(f"not a valid ISO-8601 duration: {value!r}")
    return value


#: An ISO-8601 duration string (e.g. ``PT30S``). Serializes as a bare JSON
#: string, not an object.
Duration = Annotated[str, AfterValidator(_check_duration)]


class ImageRef(ContractModel):
    """An OCI image reference.

    Always normalized to ``registry/repo[:tag][@digest]`` form at the ARM
    boundary.
    """

    ref: str = Field(..., min_length=1)
    digest: str | None = None


class OciDescriptor(ContractModel):
    """Mirrors the OCI distribution descriptor.

    The canonical "an artifact in a registry" shape that list/discover
    activities should emit and downstream activities should consume.
    """

    ref: str = Field(..., min_length=1)
    media_type: str = Field(..., alias="mediaType", min_length=1)
    digest: str = Field(..., min_length=1)
    size: int = Field(..., ge=0)
    artifact_type: str | None = Field(default=None, alias="artifactType")
    annotations: dict[str, str] | None = None


class ConnectorRef(ContractModel):
    """Opaque handle to a connector instance.

    Exposes the narrow, credential-free surface that expressions and
    activities may read. ARM resolves the handle to a full
    ``ConnectorContext`` (credentials included) only inside the sandbox.
    """

    host: str = Field(..., min_length=1)
    endpoint: str = Field(..., min_length=1)
    type: str = Field(..., min_length=1)
    labels: dict[str, str] = Field(default_factory=dict)


class ArtifactRef(ContractModel):
    """Opaque handle to a file produced by an activity.

    The activity writes ``{"kind": "ArtifactRef", "name": "<name>"}``,
    referencing a ``spec.outputs.artifacts[].name`` from its manifest. ARM
    expands the reference in place during two-phase output finalization,
    populating ``id`` / ``mediaType`` / ``digest`` / ``size``.
    """

    kind: Literal["ArtifactRef"] = "ArtifactRef"
    name: str = Field(..., min_length=1)
    id: str | None = None
    media_type: str | None = Field(default=None, alias="mediaType")
    digest: str | None = None
    size: int | None = Field(default=None, ge=0)


__all__ = [
    "ArtifactRef",
    "ConnectorRef",
    "Duration",
    "ImageRef",
    "OciDescriptor",
]

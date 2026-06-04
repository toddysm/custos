"""Tests for the I/O Broker (ARM-IMPL-009) — input/output two-phase finalization."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from copy import deepcopy
from typing import Any

import pytest
from custos_spl.errors import ArtifactNotFound
from custos_spl.ids import ArtifactId, WorkspaceId
from custos_spl.interfaces.artifact_store import ArtifactDescriptor, ArtifactStoreProvider

from custos_arm.contract.envelope import ActivitySpec, OutputsEnvelope, StepRef
from custos_arm.contract.errors import ErrorClass
from custos_arm.io import (
    InputSchemaViolationError,
    IOBroker,
    OutputInvalidArtifactRefError,
    OutputSchemaViolationError,
    OutputTooLargeError,
)
from custos_arm.manifest import ActivityManifest, parse_manifest
from custos_arm.store.artifact import ArtifactStoreClient

# ---------------------------------------------------------------------------
# Schemas + manifest fixtures (concrete JSON Schemas, no unresolvable $refs)
# ---------------------------------------------------------------------------

_INPUT_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["image"],
    "properties": {
        "image": {"type": "string"},
        "severity": {"type": "string", "enum": ["low", "medium", "high", "critical"]},
    },
}

_ARTIFACT_REF_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["kind", "name", "id", "digest", "size"],
    "properties": {
        "kind": {"const": "ArtifactRef"},
        "name": {"type": "string"},
        "id": {"type": "string"},
        "mediaType": {"type": "string"},
        "digest": {"type": "string"},
        "size": {"type": "integer"},
    },
}

_OUTPUT_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["findings", "reportRef"],
    "properties": {
        "findings": {"type": "integer"},
        "reportRef": _ARTIFACT_REF_SCHEMA,
    },
}

_BASE_MANIFEST: dict[str, Any] = {
    "apiVersion": "custos.dev/v1",
    "kind": "ActivityManifest",
    "metadata": {
        "type": "scan-image",
        "version": "1.2.0",
        "namespace": "custos.builtin",
        "description": "Scan an OCI image for vulnerabilities.",
        "owner": "custos-maintainers",
    },
    "spec": {
        "contractVersion": "1",
        "runtime": {
            "kind": "oci-container",
            "image": "ghcr.io/custos/scan-image:1.2.0",
            "digest": "sha256:abc",
        },
        "inputs": {"schema": _INPUT_SCHEMA},
        "outputs": {
            "schema": _OUTPUT_SCHEMA,
            "artifacts": [
                {"name": "report", "mediaType": "application/vnd.cyclonedx+json", "required": True}
            ],
        },
        "resources": {"timeout": "PT15M"},
    },
}


def _manifest(
    *,
    artifacts: list[dict[str, Any]] | None = None,
    output_schema: dict[str, Any] | None = None,
) -> ActivityManifest:
    raw = deepcopy(_BASE_MANIFEST)
    if artifacts is not None:
        raw["spec"]["outputs"]["artifacts"] = artifacts
    if output_schema is not None:
        raw["spec"]["outputs"]["schema"] = output_schema
    return parse_manifest(raw)


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _FakeArtifactStore:
    """Content-addressed in-memory stand-in for ``ArtifactStoreProvider``."""

    def __init__(self) -> None:
        self.blobs: dict[str, bytes] = {}

    async def put(
        self,
        workspace_id: WorkspaceId,
        content: AsyncIterator[bytes],
        media_type: str | None = None,
    ) -> ArtifactDescriptor:
        data = b""
        async for chunk in content:
            data += chunk
        digest = hashlib.sha256(data).hexdigest()
        artifact_id = f"{workspace_id}:{digest}"
        self.blobs[artifact_id] = data
        return ArtifactDescriptor(
            workspace_id=workspace_id,
            artifact_id=ArtifactId(artifact_id),
            digest=digest,
            media_type=media_type,
            size=len(data),
        )

    def get(self, workspace_id: WorkspaceId, artifact_id: ArtifactId) -> AsyncIterator[bytes]:
        return self._stream(artifact_id)

    async def _stream(self, artifact_id: str) -> AsyncIterator[bytes]:  # pragma: no cover - unused
        if artifact_id not in self.blobs:
            raise ArtifactNotFound(f"no such artifact {artifact_id}")
        yield self.blobs[artifact_id]


class _FakeReader:
    """In-memory view over the activity's produced ``/custos/out/artifacts/`` tree."""

    def __init__(self, files: dict[str, bytes]) -> None:
        self._files = files

    def has(self, name: str) -> bool:
        return name in self._files

    def open(self, name: str) -> AsyncIterator[bytes]:
        return self._aiter(self._files[name])

    async def _aiter(self, data: bytes) -> AsyncIterator[bytes]:
        for i in range(0, len(data), 8):
            yield data[i : i + 8]


def _broker(*, output_max_bytes: int = 1_048_576) -> IOBroker:
    store: ArtifactStoreProvider = _FakeArtifactStore()  # type: ignore[assignment]
    client = ArtifactStoreClient(store, max_bytes=5_368_709_120)
    return IOBroker(client, output_max_bytes=output_max_bytes)


def _step() -> StepRef:
    return StepRef(runId="run-1", stepId="step-1", attempt=1)


def _outputs_json(payload: dict[str, Any]) -> bytes:
    envelope = {"schemaVersion": "1", "contractVersion": "1", **payload}
    return json.dumps(envelope).encode("utf-8")


# ---------------------------------------------------------------------------
# Input side
# ---------------------------------------------------------------------------


def test_validate_inputs_accepts_valid() -> None:
    _broker().validate_inputs(
        inputs={"image": "ghcr.io/acme/app:v1", "severity": "high"},
        input_schema=_INPUT_SCHEMA,
    )


def test_validate_inputs_rejects_missing_required() -> None:
    with pytest.raises(InputSchemaViolationError) as exc:
        _broker().validate_inputs(inputs={"severity": "high"}, input_schema=_INPUT_SCHEMA)
    assert exc.value.code == "input.schema_violation"
    assert exc.value.error_class is ErrorClass.PERMANENT
    assert exc.value.issues


def test_materialize_inputs_builds_validated_envelope() -> None:
    envelope = _broker().materialize_inputs(
        activity=ActivitySpec(type="scan-image", version="1.2.0"),
        step=_step(),
        inputs={"image": "ghcr.io/acme/app:v1"},
        input_schema=_INPUT_SCHEMA,
    )
    assert envelope.activity.type == "scan-image"
    assert envelope.step.run_id == "run-1"
    assert envelope.inputs == {"image": "ghcr.io/acme/app:v1"}


def test_materialize_inputs_rejects_invalid() -> None:
    with pytest.raises(InputSchemaViolationError):
        _broker().materialize_inputs(
            activity=ActivitySpec(type="scan-image", version="1.2.0"),
            step=_step(),
            inputs={"severity": "nope"},
            input_schema=_INPUT_SCHEMA,
        )


# ---------------------------------------------------------------------------
# Output finalization — happy path
# ---------------------------------------------------------------------------


async def test_finalize_uploads_rewrites_and_synthesizes_produced() -> None:
    raw = _outputs_json(
        {
            "status": "success",
            "outputs": {
                "findings": 12,
                "reportRef": {"kind": "ArtifactRef", "name": "report"},
            },
        }
    )
    result = await _broker().finalize_outputs(
        raw_outputs=raw,
        manifest=_manifest(),
        step=_step(),
        workspace_id="ws-1",
        artifacts=_FakeReader({"report": b"<cyclonedx report bytes>"}),
    )
    assert isinstance(result, OutputsEnvelope)
    assert result.status == "success"
    ref = result.outputs["reportRef"]
    expected_digest = hashlib.sha256(b"<cyclonedx report bytes>").hexdigest()
    assert ref["id"] == f"ws-1:{expected_digest}"
    assert ref["digest"] == expected_digest
    assert ref["mediaType"] == "application/vnd.cyclonedx+json"
    assert ref["size"] == len(b"<cyclonedx report bytes>")
    assert result.produced is not None
    assert len(result.produced) == 1
    assert result.produced[0].name == "report"
    assert result.produced[0].id == f"ws-1:{expected_digest}"


async def test_finalize_rewrites_nested_and_list_refs() -> None:
    permissive_schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
    }
    raw = _outputs_json(
        {
            "status": "success",
            "outputs": {
                "findings": 3,
                "reports": [{"kind": "ArtifactRef", "name": "report"}],
                "meta": {"primary": {"kind": "ArtifactRef", "name": "report"}},
            },
        }
    )
    result = await _broker().finalize_outputs(
        raw_outputs=raw,
        manifest=_manifest(output_schema=permissive_schema),
        step=_step(),
        workspace_id="ws-1",
        artifacts=_FakeReader({"report": b"data"}),
    )
    assert result.outputs["reports"][0]["id"].startswith("ws-1:")
    assert result.outputs["meta"]["primary"]["id"].startswith("ws-1:")
    assert result.outputs["findings"] == 3


async def test_finalize_optional_artifact_not_produced_yields_empty_produced() -> None:
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["findings"],
        "properties": {"findings": {"type": "integer"}},
    }
    result = await _broker().finalize_outputs(
        raw_outputs=_outputs_json({"status": "success", "outputs": {"findings": 0}}),
        manifest=_manifest(
            artifacts=[{"name": "report", "mediaType": "application/json", "required": False}],
            output_schema=schema,
        ),
        step=_step(),
        workspace_id="ws-1",
        artifacts=_FakeReader({}),
    )
    assert result.produced == []


# ---------------------------------------------------------------------------
# Output finalization — failure + error paths
# ---------------------------------------------------------------------------


async def test_finalize_self_reported_failure_passes_through() -> None:
    raw = _outputs_json(
        {
            "status": "failure",
            "error": {
                "code": "registry.unauthorized",
                "class": "permanent",
                "message": "no credentials for ghcr.io/acme/app",
            },
            "outputs": {},
        }
    )
    result = await _broker().finalize_outputs(
        raw_outputs=raw,
        manifest=_manifest(),
        step=_step(),
        workspace_id="ws-1",
        artifacts=_FakeReader({}),
    )
    assert result.status == "failure"
    assert result.error is not None
    assert result.error.code == "registry.unauthorized"
    assert result.produced is None


async def test_finalize_rejects_oversized_outputs() -> None:
    raw = _outputs_json({"status": "success", "outputs": {"findings": 1}})
    with pytest.raises(OutputTooLargeError) as exc:
        await _broker(output_max_bytes=len(raw) - 1).finalize_outputs(
            raw_outputs=raw,
            manifest=_manifest(),
            step=_step(),
            workspace_id="ws-1",
            artifacts=_FakeReader({"report": b"x"}),
        )
    assert exc.value.code == "output.too_large"


async def test_finalize_rejects_invalid_json() -> None:
    with pytest.raises(OutputSchemaViolationError) as exc:
        await _broker().finalize_outputs(
            raw_outputs=b"{not json",
            manifest=_manifest(),
            step=_step(),
            workspace_id="ws-1",
            artifacts=_FakeReader({}),
        )
    assert exc.value.code == "output.schema_violation"


async def test_finalize_rejects_non_object_root() -> None:
    with pytest.raises(OutputSchemaViolationError):
        await _broker().finalize_outputs(
            raw_outputs=b"[1, 2, 3]",
            manifest=_manifest(),
            step=_step(),
            workspace_id="ws-1",
            artifacts=_FakeReader({}),
        )


async def test_finalize_rejects_malformed_envelope() -> None:
    # ``status`` omitted → envelope structure validation fails.
    raw = json.dumps({"schemaVersion": "1", "contractVersion": "1"}).encode("utf-8")
    with pytest.raises(OutputSchemaViolationError):
        await _broker().finalize_outputs(
            raw_outputs=raw,
            manifest=_manifest(),
            step=_step(),
            workspace_id="ws-1",
            artifacts=_FakeReader({}),
        )


async def test_finalize_missing_required_artifact() -> None:
    raw = _outputs_json(
        {
            "status": "success",
            "outputs": {"findings": 1, "reportRef": {"kind": "ArtifactRef", "name": "report"}},
        }
    )
    with pytest.raises(OutputInvalidArtifactRefError) as exc:
        await _broker().finalize_outputs(
            raw_outputs=raw,
            manifest=_manifest(),
            step=_step(),
            workspace_id="ws-1",
            artifacts=_FakeReader({}),
        )
    assert exc.value.code == "output.invalid_artifact_ref"


async def test_finalize_undeclared_artifact_ref() -> None:
    raw = _outputs_json(
        {
            "status": "success",
            "outputs": {"findings": 1, "reportRef": {"kind": "ArtifactRef", "name": "ghost"}},
        }
    )
    with pytest.raises(OutputInvalidArtifactRefError):
        await _broker().finalize_outputs(
            raw_outputs=raw,
            manifest=_manifest(),
            step=_step(),
            workspace_id="ws-1",
            artifacts=_FakeReader({"report": b"data"}),
        )


async def test_finalize_artifact_ref_missing_name() -> None:
    raw = _outputs_json(
        {
            "status": "success",
            "outputs": {"findings": 1, "reportRef": {"kind": "ArtifactRef"}},
        }
    )
    with pytest.raises(OutputInvalidArtifactRefError):
        await _broker().finalize_outputs(
            raw_outputs=raw,
            manifest=_manifest(),
            step=_step(),
            workspace_id="ws-1",
            artifacts=_FakeReader({"report": b"data"}),
        )


async def test_finalize_referenced_but_unproduced_optional_artifact() -> None:
    permissive_schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
    }
    raw = _outputs_json(
        {
            "status": "success",
            "outputs": {"reportRef": {"kind": "ArtifactRef", "name": "report"}},
        }
    )
    with pytest.raises(OutputInvalidArtifactRefError):
        await _broker().finalize_outputs(
            raw_outputs=raw,
            manifest=_manifest(
                artifacts=[{"name": "report", "mediaType": "application/json", "required": False}],
                output_schema=permissive_schema,
            ),
            step=_step(),
            workspace_id="ws-1",
            artifacts=_FakeReader({}),
        )


async def test_finalize_rejects_outputs_failing_schema_after_rewrite() -> None:
    raw = _outputs_json(
        {
            "status": "success",
            "outputs": {
                "findings": "twelve",  # schema requires an integer
                "reportRef": {"kind": "ArtifactRef", "name": "report"},
            },
        }
    )
    with pytest.raises(OutputSchemaViolationError) as exc:
        await _broker().finalize_outputs(
            raw_outputs=raw,
            manifest=_manifest(),
            step=_step(),
            workspace_id="ws-1",
            artifacts=_FakeReader({"report": b"data"}),
        )
    assert exc.value.code == "output.schema_violation"
    assert exc.value.issues


# ---------------------------------------------------------------------------
# Error → envelope rendering
# ---------------------------------------------------------------------------


def test_error_renders_envelope_with_issues() -> None:
    err = InputSchemaViolationError("bad inputs", issues=["image -> required"])
    envelope = err.to_error_envelope()
    assert envelope.code == "input.schema_violation"
    assert envelope.error_class is ErrorClass.PERMANENT
    assert envelope.details == {"issues": ["image -> required"]}


def test_error_renders_envelope_without_issues() -> None:
    envelope = OutputTooLargeError("too big").to_error_envelope()
    assert envelope.code == "output.too_large"
    assert envelope.details is None

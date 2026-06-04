"""The I/O Broker — input materialization + two-phase output finalization.

The broker owns the two schema-validation boundaries of an attempt (design
§ Schema validation):

- **Before start** — validates the materialized ``inputs`` payload against the
  activity's input JSON Schema (Draft 2020-12). A broken upstream that
  materializes malformed inputs fails the attempt with ``input.schema_violation``
  *before* the sandbox runs.
- **After exit** — runs two-phase output finalization (design § Two-phase output
  finalization): parse ``outputs.json`` (size-capped), upload every declared
  ``spec.outputs.artifacts[]`` via the :class:`ArtifactStoreClient`, rewrite each
  ``ArtifactRef`` in place with its store-assigned ``id``/``digest``/``mediaType``/
  ``size``, synthesize ``produced[]``, then validate the **finalized** ``outputs``
  against the activity's output JSON Schema. The orchestrator only ever sees a
  fully populated, schema-valid envelope.

All broker failures are permanent (:class:`IOBrokerError`); a malformed
input/output payload cannot be fixed by a retry.
"""

from __future__ import annotations

import json
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as SchemaValidationError
from pydantic import ValidationError

from custos_arm.contract.envelope import (
    ActivitySpec,
    InputsEnvelope,
    OutputsEnvelope,
    StepRef,
)
from custos_arm.io.errors import (
    InputSchemaViolationError,
    OutputInvalidArtifactRefError,
    OutputSchemaViolationError,
    OutputTooLargeError,
)
from custos_arm.io.models import OutputArtifactReader
from custos_arm.manifest.models import ActivityManifest
from custos_arm.store.artifact import ArtifactRecord, ArtifactStoreClient


def _pointer(error: SchemaValidationError) -> str:
    """Render a JSON-Pointer-style path (RFC 6901) to the offending element."""
    parts: list[str] = []
    for segment in error.absolute_path:
        seg = str(segment).replace("~", "~0").replace("/", "~1")
        parts.append(seg)
    return "/".join(parts)


def _schema_issues(schema: dict[str, Any], document: Any) -> list[str]:
    """Collect every Draft 2020-12 violation of ``document`` against ``schema``.

    Errors are gathered in one pass (no first-error short-circuit) and ordered
    by their JSON-Pointer path so the surfaced list is stable.
    """
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(document), key=_pointer)
    return [f"{_pointer(error) or '<root>'} -> {error.message}" for error in errors]


class IOBroker:
    """Materialize + validate inputs and finalize + validate outputs for one attempt.

    ``output_max_bytes`` is the ``ARM_OUTPUT_MAX_BYTES`` ceiling on the raw
    ``outputs.json`` blob; the per-artifact ceiling lives on the injected
    :class:`ArtifactStoreClient`.
    """

    def __init__(self, artifact_store: ArtifactStoreClient, *, output_max_bytes: int) -> None:
        self._store = artifact_store
        self._output_max_bytes = output_max_bytes

    # -- Input side -------------------------------------------------------

    def validate_inputs(self, *, inputs: dict[str, Any], input_schema: dict[str, Any]) -> None:
        """Validate ``inputs`` against the activity's input JSON Schema.

        Raises:
            InputSchemaViolationError: when ``inputs`` violates ``input_schema``.
                The exception's ``issues`` list carries every violation.
        """
        issues = _schema_issues(input_schema, inputs)
        if issues:
            raise InputSchemaViolationError(
                "materialized inputs failed input schema validation", issues=issues
            )

    def materialize_inputs(
        self,
        *,
        activity: ActivitySpec,
        step: StepRef,
        inputs: dict[str, Any],
        input_schema: dict[str, Any],
    ) -> InputsEnvelope:
        """Validate ``inputs`` and build the ``/custos/in/inputs.json`` envelope.

        Raises:
            InputSchemaViolationError: when ``inputs`` violates ``input_schema``.
        """
        self.validate_inputs(inputs=inputs, input_schema=input_schema)
        return InputsEnvelope(activity=activity, step=step, inputs=inputs)

    # -- Output side ------------------------------------------------------

    async def finalize_outputs(
        self,
        *,
        raw_outputs: bytes,
        manifest: ActivityManifest,
        step: StepRef,
        workspace_id: str,
        artifacts: OutputArtifactReader,
    ) -> OutputsEnvelope:
        """Run two-phase finalization over the raw ``outputs.json`` blob.

        On a self-reported failure the parsed envelope passes through untouched
        (its error is mapped downstream by the Result Mapper). On success ARM
        uploads every declared artifact, rewrites the ``ArtifactRef``s, appends
        ``produced[]``, and validates the finalized ``outputs`` against the
        output JSON Schema.

        Raises:
            OutputTooLargeError: ``raw_outputs`` exceeds ``output_max_bytes``.
            OutputSchemaViolationError: the envelope is malformed JSON, has a
                bad envelope structure, or the finalized ``outputs`` violates the
                output JSON Schema.
            OutputInvalidArtifactRefError: a ``required`` artifact is missing, or
                an ``ArtifactRef`` names no declared/produced artifact.
        """
        if len(raw_outputs) > self._output_max_bytes:
            raise OutputTooLargeError(
                f"outputs.json is {len(raw_outputs)} bytes, "
                f"over the {self._output_max_bytes}-byte cap"
            )

        envelope = self._parse_envelope(raw_outputs)
        if envelope.status == "failure":
            return envelope

        declared_names = {artifact.name for artifact in manifest.spec.outputs.artifacts}
        records = await self._upload_declared_artifacts(
            manifest=manifest, step=step, workspace_id=workspace_id, artifacts=artifacts
        )
        rewritten = self._rewrite_refs(envelope.outputs, records, declared_names)

        issues = _schema_issues(manifest.spec.outputs.json_schema, rewritten)
        if issues:
            raise OutputSchemaViolationError(
                "finalized outputs failed output schema validation", issues=issues
            )

        finalized = {
            "schemaVersion": envelope.schema_version,
            "contractVersion": envelope.contract_version,
            "status": "success",
            "outputs": rewritten,
            "produced": [self._ref_dict(record) for record in records.values()],
        }
        return OutputsEnvelope.model_validate(finalized)

    def _parse_envelope(self, raw_outputs: bytes) -> OutputsEnvelope:
        try:
            parsed: Any = json.loads(raw_outputs)
        except json.JSONDecodeError as exc:
            raise OutputSchemaViolationError(f"outputs.json is not valid JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise OutputSchemaViolationError(
                f"outputs.json must be a JSON object at the root, got {type(parsed).__name__}"
            )
        try:
            return OutputsEnvelope.model_validate(parsed)
        except ValidationError as exc:
            raise OutputSchemaViolationError(
                f"outputs.json envelope is malformed: {exc.error_count()} error(s)"
            ) from exc

    async def _upload_declared_artifacts(
        self,
        *,
        manifest: ActivityManifest,
        step: StepRef,
        workspace_id: str,
        artifacts: OutputArtifactReader,
    ) -> dict[str, ArtifactRecord]:
        records: dict[str, ArtifactRecord] = {}
        for artifact in manifest.spec.outputs.artifacts:
            if not artifacts.has(artifact.name):
                if artifact.required:
                    raise OutputInvalidArtifactRefError(
                        f"required artifact {artifact.name!r} was not produced"
                    )
                continue
            records[artifact.name] = await self._store.upload(
                workspace_id=workspace_id,
                name=artifact.name,
                content=artifacts.open(artifact.name),
                produced_by_run_id=step.run_id,
                produced_by_step_id=step.step_id,
                produced_by_attempt=step.attempt,
                media_type=artifact.media_type,
            )
        return records

    def _rewrite_refs(
        self, node: Any, records: dict[str, ArtifactRecord], declared_names: set[str]
    ) -> Any:
        if isinstance(node, dict):
            if node.get("kind") == "ArtifactRef":
                return self._expand_ref(node, records, declared_names)
            return {
                key: self._rewrite_refs(value, records, declared_names)
                for key, value in node.items()
            }
        if isinstance(node, list):
            return [self._rewrite_refs(item, records, declared_names) for item in node]
        return node

    def _expand_ref(
        self, node: dict[str, Any], records: dict[str, ArtifactRecord], declared_names: set[str]
    ) -> dict[str, Any]:
        name = node.get("name")
        if not isinstance(name, str) or not name:
            raise OutputInvalidArtifactRefError("ArtifactRef is missing a 'name'")
        if name not in declared_names:
            raise OutputInvalidArtifactRefError(
                f"ArtifactRef {name!r} is not a declared spec.outputs.artifacts[] name"
            )
        record = records.get(name)
        if record is None:
            raise OutputInvalidArtifactRefError(
                f"ArtifactRef {name!r} references an artifact that was not produced"
            )
        return self._ref_dict(record)

    @staticmethod
    def _ref_dict(record: ArtifactRecord) -> dict[str, Any]:
        return {
            "kind": "ArtifactRef",
            "name": record.name,
            "id": record.id,
            "mediaType": record.media_type,
            "digest": record.digest,
            "size": record.size,
        }


__all__ = ["IOBroker"]

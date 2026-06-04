"""I/O Broker (ARM-IMPL-009) — input materialization + two-phase output finalization.

ARM owns the two schema-validation boundaries of an attempt: it validates the
materialized ``inputs.json`` against the activity's input JSON Schema before the
sandbox starts, and validates the **finalized** ``outputs.json`` (after artifact
upload + ``ArtifactRef`` rewrite + ``produced[]`` synthesis) before returning to
the Workflow Service.
"""

from __future__ import annotations

from custos_arm.io.broker import IOBroker
from custos_arm.io.errors import (
    InputSchemaViolationError,
    IOBrokerError,
    OutputInvalidArtifactRefError,
    OutputSchemaViolationError,
    OutputTooLargeError,
)
from custos_arm.io.models import OutputArtifactReader

__all__ = [
    "IOBroker",
    "IOBrokerError",
    "InputSchemaViolationError",
    "OutputArtifactReader",
    "OutputInvalidArtifactRefError",
    "OutputSchemaViolationError",
    "OutputTooLargeError",
]

"""Activity Contract v1 — envelopes, platform types, and error semantics.

This package defines the file-based contract between the orchestrator and
an activity:

- :mod:`~custos_arm.contract.types` — platform types (``ImageRef``,
  ``OciDescriptor``, ``ConnectorRef``, ``ArtifactRef``, ``Duration``).
- :mod:`~custos_arm.contract.envelope` — the ``inputs.json`` / ``ctx.json``
  / ``outputs.json`` envelopes.
- :mod:`~custos_arm.contract.errors` — the error envelope, reserved
  namespaces, and the ADR-008 exit-code mapping.
"""

from __future__ import annotations

from custos_arm.contract.envelope import (
    CONTRACT_VERSION,
    SCHEMA_VERSION,
    ActivitySpec,
    CtxEnvelope,
    InputsEnvelope,
    OutputsEnvelope,
    StepRef,
)
from custos_arm.contract.errors import (
    CAUSE_MAX_DEPTH,
    DETAILS_MAX_BYTES,
    RESERVED_ERROR_NAMESPACES,
    ErrorCause,
    ErrorClass,
    ErrorEnvelope,
    ExitCode,
    ExitState,
    error_namespace,
    is_reserved_namespace,
    map_exit_code,
)
from custos_arm.contract.types import (
    ArtifactRef,
    ConnectorRef,
    Duration,
    ImageRef,
    OciDescriptor,
)

__all__ = [
    "CAUSE_MAX_DEPTH",
    "CONTRACT_VERSION",
    "DETAILS_MAX_BYTES",
    "RESERVED_ERROR_NAMESPACES",
    "SCHEMA_VERSION",
    "ActivitySpec",
    "ArtifactRef",
    "ConnectorRef",
    "CtxEnvelope",
    "Duration",
    "ErrorCause",
    "ErrorClass",
    "ErrorEnvelope",
    "ExitCode",
    "ExitState",
    "ImageRef",
    "InputsEnvelope",
    "OciDescriptor",
    "OutputsEnvelope",
    "StepRef",
    "error_namespace",
    "is_reserved_namespace",
    "map_exit_code",
]

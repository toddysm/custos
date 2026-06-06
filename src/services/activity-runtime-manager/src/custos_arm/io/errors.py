"""I/O Broker error taxonomy (design § Failure Modes).

Every failure the broker raises is **permanent** — a broken activity that
emits malformed inputs/outputs cannot be fixed by a retry. Each error carries
a reserved-namespace ``code`` (``input.*`` / ``output.*``) and renders to an
:class:`~custos_arm.contract.errors.ErrorEnvelope` the Result Mapper
(ARM-IMPL-011) folds into the attempt's terminal result.
"""

from __future__ import annotations

from typing import ClassVar

from custos_arm.contract.errors import ErrorClass, ErrorEnvelope


class IOBrokerError(Exception):
    """Base class for I/O Broker failures.

    Subclasses pin a reserved-namespace ``code`` and an :class:`ErrorClass`;
    all broker failures are :attr:`ErrorClass.PERMANENT`.
    """

    code: ClassVar[str]
    error_class: ClassVar[ErrorClass] = ErrorClass.PERMANENT

    def __init__(self, message: str, *, issues: list[str] | None = None) -> None:
        super().__init__(message)
        #: Field-level schema issues, when the failure is a schema violation.
        self.issues: list[str] = issues or []

    def to_error_envelope(self) -> ErrorEnvelope:
        """Render this failure as a contract :class:`ErrorEnvelope`."""
        details: dict[str, list[str]] | None = {"issues": self.issues} if self.issues else None
        return ErrorEnvelope.model_validate(
            {
                "code": self.code,
                "class": self.error_class.value,
                "message": str(self),
                "details": details,
            }
        )


class InputSchemaViolationError(IOBrokerError):
    """Materialized ``inputs.json`` fails the activity's input JSON Schema."""

    code: ClassVar[str] = "input.schema_violation"


class InputInvalidArtifactRefError(IOBrokerError):
    """A consumed input ``ArtifactRef`` is missing its ``name``/``id`` or names an unsafe path."""

    code: ClassVar[str] = "input.invalid_artifact_ref"


class OutputTooLargeError(IOBrokerError):
    """``outputs.json`` exceeds the ``ARM_OUTPUT_MAX_BYTES`` ceiling."""

    code: ClassVar[str] = "output.too_large"


class OutputSchemaViolationError(IOBrokerError):
    """Finalized ``outputs.json`` is malformed or fails the output JSON Schema."""

    code: ClassVar[str] = "output.schema_violation"


class OutputInvalidArtifactRefError(IOBrokerError):
    """A required artifact is missing or an ``ArtifactRef`` names no declared artifact."""

    code: ClassVar[str] = "output.invalid_artifact_ref"


__all__ = [
    "IOBrokerError",
    "InputInvalidArtifactRefError",
    "InputSchemaViolationError",
    "OutputInvalidArtifactRefError",
    "OutputSchemaViolationError",
    "OutputTooLargeError",
]

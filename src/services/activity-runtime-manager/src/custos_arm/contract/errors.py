"""Error envelope, reserved namespaces, and exit-code mapping (ADR-008).

The error envelope is the structured failure surface every activity
produces; exit codes are the coarse signal the sandbox returns. They must
agree because the orchestrator needs a deterministic answer per attempt:
**retry, fail permanently, or treat as cancelled.**
"""

from __future__ import annotations

import json
from enum import StrEnum
from typing import Any, Final

from pydantic import Field, field_validator, model_validator

from custos_arm.contract._base import ContractModel, is_iso8601_duration

#: Maximum serialized size of an ``error.details`` payload (design § Locked
#: defaults). Larger context belongs in an artifact referenced by an
#: ``ArtifactRef``.
DETAILS_MAX_BYTES: Final[int] = 4 * 1024

#: Maximum depth of a nested ``cause`` chain (design § Locked defaults).
CAUSE_MAX_DEPTH: Final[int] = 3

#: Platform-reserved error-code namespaces (everything else is
#: activity-defined). The leading dotted segment of an ``error.code``.
RESERVED_ERROR_NAMESPACES: Final[frozenset[str]] = frozenset(
    {"activity", "input", "output", "system"}
)


class ErrorClass(StrEnum):
    """The orchestrator-facing failure class carried in ``error.class``."""

    RETRYABLE = "retryable"
    PERMANENT = "permanent"
    CANCELLED = "cancelled"


class ExitState(StrEnum):
    """The four terminal states an attempt resolves to (ADR-008)."""

    SUCCESS = "success"
    RETRYABLE = "retryable"
    PERMANENT = "permanent"
    CANCELLED = "cancelled"


class ExitCode:
    """The four well-known activity exit codes (ADR-008)."""

    SUCCESS: Final[int] = 0
    RETRYABLE: Final[int] = 1
    PERMANENT: Final[int] = 2
    CANCELLED: Final[int] = 3


def map_exit_code(code: int) -> ExitState:
    """Map a sandbox exit code to its terminal state (ADR-008, 4 states).

    Any code outside ``0``-``3`` (including SIGKILL/137, SIGSEGV/139, OOM)
    maps to :attr:`ExitState.RETRYABLE` — an uncategorized crash is more
    likely transient than logically permanent.
    """
    return {
        ExitCode.SUCCESS: ExitState.SUCCESS,
        ExitCode.RETRYABLE: ExitState.RETRYABLE,
        ExitCode.PERMANENT: ExitState.PERMANENT,
        ExitCode.CANCELLED: ExitState.CANCELLED,
    }.get(code, ExitState.RETRYABLE)


def error_namespace(code: str) -> str:
    """Return the leading dotted segment of an ``error.code`` (e.g. ``registry``)."""
    return code.split(".", 1)[0]


def is_reserved_namespace(code: str) -> bool:
    """Return ``True`` when ``code`` falls in a platform-reserved namespace."""
    return error_namespace(code) in RESERVED_ERROR_NAMESPACES


def _check_details_size(details: dict[str, Any] | None) -> dict[str, Any] | None:
    if details is None:
        return None
    size = len(json.dumps(details, separators=(",", ":")).encode("utf-8"))
    if size > DETAILS_MAX_BYTES:
        raise ValueError(
            f"error.details exceeds the {DETAILS_MAX_BYTES}-byte cap ({size} bytes); "
            "move large context into an artifact"
        )
    return details


def _check_retry_after(value: str | None) -> str | None:
    if value is not None and not is_iso8601_duration(value):
        raise ValueError(f"error.retryAfter is not a valid ISO-8601 duration: {value!r}")
    return value


class ErrorCause(ContractModel):
    """A nested underlying error, preserving the failure chain.

    Mirrors :class:`ErrorEnvelope` but ``class`` is optional, matching the
    design's lean cause shape (``{"code": ..., "message": ...}``). The
    chain is depth-capped at :data:`CAUSE_MAX_DEPTH`.
    """

    code: str = Field(..., min_length=1)
    error_class: ErrorClass | None = Field(default=None, alias="class")
    message: str = Field(..., min_length=1)
    details: dict[str, Any] | None = None
    retry_after: str | None = Field(default=None, alias="retryAfter")
    cause: ErrorCause | None = None

    _validate_details = field_validator("details")(_check_details_size)
    _validate_retry_after = field_validator("retry_after")(_check_retry_after)


class ErrorEnvelope(ContractModel):
    """The structured failure surface written by an activity (or synthesized by ARM)."""

    code: str = Field(..., min_length=1)
    error_class: ErrorClass = Field(..., alias="class")
    message: str = Field(..., min_length=1)
    details: dict[str, Any] | None = None
    retry_after: str | None = Field(default=None, alias="retryAfter")
    cause: ErrorCause | None = None

    _validate_details = field_validator("details")(_check_details_size)
    _validate_retry_after = field_validator("retry_after")(_check_retry_after)

    @model_validator(mode="after")
    def _check_depth(self) -> ErrorEnvelope:
        _enforce_cause_depth(self.cause, depth=1)
        return self


def _enforce_cause_depth(cause: ErrorCause | None, *, depth: int) -> None:
    """Reject a ``cause`` chain deeper than :data:`CAUSE_MAX_DEPTH`.

    ``depth`` is the level at which ``cause`` itself sits (the top envelope's
    direct cause is depth 1).
    """
    if cause is None:
        return
    if depth > CAUSE_MAX_DEPTH:
        raise ValueError(f"error.cause chain exceeds the maximum depth of {CAUSE_MAX_DEPTH}")
    _enforce_cause_depth(cause.cause, depth=depth + 1)


__all__ = [
    "CAUSE_MAX_DEPTH",
    "DETAILS_MAX_BYTES",
    "RESERVED_ERROR_NAMESPACES",
    "ErrorCause",
    "ErrorClass",
    "ErrorEnvelope",
    "ExitCode",
    "ExitState",
    "error_namespace",
    "is_reserved_namespace",
    "map_exit_code",
]

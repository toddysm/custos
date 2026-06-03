"""Tests for the error envelope, namespaces, and exit-code mapping (ARM-IMPL-003)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from custos_arm.contract import (
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

# ---------------------------------------------------------------------------
# Exit-code mapping (ADR-008, 4 states)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (ExitCode.SUCCESS, ExitState.SUCCESS),
        (ExitCode.RETRYABLE, ExitState.RETRYABLE),
        (ExitCode.PERMANENT, ExitState.PERMANENT),
        (ExitCode.CANCELLED, ExitState.CANCELLED),
    ],
)
def test_known_exit_codes_map_to_states(code: int, expected: ExitState) -> None:
    assert map_exit_code(code) == expected


@pytest.mark.parametrize("code", [137, 139, 255, 42, -1, 4])
def test_uncategorized_exit_codes_map_to_retryable(code: int) -> None:
    assert map_exit_code(code) is ExitState.RETRYABLE


def test_exit_code_constants() -> None:
    assert (ExitCode.SUCCESS, ExitCode.RETRYABLE, ExitCode.PERMANENT, ExitCode.CANCELLED) == (
        0,
        1,
        2,
        3,
    )


# ---------------------------------------------------------------------------
# Reserved namespaces
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "code",
    ["activity.no_output", "input.schema_violation", "output.too_large", "system.sandbox_failure"],
)
def test_reserved_namespaces_detected(code: str) -> None:
    assert is_reserved_namespace(code) is True


@pytest.mark.parametrize("code", ["registry.unauthorized", "scan.engine_failed", "acme.quota"])
def test_activity_defined_namespaces_not_reserved(code: str) -> None:
    assert is_reserved_namespace(code) is False


def test_error_namespace_extraction() -> None:
    assert error_namespace("registry.unauthorized") == "registry"
    assert error_namespace("nodot") == "nodot"


def test_reserved_namespace_set() -> None:
    assert frozenset({"activity", "input", "output", "system"}) == RESERVED_ERROR_NAMESPACES


# ---------------------------------------------------------------------------
# Error envelope schema + round-trip
# ---------------------------------------------------------------------------


def test_error_envelope_round_trips_full_example() -> None:
    payload = {
        "code": "registry.unauthorized",
        "class": "permanent",
        "message": "no credentials for ghcr.io/acme/app",
        "details": {"registry": "ghcr.io", "repo": "acme/app"},
        "retryAfter": "PT30S",
        "cause": {"code": "http.401", "message": "unauthorized"},
    }
    env = ErrorEnvelope.model_validate(payload)
    assert env.error_class is ErrorClass.PERMANENT
    assert env.model_dump(by_alias=True, exclude_none=True) == payload


def test_error_envelope_requires_class() -> None:
    with pytest.raises(ValidationError):
        ErrorEnvelope.model_validate({"code": "x.y", "message": "boom"})


def test_error_cause_class_is_optional() -> None:
    cause = ErrorCause.model_validate({"code": "http.401", "message": "unauthorized"})
    assert cause.error_class is None


def test_details_within_cap_is_accepted() -> None:
    details = {"blob": "a" * (DETAILS_MAX_BYTES - 100)}
    env = ErrorEnvelope.model_validate(
        {"code": "x.y", "class": ErrorClass.PERMANENT, "message": "m", "details": details}
    )
    assert env.details == details


def test_details_over_cap_is_rejected() -> None:
    details = {"blob": "a" * (DETAILS_MAX_BYTES + 1)}
    with pytest.raises(ValidationError, match=r"exceeds the .* cap"):
        ErrorEnvelope.model_validate(
            {"code": "x.y", "class": ErrorClass.PERMANENT, "message": "m", "details": details}
        )


def test_cause_details_over_cap_is_rejected() -> None:
    with pytest.raises(ValidationError, match=r"exceeds the .* cap"):
        ErrorCause(code="x.y", message="m", details={"blob": "a" * (DETAILS_MAX_BYTES + 1)})


def test_cause_chain_at_max_depth_is_accepted() -> None:
    # depth 1 -> 2 -> 3 (== CAUSE_MAX_DEPTH) must be accepted.
    chain = ErrorCause(code="l3", message="three")
    for label in ("l2", "l1"):
        chain = ErrorCause(code=label, message=label, cause=chain)
    env = ErrorEnvelope.model_validate(
        {"code": "top", "class": ErrorClass.RETRYABLE, "message": "t", "cause": chain}
    )
    assert env.cause is not None
    assert CAUSE_MAX_DEPTH == 3


def test_cause_chain_over_max_depth_is_rejected() -> None:
    chain = ErrorCause(code="l4", message="four")
    for label in ("l3", "l2", "l1"):
        chain = ErrorCause(code=label, message=label, cause=chain)
    with pytest.raises(ValidationError, match="exceeds the maximum depth"):
        ErrorEnvelope.model_validate(
            {"code": "top", "class": ErrorClass.RETRYABLE, "message": "t", "cause": chain}
        )


def test_retry_after_must_be_iso8601() -> None:
    with pytest.raises(ValidationError, match="not a valid ISO-8601 duration"):
        ErrorEnvelope.model_validate(
            {
                "code": "x.y",
                "class": ErrorClass.RETRYABLE,
                "message": "m",
                "retryAfter": "30 seconds",
            }
        )


def test_retry_after_valid_iso8601_accepted() -> None:
    env = ErrorEnvelope.model_validate(
        {"code": "x.y", "class": ErrorClass.RETRYABLE, "message": "m", "retryAfter": "PT30S"}
    )
    assert env.retry_after == "PT30S"


def test_error_class_values() -> None:
    assert {c.value for c in ErrorClass} == {"retryable", "permanent", "cancelled"}

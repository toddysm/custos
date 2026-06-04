"""Tests for the Result Mapper (ARM-IMPL-011)."""

from __future__ import annotations

import pytest

from custos_arm.contract.envelope import OutputsEnvelope
from custos_arm.contract.errors import ErrorClass, ErrorEnvelope, ExitState
from custos_arm.result import (
    CONTRACT_VIOLATION_CODE,
    NO_OUTPUT_CODE,
    ActivityResultEnvelope,
    ResultClass,
    ResultMapper,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _error(error_class: ErrorClass, *, code: str = "registry.unauthorized") -> ErrorEnvelope:
    return ErrorEnvelope.model_validate(
        {"code": code, "class": error_class.value, "message": "boom"}
    )


def _success_envelope(**outputs: object) -> OutputsEnvelope:
    return OutputsEnvelope(status="success", outputs=dict(outputs) or {"digest": "sha256:abc"})


def _failure_envelope(error_class: ErrorClass) -> OutputsEnvelope:
    return OutputsEnvelope(status="failure", error=_error(error_class))


# ---------------------------------------------------------------------------
# ResultClass
# ---------------------------------------------------------------------------


def test_result_class_values() -> None:
    assert {c.value for c in ResultClass} == {"success", "retryable", "permanent", "cancelled"}


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        (ExitState.SUCCESS, ResultClass.SUCCESS),
        (ExitState.RETRYABLE, ResultClass.RETRYABLE),
        (ExitState.PERMANENT, ResultClass.PERMANENT),
        (ExitState.CANCELLED, ResultClass.CANCELLED),
    ],
)
def test_result_class_from_exit_state(state: ExitState, expected: ResultClass) -> None:
    assert ResultClass.from_exit_state(state) is expected


@pytest.mark.parametrize(
    ("error_class", "expected"),
    [
        (ErrorClass.RETRYABLE, ResultClass.RETRYABLE),
        (ErrorClass.PERMANENT, ResultClass.PERMANENT),
        (ErrorClass.CANCELLED, ResultClass.CANCELLED),
    ],
)
def test_result_class_from_error_class(error_class: ErrorClass, expected: ResultClass) -> None:
    assert ResultClass.from_error_class(error_class) is expected


# ---------------------------------------------------------------------------
# ActivityResultEnvelope invariants
# ---------------------------------------------------------------------------


def test_result_envelope_success_requires_outputs() -> None:
    with pytest.raises(ValueError, match="requires an 'outputs'"):
        ActivityResultEnvelope.model_validate({"class": "success", "attempt": 1})


def test_result_envelope_success_rejects_error() -> None:
    with pytest.raises(ValueError, match="must not carry an 'error'"):
        ActivityResultEnvelope.model_validate(
            {
                "class": "success",
                "attempt": 1,
                "outputs": {},
                "error": {"code": "x.y", "class": "permanent", "message": "m"},
            }
        )


def test_result_envelope_failure_requires_error() -> None:
    with pytest.raises(ValueError, match="requires an 'error'"):
        ActivityResultEnvelope.model_validate({"class": "permanent", "attempt": 1})


def test_result_envelope_failure_rejects_outputs() -> None:
    with pytest.raises(ValueError, match="must not carry 'outputs'"):
        ActivityResultEnvelope.model_validate(
            {
                "class": "retryable",
                "attempt": 1,
                "outputs": {"a": 1},
                "error": {"code": "x.y", "class": "retryable", "message": "m"},
            }
        )


def test_result_envelope_rejects_zero_attempt() -> None:
    with pytest.raises(ValueError):
        ActivityResultEnvelope.model_validate({"class": "success", "attempt": 0, "outputs": {}})


def test_result_envelope_serializes_class_by_alias() -> None:
    env = ActivityResultEnvelope.model_validate(
        {"class": "success", "attempt": 2, "outputs": {"ok": True}}
    )
    assert env.model_dump(by_alias=True)["class"] == "success"
    assert env.class_ is ResultClass.SUCCESS


# ---------------------------------------------------------------------------
# Rule 1 — exit 0 + success envelope
# ---------------------------------------------------------------------------


def test_rule1_success() -> None:
    result = ResultMapper().map_result(
        exit_code=0, finalized_outputs=_success_envelope(digest="sha256:abc"), attempt=1
    )
    assert result.class_ is ResultClass.SUCCESS
    assert result.outputs == {"digest": "sha256:abc"}
    assert result.error is None
    assert result.attempt == 1


# ---------------------------------------------------------------------------
# Rule 2 — exit non-zero + failure envelope (envelope wins)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("exit_code", "error_class", "expected"),
    [
        (1, ErrorClass.PERMANENT, ResultClass.PERMANENT),
        (2, ErrorClass.RETRYABLE, ResultClass.RETRYABLE),
        (3, ErrorClass.PERMANENT, ResultClass.PERMANENT),
        (137, ErrorClass.CANCELLED, ResultClass.CANCELLED),
    ],
)
def test_rule2_envelope_wins_over_exit_code(
    exit_code: int, error_class: ErrorClass, expected: ResultClass
) -> None:
    result = ResultMapper().map_result(
        exit_code=exit_code, finalized_outputs=_failure_envelope(error_class), attempt=3
    )
    assert result.class_ is expected
    assert result.error is not None
    assert result.error.error_class is error_class
    assert result.error.code == "registry.unauthorized"
    assert result.outputs is None
    assert result.attempt == 3


# ---------------------------------------------------------------------------
# Rule 3 — exit non-zero + no envelope (exit-code fallback)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("exit_code", "expected"),
    [
        (1, ResultClass.RETRYABLE),
        (2, ResultClass.PERMANENT),
        (3, ResultClass.CANCELLED),
        (137, ResultClass.RETRYABLE),  # uncategorized crash -> retryable
        (139, ResultClass.RETRYABLE),
    ],
)
def test_rule3_exit_code_fallback(exit_code: int, expected: ResultClass) -> None:
    result = ResultMapper().map_result(exit_code=exit_code, finalized_outputs=None, attempt=1)
    assert result.class_ is expected
    assert result.error is not None
    assert result.error.code == NO_OUTPUT_CODE
    assert result.error.error_class.value == expected.value
    assert str(exit_code) in result.error.message
    assert result.outputs is None


# ---------------------------------------------------------------------------
# Rule 4 — exit 0 + no/invalid envelope (contract violation)
# ---------------------------------------------------------------------------


def test_rule4_clean_exit_without_envelope_is_permanent() -> None:
    result = ResultMapper().map_result(exit_code=0, finalized_outputs=None, attempt=2)
    assert result.class_ is ResultClass.PERMANENT
    assert result.error is not None
    assert result.error.code == CONTRACT_VIOLATION_CODE
    assert result.error.error_class is ErrorClass.PERMANENT
    assert result.outputs is None
    assert result.attempt == 2


# ---------------------------------------------------------------------------
# Rule 5 — exit 0 + failure envelope (trust the envelope)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("error_class", "expected"),
    [
        (ErrorClass.RETRYABLE, ResultClass.RETRYABLE),
        (ErrorClass.PERMANENT, ResultClass.PERMANENT),
        (ErrorClass.CANCELLED, ResultClass.CANCELLED),
    ],
)
def test_rule5_clean_exit_self_reported_failure(
    error_class: ErrorClass, expected: ResultClass
) -> None:
    result = ResultMapper().map_result(
        exit_code=0, finalized_outputs=_failure_envelope(error_class), attempt=1
    )
    assert result.class_ is expected
    assert result.error is not None
    assert result.error.error_class is error_class


# ---------------------------------------------------------------------------
# Disagreement — non-zero exit + success envelope (contract violation)
# ---------------------------------------------------------------------------


def test_dirty_exit_with_success_envelope_is_contract_violation() -> None:
    result = ResultMapper().map_result(
        exit_code=2, finalized_outputs=_success_envelope(), attempt=1
    )
    assert result.class_ is ResultClass.PERMANENT
    assert result.error is not None
    assert result.error.code == CONTRACT_VIOLATION_CODE
    assert "exited 2" in result.error.message
    assert result.outputs is None


def test_failure_envelope_preserves_full_error() -> None:
    error = ErrorEnvelope.model_validate(
        {
            "code": "scan.engine_failed",
            "class": "retryable",
            "message": "engine timed out",
            "retryAfter": "PT30S",
            "details": {"engine": "trivy"},
        }
    )
    outputs = OutputsEnvelope(status="failure", error=error)
    result = ResultMapper().map_result(exit_code=1, finalized_outputs=outputs, attempt=4)
    assert result.error is error
    assert result.error.retry_after == "PT30S"
    assert result.error.details == {"engine": "trivy"}

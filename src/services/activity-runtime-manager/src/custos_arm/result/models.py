"""The orchestrator-facing result of a single activity attempt.

The Result Mapper resolves a sandbox exit code and the finalized
``outputs.json`` envelope into exactly one :class:`ActivityResultEnvelope`.
The orchestrator (Workflow Service) reads :attr:`ActivityResultEnvelope.class_`
to decide what to do next — succeed, retry, fail permanently, or treat the
attempt as cancelled — and never has to reconcile the two raw signals itself.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import Field, model_validator

from custos_arm.contract._base import ContractModel
from custos_arm.contract.errors import ErrorClass, ErrorEnvelope, ExitState


class ResultClass(StrEnum):
    """The terminal class the orchestrator acts on (design § Exit code semantics)."""

    SUCCESS = "success"
    RETRYABLE = "retryable"
    PERMANENT = "permanent"
    CANCELLED = "cancelled"

    @classmethod
    def from_exit_state(cls, state: ExitState) -> ResultClass:
        """Map an ADR-008 :class:`ExitState` to its result class (1:1)."""
        return cls(state.value)

    @classmethod
    def from_error_class(cls, error_class: ErrorClass) -> ResultClass:
        """Map an ``error.class`` to its result class (failure classes only)."""
        return cls(error_class.value)


class ActivityResultEnvelope(ContractModel):
    """The resolved result of one attempt the orchestrator acts on.

    Exactly one of :attr:`outputs` / :attr:`error` is populated: a
    :attr:`ResultClass.SUCCESS` result carries ``outputs`` and no ``error``;
    every failure class carries ``error`` and no ``outputs``.

    :param class_: The terminal class driving orchestrator behavior.
    :param attempt: The 1-based attempt number this result resolves.
    :param outputs: The structured success payload (``success`` only).
    :param error: The structured failure envelope (failure classes only).
    """

    class_: ResultClass = Field(..., alias="class")
    attempt: int = Field(..., ge=1)
    outputs: dict[str, Any] | None = None
    error: ErrorEnvelope | None = None

    @model_validator(mode="after")
    def _check_payload_consistency(self) -> ActivityResultEnvelope:
        if self.class_ is ResultClass.SUCCESS:
            if self.error is not None:
                raise ValueError("a success result must not carry an 'error' envelope")
            if self.outputs is None:
                raise ValueError("a success result requires an 'outputs' payload")
        else:
            if self.error is None:
                raise ValueError(f"a {self.class_.value} result requires an 'error' envelope")
            if self.outputs is not None:
                raise ValueError(f"a {self.class_.value} result must not carry 'outputs'")
        return self


__all__ = [
    "ActivityResultEnvelope",
    "ResultClass",
]

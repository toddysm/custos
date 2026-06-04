"""The Result Mapper — resolve an attempt's two terminal signals into one result.

The sandbox returns a coarse *exit code*; the activity (optionally) writes a
finalized ``outputs.json`` *envelope*. They have to agree because the
orchestrator needs a single deterministic answer per attempt. When they
disagree, ``outputs.json`` wins **when present and valid**; the exit code is
only the fallback. The mapper encodes the locked resolution rules (design
§ Source of truth: ``outputs.json`` wins when present):

#. Exit ``0`` + valid envelope ``status: "success"`` → **success**.
#. Exit non-zero + valid envelope ``status: "failure"`` → use ``error.class``;
   the exit code is logged but not interpreted.
#. Exit non-zero with **no** valid envelope → fall back to the exit-code
   mapping; synthesize ``activity.no_output`` with the derived class.
#. Exit ``0`` but the envelope is missing/invalid → **permanent**
   ``activity.contract_violation`` (a clean exit without a parseable envelope
   is a contract bug, not a transient).
#. Exit ``0`` + valid envelope ``status: "failure"`` → trust the envelope; the
   activity self-reported a failure but exited cleanly.

A non-zero exit paired with a ``status: "success"`` envelope is a contract
contradiction the locked rules do not name explicitly: the process crashed yet
claimed success. The mapper treats it as a permanent
``activity.contract_violation`` — a dirty exit cannot be trusted as success.
"""

from __future__ import annotations

from typing import Final

from custos_arm.contract.envelope import OutputsEnvelope
from custos_arm.contract.errors import (
    ErrorClass,
    ErrorEnvelope,
    ExitCode,
    map_exit_code,
)
from custos_arm.result.models import ActivityResultEnvelope, ResultClass

#: Synthesized when a non-zero exit produced no valid ``outputs.json`` (rule 3).
NO_OUTPUT_CODE: Final[str] = "activity.no_output"

#: Synthesized when the exit code and the envelope disagree in a way that
#: breaks the contract (rules 4 and the exit-non-zero / success case).
CONTRACT_VIOLATION_CODE: Final[str] = "activity.contract_violation"


class ResultMapper:
    """Resolve ``(exit_code, finalized_outputs)`` to an :class:`ActivityResultEnvelope`."""

    def map_result(
        self,
        *,
        exit_code: int,
        finalized_outputs: OutputsEnvelope | None,
        attempt: int,
    ) -> ActivityResultEnvelope:
        """Apply the locked resolution rules to one attempt's terminal signals.

        :param exit_code: The sandbox exit code (ADR-008: ``0``-``3`` are
            well-known; any other value maps to ``retryable``).
        :param finalized_outputs: The activity's finalized ``outputs.json``
            envelope, or ``None`` when none was written or it failed to parse /
            finalize.
        :param attempt: The 1-based attempt number this result resolves.
        """
        if finalized_outputs is None:
            return self._map_without_envelope(exit_code=exit_code, attempt=attempt)
        return self._map_with_envelope(
            exit_code=exit_code, outputs=finalized_outputs, attempt=attempt
        )

    def _map_without_envelope(self, *, exit_code: int, attempt: int) -> ActivityResultEnvelope:
        if exit_code == ExitCode.SUCCESS:
            # Rule 4: a clean exit with no parseable envelope is a contract bug.
            return self._failure(
                ResultClass.PERMANENT,
                self._contract_violation("activity exited 0 without a valid outputs.json envelope"),
                attempt=attempt,
            )
        # Rule 3: fall back to the exit-code mapping.
        result_class = ResultClass.from_exit_state(map_exit_code(exit_code))
        return self._failure(
            result_class,
            self._no_output(exit_code=exit_code, result_class=result_class),
            attempt=attempt,
        )

    def _map_with_envelope(
        self, *, exit_code: int, outputs: OutputsEnvelope, attempt: int
    ) -> ActivityResultEnvelope:
        if outputs.status == "failure":
            # Rules 2 & 5: the envelope is authoritative regardless of exit code.
            assert outputs.error is not None  # guaranteed by OutputsEnvelope validation
            return self._failure(
                ResultClass.from_error_class(outputs.error.error_class),
                outputs.error,
                attempt=attempt,
            )
        if exit_code == ExitCode.SUCCESS:
            # Rule 1: clean exit + self-reported success.
            return ActivityResultEnvelope.model_validate(
                {
                    "class": ResultClass.SUCCESS.value,
                    "attempt": attempt,
                    "outputs": outputs.outputs,
                }
            )
        # Signals disagree: a dirty exit cannot be trusted as success.
        return self._failure(
            ResultClass.PERMANENT,
            self._contract_violation(f"activity exited {exit_code} but reported status='success'"),
            attempt=attempt,
        )

    @staticmethod
    def _failure(
        result_class: ResultClass, error: ErrorEnvelope, *, attempt: int
    ) -> ActivityResultEnvelope:
        return ActivityResultEnvelope.model_validate(
            {"class": result_class.value, "attempt": attempt, "error": error}
        )

    @staticmethod
    def _no_output(*, exit_code: int, result_class: ResultClass) -> ErrorEnvelope:
        return ErrorEnvelope.model_validate(
            {
                "code": NO_OUTPUT_CODE,
                "class": ErrorClass(result_class.value).value,
                "message": (
                    f"activity exited {exit_code} without writing a valid outputs.json envelope"
                ),
            }
        )

    @staticmethod
    def _contract_violation(message: str) -> ErrorEnvelope:
        return ErrorEnvelope.model_validate(
            {
                "code": CONTRACT_VIOLATION_CODE,
                "class": ErrorClass.PERMANENT.value,
                "message": message,
            }
        )


__all__ = [
    "CONTRACT_VIOLATION_CODE",
    "NO_OUTPUT_CODE",
    "ResultMapper",
]

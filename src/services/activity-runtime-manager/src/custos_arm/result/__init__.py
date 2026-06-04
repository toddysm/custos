"""Result Mapper — resolve ``(exitCode, finalizedOutputs)`` to a result.

The orchestrator needs a single deterministic answer per attempt: **success,
retry, fail permanently, or treat as cancelled.** Two signals feed that
decision — the sandbox *exit code* (a coarse fallback) and the activity's
*finalized ``outputs.json`` envelope* (the authoritative source when present).
The :class:`~custos_arm.result.mapper.ResultMapper` applies the locked
resolution rules (design § Error Envelope & Exit Codes) to produce one
:class:`~custos_arm.result.models.ActivityResultEnvelope`.
"""

from __future__ import annotations

from custos_arm.result.mapper import (
    CONTRACT_VIOLATION_CODE,
    NO_OUTPUT_CODE,
    ResultMapper,
)
from custos_arm.result.models import ActivityResultEnvelope, ResultClass

__all__ = [
    "CONTRACT_VIOLATION_CODE",
    "NO_OUTPUT_CODE",
    "ActivityResultEnvelope",
    "ResultClass",
    "ResultMapper",
]

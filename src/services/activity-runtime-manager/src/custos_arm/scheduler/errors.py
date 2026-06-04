"""Translate a domain exception into an orchestrator-facing failure envelope.

Every sub-module raises typed exceptions rather than result envelopes so the
Scheduler owns the single point where domain errors become an
:class:`~custos_arm.result.ActivityResultEnvelope`. Three exception shapes are
supported, in priority order:

#. an exception exposing :meth:`to_error_envelope` (the I/O Broker family);
#. an exception carrying ``code`` + ``error_class`` attributes (the resolver,
   limiter, and OCI driver families); and
#. anything else, mapped to a retryable ``system.sandbox_failure`` so an
   unexpected infrastructure fault never wedges an attempt permanently.
"""

from __future__ import annotations

from custos_arm.contract import ErrorClass, ErrorEnvelope
from custos_arm.result import ActivityResultEnvelope, ResultClass
from custos_arm.runtime.oci import SYSTEM_SANDBOX_FAILURE


def error_envelope_for(exc: Exception) -> ErrorEnvelope:
    """Resolve ``exc`` to the structured :class:`ErrorEnvelope` it represents."""
    to_envelope = getattr(exc, "to_error_envelope", None)
    if callable(to_envelope):
        envelope = to_envelope()
        if isinstance(envelope, ErrorEnvelope):
            return envelope

    code = getattr(exc, "code", None)
    error_class = getattr(exc, "error_class", None)
    if isinstance(code, str) and isinstance(error_class, ErrorClass):
        return ErrorEnvelope.model_validate(
            {"code": code, "class": error_class.value, "message": str(exc) or code}
        )

    return ErrorEnvelope.model_validate(
        {
            "code": SYSTEM_SANDBOX_FAILURE,
            "class": ErrorClass.RETRYABLE.value,
            "message": str(exc) or "unexpected sandbox failure",
        }
    )


def synthesize_failure(exc: Exception, *, attempt: int) -> ActivityResultEnvelope:
    """Build the failure :class:`ActivityResultEnvelope` for ``exc``."""
    error = error_envelope_for(exc)
    return ActivityResultEnvelope.model_validate(
        {
            "class": ResultClass.from_error_class(error.error_class).value,
            "attempt": attempt,
            "error": error,
        }
    )


__all__ = ["error_envelope_for", "synthesize_failure"]

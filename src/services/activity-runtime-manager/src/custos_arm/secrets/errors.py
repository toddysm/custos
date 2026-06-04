"""Secret Injector error taxonomy (design § Failure Modes).

Secret materialization failures are **permanent** — a step whose required
connector slot was never bound, or whose connector context carries an empty
credential value, cannot be fixed by a retry. Each error pins a
reserved-namespace ``code`` (``input.*``) and renders to an
:class:`~custos_arm.contract.errors.ErrorEnvelope` the Result Mapper
(ARM-IMPL-011) folds into the attempt's terminal result.

Lease-refresh transport failures live in :mod:`custos_arm.secrets.lease`;
they are classified separately because a transiently-unreachable Connector
Service is retryable, unlike a malformed injection request.
"""

from __future__ import annotations

from typing import ClassVar

from custos_arm.contract.errors import ErrorClass, ErrorEnvelope


class SecretInjectorError(Exception):
    """Base class for Secret Injector failures.

    Subclasses pin a reserved-namespace ``code`` and an :class:`ErrorClass`;
    all injection failures are :attr:`ErrorClass.PERMANENT`.
    """

    code: ClassVar[str]
    error_class: ClassVar[ErrorClass] = ErrorClass.PERMANENT

    def __init__(self, message: str, *, issues: list[str] | None = None) -> None:
        super().__init__(message)
        #: Slot-level detail lines, when several connectors/keys are at fault.
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


class MissingConnectorError(SecretInjectorError):
    """A required ``spec.connectors[]`` slot has no bound ``ConnectorContext``.

    The Workflow Service's ``BindForStep`` must supply a context for every
    connector the manifest marks ``required: true``; a missing slot means the
    step can never run as authored.
    """

    code: ClassVar[str] = "input.missing_connector"


class MissingSecretError(SecretInjectorError):
    """A bound connector context carries a credential key with an empty value.

    Materializing an empty file under ``/custos/in/secrets/<slot>/<key>`` would
    hand the activity an unusable credential, so an empty value is rejected
    up front rather than failing opaquely inside the sandbox.
    """

    code: ClassVar[str] = "input.missing_secret"


__all__ = [
    "MissingConnectorError",
    "MissingSecretError",
    "SecretInjectorError",
]

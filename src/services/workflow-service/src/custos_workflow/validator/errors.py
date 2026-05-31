"""Locked structured error taxonomy for the API-Adapter Validator.

This module implements WF-IMPL-061 (issue #447) — the Validator's
half of the workflow-service public-API error contract. The full
:class:`~custos_workflow.validator.Validator` service lands in
WF-IMPL-063 (issue #449); shipping the error classes first lets
the WF-IMPL-061 RFC 7807 exception handlers
(:mod:`custos_workflow.api.errors`) bind to a stable type surface
without circular task dependencies.

The hierarchy mirrors the WF-IMPL-031 Run Controller convention
(:mod:`custos_workflow.runs.errors`):

* :class:`ValidatorError` — abstract base. Subclasses
  :class:`RuntimeError`. Defines the shared ``kind`` / ``message``
  / ``workspace_id`` attribute surface, hashable / equal-on-fields
  identity, and the :meth:`to_dict` JSON-safe serializer used by
  audit emission and the API exception handlers.
* :class:`WorkflowVersionNotFoundError` — the requested
  ``(workflowId, version)`` is absent from Catalog
  (``workflow.validator.workflow_version_not_found``). Also
  subclasses :class:`LookupError`.
* :class:`InputsSchemaError` — the supplied ``inputs`` failed
  the workflow's published JSON-Schema
  (``workflow.validator.inputs_schema_error``). Carries a list
  of structured field-level rejections. Also subclasses
  :class:`ValueError`.
* :class:`IdempotencyConflictError` — the ``(workspaceId,
  idempotencyKey)`` ledger already maps to a different request
  fingerprint (``workflow.validator.idempotency_conflict``).
* :class:`WorkspaceUnauthorizedError` — the caller's principal is
  not entitled to operate inside the target workspace
  (``workflow.validator.workspace_unauthorized``). Also
  subclasses :class:`PermissionError`.

The :attr:`KIND` string is a class-level :data:`typing.Final`
constant so ``cls.KIND`` and ``instance.kind`` are always
identical and never accidentally overridden by callers.

The closed set of ``kind`` strings is published as
:data:`LOCKED_VALIDATOR_KINDS`. The WF-IMPL-061 problem-envelope
test suite asserts the API-side ``LOCKED_API_KIND_TO_STATUS``
mapping covers every one, so adding or removing a subclass here
is a downstream contract break.

See the issue: https://github.com/toddysm/custos/issues/447
"""

from __future__ import annotations

import builtins
from typing import Any, ClassVar, Final

__all__ = [
    "LOCKED_VALIDATOR_KINDS",
    "IdempotencyConflictError",
    "InputsSchemaError",
    "ValidatorError",
    "WorkflowVersionNotFoundError",
    "WorkspaceUnauthorizedError",
]


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------


class ValidatorError(RuntimeError):
    """Base class for every structured Validator error.

    Concrete subclasses pin a stable :attr:`KIND` string. The
    constructor signature is intentionally narrow: ``message`` is
    positional, every other field is keyword-only. Subclasses
    keep the same shape so callers and pattern-matching consumers
    see a uniform surface.

    Attributes:
        kind: The :attr:`KIND` of this error's concrete class.
            Always a ``"workflow.validator.*"`` string.
        message: Human-readable explanation. Mirrors
            ``str(exception)`` for the default formatter.
        workspace_id: The affected ``workspaceId`` when known.
            ``None`` for failures that arise before a workspace is
            resolved (rare; mostly defensive — the Validator
            normally has the workspace id by construction since it
            is extracted from the path).
    """

    #: Subclasses pin this to a concrete ``"workflow.validator.*"``
    #: string. The base raises if instantiated directly because the
    #: empty kind would defeat the taxonomy.
    KIND: ClassVar[str] = ""

    def __init__(
        self,
        message: str,
        *,
        workspace_id: str | None = None,
    ) -> None:
        if not self.KIND:
            raise builtins.TypeError(
                "ValidatorError is abstract; instantiate a concrete "
                "subclass (WorkflowVersionNotFoundError, "
                "InputsSchemaError, IdempotencyConflictError, "
                "WorkspaceUnauthorizedError) instead.",
            )
        super().__init__(message)
        self.kind: str = self.KIND
        self.message: str = message
        self.workspace_id: str | None = workspace_id

    def _extra_fields(self) -> dict[str, Any]:
        """Hook for subclasses to contribute extra fields to
        :meth:`to_dict` and :meth:`__repr__` / :meth:`__eq__` /
        :meth:`__hash__`.

        The base returns an empty mapping. Subclasses override
        and return only JSON-safe primitives. The mapping's
        iteration order is preserved by :meth:`to_dict` so audit
        serialization stays deterministic.
        """
        return {}

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe dict for audit emission + RFC 7807 mapping.

        Shape (deterministic key order):

        ``{"kind": str, "message": str, "workspace_id": str | None, ...}``

        Subclasses extend the result with their structured fields
        (see :meth:`_extra_fields`). The result is deterministic
        in key order: ``kind`` first, then ``message``, then
        ``workspace_id``, then any subclass extras in declaration
        order.
        """
        out: dict[str, Any] = {
            "kind": self.kind,
            "message": self.message,
            "workspace_id": self.workspace_id,
        }
        out.update(self._extra_fields())
        return out

    def __repr__(self) -> str:
        parts: list[str] = [
            f"kind={self.kind!r}",
            f"message={self.message!r}",
            f"workspace_id={self.workspace_id!r}",
        ]
        parts.extend(f"{name}={value!r}" for name, value in self._extra_fields().items())
        return f"{type(self).__name__}({', '.join(parts)})"

    def _identity(self) -> tuple[Any, ...]:
        """Hashable identity tuple used by :meth:`__eq__` and :meth:`__hash__`.

        Concrete instances of the same subclass with identical
        fields compare equal and hash identically — different
        from the default exception-by-identity semantics. The
        Run Controller taxonomy uses the same convention so audit
        consumers can dedupe failures structurally.

        :class:`InputsSchemaError` carries a list of dicts under
        ``validation``; the identity tuple normalises it through
        :func:`_freeze` so the list-of-dicts stays hashable.
        """
        extras: list[tuple[str, Any]] = []
        for name, value in self._extra_fields().items():
            extras.append((name, _freeze(value)))
        return (
            type(self),
            self.kind,
            self.message,
            self.workspace_id,
            tuple(extras),
        )

    def __eq__(self, other: object) -> bool:
        if type(other) is not type(self):
            return NotImplemented
        return self._identity() == other._identity()

    def __hash__(self) -> int:
        return hash(self._identity())


def _freeze(value: Any) -> Any:
    """Recursively coerce a JSON-safe value into a hashable form.

    Lists become tuples; dicts become tuples of ``(key, frozen_value)``
    pairs sorted by key for deterministic order; primitives are
    returned untouched. Used by :meth:`ValidatorError._identity` so
    :class:`InputsSchemaError` instances (which carry list-of-dict
    field rejections) remain hashable.
    """
    if isinstance(value, list):
        return tuple(_freeze(v) for v in value)
    if isinstance(value, dict):
        return tuple(sorted((k, _freeze(v)) for k, v in value.items()))
    return value


# ---------------------------------------------------------------------------
# Concrete subclasses
# ---------------------------------------------------------------------------


class WorkflowVersionNotFoundError(ValidatorError, LookupError):
    """A ``StartRun`` request targets a workflow version Catalog does not hold.

    Raised by the Validator (WF-IMPL-063) after the upstream
    :class:`~custos_workflow.runs.controller.CatalogClient` returns a
    404 for the supplied ``(workflowId, version)``. Also subclasses
    :class:`LookupError` so callers using ``except LookupError:``
    still catch it.

    Attributes:
        workflow_id: The published workflow id the caller asked for.
        workflow_version: The semver string the caller asked for.
    """

    KIND: Final[str] = "workflow.validator.workflow_version_not_found"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        workspace_id: str | None = None,
        workflow_id: str | None = None,
        workflow_version: str | None = None,
    ) -> None:
        super().__init__(message, workspace_id=workspace_id)
        self.workflow_id: str | None = workflow_id
        self.workflow_version: str | None = workflow_version

    def _extra_fields(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "workflow_version": self.workflow_version,
        }


class InputsSchemaError(ValidatorError, ValueError):
    """A ``StartRun`` request's ``inputs`` failed the workflow's JSON-Schema.

    Raised by the Validator (WF-IMPL-063) after the published
    inputs schema rejects the caller-supplied payload. Carries the
    structured rejection list so clients receive field-level
    diagnostics in the RFC 7807 ``validation`` extension. Also
    subclasses :class:`ValueError` so callers using
    ``except ValueError:`` still catch it.

    Attributes:
        validation: List of ``{"loc": [...], "code": str, "message": str}``
            dicts, one per rejected field. Always a list (possibly
            empty). The dicts are JSON-safe so they round-trip
            verbatim through :meth:`to_dict`.
    """

    KIND: Final[str] = "workflow.validator.inputs_schema_error"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        workspace_id: str | None = None,
        validation: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(message, workspace_id=workspace_id)
        # Defensive shallow copy so the caller's list is not
        # captured by reference (audit emission may stash this
        # error for later serialization).
        self.validation: list[dict[str, Any]] = (
            [dict(item) for item in validation] if validation is not None else []
        )

    def _extra_fields(self) -> dict[str, Any]:
        return {"validation": list(self.validation)}


class IdempotencyConflictError(ValidatorError):
    """A ``(workspaceId, idempotencyKey)`` already maps to a divergent request.

    Raised by the Validator (WF-IMPL-063) when the in-flight or
    settled ledger entry for the supplied
    ``(workspaceId, idempotencyKey)`` has a different request
    fingerprint than the current call. Per RFC draft "The
    Idempotency-Key HTTP Header Field" this maps to 409 Conflict.

    Attributes:
        idempotency_key: The caller-supplied key that triggered the
            conflict.
    """

    KIND: Final[str] = "workflow.validator.idempotency_conflict"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        workspace_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> None:
        super().__init__(message, workspace_id=workspace_id)
        self.idempotency_key: str | None = idempotency_key

    def _extra_fields(self) -> dict[str, Any]:
        return {"idempotency_key": self.idempotency_key}


class WorkspaceUnauthorizedError(ValidatorError, PermissionError):
    """The caller's principal is not entitled to operate in the target workspace.

    Raised by the Validator (WF-IMPL-063) after the workspace
    authorization check fails (call-context principal lacks the
    required role binding inside the requested workspace). Also
    subclasses :class:`PermissionError` so callers using
    ``except PermissionError:`` still catch it.

    Attributes:
        principal: The principal id the call-context carried, when
            known. ``None`` when the call-context middleware was
            running in dev-shim mode and never asserted a real
            principal.
    """

    KIND: Final[str] = "workflow.validator.workspace_unauthorized"  # type: ignore[misc]

    def __init__(
        self,
        message: str,
        *,
        workspace_id: str | None = None,
        principal: str | None = None,
    ) -> None:
        super().__init__(message, workspace_id=workspace_id)
        self.principal: str | None = principal

    def _extra_fields(self) -> dict[str, Any]:
        return {"principal": self.principal}


# ---------------------------------------------------------------------------
# Locked taxonomy export
# ---------------------------------------------------------------------------


#: Closed set of ``kind`` strings any concrete :class:`ValidatorError`
#: subclass may emit. The RFC 7807 envelope test in WF-IMPL-061
#: asserts every member appears in
#: :data:`custos_workflow.api.errors.LOCKED_API_KIND_TO_STATUS`, so
#: adding or removing a subclass here is a downstream contract break.
LOCKED_VALIDATOR_KINDS: Final[frozenset[str]] = frozenset(
    {
        WorkflowVersionNotFoundError.KIND,
        InputsSchemaError.KIND,
        IdempotencyConflictError.KIND,
        WorkspaceUnauthorizedError.KIND,
    },
)

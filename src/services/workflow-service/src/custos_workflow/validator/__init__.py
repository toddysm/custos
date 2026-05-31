"""Validator sub-module of the workflow-service API Adapter.

This package owns the pre-execution checks that gate every
:class:`StartRun` request received by the public REST surface
(workflow-version existence, inputs JSON-Schema match, workspace
authorization, ``(workspaceId, idempotencyKey)`` dedup).

WF-IMPL-061 ships the locked error taxonomy only — see
:mod:`custos_workflow.validator.errors`. The full :class:`Validator`
service, its :class:`IdempotencyLedger` adapter, and the inputs
JSON-Schema evaluator land in WF-IMPL-063 (issue #449); until then
the error classes are importable as the contract the API-side
exception handlers (WF-IMPL-061) translate into RFC 7807 envelopes.
"""

from __future__ import annotations

from custos_workflow.validator.errors import (
    LOCKED_VALIDATOR_KINDS,
    IdempotencyConflictError,
    InputsSchemaError,
    ValidatorError,
    WorkflowVersionNotFoundError,
    WorkspaceUnauthorizedError,
)

__all__ = [
    "LOCKED_VALIDATOR_KINDS",
    "IdempotencyConflictError",
    "InputsSchemaError",
    "ValidatorError",
    "WorkflowVersionNotFoundError",
    "WorkspaceUnauthorizedError",
]

"""Validator sub-module of the workflow-service API Adapter.

This package owns the pre-execution checks that gate every
:class:`StartRun` request received by the public REST surface
(workflow-version existence, inputs JSON-Schema match, workspace
authorization, ``(workspaceId, idempotencyKey)`` dedup).

The error taxonomy ships in :mod:`custos_workflow.validator.errors`
(WF-IMPL-061, #447). The orchestrator
:class:`~custos_workflow.validator.service.StartRunValidator`, the
:class:`~custos_workflow.validator.idempotency_ledger.IdempotencyLedger`
Protocol + in-memory adapter, and the inputs JSON-Schema evaluator
land in WF-IMPL-063 (#449); the API-side exception handlers
(WF-IMPL-061) translate every raised error into an RFC 7807
envelope.
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
from custos_workflow.validator.idempotency_ledger import (
    DEFAULT_IDEMPOTENCY_KEY_TTL,
    IDEMPOTENCY_TTL_ENV_VAR,
    IdempotencyLedger,
    InMemoryIdempotencyLedger,
    LedgerEntry,
    compute_request_fingerprint,
    idempotency_ttl_from_env,
)
from custos_workflow.validator.inputs import (
    derive_inputs_schema,
    validate_inputs_against_schema,
)
from custos_workflow.validator.service import (
    StartRunValidator,
    ValidatedStartRun,
)

__all__ = [
    "DEFAULT_IDEMPOTENCY_KEY_TTL",
    "IDEMPOTENCY_TTL_ENV_VAR",
    "LOCKED_VALIDATOR_KINDS",
    "IdempotencyConflictError",
    "IdempotencyLedger",
    "InMemoryIdempotencyLedger",
    "InputsSchemaError",
    "LedgerEntry",
    "StartRunValidator",
    "ValidatedStartRun",
    "ValidatorError",
    "WorkflowVersionNotFoundError",
    "WorkspaceUnauthorizedError",
    "compute_request_fingerprint",
    "derive_inputs_schema",
    "idempotency_ttl_from_env",
    "validate_inputs_against_schema",
]

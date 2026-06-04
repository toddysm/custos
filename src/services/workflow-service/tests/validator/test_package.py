"""Tests for the API-Adapter Validator package facade."""

from __future__ import annotations

import custos_workflow.validator as validator_pkg


def test_package_reexports_locked_validator_kinds() -> None:
    """Every kind in the locked taxonomy is also a class on the package."""
    kinds = validator_pkg.LOCKED_VALIDATOR_KINDS
    expected = {
        validator_pkg.WorkflowVersionNotFoundError.KIND,
        validator_pkg.InputsSchemaError.KIND,
        validator_pkg.IdempotencyConflictError.KIND,
        validator_pkg.WorkspaceUnauthorizedError.KIND,
    }
    assert kinds == frozenset(expected)


def test_package_exports_full_validator_surface() -> None:
    """The package re-export surface matches its ``__all__``."""
    exported = set(validator_pkg.__all__)
    expected = {
        "DEFAULT_IDEMPOTENCY_KEY_TTL",
        "IDEMPOTENCY_TTL_ENV_VAR",
        "LOCKED_VALIDATOR_KINDS",
        "DurableIdempotencyLedger",
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
    }
    assert exported == expected
    for name in expected:
        assert hasattr(validator_pkg, name), f"missing re-export: {name}"

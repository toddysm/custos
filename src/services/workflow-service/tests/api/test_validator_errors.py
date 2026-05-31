"""Tests for the slim :mod:`custos_workflow.validator.errors` taxonomy.

WF-IMPL-061 (issue #447) ships the Validator error classes ahead of
the full Validator service (WF-IMPL-063) so the API-side RFC 7807
exception handlers can bind to a stable type surface. These tests
cover the shared base behaviour and each concrete subclass.

Each test exercises one acceptance criterion from issue #447:

* the four canonical subclasses + abstract base are present;
* every subclass pins an *exact* ``KIND`` string;
* every subclass also subclasses a "friendly" builtin so generic
  ``except`` blocks still see it;
* :meth:`ValidatorError.to_dict` produces a JSON-safe dict with the
  documented deterministic key order;
* :meth:`ValidatorError.__repr__` is stable and includes every field;
* every subclass is hashable and equal-on-fields (including
  :class:`InputsSchemaError` whose ``validation`` list-of-dicts has
  to round-trip through the :func:`_freeze` helper).
"""

from __future__ import annotations

import builtins
from typing import Any

import pytest

from custos_workflow.validator import (
    IdempotencyConflictError,
    InputsSchemaError,
    ValidatorError,
    WorkflowVersionNotFoundError,
    WorkspaceUnauthorizedError,
)
from custos_workflow.validator.errors import LOCKED_VALIDATOR_KINDS, _freeze

# ---------------------------------------------------------------------------
# Module surface
# ---------------------------------------------------------------------------


class TestModuleSurface:
    def test_base_is_abstract(self) -> None:
        # Constructing the base directly is rejected so callers
        # cannot accidentally emit a ``kind=""`` envelope.
        with pytest.raises(builtins.TypeError, match="abstract"):
            ValidatorError("nope")

    def test_canonical_kind_strings_are_exact(self) -> None:
        # Locked taxonomy — these strings are part of the public
        # contract. Any change is a downstream break for the API
        # envelope mapping (LOCKED_API_KIND_TO_STATUS).
        assert WorkflowVersionNotFoundError.KIND == "workflow.validator.workflow_version_not_found"
        assert InputsSchemaError.KIND == "workflow.validator.inputs_schema_error"
        assert IdempotencyConflictError.KIND == "workflow.validator.idempotency_conflict"
        assert WorkspaceUnauthorizedError.KIND == "workflow.validator.workspace_unauthorized"

    def test_locked_kinds_frozenset_is_exhaustive(self) -> None:
        assert (
            frozenset(
                {
                    WorkflowVersionNotFoundError.KIND,
                    InputsSchemaError.KIND,
                    IdempotencyConflictError.KIND,
                    WorkspaceUnauthorizedError.KIND,
                },
            )
            == LOCKED_VALIDATOR_KINDS
        )

    def test_subclasses_inherit_friendly_bases(self) -> None:
        # Each canonical class subclasses the obvious Python builtin
        # so generic ``except`` blocks (LookupError / ValueError /
        # PermissionError) still see it per the WF-IMPL-061 spec.
        assert issubclass(WorkflowVersionNotFoundError, ValidatorError)
        assert issubclass(WorkflowVersionNotFoundError, LookupError)
        assert issubclass(InputsSchemaError, ValidatorError)
        assert issubclass(InputsSchemaError, ValueError)
        assert issubclass(IdempotencyConflictError, ValidatorError)
        assert issubclass(WorkspaceUnauthorizedError, ValidatorError)
        assert issubclass(WorkspaceUnauthorizedError, PermissionError)


# ---------------------------------------------------------------------------
# WorkflowVersionNotFoundError
# ---------------------------------------------------------------------------


class TestWorkflowVersionNotFoundError:
    def _make(self) -> WorkflowVersionNotFoundError:
        return WorkflowVersionNotFoundError(
            "no such version",
            workspace_id="ws-1",
            workflow_id="wf-1",
            workflow_version="1.2.3",
        )

    def test_construction_and_kind(self) -> None:
        err = self._make()
        assert err.kind == "workflow.validator.workflow_version_not_found"
        assert err.message == "no such version"
        assert err.workspace_id == "ws-1"
        assert err.workflow_id == "wf-1"
        assert err.workflow_version == "1.2.3"
        assert isinstance(err, LookupError)
        assert str(err) == "no such version"

    def test_to_dict_shape_and_key_order(self) -> None:
        d = self._make().to_dict()
        assert list(d.keys()) == [
            "kind",
            "message",
            "workspace_id",
            "workflow_id",
            "workflow_version",
        ]
        assert d == {
            "kind": "workflow.validator.workflow_version_not_found",
            "message": "no such version",
            "workspace_id": "ws-1",
            "workflow_id": "wf-1",
            "workflow_version": "1.2.3",
        }

    def test_repr_contains_every_field(self) -> None:
        text = repr(self._make())
        assert text.startswith("WorkflowVersionNotFoundError(")
        for fragment in ("kind=", "message=", "workspace_id=", "workflow_id=", "workflow_version="):
            assert fragment in text

    def test_equal_on_fields_and_hashable(self) -> None:
        a = self._make()
        b = self._make()
        assert a == b
        assert hash(a) == hash(b)
        # Different field → not equal.
        c = WorkflowVersionNotFoundError(
            "no such version",
            workspace_id="ws-1",
            workflow_id="wf-1",
            workflow_version="9.9.9",
        )
        assert a != c
        assert {a, b, c} == {a, c}

    def test_cross_class_not_equal(self) -> None:
        err = self._make()
        other = IdempotencyConflictError("x", workspace_id="ws-1", idempotency_key="k")
        assert (err == other) is False


# ---------------------------------------------------------------------------
# InputsSchemaError
# ---------------------------------------------------------------------------


class TestInputsSchemaError:
    def _issues(self) -> list[dict[str, Any]]:
        return [
            {"loc": ["inputs", "count"], "code": "int_parsing", "message": "not int"},
            {"loc": ["inputs", "name"], "code": "missing", "message": "required"},
        ]

    def test_construction_defensively_copies_validation_list(self) -> None:
        issues = self._issues()
        err = InputsSchemaError("bad", workspace_id="ws-2", validation=issues)
        # Mutating the caller's list does not bleed into the error.
        issues.clear()
        assert len(err.validation) == 2
        assert err.validation[0]["loc"] == ["inputs", "count"]

    def test_validation_defaults_to_empty_list(self) -> None:
        err = InputsSchemaError("bad", workspace_id="ws-2")
        assert err.validation == []

    def test_to_dict_includes_validation(self) -> None:
        err = InputsSchemaError("bad", workspace_id="ws-2", validation=self._issues())
        d = err.to_dict()
        assert d["kind"] == "workflow.validator.inputs_schema_error"
        assert d["validation"] == self._issues()
        assert list(d.keys()) == ["kind", "message", "workspace_id", "validation"]

    def test_equal_and_hashable_with_list_of_dicts_validation(self) -> None:
        a = InputsSchemaError("bad", workspace_id="ws-2", validation=self._issues())
        b = InputsSchemaError("bad", workspace_id="ws-2", validation=self._issues())
        assert a == b
        # The identity tuple goes through _freeze so list-of-dicts
        # stays hashable — both instances hash identically.
        assert hash(a) == hash(b)
        # Subset / set membership round-trips.
        assert {a} == {b}


# ---------------------------------------------------------------------------
# IdempotencyConflictError
# ---------------------------------------------------------------------------


class TestIdempotencyConflictError:
    def test_construction_and_to_dict(self) -> None:
        err = IdempotencyConflictError(
            "key fingerprint differs",
            workspace_id="ws-3",
            idempotency_key="abc-123",
        )
        assert err.kind == "workflow.validator.idempotency_conflict"
        assert err.idempotency_key == "abc-123"
        assert err.to_dict() == {
            "kind": "workflow.validator.idempotency_conflict",
            "message": "key fingerprint differs",
            "workspace_id": "ws-3",
            "idempotency_key": "abc-123",
        }


# ---------------------------------------------------------------------------
# WorkspaceUnauthorizedError
# ---------------------------------------------------------------------------


class TestWorkspaceUnauthorizedError:
    def test_construction_and_permission_error_base(self) -> None:
        err = WorkspaceUnauthorizedError(
            "not entitled",
            workspace_id="ws-4",
            principal="user:alice",
        )
        assert err.kind == "workflow.validator.workspace_unauthorized"
        assert err.principal == "user:alice"
        assert isinstance(err, PermissionError)

    def test_to_dict(self) -> None:
        err = WorkspaceUnauthorizedError(
            "not entitled",
            workspace_id="ws-4",
            principal="user:alice",
        )
        assert err.to_dict() == {
            "kind": "workflow.validator.workspace_unauthorized",
            "message": "not entitled",
            "workspace_id": "ws-4",
            "principal": "user:alice",
        }


# ---------------------------------------------------------------------------
# _freeze helper
# ---------------------------------------------------------------------------


class TestFreezeHelper:
    def test_freeze_primitive_passthrough(self) -> None:
        assert _freeze("x") == "x"
        assert _freeze(42) == 42
        assert _freeze(None) is None

    def test_freeze_list_to_tuple(self) -> None:
        assert _freeze([1, 2, 3]) == (1, 2, 3)

    def test_freeze_dict_to_sorted_tuple_of_pairs(self) -> None:
        # Sorted by key so the result is deterministic regardless of
        # the input dict's insertion order.
        a = _freeze({"b": 2, "a": 1})
        b = _freeze({"a": 1, "b": 2})
        assert a == b
        assert a == (("a", 1), ("b", 2))

    def test_freeze_nested_list_of_dicts(self) -> None:
        frozen = _freeze([{"k": "v"}, {"x": 1}])
        # Result is hashable (would otherwise raise TypeError).
        assert hash(frozen)

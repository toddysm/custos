"""Tests for the Run Controller error taxonomy (WF-IMPL-031)."""

from __future__ import annotations

import json

import pytest

from custos_workflow.runs import (
    LOCKED_RUN_KINDS,
    RunControllerError,
    RunNotFoundError,
    RunStateConflictError,
    RunStateCorruptError,
    WorkflowRuntimeUnavailableError,
)

# ---------------------------------------------------------------------------
# LOCKED_RUN_KINDS
# ---------------------------------------------------------------------------


def test_locked_run_kinds_is_a_frozenset() -> None:
    """The WF-IMPL-044 OTel counter relies on this being a frozenset
    (it gets used as a closed label set)."""

    assert isinstance(LOCKED_RUN_KINDS, frozenset)


def test_locked_run_kinds_pins_published_strings() -> None:
    """If anyone ever edits a ``KIND`` constant, this test must fail."""

    assert (
        frozenset(
            {
                "run.not_found",
                "run.state_conflict",
                "run.state_corrupt",
                "run.runtime_unavailable",
            }
        )
        == LOCKED_RUN_KINDS
    )


def test_locked_run_kinds_exhaustively_covers_class_hierarchy() -> None:
    """Every concrete subclass must contribute its KIND to the locked
    set, and the locked set must contain nothing else. This is the
    invariant the WF-IMPL-044 build-time check relies on."""

    subclass_kinds = {cls.KIND for cls in RunControllerError.__subclasses__() if cls.KIND}
    assert subclass_kinds == set(LOCKED_RUN_KINDS)


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------


def test_base_run_controller_error_is_abstract() -> None:
    """Direct instantiation must fail; the empty KIND would defeat
    the taxonomy."""

    with pytest.raises(TypeError):
        RunControllerError("not allowed")


def test_base_subclasses_runtime_error() -> None:
    """Callers using broad ``except RuntimeError:`` must still catch
    every taxonomy error."""

    assert issubclass(RunControllerError, RuntimeError)


# ---------------------------------------------------------------------------
# Concrete subclass identities + builtin parent classes
# ---------------------------------------------------------------------------


def test_run_not_found_error_kind_and_builtin() -> None:
    err = RunNotFoundError("missing", run_id="r-1")
    assert err.kind == RunNotFoundError.KIND == "run.not_found"
    assert isinstance(err, LookupError)
    assert isinstance(err, RunControllerError)
    assert isinstance(err, RuntimeError)


def test_run_state_conflict_error_kind_and_builtin() -> None:
    err = RunStateConflictError(
        "illegal transition",
        run_id="r-1",
        current_status="succeeded",
        attempted_status="cancelled",
    )
    assert err.kind == RunStateConflictError.KIND == "run.state_conflict"
    # Per design: state-conflict subclasses RuntimeError (the base)
    # without an extra builtin.
    assert isinstance(err, RuntimeError)


def test_run_state_corrupt_error_kind_and_builtin() -> None:
    err = RunStateCorruptError("graph corrupted", run_id="r-1")
    assert err.kind == RunStateCorruptError.KIND == "run.state_corrupt"
    assert isinstance(err, RuntimeError)


def test_workflow_runtime_unavailable_error_kind_and_builtin() -> None:
    err = WorkflowRuntimeUnavailableError("sidecar down", run_id="r-1")
    assert err.kind == WorkflowRuntimeUnavailableError.KIND == "run.runtime_unavailable"
    assert isinstance(err, ConnectionError)
    assert isinstance(err, RunControllerError)


# ---------------------------------------------------------------------------
# to_dict() shape + stability
# ---------------------------------------------------------------------------


def test_run_not_found_to_dict_shape() -> None:
    err = RunNotFoundError("missing", run_id="r-1")
    assert err.to_dict() == {
        "kind": "run.not_found",
        "message": "missing",
        "run_id": "r-1",
    }


def test_run_state_conflict_to_dict_includes_extras() -> None:
    err = RunStateConflictError(
        "illegal",
        run_id="r-1",
        current_status="succeeded",
        attempted_status="cancelled",
    )
    assert err.to_dict() == {
        "kind": "run.state_conflict",
        "message": "illegal",
        "run_id": "r-1",
        "current_status": "succeeded",
        "attempted_status": "cancelled",
    }


def test_run_state_corrupt_to_dict_includes_cause() -> None:
    err = RunStateCorruptError("bad json", run_id="r-1", cause="ValueError('eof')")
    assert err.to_dict() == {
        "kind": "run.state_corrupt",
        "message": "bad json",
        "run_id": "r-1",
        "cause": "ValueError('eof')",
    }


def test_workflow_runtime_unavailable_to_dict_includes_cause() -> None:
    err = WorkflowRuntimeUnavailableError(
        "sidecar gone",
        run_id="r-1",
        cause="grpc.RpcError(deadline exceeded)",
    )
    assert err.to_dict() == {
        "kind": "run.runtime_unavailable",
        "message": "sidecar gone",
        "run_id": "r-1",
        "cause": "grpc.RpcError(deadline exceeded)",
    }


def test_to_dict_omits_run_id_value_when_unspecified() -> None:
    """``run_id`` is always present in the dict; absence is signalled
    with the JSON-safe ``None`` rather than a missing key, so audit
    consumers can rely on a fixed schema."""

    err = RunNotFoundError("missing")
    assert err.to_dict() == {
        "kind": "run.not_found",
        "message": "missing",
        "run_id": None,
    }


def test_to_dict_is_byte_stable_across_invocations() -> None:
    """``json.dumps(..., sort_keys=True)`` of two calls on the same
    instance must be byte-equal. The acceptance criterion is exactly
    this property."""

    err = RunStateConflictError(
        "illegal",
        run_id="r-1",
        current_status="succeeded",
        attempted_status="cancelled",
    )
    first = json.dumps(err.to_dict(), sort_keys=True)
    second = json.dumps(err.to_dict(), sort_keys=True)
    assert first == second


@pytest.mark.parametrize(
    "factory",
    [
        lambda: RunNotFoundError("m", run_id="r-1"),
        lambda: RunStateConflictError(
            "m", run_id="r-1", current_status="running", attempted_status="paused"
        ),
        lambda: RunStateCorruptError("m", run_id="r-1", cause="X"),
        lambda: WorkflowRuntimeUnavailableError("m", run_id="r-1", cause="Y"),
    ],
)
def test_every_subclass_to_dict_is_json_safe(
    factory: object,
) -> None:
    err = factory()  # type: ignore[operator]
    # Must serialise without a custom default.
    json.dumps(err.to_dict())


def test_to_dict_starts_with_kind_then_message_then_run_id() -> None:
    """Subclass extras must come AFTER the three locked top-level
    keys so audit consumers can rely on the prefix. Python preserves
    dict insertion order since 3.7, so we just check key ordering."""

    err = RunStateConflictError("m", run_id="r", current_status="a", attempted_status="b")
    keys = list(err.to_dict().keys())
    assert keys[:3] == ["kind", "message", "run_id"]


# ---------------------------------------------------------------------------
# Hash / equality
# ---------------------------------------------------------------------------


def test_equal_instances_compare_equal_and_hash_identically() -> None:
    a = RunNotFoundError("missing", run_id="r-1")
    b = RunNotFoundError("missing", run_id="r-1")
    assert a == b
    assert hash(a) == hash(b)


def test_differing_run_id_breaks_equality() -> None:
    a = RunNotFoundError("missing", run_id="r-1")
    b = RunNotFoundError("missing", run_id="r-2")
    assert a != b


def test_differing_extras_break_equality() -> None:
    a = RunStateConflictError(
        "m", run_id="r-1", current_status="running", attempted_status="paused"
    )
    b = RunStateConflictError(
        "m", run_id="r-1", current_status="running", attempted_status="cancelled"
    )
    assert a != b


def test_different_subclasses_with_same_fields_are_unequal() -> None:
    """A ``RunNotFoundError("m")`` must not collide with a
    ``RunStateConflictError("m")`` — they carry different KINDs and
    different builtins parents."""

    a = RunNotFoundError("m", run_id="r-1")
    b = RunStateConflictError("m", run_id="r-1")
    assert a != b


def test_subclasses_are_hashable_for_use_as_dict_keys() -> None:
    bucket = {
        RunNotFoundError("m", run_id="r-1"): "a",
        RunStateConflictError("m", run_id="r-1"): "b",
        RunStateCorruptError("m", run_id="r-1"): "c",
        WorkflowRuntimeUnavailableError("m", run_id="r-1"): "d",
    }
    assert len(bucket) == 4


def test_unrelated_type_comparison_returns_not_implemented() -> None:
    err = RunNotFoundError("m", run_id="r-1")
    assert err.__eq__("not-an-error") is NotImplemented


# ---------------------------------------------------------------------------
# __repr__
# ---------------------------------------------------------------------------


def test_repr_includes_class_name_and_locked_fields() -> None:
    err = RunNotFoundError("missing", run_id="r-1")
    rendered = repr(err)
    assert rendered.startswith("RunNotFoundError(")
    assert "kind='run.not_found'" in rendered
    assert "message='missing'" in rendered
    assert "run_id='r-1'" in rendered


def test_repr_includes_subclass_extras() -> None:
    err = RunStateConflictError(
        "m", run_id="r-1", current_status="running", attempted_status="paused"
    )
    rendered = repr(err)
    assert "current_status='running'" in rendered
    assert "attempted_status='paused'" in rendered


# ---------------------------------------------------------------------------
# str() / message exposure
# ---------------------------------------------------------------------------


def test_str_returns_message() -> None:
    """``str(err)`` is the human-readable message — same convention as
    the WF-IMPL-024 compile-time taxonomy."""

    err = RunNotFoundError("the run is missing", run_id="r-1")
    assert str(err) == "the run is missing"


def test_kind_attribute_equals_class_kind_constant() -> None:
    """``instance.kind`` and ``cls.KIND`` must always agree; the
    instance attribute is initialised from the class constant in
    ``__init__`` so callers can use either lookup."""

    err = RunNotFoundError("m")
    assert err.kind == RunNotFoundError.KIND

"""Tests for deterministic child-instance-id derivation (WF-IMPL-085).

Covers :mod:`custos_workflow.steps.sub_orchestration.ids`:

* :func:`child_instance_id` byte-stability, replay-identity, reserved
  separator rejection, and empty-component rejection.
* :func:`iteration_key` identity-field / primitive / index-fallback
  rules, percent-escaping of the reserved separator, and index
  validation.
* The documented duplicate-key collision behaviour.
"""

from __future__ import annotations

import pytest

from custos_workflow.steps.sub_orchestration import (
    APPROVAL_ITERATION_KEY,
    CHILD_INSTANCE_ID_SEPARATOR,
    WORKFLOW_ITERATION_KEY,
    ChildInstanceIdError,
    child_instance_id,
    iteration_key,
)

# ---------------------------------------------------------------------------
# child_instance_id
# ---------------------------------------------------------------------------


def test_child_instance_id_canonical_form() -> None:
    assert child_instance_id("run-1", "loop", "0") == "run-1/loop/0"


def test_child_instance_id_is_byte_stable() -> None:
    a = child_instance_id("run-1", "stepA", "k")
    b = child_instance_id("run-1", "stepA", "k")
    assert a == b
    assert a.encode("utf-8") == b.encode("utf-8")


def test_child_instance_id_reserved_keys() -> None:
    assert child_instance_id("r", "gate", APPROVAL_ITERATION_KEY) == "r/gate/approval"
    assert child_instance_id("r", "call", WORKFLOW_ITERATION_KEY) == "r/call/workflow"


@pytest.mark.parametrize(
    ("parent_run_id", "step_id", "key"),
    [
        ("", "s", "k"),
        ("r", "", "k"),
        ("r", "s", ""),
    ],
)
def test_child_instance_id_rejects_empty_component(
    parent_run_id: str, step_id: str, key: str
) -> None:
    with pytest.raises(ChildInstanceIdError, match="non-empty"):
        child_instance_id(parent_run_id, step_id, key)


@pytest.mark.parametrize(
    ("parent_run_id", "step_id", "key"),
    [
        ("r/x", "s", "k"),
        ("r", "s/x", "k"),
        ("r", "s", "k/x"),
    ],
)
def test_child_instance_id_rejects_reserved_separator(
    parent_run_id: str, step_id: str, key: str
) -> None:
    with pytest.raises(ChildInstanceIdError, match="reserved separator"):
        child_instance_id(parent_run_id, step_id, key)


# ---------------------------------------------------------------------------
# iteration_key — identity derivation
# ---------------------------------------------------------------------------


def test_iteration_key_mapping_id_field() -> None:
    assert iteration_key({"id": "abc", "name": "x"}, 7) == "abc"


def test_iteration_key_mapping_key_field_when_no_id() -> None:
    assert iteration_key({"key": "k1"}, 3) == "k1"


def test_iteration_key_prefers_id_over_key() -> None:
    assert iteration_key({"id": "the-id", "key": "the-key"}, 0) == "the-id"


def test_iteration_key_mapping_int_identity() -> None:
    assert iteration_key({"id": 42}, 5) == "42"


def test_iteration_key_mapping_bool_identity() -> None:
    assert iteration_key({"id": True}, 5) == "True"


def test_iteration_key_primitive_str() -> None:
    assert iteration_key("hello", 9) == "hello"


def test_iteration_key_primitive_int() -> None:
    assert iteration_key(123, 9) == "123"


# ---------------------------------------------------------------------------
# iteration_key — index fallback
# ---------------------------------------------------------------------------


def test_iteration_key_index_fallback_for_list_item() -> None:
    assert iteration_key([1, 2, 3], 4) == "4"


def test_iteration_key_index_fallback_for_mapping_without_identity() -> None:
    assert iteration_key({"name": "no-id-here"}, 2) == "2"


def test_iteration_key_index_fallback_for_null_identity_field() -> None:
    # A present-but-null ``id`` falls through to ``key``, then index.
    assert iteration_key({"id": None}, 6) == "6"


def test_iteration_key_index_fallback_for_float_item() -> None:
    # float is intentionally not a stable identity.
    assert iteration_key(1.5, 8) == "8"


def test_iteration_key_falls_through_null_id_to_key_field() -> None:
    assert iteration_key({"id": None, "key": "k"}, 1) == "k"


def test_iteration_key_empty_string_item_falls_back_to_index() -> None:
    assert iteration_key("", 4) == "4"


def test_iteration_key_empty_id_field_falls_back_to_index() -> None:
    assert iteration_key({"id": ""}, 7) == "7"


def test_iteration_key_empty_id_falls_through_to_key_field() -> None:
    # An empty ``id`` is "no stable identity"; the present ``key`` wins.
    assert iteration_key({"id": "", "key": "k2"}, 3) == "k2"


# ---------------------------------------------------------------------------
# iteration_key — escaping
# ---------------------------------------------------------------------------


def test_iteration_key_escapes_separator() -> None:
    key = iteration_key({"id": "a/b"}, 0)
    assert CHILD_INSTANCE_ID_SEPARATOR not in key
    assert key == "a%2Fb"


def test_iteration_key_escapes_percent_before_separator() -> None:
    # ``%`` must be escaped first so a literal ``%2F`` is not confused
    # with an escaped separator on the round trip.
    assert iteration_key({"id": "a%2Fb"}, 0) == "a%252Fb"


def test_iteration_key_escaped_key_is_valid_id_component() -> None:
    key = iteration_key({"id": "ns/name"}, 0)
    assert child_instance_id("r", "s", key) == "r/s/ns%2Fname"


# ---------------------------------------------------------------------------
# iteration_key — index validation
# ---------------------------------------------------------------------------


def test_iteration_key_rejects_negative_index() -> None:
    with pytest.raises(ChildInstanceIdError, match=">= 0"):
        iteration_key([], -1)


def test_iteration_key_rejects_bool_index() -> None:
    with pytest.raises(ChildInstanceIdError, match="non-negative int"):
        iteration_key([], True)


def test_iteration_key_rejects_non_int_index() -> None:
    with pytest.raises(ChildInstanceIdError, match="non-negative int"):
        iteration_key([], "0")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Collision rule
# ---------------------------------------------------------------------------


def test_duplicate_identity_keys_collide() -> None:
    # Documented rule: two items sharing an identity derive the same key
    # (and therefore the same child instance id). Detecting this is the
    # loop-expansion layer's responsibility, not this module's.
    a = iteration_key({"id": "dup"}, 0)
    b = iteration_key({"id": "dup"}, 1)
    assert a == b
    assert child_instance_id("r", "s", a) == child_instance_id("r", "s", b)


def test_index_fallback_keys_never_collide() -> None:
    keys = [iteration_key(object(), i) for i in range(5)]
    assert len(set(keys)) == len(keys)

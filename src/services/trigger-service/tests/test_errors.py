"""Tests for the locked ``trigger.*`` error taxonomy (TS-IMPL-004)."""

from __future__ import annotations

import json

import pytest

from custos_trigger.errors import (
    LOCKED_TRIGGER_KINDS,
    TriggerError,
    TriggerErrorKind,
)

#: The taxonomy locked by TS-IMPL-004. The grid below pins every code so any
#: addition, removal, or rename of a member must update this list deliberately.
_EXPECTED_KINDS: dict[str, str] = {
    "SUBSCRIPTION_NOT_FOUND": "trigger.subscription_not_found",
    "SELECTOR_INVALID": "trigger.selector_invalid",
    "SELECTOR_TYPE_ERROR": "trigger.selector_type_error",
    "DISPATCH_FAILED": "trigger.dispatch_failed",
    "RESUME_DIVERGENT": "trigger.resume_divergent",
    "DEDUP_DUPLICATE": "trigger.dedup_duplicate",
    "LOOP_DETECTED": "trigger.loop_detected",
}


def test_enum_grid_is_exhaustive_and_pinned() -> None:
    actual = {member.name: member.value for member in TriggerErrorKind}
    assert actual == _EXPECTED_KINDS


def test_locked_set_matches_enum_values() -> None:
    assert frozenset(_EXPECTED_KINDS.values()) == LOCKED_TRIGGER_KINDS
    assert frozenset(member.value for member in TriggerErrorKind) == LOCKED_TRIGGER_KINDS


def test_all_kinds_are_trigger_namespaced() -> None:
    for kind in LOCKED_TRIGGER_KINDS:
        assert kind.startswith("trigger.")


@pytest.mark.parametrize("kind", list(TriggerErrorKind))
def test_error_carries_kind_and_message(kind: TriggerErrorKind) -> None:
    err = TriggerError(kind, "boom")
    assert err.kind is kind
    assert err.message == "boom"
    assert err.details == {}
    assert str(err) == f"{kind.value}: boom"


@pytest.mark.parametrize("kind", list(TriggerErrorKind))
def test_to_dict_without_details_is_compact_and_json_safe(kind: TriggerErrorKind) -> None:
    payload = TriggerError(kind, "boom").to_dict()
    assert payload == {"kind": kind.value, "message": "boom"}
    # round-trips through JSON unchanged
    assert json.loads(json.dumps(payload)) == payload


def test_to_dict_includes_details_when_present() -> None:
    err = TriggerError(
        TriggerErrorKind.DISPATCH_FAILED,
        "exhausted retries",
        details={"attempts": 5, "subscription_id": "sub-1"},
    )
    payload = err.to_dict()
    assert payload == {
        "kind": "trigger.dispatch_failed",
        "message": "exhausted retries",
        "details": {"attempts": 5, "subscription_id": "sub-1"},
    }
    assert json.loads(json.dumps(payload)) == payload


def test_details_are_copied_not_aliased() -> None:
    source: dict[str, object] = {"attempts": 1}
    err = TriggerError(TriggerErrorKind.DISPATCH_FAILED, "boom", details=source)
    source["attempts"] = 99
    assert err.details == {"attempts": 1}


def test_trigger_error_is_an_exception() -> None:
    with pytest.raises(TriggerError) as excinfo:
        raise TriggerError(TriggerErrorKind.LOOP_DETECTED, "depth exceeded")
    assert excinfo.value.kind is TriggerErrorKind.LOOP_DETECTED

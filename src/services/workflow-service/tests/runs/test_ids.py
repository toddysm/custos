"""Tests for the deterministic ``runId`` derivation (WF-IMPL-030)."""

from __future__ import annotations

from uuid import UUID, uuid5

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from custos_workflow.runs import RUN_ID_NAMESPACE, derive_run_id

# ---------------------------------------------------------------------------
# Locked namespace value
# ---------------------------------------------------------------------------


def test_run_id_namespace_is_locked_to_published_uuid() -> None:
    """The namespace UUID is part of the public wire contract. Changing
    it would invalidate every previously issued deterministic ``runId``,
    so this test exists to fail loudly if anyone edits the constant."""

    assert UUID("d8e6c1a4-0f3a-4f8a-9f1d-1c9b6e6a9c2d") == RUN_ID_NAMESPACE


# ---------------------------------------------------------------------------
# Determinism (Hypothesis)
# ---------------------------------------------------------------------------


@given(
    workspace_id=st.text(min_size=1, max_size=64),
    idempotency_key=st.text(min_size=1, max_size=128),
)
@settings(max_examples=1000)
def test_derive_run_id_is_deterministic_for_same_inputs(
    workspace_id: str, idempotency_key: str
) -> None:
    first = derive_run_id(workspace_id, idempotency_key)
    second = derive_run_id(workspace_id, idempotency_key)
    assert first == second


@given(
    workspace_a=st.text(min_size=1, max_size=64),
    workspace_b=st.text(min_size=1, max_size=64),
    idempotency_key=st.text(min_size=1, max_size=128),
)
@settings(max_examples=200)
def test_same_key_under_different_workspaces_yields_different_ids(
    workspace_a: str, workspace_b: str, idempotency_key: str
) -> None:
    if workspace_a == workspace_b:
        # Skip: the strategy may produce equal workspaces; we only
        # assert separation when they actually differ.
        return
    a = derive_run_id(workspace_a, idempotency_key)
    b = derive_run_id(workspace_b, idempotency_key)
    assert a != b


def test_derive_run_id_matches_explicit_uuid5_formula() -> None:
    """The on-the-wire derivation must equal ``uuid5(NS, ws|key)`` so
    external consumers can independently compute the same id."""

    workspace_id = "ws-42"
    idempotency_key = "user-supplied-key"
    expected = str(uuid5(RUN_ID_NAMESPACE, f"{workspace_id}|{idempotency_key}"))
    assert derive_run_id(workspace_id, idempotency_key) == expected


# ---------------------------------------------------------------------------
# No-key path
# ---------------------------------------------------------------------------


def test_no_key_returns_distinct_uuids_per_call() -> None:
    workspace_id = "ws-1"
    ids = {derive_run_id(workspace_id, None) for _ in range(50)}
    # 50 random UUIDv4 calls should not collide.
    assert len(ids) == 50


def test_empty_key_is_treated_as_no_key_supplied() -> None:
    workspace_id = "ws-1"
    first = derive_run_id(workspace_id, "")
    second = derive_run_id(workspace_id, "")
    # Two calls with the empty-string key are independent UUIDv4s.
    assert first != second


def test_no_key_produces_valid_uuid_string() -> None:
    value = derive_run_id("ws-1", None)
    # Must round-trip through :class:`UUID`.
    UUID(value)


def test_deterministic_path_produces_valid_uuid_string() -> None:
    value = derive_run_id("ws-1", "key")
    UUID(value)


# ---------------------------------------------------------------------------
# Workspace validation
# ---------------------------------------------------------------------------


def test_empty_workspace_id_raises_value_error() -> None:
    with pytest.raises(ValueError):
        derive_run_id("", "some-key")


def test_empty_workspace_id_raises_even_without_key() -> None:
    with pytest.raises(ValueError):
        derive_run_id("", None)


# ---------------------------------------------------------------------------
# Cross-key separation
# ---------------------------------------------------------------------------


def test_same_workspace_different_keys_yield_different_ids() -> None:
    workspace_id = "ws-1"
    a = derive_run_id(workspace_id, "key-a")
    b = derive_run_id(workspace_id, "key-b")
    assert a != b


def test_separator_is_not_ambiguous() -> None:
    """``workspace_id`` + ``"|"`` + ``idempotency_key`` is the locked
    encoding. The "|" separator is structural; this test pins it so a
    rewrite that ever drops or changes the separator fails loudly."""

    # If the encoding were just ``workspace_id + idempotency_key`` (no
    # separator), then ("ab", "c") and ("a", "bc") would collide. Pin
    # the separator by asserting they DON'T collide.
    a = derive_run_id("ab", "c")
    b = derive_run_id("a", "bc")
    assert a != b

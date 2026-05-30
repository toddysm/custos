"""Tests for the deterministic ``IdempotencyTriple`` (WF-IMPL-047)."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from custos_workflow.steps import (
    IdempotencyTriple,
    IdempotencyTripleError,
    derive_triple,
)
from custos_workflow.steps.idempotency import IDEMPOTENCY_TRIPLE_SEPARATOR

# ---------------------------------------------------------------------------
# Wire-format constant
# ---------------------------------------------------------------------------


def test_separator_is_locked_to_pipe() -> None:
    """The Activity Runtime Manager and Connector Service parse on
    the canonical ``"|"`` separator. Changing it would invalidate
    every previously issued idempotency key, so this test exists to
    fail loudly if anyone edits the constant."""

    assert IDEMPOTENCY_TRIPLE_SEPARATOR == "|"


# ---------------------------------------------------------------------------
# Construction + validation
# ---------------------------------------------------------------------------


def test_construct_with_valid_components() -> None:
    triple = IdempotencyTriple(run_id="r1", step_id="s1", attempt=1)
    assert triple.run_id == "r1"
    assert triple.step_id == "s1"
    assert triple.attempt == 1


def test_derive_triple_is_a_thin_wrapper() -> None:
    direct = IdempotencyTriple(run_id="r1", step_id="s1", attempt=2)
    via_factory = derive_triple(run_id="r1", step_id="s1", attempt=2)
    assert direct == via_factory


def test_triple_is_frozen_and_hashable() -> None:
    triple = IdempotencyTriple(run_id="r1", step_id="s1", attempt=1)
    with pytest.raises(Exception):  # noqa: B017 — frozen dataclass raises FrozenInstanceError
        triple.attempt = 2  # type: ignore[misc]
    # Hashable because frozen=True + slots=True.
    assert {triple} == {IdempotencyTriple(run_id="r1", step_id="s1", attempt=1)}


@pytest.mark.parametrize("attempt", [0, -1, -100])
def test_attempt_below_one_raises_value_error(attempt: int) -> None:
    with pytest.raises(ValueError, match="attempt must be >= 1"):
        derive_triple(run_id="r1", step_id="s1", attempt=attempt)


def test_attempt_bool_true_is_rejected_even_though_int_subclass() -> None:
    # ``True`` is an int(1) in Python; explicit reject so callers
    # cannot accidentally short-circuit type-checks at the boundary.
    with pytest.raises(ValueError, match="attempt must be an int"):
        derive_triple(run_id="r1", step_id="s1", attempt=True)


def test_attempt_non_integer_is_rejected() -> None:
    with pytest.raises(ValueError, match="attempt must be an int"):
        derive_triple(run_id="r1", step_id="s1", attempt=1.5)  # type: ignore[arg-type]


def test_empty_run_id_raises() -> None:
    with pytest.raises(IdempotencyTripleError, match="run_id must be a non-empty"):
        derive_triple(run_id="", step_id="s1", attempt=1)


def test_empty_step_id_raises() -> None:
    with pytest.raises(IdempotencyTripleError, match="step_id must be a non-empty"):
        derive_triple(run_id="r1", step_id="", attempt=1)


def test_pipe_in_step_id_is_rejected() -> None:
    with pytest.raises(IdempotencyTripleError, match="step_id must not contain"):
        derive_triple(run_id="r1", step_id="a|b", attempt=1)


def test_pipe_in_run_id_is_rejected() -> None:
    with pytest.raises(IdempotencyTripleError, match="run_id must not contain"):
        derive_triple(run_id="r|x", step_id="s1", attempt=1)


# ---------------------------------------------------------------------------
# Canonical wire form
# ---------------------------------------------------------------------------


def test_to_str_uses_pipe_separated_canonical_form() -> None:
    triple = derive_triple(run_id="run-123", step_id="step-A", attempt=4)
    assert triple.to_str() == "run-123|step-A|4"


def test_to_str_is_byte_equal_across_500_calls() -> None:
    """Determinism is the entire point: re-deriving the same triple
    must produce byte-equal output across Dapr Workflow replays."""

    triple = derive_triple(run_id="run-xyz", step_id="step-unicode", attempt=7)
    expected = triple.to_str()
    samples = {
        derive_triple(run_id="run-xyz", step_id="step-unicode", attempt=7).to_str()
        for _ in range(500)
    }
    assert samples == {expected}


# ---------------------------------------------------------------------------
# Round-trip parsing
# ---------------------------------------------------------------------------


_SAFE_TEXT = st.text(
    alphabet=st.characters(
        blacklist_categories=["Cs"],
        blacklist_characters=("|",),
    ),
    min_size=1,
    max_size=64,
)


@given(
    run_id=_SAFE_TEXT,
    step_id=_SAFE_TEXT,
    attempt=st.integers(min_value=1, max_value=1_000_000),
)
@settings(max_examples=200)
def test_round_trip_through_canonical_form(run_id: str, step_id: str, attempt: int) -> None:
    triple = derive_triple(run_id=run_id, step_id=step_id, attempt=attempt)
    assert IdempotencyTriple.from_str(triple.to_str()) == triple


def test_from_str_rejects_missing_separator() -> None:
    with pytest.raises(IdempotencyTripleError, match="exactly two"):
        IdempotencyTriple.from_str("only-one-piece")


def test_from_str_rejects_too_many_separators() -> None:
    # Four parts: would otherwise round-trip ambiguously.
    with pytest.raises(IdempotencyTripleError, match="exactly two"):
        IdempotencyTriple.from_str("a|b|c|4")


def test_from_str_rejects_non_integer_attempt() -> None:
    with pytest.raises(IdempotencyTripleError, match="attempt component"):
        IdempotencyTriple.from_str("run|step|not-a-number")


def test_from_str_rejects_zero_attempt() -> None:
    # Parsed components fail the >= 1 check in the dataclass and the
    # wrapped error preserves the message.
    with pytest.raises(IdempotencyTripleError, match=">= 1"):
        IdempotencyTriple.from_str("run|step|0")


def test_from_str_rejects_empty_run_id_component() -> None:
    with pytest.raises(IdempotencyTripleError, match="run_id must be a non-empty"):
        IdempotencyTriple.from_str("|step|1")


def test_from_str_rejects_empty_step_id_component() -> None:
    with pytest.raises(IdempotencyTripleError, match="step_id must be a non-empty"):
        IdempotencyTriple.from_str("run||1")

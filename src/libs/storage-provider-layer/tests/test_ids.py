"""Tests for SPL identifier aliases.

These are mostly sanity checks. mypy verifies the type-level distinctions;
at runtime every NewType is just a str.
"""

from __future__ import annotations

from custos_spl.ids import (
    PrincipalId,
    RunId,
    StepId,
    WorkflowId,
    WorkspaceId,
)


def test_ids_are_strings_at_runtime() -> None:
    ws = WorkspaceId("ws-123")
    assert isinstance(ws, str)
    assert ws == "ws-123"


def test_ids_compare_by_value() -> None:
    a = RunId("run-1")
    b = RunId("run-1")
    c = RunId("run-2")
    assert a == b
    assert a != c
    assert hash(a) == hash(b)


def test_ids_usable_in_sets_and_dicts() -> None:
    ws_a = WorkspaceId("ws-a")
    ws_b = WorkspaceId("ws-b")
    seen: set[WorkspaceId] = {ws_a, ws_b, WorkspaceId("ws-a")}
    assert len(seen) == 2

    counters: dict[StepId, int] = {StepId("s1"): 1, StepId("s2"): 2}
    assert counters[StepId("s1")] == 1


def test_distinct_id_aliases_are_runtime_compatible() -> None:
    """NewTypes are runtime str, so cross-comparison of equal strings is True.
    The point of the NewType wrappers is the mypy-time distinction, not a
    runtime one. This test pins that runtime behavior so callers know what
    to expect.
    """
    assert WorkflowId("x") == PrincipalId("x")  # type: ignore[comparison-overlap]

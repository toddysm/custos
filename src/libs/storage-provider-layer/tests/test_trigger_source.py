"""Shared external resume associations validate and preserve their wire identity."""

from datetime import timedelta
from typing import Any

import pytest

from custos_spl.interfaces.trigger_source import (
    ResumeSource,
    parse_resume_duration,
    validate_resume_context,
)


def test_source_is_canonical_hashable_and_round_trips() -> None:
    """Mode ordering does not produce a different replay identity."""
    first = ResumeSource("registry", ("push", "pull"), 10)
    second = ResumeSource.from_wire(
        {
            "connectorInstanceId": "registry",
            "deliveryModes": ["pull", "push"],
            "pollIntervalSeconds": 10,
        }
    )
    assert first == second and hash(first) == hash(second)
    assert first.to_wire()["deliveryModes"] == ["pull", "push"]
    assert "pollIntervalSeconds" not in ResumeSource("registry", ("push",)).to_wire()


@pytest.mark.parametrize(
    "value",
    [
        None,
        [],
        {},
        {"connectorInstanceId": 1, "deliveryModes": ["push"]},
        {"connectorInstanceId": " ", "deliveryModes": ["push"]},
        {"connectorInstanceId": " ${{ inputs.key }} ", "deliveryModes": ["push"]},
        {"connectorInstanceId": "${{}}", "deliveryModes": ["push"]},
        {"connectorInstanceId": "c", "deliveryModes": "push"},
        {"connectorInstanceId": "c", "deliveryModes": []},
        {"connectorInstanceId": "c", "deliveryModes": ["push", "push"]},
        {"connectorInstanceId": "c", "deliveryModes": ["other"]},
        {"connectorInstanceId": "c", "deliveryModes": ["push"], "pollIntervalSeconds": 10},
        {"connectorInstanceId": "c", "deliveryModes": ["pull"], "pollIntervalSeconds": 9},
        {"connectorInstanceId": "c", "deliveryModes": ["pull"], "pollIntervalSeconds": True},
        {"connectorInstanceId": "c", "deliveryModes": ["pull"], "pollIntervalSeconds": "10"},
        {"connectorInstanceId": "c", "deliveryModes": ["push"], "credentials": "not-allowed"},
    ],
)
def test_invalid_wire_sources_raise_value_error(value: Any) -> None:
    """Malformed persisted data has a predictable failure, including non-object roots."""
    with pytest.raises(ValueError):
        ResumeSource.from_wire(value)


@pytest.mark.parametrize(
    ("workspace", "selector"),
    [(None, "true"), ("_resume", "true"), ("", "true"), ("ws", None), ("ws", " ")],
)
def test_external_wait_requires_workspace_and_selector(
    workspace: str | None, selector: str | None
) -> None:
    """Source associations cannot enter the legacy global namespace or match every event."""
    with pytest.raises(ValueError):
        validate_resume_context(workspace, ResumeSource("c", ("push",)), selector)


def test_legacy_and_scoped_contexts_remain_distinct() -> None:
    """Legacy unscoped waits remain valid; workspace-only registration is not ambiguous."""
    validate_resume_context(None, None, None)
    validate_resume_context("ws", ResumeSource("c", ("pull",)), "event.data.id == 'one'")
    with pytest.raises(ValueError, match="requires a source"):
        validate_resume_context("ws", None, None)


@pytest.mark.parametrize("value", ["P9999999999W", "P999999999D", "PT0.0000001S", "PT0S", "bad"])
def test_duration_contract_rejects_unrepresentable_values(value: str) -> None:
    """All consumers share positive, representable duration semantics."""
    with pytest.raises(ValueError):
        parse_resume_duration(value)


def test_duration_contract_preserves_fractional_components() -> None:
    """Large integer day components cannot erase microseconds."""
    assert parse_resume_duration("P100000DT0.000001S") == timedelta(days=100000, microseconds=1)


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("PT10000000000.000001S", timedelta(seconds=10000000000, microseconds=1)),
        ("PT100000000000.000001S", timedelta(seconds=100000000000, microseconds=1)),
        ("PT1.0000015S", timedelta(seconds=1, microseconds=2)),
        ("PT1.0000025S", timedelta(seconds=1, microseconds=2)),
        ("PT1.9999996S", timedelta(seconds=2)),
    ],
)
def test_duration_uses_exact_fraction_rounding(source: str, expected: timedelta) -> None:
    """Whole seconds cannot consume fractional precision; ties round to even microseconds."""
    assert parse_resume_duration(source) == expected

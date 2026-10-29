"""Event-root input templates resolve to detached JSON before durable dispatch capture."""

from datetime import UTC, datetime

import pytest

from custos_trigger.events import EventSource, NormalizedEvent
from custos_trigger.input_mapping import resolve_input_mapping
from custos_trigger.models import SourceType
from custos_trigger.selector import SelectorEvaluator
from custos_trigger.source_config import SourceConfigurationError


def event() -> NormalizedEvent:
    """Supply immutable clock provenance and heterogeneous JSON values."""
    return NormalizedEvent(
        event_id="event",
        kind="cron.tick",
        source=EventSource(type=SourceType.SCHEDULED, occurred_at="2026-01-01T00:00:00+00:00"),
        data={
            "number": 2,
            "skippedCount": 2,
            "subscriptionRevision": 3,
            "text": "${{ not.reinterpreted }}",
            "nested": {"value": True},
        },
    )


def test_mapping_resolves_values_interpolation_and_nested_containers() -> None:
    """Types and literal data survive, authored configuration remains unchanged."""
    incoming = event()
    mapping = {
        "number": "${{ event.data.number }}",
        "mixed": "count=${{ event.data.number }}",
        "nested": ["${{ event.data.nested }}", None, False],
        "data": "${{ event.data }}",
        "literal_result": "${{ event.data.text }}",
        "clock": "${{ now() }}",
        "occurrences": "${{ event.data.skippedCount + 1 }}",
        "revision": "revision=${{ event.data.subscriptionRevision + 1 }}",
    }
    resolved = resolve_input_mapping(
        mapping,
        incoming,
        evaluator=SelectorEvaluator(),
        subscription_id="schedule",
    )
    assert resolved["number"] == 2 and resolved["mixed"] == "count=2"
    assert resolved["nested"] == [{"value": True}, None, False]
    assert resolved["data"] == incoming.data
    assert resolved["literal_result"] == "${{ not.reinterpreted }}"
    assert datetime.fromisoformat(resolved["clock"]) == datetime(2026, 1, 1, tzinfo=UTC)
    assert mapping["number"] == "${{ event.data.number }}"
    assert resolved["occurrences"] == 3
    assert type(resolved["occurrences"]) is int
    assert resolved["revision"] == "revision=4"
    resolved["data"]["nested"]["value"] = False
    assert incoming.data["nested"]["value"] is True


@pytest.mark.parametrize("value", ["${{ missing.value }}", "${{ 1 / 0 }}", "${{ event.data.number"])
def test_mapping_failures_are_explicit(value: str) -> None:
    """Missing bindings, evaluation failures and malformed templates never become literals."""
    with pytest.raises(SourceConfigurationError, match="could not be resolved"):
        resolve_input_mapping(
            {"value": value},
            event(),
            evaluator=SelectorEvaluator(),
            subscription_id="schedule",
        )

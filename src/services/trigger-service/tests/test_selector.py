"""CEL selector evaluator tests (TS-IMPL-011)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

import custos_trigger.selector as selector_module
from custos_trigger.errors import TriggerErrorKind
from custos_trigger.events import EventSource, NormalizedEvent
from custos_trigger.models import SelectorMatchType, SourceType
from custos_trigger.selector import (
    CompiledSelector,
    SelectorEvaluator,
    SelectorInvalidError,
    SelectorTypeError,
    compute_expr_hash,
    desugar_legacy_selector,
)

_OCCURRED_AT = "2026-06-04T12:00:00Z"


def _event(
    *,
    kind: str = "workflow.completed",
    subject: str = "",
    data: dict[str, Any] | None = None,
    source_type: SourceType = SourceType.INTERNAL,
) -> NormalizedEvent:
    return NormalizedEvent(
        event_id="evt-1",
        source=EventSource(type=source_type, occurred_at=_OCCURRED_AT),
        kind=kind,
        subject=subject,
        data=data or {},
    )


# --- compute_expr_hash -------------------------------------------------------


def test_compute_expr_hash_is_deterministic() -> None:
    assert compute_expr_hash('event.kind == "x"') == compute_expr_hash('event.kind == "x"')


def test_compute_expr_hash_varies_by_expr() -> None:
    assert compute_expr_hash('event.kind == "a"') != compute_expr_hash('event.kind == "b"')


# --- compile -----------------------------------------------------------------


def test_compile_valid_selector_returns_compiled() -> None:
    evaluator = SelectorEvaluator()
    compiled = evaluator.compile('event.kind == "workflow.completed"', subscription_id="sub-1")
    assert isinstance(compiled, CompiledSelector)
    assert compiled.subscription_id == "sub-1"
    assert compiled.expr_hash == compute_expr_hash('event.kind == "workflow.completed"')


def test_compile_caches_typed_ast() -> None:
    evaluator = SelectorEvaluator()
    first = evaluator.compile('event.kind == "x"', subscription_id="sub-1")
    second = evaluator.compile('event.kind == "x"', subscription_id="sub-1")
    assert first is second


def test_compile_cache_is_keyed_by_subscription() -> None:
    evaluator = SelectorEvaluator()
    first = evaluator.compile('event.kind == "x"', subscription_id="sub-1")
    second = evaluator.compile('event.kind == "x"', subscription_id="sub-2")
    assert first is not second


def test_compile_rejects_syntax_error() -> None:
    evaluator = SelectorEvaluator()
    with pytest.raises(SelectorInvalidError) as excinfo:
        evaluator.compile("event.kind ==", subscription_id="sub-1")
    assert excinfo.value.kind == TriggerErrorKind.SELECTOR_INVALID


def test_compile_rejects_method_call_syntax() -> None:
    # The custos-cel subset disallows receiver.method(args); the design's
    # startsWith prefix desugar is therefore not expressible.
    evaluator = SelectorEvaluator()
    with pytest.raises(SelectorInvalidError):
        evaluator.compile('event.data.repository.startsWith("ghcr.io/")', subscription_id="sub-1")


def test_compile_rejects_unbound_name() -> None:
    evaluator = SelectorEvaluator()
    with pytest.raises(SelectorInvalidError):
        evaluator.compile('inputs.foo == "x"', subscription_id="sub-1")


def test_compile_rejects_type_mismatch() -> None:
    evaluator = SelectorEvaluator()
    with pytest.raises(SelectorInvalidError):
        evaluator.compile("event.kind && event.subject", subscription_id="sub-1")


# --- evaluate ----------------------------------------------------------------


def test_evaluate_true() -> None:
    evaluator = SelectorEvaluator()
    compiled = evaluator.compile('event.kind == "workflow.completed"', subscription_id="sub-1")
    assert evaluator.evaluate(compiled, _event()) is True


def test_evaluate_false() -> None:
    evaluator = SelectorEvaluator()
    compiled = evaluator.compile('event.kind == "workflow.failed"', subscription_id="sub-1")
    assert evaluator.evaluate(compiled, _event(kind="workflow.completed")) is False


def test_evaluate_reads_event_data() -> None:
    evaluator = SelectorEvaluator()
    compiled = evaluator.compile('event.data.status == "succeeded"', subscription_id="sub-1")
    assert evaluator.evaluate(compiled, _event(data={"status": "succeeded"})) is True
    assert evaluator.evaluate(compiled, _event(data={"status": "failed"})) is False


def test_evaluate_accepts_plain_mapping() -> None:
    evaluator = SelectorEvaluator()
    compiled = evaluator.compile('event.kind == "workflow.completed"', subscription_id="sub-1")
    mapping: Mapping[str, Any] = _event().model_dump(by_alias=True)
    assert evaluator.evaluate(compiled, mapping) is True


def test_evaluate_non_bool_raises_type_error() -> None:
    evaluator = SelectorEvaluator()
    compiled = evaluator.compile("event.kind", subscription_id="sub-1")
    with pytest.raises(SelectorTypeError) as excinfo:
        evaluator.evaluate(compiled, _event())
    assert excinfo.value.kind == TriggerErrorKind.SELECTOR_TYPE_ERROR
    assert excinfo.value.details == {"subscriptionId": "sub-1", "resultType": "str"}


def test_evaluate_missing_field_is_no_match() -> None:
    evaluator = SelectorEvaluator()
    compiled = evaluator.compile('event.data.absent == "x"', subscription_id="sub-1")
    assert evaluator.evaluate(compiled, _event(data={})) is False


def test_evaluate_timeout_is_no_match(monkeypatch: pytest.MonkeyPatch) -> None:
    from custos_cel import EvalTimeoutError

    def _raise_timeout(*args: Any, **kwargs: Any) -> Any:
        raise EvalTimeoutError("budget exceeded", elapsed_ms=200, timeout_ms=100)

    monkeypatch.setattr(selector_module, "cel_evaluate", _raise_timeout)
    evaluator = SelectorEvaluator()
    compiled = evaluator.compile('event.kind == "workflow.completed"', subscription_id="sub-1")
    assert evaluator.evaluate(compiled, _event()) is False


def test_matches_compiles_and_evaluates() -> None:
    evaluator = SelectorEvaluator()
    assert (
        evaluator.matches('event.kind == "workflow.completed"', _event(), subscription_id="sub-1")
        is True
    )


def test_clear_drops_cache() -> None:
    evaluator = SelectorEvaluator()
    first = evaluator.compile('event.kind == "x"', subscription_id="sub-1")
    evaluator.clear()
    second = evaluator.compile('event.kind == "x"', subscription_id="sub-1")
    assert first is not second


# --- desugar_legacy_selector -------------------------------------------------


def test_desugar_eq() -> None:
    expr = desugar_legacy_selector(
        field="status", match_type=SelectorMatchType.EQ, value="succeeded"
    )
    assert expr == 'event.data.status == "succeeded"'
    evaluator = SelectorEvaluator()
    compiled = evaluator.compile(expr, subscription_id="sub-1")
    assert evaluator.evaluate(compiled, _event(data={"status": "succeeded"})) is True
    assert evaluator.evaluate(compiled, _event(data={"status": "failed"})) is False


def test_desugar_eq_escapes_value() -> None:
    expr = desugar_legacy_selector(field="label", match_type=SelectorMatchType.EQ, value='a"b')
    evaluator = SelectorEvaluator()
    compiled = evaluator.compile(expr, subscription_id="sub-1")
    assert evaluator.evaluate(compiled, _event(data={"label": 'a"b'})) is True


def test_desugar_prefix_matches_like_starts_with() -> None:
    expr = desugar_legacy_selector(
        field="repository", match_type=SelectorMatchType.PREFIX, value="ghcr.io/acme/"
    )
    evaluator = SelectorEvaluator()
    compiled = evaluator.compile(expr, subscription_id="sub-1")
    assert evaluator.evaluate(compiled, _event(data={"repository": "ghcr.io/acme/app"})) is True
    assert evaluator.evaluate(compiled, _event(data={"repository": "ghcr.io/other/app"})) is False
    # The bare prefix without a trailing path component does not match.
    assert evaluator.evaluate(compiled, _event(data={"repository": "ghcr.io/acme"})) is False


def test_desugar_prefix_empty_value_matches_any_string() -> None:
    expr = desugar_legacy_selector(
        field="repository", match_type=SelectorMatchType.PREFIX, value=""
    )
    assert expr == 'event.data.repository >= ""'
    evaluator = SelectorEvaluator()
    compiled = evaluator.compile(expr, subscription_id="sub-1")
    assert evaluator.evaluate(compiled, _event(data={"repository": "anything"})) is True


def test_desugar_prefix_carries_past_max_code_point() -> None:
    # Final code point at U+10FFFF has no successor, so the bound carries to the
    # previous code point.
    expr = desugar_legacy_selector(
        field="repository", match_type=SelectorMatchType.PREFIX, value="a\U0010ffff"
    )
    evaluator = SelectorEvaluator()
    compiled = evaluator.compile(expr, subscription_id="sub-1")
    assert evaluator.evaluate(compiled, _event(data={"repository": "a\U0010ffffz"})) is True
    assert evaluator.evaluate(compiled, _event(data={"repository": "b"})) is False


def test_desugar_prefix_skips_surrogate_range() -> None:
    # Incrementing U+D7FF must skip the surrogate block and land on U+E000.
    expr = desugar_legacy_selector(
        field="repository", match_type=SelectorMatchType.PREFIX, value="\ud7ff"
    )
    evaluator = SelectorEvaluator()
    compiled = evaluator.compile(expr, subscription_id="sub-1")
    assert evaluator.evaluate(compiled, _event(data={"repository": "\ud7ffx"})) is True


def test_desugar_prefix_without_successor_is_rejected() -> None:
    # A value that is entirely U+10FFFF has no lexicographic successor.
    with pytest.raises(SelectorInvalidError):
        desugar_legacy_selector(
            field="repository",
            match_type=SelectorMatchType.PREFIX,
            value="\U0010ffff\U0010ffff",
        )


def test_desugar_dotted_field() -> None:
    expr = desugar_legacy_selector(
        field="outputs.image", match_type=SelectorMatchType.EQ, value="x"
    )
    assert expr == 'event.data.outputs.image == "x"'


@pytest.mark.parametrize("match_type", [SelectorMatchType.REGEX, SelectorMatchType.JSONPATH])
def test_desugar_unsupported_match_type(match_type: SelectorMatchType) -> None:
    with pytest.raises(SelectorInvalidError) as excinfo:
        desugar_legacy_selector(field="repository", match_type=match_type, value=".*")
    assert excinfo.value.kind == TriggerErrorKind.SELECTOR_INVALID


def test_desugar_rejects_invalid_field() -> None:
    with pytest.raises(SelectorInvalidError):
        desugar_legacy_selector(field="my-field", match_type=SelectorMatchType.EQ, value="x")


def test_desugar_rejects_empty_field() -> None:
    with pytest.raises(SelectorInvalidError):
        desugar_legacy_selector(field="", match_type=SelectorMatchType.EQ, value="x")


def test_desugar_cel_match_type_is_programming_error() -> None:
    with pytest.raises(ValueError, match="not a legacy match type"):
        desugar_legacy_selector(field="status", match_type=SelectorMatchType.CEL, value="x")

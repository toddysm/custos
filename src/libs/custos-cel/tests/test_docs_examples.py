"""Smoke test for the developer-facing CEL expressions doc (WF-IMPL-012).

The doc at ``docs/developers/cel-expressions.md`` advertises five
worked examples. Acceptance criterion: *every* example expression must
be copy-paste-runnable against the ``custos_cel`` public API. This
test is the verification.

For each example we:

1. Parse the literal CEL source string with ``custos_cel.parse()``.
2. Type-check it against a representative ``SchemaBindings``.
3. Evaluate it against a representative ``BindingScope`` and
   ``FixedClock``.
4. Assert the value matches what the doc text describes.

Additionally, the test asserts that the exact CEL source strings
appearing in the doc are byte-identical to the ones in this file —
so doc drift (rename a binding, change an operator) trips this test
loudly.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from custos_cel import (
    BindingScope,
    FixedClock,
    IntType,
    RunInfo,
    SchemaBindings,
    StepBinding,
    WorkflowInfo,
    evaluate,
    parse,
    type_check,
)

# ---------------------------------------------------------------------------
# Test fixtures shared by every example
# ---------------------------------------------------------------------------

_DOC_PATH = (
    Path(__file__).resolve().parent.parent.parent.parent.parent
    / "docs"
    / "developers"
    / "cel-expressions.md"
)
_FIXED_TS = datetime(2026, 5, 22, 12, 0, 0, tzinfo=UTC)


def _clock() -> FixedClock:
    return FixedClock(_FIXED_TS)


def _inputs_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "image": {"type": "string"},
            "enabled": {"type": "boolean"},
            "tags": {"type": "array", "items": {"type": "string"}},
            "targets": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "image": {"type": "string"},
                        "tag": {"type": "string"},
                    },
                },
            },
        },
    }


def _step_outputs_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {"critical": {"type": "integer"}},
    }


def _bindings(*, with_let_total: bool = False) -> SchemaBindings:
    """Type-checker bindings covering every example.

    ``with_let_total`` adds ``let.totalCritical: int`` for the
    ``let`` ternary-label example which references a prior ``let``
    binding from the same block.
    """
    let_decls: dict[str, Any] = {}
    if with_let_total:
        let_decls["totalCritical"] = IntType()
    return SchemaBindings(
        inputs=_inputs_schema(),
        prior_steps=(
            ("scan", _step_outputs_schema()),
            ("scan-alt", _step_outputs_schema()),
        ),
        let=let_decls,
    )


def _scope(
    *,
    inputs: Mapping[str, Any] | None = None,
    steps: Mapping[str, StepBinding] | None = None,
    let: Mapping[str, Any] | None = None,
) -> BindingScope:
    return BindingScope(
        run=RunInfo(id="run-1", workspace="ws-1"),
        workflow=WorkflowInfo(name="wf", version="v1"),
        now=_clock().now,
        inputs=dict(inputs or {}),
        steps=dict(steps or {}),
        let=dict(let or {}),
    )


def _step(**outputs: Any) -> StepBinding:
    sb = StepBinding(outputs=dict(outputs))
    sb.seal()
    return sb


def _run(source: str, scope: BindingScope, *, with_let_total: bool = False) -> Any:
    ast = parse(source)
    typed = type_check(ast, _bindings(with_let_total=with_let_total))
    return evaluate(typed, scope, _clock())


# ---------------------------------------------------------------------------
# Doc-source-string equality — each example expression lives twice
# (once in the doc, once in this file). The doc is the source of
# truth; this guard catches silent edits to the doc that drift away
# from the verified expressions.
# ---------------------------------------------------------------------------

_EXAMPLE_SOURCES: dict[str, str] = {
    "let_total": 'steps.scan.outputs.critical + steps["scan-alt"].outputs.critical',
    "let_label": 'let.totalCritical > 0 ? "block" : "allow"',
    "if_guard": "inputs.enabled && size(inputs.tags) > 0",
    "with_mapping": 'inputs.image + ":" + inputs.tags[0]',
    "for_iterable": "inputs.targets",
}


def test_every_example_expression_appears_verbatim_in_the_doc() -> None:
    """Doc text must contain each CEL source string at least once.

    Catches silent rewordings of the example expressions that would
    leave the doc out of sync with this smoke test.
    """
    assert _DOC_PATH.is_file(), _DOC_PATH
    doc = _DOC_PATH.read_text(encoding="utf-8")
    for key, source in _EXAMPLE_SOURCES.items():
        assert source in doc, f"example {key!r}: {source!r} missing from {_DOC_PATH}"


# ---------------------------------------------------------------------------
# Example 1 — ``let`` total
# ---------------------------------------------------------------------------


def test_example_let_total_critical_sums_two_step_outputs() -> None:
    scope = _scope(
        steps={
            "scan": _step(critical=3),
            "scan-alt": _step(critical=4),
        }
    )
    assert _run(_EXAMPLE_SOURCES["let_total"], scope) == 7


# ---------------------------------------------------------------------------
# Example 2 — ``let`` ternary label, referencing a prior ``let``
# binding from the same block.
# ---------------------------------------------------------------------------


def test_example_let_label_block_returns_block_when_critical_positive() -> None:
    scope = _scope(let={"totalCritical": 7})
    assert _run(_EXAMPLE_SOURCES["let_label"], scope, with_let_total=True) == "block"


def test_example_let_label_block_returns_allow_when_critical_zero() -> None:
    scope = _scope(let={"totalCritical": 0})
    assert _run(_EXAMPLE_SOURCES["let_label"], scope, with_let_total=True) == "allow"


# ---------------------------------------------------------------------------
# Example 3 — ``if:`` guard
# ---------------------------------------------------------------------------


def test_example_if_guard_runs_when_enabled_and_tags_nonempty() -> None:
    scope = _scope(inputs={"enabled": True, "tags": ["v1"]})
    assert _run(_EXAMPLE_SOURCES["if_guard"], scope) is True


def test_example_if_guard_skips_when_disabled() -> None:
    scope = _scope(inputs={"enabled": False, "tags": ["v1"]})
    assert _run(_EXAMPLE_SOURCES["if_guard"], scope) is False


def test_example_if_guard_skips_when_tags_empty() -> None:
    scope = _scope(inputs={"enabled": True, "tags": []})
    assert _run(_EXAMPLE_SOURCES["if_guard"], scope) is False


# ---------------------------------------------------------------------------
# Example 4 — ``with:`` input mapping
# ---------------------------------------------------------------------------


def test_example_with_mapping_concatenates_image_and_first_tag() -> None:
    scope = _scope(inputs={"image": "ghcr.io/acme/api", "tags": ["v2", "v1"]})
    assert _run(_EXAMPLE_SOURCES["with_mapping"], scope) == "ghcr.io/acme/api:v2"


# ---------------------------------------------------------------------------
# Example 5 — ``for:`` loop iterable
# ---------------------------------------------------------------------------


def test_example_for_iterable_returns_a_list_of_targets() -> None:
    targets = [
        {"image": "ghcr.io/acme/api", "tag": "v1"},
        {"image": "ghcr.io/acme/web", "tag": "v2"},
    ]
    scope = _scope(inputs={"targets": targets})
    result = _run(_EXAMPLE_SOURCES["for_iterable"], scope)
    assert result == targets


# ---------------------------------------------------------------------------
# Doctest-style: also exercise every fenced ``cel`` code block in the
# doc to catch new examples added without a corresponding test.
# ---------------------------------------------------------------------------


_CEL_FENCE_RE = re.compile(r"```cel\n(?P<body>.*?)\n```", re.DOTALL)


def test_every_fenced_cel_block_in_the_doc_is_listed_in_the_example_table() -> None:
    """Every ``cel`` fenced block in the doc must correspond to one
    of the keyed ``_EXAMPLE_SOURCES`` entries — guards against
    documenting a new example without wiring it into this test.
    """
    doc = _DOC_PATH.read_text(encoding="utf-8")
    fenced = [m.group("body").strip() for m in _CEL_FENCE_RE.finditer(doc)]
    assert fenced, "expected at least one ```cel fenced block in the doc"
    known = set(_EXAMPLE_SOURCES.values())
    leftover = [src for src in fenced if src not in known]
    assert not leftover, (
        f"doc contains ```cel blocks that are not exercised by this test: {leftover}"
    )


# ---------------------------------------------------------------------------
# Parameterized smoke: every example parses + type-checks without
# raising, against a unified scope that satisfies every binding the
# examples might read. This is the literal copy-paste-runnable
# acceptance criterion from issue #187.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("key", "source"),
    sorted(_EXAMPLE_SOURCES.items()),
)
def test_every_doc_example_parses_and_type_checks(key: str, source: str) -> None:
    ast = parse(source)
    # The ``let`` ternary example references ``let.totalCritical`` so
    # the bindings must declare it. Use the union scope.
    type_check(ast, _bindings(with_let_total=True))

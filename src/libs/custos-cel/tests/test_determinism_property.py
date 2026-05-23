"""Property-based replay-determinism tests for the Custos CEL evaluator.

Implements WF-IMPL-010 (issue #184 → #185). Per design.md § Expression
Evaluator (Determinism guarantees):

    Every binding is replay-deterministic. ``now()`` returns the same
    value across replays of the same Dapr Workflow instance. No
    expression can introduce non-determinism into the orchestration.

The four properties below are what makes the evaluator safe inside a
Dapr Workflow replay loop. Property tests catch the long tail of
expression shapes that hand-rolled unit tests miss.

Strategy
--------

Each :func:`@given` decorator wires a Hypothesis strategy that
generates **well-typed CEL source text** over a fixed schema
(:data:`_INPUTS_SCHEMA`, one prior step ``scan``, one ``let.t : int``),
then parses and type-checks it. The fixed schema means every generated
expression is statically valid; the generator avoids division /
modulo / list-index / map-lookup-by-runtime-key so no example can
raise an :class:`EvaluationError`. The result is a deterministic
typed AST that the four properties drive against
:class:`FixedClock` / :class:`BindingScope` snapshots.

CI invokes this file with ``--hypothesis-seed=0`` (the locked seed
from the acceptance criterion) so every CI run explores the same
1000 examples and any regression is reproducible from the failure
report alone.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime
from typing import Any

import pytest

# Skip the file entirely if Hypothesis is not installed. The dev extras
# pin it (``pyproject.toml``); production / non-dev environments don't
# pull it in.
hypothesis = pytest.importorskip("hypothesis")

from hypothesis import HealthCheck, given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

from custos_cel import (  # noqa: E402
    BindingScope,
    FixedClock,
    IntType,
    Node,
    RunInfo,
    SchemaBindings,
    StepBinding,
    WorkflowInfo,
    evaluate,
    from_json,
    parse,
    to_json,
    type_check,
)

# ---------------------------------------------------------------------------
# Fixed schema / scope / clock
# ---------------------------------------------------------------------------

_FIXED_TS = datetime(2026, 5, 22, 12, 0, 0, tzinfo=UTC)


def _clock() -> FixedClock:
    return FixedClock(_FIXED_TS)


# Inputs schema covers each scalar kind plus a homogeneous list. The
# generator only synthesises member accesses that are known-typed
# against this schema, so type-check always succeeds.
_INPUTS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "x": {"type": "integer"},
        "y": {"type": "integer"},
        "r": {"type": "number"},
        "s": {"type": "string"},
        "b": {"type": "boolean"},
        "lst": {"type": "array", "items": {"type": "integer"}},
        "obj": {"type": "object", "additionalProperties": {"type": "string"}},
    },
}

_SCAN_OUTPUTS: dict[str, Any] = {
    "type": "object",
    "properties": {
        "critical": {"type": "integer"},
        "verdict": {"type": "string"},
    },
}


def _bindings() -> SchemaBindings:
    return SchemaBindings(
        inputs=_INPUTS_SCHEMA,
        prior_steps=(("scan", _SCAN_OUTPUTS),),
        let={"t": IntType()},
    )


def _scope() -> BindingScope:
    return BindingScope(
        run=RunInfo(id="run-prop", workspace="ws-prop"),
        workflow=WorkflowInfo(name="wf-prop", version="v1"),
        now=_clock().now,
        inputs={
            "x": 7,
            "y": 3,
            "r": 1.5,
            "s": "alpha",
            "b": True,
            "lst": [10, 20, 30],
            "obj": {"k": "v"},
        },
        steps={"scan": StepBinding({"critical": 4, "verdict": "ok"}, sealed=True)},
        let={"t": 11},
    )


# ---------------------------------------------------------------------------
# Hypothesis strategies — generate well-typed CEL source text
# ---------------------------------------------------------------------------
#
# The generators emit *source text*, not AST nodes. ``parse`` +
# ``type_check`` cover the lift; this keeps the strategies short and
# guarantees that the type checker has actually seen the expression
# (which is the same path the Step Coordinator takes).


# Bounded integer literals. Python ints don't overflow so the bound is
# purely for runtime speed and shrinker readability.
_INT_LITERAL = st.integers(min_value=-1_000, max_value=1_000).map(str)


# Bounded float literals. We exclude NaN / infinity so that
# ``==`` over floats is reflexive (NaN != NaN would defeat the
# byte-equality assertion in property 1) and we exclude division so
# inf never arises from the evaluator path either.
def _format_double(v: float) -> str:
    # Always emit a decimal point so celpy lexes the literal as
    # ``double`` rather than ``int``.
    s = repr(v)
    if "." not in s and "e" not in s and "E" not in s:
        s += ".0"
    return s


_DOUBLE_LITERAL = st.floats(
    min_value=-1_000.0,
    max_value=1_000.0,
    allow_nan=False,
    allow_infinity=False,
).map(_format_double)


# Strings drawn from a CEL-friendly alphabet (no escapes, no quotes,
# no backslashes — so the literal is just ``"..."``). Bounded length
# keeps shrinker output legible.
_STRING_LITERAL = st.text(
    alphabet=st.characters(
        whitelist_categories=("Ll", "Lu", "Nd"),
        whitelist_characters=" _-",
    ),
    min_size=0,
    max_size=8,
).map(lambda s: f'"{s}"')


# Leaves: typed references and literals.
_INT_LEAVES = st.one_of(
    _INT_LITERAL,
    st.sampled_from(
        [
            "inputs.x",
            "inputs.y",
            "steps.scan.outputs.critical",
            "let.t",
        ]
    ),
)
_DOUBLE_LEAVES = st.one_of(
    _DOUBLE_LITERAL,
    st.just("inputs.r"),
)
_STRING_LEAVES = st.one_of(
    _STRING_LITERAL,
    st.sampled_from(
        [
            "inputs.s",
            "steps.scan.outputs.verdict",
            "run.id",
            "run.workspace",
            "workflow.name",
            "workflow.version",
        ]
    ),
)
_BOOL_LEAVES = st.one_of(
    st.sampled_from(["true", "false", "inputs.b"]),
)


def _int_expr() -> st.SearchStrategy[str]:
    """Generate well-typed integer-valued CEL expressions."""
    return st.recursive(
        _INT_LEAVES,
        lambda children: st.one_of(
            # Arithmetic (skip ``/`` and ``%`` to avoid divide-by-zero
            # runtime errors).
            st.tuples(children, children).map(lambda t: f"({t[0]} + {t[1]})"),
            st.tuples(children, children).map(lambda t: f"({t[0]} - {t[1]})"),
            st.tuples(children, children).map(lambda t: f"({t[0]} * {t[1]})"),
            # Unary minus.
            children.map(lambda c: f"(-{c})"),
            # ``size(list)`` / ``size(string)``.
            st.just("size(inputs.lst)"),
            st.just("size(inputs.s)"),
            st.just("size(steps.scan.outputs.verdict)"),
            # Ternary with a bool guard.
            st.tuples(_bool_expr(), children, children).map(
                lambda t: f"({t[0]} ? {t[1]} : {t[2]})"
            ),
        ),
        max_leaves=8,
    )


def _double_expr() -> st.SearchStrategy[str]:
    """Generate well-typed double-valued CEL expressions."""
    return st.recursive(
        _DOUBLE_LEAVES,
        lambda children: st.one_of(
            st.tuples(children, children).map(lambda t: f"({t[0]} + {t[1]})"),
            st.tuples(children, children).map(lambda t: f"({t[0]} - {t[1]})"),
            st.tuples(children, children).map(lambda t: f"({t[0]} * {t[1]})"),
            children.map(lambda c: f"(-{c})"),
            st.tuples(_bool_expr(), children, children).map(
                lambda t: f"({t[0]} ? {t[1]} : {t[2]})"
            ),
        ),
        max_leaves=6,
    )


def _string_expr() -> st.SearchStrategy[str]:
    """Generate well-typed string-valued CEL expressions."""
    return st.recursive(
        _STRING_LEAVES,
        lambda children: st.one_of(
            st.tuples(children, children).map(lambda t: f"({t[0]} + {t[1]})"),
            st.tuples(_bool_expr(), children, children).map(
                lambda t: f"({t[0]} ? {t[1]} : {t[2]})"
            ),
        ),
        max_leaves=6,
    )


def _bool_expr() -> st.SearchStrategy[str]:
    """Generate well-typed bool-valued CEL expressions."""
    return st.recursive(
        _BOOL_LEAVES,
        lambda children: st.one_of(
            # Logical operators.
            st.tuples(children, children).map(lambda t: f"({t[0]} && {t[1]})"),
            st.tuples(children, children).map(lambda t: f"({t[0]} || {t[1]})"),
            children.map(lambda c: f"(!{c})"),
            # Integer comparisons.
            st.tuples(_INT_LEAVES, _INT_LEAVES).map(lambda t: f"({t[0]} < {t[1]})"),
            st.tuples(_INT_LEAVES, _INT_LEAVES).map(lambda t: f"({t[0]} <= {t[1]})"),
            st.tuples(_INT_LEAVES, _INT_LEAVES).map(lambda t: f"({t[0]} == {t[1]})"),
            st.tuples(_INT_LEAVES, _INT_LEAVES).map(lambda t: f"({t[0]} != {t[1]})"),
            st.tuples(_INT_LEAVES, _INT_LEAVES).map(lambda t: f"({t[0]} >= {t[1]})"),
            # String comparisons.
            st.tuples(_STRING_LEAVES, _STRING_LEAVES).map(lambda t: f"({t[0]} == {t[1]})"),
            st.tuples(_STRING_LEAVES, _STRING_LEAVES).map(lambda t: f"({t[0]} != {t[1]})"),
            # ``has()`` macro on object members. Every inputs member
            # exists in the fixed scope so the result is always True.
            st.sampled_from(
                [
                    "has(inputs.x)",
                    "has(inputs.s)",
                    "has(inputs.b)",
                    "has(steps.scan.outputs.critical)",
                ]
            ),
        ),
        max_leaves=8,
    )


# Top-level: any of the four typed expressions.
_ANY_EXPR = st.one_of(_int_expr(), _double_expr(), _string_expr(), _bool_expr())


def _typed(src: str) -> Node:
    return type_check(parse(src), _bindings())


# ---------------------------------------------------------------------------
# Hypothesis profile
# ---------------------------------------------------------------------------
#
# The acceptance criterion is "All four properties hold across at
# least 1000 examples." We register a single 1000-example profile and
# load it for every test in this file. ``deadline=None`` because
# parse + type_check + evaluate combined can occasionally exceed
# Hypothesis's 200 ms default on slow CI runners, and the per-example
# work is bounded by the generator (``max_leaves<=8``) — there is no
# pathological-input concern to gate against.

settings.register_profile(
    "custos-cel-property",
    max_examples=1000,
    deadline=None,
    suppress_health_check=[
        HealthCheck.too_slow,
        HealthCheck.data_too_large,
    ],
)
settings.load_profile("custos-cel-property")


# ---------------------------------------------------------------------------
# Property 1: byte-equal across repeated evaluations
# ---------------------------------------------------------------------------


@given(src=_ANY_EXPR)
def test_property_1_byte_equal_across_repeats(src: str) -> None:
    """Same typed AST + same scope + same fixed clock → byte-equal result.

    Acceptance text says "for 100 repeats". 100 evaluations per
    Hypothesis example times ``max_examples=1000`` = 100k evaluations.
    Each evaluation is microseconds against the bounded generator so
    runtime stays modest in CI.
    """
    typed = _typed(src)
    scope = _scope()
    first = evaluate(typed, scope, _clock())
    for _ in range(99):
        again = evaluate(typed, scope, _clock())
        assert again == first, f"non-deterministic result for {src!r}"


# ---------------------------------------------------------------------------
# Property 2: typed-AST round-trip preserves evaluation result
# ---------------------------------------------------------------------------


@given(src=_ANY_EXPR)
def test_property_2_round_trip_preserves_evaluation(src: str) -> None:
    """``evaluate(ast) == evaluate(from_json(to_json(ast)))``.

    Also asserts structural equality on the round-tripped AST so a
    schema-version drift in :mod:`custos_cel.ast` surfaces here too.
    """
    typed = _typed(src)
    restored = from_json(to_json(typed))
    assert restored == typed, f"AST not round-trip equal for {src!r}"

    scope = _scope()
    clock = _clock()
    expected = evaluate(typed, scope, clock)
    actual = evaluate(restored, scope, clock)
    assert actual == expected, f"round-trip changed evaluation result for {src!r}"


# ---------------------------------------------------------------------------
# Property 3: now() invariance within a single evaluation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "src",
    [
        "now() == now()",
        # ``now()`` invoked from both branches of a binary; the
        # evaluator must hand each call the same datetime.
        "now() == now() && now() == now()",
        # Mixed in with arithmetic so the call sites are interleaved.
        "(now() == now() ? 1 : 0) + (now() == now() ? 1 : 0) == 2",
    ],
)
def test_property_3_now_invariance_within_single_evaluation(src: str) -> None:
    """Multiple ``now()`` calls inside one evaluation return the same value."""
    result = evaluate(_typed(src), _scope(), _clock())
    assert result is True, f"now() returned different values within {src!r}"


@given(guard=_bool_expr())
def test_property_3_now_invariance_under_random_wrapping(guard: str) -> None:
    """For any generated bool ``guard``, ``guard || (now() != now())`` ≡ ``guard``.

    ``now() != now()`` must be ``False`` inside a single evaluation,
    so OR-ing it with any bool is the identity. The property holds
    whether ``guard`` short-circuits past the ``now()`` calls or not.
    """
    expected = evaluate(_typed(guard), _scope(), _clock())
    wrapped = f"({guard}) || (now() != now())"
    actual = evaluate(_typed(wrapped), _scope(), _clock())
    assert actual == expected, f"now() invariance broken when wrapping {guard!r}"


# ---------------------------------------------------------------------------
# Property 4: sandbox containment
# ---------------------------------------------------------------------------


@given(src=_ANY_EXPR)
def test_property_4_sandbox_containment(src: str) -> None:
    """Evaluation must not mutate ``os.environ`` or ``sys.modules``.

    Snapshots are byte-stable JSON (sorted keys); a structural compare
    after evaluation catches any process-wide state leak the
    evaluator might introduce. We capture ``sys.modules`` keys only
    — module objects themselves are not equality-comparable in a
    useful way and the property we care about is "no new module was
    imported during evaluation".
    """
    typed = _typed(src)

    env_before = json.dumps(dict(os.environ), sort_keys=True)
    modules_before = frozenset(sys.modules)

    evaluate(typed, _scope(), _clock())

    env_after = json.dumps(dict(os.environ), sort_keys=True)
    modules_after = frozenset(sys.modules)

    assert env_before == env_after, f"os.environ mutated by evaluation of {src!r}"
    assert modules_before == modules_after, (
        f"sys.modules mutated by evaluation of {src!r}: "
        f"added={modules_after - modules_before!r}, "
        f"removed={modules_before - modules_after!r}"
    )

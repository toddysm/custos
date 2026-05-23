"""WF-IMPL-008 (issue #183): public API surface + locked error taxonomy.

Covers:

* The single-import contract:
  ``from custos_cel import parse, type_check, evaluate, BindingScope,
  FixedClock, SchemaBindings`` resolves and the four functions /
  classes are the same objects the submodules export.
* The full lifecycle of a non-trivial expression:
  ``parse`` → ``type_check`` → ``evaluate`` produces the expected
  value and propagates the typed-AST shape end-to-end.
* The :mod:`custos_cel.errors` taxonomy: every concrete subclass has
  the locked ``kind`` string, subclasses :class:`CelError`,
  subclasses the appropriate Python builtin (``ValueError`` /
  ``TypeError`` / ``LookupError`` / ``TimeoutError`` /
  ``RuntimeError``), is hashable, has a structured ``__repr__``, and
  round-trips through :meth:`to_dict` deterministically.
* Backwards-compat aliases: ``EvalError``, ``EvalTimeoutError``, and
  ``TypeCheckError`` resolve to the canonical taxonomy classes;
  ``UnboundNameError.chain`` / ``.pos`` still work for WF-IMPL-004
  callers.
* :func:`custos_cel.parse` re-raises ``celpy.celparser.CELParseError``
  as :class:`custos_cel.errors.ParseError` so the public surface
  only ever raises one parse-error shape.
"""

from __future__ import annotations

import builtins
from datetime import UTC, datetime
from typing import Any

import pytest

import custos_cel
from custos_cel import (
    BindingScope,
    FixedClock,
    Ident,
    IntType,
    Literal,
    LiteralKind,
    RunInfo,
    SchemaBindings,
    SourcePosition,
    StepBinding,
    WorkflowInfo,
    errors,
    evaluate,
    parse,
    type_check,
)
from custos_cel.errors import (
    CelError,
    DivergenceError,
    EvaluationError,
    ParseError,
    UnboundNameError,
)
from custos_cel.errors import (
    TimeoutError as CelTimeoutError,
)
from custos_cel.errors import (
    TypeError as CelTypeError,
)

# ---------------------------------------------------------------------------
# Public-surface single-import contract
# ---------------------------------------------------------------------------


def test_single_import_resolves_documented_surface() -> None:
    # Acceptance criterion from issue #183: a single-import line must
    # surface the parse / type_check / evaluate triple plus the scope,
    # clock, and schema-bindings types.
    from custos_cel import (
        BindingScope as _BS,
    )
    from custos_cel import (
        FixedClock as _FC,
    )
    from custos_cel import (
        SchemaBindings as _SB,
    )
    from custos_cel import (
        evaluate as _ev,
    )
    from custos_cel import (
        parse as _p,
    )
    from custos_cel import (
        type_check as _tc,
    )

    assert _p is parse
    assert _tc is type_check
    assert _ev is evaluate
    assert _BS is BindingScope
    assert _FC is FixedClock
    assert _SB is SchemaBindings


def test_errors_submodule_is_re_exported() -> None:
    # ``custos_cel.errors`` is a documented module; tests and audit
    # consumers reach it via either ``import custos_cel.errors`` or
    # ``from custos_cel import errors``. Both resolve to the same
    # object.
    import custos_cel.errors as direct

    assert errors is direct


def test_package_re_exports_locked_taxonomy() -> None:
    # The locked names are exposed on the top-level package (under
    # unprefixed names where they do not clash with Python builtins,
    # and via the ``errors`` submodule for the two that do —
    # ``TypeError`` / ``TimeoutError`` would shadow builtins if
    # exported bare).
    assert custos_cel.CelError is CelError
    assert custos_cel.ParseError is ParseError
    assert custos_cel.EvaluationError is EvaluationError
    assert custos_cel.DivergenceError is DivergenceError
    assert custos_cel.UnboundNameError is UnboundNameError
    # ``TypeError`` / ``TimeoutError`` live on the errors submodule
    # only, so ``from custos_cel import *`` cannot accidentally
    # shadow the builtins.
    assert "TypeError" not in custos_cel.__all__
    assert "TimeoutError" not in custos_cel.__all__


def test_backwards_compat_aliases_resolve_to_taxonomy() -> None:
    # WF-IMPL-006 / WF-IMPL-007 names continue to work and are the
    # same class objects as the WF-IMPL-008 canonical names — so
    # ``isinstance`` checks and ``except`` clauses are interchangeable.
    assert custos_cel.EvalError is EvaluationError
    assert custos_cel.EvalTimeoutError is CelTimeoutError
    assert custos_cel.TypeCheckError is CelTypeError


# ---------------------------------------------------------------------------
# Locked ``kind`` strings
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("cls", "expected_kind"),
    [
        (ParseError, "expression.parse_error"),
        (CelTypeError, "expression.type_error"),
        (UnboundNameError, "expression.unbound_name"),
        (CelTimeoutError, "expression.timeout"),
        (EvaluationError, "expression.evaluation_error"),
        (DivergenceError, "expression.divergence"),
    ],
)
def test_taxonomy_kind_strings_are_locked(cls: type[CelError], expected_kind: str) -> None:
    # The kind strings are part of the audit-event contract; downstream
    # consumers (Observability Service, Step Coordinator emission)
    # key off them. Changing one is a downstream contract break.
    assert expected_kind == cls.KIND


def test_celerror_base_is_abstract() -> None:
    # CelError on its own would have an empty kind which would defeat
    # the taxonomy. The constructor refuses to admit a bare instance.
    with pytest.raises(builtins.TypeError, match="abstract"):
        CelError("boom")


# ---------------------------------------------------------------------------
# Python-builtin parents (so generic catch blocks still fire)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("cls", "builtin_parent"),
    [
        (ParseError, ValueError),
        (CelTypeError, builtins.TypeError),
        (UnboundNameError, LookupError),
        (CelTimeoutError, builtins.TimeoutError),
        (EvaluationError, RuntimeError),
        (DivergenceError, RuntimeError),
    ],
)
def test_taxonomy_preserves_builtin_parents(cls: type, builtin_parent: type) -> None:
    # Every error class is also catchable via the most relevant
    # Python builtin so existing generic validation idioms continue
    # to fire.
    assert issubclass(cls, CelError)
    assert issubclass(cls, builtin_parent)


def test_taxonomy_classes_are_hashable() -> None:
    # Hashable error instances let downstream consumers de-dupe
    # repeated audit emissions through a ``set``.
    instances: list[CelError] = [
        ParseError("p"),
        CelTypeError("t"),
        UnboundNameError(("x",)),
        CelTimeoutError("to", elapsed_ms=10, timeout_ms=5),
        EvaluationError("e"),
        DivergenceError("d"),
    ]
    seen: set[CelError] = set()
    for inst in instances:
        seen.add(inst)
    assert len(seen) == len(instances)


# ---------------------------------------------------------------------------
# ``to_dict`` round-trip + ``__repr__`` shape
# ---------------------------------------------------------------------------


def test_parseerror_to_dict_minimal() -> None:
    err = ParseError("syntax")
    assert err.to_dict() == {
        "kind": "expression.parse_error",
        "message": "syntax",
        "source_position": None,
    }


def test_parseerror_to_dict_with_position() -> None:
    pos = SourcePosition(line=4, column=7, offset=42)
    err = ParseError("bad token", source_position=pos)
    d = err.to_dict()
    assert d["kind"] == "expression.parse_error"
    assert d["source_position"] == {"line": 4, "column": 7, "offset": 42}


def test_typeerror_to_dict_includes_expected_actual() -> None:
    pos = SourcePosition(line=1, column=2, offset=1)
    err = CelTypeError(
        "needs numeric",
        source_position=pos,
        expected_type="numeric",
        actual_type=IntType(),
    )
    d = err.to_dict()
    assert d == {
        "kind": "expression.type_error",
        "message": "needs numeric",
        "source_position": {"line": 1, "column": 2, "offset": 1},
        "expected_type": "numeric",
        "actual_type": repr(IntType()),
    }


def test_typeerror_to_dict_null_types() -> None:
    err = CelTypeError("oops")
    d = err.to_dict()
    assert d["expected_type"] is None
    assert d["actual_type"] is None


def test_unboundnameerror_to_dict() -> None:
    err = UnboundNameError(("inputs", "missing"), reason="no such field")
    d = err.to_dict()
    assert d == {
        "kind": "expression.unbound_name",
        "message": "unbound name: inputs.missing (no such field)",
        "source_position": None,
        "name_chain": ["inputs", "missing"],
        "reason": "no such field",
    }


def test_unboundnameerror_empty_chain_renders() -> None:
    # Defensive: an empty chain still produces a useful message and a
    # deterministic ``name_chain`` field.
    err = UnboundNameError(())
    assert err.message == "unbound name: <empty>"
    assert err.to_dict()["name_chain"] == []


def test_unboundnameerror_rejects_bare_str_chain() -> None:
    # ``str`` is iterable as ``Sequence[str]`` of single characters,
    # which would silently corrupt the audit event. The constructor
    # wraps a bare ``str`` into a single-element tuple instead of
    # iterating it.
    err = UnboundNameError("inputs.missing")
    assert err.name_chain == ("inputs.missing",)


def test_unboundnameerror_backwards_compat_aliases() -> None:
    # WF-IMPL-004 callers used ``chain`` and ``pos``; both must keep
    # working and reference the same data.
    pos = SourcePosition(line=2, column=3, offset=10)
    err = UnboundNameError(("inputs", "x"), pos=pos, reason="r")
    assert err.chain == ("inputs", "x")
    assert err.chain is err.name_chain
    assert err.pos is pos
    assert err.pos is err.source_position


def test_timeouterror_to_dict() -> None:
    err = CelTimeoutError("budget exceeded", elapsed_ms=123, timeout_ms=100)
    d = err.to_dict()
    assert d == {
        "kind": "expression.timeout",
        "message": "budget exceeded",
        "source_position": None,
        "elapsed_ms": 123,
        "timeout_ms": 100,
    }


def test_evaluationerror_to_dict() -> None:
    pos = SourcePosition(line=1, column=1, offset=0)
    err = EvaluationError("divide by zero", source_position=pos)
    d = err.to_dict()
    assert d == {
        "kind": "expression.evaluation_error",
        "message": "divide by zero",
        "source_position": {"line": 1, "column": 1, "offset": 0},
    }


def test_divergenceerror_to_dict() -> None:
    err = DivergenceError("replay diverged at step scan")
    assert err.to_dict() == {
        "kind": "expression.divergence",
        "message": "replay diverged at step scan",
        "source_position": None,
    }


def test_to_dict_key_order_is_stable() -> None:
    # ``kind``, ``message``, ``source_position`` come first in that
    # order so audit serialization is byte-stable without an extra
    # canonicalization step.
    err = CelTypeError("oops", expected_type="int", actual_type="string")
    keys = list(err.to_dict().keys())
    assert keys[:3] == ["kind", "message", "source_position"]
    assert "expected_type" in keys[3:]
    assert "actual_type" in keys[3:]


def test_repr_includes_kind_and_extras() -> None:
    err = CelTimeoutError("over", elapsed_ms=42, timeout_ms=10)
    r = repr(err)
    assert "TimeoutError(" in r
    assert "kind='expression.timeout'" in r
    assert "elapsed_ms=42" in r
    assert "timeout_ms=10" in r


# ---------------------------------------------------------------------------
# ``parse`` wraps celpy parse errors
# ---------------------------------------------------------------------------


def test_parse_raises_parseerror_on_syntax_failure() -> None:
    # WF-IMPL-008: the public ``parse`` surface only ever raises
    # taxonomy errors. The underlying ``celpy.celparser.CELParseError``
    # is wrapped and chained.
    with pytest.raises(ParseError) as excinfo:
        parse("1 +")
    err = excinfo.value
    assert err.kind == "expression.parse_error"
    assert err.__cause__ is not None
    # It is also a ``ValueError`` so existing generic validation
    # idioms keep working.
    assert isinstance(err, ValueError)


def test_parse_raises_parseerror_on_unsupported_construct() -> None:
    # The converter's ``CelConvertError`` is a ``ParseError`` subclass
    # under WF-IMPL-008 — a single ``except ParseError`` covers both
    # the lexer/parser path and the converter's structural rejections.
    with pytest.raises(ParseError):
        parse('"abc".size()')


def test_parse_raises_celconverterror_on_unsupported_construct() -> None:
    # The narrower CelConvertError surface is preserved so existing
    # callers / tests pinning that exact class keep working.
    with pytest.raises(custos_cel.CelConvertError):
        parse('"abc".size()')


# ---------------------------------------------------------------------------
# Full lifecycle: parse → type_check → evaluate
# ---------------------------------------------------------------------------


def _bindings_for_scan_steps() -> SchemaBindings:
    return SchemaBindings(
        inputs={
            "type": "object",
            "properties": {
                "threshold": {"type": "integer"},
            },
        },
        prior_steps=[
            (
                "scan",
                {
                    "type": "object",
                    "properties": {
                        "critical": {"type": "integer"},
                    },
                },
            ),
            (
                "scan-alt",
                {
                    "type": "object",
                    "properties": {
                        "critical": {"type": "integer"},
                    },
                },
            ),
        ],
    )


def _scope_for_scan_steps() -> BindingScope:
    return BindingScope(
        run=RunInfo(id="r1", workspace="ws1"),
        workflow=WorkflowInfo(name="wf", version="1"),
        now=FixedClock(datetime(2026, 5, 22, tzinfo=UTC)).now,
        inputs={"threshold": 5},
        steps={
            "scan": StepBinding({"critical": 3}, sealed=True),
            "scan-alt": StepBinding({"critical": 4}, sealed=True),
        },
    )


def test_full_lifecycle_non_trivial_expression() -> None:
    # The canonical workflow expression: combine outputs from two
    # prior steps (one with a hyphenated id, so the bracket form is
    # required) and compare against an input threshold. Exercises
    # parse → type_check → evaluate plus member / index / binary /
    # comparison nodes.
    source = (
        'steps["scan"].outputs.critical + steps["scan-alt"].outputs.critical >= inputs.threshold'
    )
    ast = parse(source)
    typed = type_check(ast, _bindings_for_scan_steps())
    # The type checker must annotate the root.
    assert typed.cel_type is not None
    scope = _scope_for_scan_steps()
    result = evaluate(typed, scope, FixedClock(datetime(2026, 5, 22, tzinfo=UTC)))
    assert result is True


def test_full_lifecycle_returns_value_not_just_bool() -> None:
    # Sanity: the lifecycle handles integer expressions end-to-end.
    typed = type_check(
        parse("inputs.threshold + 1"),
        _bindings_for_scan_steps(),
    )
    result = evaluate(
        typed,
        _scope_for_scan_steps(),
        FixedClock(datetime(2026, 5, 22, tzinfo=UTC)),
    )
    assert result == 6


def test_full_lifecycle_surfaces_type_error_kind() -> None:
    # A type mismatch surfaces with the locked ``expression.type_error``
    # kind. The Definition Compiler can route the resulting
    # ``to_dict`` straight onto an audit event.
    with pytest.raises(CelTypeError) as excinfo:
        type_check(parse("inputs.threshold + 'x'"), _bindings_for_scan_steps())
    assert excinfo.value.kind == "expression.type_error"
    d = excinfo.value.to_dict()
    assert d["kind"] == "expression.type_error"
    assert d["source_position"] is not None


def test_full_lifecycle_surfaces_evaluation_error_kind() -> None:
    # Runtime division-by-zero surfaces with the
    # ``expression.evaluation_error`` kind.
    typed = type_check(parse("1 / 0"), SchemaBindings())
    clock = FixedClock(datetime(2026, 5, 22, tzinfo=UTC))
    with pytest.raises(EvaluationError) as excinfo:
        evaluate(
            typed,
            BindingScope(
                run=RunInfo(id="r", workspace="ws"),
                workflow=WorkflowInfo(name="w", version="1"),
                now=clock.now,
            ),
            clock,
        )
    assert excinfo.value.kind == "expression.evaluation_error"


# ---------------------------------------------------------------------------
# Subclass extension hook
# ---------------------------------------------------------------------------


def test_extra_fields_is_used_by_to_dict_and_repr() -> None:
    # Concrete classes use the ``_extra_fields`` hook; the base
    # returns ``{}`` so a minimal subclass's dict / repr show no
    # extras. Confirms the hook contract.

    class _Probe(CelError):
        KIND = "expression.evaluation_error"

    err = _Probe("hi")
    assert err.to_dict() == {
        "kind": "expression.evaluation_error",
        "message": "hi",
        "source_position": None,
    }
    assert "Probe" in repr(err)


def test_to_dict_is_json_safe() -> None:
    # Every taxonomy class round-trips through ``json.dumps`` without
    # a custom encoder so audit emission is straightforward.
    import json

    for err in [
        ParseError("p"),
        CelTypeError("t", expected_type="int", actual_type="string"),
        UnboundNameError(("inputs", "x"), reason="no such field"),
        CelTimeoutError("to", elapsed_ms=10, timeout_ms=5),
        EvaluationError("e"),
        DivergenceError("d"),
    ]:
        rendered = json.dumps(err.to_dict(), sort_keys=False)
        # Round-trip back to a dict to make sure shape is preserved.
        loaded: dict[str, Any] = json.loads(rendered)
        assert loaded["kind"] == err.kind
        assert loaded["message"] == err.message


# ---------------------------------------------------------------------------
# Source-position propagation through the public surface
# ---------------------------------------------------------------------------


def test_parse_attaches_source_positions() -> None:
    # The parser annotates every node with a source position;
    # WF-IMPL-008's error taxonomy relies on those positions to
    # populate ``source_position`` when errors fire downstream.
    ast = parse("inputs.threshold + 1")
    # Walk one level: the binary node must carry a position.
    assert ast.pos is not None


def test_typed_ast_carries_positions_for_error_reporting() -> None:
    # Programmatic AST: even a hand-built typed AST plays nicely with
    # the error taxonomy if it carries positions.
    pos = SourcePosition(line=1, column=1, offset=0)
    node = Ident(pos=pos, name="missing", cel_type=IntType())
    err = UnboundNameError(("missing",), pos=node.pos, reason="unknown root")
    assert err.source_position is pos
    assert err.to_dict()["source_position"] == {
        "line": 1,
        "column": 1,
        "offset": 0,
    }


def test_literal_node_unaffected_by_taxonomy_refactor() -> None:
    # Belt-and-suspenders sanity check: the AST data model is
    # unchanged by WF-IMPL-008; Literal still constructs the same way.
    pos = SourcePosition(line=1, column=1, offset=0)
    n = Literal(pos=pos, kind=LiteralKind.INT, value=42)
    assert n.kind is LiteralKind.INT
    assert n.value == 42

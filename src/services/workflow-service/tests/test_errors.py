"""Tests for the locked compile-time error taxonomy (WF-IMPL-024).

Each test exercises one acceptance criterion from issue #358:

* the four canonical subclasses + base class are present;
* every subclass pins an *exact* ``kind`` string (no fuzzy match);
* :meth:`CompileError.to_dict` produces a JSON-safe dict with the
  documented deterministic key order;
* :meth:`CompileError.__repr__` is stable and includes every field;
* every subclass is hashable and equal-on-fields;
* wrapping a :class:`custos_cel.CelError` preserves the underlying
  ``kind`` / ``message`` in :meth:`to_dict`\\ ``["cause"]``.
"""

from __future__ import annotations

import builtins

import pytest
from custos_cel.ast import SourcePosition
from custos_cel.errors import ParseError as CelParseError
from custos_cel.errors import TypeError as CelTypeError

from custos_workflow.errors import (
    CompileError,
    CompileParseError,
    CompileRetryPolicyError,
    CompileTopologyError,
    CompileTypeError,
)

# ---------------------------------------------------------------------------
# Module surface
# ---------------------------------------------------------------------------


class TestModuleSurface:
    def test_base_is_abstract(self) -> None:
        # Constructing the base directly is rejected so callers
        # cannot accidentally emit a "kind=''" envelope.
        with pytest.raises(builtins.TypeError, match="abstract"):
            CompileError("nope")

    def test_canonical_kind_strings_are_exact(self) -> None:
        # Locked taxonomy — these strings are part of the public
        # contract per design.md § Failure Modes. Any change here
        # is a downstream contract break for the audit consumers.
        assert CompileParseError.KIND == "compile.parse_error"
        assert CompileTypeError.KIND == "compile.type_error"
        assert CompileTopologyError.KIND == "compile.topology_error"
        assert CompileRetryPolicyError.KIND == "compile.retry_policy_error"

    def test_subclasses_inherit_from_compile_error_and_friendly_base(self) -> None:
        # Each canonical class must also subclass a "friendly" base
        # (ValueError / TypeError) so generic ``except`` blocks
        # still see them per WF-IMPL-024 spec.
        assert issubclass(CompileParseError, CompileError)
        assert issubclass(CompileParseError, ValueError)
        assert issubclass(CompileTypeError, CompileError)
        assert issubclass(CompileTypeError, builtins.TypeError)
        assert issubclass(CompileTopologyError, CompileError)
        assert issubclass(CompileTopologyError, ValueError)
        assert issubclass(CompileRetryPolicyError, CompileError)
        assert issubclass(CompileRetryPolicyError, ValueError)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pos() -> SourcePosition:
    return SourcePosition(line=3, column=7, offset=42)


# ---------------------------------------------------------------------------
# CompileParseError
# ---------------------------------------------------------------------------


class TestCompileParseError:
    def test_construction_happy_path(self) -> None:
        err = CompileParseError(
            "syntax error",
            source_position=_pos(),
            step_id="scan",
            call_site_path="if",
        )
        assert err.kind == "compile.parse_error"
        assert err.message == "syntax error"
        assert err.source_position == _pos()
        assert err.step_id == "scan"
        assert err.call_site_path == "if"
        assert err.cause is None
        # ValueError catch path must still see it.
        assert isinstance(err, ValueError)

    def test_to_dict_shape_and_key_order(self) -> None:
        err = CompileParseError(
            "syntax error",
            source_position=_pos(),
            step_id="scan",
            call_site_path="if",
        )
        d = err.to_dict()
        assert list(d.keys()) == [
            "kind",
            "message",
            "source_position",
            "step_id",
            "call_site_path",
            "cause",
        ]
        assert d == {
            "kind": "compile.parse_error",
            "message": "syntax error",
            "source_position": {"line": 3, "column": 7, "offset": 42},
            "step_id": "scan",
            "call_site_path": "if",
            "cause": None,
        }

    def test_to_dict_source_position_is_none_when_missing(self) -> None:
        err = CompileParseError("syntax error")
        assert err.to_dict()["source_position"] is None

    def test_repr_is_stable_and_includes_every_field(self) -> None:
        err = CompileParseError(
            "syntax error",
            step_id="scan",
            call_site_path="if",
        )
        r = repr(err)
        # Deterministic string: type name + every field name.
        assert r.startswith("CompileParseError(")
        assert "kind='compile.parse_error'" in r
        assert "message='syntax error'" in r
        assert "source_position=None" in r
        assert "step_id='scan'" in r
        assert "call_site_path='if'" in r
        assert "cause=None" in r

    def test_hashable_and_equal_on_fields(self) -> None:
        a = CompileParseError("m", step_id="s", call_site_path="if")
        b = CompileParseError("m", step_id="s", call_site_path="if")
        c = CompileParseError("m", step_id="s", call_site_path="other")
        assert a == b
        assert hash(a) == hash(b)
        assert a != c
        # Hashable identity: insertable into a set.
        assert {a, b, c} == {a, c}

    def test_wraps_cel_parse_error_preserves_kind_and_message(self) -> None:
        cel_cause = CelParseError(
            "celpy: unexpected token at line 1",
            source_position=SourcePosition(line=1, column=4, offset=3),
        )
        err = CompileParseError(
            "compile: parse failed",
            step_id="scan",
            call_site_path="if",
            cause=cel_cause,
        )
        cause_dict = err.to_dict()["cause"]
        assert cause_dict == {
            "kind": "expression.parse_error",
            "message": "celpy: unexpected token at line 1",
        }


# ---------------------------------------------------------------------------
# CompileTypeError
# ---------------------------------------------------------------------------


class TestCompileTypeError:
    def test_construction_happy_path(self) -> None:
        err = CompileTypeError(
            "expected string, got int",
            source_position=_pos(),
            step_id="scan",
            call_site_path="with.image",
        )
        assert err.kind == "compile.type_error"
        assert err.message == "expected string, got int"
        assert err.step_id == "scan"
        assert err.call_site_path == "with.image"
        assert err.cause is None
        # builtins.TypeError catch path must still see it.
        assert isinstance(err, builtins.TypeError)

    def test_to_dict_shape_and_key_order(self) -> None:
        err = CompileTypeError(
            "expected string, got int",
            source_position=_pos(),
            step_id="scan",
            call_site_path="with.image",
        )
        d = err.to_dict()
        assert list(d.keys()) == [
            "kind",
            "message",
            "source_position",
            "step_id",
            "call_site_path",
            "cause",
        ]
        assert d["kind"] == "compile.type_error"
        assert d["step_id"] == "scan"
        assert d["call_site_path"] == "with.image"

    def test_repr_is_stable(self) -> None:
        err = CompileTypeError("x", step_id="scan", call_site_path="with.image")
        r = repr(err)
        assert r.startswith("CompileTypeError(")
        assert "kind='compile.type_error'" in r
        assert "step_id='scan'" in r
        assert "call_site_path='with.image'" in r

    def test_hashable_and_equal_on_fields(self) -> None:
        a = CompileTypeError("m", step_id="s", call_site_path="p")
        b = CompileTypeError("m", step_id="s", call_site_path="p")
        c = CompileTypeError("m", step_id="t", call_site_path="p")
        assert a == b
        assert hash(a) == hash(b)
        assert a != c

    def test_wraps_cel_type_error_preserves_kind_and_message(self) -> None:
        cel_cause = CelTypeError(
            "expected string, got int",
            expected_type="string",
            actual_type="int",
        )
        err = CompileTypeError(
            "compile: type mismatch",
            step_id="scan",
            call_site_path="with.image",
            cause=cel_cause,
        )
        cause_dict = err.to_dict()["cause"]
        assert cause_dict == {
            "kind": "expression.type_error",
            "message": "expected string, got int",
        }


# ---------------------------------------------------------------------------
# CompileTopologyError
# ---------------------------------------------------------------------------


class TestCompileTopologyError:
    def test_construction_happy_path(self) -> None:
        err = CompileTopologyError(
            "cycle detected: a -> b -> a",
            cycle=("a", "b", "a"),
        )
        assert err.kind == "compile.topology_error"
        assert err.message == "cycle detected: a -> b -> a"
        assert err.cycle == ("a", "b", "a")
        assert err.source_position is None
        assert isinstance(err, ValueError)

    def test_to_dict_shape_and_key_order(self) -> None:
        err = CompileTopologyError(
            "cycle detected: a -> b -> a",
            cycle=("a", "b", "a"),
        )
        d = err.to_dict()
        assert list(d.keys()) == [
            "kind",
            "message",
            "source_position",
            "cycle",
        ]
        assert d == {
            "kind": "compile.topology_error",
            "message": "cycle detected: a -> b -> a",
            "source_position": None,
            "cycle": ["a", "b", "a"],
        }

    def test_default_cycle_is_empty_tuple(self) -> None:
        # Not every topology rejection is a cycle (e.g. forward
        # reference); callers should be able to construct without
        # one and still get a well-formed envelope.
        err = CompileTopologyError("forward reference to step 'ghost'")
        assert err.cycle == ()
        assert err.to_dict()["cycle"] == []

    def test_repr_is_stable(self) -> None:
        err = CompileTopologyError("cycle", cycle=("a", "b"))
        r = repr(err)
        assert r.startswith("CompileTopologyError(")
        # ``cycle`` round-trips through ``to_dict`` as a list so the
        # JSON-rendered shape is byte-stable. ``__repr__`` mirrors
        # that to_dict-level shape rather than the raw tuple.
        assert "cycle=['a', 'b']" in r

    def test_hashable_and_equal_on_fields(self) -> None:
        a = CompileTopologyError("m", cycle=("a", "b"))
        b = CompileTopologyError("m", cycle=("a", "b"))
        c = CompileTopologyError("m", cycle=("a", "c"))
        assert a == b
        assert hash(a) == hash(b)
        assert a != c


# ---------------------------------------------------------------------------
# CompileRetryPolicyError
# ---------------------------------------------------------------------------


class TestCompileRetryPolicyError:
    def test_construction_happy_path(self) -> None:
        err = CompileRetryPolicyError(
            "max_delay < initial_delay",
            field="backoff.maxDelay",
            reason="max_delay < initial_delay",
        )
        assert err.kind == "compile.retry_policy_error"
        assert err.message == "max_delay < initial_delay"
        assert err.field == "backoff.maxDelay"
        assert err.reason == "max_delay < initial_delay"
        assert isinstance(err, ValueError)

    def test_to_dict_shape_and_key_order(self) -> None:
        err = CompileRetryPolicyError(
            "max_delay < initial_delay",
            field="backoff.maxDelay",
            reason="max_delay < initial_delay",
        )
        d = err.to_dict()
        assert list(d.keys()) == [
            "kind",
            "message",
            "source_position",
            "field",
            "reason",
        ]
        assert d == {
            "kind": "compile.retry_policy_error",
            "message": "max_delay < initial_delay",
            "source_position": None,
            "field": "backoff.maxDelay",
            "reason": "max_delay < initial_delay",
        }

    def test_optional_fields_default_to_none(self) -> None:
        err = CompileRetryPolicyError("policy invalid")
        assert err.field is None
        assert err.reason is None
        assert err.to_dict()["field"] is None
        assert err.to_dict()["reason"] is None

    def test_repr_is_stable(self) -> None:
        err = CompileRetryPolicyError("bad", field="backoff.maxDelay", reason="oops")
        r = repr(err)
        assert r.startswith("CompileRetryPolicyError(")
        assert "field='backoff.maxDelay'" in r
        assert "reason='oops'" in r

    def test_hashable_and_equal_on_fields(self) -> None:
        a = CompileRetryPolicyError("m", field="f", reason="r")
        b = CompileRetryPolicyError("m", field="f", reason="r")
        c = CompileRetryPolicyError("m", field="f", reason="other")
        assert a == b
        assert hash(a) == hash(b)
        assert a != c


# ---------------------------------------------------------------------------
# Cross-class equality & identity
# ---------------------------------------------------------------------------


class TestCrossClassIdentity:
    def test_different_subclasses_with_same_message_not_equal(self) -> None:
        # The hashable identity is keyed on the concrete type so a
        # CompileParseError and a CompileTopologyError with the
        # same message do not collide in dedup-by-set audit
        # pipelines.
        a = CompileParseError("oops")
        b = CompileTopologyError("oops")
        assert a != b
        assert hash(a) != hash(b)

    def test_subclass_eq_returns_not_implemented_against_other_types(self) -> None:
        # __eq__ must return NotImplemented (not False) so Python's
        # reflected-equality protocol still works for foreign types.
        err = CompileParseError("oops")
        assert err.__eq__("oops") is NotImplemented
        assert err.__eq__(object()) is NotImplemented

    def test_cause_dict_round_trips_through_identity(self) -> None:
        # ``cause`` is rendered into a dict by
        # :meth:`_extra_fields`; the identity tuple must freeze
        # the dict into a tuple-of-tuples so the error remains
        # hashable. Two errors with identical causes hash equal.
        cel_cause = CelParseError("celpy: boom")
        a = CompileParseError("x", step_id="s", call_site_path="p", cause=cel_cause)
        b = CompileParseError("x", step_id="s", call_site_path="p", cause=cel_cause)
        assert a == b
        assert hash(a) == hash(b)
        # Inserting into a set proves the identity remained hashable.
        assert {a, b} == {a}

    def test_base_extra_fields_hook_default_is_empty(self) -> None:
        # Covers the base-class ``_extra_fields`` default. Subclasses
        # always override, so this hook is only invoked when a
        # consumer builds a custom CompileError subclass without
        # extras.

        class _CustomCompileError(CompileError):
            KIND = "compile.test_only_error"

        err = _CustomCompileError("plain")
        assert err._extra_fields() == {}
        assert err.to_dict() == {
            "kind": "compile.test_only_error",
            "message": "plain",
            "source_position": None,
        }

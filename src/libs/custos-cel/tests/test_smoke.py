"""Smoke tests for the custos_cel public surface.

These tests assert that the public-API surface exists and that the
remaining placeholder stubs (:func:`custos_cel.type_check` and
:func:`custos_cel.evaluate`) raise :class:`NotImplementedError`. The
parser stub became real in WF-IMPL-003 — see ``test_parse.py``. Full
behavior tests for the type checker and evaluator land in WF-IMPL-009 /
WF-IMPL-010.
"""

from __future__ import annotations

import pytest

import custos_cel
from custos_cel import Ident, SourcePosition


def _stub_ast() -> custos_cel.Node:
    return Ident(pos=SourcePosition(line=1, column=1), name="x")


def test_package_imports() -> None:
    assert custos_cel.__version__ == "0.1.0"


def test_public_api_reexports_present() -> None:
    for name in ("parse", "type_check", "evaluate"):
        assert hasattr(custos_cel, name), f"custos_cel.{name} missing"
        assert callable(getattr(custos_cel, name)), f"custos_cel.{name} not callable"


def test_public_type_aliases_present() -> None:
    for name in ("AST", "TypedAST"):
        assert hasattr(custos_cel, name), f"custos_cel.{name} missing"
    # Both aliases resolve to the Node class.
    assert custos_cel.AST is custos_cel.Node
    assert custos_cel.TypedAST is custos_cel.Node


def test_public_api_in_dunder_all() -> None:
    for name in ("AST", "TypedAST", "parse", "type_check", "evaluate"):
        assert name in custos_cel.__all__


def test_type_check_rejects_non_schema_bindings() -> None:
    # ``type_check`` was a NotImplementedError stub up to WF-IMPL-004;
    # WF-IMPL-005 wires the real implementation. A non-``SchemaBindings``
    # argument is rejected up front, before any walk.
    with pytest.raises(TypeError, match="SchemaBindings"):
        custos_cel.type_check(ast=_stub_ast(), bindings=None)  # type: ignore[arg-type]


def test_evaluate_stub_raises_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        custos_cel.evaluate(ast=_stub_ast(), bindings=None)

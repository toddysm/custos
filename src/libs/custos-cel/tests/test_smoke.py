"""Smoke tests for the custos_cel scaffold.

These tests assert only that the public-API surface exists and that the
placeholder stubs raise :class:`NotImplementedError`. Real behavior tests
land in WF-IMPL-009 / WF-IMPL-010.
"""

from __future__ import annotations

import pytest

import custos_cel


def test_package_imports() -> None:
    assert custos_cel.__version__ == "0.1.0"


def test_public_api_reexports_present() -> None:
    for name in ("parse", "type_check", "evaluate"):
        assert hasattr(custos_cel, name), f"custos_cel.{name} missing"
        assert callable(getattr(custos_cel, name)), f"custos_cel.{name} not callable"


def test_public_type_aliases_present() -> None:
    for name in ("AST", "TypedAST"):
        assert hasattr(custos_cel, name), f"custos_cel.{name} missing"


def test_public_api_in_dunder_all() -> None:
    for name in ("AST", "TypedAST", "parse", "type_check", "evaluate"):
        assert name in custos_cel.__all__


def test_parse_stub_raises_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        custos_cel.parse("1 + 1")


def test_type_check_stub_raises_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        custos_cel.type_check(ast=None, bindings=None)


def test_evaluate_stub_raises_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        custos_cel.evaluate(ast=None, bindings=None)

"""Proof-of-life that the chosen CEL parser (cel-python / celpy) is wired in
and can parse the canonical example expression from the WF-IMPL-002 issue.

This is a parser-only smoke. We never call ``program()`` or ``evaluate()``;
those are implemented in WF-IMPL-005 / WF-IMPL-006 once the type checker and
sandbox land. The parse-only contract is exactly the surface that Catalog
Service needs at publish time per change record bundle-h (2026-05-18-003).

These tests intentionally exercise celpy directly (not via the
``custos_cel`` public surface) because the public surface is the
NotImplementedError scaffold at WF-IMPL-001 and the wrapping data model
lands in WF-IMPL-003.
"""

from __future__ import annotations

import celpy
import pytest
from celpy.celparser import CELParseError


def _compile(source: str) -> object:
    env = celpy.Environment()
    return env.compile(source)


def test_celpy_imports_and_parses_trivial_expression() -> None:
    ast = _compile("1 + 1")
    assert ast is not None


def test_celpy_parses_indexed_step_outputs_expression() -> None:
    # Bracket form is required when a step id contains a hyphen because CEL
    # identifiers are [A-Za-z_][A-Za-z0-9_]* (a hyphen would otherwise be
    # parsed as subtraction). This is the form workflow compilation will
    # use whenever a step id is not a bare identifier.
    source = 'steps["scan"].outputs.critical + steps["scan-alt"].outputs.critical'
    ast = _compile(source)
    assert ast is not None


def test_celpy_rejects_obviously_malformed_expression() -> None:
    with pytest.raises(CELParseError):
        _compile("1 +")


def test_celpy_parses_unbound_identifier_without_evaluation() -> None:
    # Parse-only: unbound identifiers are a type-check / evaluation concern,
    # not a parse concern. This is precisely the property Catalog relies on
    # to do publish-time syntactic validation without a binding scope.
    ast = _compile("does_not_exist + 1")
    assert ast is not None

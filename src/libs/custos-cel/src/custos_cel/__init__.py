"""Custos sandboxed CEL-like expression evaluator.

This package hosts the parser, type checker, and replay-deterministic runtime
for workflow expressions used by the Workflow Service Step Coordinator and by
the Catalog Service publish-time validator (parser half only).

See the design at:
https://github.com/toddysm/custos/blob/main/design/components/workflow-service/design.md
(§ Expression Evaluator / ADR-011).

This module currently exposes only the public-API skeleton; concrete
implementations land in follow-up issues (WF-IMPL-002 through WF-IMPL-012).
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "__version__",
    "evaluate",
    "parse",
    "type_check",
]

__version__ = "0.1.0"


def parse(source: str) -> Any:
    """Parse a CEL-like expression source string into a typed AST.

    Args:
        source: The expression source text.

    Returns:
        A serializable typed-AST node. Concrete type defined in WF-IMPL-003.

    Raises:
        NotImplementedError: Always. Implementation lands in WF-IMPL-002 /
            WF-IMPL-003.
    """
    raise NotImplementedError(
        "custos_cel.parse is not yet implemented; see WF-IMPL-002 / WF-IMPL-003."
    )


def type_check(ast: Any, bindings: Any) -> Any:
    """Type-check a parsed AST against JSON Schema bindings.

    Args:
        ast: A typed AST produced by :func:`parse`.
        bindings: The binding scope describing available identifiers and
            their JSON Schemas.

    Returns:
        A typed-AST annotated with resolved types. Concrete type defined in
        WF-IMPL-005.

    Raises:
        NotImplementedError: Always. Implementation lands in WF-IMPL-005.
    """
    raise NotImplementedError("custos_cel.type_check is not yet implemented; see WF-IMPL-005.")


def evaluate(ast: Any, bindings: Any) -> Any:
    """Evaluate a type-checked AST against a binding scope.

    Args:
        ast: A type-checked AST.
        bindings: The binding scope providing concrete values.

    Returns:
        The evaluated result. Concrete type defined in WF-IMPL-006.

    Raises:
        NotImplementedError: Always. Implementation lands in WF-IMPL-006.
    """
    raise NotImplementedError("custos_cel.evaluate is not yet implemented; see WF-IMPL-006.")

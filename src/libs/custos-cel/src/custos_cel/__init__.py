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
    "AST",
    "TypedAST",
    "__version__",
    "evaluate",
    "parse",
    "type_check",
]

__version__ = "0.1.0"

# Public type aliases — terminology locked in WF-IMPL-001. The concrete node
# classes land later but the names are part of the public contract now, so
# downstream consumers (Workflow Service, Catalog Service) can write
# signatures against stable names.
#
# - AST: the untyped, purely structural parse tree produced by :func:`parse`.
#   Carries source positions but no resolved types or binding information.
#   Concrete data model lands in WF-IMPL-003.
# - TypedAST: the AST annotated with resolved types after binding-scope and
#   JSON-Schema validation by :func:`type_check`. Required input to
#   :func:`evaluate`. Concrete data model lands in WF-IMPL-005.
#
# Both are aliased to ``Any`` in the scaffold so the public surface compiles
# under ``mypy --strict`` before the concrete classes exist. The aliases will
# be re-pointed (not renamed) at their concrete classes in their respective
# follow-up issues — callers writing ``custos_cel.AST`` / ``custos_cel.TypedAST``
# today will keep working without source changes.
AST = Any
TypedAST = Any


def parse(source: str) -> AST:
    """Parse a CEL-like expression source string into an untyped AST.

    The returned node is purely structural — it carries source positions but
    **no** resolved types and **no** binding information. Use
    :func:`type_check` to lift the result to a :data:`TypedAST` before
    handing it to :func:`evaluate`.

    Args:
        source: The expression source text.

    Returns:
        An :data:`AST` node. Concrete data model lands in WF-IMPL-003.

    Raises:
        NotImplementedError: Always. Implementation lands in WF-IMPL-002 /
            WF-IMPL-003.
    """
    raise NotImplementedError(
        "custos_cel.parse is not yet implemented; see WF-IMPL-002 / WF-IMPL-003."
    )


def type_check(ast: AST, bindings: Any) -> TypedAST:
    """Type-check an :data:`AST` against JSON Schema bindings.

    Resolves every identifier against ``bindings`` and annotates each node
    with its inferred type, producing a :data:`TypedAST`. The result is the
    only input shape accepted by :func:`evaluate`.

    Args:
        ast: An :data:`AST` produced by :func:`parse`. Must **not** be a
            :data:`TypedAST` (the type checker is not idempotent at the
            value level — re-checking a typed tree is a usage error and
            will be rejected once WF-IMPL-005 lands).
        bindings: The binding scope describing available identifiers and
            their JSON Schemas.

    Returns:
        A :data:`TypedAST` — the input tree annotated with resolved types.
        Concrete data model lands in WF-IMPL-005.

    Raises:
        NotImplementedError: Always. Implementation lands in WF-IMPL-005.
    """
    raise NotImplementedError("custos_cel.type_check is not yet implemented; see WF-IMPL-005.")


def evaluate(ast: TypedAST, bindings: Any) -> Any:
    """Evaluate a :data:`TypedAST` against a binding scope.

    Requires a :data:`TypedAST` (the output of :func:`type_check`). Passing
    an untyped :data:`AST` directly from :func:`parse` is a usage error and
    will be rejected once WF-IMPL-006 lands.

    Args:
        ast: A :data:`TypedAST` produced by :func:`type_check`.
        bindings: The binding scope providing concrete values.

    Returns:
        The evaluated result. Concrete type defined in WF-IMPL-006.

    Raises:
        NotImplementedError: Always. Implementation lands in WF-IMPL-006.
    """
    raise NotImplementedError("custos_cel.evaluate is not yet implemented; see WF-IMPL-006.")

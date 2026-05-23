"""Publish-time CEL syntactic + name-binding validator (CS-IMPL-007).

Per ``bundle-h``
(:file:`design/components/workflow-service/changes/2026-05-18-003-bundle-h-cel-parse-surface.md`)
Catalog is the **sole syntactic gate** for CEL. The Workflow Service
re-parses each expression at ``StartRun`` from the original source
string stored on ``WorkflowVersion.document``; this module's job is to
make sure that re-parse will succeed, and that every identifier root
refers to something the workflow has actually declared.

Pipeline position (third gate, after the schema and normalize passes):

    raw doc  ->  validate_workflow / validate_template (CS-IMPL-005)
             ->  normalize_workflow / normalize_template (CS-IMPL-006)
             ->  validate_expressions (THIS MODULE)
             ->  apply_resolutions (CS-IMPL-008)

Scope of name-binding checks (per design § Publish-Time Validation
Scope and `custos_cel.scope`):

* Root identifiers must be one of ``inputs``, ``steps``, ``run``,
  ``workflow``, ``let``, ``placeholders`` (templates only), or
  ``item`` (only inside a ``forEach`` step). Anything else is an
  :class:`CelNameBindingError`.
* ``inputs.<n>`` requires ``<n>`` declared in ``spec.inputs``.
* ``steps.<id>`` requires ``<id>`` to be a step that appears
  *earlier* in ``spec.steps[]`` (the current step can reference its
  own ``let`` entries via ``let.<name>``; forward references and
  self-references through ``steps.*`` are rejected at publish).
* ``placeholders.<n>`` (template) requires ``<n>`` declared in
  ``spec.placeholders[]``.

Type-binding (e.g. whether ``steps.scan.outputs.critical`` is an
integer) is intentionally **not** done here — that is the Workflow
Service Definition Compiler's job (WF-IMPL-005). Function name
allow-listing is similarly deferred to runtime; we walk into Call
arguments but do not validate the function name.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Final

from custos_cel import (
    Call,
    Conditional,
    Ident,
    Index,
    Member,
    Node,
    ParseError,
    SourcePosition,
    parse,
)
from custos_cel.ast import (
    Binary,
    ListLit,
    Literal,
    MapLit,
    Unary,
)

from custos_catalog.normalize import NormalizedTemplate, NormalizedWorkflow

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Root identifiers permitted at the top of any access chain. Matches
#: the binding model in ``src/libs/custos-cel/src/custos_cel/scope.py``
#: minus the per-step ``item`` (added dynamically when a step has
#: ``forEach``) and ``placeholders`` (added dynamically for templates).
_GLOBAL_ROOTS: Final[frozenset[str]] = frozenset(
    {
        "inputs",
        "steps",
        "run",
        "workflow",
        "let",
        "now",
    },
)

#: Wrapper pattern. The validator slices off the wrapper before
#: handing the inner source to :func:`custos_cel.parse`. We accept
#: optional whitespace around the inner expression to match how
#: humans write these in YAML.
_WRAPPER_RE: Final[re.Pattern[str]] = re.compile(r"^\s*\$\{\{(.+)\}\}\s*$", re.DOTALL)


# ---------------------------------------------------------------------------
# Slot model
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ExpressionScope:
    """The set of names bound for one expression slot.

    The scope is computed by :func:`_walk_workflow_slots` /
    :func:`_walk_template_slots` per slot — different positions inside
    the same step see different scopes (e.g. ``forEach`` does NOT see
    ``item`` because ``item`` is what ``forEach`` *produces*).
    """

    inputs: frozenset[str] = frozenset()
    placeholders: frozenset[str] = frozenset()
    step_ids_in_scope: frozenset[str] = frozenset()
    let_names: frozenset[str] = frozenset()
    item_bound: bool = False


@dataclass(frozen=True, slots=True)
class CelSlot:
    """A discovered CEL expression slot in the normalized document.

    Attributes:
        path: Human-readable JSON-Pointer-style location of the slot
            in the source document (e.g. ``"spec.steps[2].if"``).
            Used as the prefix for error messages so operators can
            jump to the offending field directly.
        source: The inner expression source, with the ``${{ }}``
            wrapper stripped. This is what
            :func:`custos_cel.parse` receives.
        scope: Names bound for this slot. Mismatched references
            surface as :class:`CelNameBindingError`.
    """

    path: str
    source: str
    scope: ExpressionScope


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CelValidationIssue:
    """One CEL parse or name-binding failure.

    Attributes:
        path: The document path of the offending slot (e.g.
            ``"spec.steps[2].if"``).
        message: Human-readable explanation, including any candidate
            names for name-binding errors.
        source_position: Position inside the expression's source
            text (line, column, offset) when available. ``None`` for
            errors that happen before parse-tree walking starts.
    """

    path: str
    message: str
    source_position: SourcePosition | None = None


class CelValidationError(ValueError):
    """Base class for CEL parse / name-binding failures collected in one pass."""

    def __init__(self, kind: str, issues: list[CelValidationIssue]) -> None:
        self.kind = kind
        self.issues = issues
        rendered = "; ".join(f"{i.path}: {i.message}" for i in issues)
        super().__init__(f"{kind}: {len(issues)} issue(s): {rendered}")


class CelSyntaxError(CelValidationError):
    """Raised when at least one expression slot fails to parse.

    Wraps :class:`custos_cel.ParseError` with the document path of the
    offending slot prepended.
    """

    def __init__(self, issues: list[CelValidationIssue]) -> None:
        super().__init__("CEL syntax errors", issues)


class CelNameBindingError(CelValidationError):
    """Raised when at least one identifier root or member is unresolved."""

    def __init__(self, issues: list[CelValidationIssue]) -> None:
        super().__init__("CEL name binding errors", issues)


# ---------------------------------------------------------------------------
# AST walking helpers
# ---------------------------------------------------------------------------


def _leftmost_target(node: Node) -> Node:
    """Walk down a Member/Index chain to the leftmost node.

    For ``steps.scan.outputs.critical`` this returns the ``Ident("steps")``
    leaf; for ``steps["a-b"].outputs.x`` it returns the same.
    """
    current = node
    while isinstance(current, Member | Index):
        current = current.target
    return current


def _iter_ast(node: Node) -> Iterator[Node]:
    """Pre-order traversal of an AST yielding every node.

    Container nodes that hold children in tuples (``Call.args``,
    ``ListLit.elements``, ``MapLit.entries``) are descended into.
    """
    yield node
    if isinstance(node, Member | Index):
        yield from _iter_ast(node.target)
        if isinstance(node, Index):
            yield from _iter_ast(node.index)
    elif isinstance(node, Call):
        for arg in node.args:
            yield from _iter_ast(arg)
    elif isinstance(node, Binary):
        yield from _iter_ast(node.left)
        yield from _iter_ast(node.right)
    elif isinstance(node, Unary):
        yield from _iter_ast(node.operand)
    elif isinstance(node, Conditional):
        yield from _iter_ast(node.cond)
        yield from _iter_ast(node.then_branch)
        yield from _iter_ast(node.else_branch)
    elif isinstance(node, ListLit):
        for el in node.elements:
            yield from _iter_ast(el)
    elif isinstance(node, MapLit):
        for k, v in node.entries:
            yield from _iter_ast(k)
            yield from _iter_ast(v)
    # Ident / Literal: leaf — nothing more to yield.


def _second_level_name(parent: Node) -> str | None:
    """Return the immediate member/index key off the root, if any.

    For ``inputs.image`` (``Member(target=Ident("inputs"), name="image")``)
    the caller passes the ``Member`` parent and we return ``"image"``.
    For ``steps["scan-alt"].outputs.x`` we walk up the chain until we
    find the immediate Member/Index off the leftmost Ident; here the
    immediate access off ``steps`` is the ``Index`` with the
    ``"scan-alt"`` literal, and we return ``"scan-alt"``.
    """
    if isinstance(parent, Member):
        return parent.name
    if isinstance(parent, Index):
        if isinstance(parent.index, Literal) and isinstance(parent.index.value, str):
            return parent.index.value
        return None
    return None


def _check_roots(
    root: Node,
    *,
    parent: Node | None,
    scope: ExpressionScope,
    slot_path: str,
) -> list[CelValidationIssue]:
    """Validate one root identifier against the scope.

    ``root`` is the leftmost node of an access chain (or a bare
    Ident at the top level). ``parent`` is the closest Member/Index
    node above ``root`` (used to extract the second-level name for
    ``inputs.X`` / ``steps.X`` / ``placeholders.X`` checks). Returns
    a list of issues — empty when this root is well-formed.
    """
    if not isinstance(root, Ident):
        return []

    issues: list[CelValidationIssue] = []
    allowed_roots = set(_GLOBAL_ROOTS)
    if scope.placeholders:
        allowed_roots.add("placeholders")
    if scope.item_bound:
        allowed_roots.add("item")

    if root.name not in allowed_roots:
        candidates = sorted(allowed_roots)
        # Include `let` names in the candidate set hint so authors get
        # useful suggestions when they mis-spell a binding.
        msg = (
            f"unknown identifier {root.name!r}; expected one of "
            f"{candidates} (or a bound `let.<name>`)"
        )
        issues.append(
            CelValidationIssue(path=slot_path, message=msg, source_position=root.pos),
        )
        return issues

    # Root is a known kind — check the second-level name where the
    # workflow's structure constrains it.
    if parent is None:
        # Bare reference like `now` (a function or a value alias).
        # Nothing to validate beyond root presence.
        return issues

    second = _second_level_name(parent)
    if root.name == "inputs":
        if second is not None and second not in scope.inputs:
            issues.append(
                CelValidationIssue(
                    path=slot_path,
                    message=(
                        f"unknown input {second!r}; declared inputs: "
                        f"{sorted(scope.inputs) or '(none)'}"
                    ),
                    source_position=parent.pos,
                ),
            )
    elif root.name == "steps":
        if second is not None and second not in scope.step_ids_in_scope:
            hint = ""
            if "-" in second and isinstance(parent, Member):
                # `steps.foo-bar` parses as subtraction; tell the
                # author to use bracket form.
                hint = (
                    f"; hyphenated step ids must use bracket form, "
                    f'e.g. `steps["{second}"]`'
                )
            issues.append(
                CelValidationIssue(
                    path=slot_path,
                    message=(
                        f"unknown step id {second!r}; steps in scope: "
                        f"{sorted(scope.step_ids_in_scope) or '(none)'}{hint}"
                    ),
                    source_position=parent.pos,
                ),
            )
    elif root.name == "placeholders":
        if second is not None and second not in scope.placeholders:
            issues.append(
                CelValidationIssue(
                    path=slot_path,
                    message=(
                        f"unknown placeholder {second!r}; declared: "
                        f"{sorted(scope.placeholders) or '(none)'}"
                    ),
                    source_position=parent.pos,
                ),
            )
    elif root.name == "let":  # noqa: SIM102
        if second is not None and second not in scope.let_names:
            issues.append(
                CelValidationIssue(
                    path=slot_path,
                    message=(
                        f"unknown let binding {second!r}; bound earlier "
                        f"in this step: {sorted(scope.let_names) or '(none)'}"
                    ),
                    source_position=parent.pos,
                ),
            )
    return issues


def _validate_expression(slot: CelSlot) -> tuple[
    list[CelValidationIssue],
    list[CelValidationIssue],
]:
    """Parse one slot and check its name bindings.

    Returns a (syntax_issues, binding_issues) tuple. A non-empty
    syntax list means the AST could not be built and the binding
    list will be empty.
    """
    try:
        ast = parse(slot.source)
    except ParseError as exc:
        return (
            [
                CelValidationIssue(
                    path=slot.path,
                    message=f"parse error: {exc.message}",
                    source_position=exc.source_position,
                ),
            ],
            [],
        )

    # Walk the AST and check every Member/Index chain root.
    binding_issues: list[CelValidationIssue] = []

    # Pre-compute parent links by traversing. For each Member/Index/Call,
    # find the leftmost node of the access chain (`_leftmost_target`),
    # then validate that leftmost root with the immediate parent.
    for node in _iter_ast(ast):
        # Two cases for "root identifier visible to the validator":
        #
        # 1. A bare `Ident` that is NOT the target of a Member/Index
        #    chain (i.e. a stand-alone variable reference like just
        #    `inputs` or `let`). We catch these by checking every
        #    Ident node and only flagging the ones not consumed as a
        #    Member/Index target — easiest implementation is to walk
        #    every Member/Index chain top-down and only process it
        #    once.
        # 2. The leftmost Ident of a Member/Index chain. Picked off
        #    by `_leftmost_target` when we visit the top of the chain.
        if isinstance(node, Member | Index):
            # Only process the top of each chain — skip if this node
            # is itself the target of another Member/Index.
            # Detecting "topmost" without explicit parent links is
            # awkward; instead we process *every* Member/Index and
            # accept that the leftmost root is re-checked once per
            # chain segment. For chains like `steps.scan.outputs.x`
            # that yields the same root four times, but the issue
            # collection deduplicates by (path, message, position).
            root = _leftmost_target(node)
            # The "parent" used for the second-level name is the
            # Member/Index whose target IS the leftmost root.
            parent: Node = node
            while isinstance(parent, Member | Index) and parent.target is not root:
                parent = parent.target
            binding_issues.extend(
                _check_roots(root, parent=parent, scope=slot.scope, slot_path=slot.path),
            )
        elif isinstance(node, Ident):
            # A bare identifier — we'll see it whether it's part of a
            # chain or not. When it's part of a chain, the chain's
            # leftmost root catch above will produce the same checks
            # (with the parent available). When it's bare, we still
            # need to validate the root name.
            binding_issues.extend(
                _check_roots(node, parent=None, scope=slot.scope, slot_path=slot.path),
            )

    # Deduplicate (path, message, source_position) tuples.
    seen: set[tuple[str, str, tuple[int | None, int | None, int | None] | None]] = set()
    unique: list[CelValidationIssue] = []
    for issue in binding_issues:
        sp = issue.source_position
        key = (
            issue.path,
            issue.message,
            (sp.line, sp.column, sp.offset) if sp is not None else None,
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(issue)
    return ([], unique)


# ---------------------------------------------------------------------------
# Slot discovery
# ---------------------------------------------------------------------------


def _extract_expression(value: object) -> str | None:
    """Strip the `${{ ... }}` wrapper from ``value``, if present."""
    if not isinstance(value, str):
        return None
    match = _WRAPPER_RE.match(value)
    if not match:
        return None
    return match.group(1).strip()


def _walk_workflow_slots(
    spec: dict[str, Any],
    *,
    path_prefix: str,
    placeholders: frozenset[str] = frozenset(),
) -> Iterator[CelSlot]:
    """Yield every CEL slot in a workflow ``spec``.

    The ``placeholders`` argument is non-empty when this spec is the
    inner ``workflow:`` body of a template; the resulting slots see
    ``placeholders.<name>`` as a legal root.
    """
    inputs = frozenset((spec.get("inputs") or {}).keys())

    # Triggers: trigger.connector when expression-bound.
    for trig_idx, trigger in enumerate(spec.get("triggers", []) or []):
        if not isinstance(trigger, dict):
            continue
        expr = _extract_expression(trigger.get("connector"))
        if expr is not None:
            yield CelSlot(
                path=f"{path_prefix}.triggers[{trig_idx}].connector",
                source=expr,
                scope=ExpressionScope(inputs=inputs, placeholders=placeholders),
            )

    # Steps: walk in declaration order so `step_ids_in_scope` only
    # contains earlier ids when a slot for step N is built.
    seen_step_ids: set[str] = set()
    steps = spec.get("steps", []) or []
    for step_idx, step in enumerate(steps):
        if not isinstance(step, dict):
            continue
        step_id = step.get("id") if isinstance(step.get("id"), str) else f"step_{step_idx}"
        step_path = f"{path_prefix}.steps[{step_idx}]"
        base_scope_kwargs: dict[str, Any] = {
            "inputs": inputs,
            "placeholders": placeholders,
            "step_ids_in_scope": frozenset(seen_step_ids),
        }

        # `forEach` evaluates in the outer (item-less) scope.
        for_each = _extract_expression(step.get("forEach"))
        item_bound = for_each is not None
        if for_each is not None:
            yield CelSlot(
                path=f"{step_path}.forEach",
                source=for_each,
                scope=ExpressionScope(**base_scope_kwargs),
            )

        # All other expression slots see `item` iff `forEach` is set.
        item_scope_kwargs = {**base_scope_kwargs, "item_bound": item_bound}

        for field_name in ("if", "when", "unless", "where"):
            expr = _extract_expression(step.get(field_name))
            if expr is not None:
                yield CelSlot(
                    path=f"{step_path}.{field_name}",
                    source=expr,
                    scope=ExpressionScope(**item_scope_kwargs),
                )

        # `with` values: dict of names to (literal or expression).
        with_block = step.get("with")
        if isinstance(with_block, dict):
            for key in sorted(with_block.keys()):
                expr = _extract_expression(with_block[key])
                if expr is not None:
                    yield CelSlot(
                        path=f"{step_path}.with.{key}",
                        source=expr,
                        scope=ExpressionScope(**item_scope_kwargs),
                    )

        # `let` block: bind names progressively so later entries can
        # reference earlier ones via `let.<name>`.
        let_block = step.get("let")
        if isinstance(let_block, dict):
            running_let: set[str] = set()
            # Iterate by declaration order (after normalization,
            # which sorts keys). Sorted order is what authors will see
            # in the resulting WorkflowVersion.document; we accept
            # that this slightly constrains semantics for now.
            for name in sorted(let_block.keys()):
                expr = _extract_expression(let_block[name])
                if expr is not None:
                    yield CelSlot(
                        path=f"{step_path}.let.{name}",
                        source=expr,
                        scope=ExpressionScope(
                            **{**item_scope_kwargs, "let_names": frozenset(running_let)},
                        ),
                    )
                running_let.add(name)

        # Activity / workflow / connector / connectors values can each
        # be a `${{ ... }}` template-style interpolation.
        for field_name in ("activity", "workflow", "connector"):
            expr = _extract_expression(step.get(field_name))
            if expr is not None:
                yield CelSlot(
                    path=f"{step_path}.{field_name}",
                    source=expr,
                    scope=ExpressionScope(**item_scope_kwargs),
                )

        connectors = step.get("connectors")
        if isinstance(connectors, dict):
            for alias in sorted(connectors.keys()):
                expr = _extract_expression(connectors[alias])
                if expr is not None:
                    yield CelSlot(
                        path=f"{step_path}.connectors.{alias}",
                        source=expr,
                        scope=ExpressionScope(**item_scope_kwargs),
                    )

        if isinstance(step_id, str):
            seen_step_ids.add(step_id)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def discover_workflow_slots(norm: NormalizedWorkflow) -> list[CelSlot]:
    """Return the list of CEL slots inside a normalized workflow."""
    spec = norm.document.get("spec") if isinstance(norm.document, dict) else None
    if not isinstance(spec, dict):
        return []
    return list(_walk_workflow_slots(spec, path_prefix="spec"))


def discover_template_slots(norm: NormalizedTemplate) -> list[CelSlot]:
    """Return the list of CEL slots inside a normalized template."""
    template_spec = norm.document.get("spec") if isinstance(norm.document, dict) else None
    if not isinstance(template_spec, dict):
        return []
    placeholder_decls = template_spec.get("placeholders") or []
    names: set[str] = set()
    for decl in placeholder_decls:
        if isinstance(decl, dict) and isinstance(decl.get("name"), str):
            names.add(decl["name"])
    inner = template_spec.get("workflow")
    if not isinstance(inner, dict):
        return []
    return list(
        _walk_workflow_slots(
            inner,
            path_prefix="spec.workflow",
            placeholders=frozenset(names),
        ),
    )


@dataclass(frozen=True, slots=True)
class _ValidationResult:
    """Internal aggregate of all issues collected during validation."""

    syntax: list[CelValidationIssue] = field(default_factory=list)
    binding: list[CelValidationIssue] = field(default_factory=list)


def _run_validation(slots: list[CelSlot]) -> _ValidationResult:
    result = _ValidationResult()
    for slot in slots:
        syn, bind = _validate_expression(slot)
        result.syntax.extend(syn)
        result.binding.extend(bind)
    return result


def validate_expressions(norm: NormalizedWorkflow) -> None:
    """Validate every CEL expression in a normalized workflow.

    Collects all syntax and name-binding errors in one pass. When
    both kinds are present, :class:`CelSyntaxError` is raised first
    (the binding pass cannot run on un-parseable expressions, so a
    fix-and-retry loop converges).

    Raises:
        CelSyntaxError: When at least one slot fails to parse.
        CelNameBindingError: When every slot parses but at least one
            identifier root or member is unresolved.
    """
    result = _run_validation(discover_workflow_slots(norm))
    if result.syntax:
        raise CelSyntaxError(result.syntax)
    if result.binding:
        raise CelNameBindingError(result.binding)


def validate_template_expressions(norm: NormalizedTemplate) -> None:
    """Validate every CEL expression in a normalized template.

    Equivalent to :func:`validate_expressions` but uses the template's
    inner ``spec.workflow`` body as the slot source and exposes the
    declared placeholder names as legal roots.
    """
    result = _run_validation(discover_template_slots(norm))
    if result.syntax:
        raise CelSyntaxError(result.syntax)
    if result.binding:
        raise CelNameBindingError(result.binding)

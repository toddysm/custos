"""Type checker for the Custos CEL evaluator (WF-IMPL-005).

This module implements :func:`type_check`, the StartRun-time gate that
turns a raw :data:`~custos_cel.AST` into a :data:`~custos_cel.TypedAST`
by walking the tree, validating it against JSON-Schema-backed binding
declarations, and annotating every node with its inferred
:class:`~custos_cel.ast.CelType`.

Where it runs: the Workflow Service Definition Compiler at ``StartRun``
(per the bundle-h change record
``2026-05-18-003-bundle-h-cel-parse-surface.md``). Catalog Service has
already gated syntax at publish time; this is the only place a
type-error path runs. Failure is permanent: the Validator rejects the
``StartRun`` request before a ``runId`` is issued.

See the issue: https://github.com/toddysm/custos/issues/180
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any, Final

from custos_cel.ast import (
    Binary,
    BinaryOp,
    BoolType,
    BytesType,
    Call,
    CelType,
    Conditional,
    DoubleType,
    Ident,
    Index,
    IntType,
    ListLit,
    ListType,
    Literal,
    LiteralKind,
    MapLit,
    MapType,
    Member,
    Node,
    NullType,
    SourcePosition,
    StringType,
    TimestampType,
    UintType,
    Unary,
    UnaryOp,
)
from custos_cel.errors import TypeError as _CelTypeError
from custos_cel.scope import UnboundNameError

__all__ = ["SchemaBindings", "TypeCheckError", "type_check"]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


# WF-IMPL-008 (issue #183) lifted the canonical type-check error into
# :mod:`custos_cel.errors` as :class:`custos_cel.errors.TypeError`. The
# WF-IMPL-005 name ``TypeCheckError`` remains the public surface for
# this module and is kept as an alias so existing call sites and the
# ``custos_cel`` public re-export keep working. The class identity is
# the same — ``isinstance(err, TypeCheckError)`` and
# ``isinstance(err, custos_cel.errors.TypeError)`` are interchangeable.
TypeCheckError = _CelTypeError


# ---------------------------------------------------------------------------
# Schema bindings
# ---------------------------------------------------------------------------


_DEFAULT_RUN_TYPES: Final[Mapping[str, CelType]] = MappingProxyType(
    {"id": StringType(), "workspace": StringType()}
)
_DEFAULT_WORKFLOW_TYPES: Final[Mapping[str, CelType]] = MappingProxyType(
    {"name": StringType(), "version": StringType()}
)


@dataclass(frozen=True, kw_only=True, slots=True)
class SchemaBindings:
    """JSON-Schema-backed binding declarations for the type checker.

    Mirrors :class:`custos_cel.BindingScope` at type-check time. The WF
    Definition Compiler constructs one :class:`SchemaBindings` from the
    bound activity input / output schemas in
    ``WorkflowVersion.document`` plus any declared ``let`` types, then
    type-checks every CEL source string in the workflow against it.

    Attributes:
        inputs: JSON Schema (typically an ``object`` schema with
            ``properties``) describing the run's inputs.
        prior_steps: Ordered ``(step_id, outputs_schema)`` pairs for
            steps that have already produced outputs at the point this
            expression appears. Order is preserved for error messages;
            lookup is by step id.
        let: Mapping from ``let.<name>`` to its declared
            :class:`~custos_cel.ast.CelType`. The Catalog Service
            publish gate validates these structurally; here we trust
            them and use them for resolution.
        run: Static types of ``run.*`` members. Defaults to
            ``{"id": StringType, "workspace": StringType}``.
        workflow: Static types of ``workflow.*`` members. Defaults to
            ``{"name": StringType, "version": StringType}``.
        now: Static return type of the ``now()`` call. Defaults to
            :class:`TimestampType`.
    """

    inputs: Mapping[str, Any] = field(default_factory=dict)
    prior_steps: Sequence[tuple[str, Mapping[str, Any]]] = field(default_factory=tuple)
    let: Mapping[str, CelType] = field(default_factory=dict)
    run: Mapping[str, CelType] = field(default=_DEFAULT_RUN_TYPES)
    workflow: Mapping[str, CelType] = field(default=_DEFAULT_WORKFLOW_TYPES)
    now: CelType = field(default_factory=TimestampType)

    def __post_init__(self) -> None:
        # Wrap mutable mappings as immutable views (same idiom as
        # BindingScope). Callers may pass plain dicts for ergonomic
        # construction; the bindings instance guarantees the references
        # it stores are read-only afterwards.
        if not isinstance(self.inputs, MappingProxyType):
            object.__setattr__(self, "inputs", MappingProxyType(dict(self.inputs)))
        if not isinstance(self.prior_steps, tuple):
            object.__setattr__(self, "prior_steps", tuple(self.prior_steps))
        if not isinstance(self.let, MappingProxyType):
            object.__setattr__(self, "let", MappingProxyType(dict(self.let)))
        if not isinstance(self.run, MappingProxyType):
            object.__setattr__(self, "run", MappingProxyType(dict(self.run)))
        if not isinstance(self.workflow, MappingProxyType):
            object.__setattr__(self, "workflow", MappingProxyType(dict(self.workflow)))

    def step_outputs_schema(self, step_id: str) -> Mapping[str, Any] | None:
        """Return the JSON Schema for ``step_id``'s outputs, or ``None``."""
        for sid, schema in self.prior_steps:
            if sid == step_id:
                return schema
        return None


# ---------------------------------------------------------------------------
# JSON Schema → CelType translation
# ---------------------------------------------------------------------------


def _schema_to_celtype(
    schema: Mapping[str, Any], pos: SourcePosition | None, label: str
) -> CelType:
    """Translate a JSON Schema fragment to an internal :class:`CelType`.

    Supported shapes:

    * ``"integer"`` → :class:`IntType`
    * ``"string"`` → :class:`StringType`
    * ``"boolean"`` → :class:`BoolType`
    * ``"number"`` → :class:`DoubleType`
    * ``"array"`` with an ``items`` sub-schema → :class:`ListType`
    * ``"object"`` with ``additionalProperties`` (homogeneous) →
      :class:`MapType` keyed by string.
    * ``"object"`` with ``properties`` (heterogeneous record) →
      :class:`MapType` keyed by string with :class:`NullType` as
      placeholder value type; member-access drills into ``properties``
      directly so the placeholder is only observable when the
      heterogeneous object is used as a value.
    * A list-typed ``"type": ["X", "null"]`` is treated as ``"X"``
      (nullable scalars).
    """
    if not isinstance(schema, Mapping):
        raise TypeCheckError(
            f"expected JSON Schema mapping at {label}, got {type(schema).__name__}",
            source_position=pos,
        )
    jtype_raw: Any = schema.get("type")
    if isinstance(jtype_raw, list):
        non_null = [t for t in jtype_raw if t != "null"]
        if len(non_null) != 1:
            raise TypeCheckError(
                f"unsupported JSON Schema 'type' list at {label}: {jtype_raw!r}",
                source_position=pos,
            )
        jtype: Any = non_null[0]
    else:
        jtype = jtype_raw
    if jtype == "integer":
        return IntType()
    if jtype == "string":
        return StringType()
    if jtype == "boolean":
        return BoolType()
    if jtype == "number":
        return DoubleType()
    if jtype == "array":
        items = schema.get("items")
        if not isinstance(items, Mapping):
            raise TypeCheckError(
                f"array schema at {label} must declare an 'items' sub-schema",
                source_position=pos,
            )
        return ListType(element=_schema_to_celtype(items, pos, f"{label}[]"))
    if jtype == "object":
        additional = schema.get("additionalProperties")
        if isinstance(additional, Mapping):
            return MapType(
                key=StringType(),
                value=_schema_to_celtype(additional, pos, f"{label}.*"),
            )
        # Heterogeneous record — keep the placeholder; drilling into
        # ``properties`` produces concrete per-key types separately.
        return MapType(key=StringType(), value=NullType())
    raise TypeCheckError(
        f"unsupported JSON Schema type {jtype!r} at {label}",
        source_position=pos,
    )


# ---------------------------------------------------------------------------
# Drill state
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Drill:
    """Internal state piggy-backed onto each node's inference result.

    Tracks how to continue resolving the *next* Member or Index access
    against the binding declarations. ``None`` (the absence of a drill)
    means the node represents a plain value (a literal, an operator
    result, a fully-resolved leaf) — Member access on such a node is a
    type error.

    ``kind`` values:

    * ``"schema"``: drilling a JSON Schema fragment (``inputs``
      subtree, step outputs subtree, nested object).
    * ``"name_types"``: drilling a ``name → CelType`` map (``run``,
      ``workflow``, ``let``).
    * ``"steps_root"``: at the bare ``steps`` identifier — next
      member/index must be a step id.
    * ``"step"``: at ``steps.<id>`` — next member must be ``outputs``.
    * ``"list_element"``: at a JSON-Schema-backed array binding —
      ``schema`` carries the array's ``items`` sub-schema. An ``Index``
      operation consumes this drill state to produce the element's
      drill (which may itself be a ``schema`` or another
      ``list_element``). Dotted member access on a list value is a
      type error and is rejected by :func:`_drill_by_name`.
    """

    kind: str
    schema: Mapping[str, Any] | None = None
    types_map: Mapping[str, CelType] | None = None
    step_id: str | None = None
    label: str = ""


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def type_check(ast: Node, bindings: SchemaBindings) -> Node:
    """Type-check ``ast`` against ``bindings`` and return a TypedAST.

    Walks the tree once, resolving every identifier against the binding
    declarations and annotating each node with its inferred
    :class:`~custos_cel.ast.CelType`. The returned tree has the same
    structure as the input but every :attr:`Node.cel_type` is populated
    — i.e. the output is a :data:`~custos_cel.TypedAST` suitable for
    :func:`custos_cel.evaluate`.

    Args:
        ast: An untyped AST produced by :func:`custos_cel.parse`.
        bindings: The :class:`SchemaBindings` describing every binding
            root visible to the expression.

    Returns:
        A fresh :data:`TypedAST` (input tree is unchanged).

    Raises:
        TypeCheckError: For any type mismatch (operator-arity violation,
            branch-type divergence in a ternary, schema/value-type
            mismatch, unsupported language construct).
        UnboundNameError: For any identifier (or step id, or schema
            field) not declared in ``bindings``.
    """
    if not isinstance(bindings, SchemaBindings):
        raise TypeError(
            f"type_check: bindings must be a SchemaBindings instance, got {type(bindings).__name__}"
        )
    _cel_type, _drill, typed = _infer(ast, bindings)
    return typed


# ---------------------------------------------------------------------------
# Inference core
# ---------------------------------------------------------------------------


# Placeholder type for binding-root identifiers (``inputs``, ``steps``,
# ``run``, ``workflow``, ``let``). These idents are only ever meant to be
# drilled into; their static value-type is "a map of string to anything",
# which we model as ``MapType(string, null)`` to signal the value-type
# slot is intentionally unmodelled.
def _placeholder_root_type() -> CelType:
    return MapType(key=StringType(), value=NullType())


def _infer(node: Node, bindings: SchemaBindings) -> tuple[CelType, _Drill | None, Node]:
    """Recursive type inference. Returns ``(cel_type, drill, typed_node)``.

    The caller passes ``typed_node`` upward as the new child for its own
    reconstruction; ``drill`` is consumed by an enclosing Member/Index
    parent and otherwise discarded.
    """
    if isinstance(node, Literal):
        cel_type = _literal_type(node)
        return cel_type, None, replace(node, cel_type=cel_type)

    if isinstance(node, Ident):
        cel_type, drill = _resolve_root(node.name, node.pos, bindings)
        return cel_type, drill, replace(node, cel_type=cel_type)

    if isinstance(node, Member):
        return _infer_member(node, bindings)

    if isinstance(node, Index):
        return _infer_index(node, bindings)

    if isinstance(node, Call):
        return _infer_call(node, bindings)

    if isinstance(node, Conditional):
        return _infer_conditional(node, bindings)

    if isinstance(node, Binary):
        return _infer_binary(node, bindings)

    if isinstance(node, Unary):
        return _infer_unary(node, bindings)

    if isinstance(node, ListLit):
        return _infer_list_lit(node, bindings)

    if isinstance(node, MapLit):
        return _infer_map_lit(node, bindings)

    raise TypeCheckError(
        f"unsupported AST node kind: {type(node).__name__}",
        source_position=getattr(node, "pos", None),
    )


def _literal_type(lit: Literal) -> CelType:
    k = lit.kind
    if k is LiteralKind.INT:
        return IntType()
    if k is LiteralKind.UINT:
        return UintType()
    if k is LiteralKind.DOUBLE:
        return DoubleType()
    if k is LiteralKind.BOOL:
        return BoolType()
    if k is LiteralKind.STRING:
        return StringType()
    if k is LiteralKind.BYTES:
        return BytesType()
    if k is LiteralKind.NULL:
        return NullType()
    raise TypeCheckError(  # pragma: no cover - exhaustive
        f"unhandled literal kind {k}", source_position=lit.pos
    )


def _resolve_root(
    name: str, pos: SourcePosition | None, bindings: SchemaBindings
) -> tuple[CelType, _Drill | None]:
    if name == "inputs":
        return _placeholder_root_type(), _Drill(
            kind="schema", schema=bindings.inputs, label="inputs"
        )
    if name == "steps":
        return _placeholder_root_type(), _Drill(kind="steps_root", label="steps")
    if name == "let":
        return _placeholder_root_type(), _Drill(
            kind="name_types", types_map=bindings.let, label="let"
        )
    if name == "run":
        return _placeholder_root_type(), _Drill(
            kind="name_types", types_map=bindings.run, label="run"
        )
    if name == "workflow":
        return _placeholder_root_type(), _Drill(
            kind="name_types", types_map=bindings.workflow, label="workflow"
        )
    if name == "now":
        # ``now`` is a function name, not a value-typed identifier; a
        # bare reference is a usage error. The Call path handles
        # ``now()`` correctly.
        raise TypeCheckError(
            "'now' is a function and must be called as 'now()'",
            source_position=pos,
        )
    raise UnboundNameError([name], pos=pos, reason=f"unknown root {name!r}")


# ---------------------------------------------------------------------------
# Member / Index
# ---------------------------------------------------------------------------


def _infer_member(node: Member, bindings: SchemaBindings) -> tuple[CelType, _Drill | None, Node]:
    target_type, target_drill, new_target = _infer(node.target, bindings)
    chain = _chain_for(node)
    if target_drill is not None:
        cel_type, drill = _drill_by_name(target_drill, node.name, chain, node.pos, bindings)
        return (
            cel_type,
            drill,
            Member(pos=node.pos, cel_type=cel_type, target=new_target, name=node.name),
        )
    # No drill state — member access on a plain value is rejected. CEL
    # has no structural field access on scalars / lists / homogeneous
    # maps via dot syntax (use bracket form for maps).
    raise TypeCheckError(
        f"cannot access member {node.name!r} on a value of type {_render_type(target_type)}",
        source_position=node.pos,
        expected_type="record",
        actual_type=target_type,
    )


def _infer_index(node: Index, bindings: SchemaBindings) -> tuple[CelType, _Drill | None, Node]:
    target_type, target_drill, new_target = _infer(node.target, bindings)
    _index_type, _index_drill, new_index = _infer(node.index, bindings)
    # Bracket form on a binding subtree with a string literal index is
    # equivalent to dotted member access (this is the canonical way to
    # reference step ids that are not valid CEL identifiers, e.g.
    # ``steps["scan-alt"]``). A ``list_element`` drill is *not* a
    # record-like subtree — its ``schema`` holds the array's items
    # sub-schema, not a ``properties`` mapping — so it must fall
    # through to the static-type branch below, which raises the
    # correct "list index must be int" error for string indices.
    if (
        target_drill is not None
        and target_drill.kind != "list_element"
        and isinstance(node.index, Literal)
        and node.index.kind is LiteralKind.STRING
        and isinstance(node.index.value, str)
    ):
        chain = _chain_for(node)
        cel_type, drill = _drill_by_name(target_drill, node.index.value, chain, node.pos, bindings)
        return (
            cel_type,
            drill,
            Index(pos=node.pos, cel_type=cel_type, target=new_target, index=new_index),
        )
    # Otherwise the target must reduce to a concrete list/map static
    # type and the index type must match the container's key type.
    if isinstance(target_type, ListType):
        if not isinstance(_index_type, IntType):
            raise TypeCheckError(
                "list index must be int, got " + _render_type(_index_type),
                source_position=node.pos,
                expected_type=IntType(),
                actual_type=_index_type,
            )
        elem = target_type.element
        # When the list was reached via a JSON-Schema-backed array
        # binding (e.g. ``inputs.targets`` with ``type: array, items:
        # {...}``), the parent drill carries the items sub-schema.
        # Propagate a drill derived from that sub-schema so further
        # member / index access on the element (``inputs.targets[0]
        # .image``, ``inputs.matrix[0][1]``) drills correctly.
        elem_drill: _Drill | None = None
        if (
            target_drill is not None
            and target_drill.kind == "list_element"
            and target_drill.schema is not None
        ):
            elem_label = f"{target_drill.label}[]" if target_drill.label else "[]"
            elem_drill = _drill_for_subschema(target_drill.schema, elem_label)
        return (
            elem,
            elem_drill,
            Index(pos=node.pos, cel_type=elem, target=new_target, index=new_index),
        )
    if isinstance(target_type, MapType):
        if type(_index_type) is not type(target_type.key):
            raise TypeCheckError(
                f"map index must be {_render_type(target_type.key)}, got "
                f"{_render_type(_index_type)}",
                source_position=node.pos,
                expected_type=target_type.key,
                actual_type=_index_type,
            )
        val = target_type.value
        # When the map was reached via a JSON-Schema-backed
        # ``additionalProperties`` declaration, the parent drill is a
        # ``schema`` drill on the enclosing object. Derive the value
        # drill from that ``additionalProperties`` sub-schema so
        # follow-on access on the value (``inputs.records[k].field``,
        # ``inputs.matrices[k][0]``) drills correctly. String-literal
        # keys do not reach this branch — they are routed through
        # ``_drill_by_name`` / ``_drill_schema`` above, which already
        # handles the additionalProperties fallback.
        val_drill: _Drill | None = None
        if (
            target_drill is not None
            and target_drill.kind == "schema"
            and target_drill.schema is not None
        ):
            additional = target_drill.schema.get("additionalProperties")
            if isinstance(additional, Mapping):
                val_label = f"{target_drill.label}[*]" if target_drill.label else "[*]"
                val_drill = _drill_for_subschema(additional, val_label)
        return (
            val,
            val_drill,
            Index(pos=node.pos, cel_type=val, target=new_target, index=new_index),
        )
    raise TypeCheckError(
        f"cannot index a value of type {_render_type(target_type)}",
        source_position=node.pos,
        actual_type=target_type,
    )


def _drill_by_name(
    parent_drill: _Drill,
    name: str,
    chain: tuple[str, ...],
    pos: SourcePosition | None,
    bindings: SchemaBindings,
) -> tuple[CelType, _Drill | None]:
    if parent_drill.kind == "schema":
        assert parent_drill.schema is not None
        return _drill_schema(parent_drill.schema, name, chain, pos, parent_drill.label)
    if parent_drill.kind == "name_types":
        assert parent_drill.types_map is not None
        if name not in parent_drill.types_map:
            raise UnboundNameError(
                chain, pos=pos, reason=f"unknown {parent_drill.label} field {name!r}"
            )
        return parent_drill.types_map[name], None
    if parent_drill.kind == "steps_root":
        if bindings.step_outputs_schema(name) is None:
            raise UnboundNameError(chain, pos=pos, reason=f"no such step {name!r}")
        return _placeholder_root_type(), _Drill(kind="step", step_id=name, label=f"steps.{name}")
    if parent_drill.kind == "step":
        if name != "outputs":
            raise UnboundNameError(
                chain,
                pos=pos,
                reason=f"step access must use 'outputs', got {name!r}",
            )
        assert parent_drill.step_id is not None
        schema = bindings.step_outputs_schema(parent_drill.step_id)
        assert schema is not None  # verified at the "steps_root" → "step" hop
        return _placeholder_root_type(), _Drill(
            kind="schema",
            schema=schema,
            label=f"steps.{parent_drill.step_id}.outputs",
        )
    if parent_drill.kind == "list_element":
        # Dotted (or string-bracket) member access on a list value is
        # not a CEL operation — the caller must index first. Producing
        # the same error here that ``_infer_member`` raises for a
        # drill-less list keeps the surface uniform.
        label = parent_drill.label or "list"
        raise TypeCheckError(
            f"cannot access member {name!r} on list {label} (index first with [N])",
            source_position=pos,
            expected_type="record",
        )
    raise TypeCheckError(  # pragma: no cover - exhaustive
        f"internal: unknown drill kind {parent_drill.kind!r}",
        source_position=pos,
    )


def _drill_schema(
    schema: Mapping[str, Any],
    name: str,
    chain: tuple[str, ...],
    pos: SourcePosition | None,
    label: str,
) -> tuple[CelType, _Drill | None]:
    properties = schema.get("properties")
    if isinstance(properties, Mapping) and name in properties:
        sub = properties[name]
        if not isinstance(sub, Mapping):
            raise TypeCheckError(
                f"schema property {label}.{name} is not a JSON Schema mapping",
                source_position=pos,
            )
        sub_label = f"{label}.{name}" if label else name
        cel_type = _schema_to_celtype(sub, pos, sub_label)
        return cel_type, _drill_for_subschema(sub, sub_label)
    additional = schema.get("additionalProperties")
    if isinstance(additional, Mapping):
        sub_label = f"{label}.{name}" if label else name
        cel_type = _schema_to_celtype(additional, pos, sub_label)
        return cel_type, _drill_for_subschema(additional, sub_label)
    raise UnboundNameError(chain, pos=pos, reason=f"no such field {name!r} in {label or 'schema'}")


def _drill_for_subschema(sub_schema: Mapping[str, Any], label: str) -> _Drill | None:
    """Return a drill state for a sub-schema if further drilling is possible.

    Objects yield a ``"schema"`` drill so member access can resolve
    ``properties``. Arrays with an ``items`` sub-schema yield a
    ``"list_element"`` drill so ``Index`` can propagate the element's
    drill state — this is what makes ``inputs.targets[0].image``
    type-check when ``targets`` is declared as ``type: array, items:
    {type: object, properties: {...}}``. Anything else returns
    ``None``.
    """
    jtype = sub_schema.get("type")
    if jtype == "object":
        return _Drill(kind="schema", schema=sub_schema, label=label)
    if jtype == "array":
        items = sub_schema.get("items")
        if isinstance(items, Mapping):
            return _Drill(kind="list_element", schema=items, label=label)
    return None


def _drill_for_subschema_celtype(cel_type: CelType) -> _Drill | None:
    """Subschema drill derived from a CelType only.

    Retained for use sites where only a CelType is available (no JSON
    Schema context). Currently always returns ``None`` because dotted
    drilling requires schema information; ``Index`` on a JSON-Schema
    array now propagates the items sub-schema via the parent drill
    instead of through the element CelType.
    """
    del cel_type
    return None


# ---------------------------------------------------------------------------
# Calls
# ---------------------------------------------------------------------------


def _infer_call(node: Call, bindings: SchemaBindings) -> tuple[CelType, _Drill | None, Node]:
    if node.function == "now":
        if node.args:
            raise TypeCheckError(
                f"'now' takes no arguments, got {len(node.args)}",
                source_position=node.pos,
            )
        return (
            bindings.now,
            None,
            Call(pos=node.pos, cel_type=bindings.now, function=node.function, args=()),
        )
    if node.function == "size":
        if len(node.args) != 1:
            raise TypeCheckError(
                f"'size' takes exactly one argument, got {len(node.args)}",
                source_position=node.pos,
            )
        arg_type, _drill, new_arg = _infer(node.args[0], bindings)
        if not isinstance(arg_type, (StringType, BytesType, ListType, MapType)):
            raise TypeCheckError(
                "'size' argument must be string, bytes, list, or map; got "
                + _render_type(arg_type),
                source_position=node.args[0].pos,
                expected_type="string|bytes|list|map",
                actual_type=arg_type,
            )
        return (
            IntType(),
            None,
            Call(pos=node.pos, cel_type=IntType(), function=node.function, args=(new_arg,)),
        )
    if node.function == "type":
        if len(node.args) != 1:
            raise TypeCheckError(
                f"'type' takes exactly one argument, got {len(node.args)}",
                source_position=node.pos,
            )
        _arg_type, _drill, new_arg = _infer(node.args[0], bindings)
        return (
            StringType(),
            None,
            Call(pos=node.pos, cel_type=StringType(), function=node.function, args=(new_arg,)),
        )
    if node.function == "has":
        # ``has(x.field)`` / ``has(x[k])`` — CEL macro semantics:
        # the argument is a member or index access whose target is
        # type-checked but whose final accessor is *not* required to
        # exist. We still require the accessor to be statically known
        # (string-literal index or dotted member), matching the CEL
        # ``has()`` macro contract; runtime-resolved field probes are
        # not part of the subset.
        if len(node.args) != 1:
            raise TypeCheckError(
                f"'has' takes exactly one argument, got {len(node.args)}",
                source_position=node.pos,
            )
        arg = node.args[0]
        new_arg = _typecheck_has_argument(arg, bindings)
        return (
            BoolType(),
            None,
            Call(pos=node.pos, cel_type=BoolType(), function=node.function, args=(new_arg,)),
        )
    # No other functions are part of the Custos CEL subset.
    raise TypeCheckError(
        f"unknown function {node.function!r}",
        source_position=node.pos,
    )


def _typecheck_has_argument(arg: Node, bindings: SchemaBindings) -> Node:
    """Type-check the single argument of ``has()``.

    The argument must be a ``Member`` access or a string-literal
    ``Index`` access whose target type-checks normally. The final
    accessor itself is *not* required to be statically declared — that
    is the whole point of ``has()``: probe for existence without
    failing.
    """
    if isinstance(arg, Member):
        _t, _d, new_target = _infer(arg.target, bindings)
        return Member(pos=arg.pos, cel_type=BoolType(), target=new_target, name=arg.name)
    if isinstance(arg, Index):
        if not (
            isinstance(arg.index, Literal)
            and arg.index.kind is LiteralKind.STRING
            and isinstance(arg.index.value, str)
        ):
            raise TypeCheckError(
                "'has' argument must be a dotted member or a string-literal index",
                source_position=arg.pos,
            )
        _t, _d, new_target = _infer(arg.target, bindings)
        _it, _id, new_index = _infer(arg.index, bindings)
        return Index(pos=arg.pos, cel_type=BoolType(), target=new_target, index=new_index)
    raise TypeCheckError(
        "'has' argument must be a dotted member or a string-literal index, "
        f"got {type(arg).__name__}",
        source_position=arg.pos,
    )


# ---------------------------------------------------------------------------
# Conditional (ternary)
# ---------------------------------------------------------------------------


def _infer_conditional(
    node: Conditional, bindings: SchemaBindings
) -> tuple[CelType, _Drill | None, Node]:
    cond_type, _cd, new_cond = _infer(node.cond, bindings)
    if not isinstance(cond_type, BoolType):
        raise TypeCheckError(
            "ternary condition must be bool, got " + _render_type(cond_type),
            source_position=node.cond.pos,
            expected_type=BoolType(),
            actual_type=cond_type,
        )
    then_type, _td, new_then = _infer(node.then_branch, bindings)
    else_type, _ed, new_else = _infer(node.else_branch, bindings)
    unified = _unify_branch_types(then_type, else_type)
    if unified is None:
        raise TypeCheckError(
            f"ternary branches have incompatible types: "
            f"then={_render_type(then_type)}, else={_render_type(else_type)}",
            source_position=node.pos,
            expected_type=then_type,
            actual_type=else_type,
        )
    return (
        unified,
        None,
        Conditional(
            pos=node.pos,
            cel_type=unified,
            cond=new_cond,
            then_branch=new_then,
            else_branch=new_else,
        ),
    )


def _unify_branch_types(a: CelType, b: CelType) -> CelType | None:
    """Unify two branch types for ternaries / list-element collections.

    Rules:

    * Equal types unify to themselves.
    * ``NullType`` on either side unifies to the other side (CEL
      conventionally allows a null branch in a ternary returning a
      reference-type result; we extend that to any non-null type for
      simplicity).
    * Parametric types (``ListType``, ``MapType``) unify element-wise.
    * Otherwise the types are incompatible.
    """
    if a == b:
        return a
    if isinstance(a, NullType):
        return b
    if isinstance(b, NullType):
        return a
    if isinstance(a, ListType) and isinstance(b, ListType):
        sub = _unify_branch_types(a.element, b.element)
        return ListType(element=sub) if sub is not None else None
    if isinstance(a, MapType) and isinstance(b, MapType):
        k = _unify_branch_types(a.key, b.key)
        v = _unify_branch_types(a.value, b.value)
        if k is None or v is None:
            return None
        return MapType(key=k, value=v)
    return None


# ---------------------------------------------------------------------------
# Binary / Unary
# ---------------------------------------------------------------------------


_NUMERIC_TYPES: Final = (IntType, UintType, DoubleType)
_COMPARABLE_TYPES: Final = (
    IntType,
    UintType,
    DoubleType,
    StringType,
    BytesType,
    TimestampType,
)


def _infer_binary(node: Binary, bindings: SchemaBindings) -> tuple[CelType, _Drill | None, Node]:
    left_type, _ld, new_left = _infer(node.left, bindings)
    right_type, _rd, new_right = _infer(node.right, bindings)
    op = node.op
    result_type = _binary_result_type(op, left_type, right_type, node.pos)
    return (
        result_type,
        None,
        Binary(
            pos=node.pos,
            cel_type=result_type,
            op=op,
            left=new_left,
            right=new_right,
        ),
    )


def _binary_result_type(
    op: BinaryOp, left: CelType, right: CelType, pos: SourcePosition | None
) -> CelType:
    if op is BinaryOp.ADD:
        if type(left) is type(right) and isinstance(left, _NUMERIC_TYPES):
            return left
        if isinstance(left, StringType) and isinstance(right, StringType):
            return StringType()
        if isinstance(left, BytesType) and isinstance(right, BytesType):
            return BytesType()
        if isinstance(left, ListType) and isinstance(right, ListType):
            unified = _unify_branch_types(left.element, right.element)
            if unified is None:
                raise TypeCheckError(
                    "list concatenation requires matching element types: "
                    f"left={_render_type(left)}, right={_render_type(right)}",
                    source_position=pos,
                    expected_type=left,
                    actual_type=right,
                )
            return ListType(element=unified)
        raise TypeCheckError(
            f"operator '+' is not defined for {_render_type(left)} + {_render_type(right)}",
            source_position=pos,
            expected_type=left,
            actual_type=right,
        )
    if op in (BinaryOp.SUB, BinaryOp.MUL, BinaryOp.DIV, BinaryOp.MOD):
        if type(left) is type(right) and isinstance(left, _NUMERIC_TYPES):
            return left
        raise TypeCheckError(
            f"operator {op.value!r} requires matching numeric operands, got "
            f"{_render_type(left)} and {_render_type(right)}",
            source_position=pos,
            expected_type="numeric",
            actual_type=left if not isinstance(left, _NUMERIC_TYPES) else right,
        )
    if op in (BinaryOp.LT, BinaryOp.LE, BinaryOp.GT, BinaryOp.GE):
        if type(left) is type(right) and isinstance(left, _COMPARABLE_TYPES):
            return BoolType()
        raise TypeCheckError(
            f"operator {op.value!r} requires matching comparable operands, got "
            f"{_render_type(left)} and {_render_type(right)}",
            source_position=pos,
            expected_type="comparable",
            actual_type=left,
        )
    if op in (BinaryOp.EQ, BinaryOp.NE):
        if type(left) is type(right) or isinstance(left, NullType) or isinstance(right, NullType):
            return BoolType()
        raise TypeCheckError(
            f"operator {op.value!r} requires matching operand types, got "
            f"{_render_type(left)} and {_render_type(right)}",
            source_position=pos,
            expected_type=left,
            actual_type=right,
        )
    if op in (BinaryOp.AND, BinaryOp.OR):
        if isinstance(left, BoolType) and isinstance(right, BoolType):
            return BoolType()
        bad = left if not isinstance(left, BoolType) else right
        raise TypeCheckError(
            f"operator {op.value!r} requires bool operands, got "
            f"{_render_type(left)} and {_render_type(right)}",
            source_position=pos,
            expected_type=BoolType(),
            actual_type=bad,
        )
    if op is BinaryOp.IN:
        if isinstance(right, ListType):
            if type(left) is type(right.element) or isinstance(left, NullType):
                return BoolType()
            raise TypeCheckError(
                f"'in' element type mismatch: "
                f"{_render_type(left)} in list<{_render_type(right.element)}>",
                source_position=pos,
                expected_type=right.element,
                actual_type=left,
            )
        if isinstance(right, MapType):
            if type(left) is type(right.key) or isinstance(left, NullType):
                return BoolType()
            raise TypeCheckError(
                f"'in' key type mismatch: "
                f"{_render_type(left)} in map<{_render_type(right.key)}, ...>",
                source_position=pos,
                expected_type=right.key,
                actual_type=left,
            )
        raise TypeCheckError(
            f"'in' right operand must be list or map, got {_render_type(right)}",
            source_position=pos,
            expected_type="list or map",
            actual_type=right,
        )
    raise TypeCheckError(  # pragma: no cover - exhaustive
        f"unsupported binary operator {op!r}", source_position=pos
    )


def _infer_unary(node: Unary, bindings: SchemaBindings) -> tuple[CelType, _Drill | None, Node]:
    operand_type, _od, new_operand = _infer(node.operand, bindings)
    if node.op is UnaryOp.NOT:
        if not isinstance(operand_type, BoolType):
            raise TypeCheckError(
                "operator '!' requires bool, got " + _render_type(operand_type),
                source_position=node.pos,
                expected_type=BoolType(),
                actual_type=operand_type,
            )
        result: CelType = BoolType()
    else:  # UnaryOp.NEG
        if isinstance(operand_type, (IntType, DoubleType)):
            result = operand_type
        else:
            # Strict CEL disallows negating uint (no negative uint) and
            # any non-numeric type.
            raise TypeCheckError(
                "operator '-' requires int or double operand, got " + _render_type(operand_type),
                source_position=node.pos,
                expected_type="int or double",
                actual_type=operand_type,
            )
    return result, None, Unary(pos=node.pos, cel_type=result, op=node.op, operand=new_operand)


# ---------------------------------------------------------------------------
# Collection literals
# ---------------------------------------------------------------------------


def _infer_list_lit(node: ListLit, bindings: SchemaBindings) -> tuple[CelType, _Drill | None, Node]:
    if not node.elements:
        raise TypeCheckError(
            "empty list literal cannot be type-inferred without a context type",
            source_position=node.pos,
        )
    new_elems: list[Node] = []
    unified: CelType | None = None
    for elem in node.elements:
        elem_type, _ed, new_elem = _infer(elem, bindings)
        new_elems.append(new_elem)
        if unified is None:
            unified = elem_type
        else:
            merged = _unify_branch_types(unified, elem_type)
            if merged is None:
                raise TypeCheckError(
                    "list literal has heterogeneous element types: "
                    f"{_render_type(unified)} vs {_render_type(elem_type)}",
                    source_position=elem.pos,
                    expected_type=unified,
                    actual_type=elem_type,
                )
            unified = merged
    assert unified is not None
    list_type = ListType(element=unified)
    return list_type, None, ListLit(pos=node.pos, cel_type=list_type, elements=tuple(new_elems))


def _infer_map_lit(node: MapLit, bindings: SchemaBindings) -> tuple[CelType, _Drill | None, Node]:
    if not node.entries:
        raise TypeCheckError(
            "empty map literal cannot be type-inferred without a context type",
            source_position=node.pos,
        )
    new_entries: list[tuple[Node, Node]] = []
    key_unified: CelType | None = None
    val_unified: CelType | None = None
    for k_node, v_node in node.entries:
        k_type, _kd, new_k = _infer(k_node, bindings)
        if isinstance(k_type, NullType):
            raise TypeCheckError(
                "map literal keys cannot be null",
                source_position=k_node.pos,
                expected_type="non-null key",
                actual_type=k_type,
            )
        v_type, _vd, new_v = _infer(v_node, bindings)
        new_entries.append((new_k, new_v))
        if key_unified is None:
            key_unified = k_type
        else:
            merged = _unify_branch_types(key_unified, k_type)
            if merged is None:
                raise TypeCheckError(
                    "map literal has heterogeneous key types: "
                    f"{_render_type(key_unified)} vs {_render_type(k_type)}",
                    source_position=k_node.pos,
                    expected_type=key_unified,
                    actual_type=k_type,
                )
            key_unified = merged
        if val_unified is None:
            val_unified = v_type
        else:
            merged_v = _unify_branch_types(val_unified, v_type)
            if merged_v is None:
                raise TypeCheckError(
                    "map literal has heterogeneous value types: "
                    f"{_render_type(val_unified)} vs {_render_type(v_type)}",
                    source_position=v_node.pos,
                    expected_type=val_unified,
                    actual_type=v_type,
                )
            val_unified = merged_v
    assert key_unified is not None
    assert val_unified is not None
    map_type = MapType(key=key_unified, value=val_unified)
    return map_type, None, MapLit(pos=node.pos, cel_type=map_type, entries=tuple(new_entries))


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _render_type(t: CelType) -> str:
    """Short human-readable rendering of a :class:`CelType` for messages."""
    if isinstance(t, ListType):
        return f"list<{_render_type(t.element)}>"
    if isinstance(t, MapType):
        return f"map<{_render_type(t.key)}, {_render_type(t.value)}>"
    return t.TYPE_KIND or type(t).__name__


def _chain_for(node: Node) -> tuple[str, ...]:
    """Best-effort dotted-chain rendering for error messages."""
    parts: list[str] = []
    cur: Node = node
    while True:
        if isinstance(cur, Ident):
            parts.append(cur.name)
            break
        if isinstance(cur, Member):
            parts.append(cur.name)
            cur = cur.target
            continue
        if isinstance(cur, Index):
            if (
                isinstance(cur.index, Literal)
                and cur.index.kind is LiteralKind.STRING
                and isinstance(cur.index.value, str)
            ):
                parts.append(cur.index.value)
            else:
                parts.append("?")
            cur = cur.target
            continue
        break
    return tuple(reversed(parts))

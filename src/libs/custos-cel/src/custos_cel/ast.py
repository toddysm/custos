"""AST and TypedAST data model for ``custos_cel``.

This module defines the small set of node types the parser emits and the
type checker annotates. The same classes are used for both the **untyped**
AST (produced by :func:`custos_cel.parse`) and the **TypedAST** (produced
by :func:`custos_cel.type_check`): every node carries an optional
``cel_type`` attribute that is ``None`` after :func:`parse` and fully
populated after :func:`type_check`.

Design constraints (from change records 003 and 005 and ADR-011):

* Every node carries a :class:`SourcePosition` so downstream error
  reporting can point back into the original CEL source string.
* Every node is JSON-serializable via :meth:`Node.to_dict` /
  :meth:`Node.from_dict`. JSON round-trip is byte-stable when keys are
  serialized in sorted order; see :func:`to_json` / :func:`from_json`.
* Nodes are immutable (``@dataclass(frozen=True, kw_only=True)``) so they
  can be shared across the parser cache, the type checker, and the
  evaluator without defensive copying.
* The data model is deliberately minimal: it covers exactly the CEL
  subset Custos accepts (no macros, no ``dyn``, no extension functions).
  Function-call nodes are surfaced uniformly so the type checker
  (WF-IMPL-005) can reject unknown function names against a whitelist.

The classes live here so that downstream consumers (Workflow Service,
Catalog Service) can import them from ``custos_cel`` without depending
on the chosen parser implementation. The conversion from the parser's
native parse tree happens in :mod:`custos_cel._celpy_convert`.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, ClassVar, Final

__all__ = [
    "AST_SCHEMA_VERSION",
    "Binary",
    "BinaryOp",
    "BoolType",
    "BytesType",
    "Call",
    "CelType",
    "Conditional",
    "DoubleType",
    "Ident",
    "Index",
    "IntType",
    "ListLit",
    "ListType",
    "Literal",
    "LiteralKind",
    "MapLit",
    "MapType",
    "Member",
    "Node",
    "NullType",
    "SourcePosition",
    "StringType",
    "UintType",
    "Unary",
    "UnaryOp",
    "from_dict",
    "from_json",
    "node_from_dict",
    "to_json",
]

#: Version of the on-the-wire JSON shape produced by :meth:`Node.to_dict`.
#: Bump on any breaking change to the serialized form. Used to gate
#: deserialization in :func:`from_dict`.
AST_SCHEMA_VERSION: Final[int] = 1


# ---------------------------------------------------------------------------
# Source position
# ---------------------------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class SourcePosition:
    """Source position attached to every AST node.

    ``line`` and ``column`` are 1-indexed. ``offset`` is a 0-indexed
    character offset from the start of the source string, matching the
    underlying parser position data.

    All three fields are optional because the underlying parser
    (``cel-python``) does not always emit positions for every grammar
    rule (notably bare boolean keywords). Downstream error reporting
    must therefore tolerate missing positions.
    """

    line: int | None = None
    column: int | None = None
    offset: int | None = None

    def to_dict(self) -> dict[str, int | None]:
        return {"column": self.column, "line": self.line, "offset": self.offset}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SourcePosition:
        return cls(
            line=_optional_int(data.get("line")),
            column=_optional_int(data.get("column")),
            offset=_optional_int(data.get("offset")),
        )


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        # bool is a subclass of int in Python; reject explicitly.
        raise TypeError(f"expected int or None for position field, got bool: {value!r}")
    if not isinstance(value, int):
        raise TypeError(f"expected int or None for position field, got {type(value).__name__}")
    return value


# ---------------------------------------------------------------------------
# CEL type system
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CelType:
    """Base class for inferred CEL types attached to TypedAST nodes."""

    TYPE_KIND: ClassVar[str] = ""

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.TYPE_KIND}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CelType:
        kind = data.get("kind")
        if not isinstance(kind, str):
            raise ValueError(f"CelType.from_dict: missing or non-string 'kind' in {data!r}")
        try:
            sub = _CEL_TYPE_REGISTRY[kind]
        except KeyError as exc:
            raise ValueError(f"CelType.from_dict: unknown type kind {kind!r}") from exc
        return sub._from_dict_payload(data)

    @classmethod
    def _from_dict_payload(cls, data: Mapping[str, Any]) -> CelType:
        return cls()


@dataclass(frozen=True)
class IntType(CelType):
    TYPE_KIND: ClassVar[str] = "int"


@dataclass(frozen=True)
class UintType(CelType):
    TYPE_KIND: ClassVar[str] = "uint"


@dataclass(frozen=True)
class DoubleType(CelType):
    TYPE_KIND: ClassVar[str] = "double"


@dataclass(frozen=True)
class BoolType(CelType):
    TYPE_KIND: ClassVar[str] = "bool"


@dataclass(frozen=True)
class StringType(CelType):
    TYPE_KIND: ClassVar[str] = "string"


@dataclass(frozen=True)
class BytesType(CelType):
    TYPE_KIND: ClassVar[str] = "bytes"


@dataclass(frozen=True)
class NullType(CelType):
    TYPE_KIND: ClassVar[str] = "null"


@dataclass(frozen=True)
class ListType(CelType):
    TYPE_KIND: ClassVar[str] = "list"
    element: CelType

    def to_dict(self) -> dict[str, Any]:
        return {"element": self.element.to_dict(), "kind": self.TYPE_KIND}

    @classmethod
    def _from_dict_payload(cls, data: Mapping[str, Any]) -> CelType:
        element_raw = data.get("element")
        if not isinstance(element_raw, Mapping):
            raise ValueError("ListType.from_dict: missing 'element' mapping")
        return cls(element=CelType.from_dict(element_raw))


@dataclass(frozen=True)
class MapType(CelType):
    TYPE_KIND: ClassVar[str] = "map"
    key: CelType
    value: CelType

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key.to_dict(),
            "kind": self.TYPE_KIND,
            "value": self.value.to_dict(),
        }

    @classmethod
    def _from_dict_payload(cls, data: Mapping[str, Any]) -> CelType:
        key_raw = data.get("key")
        value_raw = data.get("value")
        if not isinstance(key_raw, Mapping):
            raise ValueError("MapType.from_dict: missing 'key' mapping")
        if not isinstance(value_raw, Mapping):
            raise ValueError("MapType.from_dict: missing 'value' mapping")
        return cls(key=CelType.from_dict(key_raw), value=CelType.from_dict(value_raw))


_CEL_TYPE_REGISTRY: dict[str, type[CelType]] = {
    IntType.TYPE_KIND: IntType,
    UintType.TYPE_KIND: UintType,
    DoubleType.TYPE_KIND: DoubleType,
    BoolType.TYPE_KIND: BoolType,
    StringType.TYPE_KIND: StringType,
    BytesType.TYPE_KIND: BytesType,
    NullType.TYPE_KIND: NullType,
    ListType.TYPE_KIND: ListType,
    MapType.TYPE_KIND: MapType,
}


# ---------------------------------------------------------------------------
# Enums for literal / operator discriminators
# ---------------------------------------------------------------------------


class LiteralKind(StrEnum):
    INT = "int"
    UINT = "uint"
    DOUBLE = "double"
    BOOL = "bool"
    STRING = "string"
    BYTES = "bytes"
    NULL = "null"


class BinaryOp(StrEnum):
    ADD = "+"
    SUB = "-"
    MUL = "*"
    DIV = "/"
    MOD = "%"
    EQ = "=="
    NE = "!="
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="
    AND = "&&"
    OR = "||"
    IN = "in"


class UnaryOp(StrEnum):
    NEG = "-"
    NOT = "!"


# ---------------------------------------------------------------------------
# Node hierarchy
# ---------------------------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class Node:
    """Base class for all AST nodes.

    ``cel_type`` is populated by the type checker (WF-IMPL-005). When
    ``None``, the tree is an untyped :data:`custos_cel.AST`; when every
    node in the tree has it set, the tree is a
    :data:`custos_cel.TypedAST`. The same Python class therefore
    represents both stages.
    """

    NODE_KIND: ClassVar[str] = ""

    pos: SourcePosition
    cel_type: CelType | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = self._payload_to_dict()
        out["node"] = self.NODE_KIND
        out["pos"] = self.pos.to_dict()
        if self.cel_type is not None:
            out["cel_type"] = self.cel_type.to_dict()
        return out

    def _payload_to_dict(self) -> dict[str, Any]:
        """Subclass-specific fields. Override in each concrete node."""
        return {}

    @classmethod
    def _from_dict_payload(
        cls,
        data: Mapping[str, Any],
        pos: SourcePosition,
        cel_type: CelType | None,
    ) -> Node:
        raise NotImplementedError


@dataclass(frozen=True, kw_only=True)
class Literal(Node):
    NODE_KIND: ClassVar[str] = "Literal"

    kind: LiteralKind
    value: int | float | bool | str | bytes | None

    def _payload_to_dict(self) -> dict[str, Any]:
        if self.kind is LiteralKind.BYTES:
            assert isinstance(self.value, bytes)
            return {"kind": self.kind.value, "value": _bytes_to_str(self.value)}
        return {"kind": self.kind.value, "value": self.value}

    @classmethod
    def _from_dict_payload(
        cls, data: Mapping[str, Any], pos: SourcePosition, cel_type: CelType | None
    ) -> Literal:
        kind = LiteralKind(data["kind"])
        raw = data.get("value")
        value: int | float | bool | str | bytes | None
        if kind is LiteralKind.NULL:
            value = None
        elif kind is LiteralKind.BYTES:
            if not isinstance(raw, str):
                raise ValueError("Literal.from_dict: bytes literal value must be a hex string")
            value = _str_to_bytes(raw)
        elif kind is LiteralKind.BOOL:
            if not isinstance(raw, bool):
                raise ValueError("Literal.from_dict: bool literal value must be a JSON bool")
            value = raw
        elif kind is LiteralKind.STRING:
            if not isinstance(raw, str):
                raise ValueError("Literal.from_dict: string literal value must be a JSON string")
            value = raw
        elif kind in (LiteralKind.INT, LiteralKind.UINT):
            if isinstance(raw, bool) or not isinstance(raw, int):
                raise ValueError(
                    f"Literal.from_dict: {kind.value} literal value must be a JSON int"
                )
            value = raw
        elif kind is LiteralKind.DOUBLE:
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                raise ValueError("Literal.from_dict: double literal value must be a JSON number")
            value = float(raw)
        else:  # pragma: no cover - exhaustive
            raise ValueError(f"Literal.from_dict: unhandled literal kind {kind!r}")
        return cls(pos=pos, cel_type=cel_type, kind=kind, value=value)


def _bytes_to_str(b: bytes) -> str:
    return b.hex()


def _str_to_bytes(s: str) -> bytes:
    try:
        return bytes.fromhex(s)
    except ValueError as exc:
        raise ValueError(f"bytes literal must be a hex string, got {s!r}") from exc


@dataclass(frozen=True, kw_only=True)
class Ident(Node):
    NODE_KIND: ClassVar[str] = "Ident"

    name: str

    def _payload_to_dict(self) -> dict[str, Any]:
        return {"name": self.name}

    @classmethod
    def _from_dict_payload(
        cls, data: Mapping[str, Any], pos: SourcePosition, cel_type: CelType | None
    ) -> Ident:
        name = data["name"]
        if not isinstance(name, str):
            raise ValueError("Ident.from_dict: 'name' must be a string")
        return cls(pos=pos, cel_type=cel_type, name=name)


@dataclass(frozen=True, kw_only=True)
class Member(Node):
    """Dot-style member access: ``target.name``."""

    NODE_KIND: ClassVar[str] = "Member"

    target: Node
    name: str

    def _payload_to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "target": self.target.to_dict()}

    @classmethod
    def _from_dict_payload(
        cls, data: Mapping[str, Any], pos: SourcePosition, cel_type: CelType | None
    ) -> Member:
        name = data["name"]
        if not isinstance(name, str):
            raise ValueError("Member.from_dict: 'name' must be a string")
        return cls(
            pos=pos,
            cel_type=cel_type,
            target=node_from_dict(data["target"]),
            name=name,
        )


@dataclass(frozen=True, kw_only=True)
class Index(Node):
    """Bracket-style index access: ``target[index]``."""

    NODE_KIND: ClassVar[str] = "Index"

    target: Node
    index: Node

    def _payload_to_dict(self) -> dict[str, Any]:
        return {"index": self.index.to_dict(), "target": self.target.to_dict()}

    @classmethod
    def _from_dict_payload(
        cls, data: Mapping[str, Any], pos: SourcePosition, cel_type: CelType | None
    ) -> Index:
        return cls(
            pos=pos,
            cel_type=cel_type,
            target=node_from_dict(data["target"]),
            index=node_from_dict(data["index"]),
        )


@dataclass(frozen=True, kw_only=True)
class Call(Node):
    """Function call: ``function(args...)``.

    Custos's CEL subset disallows method calls (``a.b(c)``) and macros,
    so ``function`` is always a bare identifier — never an expression.
    The whitelist of allowed function names is enforced by the type
    checker (WF-IMPL-005), not at the AST level.
    """

    NODE_KIND: ClassVar[str] = "Call"

    function: str
    args: tuple[Node, ...] = field(default_factory=tuple)

    def _payload_to_dict(self) -> dict[str, Any]:
        return {"args": [a.to_dict() for a in self.args], "function": self.function}

    @classmethod
    def _from_dict_payload(
        cls, data: Mapping[str, Any], pos: SourcePosition, cel_type: CelType | None
    ) -> Call:
        function = data["function"]
        if not isinstance(function, str):
            raise ValueError("Call.from_dict: 'function' must be a string")
        args_raw = data.get("args", [])
        if not isinstance(args_raw, Sequence) or isinstance(args_raw, (str, bytes)):
            raise ValueError("Call.from_dict: 'args' must be a list")
        return cls(
            pos=pos,
            cel_type=cel_type,
            function=function,
            args=tuple(node_from_dict(a) for a in args_raw),
        )


@dataclass(frozen=True, kw_only=True)
class Conditional(Node):
    """Ternary: ``cond ? then_branch : else_branch``."""

    NODE_KIND: ClassVar[str] = "Conditional"

    cond: Node
    then_branch: Node
    else_branch: Node

    def _payload_to_dict(self) -> dict[str, Any]:
        return {
            "cond": self.cond.to_dict(),
            "else_branch": self.else_branch.to_dict(),
            "then_branch": self.then_branch.to_dict(),
        }

    @classmethod
    def _from_dict_payload(
        cls, data: Mapping[str, Any], pos: SourcePosition, cel_type: CelType | None
    ) -> Conditional:
        return cls(
            pos=pos,
            cel_type=cel_type,
            cond=node_from_dict(data["cond"]),
            then_branch=node_from_dict(data["then_branch"]),
            else_branch=node_from_dict(data["else_branch"]),
        )


@dataclass(frozen=True, kw_only=True)
class Binary(Node):
    NODE_KIND: ClassVar[str] = "Binary"

    op: BinaryOp
    left: Node
    right: Node

    def _payload_to_dict(self) -> dict[str, Any]:
        return {
            "left": self.left.to_dict(),
            "op": self.op.value,
            "right": self.right.to_dict(),
        }

    @classmethod
    def _from_dict_payload(
        cls, data: Mapping[str, Any], pos: SourcePosition, cel_type: CelType | None
    ) -> Binary:
        return cls(
            pos=pos,
            cel_type=cel_type,
            op=BinaryOp(data["op"]),
            left=node_from_dict(data["left"]),
            right=node_from_dict(data["right"]),
        )


@dataclass(frozen=True, kw_only=True)
class Unary(Node):
    NODE_KIND: ClassVar[str] = "Unary"

    op: UnaryOp
    operand: Node

    def _payload_to_dict(self) -> dict[str, Any]:
        return {"op": self.op.value, "operand": self.operand.to_dict()}

    @classmethod
    def _from_dict_payload(
        cls, data: Mapping[str, Any], pos: SourcePosition, cel_type: CelType | None
    ) -> Unary:
        return cls(
            pos=pos,
            cel_type=cel_type,
            op=UnaryOp(data["op"]),
            operand=node_from_dict(data["operand"]),
        )


@dataclass(frozen=True, kw_only=True)
class ListLit(Node):
    NODE_KIND: ClassVar[str] = "ListLit"

    elements: tuple[Node, ...] = field(default_factory=tuple)

    def _payload_to_dict(self) -> dict[str, Any]:
        return {"elements": [e.to_dict() for e in self.elements]}

    @classmethod
    def _from_dict_payload(
        cls, data: Mapping[str, Any], pos: SourcePosition, cel_type: CelType | None
    ) -> ListLit:
        elements_raw = data.get("elements", [])
        if not isinstance(elements_raw, Sequence) or isinstance(elements_raw, (str, bytes)):
            raise ValueError("ListLit.from_dict: 'elements' must be a list")
        return cls(
            pos=pos,
            cel_type=cel_type,
            elements=tuple(node_from_dict(e) for e in elements_raw),
        )


@dataclass(frozen=True, kw_only=True)
class MapLit(Node):
    NODE_KIND: ClassVar[str] = "MapLit"

    #: Ordered key/value pairs as they appear in source.
    entries: tuple[tuple[Node, Node], ...] = field(default_factory=tuple)

    def _payload_to_dict(self) -> dict[str, Any]:
        return {"entries": [[k.to_dict(), v.to_dict()] for k, v in self.entries]}

    @classmethod
    def _from_dict_payload(
        cls, data: Mapping[str, Any], pos: SourcePosition, cel_type: CelType | None
    ) -> MapLit:
        entries_raw = data.get("entries", [])
        if not isinstance(entries_raw, Sequence) or isinstance(entries_raw, (str, bytes)):
            raise ValueError("MapLit.from_dict: 'entries' must be a list of [k, v] pairs")
        entries: list[tuple[Node, Node]] = []
        for pair in entries_raw:
            if not isinstance(pair, Sequence) or isinstance(pair, (str, bytes)) or len(pair) != 2:
                raise ValueError("MapLit.from_dict: each entry must be a [k, v] pair")
            entries.append((node_from_dict(pair[0]), node_from_dict(pair[1])))
        return cls(pos=pos, cel_type=cel_type, entries=tuple(entries))


_NODE_REGISTRY: dict[str, type[Node]] = {
    Literal.NODE_KIND: Literal,
    Ident.NODE_KIND: Ident,
    Member.NODE_KIND: Member,
    Index.NODE_KIND: Index,
    Call.NODE_KIND: Call,
    Conditional.NODE_KIND: Conditional,
    Binary.NODE_KIND: Binary,
    Unary.NODE_KIND: Unary,
    ListLit.NODE_KIND: ListLit,
    MapLit.NODE_KIND: MapLit,
}


# ---------------------------------------------------------------------------
# Top-level serialization helpers
# ---------------------------------------------------------------------------


def node_from_dict(data: Mapping[str, Any]) -> Node:
    """Reconstruct a :class:`Node` from its :meth:`Node.to_dict` form.

    Use :func:`from_dict` for the top-level envelope that also carries
    the schema version.
    """
    if not isinstance(data, Mapping):
        raise TypeError(f"node_from_dict: expected mapping, got {type(data).__name__}")
    kind = data.get("node")
    if not isinstance(kind, str):
        raise ValueError(f"node_from_dict: missing or non-string 'node' in {data!r}")
    try:
        cls = _NODE_REGISTRY[kind]
    except KeyError as exc:
        raise ValueError(f"node_from_dict: unknown node kind {kind!r}") from exc
    pos_raw = data.get("pos", {})
    if not isinstance(pos_raw, Mapping):
        raise ValueError("node_from_dict: 'pos' must be a mapping")
    pos = SourcePosition.from_dict(pos_raw)
    if "cel_type" in data:
        cel_type_raw = data["cel_type"]
        if not isinstance(cel_type_raw, Mapping):
            raise ValueError("node_from_dict: 'cel_type' must be a mapping")
        cel_type = CelType.from_dict(cel_type_raw)
    else:
        cel_type = None
    return cls._from_dict_payload(data, pos, cel_type)


def from_dict(envelope: Mapping[str, Any]) -> Node:
    """Inverse of :func:`to_dict_envelope`.

    Validates the schema version and returns the root node.
    """
    if not isinstance(envelope, Mapping):
        raise TypeError(f"from_dict: expected mapping, got {type(envelope).__name__}")
    version = envelope.get("schema_version")
    if version != AST_SCHEMA_VERSION:
        raise ValueError(
            f"from_dict: unsupported schema version {version!r}; "
            f"this build understands version {AST_SCHEMA_VERSION}"
        )
    root = envelope.get("root")
    if not isinstance(root, Mapping):
        raise ValueError("from_dict: missing 'root' mapping")
    return node_from_dict(root)


def to_dict_envelope(root: Node) -> dict[str, Any]:
    """Wrap a node tree in the versioned on-the-wire envelope."""
    return {"root": root.to_dict(), "schema_version": AST_SCHEMA_VERSION}


def to_json(root: Node) -> str:
    """Serialize a node tree to canonical (byte-stable) JSON.

    Keys are emitted in sorted order and separators are minimized so
    that two invocations on equal trees yield byte-identical output.
    This is the property :class:`Run.compiledGraph` relies on for cache
    keys and equality checks (per bundle-h change record 003).
    """
    import json

    return json.dumps(to_dict_envelope(root), sort_keys=True, separators=(",", ":"))


def from_json(text: str) -> Node:
    """Inverse of :func:`to_json`."""
    import json

    return from_dict(json.loads(text))

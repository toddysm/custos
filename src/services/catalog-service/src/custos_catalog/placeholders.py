"""Placeholder Schema Validator (CS-IMPL-012).

WorkflowTemplate documents declare typed placeholder slots under
``spec.placeholders[]`` per ADR-009. The JSON Schema validator
(:func:`custos_catalog.schema.validate.validate_template`) already
enforces the structural shape of each declaration — name pattern,
required ``type`` keyword, ``connectorType`` / ``activityType``
cross-fields. This module adds the cross-declaration well-formedness
checks the schema cannot express:

* duplicate placeholder names are rejected,
* default values (when present and not a ``${{ ... }}`` expression)
  must be type-compatible with the declared placeholder type.

It also implements binding-time validation used by the
:meth:`TemplateManager.materialize` operation (CS-IMPL-013): given a
list of :class:`PlaceholderDeclaration` and a mapping of supplied
bindings, the validator confirms every required placeholder has a
value (either an explicit binding or a default) and that each
supplied value is type-compatible with the declaration.

The validator never evaluates expressions. A default value that is a
``${{ ... }}`` string is treated as opaque (CEL evaluation runs only
at workflow execution time); type compatibility is only enforced for
literal values. The same rule applies to bindings supplied by a
caller: a binding that is itself a ``${{ ... }}`` expression is
passed through unchanged.

Types
-----

The seven placeholder types come from
:data:`custos_catalog.schema.template.TEMPLATE_SCHEMA`:

================  =====================================================
``connectorRef``  Resolves to a connector instance reference. Binding
                  must be a string; the declaration's ``connectorType``
                  pins the expected connector type at materialize time.
``activityRef``   Resolves to an activity-type reference. Binding must
                  be a string; the declaration's ``activityType`` pins
                  the activity family.
``string``        Free-form string.
``integer``       JSON integer (``int`` in Python; ``bool`` excluded).
``number``        JSON number (``int`` or ``float``; ``bool`` excluded).
``boolean``       JSON boolean.
``json``          Arbitrary JSON-compatible value.
================  =====================================================
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final, Literal, get_args

PlaceholderType = Literal[
    "connectorRef",
    "activityRef",
    "string",
    "integer",
    "number",
    "boolean",
    "json",
]

#: All known placeholder types. Used to validate the ``type`` field
#: when callers construct declarations programmatically (the schema
#: validator catches the same condition for documents loaded from
#: source).
PLACEHOLDER_TYPES: Final[frozenset[str]] = frozenset(get_args(PlaceholderType))


_EXPRESSION_PREFIX: Final[str] = "${{"


def _is_expression(value: object) -> bool:
    """Return ``True`` if ``value`` looks like a ``${{ ... }}`` expression.

    Expression-bound defaults and bindings are passed through opaque
    — CEL evaluation lives at workflow execution time, not at publish
    or materialize time, so type compatibility cannot be enforced
    against the resolved value here.
    """
    return isinstance(value, str) and value.strip().startswith(_EXPRESSION_PREFIX)


@dataclass(frozen=True, slots=True)
class PlaceholderDeclaration:
    """A single declared placeholder slot on a WorkflowTemplate.

    Mirrors :data:`custos_catalog.schema.template.TEMPLATE_SCHEMA`
    ``spec.placeholders[]`` items, with the following defaults applied:

    * ``required`` defaults to ``True`` when omitted (matching the
      schema's behaviour of treating the field as optional but
      semantically required-by-default).

    Attributes:
        name: The placeholder identifier (e.g. ``registryConnector``).
            Must match the schema's name pattern; the schema gate is
            the canonical enforcement point.
        type: One of :data:`PlaceholderType`.
        required: When ``True`` (the default), bindings must supply a
            value or the declaration must carry a ``default``.
        default: Optional default value applied when the binding does
            not supply one. May be a literal or a ``${{ ... }}``
            expression; literals are type-checked against ``type``.
        description: Free-form description for documentation surfaces.
        connector_type: Pins the expected connector type for
            ``connectorRef`` placeholders. ``None`` for other types.
        activity_type: Pins the expected activity family for
            ``activityRef`` placeholders. ``None`` for other types.
    """

    name: str
    type: PlaceholderType
    required: bool = True
    default: Any = None
    description: str | None = None
    connector_type: str | None = None
    activity_type: str | None = None

    @property
    def has_default(self) -> bool:
        """Return ``True`` if the declaration carries an explicit default.

        ``None`` is not a valid placeholder default (every JSON type
        we support has its own non-``None`` zero value, and the schema
        gates ``default`` to non-null), so we can use it as the
        "absent" sentinel.
        """
        return self.default is not None


class PlaceholderError(ValueError):
    """Base class for placeholder declaration / binding failures.

    Mirrors the publish-pipeline pattern of carrying a stable ``code``
    plus an :attr:`issues` list of field-level violations so the API
    surface (CS-IMPL-017) can return the full set in one response.
    """

    code: str = "catalog.placeholder_validation_failed"

    def __init__(self, issues: Sequence[PlaceholderIssue]) -> None:
        self.issues = list(issues)
        rendered = "; ".join(
            f"{issue.path or '<root>'} -> {issue.message}" for issue in self.issues
        )
        super().__init__(f"{len(self.issues)} placeholder issue(s): {rendered}")


class PlaceholderDeclarationError(PlaceholderError):
    """Raised when ``placeholders[]`` are not well-formed across declarations."""

    code: str = "catalog.placeholder_declaration_invalid"


class PlaceholderBindingError(PlaceholderError):
    """Raised when supplied bindings do not satisfy the declared placeholders."""

    code: str = "catalog.placeholder_binding_invalid"


@dataclass(frozen=True, slots=True)
class PlaceholderIssue:
    """One placeholder-validation issue.

    Attributes:
        path: Dotted path to the offending element, rooted at
            ``placeholders[<i>]`` for declaration issues or
            ``bindings.<name>`` for binding issues.
        code: A stable machine-readable code (e.g. ``"duplicate_name"``,
            ``"default_type_mismatch"``, ``"required_binding_missing"``,
            ``"binding_type_mismatch"``, ``"unknown_placeholder"``).
        message: Human-readable explanation.
    """

    path: str
    code: str
    message: str


def parse_declarations(raw: Sequence[Mapping[str, Any]]) -> list[PlaceholderDeclaration]:
    """Convert raw schema-validated placeholder dicts to dataclasses.

    Assumes the input has already passed
    :func:`custos_catalog.schema.validate.validate_template`; missing
    fields default per :class:`PlaceholderDeclaration`. Unknown
    placeholder types are accepted (the schema gate is canonical)
    but downstream binding checks will refuse them.
    """
    out: list[PlaceholderDeclaration] = []
    for item in raw:
        out.append(
            PlaceholderDeclaration(
                name=str(item["name"]),
                type=item["type"],
                required=bool(item.get("required", True)),
                default=item.get("default"),
                description=item.get("description"),
                connector_type=item.get("connectorType"),
                activity_type=item.get("activityType"),
            ),
        )
    return out


def validate_placeholder_declarations(
    decls: Sequence[PlaceholderDeclaration],
) -> None:
    """Run cross-declaration well-formedness checks.

    Specifically:

    * No two declarations may share the same ``name``.
    * Each declaration's ``type`` must be one of
      :data:`PLACEHOLDER_TYPES`.
    * Literal default values must be type-compatible with the declared
      type. Expression-form defaults (``${{ ... }}``) are passed
      through.

    Collects every violation in one pass and raises
    :class:`PlaceholderDeclarationError` if any are found.
    """
    issues: list[PlaceholderIssue] = []
    seen: set[str] = set()
    for idx, decl in enumerate(decls):
        path = f"placeholders[{idx}]"
        if decl.name in seen:
            issues.append(
                PlaceholderIssue(
                    path=f"{path}.name",
                    code="duplicate_name",
                    message=f"placeholder name {decl.name!r} is declared more than once",
                ),
            )
        else:
            seen.add(decl.name)
        if decl.type not in PLACEHOLDER_TYPES:
            issues.append(
                PlaceholderIssue(
                    path=f"{path}.type",
                    code="unknown_type",
                    message=(
                        f"placeholder type {decl.type!r} is not one of {sorted(PLACEHOLDER_TYPES)}"
                    ),
                ),
            )
            # Skip default type-check if the type is unknown.
            continue
        if decl.has_default and not _is_expression(decl.default):
            err = _check_value_type(decl.default, decl.type)
            if err is not None:
                issues.append(
                    PlaceholderIssue(
                        path=f"{path}.default",
                        code="default_type_mismatch",
                        message=err,
                    ),
                )
    if issues:
        raise PlaceholderDeclarationError(issues)


def validate_placeholder_bindings(
    decls: Sequence[PlaceholderDeclaration],
    bindings: Mapping[str, Any],
) -> None:
    """Verify a binding mapping satisfies the declared placeholders.

    For each declaration, check that:

    * if ``required`` and no default is set, a binding is present;
    * the supplied binding (literal only — expression-form passes
      through) is type-compatible with the declaration.

    Bindings naming an unknown placeholder are rejected. Collects all
    violations in one pass.

    Raises:
        PlaceholderBindingError: When at least one declaration is
            unsatisfied or one binding references an unknown
            placeholder.
    """
    issues: list[PlaceholderIssue] = []
    decl_by_name = {d.name: d for d in decls}

    for decl in decls:
        bound = decl.name in bindings
        if not bound:
            if decl.required and not decl.has_default:
                issues.append(
                    PlaceholderIssue(
                        path=f"bindings.{decl.name}",
                        code="required_binding_missing",
                        message=(f"placeholder {decl.name!r} is required and has no default"),
                    ),
                )
            continue
        value = bindings[decl.name]
        if _is_expression(value):
            continue
        err = _check_value_type(value, decl.type)
        if err is not None:
            issues.append(
                PlaceholderIssue(
                    path=f"bindings.{decl.name}",
                    code="binding_type_mismatch",
                    message=err,
                ),
            )

    for name in bindings:
        if name not in decl_by_name:
            issues.append(
                PlaceholderIssue(
                    path=f"bindings.{name}",
                    code="unknown_placeholder",
                    message=f"binding {name!r} does not match any declared placeholder",
                ),
            )

    if issues:
        raise PlaceholderBindingError(issues)


def effective_bindings(
    decls: Sequence[PlaceholderDeclaration],
    bindings: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the binding mapping with declaration defaults applied.

    Assumes :func:`validate_placeholder_bindings` has already passed
    — unknown bindings are silently dropped, and missing required
    bindings without a default would have already raised.
    """
    out: dict[str, Any] = {}
    for decl in decls:
        if decl.name in bindings:
            out[decl.name] = bindings[decl.name]
        elif decl.has_default:
            out[decl.name] = decl.default
    return out


def _check_value_type(value: Any, declared: PlaceholderType) -> str | None:
    """Return ``None`` if ``value`` is type-compatible with ``declared``, else an error message.

    Compatibility rules:

    * ``connectorRef`` / ``activityRef`` — non-empty ``str``. Deeper
      validation (resolver lookup) runs at publish time on the
      materialized workflow, not here.
    * ``string`` — ``str``.
    * ``integer`` — ``int`` excluding ``bool``.
    * ``number`` — ``int`` or ``float`` excluding ``bool``.
    * ``boolean`` — ``bool``.
    * ``json`` — anything JSON-compatible (``dict``, ``list``, ``str``,
      ``int``, ``float``, ``bool``, or ``None``). The schema gate has
      already enforced JSON-compatibility on the document so we only
      reject obviously-non-JSON Python objects here.
    """
    if declared in {"connectorRef", "activityRef"}:
        if not isinstance(value, str):
            return f"expected string for {declared}, got {type(value).__name__}"
        if not value:
            return f"expected non-empty string for {declared}"
        return None
    if declared == "string":
        if not isinstance(value, str):
            return f"expected string, got {type(value).__name__}"
        return None
    if declared == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            return f"expected integer, got {type(value).__name__}"
        return None
    if declared == "number":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return f"expected number, got {type(value).__name__}"
        return None
    if declared == "boolean":
        if not isinstance(value, bool):
            return f"expected boolean, got {type(value).__name__}"
        return None
    if declared == "json":
        return _check_json_value(value)
    return f"unknown placeholder type {declared!r}"  # pragma: no cover


def _check_json_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return None
    if isinstance(value, list):
        for idx, item in enumerate(value):
            err = _check_json_value(item)
            if err is not None:
                return f"list item {idx}: {err}"
        return None
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                return f"dict key {key!r}: must be string"
            err = _check_json_value(item)
            if err is not None:
                return f"dict key {key!r}: {err}"
        return None
    return f"value of type {type(value).__name__} is not JSON-compatible"


__all__ = [
    "PLACEHOLDER_TYPES",
    "PlaceholderBindingError",
    "PlaceholderDeclaration",
    "PlaceholderDeclarationError",
    "PlaceholderError",
    "PlaceholderIssue",
    "PlaceholderType",
    "effective_bindings",
    "parse_declarations",
    "validate_placeholder_bindings",
    "validate_placeholder_declarations",
]

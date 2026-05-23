"""Workflow / template document normalizer + canonical hash (CS-IMPL-006).

The normalizer is the second step in the publish-time pipeline:

    raw doc  ->  schema validation (CS-IMPL-005)
             ->  normalize  (this module)
             ->  CEL validation (CS-IMPL-007)
             ->  reference resolution (CS-IMPL-008)
             ->  WorkflowVersion.document is the resolved canonical form

Per ``design/components/catalog-service/design.md`` § Data Models,
``WorkflowVersion.document`` is the **normalized JSON** form: keys
sorted at every level, fully-qualified references, digest-pinned
activity references. This module owns the first two of those —
canonical ordering and slot discovery. Reference filling is done by
CS-IMPL-008's :func:`custos_catalog.resolve.apply_resolutions`, which
consumes the :class:`RefResolutionSlot` tuple emitted here.

The normalizer does **not** rewrite reference strings; it preserves
author-supplied refs verbatim and emits side-band slots that name the
positions in the document tree where the resolver should write. This
keeps the normalizer pure (no I/O, no registry lookups) and makes the
resolver step independently testable.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Literal

#: A `${{ ... }}` interpolation marker. The normalizer never emits a
#: slot for an expression-typed reference — the resolver only fills
#: positions that hold concrete reference strings. Expression-bound
#: refs are resolved at run-time (Workflow Service) and, for templates,
#: at materialization time.
_CEL_PREFIX = "${{"
_CEL_SUFFIX = "}}"


def _is_expression(value: object) -> bool:
    """Return True iff ``value`` is a `${{ ... }}` interpolation token."""
    return (
        isinstance(value, str)
        and value.lstrip().startswith(_CEL_PREFIX)
        and value.rstrip().endswith(_CEL_SUFFIX)
    )


SlotKind = Literal["activity", "subworkflow", "connector_instance"]


@dataclass(frozen=True, slots=True)
class RefResolutionSlot:
    """A reference position discovered by the normalizer.

    Slots are emitted side-band; the normalizer does NOT modify the
    document body. CS-IMPL-008's
    :func:`custos_catalog.resolve.apply_resolutions` walks the slot
    tuple, queries the appropriate registry/store, and produces a new
    :class:`NormalizedWorkflow` whose document has the fully-qualified
    reference substituted at :attr:`path`.

    Attributes:
        kind: Discriminator (``activity`` / ``subworkflow`` /
            ``connector_instance``) that picks which registry the
            resolver should consult.
        path: Sequence of dict keys (``str``) and list indices
            (``int``) navigating from the document root to the
            reference string.
        original_ref: The exact author-supplied reference string,
            preserved verbatim for audit / error reporting.
    """

    kind: SlotKind
    path: tuple[str | int, ...]
    original_ref: str


@dataclass(frozen=True, slots=True)
class NormalizedWorkflow:
    """The canonical form of a Workflow document plus discovered slots."""

    document: dict[str, Any]
    slots: tuple[RefResolutionSlot, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class NormalizedTemplate:
    """The canonical form of a WorkflowTemplate document plus slots.

    Templates typically carry few or no slots because their activity
    and connector references are placeholder-bound (``${{ placeholders.
    foo }}``) and only become resolvable at materialization time. Any
    *concrete* references that appear inside the inner ``spec.workflow``
    block still emit slots so the materialized workflow can be
    re-resolved against the live catalog at materialize-and-publish.
    """

    document: dict[str, Any]
    slots: tuple[RefResolutionSlot, ...] = field(default_factory=tuple)


# ---------------------------------------------------------------------------
# Canonicalization (key ordering)
# ---------------------------------------------------------------------------


def _sort_key(key: Any) -> tuple[str, str]:
    """Return a total-ordering sort key for a dict key of any type.

    Plain ``sorted(dict.keys())`` raises :class:`TypeError` when a
    mapping mixes key types (the canonical case being YAML documents
    with both string and integer keys, e.g. ``{1: "x", "a": "y"}``).
    Mixed-key documents cannot pass the JSON Schema gate, but per the
    publish-time pipeline contract the normalizer must stay total —
    its output is fed to the CEL validator and resolver, which surface
    structured errors. Allowing :func:`sorted` to explode here would
    short-circuit those gates with an opaque ``TypeError``.

    Sorting by ``(type_name, str(key))`` gives a deterministic total
    order across heterogeneous key types while preserving the natural
    str-against-str ordering used by every well-formed document.
    """
    return (type(key).__name__, str(key))


def _canonicalize(node: Any) -> Any:
    """Recursively sort dict keys at every level.

    Lists keep their order (the workflow step order is semantically
    significant; arbitrary sorting would change execution behaviour).
    Scalars pass through unchanged. Mixed-type dict keys are tolerated
    via :func:`_sort_key` so the normalizer remains total even on
    malformed input (the schema gate is the canonical place to reject
    such documents).
    """
    if isinstance(node, dict):
        return {key: _canonicalize(node[key]) for key in sorted(node, key=_sort_key)}
    if isinstance(node, list):
        return [_canonicalize(item) for item in node]
    return node


def canonical_json(doc: dict[str, Any]) -> str:
    """Render ``doc`` as canonical JSON (sorted keys, tight separators).

    The output is the byte-stable representation hashed by
    :func:`canonical_hash` and stored on ``WorkflowVersion.document``
    after the resolver step substitutes resolved reference strings.

    Pre-canonicalizes via :func:`_canonicalize` rather than relying on
    ``json.dumps(sort_keys=True)`` so heterogeneous-keyed documents
    (rejected by the schema gate but still occasionally handed to this
    function in error paths) produce a deterministic byte string
    instead of raising :class:`TypeError`.
    """
    return json.dumps(
        _canonicalize(doc),
        separators=(",", ":"),
        ensure_ascii=False,
    )


def canonical_hash(doc: dict[str, Any]) -> str:
    """Return the SHA-256 hex digest of the canonical JSON of ``doc``.

    Used for content-addressed identity (CS-IMPL-014 template
    round-trip equality) and to detect accidental document drift in
    audit logs.
    """
    encoded = canonical_json(doc).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


# ---------------------------------------------------------------------------
# Slot discovery
# ---------------------------------------------------------------------------


def _discover_workflow_slots(
    spec: dict[str, Any],
    *,
    base_path: tuple[str | int, ...],
) -> list[RefResolutionSlot]:
    """Walk a workflow ``spec`` and emit one slot per concrete reference.

    Concrete = a literal string. ``${{ ... }}`` expressions are NOT
    slots — they're resolved at runtime by the Workflow Service.
    """
    slots: list[RefResolutionSlot] = []

    # Triggers carry a connector reference per trigger.
    for trig_idx, trigger in enumerate(spec.get("triggers", []) or []):
        if not isinstance(trigger, dict):
            continue
        connector = trigger.get("connector")
        if isinstance(connector, str) and not _is_expression(connector):
            slots.append(
                RefResolutionSlot(
                    kind="connector_instance",
                    path=(*base_path, "triggers", trig_idx, "connector"),
                    original_ref=connector,
                ),
            )

    # Steps: activity / workflow / connector / connectors.
    for step_idx, step in enumerate(spec.get("steps", []) or []):
        if not isinstance(step, dict):
            continue
        step_path = (*base_path, "steps", step_idx)

        if isinstance(step.get("activity"), str) and not _is_expression(step["activity"]):
            slots.append(
                RefResolutionSlot(
                    kind="activity",
                    path=(*step_path, "activity"),
                    original_ref=step["activity"],
                ),
            )

        if isinstance(step.get("workflow"), str) and not _is_expression(step["workflow"]):
            slots.append(
                RefResolutionSlot(
                    kind="subworkflow",
                    path=(*step_path, "workflow"),
                    original_ref=step["workflow"],
                ),
            )

        connector = step.get("connector")
        if isinstance(connector, str) and not _is_expression(connector):
            slots.append(
                RefResolutionSlot(
                    kind="connector_instance",
                    path=(*step_path, "connector"),
                    original_ref=connector,
                ),
            )

        connectors = step.get("connectors")
        if isinstance(connectors, dict):
            # Iterate by sorted key for deterministic slot ordering.
            for alias in sorted(connectors.keys()):
                value = connectors[alias]
                if isinstance(value, str) and not _is_expression(value):
                    slots.append(
                        RefResolutionSlot(
                            kind="connector_instance",
                            path=(*step_path, "connectors", alias),
                            original_ref=value,
                        ),
                    )

    return slots


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def normalize_workflow(doc: dict[str, Any]) -> NormalizedWorkflow:
    """Normalize ``doc`` and discover the reference slots it carries.

    The caller is expected to have run :func:`validate_workflow`
    first; passing an unvalidated document is supported (the
    normalizer never raises on shape problems) but downstream behaviour
    is undefined.
    """
    canonical = _canonicalize(doc)
    spec = canonical.get("spec") if isinstance(canonical, dict) else None
    if isinstance(spec, dict):
        slots = tuple(_discover_workflow_slots(spec, base_path=("spec",)))
    else:
        slots = ()
    return NormalizedWorkflow(document=canonical, slots=slots)


def normalize_template(doc: dict[str, Any]) -> NormalizedTemplate:
    """Normalize a WorkflowTemplate document.

    The placeholder block is canonicalized along with the nested
    ``spec.workflow`` body. Slot discovery runs against the inner
    workflow spec so any concrete (non-placeholder-bound) references
    still emit slots for the materialized workflow's downstream
    resolver pass.
    """
    canonical = _canonicalize(doc)
    template_spec = canonical.get("spec") if isinstance(canonical, dict) else None
    slots: tuple[RefResolutionSlot, ...] = ()
    if isinstance(template_spec, dict):
        inner = template_spec.get("workflow")
        if isinstance(inner, dict):
            slots = tuple(
                _discover_workflow_slots(inner, base_path=("spec", "workflow")),
            )
    return NormalizedTemplate(document=canonical, slots=slots)

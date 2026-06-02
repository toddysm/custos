"""Per-step :class:`SchemaBindings` derivation (WF-IMPL-017).

For each step in a parsed :class:`~custos_workflow.document.WorkflowDocument`,
build the :class:`custos_cel.SchemaBindings` view the type checker uses
at that step's call sites. The view sees:

- ``inputs.*`` — the workflow's declared ``spec.inputs`` translated
  into a JSON Schema object.
- ``steps.<id>.outputs.*`` — the outputs schema of every step that
  appears **before** this step in ``spec.steps`` order. Activity
  steps resolve through the :class:`ActivityTypeRegistry`; ``let``
  steps expose one property per let key; ``workflow`` (sub-workflow)
  steps fall back to a permissive ``{"type": "object"}`` schema and
  emit a structured warning until the Catalog client follow-up lands.
- ``run.*`` / ``workflow.*`` / ``now()`` — the static defaults baked
  into :class:`SchemaBindings`.
- ``let.<name>`` — left empty at the step level. Per-call-site let
  layering inside a step (a later let value referencing an earlier
  one in the same ``let:`` block) is the call-site collector's job
  (WF-IMPL-020).

**Ordering**: ``spec.steps`` order *is* the topological order for
this milestone — the document author lists steps in dependency order.
The full implicit-edge topology builder (WF-IMPL-019) will replace
this with cycle-detecting traversal; the public ``derive_bindings``
return shape stays stable across that change because both produce
the same per-step prior-set.

**forEach ``item.*`` binding**: the design exposes ``item.*`` inside
forEach loops, but today's CEL surface only recognises the roots
``inputs`` / ``steps`` / ``let`` / ``run`` / ``workflow`` / ``now``
(see :func:`custos_cel.types._resolve_root`). Adding an ``item``
root is a follow-up tied to WF-IMPL-020 (call-site collector emits
the per-iteration scope) and WF-IMPL-022 (type checker resolves
``item`` against the iterable element type). This module surfaces a
deliberate gap rather than silently mistyping forEach call sites.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from custos_cel import SchemaBindings

from custos_workflow.bindings.registry import (
    ActivityTypeNotFoundError,
    ActivityTypeRegistry,
)
from custos_workflow.document import (
    ActivityStep,
    ApprovalStep,
    LetStep,
    WaitForStep,
    WaitStep,
    WorkflowDocument,
    WorkflowStep,
)

_LOGGER = logging.getLogger(__name__)

#: Permissive outputs schema used when the real schema is not yet
#: knowable (sub-workflow stub). Matches what the type checker
#: accepts as an opaque object.
_PERMISSIVE_OUTPUTS: Mapping[str, Any] = {"type": "object"}


def _inputs_schema(doc: WorkflowDocument) -> dict[str, Any]:
    """Translate the workflow's ``spec.inputs`` into a JSON Schema object.

    Each ``InputDefinition`` becomes one property; ``required: True``
    entries appear in the schema's ``required`` array. Extra metadata
    (``default`` / ``description``) is preserved verbatim so the type
    checker and any downstream tooling can read it.
    """
    properties: dict[str, dict[str, Any]] = {}
    required: list[str] = []
    inputs = doc.spec.inputs or {}
    for name, decl in inputs.items():
        prop: dict[str, Any] = {"type": decl.type}
        # ``custos_cel._schema_to_celtype`` rejects an array schema
        # without an ``items`` sub-schema, so an ``inputs.targets`` of
        # type ``array`` would make every reference to that input fail
        # type-check. The Catalog ``InputDefinition`` does not yet
        # carry an element schema, so we emit a permissive object
        # fallback (``ListType(MapType(string→null))`` in CEL terms).
        # Tightening element types is a follow-up alongside richer
        # ``InputDefinition`` shape.
        if decl.type == "array":
            prop["items"] = {"type": "object"}
        if decl.description is not None:
            prop["description"] = decl.description
        if decl.default is not None:
            prop["default"] = decl.default
        properties[name] = prop
        if decl.required:
            required.append(name)
    schema: dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


def _activity_outputs_schema(
    step: ActivityStep, registry: ActivityTypeRegistry
) -> Mapping[str, Any]:
    """Look up the outputs schema for an activity step.

    A missing registry entry is a hard error — the Catalog publish
    gate guarantees every referenced activity exists, so a miss at
    compile time means the registry is out of sync and the run must
    not start.
    """
    try:
        return registry.get_outputs_schema(step.activity)
    except ActivityTypeNotFoundError as exc:
        # Re-raise carrying both the machine-readable ref (preserved
        # in ``args[0]`` and ``.activity_ref``) and a richer human
        # message. The public taxonomy lift happens in WF-IMPL-024.
        raise ActivityTypeNotFoundError(
            exc.activity_ref,
            message=(
                f"step {step.id!r}: activity reference {step.activity!r} is not "
                "registered with the ActivityTypeRegistry"
            ),
        ) from None


def _let_outputs_schema(step: LetStep) -> dict[str, Any]:
    """Build an outputs schema from a let step's binding names.

    The Compiler does not yet know each let value's type — that is
    WF-IMPL-022's responsibility (the type checker walks each let
    value's CEL AST and records its inferred type). Emit one
    permissive object property per let key so downstream type-check
    resolves ``steps.<id>.outputs.<name>`` to an opaque object
    (``MapType(string→null)`` in CEL terms) until tightened. A plain
    ``{}`` is rejected by ``custos_cel._schema_to_celtype`` because
    it has no ``type`` key, which would silently break every later
    reference to a derived let value.
    """
    properties: dict[str, dict[str, Any]] = {name: {"type": "object"} for name in step.let}
    return {
        "type": "object",
        "properties": properties,
    }


def _sub_workflow_outputs_schema(step: WorkflowStep, logger: logging.Logger) -> Mapping[str, Any]:
    """Permissive stub for sub-workflow outputs.

    Resolving the child workflow's actual outputs schema requires a
    Catalog lookup against ``WorkflowVersion`` — a cross-component
    follow-up parallel to the activity-type wiring. For this
    milestone we emit a structured warning and return an open object
    so type-check proceeds without false negatives.
    """
    logger.warning(
        "binding.unresolved_sub_workflow",
        extra={
            "step_id": step.id,
            "workflow_ref": step.workflow,
            "note": (
                "sub-workflow outputs schema unresolved; using permissive "
                "stub until the Catalog client follow-up lands"
            ),
        },
    )
    return _PERMISSIVE_OUTPUTS


def _approval_outputs_schema(step: ApprovalStep, logger: logging.Logger) -> Mapping[str, Any]:
    """Permissive stub for approval-gate outputs.

    The approval decision payload (approver identity, decision,
    decided-at timestamp) is delivered via the external approval
    signal and is not locked in the wire schema yet — the full
    approval-execution path lands in a later Sub-Orchestration
    Manager task. For this milestone we emit a structured warning
    and return an open object so any ``steps.<id>.outputs`` reference
    type-checks without a false negative.
    """
    logger.warning(
        "binding.unresolved_approval",
        extra={
            "step_id": step.id,
            "note": (
                "approval-gate outputs schema unresolved; using permissive "
                "stub until the approval-execution follow-up lands"
            ),
        },
    )
    return _PERMISSIVE_OUTPUTS


def _wait_for_outputs_schema(step: WaitForStep, logger: logging.Logger) -> Mapping[str, Any]:
    """Permissive stub for ``waitFor:`` resume outputs (REQ-081).

    When the run resumes, the external event payload delivered by the
    Trigger Service becomes the step's outputs. That payload schema
    is event-specific and is not locked in the wire schema yet — the
    full Resume Subscription Manager execution path lands in a later
    task. For this milestone we emit a structured warning and return
    an open object so any ``steps.<id>.outputs`` reference type-checks
    without a false negative.
    """
    logger.warning(
        "binding.unresolved_wait_for",
        extra={
            "step_id": step.id,
            "note": (
                "waitFor resume outputs schema unresolved; using permissive "
                "stub until the Resume Subscription Manager execution "
                "follow-up lands"
            ),
        },
    )
    return _PERMISSIVE_OUTPUTS


def _step_outputs_schema(
    step: ActivityStep | LetStep | WorkflowStep | WaitStep | ApprovalStep | WaitForStep,
    registry: ActivityTypeRegistry,
    logger: logging.Logger,
) -> Mapping[str, Any]:
    if isinstance(step, ActivityStep):
        return _activity_outputs_schema(step, registry)
    if isinstance(step, LetStep):
        return _let_outputs_schema(step)
    if isinstance(step, WaitStep):
        # ``wait:`` produces no outputs — it is a pure delay primitive.
        # The empty-properties object lets the type checker resolve
        # ``steps.<id>.outputs`` to an empty map without any
        # field-level lookups (which would all be unbound names).
        return {"type": "object", "properties": {}}
    if isinstance(step, ApprovalStep):
        return _approval_outputs_schema(step, logger)
    if isinstance(step, WaitForStep):
        return _wait_for_outputs_schema(step, logger)
    # WorkflowStep — narrowed by exhaustion.
    return _sub_workflow_outputs_schema(step, logger)


def derive_bindings(
    doc: WorkflowDocument,
    registry: ActivityTypeRegistry,
    *,
    logger: logging.Logger | None = None,
) -> dict[str, SchemaBindings]:
    """Return ``{step_id: SchemaBindings}`` for every step in ``doc``.

    Each entry's :attr:`SchemaBindings.prior_steps` contains only the
    steps that precede the current step in ``spec.steps`` order, so a
    call-site inside step *B* never sees outputs of a step that runs
    after *B*. The ``inputs`` / ``run`` / ``workflow`` / ``now`` slots
    are identical across every step in the document — they describe
    the run, not the call-site.

    Args:
        doc: A parsed :class:`WorkflowDocument`.
        registry: An :class:`ActivityTypeRegistry` providing outputs
            schemas for activity references. Missing entries raise
            :class:`ActivityTypeNotFoundError` (hard failure — see
            :func:`_activity_outputs_schema`).
        logger: Optional logger for structured warnings (sub-workflow
            stub). Defaults to the module logger so callers can hook
            ``custos_workflow.bindings.derive`` directly.

    Returns:
        A dict keyed by step id. Insertion order matches
        ``spec.steps`` order.

    Raises:
        ActivityTypeNotFoundError: Any activity step references an
            activity not present in ``registry``.
    """

    log = logger or _LOGGER
    inputs_schema = _inputs_schema(doc)
    result: dict[str, SchemaBindings] = {}
    accumulated_prior: list[tuple[str, Mapping[str, Any]]] = []

    for step in doc.spec.steps:
        # Snapshot the prior list BEFORE adding this step so the
        # bindings view at step S sees only steps strictly before S.
        bindings = SchemaBindings(
            inputs=inputs_schema,
            prior_steps=tuple(accumulated_prior),
        )
        result[step.id] = bindings
        accumulated_prior.append((step.id, _step_outputs_schema(step, registry, log)))

    return result

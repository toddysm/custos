"""``with:`` input resolver for the Step Coordinator (WF-IMPL-051).

The Step Coordinator's :class:`ActivityStepHandler` (WF-IMPL-054)
must turn the compiled ``with:`` block of an
:class:`~custos_workflow.graph.model.ExecutionNode` into the
concrete input mapping it hands to
:class:`~custos_workflow.clients.ScheduleActivityRequest.inputs`.
The Definition Compiler (WF-IMPL-020) has already parsed +
type-checked every ``${{ ... }}`` segment under ``with:`` and
attached the resulting :class:`TypedAST`s to
:attr:`ExecutionNode.call_sites` under the
``"with.<key>"`` / ``"with.<key>[<idx>]"`` slot labels. This module
runs the *evaluate* phase: it pulls each typed AST from the node,
hands it to :func:`custos_cel.evaluate` against the per-run
:class:`BindingScope`, and assembles the result.

Rules (mirrored from ``design.md`` § *with:* semantics):

* Non-string values pass through unchanged (the document model
  already accepts numbers, booleans, mappings, sequences, and
  ``None`` as plain data).
* A string with **no** ``${{ ... }}`` placeholders passes through
  as a literal string.
* A string consisting of **a single** ``${{ ... }}`` placeholder
  (modulo surrounding whitespace) is evaluated and the **raw**
  CEL value is returned — this preserves the value's CEL type so
  e.g. ``${{ 1 + 2 }}`` lands as an ``int`` in the activity's
  input mapping, not as the string ``"3"``.
* A string with **multiple** placeholders (or one placeholder
  embedded in literal text) is interpolated: each placeholder is
  evaluated and ``str()``-converted, then concatenated with the
  intervening literal slices in source order.

Any :class:`custos_cel.CelError` (or subclass — parse / type /
unbound-name / timeout / evaluation) is wrapped in
:class:`WithInputResolutionError` with the underlying ``kind``
preserved on :attr:`~WithInputResolutionError.cause_kind` so audit
consumers can still dispatch on the root cause without traversing
the wrapper hierarchy.

The resolver is **pure**: it performs no I/O, holds no state, and
receives every external dependency (scope + clock) as an argument.
The same ``(node, scope, clock)`` triple always produces the same
output, which is what the WF-IMPL-053 retry decision driver and
the Dapr Workflow replay model both require.

Acceptance criteria (mirrored from #422):

* All five locked CEL ``kind``s (``expression.parse_error``,
  ``expression.type_error``, ``expression.unbound_name``,
  ``expression.timeout``, ``expression.evaluation_error``) round-trip
  into :class:`WithInputResolutionError` with the underlying
  ``kind`` preserved on :attr:`cause_kind`.
* The resolver is pure — no I/O.
* 100 % coverage on this module.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, Final

from custos_cel import BindingScope, CelError, Clock, evaluate

from custos_workflow.callsites import extract_placeholders
from custos_workflow.graph.model import CallSiteKind, ExecutionNode
from custos_workflow.steps.errors import WithInputResolutionError

__all__ = ["WithInputResolver"]


# Sentinel mapping returned for nodes without a ``with:`` block —
# saves a per-call ``MappingProxyType(dict())`` allocation.
_EMPTY_INPUTS: Final[Mapping[str, Any]] = MappingProxyType({})


class WithInputResolver:
    """Pure resolver for ``with:`` input mappings.

    The class is stateless — the single :meth:`resolve` method is
    effectively a free function namespaced under the class so the
    Step Coordinator can inject it via :class:`typing.Protocol` if
    a future test ever wants to substitute a different
    implementation. Today's only implementation is this one.
    """

    def resolve(
        self,
        node: ExecutionNode,
        scope: BindingScope,
        clock: Clock,
        *,
        run_id: str | None = None,
        attempt: int | None = None,
    ) -> Mapping[str, Any]:
        """Evaluate ``node``'s ``with:`` block against ``scope``.

        :param node: The compiled step node. Only its
            :attr:`~ExecutionNode.step_source` (for the source
            ``with:`` dict) and :attr:`~ExecutionNode.call_sites`
            (for the pre-parsed :class:`TypedAST`s) are read; the
            method does not mutate either.
        :param scope: The per-run :class:`BindingScope` carrying
            ``inputs`` / ``steps`` / ``run`` / ``workflow`` / ``let``.
            Already immutable per its own contract.
        :param clock: The :class:`Clock` adapter that powers
            ``now()`` inside CEL evaluations.
        :param run_id: Optional — recorded on the
            :class:`WithInputResolutionError` if one is raised, so
            the audit emitter has the full triple without an extra
            lookup.
        :param attempt: Optional — recorded on the error envelope
            for the same reason.

        :returns: A :class:`types.MappingProxyType` snapshot of the
            resolved input mapping (``key -> value``). The Step
            Coordinator forwards it straight into
            :attr:`ScheduleActivityRequest.inputs`.

        :raises WithInputResolutionError: If any underlying
            :class:`custos_cel.CelError` propagates. The wrapper
            carries the failing slot's :attr:`binding_name`, the
            original CEL :attr:`cause_kind`, and the source text
            of the offending segment.
        """
        with_block = getattr(node.step_source, "with_", None)
        if not with_block:
            return _EMPTY_INPUTS

        resolved: dict[str, Any] = {}
        for key, value in with_block.items():
            resolved[key] = self._resolve_value(
                node=node,
                key=key,
                value=value,
                scope=scope,
                clock=clock,
                run_id=run_id,
                attempt=attempt,
            )
        return MappingProxyType(resolved)

    # ------------------------------------------------------------------
    # Per-value dispatch
    # ------------------------------------------------------------------

    def _resolve_value(
        self,
        *,
        node: ExecutionNode,
        key: str,
        value: Any,
        scope: BindingScope,
        clock: Clock,
        run_id: str | None,
        attempt: int | None,
    ) -> Any:
        # Non-string values are pure data; the document model
        # already accepts numbers, bools, dicts, lists, None.
        if not isinstance(value, str):
            return value

        segments = extract_placeholders(value)

        # Plain literal string — no CEL involvement.
        if not segments:
            return value

        # Single placeholder that covers the whole string (modulo
        # surrounding whitespace) — return the raw CEL value so
        # the type (int / bool / list / dict / datetime) survives
        # into the activity's input mapping. The collector keys
        # this case as ``"with.<key>"`` (no ``[<idx>]`` suffix).
        if len(segments) == 1 and _segment_covers_value(value, segments[0]):
            slot_label = f"with.{key}"
            return self._evaluate_slot(
                node=node,
                slot_label=slot_label,
                segment_source=segments[0].token,
                scope=scope,
                clock=clock,
                binding_name=key,
                run_id=run_id,
                attempt=attempt,
            )

        # Multi-placeholder OR mixed-content string — interpolate.
        # The collector keys each segment as ``"with.<key>[<idx>]"``
        # in source order; we walk segments + intervening literals
        # the same way to produce the concatenated result.
        parts: list[str] = []
        cursor = 0
        for idx, segment in enumerate(segments):
            if segment.start > cursor:
                parts.append(value[cursor : segment.start])
            slot_label = f"with.{key}[{idx}]"
            evaluated = self._evaluate_slot(
                node=node,
                slot_label=slot_label,
                segment_source=segment.token,
                scope=scope,
                clock=clock,
                binding_name=key,
                run_id=run_id,
                attempt=attempt,
            )
            parts.append(str(evaluated))
            cursor = segment.end
        if cursor < len(value):
            parts.append(value[cursor:])
        return "".join(parts)

    # ------------------------------------------------------------------
    # CEL evaluation + error wrapping
    # ------------------------------------------------------------------

    def _evaluate_slot(
        self,
        *,
        node: ExecutionNode,
        slot_label: str,
        segment_source: str,
        scope: BindingScope,
        clock: Clock,
        binding_name: str,
        run_id: str | None,
        attempt: int | None,
    ) -> Any:
        # The compiler guarantees the slot is present — if not, the
        # graph blob is structurally broken (the call-site collector
        # would have raised at compile time). Surface a clean
        # ``WithInputResolutionError`` rather than a bare
        # ``KeyError`` so the audit envelope still carries the slot
        # context, but pin a stable message so tests can pattern-
        # match it.
        try:
            call_site = node.call_sites[slot_label]
        except KeyError as exc:
            raise WithInputResolutionError(
                f"compiled graph is missing TypedAST for slot {slot_label!r} "
                f"on step {node.step_id!r}; this indicates a graph blob "
                "that was not produced by the WF-IMPL-020 collector",
                run_id=run_id,
                step_id=node.step_id,
                attempt=attempt,
                binding_name=binding_name,
                source=segment_source,
            ) from exc

        # The collector emits ``CallSiteKind.WITH`` for every
        # ``${{ ... }}`` under a ``with:`` block. Defensive guard
        # against a slot label collision (e.g. a future kind
        # accidentally reusing the ``with.`` prefix).
        if call_site.kind is not CallSiteKind.WITH:
            raise WithInputResolutionError(
                f"slot {slot_label!r} on step {node.step_id!r} is not a "
                f"with: call site (kind={call_site.kind.value!r}); the resolver "
                "only handles CallSiteKind.WITH entries",
                run_id=run_id,
                step_id=node.step_id,
                attempt=attempt,
                binding_name=binding_name,
                source=segment_source,
            )

        try:
            return evaluate(call_site.typed_ast, scope, clock)
        except CelError as exc:
            # Wrap any CEL failure (parse / type / unbound-name /
            # timeout / evaluation) so the underlying kind survives
            # on ``cause_kind`` per the locked acceptance criterion.
            raise WithInputResolutionError(
                f"failed to evaluate with:{binding_name!r} on step {node.step_id!r}: {exc}",
                run_id=run_id,
                step_id=node.step_id,
                attempt=attempt,
                binding_name=binding_name,
                cause_kind=exc.kind,
                source=call_site.source,
            ) from exc


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _segment_covers_value(value: str, segment: Any) -> bool:
    """Return ``True`` iff ``segment`` spans the whole of ``value``.

    Mirrors the same predicate the WF-IMPL-020 call-site collector
    uses to decide whether a ``let:`` binding's string scalar is a
    single CEL expression. Tolerates leading and trailing
    whitespace OUTSIDE the placeholder (CEL is
    whitespace-insensitive and authors legitimately insert space
    for readability).
    """
    before = value[: segment.start]
    after = value[segment.end :]
    return before.strip() == "" and after.strip() == ""

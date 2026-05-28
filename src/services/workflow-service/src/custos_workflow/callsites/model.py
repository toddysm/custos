"""Frozen dataclasses for collected CEL call sites (WF-IMPL-020).

The collector lifts every CEL expression occurrence in a
:class:`~custos_workflow.document.WorkflowDocument` into a
:class:`CallSite` instance. Downstream consumers — the topology
layer's data-dependency pass (WF-IMPL-019) and the type-checking
driver (WF-IMPL-021) — read these instances; they MUST NOT walk
the raw document a second time, so the collector's output is the
single source of truth for "where do CEL expressions live".

Records are ``@dataclass(frozen=True, slots=True)`` because the
Definition Compiler hands them to Dapr Workflow activities by
value: immutability keeps replay deterministic (no accidental
mutation between activity runs) and ``slots=True`` shrinks the
per-instance footprint on the order of a hundred call sites per
workflow.

The shape is intentionally distinct from
:class:`custos_workflow.graph.model.TypedCallSite`:

- :class:`CallSite` is the **untyped** form: AST is from
  :func:`custos_cel.parse` only; no schema bindings have been
  applied. It is the *input* to the type checker.
- :class:`TypedCallSite` is the **typed** form: AST is from
  :func:`custos_cel.type_check`. It is what the compiled
  :class:`~custos_workflow.graph.model.ExecutionGraph` stores.

The two are wire-disjoint: the topology layer takes a mapping keyed
by call-site label and shaped as either; the type-checker driver
will translate one to the other in WF-IMPL-021.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from custos_cel import AST

    from custos_workflow.graph.model import CallSiteKind


@dataclass(frozen=True, slots=True)
class SourcePosition:
    """Where a CEL call site appears in the source ``WorkflowDocument``.

    The breadcrumb is intentionally cheap and structural — neither
    PyYAML nor Pydantic give us line/column information once the
    document has been loaded, and the compile-time work happens
    against the already-parsed Python model. The two-field shape is
    enough for diagnostics ("error at ``spec.steps[2].with.image``,
    placeholder at offset 9") and survives JSON round-tripping
    cleanly.

    Attributes:
        document_path: Dotted / JSON-pointer-like location of the
            **field** that holds the call site, rooted at the
            document body. For example
            ``spec.steps[2].with.image`` or
            ``spec.steps[0].when``. List indices use ``[N]``
            (zero-based) so the breadcrumb mirrors the YAML node
            path that authors see in editor diagnostics.
        text_offset: Character offset of the call site's opening
            ``${{`` within the raw string value of the field. For
            single-call-site fields (``if`` / ``when`` / ``unless``
            / ``forEach`` / ``where`` / ``let.<name>``) the entire
            field IS the call site and the offset is ``0``. For
            ``with`` fields that interleave literals and
            placeholders (e.g. ``"registry/${{ x }}:${{ y }}"``)
            each emitted placeholder carries its own offset so the
            compiler can underline the exact ``${{ ... }}`` segment
            in error output.
    """

    document_path: str
    text_offset: int = 0


@dataclass(frozen=True, slots=True)
class CallSite:
    """One collected CEL call site.

    The collector emits one :class:`CallSite` per ``${{ ... }}``
    occurrence (not per field): a ``with:`` value with two embedded
    placeholders produces two :class:`CallSite` records, both with
    the same ``path`` and different :attr:`position.text_offset`
    values.

    Attributes:
        step_id: The owning step's id.
        kind: The structural call-site slot — see
            :class:`~custos_workflow.graph.model.CallSiteKind` for
            the full enumeration. The collector NEVER emits
            ``CallSiteKind.PLACEHOLDER``; that tag is reserved for
            compiler-internal synthetics (per the enum's docstring).
        path: A compact location within the owning step, useful as a
            stable dict key and in log messages. The format is one
            of ``"if"`` / ``"when"`` / ``"unless"`` / ``"forEach"`` /
            ``"where"`` / ``"let.<name>"`` / ``"with.<key>"`` /
            ``"with.<key>[N]"`` (the bracketed form is used for the
            ``N``th placeholder when the same ``with:`` key holds
            multiple embedded placeholders).
        source: The original CEL token text, including the
            ``${{ ... }}`` wrapper, exactly as it appears in the
            source document. Preserved verbatim so error messages
            reproduce the author's wording byte-for-byte.
        position: Structural source breadcrumb — see
            :class:`SourcePosition`.
        parsed_ast: The result of :func:`custos_cel.parse` applied
            to the inner CEL expression (with the ``${{ }}``
            wrapper stripped). The AST is **untyped** — no schema
            bindings have been resolved — until the type-checker
            driver in WF-IMPL-021 lifts it.
    """

    step_id: str
    kind: CallSiteKind
    path: str
    source: str
    position: SourcePosition
    parsed_ast: AST

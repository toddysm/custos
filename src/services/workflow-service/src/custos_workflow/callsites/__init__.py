"""CEL call-site collection for the Workflow Service (WF-IMPL-020).

Extracts every CEL expression occurrence from a parsed
:class:`~custos_workflow.document.WorkflowDocument` into a flat
``{step_id: [CallSite, ...]}`` mapping. Each :class:`CallSite` carries

- the original ``${{ ... }}`` source text (verbatim from the YAML),
- a :class:`SourcePosition` breadcrumb for diagnostics,
- the parsed (but not yet type-checked) AST returned by
  :func:`custos_cel.parse`.

This is the input shape consumed by the topology layer's
data-dependency pass (WF-IMPL-019, ``collect_data_dependencies``)
and the type-checking driver (WF-IMPL-021). It mechanically
enumerates the call sites locked in
``docs/developers/cel-expressions.md`` § *Where expressions are used*:

============================  =================
YAML surface                   :class:`CallSiteKind`
============================  =================
``if:`` / ``when:`` /          ``IF`` / ``WHEN`` /
``unless:``                    ``UNLESS``
``forEach:`` / ``where:``      ``FOR_EACH`` /
                               ``WHERE``
``let.<name>``                 ``LET``
``with.<key>``                 ``WITH``
============================  =================

Single-call-site fields (``if``, ``when``, ``unless``, ``forEach``,
``where``, ``let.<name>``) are validated by the document model
(``_StepCommon._check_cel_wrappers``) to be a complete
``${{ ... }}`` token; the collector emits one :class:`CallSite` per
field with ``text_offset = 0``.

``with`` values are arbitrary scalars and MAY contain *embedded*
placeholders — e.g. ``image: "registry/${{ inputs.image }}:latest"``
or ``tag: "${{ a }}-${{ b }}"`` — so the collector walks every
string scalar with :func:`extract_placeholders` and emits one
``WITH`` call site per ``${{ ... }}`` segment, each carrying the
character offset within the original field for downstream error
reporting.

A backslash-escaped placeholder (``\\${{ ... }}``) is treated as a
literal and skipped, consistent with the standard escape convention
used in templating engines and locked at the schema layer.
"""

from __future__ import annotations

from custos_workflow.callsites.collect import (
    CallSiteParseError,
    collect_call_sites,
)
from custos_workflow.callsites.model import CallSite, SourcePosition
from custos_workflow.callsites.placeholders import (
    PlaceholderSegment,
    extract_placeholders,
)

__all__ = [
    "CallSite",
    "CallSiteParseError",
    "PlaceholderSegment",
    "SourcePosition",
    "collect_call_sites",
    "extract_placeholders",
]

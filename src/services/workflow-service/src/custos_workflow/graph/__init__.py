"""``ExecutionGraph`` data model + byte-stable JSON serializer (WF-IMPL-018).

The :class:`ExecutionGraph` is the compiled, runtime-ready
representation of a :class:`~custos_workflow.document.WorkflowDocument`.
It is persisted on ``Run.compiledGraph`` at ``StartRun`` time so a
Catalog Service outage cannot pause in-flight runs, and so that Dapr
Workflow replay never re-fetches the source ``WorkflowVersion``
(design.md § Pod Restart / Dapr Replay).

This module owns the **shape** of that compiled artefact and the
serializer that turns it into byte-stable JSON. The wiring that
*populates* nodes, edges, retry policy and on-error routes is split
across the remaining WF-IMPL-019 → WF-IMPL-023 tasks; the data
contract here is what every later task writes into.

Public exports:

- :class:`ExecutionGraph` + :class:`GraphMetadata`.
- :class:`ExecutionNode` + :class:`Edge` + :class:`TypedCallSite`.
- :class:`ResolvedRetryPolicy` + :class:`OnErrorRoute` (frozen-now,
  details filled by WF-IMPL-022 / WF-IMPL-023; the shape is locked
  here so later tasks do not need to rewrite the serializer).
- Enum tags: :class:`StepKind`, :class:`PrimitiveHandler`,
  :class:`EdgeKind`, :class:`CallSiteKind`,
  :class:`OnErrorActionTag`, :class:`BackoffStrategyTag`,
  :class:`JitterStrategyTag`.
- :data:`GRAPH_SCHEMA_VERSION` — bumped any time the on-disk JSON
  envelope changes shape (separate from the
  :data:`custos_cel.AST_SCHEMA_VERSION` envelope embedded inside
  each typed call site).
- :func:`to_json` / :func:`from_json` — byte-stable JSON round-trip.
"""

from __future__ import annotations

from custos_workflow.graph.model import (
    BackoffStrategyTag,
    CallSiteKind,
    Edge,
    EdgeKind,
    ExecutionGraph,
    ExecutionNode,
    GraphMetadata,
    JitterStrategyTag,
    OnErrorActionTag,
    OnErrorRoute,
    PrimitiveHandler,
    ResolvedBackoffPolicy,
    ResolvedRetryPolicy,
    StepKind,
    TypedCallSite,
)
from custos_workflow.graph.serialize import (
    GRAPH_SCHEMA_VERSION,
    GraphSerializationError,
    from_json,
    to_json,
)

__all__ = [
    "GRAPH_SCHEMA_VERSION",
    "BackoffStrategyTag",
    "CallSiteKind",
    "Edge",
    "EdgeKind",
    "ExecutionGraph",
    "ExecutionNode",
    "GraphMetadata",
    "GraphSerializationError",
    "JitterStrategyTag",
    "OnErrorActionTag",
    "OnErrorRoute",
    "PrimitiveHandler",
    "ResolvedBackoffPolicy",
    "ResolvedRetryPolicy",
    "StepKind",
    "TypedCallSite",
    "from_json",
    "to_json",
]

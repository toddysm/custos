"""Byte-stable JSON serializer for :class:`ExecutionGraph`.

The compiled graph is persisted on ``Run.compiledGraph`` and read back
on every Dapr Workflow replay. The format must therefore be:

- **Byte-stable** — two calls to :func:`to_json` on equal graphs must
  produce identical bytes so the persisted blob is a deterministic
  function of the source ``WorkflowVersion`` (design.md § Replay-safe
  Immutability). ``json.dumps(..., sort_keys=True,
  separators=(",", ":"))`` plus deterministic ordering of every list
  guarantees this.
- **Schema-versioned** — :data:`GRAPH_SCHEMA_VERSION` is bumped any
  time the envelope shape changes. The embedded ``custos_cel``
  payloads carry their own :data:`custos_cel.AST_SCHEMA_VERSION`
  inside each typed call site, so the two layers version
  independently.
- **Round-trippable** — :func:`from_json` returns an
  :class:`ExecutionGraph` whose structural equality matches the
  original. The pydantic ``step_source`` field is round-tripped via
  ``TypeAdapter[Step].dump_python(mode="json", by_alias=True)`` so
  wire field names (e.g. ``forEach``) survive.

Any failure during deserialization raises
:class:`GraphSerializationError` — a subclass of :class:`ValueError`
so callers that already catch ``ValueError`` (json decoding,
pydantic) keep working.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Final

from custos_cel import (
    AST_SCHEMA_VERSION,
    TypedAST,
)
from custos_cel import (
    from_dict as _cel_from_dict,
)
from custos_cel.ast import to_dict_envelope as _cel_to_dict_envelope
from pydantic import TypeAdapter, ValidationError

from custos_workflow.document import Step
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

#: On-disk schema version for the :class:`ExecutionGraph` envelope.
#: Bumped on any wire-shape change. The embedded ``custos_cel``
#: payloads carry their own :data:`custos_cel.AST_SCHEMA_VERSION`.
GRAPH_SCHEMA_VERSION: Final[int] = 1

_STEP_TYPE_ADAPTER: Final[TypeAdapter[Step]] = TypeAdapter(Step)


class GraphSerializationError(ValueError):
    """Raised when :func:`from_json` cannot rebuild an
    :class:`ExecutionGraph`.

    Wraps the originating JSON / Pydantic / value error in
    ``__cause__`` so the structured cause survives without callers
    having to introspect a string.
    """


# ---------------------------------------------------------------------------
# Encoders (model → JSON-ready dict)
# ---------------------------------------------------------------------------


def _encode_backoff(b: ResolvedBackoffPolicy) -> dict[str, Any]:
    return {
        "initial_delay_ms": b.initial_delay_ms,
        "max_delay_ms": b.max_delay_ms,
        "multiplier": b.multiplier,
        "strategy": b.strategy.value,
    }


def _encode_retry(r: ResolvedRetryPolicy) -> dict[str, Any]:
    return {
        "backoff": _encode_backoff(r.backoff),
        "jitter": r.jitter.value,
        "max_attempts": r.max_attempts,
        "respect_retry_after": r.respect_retry_after,
    }


def _encode_on_error_route(route: OnErrorRoute) -> dict[str, Any]:
    out: dict[str, Any] = {"action": route.action.value}
    if route.code is not None:
        out["code"] = route.code
    if route.code_prefix is not None:
        out["code_prefix"] = route.code_prefix
    if route.cls is not None:
        out["class"] = route.cls
    if route.retry is not None:
        out["retry"] = _encode_retry(route.retry)
    return out


def _encode_call_site(cs: TypedCallSite) -> dict[str, Any]:
    # The CEL envelope is embedded as a dict (NOT a JSON string) so the
    # outer ``json.dumps(sort_keys=True)`` recurses into it and the
    # whole document remains byte-stable end-to-end.
    return {
        "document_path": cs.document_path,
        "kind": cs.kind.value,
        "source": cs.source,
        "typed_ast": _cel_to_dict_envelope(cs.typed_ast),
    }


def _encode_node(node: ExecutionNode) -> dict[str, Any]:
    out: dict[str, Any] = {
        "call_sites": {label: _encode_call_site(cs) for label, cs in node.call_sites.items()},
        "kind": node.kind.value,
        "on_error_routes": [_encode_on_error_route(r) for r in node.on_error_routes],
        "primitive_handler": node.primitive_handler.value,
        "step_id": node.step_id,
        "step_source": _STEP_TYPE_ADAPTER.dump_python(
            node.step_source,
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
    }
    if node.retry_policy is not None:
        out["retry_policy"] = _encode_retry(node.retry_policy)
    return out


def _encode_edge(edge: Edge) -> dict[str, Any]:
    return {
        "from": edge.from_step,
        "kind": edge.kind.value,
        "to": edge.to_step,
    }


def _encode_metadata(meta: GraphMetadata) -> dict[str, Any]:
    out: dict[str, Any] = {
        "document_api_version": meta.document_api_version,
        "workflow_name": meta.workflow_name,
    }
    if meta.workflow_workspace is not None:
        out["workflow_workspace"] = meta.workflow_workspace
    return out


def _edge_sort_key(edge: Edge) -> tuple[str, str, str]:
    return (edge.from_step, edge.to_step, edge.kind.value)


def to_json(graph: ExecutionGraph) -> str:
    """Serialize ``graph`` to canonical, byte-stable JSON.

    Nodes are emitted in :attr:`ExecutionGraph.topological_order`;
    edges are sorted lexicographically by
    ``(from_step, to_step, kind)`` so the output is independent of
    the order the topology builder produced them in.
    """
    nodes_by_id = {n.step_id: n for n in graph.nodes}
    nodes_payload = [_encode_node(nodes_by_id[sid]) for sid in graph.topological_order]
    edges_payload = [_encode_edge(e) for e in sorted(graph.edges, key=_edge_sort_key)]
    envelope = {
        "ast_schema_version": AST_SCHEMA_VERSION,
        "edges": edges_payload,
        "graph_schema_version": GRAPH_SCHEMA_VERSION,
        "metadata": _encode_metadata(graph.metadata),
        "nodes": nodes_payload,
        "topological_order": list(graph.topological_order),
    }
    return json.dumps(envelope, sort_keys=True, separators=(",", ":"))


# ---------------------------------------------------------------------------
# Decoders (JSON-ready dict → model)
# ---------------------------------------------------------------------------


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise GraphSerializationError(
            f"{label}: expected JSON object, got {type(value).__name__}",
        )
    return value


def _decode_backoff(data: Mapping[str, Any]) -> ResolvedBackoffPolicy:
    return ResolvedBackoffPolicy(
        strategy=BackoffStrategyTag(data["strategy"]),
        initial_delay_ms=int(data["initial_delay_ms"]),
        max_delay_ms=int(data["max_delay_ms"]),
        multiplier=float(data["multiplier"]),
    )


def _decode_retry(data: Mapping[str, Any]) -> ResolvedRetryPolicy:
    return ResolvedRetryPolicy(
        max_attempts=int(data["max_attempts"]),
        backoff=_decode_backoff(_require_mapping(data["backoff"], "retry.backoff")),
        jitter=JitterStrategyTag(data["jitter"]),
        respect_retry_after=bool(data["respect_retry_after"]),
    )


def _decode_on_error_route(data: Mapping[str, Any]) -> OnErrorRoute:
    retry_payload = data.get("retry")
    retry = (
        _decode_retry(_require_mapping(retry_payload, "on_error.retry"))
        if retry_payload is not None
        else None
    )
    return OnErrorRoute(
        action=OnErrorActionTag(data["action"]),
        code=data.get("code"),
        code_prefix=data.get("code_prefix"),
        cls=data.get("class"),
        retry=retry,
    )


def _decode_typed_ast(payload: Mapping[str, Any], label: str) -> TypedAST:
    # Round-trip through the shared ``custos_cel`` envelope. The call
    # validates the schema version internally and raises bare
    # ``ValueError`` / ``TypeError`` on any structural problem
    # (unknown node kind, missing ``root``, version mismatch, …); wrap
    # those into the module's public error type so callers only need
    # to catch ``GraphSerializationError`` for a malformed blob.
    try:
        return _cel_from_dict(payload)
    except (ValueError, TypeError) as exc:
        raise GraphSerializationError(
            f"{label}: typed_ast payload could not be decoded: {exc}",
        ) from exc


def _decode_call_site(data: Mapping[str, Any], label: str) -> TypedCallSite:
    return TypedCallSite(
        source=str(data["source"]),
        typed_ast=_decode_typed_ast(
            _require_mapping(data["typed_ast"], f"{label}.typed_ast"),
            f"{label}.typed_ast",
        ),
        kind=CallSiteKind(data["kind"]),
        document_path=str(data["document_path"]),
    )


def _decode_node(data: Mapping[str, Any]) -> ExecutionNode:
    call_sites_raw = _require_mapping(data["call_sites"], "node.call_sites")
    call_sites = {
        label: _decode_call_site(
            _require_mapping(cs, f"call_sites[{label!r}]"),
            f"call_sites[{label!r}]",
        )
        for label, cs in call_sites_raw.items()
    }
    on_error_raw = data.get("on_error_routes", [])
    if not isinstance(on_error_raw, list):
        raise GraphSerializationError(
            f"node.on_error_routes: expected JSON array, got {type(on_error_raw).__name__}",
        )
    routes = tuple(
        _decode_on_error_route(_require_mapping(r, "on_error_routes[]")) for r in on_error_raw
    )
    retry_payload = data.get("retry_policy")
    retry = (
        _decode_retry(_require_mapping(retry_payload, "node.retry_policy"))
        if retry_payload is not None
        else None
    )
    step_source_raw = _require_mapping(data["step_source"], "node.step_source")
    try:
        step_source = _STEP_TYPE_ADAPTER.validate_python(dict(step_source_raw))
    except ValidationError as exc:
        raise GraphSerializationError(
            f"node {data.get('step_id')!r}: step_source does not match Step union",
        ) from exc
    return ExecutionNode(
        step_id=str(data["step_id"]),
        kind=StepKind(data["kind"]),
        primitive_handler=PrimitiveHandler(data["primitive_handler"]),
        retry_policy=retry,
        on_error_routes=routes,
        call_sites=call_sites,
        step_source=step_source,
    )


def _decode_edge(data: Mapping[str, Any]) -> Edge:
    return Edge(
        from_step=str(data["from"]),
        to_step=str(data["to"]),
        kind=EdgeKind(data["kind"]),
    )


def _decode_metadata(data: Mapping[str, Any]) -> GraphMetadata:
    return GraphMetadata(
        workflow_name=str(data["workflow_name"]),
        workflow_workspace=(
            str(data["workflow_workspace"]) if data.get("workflow_workspace") is not None else None
        ),
        document_api_version=str(data["document_api_version"]),
    )


def from_json(text: str) -> ExecutionGraph:
    """Rebuild an :class:`ExecutionGraph` from canonical JSON.

    Raises:
        GraphSerializationError: The input is not valid JSON, its
            envelope version does not match this build, or any
            sub-payload fails to round-trip into the dataclass shape.
    """
    try:
        envelope = json.loads(text)
    except json.JSONDecodeError as exc:
        raise GraphSerializationError(f"invalid JSON: {exc.msg}") from exc

    envelope = _require_mapping(envelope, "envelope")
    schema_version = envelope.get("graph_schema_version")
    if schema_version != GRAPH_SCHEMA_VERSION:
        raise GraphSerializationError(
            f"unsupported graph_schema_version {schema_version!r}; "
            f"this build understands version {GRAPH_SCHEMA_VERSION}",
        )
    ast_schema_version = envelope.get("ast_schema_version")
    if ast_schema_version != AST_SCHEMA_VERSION:
        raise GraphSerializationError(
            f"unsupported ast_schema_version {ast_schema_version!r}; "
            f"this build understands version {AST_SCHEMA_VERSION}",
        )

    nodes_raw = envelope.get("nodes")
    if not isinstance(nodes_raw, list):
        raise GraphSerializationError(
            f"envelope.nodes: expected JSON array, got {type(nodes_raw).__name__}",
        )
    nodes = tuple(_decode_node(_require_mapping(n, "nodes[]")) for n in nodes_raw)

    edges_raw = envelope.get("edges")
    if not isinstance(edges_raw, list):
        raise GraphSerializationError(
            f"envelope.edges: expected JSON array, got {type(edges_raw).__name__}",
        )
    edges = tuple(_decode_edge(_require_mapping(e, "edges[]")) for e in edges_raw)

    topo_raw = envelope.get("topological_order")
    if not isinstance(topo_raw, list):
        raise GraphSerializationError(
            f"envelope.topological_order: expected JSON array, got {type(topo_raw).__name__}",
        )
    topological_order = tuple(str(s) for s in topo_raw)

    metadata = _decode_metadata(_require_mapping(envelope["metadata"], "envelope.metadata"))

    try:
        return ExecutionGraph(
            nodes=nodes,
            edges=edges,
            topological_order=topological_order,
            metadata=metadata,
        )
    except ValueError as exc:
        # ``ExecutionGraph.__post_init__`` raises ValueError on
        # structural inconsistencies (edge refs unknown step, etc.).
        raise GraphSerializationError(str(exc)) from exc

"""Bound public-ingress failure evidence before and after authentication.

Records contain only validated correlation, closed taxonomy, numeric status
and bounded source locations. Response bodies, JWT claims, key IDs, URLs and
exception messages are deliberately excluded.
"""

from __future__ import annotations

import json
import logging
import re
import traceback
from pathlib import Path
from typing import Literal

import httpx
from custos_callctx import InvalidReason
from grpc import Call as GrpcCall
from grpc import StatusCode

from custos_workflow.clients._errors import OutboundRpcStatusError

# Preserve configured filters for the existing public-start diagnostic stream.
logger = logging.getLogger("custos_workflow.runs.public_start")
_AUTH_FAILURE_REASONS = frozenset(
    {reason.value for reason in InvalidReason}
    | {
        "verifier_unavailable",
        "mesh_unavailable",
        "mesh_unauthenticated",
        "mesh_caller_denied",
        "duplicate_header",
        "scope_denied",
        "route_denied",
    }
)


def log_public_start_failure(
    run_id: str | None,
    correlation_id: str | None,
    phase: Literal[
        "authentication",
        "jwks",
        "activation",
        "lookup",
        "identity",
        "retention",
        "recovery",
        "validate",
        "durable-start",
    ],
    error: Exception | None,
    *,
    reason: str | None = None,
    response_status: int | None = None,
) -> None:
    """Retain a bounded ingress cause without changing its propagation or HTTP mapping.

    Follow at most three explicit causes. Unknown types/reasons are marked
    unrecognized; invalid correlation/status is null. Authentication callers
    supply a reason and their response status separately from any upstream HTTP
    status. Rejections without an exception have no causal type or frames.
    """
    correlation = (
        correlation_id
        if isinstance(correlation_id, str)
        and re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
            correlation_id,
        )
        else None
    )
    root: BaseException | None = error
    for _ in range(3):
        if root is None or root.__cause__ is None:
            break
        root = root.__cause__
    name = None if root is None else type(root).__name__
    allowed_types = {
        "WorkflowRuntimeUnavailableError",
        "RunStateConflictError",
        "ValueError",
        "BackendUnavailable",
        "LeaseBusy",
        "LeaseExpired",
        "MigrationRequired",
        "OutboundRpcStatusError",
        "OutboundRpcTransportError",
        "OutboundRpcDecodeError",
        "OSError",
        "TimeoutError",
        "LifecycleEventPublishError",
        "_InactiveRpcError",
        "InvalidCallContextError",
        "MeshAuthenticationError",
        "HTTPStatusError",
        "ConnectError",
        "ConnectTimeout",
        "ReadError",
        "ReadTimeout",
        "WriteError",
        "WriteTimeout",
        "PoolTimeout",
        "RemoteProtocolError",
        "JSONDecodeError",
        "UnicodeDecodeError",
    }
    frames = [
        {"file": Path(frame.filename).name, "function": frame.name, "line": frame.lineno}
        for frame in traceback.extract_tb(None if root is None else root.__traceback__)[-5:]
        if re.fullmatch(r"[A-Za-z0-9_-]{1,100}\.py", Path(frame.filename).name)
        and re.fullmatch(r"[A-Za-z0-9_<>-]{1,100}", frame.name)
    ]
    status: int | None
    if isinstance(root, OutboundRpcStatusError):
        status = root.status_code
    elif isinstance(root, httpx.HTTPStatusError):
        status = root.response.status_code
    else:
        status = None
    grpc_status = root.code() if isinstance(root, GrpcCall) else None
    record: dict[str, object] = {
        "correlationId": correlation,
        "correlationUnavailable": correlation is None,
        "runId": run_id,
        "phase": phase,
        "errorType": name if name is None or name in allowed_types else "unrecognized",
        "httpStatus": status if type(status) is int and 100 <= status <= 599 else None,
        "grpcStatus": grpc_status.name if isinstance(grpc_status, StatusCode) else None,
        "frames": frames,
    }
    if reason is not None:
        record["reason"] = reason if reason in _AUTH_FAILURE_REASONS else "unrecognized"
        record["responseStatus"] = (
            response_status
            if type(response_status) is int and 100 <= response_status <= 599
            else None
        )
    logger.warning("WORKFLOW_PUBLIC_START_FAILURE %s", json.dumps(record))

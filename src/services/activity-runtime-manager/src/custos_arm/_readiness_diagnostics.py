"""Bound ARM readiness evidence before concurrent dependency errors lose component identity.

Only closed component/phase/reason/type values, opaque probe-cycle IDs, numeric
HTTP status and source locations are logged. Never serialize exception text,
request/response data, credentials, configured addresses or resource names.
"""

from __future__ import annotations

import json
import logging
import re
import traceback
from collections.abc import Awaitable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Literal, TypeVar
from uuid import uuid4

import httpx
from asyncpg import InterfaceError, PostgresError  # type: ignore[import-untyped]
from custos_callctx.execution_client import ExecutionAuthorizationError
from custos_spl.errors import SPLError
from kubernetes.client.exceptions import ApiException
from urllib3.exceptions import HTTPError as KubernetesTransportError

Component = Literal[
    "metadata-schema",
    "execution-schema",
    "lease-schema",
    "metadata",
    "artifacts",
    "execution-authorization",
    "catalog",
    "connector",
    "kubernetes-jobs",
    "kubernetes-pods",
    "dependencies",
    "scheduler",
    "lifecycle",
]
Phase = Literal["startup", "readiness"]
Reason = Literal[
    "error",
    "http_status",
    "group_failed",
    "scheduler_missing",
    "gate_closed",
    "unavailable",
    "denied",
]
PROBE_ERRORS = (
    SPLError,
    httpx.HTTPError,
    RuntimeError,
    ApiException,
    KubernetesTransportError,
    OSError,
    TimeoutError,
)
_cycle: ContextVar[tuple[Phase, str] | None] = ContextVar("arm_readiness_cycle", default=None)
_T = TypeVar("_T")
logger = logging.getLogger(__name__)


@contextmanager
def readiness_cycle(phase: Phase) -> Iterator[None]:
    """Share one opaque ID with child tasks; nested probes reuse it and reset on every exit."""
    if _cycle.get() is not None:
        yield
        return
    token = _cycle.set((phase, uuid4().hex))
    try:
        yield
    finally:
        _cycle.reset(token)


def log_readiness_failure(
    component: Component,
    reason: Reason,
    error: BaseException | None = None,
    *,
    http_status: int | None = None,
) -> None:
    """Emit bounded metadata only; do not inspect exception groups, contexts or messages.

    Individual component wrappers log before grouping. Numeric upstream status
    is extracted only from known HTTP/Kubernetes exception types. No arbitrary
    exception attributes, configured values or authorization headers are read.
    """
    cycle = _cycle.get()
    phase, probe_id = cycle if cycle is not None else ("startup", uuid4().hex)
    if isinstance(error, httpx.HTTPStatusError):
        http_status = error.response.status_code
    elif isinstance(error, ApiException):
        http_status = error.status
    if isinstance(error, ExecutionAuthorizationError):
        reason = "unavailable" if error.unavailable else "denied"
    name = None if error is None else type(error).__name__
    allowed_types = {
        "RuntimeError",
        "ExceptionGroup",
        "TimeoutError",
        "OSError",
        "PermissionError",
        "FileNotFoundError",
        "BackendUnavailable",
        "MigrationRequired",
        "ExecutionAuthorizationError",
        "HTTPStatusError",
        "ConnectError",
        "ConnectTimeout",
        "ReadError",
        "ReadTimeout",
        "WriteError",
        "WriteTimeout",
        "PoolTimeout",
        "RemoteProtocolError",
        "ApiException",
        "MaxRetryError",
        "ProtocolError",
        "SSLError",
    }
    frames = [
        {"file": Path(frame.filename).name, "function": frame.name, "line": frame.lineno}
        for frame in traceback.extract_tb(None if error is None else error.__traceback__, limit=-5)
        if re.fullmatch(r"[A-Za-z0-9_-]{1,100}\.py", Path(frame.filename).name)
        and re.fullmatch(r"[A-Za-z0-9_<>-]{1,100}", frame.name)
    ]
    logger.warning(
        "ARM_READINESS_FAILURE %s",
        json.dumps(
            {
                "probeId": probe_id,
                "phase": phase,
                "component": component,
                "reason": reason,
                "errorType": name if name is None or name in allowed_types else "unrecognized",
                "httpStatus": (
                    http_status if type(http_status) is int and 100 <= http_status <= 599 else None
                ),
                "frames": frames,
            }
        ),
    )


async def probe_component(component: Component, operation: Awaitable[_T]) -> _T:
    """Log dependency/driver failures and re-raise unchanged; cancellation is not a failure.

    The caller owns deadlines. Sibling cancellation cannot masquerade as a
    component failure, and blocking Kubernetes work still joins before cleanup.
    Raw PostgreSQL errors are logged here without widening the group-conversion set.
    """
    try:
        return await operation
    except (*PROBE_ERRORS, PostgresError, InterfaceError) as error:
        log_readiness_failure(component, "error", error)
        raise

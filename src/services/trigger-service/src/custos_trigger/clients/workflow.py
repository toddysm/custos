"""Workflow Service client — Dapr service-invocation adapter (TS-IMPL-013).

The Dispatcher (``TS-IMPL-014``) calls the Workflow Service's two inbound
Internal RPCs through this client, over the local Dapr sidecar:

* ``start_run`` → ``POST /internal/runs:start`` (a start-subscription match).
* ``raise_external_event`` →
  ``POST /internal/runs/{runId}/steps/{stepId}:raiseEvent`` (a resume match).

The request bodies mirror the Workflow Service's ``InternalStartRunRequest`` /
``RaiseExternalEventRequest`` wire contracts (camelCase via
:class:`~custos_trigger._wire.WireModel`). The transport mirrors the Workflow
Service's own ``_dapr_invoke`` precedent: a lifespan-owned ``httpx.AsyncClient``
posting to ``http://{host}:{port}/v1.0/invoke/{appId}/method/{method}``.

Transport failures and transient HTTP responses (``408``/``429``/``5xx``) raise a
:class:`WorkflowClientError` with ``retryable=True`` so the dispatcher can back
off and retry; permanent ``4xx`` responses and decode failures raise with
``retryable=False`` (dead-letter). :class:`NoopWorkflowServiceClient` and
:class:`FakeWorkflowServiceClient` are test/dev doubles.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

import httpx
from pydantic import Field

from custos_trigger._wire import WireModel

__all__ = [
    "DEFAULT_DAPR_HTTP_HOST",
    "DEFAULT_DAPR_HTTP_PORT",
    "DEFAULT_RPC_TIMEOUT_SECONDS",
    "ENV_DAPR_HTTP_HOST",
    "ENV_DAPR_HTTP_PORT",
    "START_RUN_METHOD",
    "DaprEndpoint",
    "DaprWorkflowServiceClient",
    "FakeWorkflowServiceClient",
    "NoopWorkflowServiceClient",
    "RaiseExternalEventRequest",
    "RunRef",
    "StartRunRequest",
    "WorkflowClientDecodeError",
    "WorkflowClientError",
    "WorkflowClientStatusError",
    "WorkflowClientTransportError",
    "WorkflowServiceClient",
    "build_invoke_url",
    "raise_event_method",
    "read_dapr_endpoint",
]

#: Dapr sidecar host/port env knobs (shared platform convention).
ENV_DAPR_HTTP_HOST: str = "DAPR_HTTP_HOST"
ENV_DAPR_HTTP_PORT: str = "DAPR_HTTP_PORT"
DEFAULT_DAPR_HTTP_HOST: str = "127.0.0.1"
DEFAULT_DAPR_HTTP_PORT: int = 3500
DEFAULT_RPC_TIMEOUT_SECONDS: float = 10.0

#: The Workflow Service inbound method paths (Dapr invoke method segment).
START_RUN_METHOD: str = "internal/runs:start"


def raise_event_method(run_id: str, step_id: str) -> str:
    """Return the Dapr invoke method for raising an event on a waiting step."""
    return f"internal/runs/{run_id}/steps/{step_id}:raiseEvent"


# --- Wire models -------------------------------------------------------------


class StartRunRequest(WireModel):
    """Body of ``POST /internal/runs:start`` (WF ``InternalStartRunRequest``)."""

    workspace_id: str = Field(..., min_length=1)
    workflow_version_id: str = Field(..., min_length=1)
    inputs: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = None


class RaiseExternalEventRequest(WireModel):
    """Body of ``…:raiseEvent`` (WF ``RaiseExternalEventRequest``).

    ``run_id`` / ``step_id`` are carried in the URL path, not the body.
    """

    workspace_id: str = Field(..., min_length=1)
    event_name: str = Field(..., min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = None


class RunRef(WireModel):
    """Response of ``start_run`` (WF ``RunRefResponse``)."""

    run_id: str = Field(..., min_length=1)
    status: str = Field(..., min_length=1)
    workspace_id: str = Field(..., min_length=1)
    workflow_version_id: str = Field(..., min_length=1)
    started_at: datetime | None = None


# --- Errors ------------------------------------------------------------------


class WorkflowClientError(Exception):
    """A failure invoking the Workflow Service.

    ``retryable`` tells the dispatcher whether a backoff-and-retry can plausibly
    succeed (transport blips, ``408``/``429``/``5xx``) or whether the call is a
    permanent failure to dead-letter (contract ``4xx``, undecodable response).
    """

    def __init__(self, message: str, *, retryable: bool) -> None:
        super().__init__(message)
        self.retryable = retryable


class WorkflowClientTransportError(WorkflowClientError):
    """The HTTP request failed before a response arrived (always retryable)."""

    def __init__(self, message: str) -> None:
        super().__init__(message, retryable=True)


class WorkflowClientStatusError(WorkflowClientError):
    """The Workflow Service returned a non-2xx response.

    ``408``/``429``/``5xx`` are retryable (transient); every other status is a
    permanent contract failure.
    """

    def __init__(self, message: str, *, status_code: int) -> None:
        retryable = status_code in (408, 429) or status_code // 100 == 5
        super().__init__(message, retryable=retryable)
        self.status_code = status_code


class WorkflowClientDecodeError(WorkflowClientError):
    """A 2xx response body could not be decoded into the expected shape."""

    def __init__(self, message: str) -> None:
        super().__init__(message, retryable=False)


# --- Endpoint ----------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DaprEndpoint:
    """A resolved Dapr service-invocation target (sidecar host/port + app id)."""

    host: str
    http_port: int
    app_id: str

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("DaprEndpoint.host must be a non-empty string")
        if not self.app_id:
            raise ValueError("DaprEndpoint.app_id must be a non-empty string")
        if self.http_port <= 0:
            raise ValueError(
                f"DaprEndpoint.http_port must be a positive integer (got {self.http_port})"
            )


def build_invoke_url(endpoint: DaprEndpoint, method: str) -> str:
    """Build the Dapr service-invocation URL for ``method`` on ``endpoint``.

    Raises:
        ValueError: If ``method`` is empty or only slashes.
    """
    normalized = method.lstrip("/")
    if not normalized:
        raise ValueError("method must be a non-empty Dapr invoke method path")
    return (
        f"http://{endpoint.host}:{endpoint.http_port}"
        f"/v1.0/invoke/{endpoint.app_id}/method/{normalized}"
    )


def read_dapr_endpoint(env: Mapping[str, str], *, app_id: str) -> DaprEndpoint:
    """Resolve a :class:`DaprEndpoint` from the environment.

    ``app_id`` is the target service's Dapr app id (e.g. the Trigger Service's
    ``TRIGGER_WF_ENDPOINT`` value, ``"workflow-service"``); the sidecar host and
    port come from ``DAPR_HTTP_HOST`` / ``DAPR_HTTP_PORT`` (falling back to the
    Dapr defaults). A non-empty ``app_id`` is required.

    Raises:
        ValueError: If ``app_id`` is empty, or ``DAPR_HTTP_PORT`` is not an int.
    """
    if not app_id:
        raise ValueError("app_id is required to target a Dapr service-invocation endpoint")
    host = env.get(ENV_DAPR_HTTP_HOST, "").strip() or DEFAULT_DAPR_HTTP_HOST
    raw_port = env.get(ENV_DAPR_HTTP_PORT, "").strip()
    if raw_port == "":
        http_port = DEFAULT_DAPR_HTTP_PORT
    else:
        try:
            http_port = int(raw_port)
        except ValueError as exc:
            raise ValueError(f"{ENV_DAPR_HTTP_PORT} must be an integer (got {raw_port!r})") from exc
    return DaprEndpoint(host=host, http_port=http_port, app_id=app_id)


# --- Client interface --------------------------------------------------------


@runtime_checkable
class WorkflowServiceClient(Protocol):
    """The outbound surface the dispatcher depends on."""

    async def start_run(self, request: StartRunRequest) -> RunRef:
        """Start a workflow run; returns the run reference (WF 202)."""
        ...

    async def raise_external_event(
        self, run_id: str, step_id: str, request: RaiseExternalEventRequest
    ) -> None:
        """Deliver an external event to a waiting step (WF 202, empty body)."""
        ...


# --- Real Dapr client --------------------------------------------------------


@dataclass(slots=True)
class DaprWorkflowServiceClient:
    """Calls the Workflow Service Internal RPCs over the local Dapr sidecar.

    The ``http_client`` is owned by the app lifespan (not by this client) so it
    is shared and closed once at shutdown.
    """

    http_client: httpx.AsyncClient
    endpoint: DaprEndpoint
    timeout: float = DEFAULT_RPC_TIMEOUT_SECONDS

    async def start_run(self, request: StartRunRequest) -> RunRef:
        url = build_invoke_url(self.endpoint, START_RUN_METHOD)
        response = await self._post(url, request, what="StartRun")
        try:
            body = response.json()
        except ValueError as exc:
            raise WorkflowClientDecodeError(
                f"StartRun response was not valid JSON: {exc!r}"
            ) from exc
        try:
            return RunRef.model_validate(body)
        except ValueError as exc:
            raise WorkflowClientDecodeError(
                f"StartRun response did not match RunRef: {exc!r}"
            ) from exc

    async def raise_external_event(
        self, run_id: str, step_id: str, request: RaiseExternalEventRequest
    ) -> None:
        url = build_invoke_url(self.endpoint, raise_event_method(run_id, step_id))
        await self._post(url, request, what="RaiseExternalEvent")

    async def _post(
        self, url: str, request: StartRunRequest | RaiseExternalEventRequest, *, what: str
    ) -> httpx.Response:
        wire = request.model_dump(by_alias=True)
        headers = {"Content-Type": "application/json"}
        if request.idempotency_key:
            # Mirror the RFC fallback the WF route honours; the body field is
            # authoritative but the header keeps the call idempotent through
            # any intermediary that only inspects headers.
            headers["Idempotency-Key"] = request.idempotency_key
        try:
            response = await self.http_client.post(
                url, json=wire, timeout=self.timeout, headers=headers
            )
        except httpx.HTTPError as exc:
            raise WorkflowClientTransportError(f"{what} transport failure: {exc!r}") from exc
        if response.status_code // 100 != 2:
            preview = response.text[:200] if response.text else ""
            raise WorkflowClientStatusError(
                f"{what} returned HTTP {response.status_code}: {preview!r}",
                status_code=response.status_code,
            )
        return response


# --- Test / dev doubles ------------------------------------------------------


@dataclass(slots=True)
class NoopWorkflowServiceClient:
    """A do-nothing client (dispatch disabled / dry run).

    ``start_run`` echoes the request into a synthetic ``RunRef`` (status
    ``"noop"``); ``raise_external_event`` is a no-op.
    """

    async def start_run(self, request: StartRunRequest) -> RunRef:
        return RunRef(
            run_id="noop",
            status="noop",
            workspace_id=request.workspace_id,
            workflow_version_id=request.workflow_version_id,
        )

    async def raise_external_event(
        self, run_id: str, step_id: str, request: RaiseExternalEventRequest
    ) -> None:
        return None


@dataclass(slots=True)
class FakeWorkflowServiceClient:
    """Recording double for tests.

    Records every ``start_run`` / ``raise_external_event`` call. ``start_run``
    returns ``run_ref`` (or a synthetic default); set ``error`` to make either
    method raise it (to exercise the dispatcher's retry / dead-letter paths).
    """

    run_ref: RunRef | None = None
    error: WorkflowClientError | None = None
    start_run_calls: list[StartRunRequest] = field(default_factory=list)
    raise_event_calls: list[tuple[str, str, RaiseExternalEventRequest]] = field(
        default_factory=list
    )

    async def start_run(self, request: StartRunRequest) -> RunRef:
        self.start_run_calls.append(request)
        if self.error is not None:
            raise self.error
        if self.run_ref is not None:
            return self.run_ref
        return RunRef(
            run_id="run-fake",
            status="queued",
            workspace_id=request.workspace_id,
            workflow_version_id=request.workflow_version_id,
        )

    async def raise_external_event(
        self, run_id: str, step_id: str, request: RaiseExternalEventRequest
    ) -> None:
        self.raise_event_calls.append((run_id, step_id, request))
        if self.error is not None:
            raise self.error
        return None

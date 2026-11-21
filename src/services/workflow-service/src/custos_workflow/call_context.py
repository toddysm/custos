"""Authenticate public Gateway contexts separately from private Trigger execution.

Public production requests require both authenticated Dapr Gateway provenance
and an Auth-signed context scoped to the route permission and URL workspace.
The shared verifier owns JWT/JWKS validation; this module never trusts plain
identity headers when verification or mandatory context enforcement is enabled.
The unsigned header shim remains available only for explicit development use.
Health probes do not require identity.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from custos_callctx import (
    CALLCTX_HEADER,
    CallContextVerifier,
    InvalidCallContextError,
    InvalidReason,
)
from custos_callctx.execution import EXECUTION_PERMISSIONS, ExecutionIdentity, ExecutionOperation
from custos_callctx.mesh import MeshAuthenticationError, require_service_caller
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import compile_path

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from starlette.types import ASGIApp

#: Wire header carrying the workspace identifier.
WORKSPACE_HEADER: str = "x-custos-workspace"

#: Wire header carrying the principal identifier.
PRINCIPAL_HEADER: str = "x-custos-principal"

#: Paths the middleware deliberately bypasses (no identity on probes).
_BYPASS_PATHS: frozenset[str] = frozenset({"/healthz", "/readyz"})
_PUBLIC_ROUTES = tuple(
    (method, compile_path(path)[0], permission)
    for method, path, permission in (
        ("POST", "/v1/workspaces/{ws}/runs", "workflow:execute"),
        ("GET", "/v1/workspaces/{ws}/runs", "run:read"),
        ("GET", "/v1/workspaces/{ws}/runs/{run_id}", "run:read"),
        ("POST", "/v1/workspaces/{ws}/runs/{run_id}:cancel", "run:cancel"),
        ("GET", "/v1/workspaces/{ws}/runs/{run_id}/steps/{step_id}", "run:read"),
        ("GET", "/v1/workspaces/{ws}/runs/{run_id}/steps/{step_id}/logs", "run:read"),
    )
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CallContext:
    """Per-request authorization context attached to ``request.state``.

    Identity is populated only from verified claims in production. Development
    requests can have absent fields. ``execution_verified`` is reserved for
    the independently authenticated Trigger execution boundary, never Gateway
    public requests.
    """

    workspace: str | None
    principal: str | None
    execution_verified: bool = False
    execution_identity: ExecutionIdentity | None = None


def _envelope(status_code: int, code: str, detail: str) -> JSONResponse:
    """Render the shared call-context error envelope."""
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "detail": detail}},
    )


class CallContextMiddleware(BaseHTTPMiddleware):
    """Bind verified identity after the applicable public or execution checks.

    Args:
        app: The downstream ASGI application (FastAPI passes itself).
        require_call_context: Fail closed without a public verifier; false
            permits the legacy development shim only when no verifier exists.
        call_context_verifier: Shared verifier for public Gateway JWTs.
        execution_verifier: Independent verifier for private Trigger execution.
        app_api_token: Token shared only with this application's local sidecar.
        gateway_namespace: Deployment-owned authenticated Gateway namespace.

    Construction does not perform I/O. Public and execution contexts cannot
    substitute for one another, even when they share verification keys.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        require_call_context: bool,
        call_context_verifier: CallContextVerifier | None = None,
        execution_verifier: CallContextVerifier | None = None,
        app_api_token: str | None = None,
        gateway_namespace: str | None = None,
        gateway_app_id: str = "api-gateway",
        trigger_namespace: str | None = None,
        trigger_app_id: str = "trigger-service",
    ) -> None:
        """Retain deployment-owned trust configuration without fetching keys."""
        super().__init__(app)
        self._require = require_call_context
        self._verifier = call_context_verifier
        self._execution_verifier = execution_verifier
        self._app_api_token = app_api_token
        self._gateway_namespace = gateway_namespace
        self._gateway_app_id = gateway_app_id
        self._trigger_namespace = trigger_namespace
        self._trigger_app_id = trigger_app_id

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        """Authenticate before downstream handlers can inspect workspace state."""
        if request.method == "GET" and request.url.path in _BYPASS_PATHS:
            return await call_next(request)

        expected_operation: ExecutionOperation | None = None
        if request.method == "POST":
            if request.url.path == "/internal/runs:start":
                expected_operation = "workflow-start"
            elif request.url.path == "/internal/runs:raiseEvent" or re.fullmatch(
                r"/internal/runs/.+/steps/[^/]+:raiseEvent", request.url.path
            ):
                expected_operation = "workflow-resume"
            elif re.fullmatch(r"/internal/runs/[^/]+:cancel", request.url.path):
                expected_operation = "workflow-cancel"
        if self._execution_verifier is not None and expected_operation is not None:
            try:
                require_service_caller(
                    request.headers,
                    token=self._app_api_token,
                    app_id=self._trigger_app_id,
                    namespace=self._trigger_namespace,
                )
                if len(request.headers.getlist("x-custos-callctx")) != 1:
                    return _envelope(
                        401, "callctx_invalid", "one signed execution context is required"
                    )
                verified = await self._execution_verifier.verify(metadata=request.headers)
            except MeshAuthenticationError as exc:
                logger.warning("Execution caller rejected: %s", exc.code)
                return _envelope(exc.status_code, exc.code, exc.detail)
            except InvalidCallContextError as exc:
                logger.warning("Execution context rejected: %s", exc.reason.value)
                return _envelope(
                    503 if exc.reason.value == "jwks_unavailable" else 401,
                    "callctx_invalid",
                    "execution context verification failed",
                )
            if (
                not verified.workspace_id
                or EXECUTION_PERMISSIONS[expected_operation] not in verified.permissions
                or verified.caller_component != self._trigger_app_id
                or verified.execution_operation != expected_operation
            ):
                return _envelope(403, "execution_denied", "scoped execution permission required")
            request.state.call_context = CallContext(
                verified.workspace_id,
                verified.acting_principal_id,
                execution_verified=True,
            )
            return await call_next(request)

        if self._verifier is not None or self._require:
            rejection = await self._authenticate_public(request)
            if rejection is not None:
                return rejection
            return await call_next(request)

        workspace = request.headers.get(WORKSPACE_HEADER)
        principal = request.headers.get(PRINCIPAL_HEADER)

        request.state.call_context = CallContext(
            workspace=workspace or None,
            principal=principal or None,
        )
        return await call_next(request)

    async def _authenticate_public(self, request: Request) -> JSONResponse | None:
        """Bind public identity or return a sanitized rejection without entering a handler."""
        if self._verifier is None:
            logger.error("Public call-context verification is not configured")
            return _envelope(503, "callctx_unavailable", "signed context verification is required")
        try:
            require_service_caller(
                request.headers,
                token=self._app_api_token,
                app_id=self._gateway_app_id,
                namespace=self._gateway_namespace,
            )
            contexts = request.headers.getlist(CALLCTX_HEADER)
            if not contexts:
                return _envelope(401, "callctx_missing", "a signed Gateway context is required")
            if len(contexts) != 1:
                return _envelope(401, "callctx_invalid", "one signed Gateway context is required")
            verified = await self._verifier.verify(metadata=request.headers)
        except MeshAuthenticationError as exc:
            logger.warning("Public caller rejected: %s", exc.code)
            return _envelope(exc.status_code, exc.code, exc.detail)
        except InvalidCallContextError as exc:
            logger.warning("Public context rejected: %s", exc.reason.value)
            return _envelope(
                503 if exc.reason is InvalidReason.JWKS_UNAVAILABLE else 401,
                "callctx_invalid",
                "public context verification failed",
            )
        for method, pattern, permission in _PUBLIC_ROUTES:
            match = pattern.fullmatch(request.url.path) if request.method == method else None
            if match is None:
                continue
            if (
                verified.caller_component != self._gateway_app_id
                or verified.execution_operation is not None
                or verified.workspace_id != match["ws"]
                or permission not in verified.permissions
            ):
                return _envelope(
                    403, "callctx_denied", "Gateway route permission and workspace scope required"
                )
            request.state.call_context = CallContext(
                workspace=verified.workspace_id,
                principal=verified.acting_principal_id,
            )
            return None
        return _envelope(403, "callctx_denied", "a public Workflow route is required")


__all__ = [
    "PRINCIPAL_HEADER",
    "WORKSPACE_HEADER",
    "CallContext",
    "CallContextMiddleware",
]

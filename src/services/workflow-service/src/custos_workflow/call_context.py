"""Call-context middleware shim (WF-IMPL-015).

The API Gateway design (COMP-001) carries workspace + principal identity on
every internal request. The full Auth Service integration — signed JWT
header, JWKS verification, and a per-permission ``/authorize`` round-trip —
is tracked separately as a cross-component Phase J follow-up. This module
ships the **minimal shim** the Workflow Service needs so later WF-IMPL-* tasks
can be exercised end-to-end against a real cluster:

- Two plain headers, ``X-Custos-Workspace`` and ``X-Custos-Principal``, are
  read off each request and surfaced on ``request.state.call_context``.
- In dev mode (``WF_REQUIRE_CALL_CONTEXT`` unset or any value other than
  ``"1"``), absence of the headers yields a :class:`CallContext` with
  ``workspace=None`` / ``principal=None``. Route handlers that need an
  identity must check explicitly.
- In production mode (``WF_REQUIRE_CALL_CONTEXT=1``), absence of either
  header makes the middleware return ``401`` with a stable error envelope.

The ``/healthz`` and ``/readyz`` probes are deliberately bypassed so the
Kubernetes liveness / readiness checks never depend on identity headers.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from custos_callctx import CallContextVerifier, InvalidCallContextError
from custos_callctx.execution import ExecutionIdentity
from custos_callctx.mesh import MeshAuthenticationError, require_service_caller
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from starlette.types import ASGIApp

#: Wire header carrying the workspace identifier.
WORKSPACE_HEADER: str = "x-custos-workspace"

#: Wire header carrying the principal identifier.
PRINCIPAL_HEADER: str = "x-custos-principal"

#: Paths the middleware deliberately bypasses (no identity on probes).
_BYPASS_PATHS: frozenset[str] = frozenset({"/healthz", "/readyz"})
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CallContext:
    """Per-request authorization context attached to ``request.state``.

    Both fields are optional. In dev mode the middleware constructs a
    ``CallContext(workspace=None, principal=None)`` when the headers are
    absent so unauthenticated probes still flow through. Production mode
    short-circuits with a 401 before ``request.state.call_context`` is
    set, so a non-bypass route that reaches a handler in production is
    guaranteed to see both fields populated.
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
    """Extract workspace + principal headers onto ``request.state``.

    Args:
        app: The downstream ASGI application (FastAPI passes itself).
        require_call_context: When true, missing
            ``X-Custos-Workspace`` or ``X-Custos-Principal`` makes the
            middleware return ``401`` with a stable error envelope.
            When false (the dev default), missing headers yield a
            :class:`CallContext` with ``None`` fields.

    The constructor is side-effect free so :func:`create_app` can install
    the middleware without contacting a network. The flag is read from
    ``WF_REQUIRE_CALL_CONTEXT`` exactly once at app-construction time
    inside :func:`create_app`; tests pass the flag explicitly.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        require_call_context: bool,
        execution_verifier: CallContextVerifier | None = None,
        app_api_token: str | None = None,
        trigger_namespace: str | None = None,
        trigger_app_id: str = "trigger-service",
    ) -> None:
        super().__init__(app)
        self._require = require_call_context
        self._execution_verifier = execution_verifier
        self._app_api_token = app_api_token
        self._trigger_namespace = trigger_namespace
        self._trigger_app_id = trigger_app_id

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        if request.url.path in _BYPASS_PATHS:
            return await call_next(request)

        execution_route = request.method == "POST" and (
            request.url.path in {"/internal/runs:start", "/internal/runs:raiseEvent"}
            or re.fullmatch(r"/internal/runs/.+/steps/[^/]+:raiseEvent", request.url.path)
            is not None
        )
        if self._execution_verifier is not None and execution_route:
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
                or "workflow:execute" not in verified.permissions
                or verified.caller_component != self._trigger_app_id
                or verified.execution_operation
                != (
                    "workflow-start"
                    if request.url.path == "/internal/runs:start"
                    else "workflow-resume"
                )
            ):
                return _envelope(403, "execution_denied", "scoped execution permission required")
            request.state.call_context = CallContext(
                verified.workspace_id,
                verified.acting_principal_id,
                execution_verified=True,
            )
            return await call_next(request)

        workspace = request.headers.get(WORKSPACE_HEADER)
        principal = request.headers.get(PRINCIPAL_HEADER)

        if self._require:
            missing = [
                name
                for name, value in (
                    (WORKSPACE_HEADER, workspace),
                    (PRINCIPAL_HEADER, principal),
                )
                if not value
            ]
            if missing:
                return _envelope(
                    401,
                    "callctx_missing",
                    f"missing required header(s): {', '.join(missing)}",
                )

        request.state.call_context = CallContext(
            workspace=workspace or None,
            principal=principal or None,
        )
        return await call_next(request)


__all__ = [
    "PRINCIPAL_HEADER",
    "WORKSPACE_HEADER",
    "CallContext",
    "CallContextMiddleware",
]

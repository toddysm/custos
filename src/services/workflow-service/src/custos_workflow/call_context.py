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

from dataclasses import dataclass
from typing import TYPE_CHECKING

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
    ) -> None:
        super().__init__(app)
        self._require = require_call_context

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        if request.url.path in _BYPASS_PATHS:
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

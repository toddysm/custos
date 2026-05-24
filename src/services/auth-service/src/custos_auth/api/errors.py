"""Manager-exception → HTTP envelope mapping for auth-service.

Every error a route handler can raise is registered here. The handlers
all emit the same ``{"error": {"code", "detail", "issues?"}}`` envelope
used by the call-context middleware so clients see one error shape
regardless of which layer produced the response.

Phase C surfaces three logical failure modes:

* :class:`NotFound` (404) — tenant / workspace / principal lookup miss
  **or** cross-tenant read attempt (collapsed to 404 to avoid leaking
  existence; see ``design/components/auth-service/design.md`` §
  Authorization).
* :class:`Conflict` (409) — duplicate tenant / workspace / principal
  identifiers, write-once OIDC binding collisions, and other
  immutability/write-once conflicts surfaced by the service layer.
* :class:`ValidationFailure` (400) — invalid request body or
  business-rule violation that FastAPI's Pydantic validator could not
  catch (e.g. workspace creation under a disabled tenant).
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import JSONResponse

from custos_auth.middleware.callctx import (
    CallContextError,
    call_context_error_handler,
)


def _envelope(
    status_code: int,
    code: str,
    detail: str,
    *,
    issues: list[dict[str, Any]] | None = None,
) -> JSONResponse:
    body: dict[str, Any] = {"code": code, "detail": detail}
    if issues is not None:
        body["issues"] = issues
    return JSONResponse(status_code=status_code, content={"error": body})


# ---------------------------------------------------------------------------
# Exception types
# ---------------------------------------------------------------------------


class AuthApiError(Exception):
    """Base class for auth-service HTTP-rendered errors.

    Subclasses pin a ``status_code`` and ``code`` (machine-readable
    error code in the envelope).
    """

    status_code: int = 500
    code: str = "internal_error"

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class NotFound(AuthApiError):
    """404 — entity does not exist or caller cannot see it.

    Cross-tenant reads collapse to NotFound to avoid leaking existence.
    """

    status_code = 404
    code = "not_found"


class Conflict(AuthApiError):
    """409 — duplicate identifier or write-once violation."""

    status_code = 409
    code = "conflict"


class ValidationFailure(AuthApiError):
    """400 — business-rule violation not catchable by Pydantic alone."""

    status_code = 400
    code = "invalid_request"


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


async def handle_auth_api_error(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, AuthApiError)
    return _envelope(exc.status_code, exc.code, exc.detail)


async def handle_validation_error(
    _request: Request,
    exc: Exception,
) -> JSONResponse:
    """Render FastAPI's Pydantic body-validation errors.

    Pydantic's per-field errors are serialised into the envelope's
    ``issues`` list so clients see *which* fields failed and why,
    matching catalog-service's response shape.
    """
    assert isinstance(exc, RequestValidationError)
    issues: list[dict[str, Any]] = []
    for err in exc.errors():
        # ``loc`` may include non-JSON-serialisable types; coerce to str.
        loc = [str(p) for p in err.get("loc", [])]
        issues.append(
            {
                "loc": loc,
                "msg": err.get("msg", "invalid value"),
                "type": err.get("type", "value_error"),
            }
        )
    return _envelope(
        422,
        "request_validation_failed",
        "one or more request fields are invalid",
        issues=issues,
    )


async def handle_http_exception(
    _request: Request,
    exc: Exception,
) -> JSONResponse:
    """Render Starlette's ``HTTPException`` through the shared envelope.

    Without this, ``raise HTTPException(404)`` would emit the default
    Starlette ``{"detail": "Not Found"}`` shape — wrong for clients
    that key on ``error.code``.
    """
    assert isinstance(exc, StarletteHTTPException)
    detail = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
    return _envelope(exc.status_code, "http_error", detail)


def register_exception_handlers(app: FastAPI) -> None:
    """Install every exception handler the routes rely on.

    Pair with the :class:`CallContextMiddleware` to get one consistent
    envelope for every failure mode the service can produce.
    """
    app.add_exception_handler(CallContextError, call_context_error_handler)
    app.add_exception_handler(AuthApiError, handle_auth_api_error)
    app.add_exception_handler(NotFound, handle_auth_api_error)
    app.add_exception_handler(Conflict, handle_auth_api_error)
    app.add_exception_handler(ValidationFailure, handle_auth_api_error)
    app.add_exception_handler(RequestValidationError, handle_validation_error)
    app.add_exception_handler(StarletteHTTPException, handle_http_exception)


__all__ = [
    "AuthApiError",
    "Conflict",
    "NotFound",
    "ValidationFailure",
    "register_exception_handlers",
]

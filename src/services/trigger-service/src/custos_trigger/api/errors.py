"""RFC 7807 ``application/problem+json`` envelope + exception handlers.

The Trigger Service REST surface (TS-IMPL-015) reports every non-2xx outcome
through a single, uniform Problem Details body so the SDK never sees FastAPI's
default error shape. The envelope mirrors RFC 7807 § 3.1 with one extension —
``code`` — the structured ``kind`` string clients branch on (the ``type`` URI
MAY change without bumping ``code``).

Two families of ``kind`` flow through here:

* The locked ``trigger.*`` domain taxonomy from
  :mod:`custos_trigger.errors` (:class:`~custos_trigger.errors.TriggerError`).
* A small set of route-local ``trigger.api.*`` kinds for HTTP-shaped outcomes
  that are not domain failures (a malformed request body; a manual fire that
  matched no fireable subscription). These live only in this module's status
  table — they are deliberately *not* members of the locked
  :class:`~custos_trigger.errors.TriggerErrorKind` enum, mirroring the
  workflow-service ``workflow.api.*`` convention.
"""

from __future__ import annotations

from typing import Any, Final

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from custos_trigger.errors import TriggerError

__all__ = [
    "LOCKED_API_KINDS",
    "LOCKED_API_KIND_TO_STATUS",
    "PROBLEM_MEDIA_TYPE",
    "PROBLEM_TYPE_PREFIX",
    "ProblemDetail",
    "problem_response",
    "register_exception_handlers",
]

#: Wire content type for the envelope (RFC 7807 § 3).
PROBLEM_MEDIA_TYPE: Final[str] = "application/problem+json"

#: Absolute-URI prefix the ``type`` field is derived from: the ``kind`` with
#: dots replaced by slashes, e.g. ``trigger.selector_invalid`` becomes
#: ``https://errors.custos.dev/trigger/selector_invalid``. Clients SHOULD treat
#: the URI as opaque and branch on ``code`` instead.
PROBLEM_TYPE_PREFIX: Final[str] = "https://errors.custos.dev/"

#: Route-local ``trigger.api.*`` kinds. Unlike the locked domain taxonomy these
#: describe HTTP-shaped outcomes, not domain failures, so they are scoped to the
#: API layer.
API_BAD_REQUEST: Final[str] = "trigger.api.bad_request"
API_SUBSCRIPTION_NOT_FIREABLE: Final[str] = "trigger.api.subscription_not_fireable"

#: Kind-string → HTTP status. The keys form the closed set the envelope may
#: emit; a kind absent from this table can never reach the wire (``from_kind``
#: raises ``KeyError``), so an undocumented failure is a loud bug, not a 500
#: with a leaked stack trace.
LOCKED_API_KIND_TO_STATUS: Final[dict[str, int]] = {
    # Domain taxonomy (custos_trigger.errors.TriggerErrorKind)
    "trigger.subscription_not_found": 404,
    "trigger.selector_invalid": 422,
    "trigger.selector_type_error": 422,
    "trigger.dispatch_failed": 502,
    "trigger.dedup_duplicate": 409,
    "trigger.loop_detected": 409,
    # Route-local API kinds
    API_BAD_REQUEST: 400,
    API_SUBSCRIPTION_NOT_FIREABLE: 409,
}

#: Frozen view for fast membership tests.
LOCKED_API_KINDS: Final[frozenset[str]] = frozenset(LOCKED_API_KIND_TO_STATUS)

#: Short, English-only human-readable title per kind.
_TITLE_FOR_KIND: Final[dict[str, str]] = {
    "trigger.subscription_not_found": "Subscription not found",
    "trigger.selector_invalid": "Selector failed to compile",
    "trigger.selector_type_error": "Selector did not evaluate to a boolean",
    "trigger.dispatch_failed": "Dispatch to the Workflow Service failed",
    "trigger.dedup_duplicate": "Duplicate request",
    "trigger.loop_detected": "Fan-out loop detected",
    API_BAD_REQUEST: "Bad request",
    API_SUBSCRIPTION_NOT_FIREABLE: "Subscription is not fireable",
}


def _type_uri_for_kind(kind: str) -> str:
    """Render the RFC 7807 ``type`` URI for ``kind`` (dots → path segments)."""
    return f"{PROBLEM_TYPE_PREFIX}{kind.replace('.', '/')}"


class ProblemDetail(BaseModel):
    """Wire shape of the ``application/problem+json`` envelope (RFC 7807 § 3.1).

    The ``code`` extension carries the structured ``kind`` string — the
    canonical machine-readable selector for client branch logic. Per-kind
    extension fields (``subscriptionId`` …) are allowed via ``extra="allow"``
    and populated only when known.
    """

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    type: str = Field(..., description="Absolute URI identifying the problem type.")
    title: str = Field(..., description="Short human-readable summary.")
    status: int = Field(..., ge=100, le=599, description="HTTP status echoed into the body.")
    detail: str = Field(..., description="Long human-readable explanation.")
    instance: str | None = Field(default=None, description="Request path for correlation.")
    code: str = Field(..., description="Structured ``kind`` selector.")

    @classmethod
    def from_kind(
        cls,
        kind: str,
        *,
        detail: str,
        instance: str | None,
        extras: dict[str, Any] | None = None,
    ) -> ProblemDetail:
        """Build a :class:`ProblemDetail` from a kind in :data:`LOCKED_API_KINDS`.

        Raises:
            KeyError: When ``kind`` is not in the locked table, so an
                undocumented kind can never escape onto the wire.
        """
        status = LOCKED_API_KIND_TO_STATUS[kind]
        payload: dict[str, Any] = {
            "type": _type_uri_for_kind(kind),
            "title": _TITLE_FOR_KIND[kind],
            "status": status,
            "detail": detail,
            "instance": instance,
            "code": kind,
        }
        if extras:
            for name, value in extras.items():
                if value is None:
                    continue
                payload[name] = value
        return cls.model_validate(payload)


def problem_response(
    request: Request,
    *,
    kind: str,
    detail: str,
    extras: dict[str, Any] | None = None,
) -> JSONResponse:
    """Materialise a :class:`ProblemDetail` into a Problem+JSON response.

    ``model_dump(exclude_none=True)`` keeps the envelope minimal so extension
    fields that were not populated never appear.
    """
    problem = ProblemDetail.from_kind(kind, detail=detail, instance=request.url.path, extras=extras)
    return JSONResponse(
        status_code=problem.status,
        media_type=PROBLEM_MEDIA_TYPE,
        content=problem.model_dump(exclude_none=True),
    )


async def _trigger_error_handler(request: Request, exc: Exception) -> JSONResponse:
    """Render a :class:`TriggerError` through the Problem+JSON envelope."""
    assert isinstance(exc, TriggerError)
    return problem_response(
        request,
        kind=exc.kind.value,
        detail=exc.message,
        extras=exc.details or None,
    )


async def _validation_error_handler(request: Request, exc: Exception) -> JSONResponse:
    """Render a request-validation rejection as ``trigger.api.bad_request``."""
    assert isinstance(exc, RequestValidationError)
    errors = exc.errors()
    if errors:
        first = errors[0]
        loc = ".".join(str(part) for part in first.get("loc", ()))
        detail = f"{loc}: {first.get('msg', 'invalid request')}".lstrip(": ")
    else:
        detail = "invalid request"
    return problem_response(request, kind=API_BAD_REQUEST, detail=detail)


def register_exception_handlers(app: FastAPI) -> None:
    """Register the Problem+JSON handlers on ``app``.

    Wires :class:`TriggerError` (the locked domain taxonomy) and FastAPI's
    :class:`RequestValidationError` (malformed bodies / params) onto the
    uniform envelope. Call-context 4xx failures keep their own envelope via
    :func:`custos_trigger.middleware.call_context_error_handler`.
    """
    app.add_exception_handler(TriggerError, _trigger_error_handler)
    app.add_exception_handler(RequestValidationError, _validation_error_handler)

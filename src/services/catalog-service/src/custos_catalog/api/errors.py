"""Single source of truth for the manager-exception → HTTP envelope mapping.

Every error a manager can raise is registered here. The handlers all
emit the same ``{"error": {"code", "detail", "issues?"}}`` envelope used
by the call-context middleware (CS-IMPL-004) so clients see a single
error shape regardless of which layer produced the response.
"""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import JSONResponse

from custos_catalog.clients.connector import ConnectorServiceUnavailable
from custos_catalog.managers.activity_registry import (
    ActivityManifestError,
    ActivityNamespaceError,
    ActivityRegistryConflict,
    ActivityRegistryError,
    ActivityTypeNotFound,
)
from custos_catalog.managers.connector_registry import (
    ConnectorManifestError,
    ConnectorRegistryConflict,
    ConnectorRegistryError,
    ConnectorTypeNotFound,
)
from custos_catalog.managers.definition import (
    PublishValidationError,
    WorkflowNotFound,
)
from custos_catalog.managers.template import (
    ExtractionError,
    MaterializationError,
    TemplateNotFound,
)
from custos_catalog.versioning import (
    TemplateImmutabilityError,
    WorkflowImmutabilityError,
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


def _issues_from_dataclasses(items: list[Any]) -> list[dict[str, Any]]:
    """Render a list of frozen-dataclass issue records to plain dicts."""
    result: list[dict[str, Any]] = []
    for item in items:
        if is_dataclass(item) and not isinstance(item, type):
            result.append(asdict(item))
        elif isinstance(item, dict):
            result.append(dict(item))
        else:  # pragma: no cover - defensive
            result.append({"message": str(item)})
    return result


# ---------------------------------------------------------------------------
# Handlers — one per exception class
# ---------------------------------------------------------------------------


async def handle_publish_validation_error(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, PublishValidationError)
    return _envelope(
        status_code=400,
        code=f"catalog.publish.{exc.stage}",
        detail=str(exc),
        issues=_issues_from_dataclasses(exc.issues),
    )


async def handle_connector_service_unavailable(
    _request: Request,
    exc: Exception,
) -> JSONResponse:
    """Map :class:`ConnectorServiceUnavailable` to a 503 envelope.

    Connector Service is the only outbound dependency at workflow
    publish time; when it is unreachable or returns 5xx the publish
    cannot proceed and the caller may retry. Per design § Failure
    Modes (catalog-service / CS-IMPL-023) we surface this as a 503
    with code ``catalog.dependency_unavailable`` so operators (and
    SDKs) distinguish a transient infra fault from the 4xx publish
    rejections handled by :func:`handle_publish_validation_error`.
    """
    assert isinstance(exc, ConnectorServiceUnavailable)
    return _envelope(
        status_code=503,
        code=exc.code,
        detail=str(exc),
    )


async def handle_workflow_not_found(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, WorkflowNotFound)
    return _envelope(
        status_code=404,
        code="catalog.workflow_not_found",
        detail=str(exc),
    )


async def handle_template_not_found(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, TemplateNotFound)
    return _envelope(
        status_code=404,
        code="catalog.template_not_found",
        detail=str(exc),
    )


async def handle_extraction_error(_request: Request, exc: Exception) -> JSONResponse:
    """Translate :class:`ExtractionError`.

    The wrapped cause carries the granular detail. We surface the
    cause's class name as the trailing fragment of the code so clients
    can branch on it; 400 is the right status because every cause is a
    caller-supplied input failure (bad selectors, round-trip mismatch,
    publish validation).
    """
    assert isinstance(exc, ExtractionError)
    cause = exc.cause
    cause_name = type(cause).__name__
    issues: list[dict[str, Any]] | None = None
    if isinstance(cause, PublishValidationError):
        issues = _issues_from_dataclasses(cause.issues)
    return _envelope(
        status_code=400,
        code=f"catalog.template_extract_failed.{cause_name}",
        detail=str(exc),
        issues=issues,
    )


async def handle_materialization_error(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, MaterializationError)
    cause = exc.cause
    cause_name = type(cause).__name__
    issues: list[dict[str, Any]] | None = None
    if isinstance(cause, PublishValidationError):
        issues = _issues_from_dataclasses(cause.issues)
    return _envelope(
        status_code=400,
        code=f"catalog.template_materialization_failed.{cause_name}",
        detail=str(exc),
        issues=issues,
    )


async def handle_workflow_immutability_error(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, WorkflowImmutabilityError)
    return _envelope(
        status_code=409,
        code="catalog.workflow_immutability_violation",
        detail=str(exc),
    )


async def handle_template_immutability_error(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, TemplateImmutabilityError)
    return _envelope(
        status_code=409,
        code="catalog.template_immutability_violation",
        detail=str(exc),
    )


# ----- activity registry ----------------------------------------------------


async def handle_activity_manifest_error(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, ActivityManifestError)
    return _envelope(
        status_code=400,
        code="catalog.activity_manifest_invalid",
        detail=str(exc),
        issues=_issues_from_dataclasses(exc.issues),
    )


async def handle_activity_namespace_error(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, ActivityNamespaceError)
    return _envelope(
        status_code=403,
        code="catalog.activity_namespace_forbidden",
        detail=str(exc),
    )


async def handle_activity_registry_conflict(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, ActivityRegistryConflict)
    return _envelope(
        status_code=409,
        code="catalog.activity_type_digest_conflict",
        detail=str(exc),
        issues=[
            {
                "namespace": exc.namespace,
                "type": exc.type,
                "version": exc.version,
                "suppliedDigest": exc.supplied_digest,
                "storedDigest": exc.stored_digest,
            },
        ],
    )


async def handle_activity_type_not_found(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, ActivityTypeNotFound)
    return _envelope(
        status_code=404,
        code="catalog.activity_type_not_found",
        detail=str(exc),
    )


async def handle_activity_registry_error(_request: Request, exc: Exception) -> JSONResponse:
    """Catch-all for any future :class:`ActivityRegistryError` subclass.

    Subclasses with their own handler registered above are dispatched
    first by FastAPI's MRO-walking handler resolver — this handler is
    only used for the base class itself. Keeping it explicit means a
    new subclass without a handler returns a clean 500 instead of a
    bare ``Internal Server Error`` page.
    """
    assert isinstance(exc, ActivityRegistryError)
    return _envelope(
        status_code=500,
        code="catalog.activity_registry_internal_error",
        detail=str(exc),
    )


# ----- connector registry ---------------------------------------------------


async def handle_connector_manifest_error(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, ConnectorManifestError)
    return _envelope(
        status_code=400,
        code="catalog.connector_manifest_invalid",
        detail=str(exc),
        issues=_issues_from_dataclasses(exc.issues),
    )


async def handle_connector_registry_conflict(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, ConnectorRegistryConflict)
    return _envelope(
        status_code=409,
        code="catalog.connector_type_digest_conflict",
        detail=str(exc),
        issues=[
            {
                "type": exc.type,
                "version": exc.version,
                "suppliedDigest": exc.supplied_digest,
                "storedDigest": exc.stored_digest,
            },
        ],
    )


async def handle_connector_type_not_found(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, ConnectorTypeNotFound)
    return _envelope(
        status_code=404,
        code="catalog.connector_type_not_found",
        detail=str(exc),
    )


async def handle_connector_registry_error(_request: Request, exc: Exception) -> JSONResponse:
    assert isinstance(exc, ConnectorRegistryError)
    return _envelope(
        status_code=500,
        code="catalog.connector_registry_internal_error",
        detail=str(exc),
    )


# ----- request body validation ---------------------------------------------


async def handle_request_validation_error(_request: Request, exc: Exception) -> JSONResponse:
    """Translate Pydantic body / query validation failures to the envelope."""
    assert isinstance(exc, RequestValidationError)
    issues: list[dict[str, Any]] = []
    for err in exc.errors():
        issues.append(
            {
                "loc": list(err.get("loc", ())),
                "code": err.get("type", "value_error"),
                "message": err.get("msg", "invalid"),
            },
        )
    return _envelope(
        status_code=422,
        code="catalog.request_invalid",
        detail="request body or parameters failed validation",
        issues=issues,
    )


# ----- HTTPException pass-through ------------------------------------------


async def handle_http_exception(_request: Request, exc: Exception) -> JSONResponse:
    """Honour the envelope shape when a route raises ``HTTPException``.

    Route handlers that need to short-circuit (malformed path tokens,
    405 on a shadowed sub-route, etc.) raise ``HTTPException`` with
    ``detail`` already in envelope shape (``{"error": {...}}``). The
    default FastAPI exception handler would wrap that under another
    ``detail`` key — this handler unwraps so clients see the same
    ``{"error": {...}}`` envelope as every other failure.
    """
    assert isinstance(exc, StarletteHTTPException)
    detail = exc.detail
    if isinstance(detail, dict) and "error" in detail:
        return JSONResponse(status_code=exc.status_code, content=detail)
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": f"catalog.http_{exc.status_code}",
                "detail": str(detail),
            },
        },
    )


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------


def register_exception_handlers(app: FastAPI) -> None:
    """Install every Catalog manager-error handler on ``app``.

    Subclass handlers must come **before** their base class so the
    FastAPI dispatcher (which walks MRO) chooses the more specific
    handler. Order matters here.
    """
    # ----- workflow / template managers -----
    app.add_exception_handler(PublishValidationError, handle_publish_validation_error)
    app.add_exception_handler(ConnectorServiceUnavailable, handle_connector_service_unavailable)
    app.add_exception_handler(WorkflowNotFound, handle_workflow_not_found)
    app.add_exception_handler(TemplateNotFound, handle_template_not_found)
    app.add_exception_handler(ExtractionError, handle_extraction_error)
    app.add_exception_handler(MaterializationError, handle_materialization_error)
    app.add_exception_handler(WorkflowImmutabilityError, handle_workflow_immutability_error)
    app.add_exception_handler(TemplateImmutabilityError, handle_template_immutability_error)

    # ----- activity registry: subclasses before the base -----
    app.add_exception_handler(ActivityManifestError, handle_activity_manifest_error)
    app.add_exception_handler(ActivityNamespaceError, handle_activity_namespace_error)
    app.add_exception_handler(ActivityRegistryConflict, handle_activity_registry_conflict)
    app.add_exception_handler(ActivityTypeNotFound, handle_activity_type_not_found)
    app.add_exception_handler(ActivityRegistryError, handle_activity_registry_error)

    # ----- connector registry: subclasses before the base -----
    app.add_exception_handler(ConnectorManifestError, handle_connector_manifest_error)
    app.add_exception_handler(ConnectorRegistryConflict, handle_connector_registry_conflict)
    app.add_exception_handler(ConnectorTypeNotFound, handle_connector_type_not_found)
    app.add_exception_handler(ConnectorRegistryError, handle_connector_registry_error)

    # ----- generic request validation -----
    app.add_exception_handler(RequestValidationError, handle_request_validation_error)
    app.add_exception_handler(StarletteHTTPException, handle_http_exception)


__all__ = ["register_exception_handlers"]

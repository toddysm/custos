"""RFC 7807 ``application/problem+json`` envelope + locked exception handlers.

This module implements WF-IMPL-061 (issue #447) — the public-API
half of the workflow-service error contract. Every error a
:mod:`custos_workflow.runs.errors` Run Controller method or a
:mod:`custos_workflow.validator.errors` Validator method may
raise is translated here into a single
``application/problem+json`` envelope.

The locked taxonomy (8 kinds; the last is a catch-all):

* ``workflow.run_not_found`` (404) — :class:`RunNotFoundError`
* ``workflow.run_state_conflict`` (409) — :class:`RunStateConflictError`
* ``workflow.workflow_runtime_unavailable`` (503) —
  :class:`WorkflowRuntimeUnavailableError`
* ``workflow.validator.workflow_version_not_found`` (404) —
  :class:`WorkflowVersionNotFoundError`
* ``workflow.validator.inputs_schema_error`` (422) —
  :class:`InputsSchemaError`
* ``workflow.validator.idempotency_conflict`` (409) —
  :class:`IdempotencyConflictError`
* ``workflow.validator.workspace_unauthorized`` (403) —
  :class:`WorkspaceUnauthorizedError`
* ``workflow.step_not_found`` (404) — surfaced by REST routes that
  fetch a single step (WF-IMPL-066) when the persisted run has
  no compiled step with the requested id.
* ``workflow.api.not_implemented`` (501) — surfaced by routes that
  ship as a documented stub (today: the step log-stream endpoint;
  see WF-IMPL-066) until a follow-on sub-module lands the real
  behaviour.
* ``workflow.api.bad_request`` (400 / 422) — catch-all for
  :exc:`RequestValidationError` and :class:`StarletteHTTPException`

The mapping is exported as :data:`LOCKED_API_KIND_TO_STATUS` and the
set of kinds as :data:`LOCKED_API_KINDS`. Subsequent API-Adapter
tasks (WF-IMPL-064..072) and the dev docs (WF-IMPL-072) consume the
table verbatim; the test suite asserts every concrete error class
maps into the table so adding a new kind without an entry fails the
build loudly.

The envelope is a strict superset of RFC 7807:

* ``type`` — absolute URI under :data:`PROBLEM_TYPE_PREFIX` derived
  from the kind (dots → slashes); clients SHOULD treat the URI as
  opaque and key off ``code`` for branch logic.
* ``title`` — short human-readable summary (kind-derived).
* ``status`` — HTTP status code mirroring the response status.
* ``detail`` — long human-readable explanation; mirrors
  ``error.message``.
* ``instance`` — request path ``request.url.path`` for correlation.
* ``code`` — the structured ``kind`` string (extension field; the
  canonical machine-readable selector for branch logic).
* Per-kind extension fields (``runId``, ``workflowId``,
  ``workflowVersion``, ``workspaceId``, ``idempotencyKey``,
  ``validation``, ``principal`` …) populated only when known.

``register_exception_handlers`` installs every handler on a FastAPI
app. It is idempotent: calling it twice is a no-op (used by both
:func:`custos_workflow.app.create_app` and the WF-IMPL-071 test
fixtures).

See the issue: https://github.com/toddysm/custos/issues/447
"""

from __future__ import annotations

from typing import Any, Final

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ConfigDict, Field
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import JSONResponse

from custos_workflow.runs.errors import (
    RunControllerError,
    RunNotFoundError,
    RunStateConflictError,
    WorkflowRuntimeUnavailableError,
)
from custos_workflow.validator.errors import (
    IdempotencyConflictError,
    InputsSchemaError,
    ValidatorError,
    WorkflowVersionNotFoundError,
    WorkspaceUnauthorizedError,
)

__all__ = [
    "LOCKED_API_KINDS",
    "LOCKED_API_KIND_TO_STATUS",
    "PROBLEM_MEDIA_TYPE",
    "PROBLEM_TYPE_PREFIX",
    "ProblemDetail",
    "problem_response",
    "register_exception_handlers",
]


#: Wire content type for the envelope. Per RFC 7807 § 3.
PROBLEM_MEDIA_TYPE: Final[str] = "application/problem+json"

#: Absolute-URI prefix used to derive the ``type`` field from a
#: ``kind`` string. The path segment is the kind with dots replaced
#: by slashes so ``workflow.validator.inputs_schema_error`` becomes
#: ``https://errors.custos.dev/workflow/validator/inputs_schema_error``.
#: Clients SHOULD treat the URI as opaque — branch logic keys off
#: ``code`` (the structured kind string).
PROBLEM_TYPE_PREFIX: Final[str] = "https://errors.custos.dev/"


# ---------------------------------------------------------------------------
# Locked taxonomy table
# ---------------------------------------------------------------------------


#: Kind-string → HTTP status mapping. Mirrors the table in the
#: module docstring. The keys form the closed set
#: :data:`LOCKED_API_KINDS`. Adding a new kind requires adding an
#: entry here AND adding a handler below AND extending the
#: WF-IMPL-061 test suite.
LOCKED_API_KIND_TO_STATUS: Final[dict[str, int]] = {
    # Run Controller (custos_workflow.runs.errors)
    "workflow.run_not_found": 404,
    "workflow.run_state_conflict": 409,
    "workflow.workflow_runtime_unavailable": 503,
    # Validator (custos_workflow.validator.errors)
    "workflow.validator.workflow_version_not_found": 404,
    "workflow.validator.inputs_schema_error": 422,
    "workflow.validator.idempotency_conflict": 409,
    "workflow.validator.workspace_unauthorized": 403,
    # REST step-fetch (WF-IMPL-066) — the persisted run carries no
    # compiled step with the requested id.
    "workflow.step_not_found": 404,
    # Documented stub routes that ship before the real handler
    # lands (WF-IMPL-066: step log streaming; see module docstring).
    "workflow.api.not_implemented": 501,
    # Catch-all for FastAPI request-body / query validation
    # rejections (Pydantic) — keeps the wire shape uniform so the
    # SDK never sees the raw FastAPI default envelope.
    "workflow.api.bad_request": 400,
}

#: Frozen set view of the table for fast membership tests.
LOCKED_API_KINDS: Final[frozenset[str]] = frozenset(LOCKED_API_KIND_TO_STATUS)


#: Short human-readable title per kind. Subsequent tasks may
#: localize this; today it is English-only by design.
_TITLE_FOR_KIND: Final[dict[str, str]] = {
    "workflow.run_not_found": "Run not found",
    "workflow.run_state_conflict": "Run state conflict",
    "workflow.workflow_runtime_unavailable": "Workflow runtime unavailable",
    "workflow.validator.workflow_version_not_found": "Workflow version not found",
    "workflow.validator.inputs_schema_error": "Inputs failed schema validation",
    "workflow.validator.idempotency_conflict": "Idempotency key conflict",
    "workflow.validator.workspace_unauthorized": "Workspace access denied",
    "workflow.step_not_found": "Step not found",
    "workflow.api.not_implemented": "Not implemented",
    "workflow.api.bad_request": "Bad request",
}


def _type_uri_for_kind(kind: str) -> str:
    """Render the RFC 7807 ``type`` URI for ``kind``.

    Dots are mapped to slashes so the URI's path mirrors the kind
    hierarchy. The prefix is :data:`PROBLEM_TYPE_PREFIX`.
    """
    return f"{PROBLEM_TYPE_PREFIX}{kind.replace('.', '/')}"


# ---------------------------------------------------------------------------
# Wire model
# ---------------------------------------------------------------------------


class ProblemDetail(BaseModel):
    """Wire shape of the ``application/problem+json`` envelope.

    Mirrors RFC 7807 § 3.1 with two extensions:

    * ``code`` — the structured ``kind`` string. The canonical
      machine-readable selector for client branch logic (per the
      WF-IMPL-061 contract; the ``type`` URI MAY change in
      future without bumping the code).
    * Per-kind extension fields (``runId``, ``workflowId``,
      ``workflowVersion``, ``workspaceId``, ``idempotencyKey``,
      ``validation``, ``currentStatus``, ``attemptedStatus``,
      ``principal`` …). All optional; populated only when known.

    The model is constructed via :meth:`from_error` so callers
    cannot accidentally emit an envelope whose ``status`` /
    ``code`` pair is not in the locked table.
    """

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    type: str = Field(
        ...,
        description="Absolute URI identifying the problem type (RFC 7807 § 3.1).",
    )
    title: str = Field(
        ...,
        description="Short human-readable summary of the problem (RFC 7807 § 3.1).",
    )
    status: int = Field(
        ...,
        description="HTTP status code echoed into the body (RFC 7807 § 3.1).",
        ge=100,
        le=599,
    )
    detail: str = Field(
        ...,
        description="Long human-readable explanation (RFC 7807 § 3.1).",
    )
    instance: str | None = Field(
        default=None,
        description="Request path for correlation (RFC 7807 § 3.1, optional).",
    )
    code: str = Field(
        ...,
        description="Structured ``kind`` selector — the canonical machine-readable id.",
    )

    @classmethod
    def from_kind(
        cls,
        kind: str,
        *,
        detail: str,
        instance: str | None,
        extras: dict[str, Any] | None = None,
    ) -> ProblemDetail:
        """Construct a :class:`ProblemDetail` from a locked ``kind``.

        Args:
            kind: One of :data:`LOCKED_API_KINDS`. Raises
                :class:`KeyError` otherwise so an undocumented
                kind never escapes onto the wire.
            detail: Long human-readable explanation. Surfaced
                verbatim as the ``detail`` field.
            instance: Request path for correlation; usually
                ``request.url.path``. ``None`` when no request
                context is available.
            extras: Optional per-kind extension fields. ``None``
                entries are dropped so the envelope stays
                minimal. Keys are CamelCase per the workflow-service
                public-API style (``runId``, ``workflowVersion`` …).
        """
        status = LOCKED_API_KIND_TO_STATUS[kind]
        title = _TITLE_FOR_KIND[kind]
        payload: dict[str, Any] = {
            "type": _type_uri_for_kind(kind),
            "title": title,
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


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _problem_response(
    *,
    kind: str,
    detail: str,
    instance: str | None,
    extras: dict[str, Any] | None = None,
) -> JSONResponse:
    """Materialise a :class:`ProblemDetail` into a :class:`JSONResponse`.

    The response uses :data:`PROBLEM_MEDIA_TYPE` so clients honour
    the RFC 7807 contract. ``model_dump(exclude_none=True)`` keeps
    the wire envelope minimal — extension fields that were not
    populated never appear.
    """
    status = LOCKED_API_KIND_TO_STATUS[kind]
    problem = ProblemDetail.from_kind(kind, detail=detail, instance=instance, extras=extras)
    return JSONResponse(
        status_code=status,
        media_type=PROBLEM_MEDIA_TYPE,
        content=problem.model_dump(exclude_none=True),
    )


#: Public alias of :func:`_problem_response` for route modules that
#: render envelopes for kinds without a dedicated exception class
#: (e.g. ``workflow.step_not_found``, ``workflow.api.not_implemented``).
#: Keeping the original underscore-prefixed name as the
#: implementation symbol preserves the previously-shipped private
#: contract; route code SHOULD reach for :func:`problem_response`.
problem_response = _problem_response


def _instance_for(request: Request) -> str:
    """Pull the request path off ``request`` for the ``instance`` field.

    The URL path (without query string) is the most useful
    correlation key — query strings often carry idempotency keys
    or pagination cursors that bloat the envelope without
    correlating anything operationally useful.
    """
    return request.url.path


# ---------------------------------------------------------------------------
# Handlers — one per concrete exception class
# ---------------------------------------------------------------------------


async def handle_run_not_found(request: Request, exc: Exception) -> JSONResponse:
    """Translate :class:`RunNotFoundError` → ``workflow.run_not_found`` (404).

    The Run Controller's internal ``kind`` is ``run.not_found`` per the
    WF-IMPL-031 taxonomy (used by audit emission). The public-API
    contract surfaces the same condition under the namespaced kind
    ``workflow.run_not_found`` so SDK branch logic is unambiguous
    across services. The handler bridges the two namespaces here.
    """
    assert isinstance(exc, RunNotFoundError)
    return _problem_response(
        kind="workflow.run_not_found",
        detail=exc.message,
        instance=_instance_for(request),
        extras={"runId": exc.run_id},
    )


async def handle_run_state_conflict(request: Request, exc: Exception) -> JSONResponse:
    """Translate :class:`RunStateConflictError` → ``workflow.run_state_conflict`` (409)."""
    assert isinstance(exc, RunStateConflictError)
    return _problem_response(
        kind="workflow.run_state_conflict",
        detail=exc.message,
        instance=_instance_for(request),
        extras={
            "runId": exc.run_id,
            "currentStatus": exc.current_status,
            "attemptedStatus": exc.attempted_status,
        },
    )


async def handle_workflow_runtime_unavailable(request: Request, exc: Exception) -> JSONResponse:
    """Translate :class:`WorkflowRuntimeUnavailableError` → 503."""
    assert isinstance(exc, WorkflowRuntimeUnavailableError)
    return _problem_response(
        kind="workflow.workflow_runtime_unavailable",
        detail=exc.message,
        instance=_instance_for(request),
        extras={"runId": exc.run_id},
    )


async def handle_run_controller_error(request: Request, exc: Exception) -> JSONResponse:
    """Fallback for :class:`RunControllerError` subclasses without a dedicated handler.

    Today this only catches :class:`RunStateCorruptError` (which is
    a server-side data-integrity bug, not a contract surface): we
    emit the catch-all ``workflow.api.bad_request`` envelope at the
    locked status (400) so SDK clients still see a uniform envelope
    shape. The audit pipeline (LifecycleEvent publisher) captures
    the underlying ``kind`` separately, and the wire body preserves
    it under the ``underlyingKind`` extension so operators debugging
    from the response still see the precise failure mode.
    """
    assert isinstance(exc, RunControllerError)
    return _problem_response(
        kind="workflow.api.bad_request",
        detail=exc.message,
        instance=_instance_for(request),
        extras={"runId": exc.run_id, "underlyingKind": exc.kind},
    )


async def handle_workflow_version_not_found(request: Request, exc: Exception) -> JSONResponse:
    """Translate :class:`WorkflowVersionNotFoundError` → 404."""
    assert isinstance(exc, WorkflowVersionNotFoundError)
    return _problem_response(
        kind=exc.kind,
        detail=exc.message,
        instance=_instance_for(request),
        extras={
            "workspaceId": exc.workspace_id,
            "workflowId": exc.workflow_id,
            "workflowVersion": exc.workflow_version,
        },
    )


async def handle_inputs_schema_error(request: Request, exc: Exception) -> JSONResponse:
    """Translate :class:`InputsSchemaError` → 422 with the ``validation`` extension."""
    assert isinstance(exc, InputsSchemaError)
    return _problem_response(
        kind=exc.kind,
        detail=exc.message,
        instance=_instance_for(request),
        extras={
            "workspaceId": exc.workspace_id,
            "validation": list(exc.validation),
        },
    )


async def handle_idempotency_conflict(request: Request, exc: Exception) -> JSONResponse:
    """Translate :class:`IdempotencyConflictError` → 409."""
    assert isinstance(exc, IdempotencyConflictError)
    return _problem_response(
        kind=exc.kind,
        detail=exc.message,
        instance=_instance_for(request),
        extras={
            "workspaceId": exc.workspace_id,
            "idempotencyKey": exc.idempotency_key,
        },
    )


async def handle_workspace_unauthorized(request: Request, exc: Exception) -> JSONResponse:
    """Translate :class:`WorkspaceUnauthorizedError` → 403."""
    assert isinstance(exc, WorkspaceUnauthorizedError)
    return _problem_response(
        kind=exc.kind,
        detail=exc.message,
        instance=_instance_for(request),
        extras={
            "workspaceId": exc.workspace_id,
            "principal": exc.principal,
        },
    )


async def handle_validator_error(request: Request, exc: Exception) -> JSONResponse:
    """Fallback for :class:`ValidatorError` subclasses without a dedicated handler.

    Defensive: WF-IMPL-061 covers every concrete subclass listed
    in :data:`~custos_workflow.validator.errors.LOCKED_VALIDATOR_KINDS`.
    If a future task adds a new subclass without updating the
    handler registration table, this fallback keeps the wire
    envelope well-formed while the test suite (which asserts the
    table covers every subclass) flags the gap.
    """
    assert isinstance(exc, ValidatorError)
    return _problem_response(
        kind="workflow.api.bad_request",
        detail=exc.message,
        instance=_instance_for(request),
        extras={"workspaceId": exc.workspace_id, "underlyingKind": exc.kind},
    )


async def handle_request_validation_error(request: Request, exc: Exception) -> JSONResponse:
    """Translate Pydantic body / query validation failures to ``workflow.api.bad_request``.

    FastAPI's default handler returns the raw Pydantic error list
    under a ``detail`` key. We re-shape it into the RFC 7807
    envelope so SDK branch logic keys off ``code`` uniformly across
    every 4xx the workflow-service surfaces. The original
    Pydantic error records are preserved under ``validation`` so
    clients keep the field-level diagnostics.
    """
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
    return _problem_response(
        kind="workflow.api.bad_request",
        detail="request body or parameters failed validation",
        instance=_instance_for(request),
        extras={"validation": issues},
    )


async def handle_http_exception(request: Request, exc: Exception) -> JSONResponse:
    """Re-envelope any :class:`StarletteHTTPException` raised by route handlers.

    Route handlers that need to short-circuit (e.g. ``raise HTTPException(404)``
    for a path token Pydantic could not type-narrow) get a uniform
    envelope: ``code`` is always ``workflow.api.bad_request`` and the
    original ``exc.status_code`` is preserved verbatim in the body's
    ``status`` field (and the HTTP response status), so the SDK never
    sees the FastAPI default ``{"detail": "..."}`` shape regardless
    of whether the underlying status is 4xx or 5xx.
    """
    assert isinstance(exc, StarletteHTTPException)
    # Every StarletteHTTPException flows through the same envelope:
    # `code` is the catch-all `workflow.api.bad_request` and the
    # original `exc.status_code` is preserved verbatim on both the
    # HTTP response and the body's `status` field. The locked-table
    # status for the catch-all is 400 by default; this handler
    # bypasses :func:`_problem_response` so the HTTPException's
    # actual status is honoured (404, 405, 415, 500, …).
    status = exc.status_code
    detail = str(exc.detail) if exc.detail is not None else ""
    # We bypass _problem_response because the locked-table status
    # for ``workflow.api.bad_request`` is 400; HTTPException carries
    # its own status (404, 405, 415, …) which we honour verbatim.
    problem = ProblemDetail.model_validate(
        {
            "type": _type_uri_for_kind("workflow.api.bad_request"),
            "title": _TITLE_FOR_KIND["workflow.api.bad_request"],
            "status": status,
            "detail": detail or _TITLE_FOR_KIND["workflow.api.bad_request"],
            "instance": _instance_for(request),
            "code": "workflow.api.bad_request",
        },
    )
    return JSONResponse(
        status_code=status,
        media_type=PROBLEM_MEDIA_TYPE,
        content=problem.model_dump(exclude_none=True),
    )


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------


#: Attribute name used on the FastAPI app instance to flag that the
#: handlers are already installed. ``register_exception_handlers``
#: is idempotent — both :func:`custos_workflow.app.create_app` and
#: the test fixtures call it, and double-registration would have no
#: functional effect (FastAPI overwrites the prior mapping) but
#: would emit duplicate audit-log lines under WF-IMPL-070.
_REGISTERED_FLAG: Final[str] = "_custos_workflow_problem_handlers_registered"


def register_exception_handlers(app: FastAPI) -> None:
    """Install every WF-IMPL-061 RFC 7807 handler on ``app``.

    Subclass handlers come **before** their base class so the
    FastAPI dispatcher (which walks MRO) picks the more specific
    handler. The fallback handlers (:func:`handle_run_controller_error`
    / :func:`handle_validator_error`) catch unrecognised subclasses
    so the wire envelope stays well-formed even if a future task
    introduces an error class without updating the registration
    table.

    Idempotent: calling twice on the same app instance is a no-op.
    """
    if getattr(app, _REGISTERED_FLAG, False):
        return

    # ----- Run Controller: subclasses before the base -----
    app.add_exception_handler(RunNotFoundError, handle_run_not_found)
    app.add_exception_handler(RunStateConflictError, handle_run_state_conflict)
    app.add_exception_handler(WorkflowRuntimeUnavailableError, handle_workflow_runtime_unavailable)
    app.add_exception_handler(RunControllerError, handle_run_controller_error)

    # ----- Validator: subclasses before the base -----
    app.add_exception_handler(WorkflowVersionNotFoundError, handle_workflow_version_not_found)
    app.add_exception_handler(InputsSchemaError, handle_inputs_schema_error)
    app.add_exception_handler(IdempotencyConflictError, handle_idempotency_conflict)
    app.add_exception_handler(WorkspaceUnauthorizedError, handle_workspace_unauthorized)
    app.add_exception_handler(ValidatorError, handle_validator_error)

    # ----- request validation + HTTPException pass-through -----
    app.add_exception_handler(RequestValidationError, handle_request_validation_error)
    app.add_exception_handler(StarletteHTTPException, handle_http_exception)

    setattr(app, _REGISTERED_FLAG, True)

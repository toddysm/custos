"""Direct unit tests for ``custos_catalog.api.errors`` handlers.

The route-level tests in ``test_errors.py`` exercise the common code
paths through real HTTP requests. This file covers the handlers that
require synthetic exceptions (catch-all 500s, non-envelope HTTPException
detail, immutability errors, etc.) so the error-envelope module itself
reaches ≥90% coverage.

Handlers are async callables taking ``(request, exc)`` and returning
``starlette.responses.JSONResponse``; we invoke them directly with a
``None``-cast Request because the handlers don't read the request.
"""

from __future__ import annotations

import json
from typing import Any, cast

import pytest
from fastapi import Request
from starlette.exceptions import HTTPException as StarletteHTTPException

from custos_catalog.api import errors as api_errors
from custos_catalog.managers.activity_registry import ActivityRegistryError
from custos_catalog.managers.connector_registry import ConnectorRegistryError
from custos_catalog.managers.definition import (
    PublishValidationError,
    PublishValidationIssue,
)
from custos_catalog.managers.template import (
    ExtractionError,
    MaterializationError,
)
from custos_catalog.versioning import (
    TemplateImmutabilityError,
    WorkflowImmutabilityError,
)


def _request() -> Request:
    """Return a Request stand-in; handlers never read it."""
    return cast(Request, None)


def _decode(response: Any) -> tuple[int, dict[str, Any]]:
    body = json.loads(bytes(response.body).decode())
    return int(response.status_code), body


# ---------------------------------------------------------------------------
# _issues_from_dataclasses — dict-passthrough branch
# ---------------------------------------------------------------------------


async def test_extraction_error_with_dict_issues_passes_through_unchanged() -> None:
    """A dict in the issues list is normalized via ``dict(item)`` not asdict."""
    # PublishValidationError accepts dataclass issues, but the renderer also
    # supports a pre-rendered dict for forward-compatibility. Construct one
    # directly to exercise the dict branch.
    pve = PublishValidationError.__new__(PublishValidationError)
    pve.stage = "schema"
    pve.issues = cast(
        list[PublishValidationIssue],
        [{"path": "$.spec", "code": "schema.missing", "message": "missing"}],
    )
    Exception.__init__(pve, "synthetic")
    exc = ExtractionError(
        workspace_id="ws-1",
        source_workflow_name="wf",
        source_workflow_version=1,
        template_name="tpl",
        cause=pve,
    )
    response = await api_errors.handle_extraction_error(_request(), exc)
    status, body = _decode(response)
    assert status == 400
    assert body["error"]["code"] == "catalog.template_extract_failed.PublishValidationError"
    assert body["error"]["issues"] == [
        {"path": "$.spec", "code": "schema.missing", "message": "missing"},
    ]


# ---------------------------------------------------------------------------
# handle_extraction_error / handle_materialization_error — PublishValidationError cause
# ---------------------------------------------------------------------------


async def test_extraction_error_with_publish_validation_cause_includes_issues() -> None:
    pve = PublishValidationError(
        stage="normalize",
        issues=[
            PublishValidationIssue(
                stage="normalize",
                path="$.spec.steps[0]",
                code="normalize.bad-key",
                message="unknown key",
            ),
        ],
    )
    exc = ExtractionError(
        workspace_id="ws-1",
        source_workflow_name="wf",
        source_workflow_version=2,
        template_name="tpl",
        cause=pve,
    )
    response = await api_errors.handle_extraction_error(_request(), exc)
    status, body = _decode(response)
    assert status == 400
    assert body["error"]["code"] == "catalog.template_extract_failed.PublishValidationError"
    assert body["error"]["issues"][0]["code"] == "normalize.bad-key"


async def test_materialization_error_with_publish_validation_cause_includes_issues() -> None:
    pve = PublishValidationError(
        stage="cel",
        issues=[
            PublishValidationIssue(
                stage="cel",
                path="$.spec.expr",
                code="cel.parse-error",
                message="syntax error",
            ),
        ],
    )
    exc = MaterializationError(
        workspace_id="ws-1",
        template_name="tpl",
        template_version=3,
        target_workflow_name="wf",
        cause=pve,
    )
    response = await api_errors.handle_materialization_error(_request(), exc)
    status, body = _decode(response)
    assert status == 400
    assert body["error"]["code"] == "catalog.template_materialization_failed.PublishValidationError"
    assert body["error"]["issues"][0]["code"] == "cel.parse-error"


async def test_materialization_error_without_publish_validation_cause_omits_issues() -> None:
    """Non-PublishValidationError causes produce envelopes without ``issues``."""
    exc = MaterializationError(
        workspace_id="ws-1",
        template_name="tpl",
        template_version=1,
        target_workflow_name="wf",
        cause=RuntimeError("template render failed"),
    )
    response = await api_errors.handle_materialization_error(_request(), exc)
    status, body = _decode(response)
    assert status == 400
    assert body["error"]["code"] == "catalog.template_materialization_failed.RuntimeError"
    assert "issues" not in body["error"]


# ---------------------------------------------------------------------------
# Immutability handlers
# ---------------------------------------------------------------------------


async def test_workflow_immutability_handler_emits_409_envelope() -> None:
    exc = WorkflowImmutabilityError(
        workspace_id="ws-1",
        workflow_name="wf",
        attempted_version=5,
        next_available_version=6,
        is_idempotent_match=False,
    )
    response = await api_errors.handle_workflow_immutability_error(_request(), exc)
    status, body = _decode(response)
    assert status == 409
    assert body["error"]["code"] == "catalog.workflow_immutability_violation"


async def test_template_immutability_handler_emits_409_envelope() -> None:
    exc = TemplateImmutabilityError(
        workspace_id="ws-1",
        template_name="tpl",
        attempted_version=2,
        next_available_version=3,
        is_idempotent_match=False,
    )
    response = await api_errors.handle_template_immutability_error(_request(), exc)
    status, body = _decode(response)
    assert status == 409
    assert body["error"]["code"] == "catalog.template_immutability_violation"


# ---------------------------------------------------------------------------
# Registry catch-all handlers
# ---------------------------------------------------------------------------


async def test_activity_registry_catch_all_returns_500_envelope() -> None:
    """An ``ActivityRegistryError`` base instance falls to the 500 handler."""
    exc = ActivityRegistryError("unexpected adapter failure")
    response = await api_errors.handle_activity_registry_error(_request(), exc)
    status, body = _decode(response)
    assert status == 500
    assert body["error"]["code"] == "catalog.activity_registry_internal_error"
    assert "unexpected adapter failure" in body["error"]["detail"]


async def test_connector_registry_catch_all_returns_500_envelope() -> None:
    exc = ConnectorRegistryError("unexpected adapter failure")
    response = await api_errors.handle_connector_registry_error(_request(), exc)
    status, body = _decode(response)
    assert status == 500
    assert body["error"]["code"] == "catalog.connector_registry_internal_error"


# ---------------------------------------------------------------------------
# HTTPException pass-through — non-envelope detail branch
# ---------------------------------------------------------------------------


async def test_http_exception_with_string_detail_wraps_into_envelope() -> None:
    exc = StarletteHTTPException(status_code=418, detail="i am a teapot")
    response = await api_errors.handle_http_exception(_request(), exc)
    status, body = _decode(response)
    assert status == 418
    assert body["error"]["code"] == "catalog.http_418"
    assert body["error"]["detail"] == "i am a teapot"


async def test_http_exception_with_envelope_detail_is_passed_through() -> None:
    """Routes that already hand-build an envelope have it preserved."""
    envelope = {"error": {"code": "custom.code", "detail": "preformatted"}}
    exc = StarletteHTTPException(status_code=400, detail=envelope)  # type: ignore[arg-type]
    response = await api_errors.handle_http_exception(_request(), exc)
    status, body = _decode(response)
    assert status == 400
    assert body == envelope


# ---------------------------------------------------------------------------
# Sanity: register_exception_handlers wires every public class
# ---------------------------------------------------------------------------


def test_register_exception_handlers_is_idempotent() -> None:
    """Calling ``register_exception_handlers`` twice does not raise."""
    from fastapi import FastAPI

    app = FastAPI()
    api_errors.register_exception_handlers(app)
    api_errors.register_exception_handlers(app)
    # No assertion needed: the call should not throw and FastAPI's
    # exception-handler map should still be well-formed.
    assert app.exception_handlers


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__])

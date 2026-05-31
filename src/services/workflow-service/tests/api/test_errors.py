"""Tests for the WF-IMPL-061 RFC 7807 public-API error envelope.

Each test exercises one acceptance criterion from issue #447:

* the locked taxonomy table covers every concrete error class in
  :mod:`custos_workflow.runs.errors` and
  :mod:`custos_workflow.validator.errors`;
* every documented kind round-trips through its handler end-to-end
  via :class:`fastapi.testclient.TestClient` with the correct status
  code, ``application/problem+json`` media type, and extension
  fields populated from the exception's attributes;
* the catch-all ``workflow.api.bad_request`` envelope is emitted
  for FastAPI ``RequestValidationError`` and unknown
  :class:`RunControllerError` / :class:`ValidatorError` subclasses;
* the :class:`StarletteHTTPException` pass-through preserves the
  original status while normalising the envelope shape;
* :func:`register_exception_handlers` is idempotent.
"""

from __future__ import annotations

from typing import Any, Final

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import BaseModel

from custos_workflow.api import (
    LOCKED_API_KIND_TO_STATUS,
    LOCKED_API_KINDS,
    PROBLEM_TYPE_PREFIX,
    ProblemDetail,
    register_exception_handlers,
)
from custos_workflow.api.errors import (
    _REGISTERED_FLAG,
    PROBLEM_MEDIA_TYPE,
    _type_uri_for_kind,
)
from custos_workflow.runs.errors import (
    LOCKED_RUN_KINDS,
    RunNotFoundError,
    RunStateConflictError,
    RunStateCorruptError,
    WorkflowRuntimeUnavailableError,
)
from custos_workflow.validator.errors import (
    LOCKED_VALIDATOR_KINDS,
    IdempotencyConflictError,
    InputsSchemaError,
    ValidatorError,
    WorkflowVersionNotFoundError,
    WorkspaceUnauthorizedError,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


PUBLIC_RUN_KINDS_IN_API: Final[frozenset[str]] = frozenset(
    {
        "workflow.run_not_found",
        "workflow.run_state_conflict",
        "workflow.workflow_runtime_unavailable",
    },
)
"""Run Controller kinds documented as a public-API surface in the
locked WF-IMPL-061 table. ``run.state_corrupt`` is intentionally
excluded — it is a server-side data-integrity bug that maps to
the catch-all ``workflow.api.bad_request`` envelope so SDK clients
never see a kind they cannot branch on."""


def _build_app(routes: dict[str, Any]) -> FastAPI:
    """Build a minimal FastAPI app with one route per ``(path, callable)`` pair.

    Tests register their own routes so each exception class can be
    raised in isolation and the handler-registered envelope can be
    asserted end-to-end through :class:`TestClient`.
    """
    app = FastAPI()
    register_exception_handlers(app)
    for path, handler in routes.items():
        app.get(path)(handler)
    return app


class _EchoBody(BaseModel):
    """Module-level body model used by the request-validation test.

    Defined at module scope (not inside the test method) so Pydantic
    can resolve the forward reference when FastAPI builds its
    :class:`pydantic.TypeAdapter` for the route's ``body`` parameter
    (an in-function class fails with "not fully defined").
    """

    count: int


# ---------------------------------------------------------------------------
# Locked taxonomy table
# ---------------------------------------------------------------------------


class TestLockedTaxonomy:
    """The taxonomy table is the contract surface — any drift breaks SDKs."""

    def test_table_has_eight_entries(self) -> None:
        # Adding or removing a kind requires a coordinated change to
        # the SDK + the dev docs (WF-IMPL-072). Pin the size.
        assert len(LOCKED_API_KIND_TO_STATUS) == 8

    def test_table_keys_match_kinds_frozenset(self) -> None:
        assert frozenset(LOCKED_API_KIND_TO_STATUS) == LOCKED_API_KINDS

    def test_table_covers_public_run_controller_kinds(self) -> None:
        # Every Run Controller kind documented as a public-API
        # surface MUST have an entry. `run.state_corrupt` is
        # intentionally absent (data-integrity bug, maps via the
        # catch-all envelope).
        assert PUBLIC_RUN_KINDS_IN_API.issubset(LOCKED_API_KINDS)

    def test_table_covers_every_validator_kind(self) -> None:
        # Every Validator kind MUST have an entry — there is no
        # data-integrity-only validator failure.
        assert LOCKED_VALIDATOR_KINDS.issubset(LOCKED_API_KINDS)

    def test_table_status_codes_in_valid_range(self) -> None:
        for kind, status in LOCKED_API_KIND_TO_STATUS.items():
            assert 100 <= status <= 599, f"{kind} -> {status}"

    def test_locked_run_kinds_unchanged(self) -> None:
        # Defensive: surface drift in the Run Controller taxonomy
        # so the API mapping stays in sync.
        assert (
            frozenset(
                {
                    "run.not_found",
                    "run.state_conflict",
                    "run.state_corrupt",
                    "run.runtime_unavailable",
                },
            )
            == LOCKED_RUN_KINDS
        )


# ---------------------------------------------------------------------------
# ProblemDetail wire model
# ---------------------------------------------------------------------------


class TestProblemDetailModel:
    def test_from_kind_populates_locked_fields(self) -> None:
        problem = ProblemDetail.from_kind(
            "workflow.run_not_found",
            detail="run X missing",
            instance="/v1/workspaces/ws-1/runs/X",
            extras={"runId": "X"},
        )
        assert problem.type == _type_uri_for_kind("workflow.run_not_found")
        assert problem.type.startswith(PROBLEM_TYPE_PREFIX)
        assert problem.title == "Run not found"
        assert problem.status == 404
        assert problem.detail == "run X missing"
        assert problem.instance == "/v1/workspaces/ws-1/runs/X"
        assert problem.code == "workflow.run_not_found"

    def test_from_kind_drops_none_extras(self) -> None:
        problem = ProblemDetail.from_kind(
            "workflow.run_not_found",
            detail="missing",
            instance="/path",
            extras={"runId": None, "extra": "value"},
        )
        dumped = problem.model_dump(exclude_none=True)
        assert "runId" not in dumped
        assert dumped["extra"] == "value"

    def test_from_kind_rejects_unknown_kind(self) -> None:
        with pytest.raises(KeyError):
            ProblemDetail.from_kind(
                "workflow.not_a_real_kind",
                detail="x",
                instance=None,
            )

    def test_type_uri_format(self) -> None:
        # Dots become slashes so the path mirrors the kind hierarchy.
        assert (
            _type_uri_for_kind("workflow.validator.inputs_schema_error")
            == f"{PROBLEM_TYPE_PREFIX}workflow/validator/inputs_schema_error"
        )


# ---------------------------------------------------------------------------
# Per-kind end-to-end handler tests
# ---------------------------------------------------------------------------


class TestRunControllerHandlers:
    def test_run_not_found_envelope(self) -> None:
        async def route() -> None:
            raise RunNotFoundError("run X missing", run_id="X")

        client = TestClient(_build_app({"/raise": route}))
        resp = client.get("/raise")
        assert resp.status_code == 404
        assert resp.headers["content-type"].startswith(PROBLEM_MEDIA_TYPE)
        body = resp.json()
        assert body["code"] == "workflow.run_not_found"
        assert body["status"] == 404
        assert body["title"] == "Run not found"
        assert body["detail"] == "run X missing"
        assert body["instance"] == "/raise"
        assert body["runId"] == "X"
        assert body["type"] == _type_uri_for_kind("workflow.run_not_found")

    def test_run_state_conflict_envelope_includes_status_pair(self) -> None:
        async def route() -> None:
            raise RunStateConflictError(
                "cannot cancel a succeeded run",
                run_id="r-1",
                current_status="succeeded",
                attempted_status="cancelled",
            )

        client = TestClient(_build_app({"/raise": route}))
        resp = client.get("/raise")
        assert resp.status_code == 409
        body = resp.json()
        assert body["code"] == "workflow.run_state_conflict"
        assert body["currentStatus"] == "succeeded"
        assert body["attemptedStatus"] == "cancelled"
        assert body["runId"] == "r-1"

    def test_workflow_runtime_unavailable_envelope(self) -> None:
        async def route() -> None:
            raise WorkflowRuntimeUnavailableError(
                "dapr sidecar unreachable",
                run_id="r-2",
            )

        client = TestClient(_build_app({"/raise": route}))
        resp = client.get("/raise")
        assert resp.status_code == 503
        body = resp.json()
        assert body["code"] == "workflow.workflow_runtime_unavailable"
        assert body["runId"] == "r-2"

    def test_unknown_run_controller_subclass_falls_back_to_bad_request(self) -> None:
        # RunStateCorruptError is intentionally NOT mapped to its
        # own kind — it is a data-integrity bug. The fallback
        # handler emits a `workflow.api.bad_request` envelope so
        # SDK clients still see a kind they can branch on.
        async def route() -> None:
            raise RunStateCorruptError("corrupt graph json", run_id="r-3")

        client = TestClient(_build_app({"/raise": route}))
        resp = client.get("/raise")
        assert resp.status_code == 400
        body = resp.json()
        assert body["code"] == "workflow.api.bad_request"
        assert body["runId"] == "r-3"
        # The underlying kind is preserved as a hint so operators
        # debugging from the wire body can still recognise the bug.
        assert body["underlyingKind"] == "run.state_corrupt"


class TestValidatorHandlers:
    def test_workflow_version_not_found_envelope(self) -> None:
        async def route() -> None:
            raise WorkflowVersionNotFoundError(
                "catalog has no such version",
                workspace_id="ws-1",
                workflow_id="wf-1",
                workflow_version="1.2.3",
            )

        client = TestClient(_build_app({"/raise": route}))
        resp = client.get("/raise")
        assert resp.status_code == 404
        body = resp.json()
        assert body["code"] == "workflow.validator.workflow_version_not_found"
        assert body["workspaceId"] == "ws-1"
        assert body["workflowId"] == "wf-1"
        assert body["workflowVersion"] == "1.2.3"

    def test_inputs_schema_error_envelope_preserves_validation_list(self) -> None:
        issues: list[dict[str, Any]] = [
            {"loc": ["inputs", "count"], "code": "int_parsing", "message": "not int"},
            {"loc": ["inputs", "name"], "code": "missing", "message": "required"},
        ]

        async def route() -> None:
            raise InputsSchemaError(
                "inputs failed schema",
                workspace_id="ws-2",
                validation=issues,
            )

        client = TestClient(_build_app({"/raise": route}))
        resp = client.get("/raise")
        assert resp.status_code == 422
        body = resp.json()
        assert body["code"] == "workflow.validator.inputs_schema_error"
        assert body["workspaceId"] == "ws-2"
        assert body["validation"] == issues

    def test_idempotency_conflict_envelope(self) -> None:
        async def route() -> None:
            raise IdempotencyConflictError(
                "key already in flight with a different fingerprint",
                workspace_id="ws-3",
                idempotency_key="abc-123",
            )

        client = TestClient(_build_app({"/raise": route}))
        resp = client.get("/raise")
        assert resp.status_code == 409
        body = resp.json()
        assert body["code"] == "workflow.validator.idempotency_conflict"
        assert body["workspaceId"] == "ws-3"
        assert body["idempotencyKey"] == "abc-123"

    def test_workspace_unauthorized_envelope(self) -> None:
        async def route() -> None:
            raise WorkspaceUnauthorizedError(
                "principal not entitled in workspace",
                workspace_id="ws-4",
                principal="user:alice",
            )

        client = TestClient(_build_app({"/raise": route}))
        resp = client.get("/raise")
        assert resp.status_code == 403
        body = resp.json()
        assert body["code"] == "workflow.validator.workspace_unauthorized"
        assert body["workspaceId"] == "ws-4"
        assert body["principal"] == "user:alice"

    def test_unknown_validator_subclass_falls_back_to_bad_request(self) -> None:
        # Synthesize an unrecognised ValidatorError subclass to
        # exercise the fallback handler. Real subclasses always
        # have a dedicated handler.
        class _UnknownValidatorError(ValidatorError):
            KIND = "workflow.validator.future_kind"

        async def route() -> None:
            raise _UnknownValidatorError("future failure", workspace_id="ws-5")

        client = TestClient(_build_app({"/raise": route}))
        resp = client.get("/raise")
        assert resp.status_code == 400
        body = resp.json()
        assert body["code"] == "workflow.api.bad_request"
        assert body["workspaceId"] == "ws-5"
        assert body["underlyingKind"] == "workflow.validator.future_kind"


# ---------------------------------------------------------------------------
# Catch-all handlers
# ---------------------------------------------------------------------------


class TestRequestValidationHandler:
    def test_pydantic_body_rejection_renders_bad_request_envelope(self) -> None:
        app = FastAPI()
        register_exception_handlers(app)

        @app.post("/echo")
        async def echo(body: _EchoBody) -> dict[str, int]:
            return {"count": body.count}

        client = TestClient(app)
        resp = client.post("/echo", json={"count": "not-an-int"})
        assert resp.status_code == 400
        assert resp.headers["content-type"].startswith(PROBLEM_MEDIA_TYPE)
        body = resp.json()
        assert body["code"] == "workflow.api.bad_request"
        assert body["status"] == 400
        # Validation list preserved so clients keep field-level diagnostics.
        assert isinstance(body["validation"], list)
        assert body["validation"], "expected at least one issue"
        first = body["validation"][0]
        assert isinstance(first["loc"], list)
        # FastAPI emits ['body', 'count', ...] for body-field rejections.
        assert "body" in first["loc"]
        assert "count" in first["loc"]
        assert "code" in first
        assert "message" in first

    def test_http_exception_pass_through_preserves_status(self) -> None:
        async def route() -> None:
            raise HTTPException(status_code=405, detail="method not allowed here")

        client = TestClient(_build_app({"/raise": route}))
        resp = client.get("/raise")
        assert resp.status_code == 405
        assert resp.headers["content-type"].startswith(PROBLEM_MEDIA_TYPE)
        body = resp.json()
        assert body["code"] == "workflow.api.bad_request"
        # The original status flows through verbatim even though
        # the locked table maps bad_request to 400 by default.
        assert body["status"] == 405
        assert body["detail"] == "method not allowed here"


# ---------------------------------------------------------------------------
# Registration idempotency
# ---------------------------------------------------------------------------


class TestRegistrationIdempotency:
    def test_register_twice_is_noop(self) -> None:
        app = FastAPI()
        register_exception_handlers(app)
        first_handler_count = len(app.exception_handlers)
        assert getattr(app, _REGISTERED_FLAG) is True

        register_exception_handlers(app)
        # Second call sets nothing new because the registered-flag
        # short-circuits the function. Handler count is identical.
        assert len(app.exception_handlers) == first_handler_count
        assert getattr(app, _REGISTERED_FLAG) is True

    def test_registered_app_actually_uses_the_handlers(self) -> None:
        # Sanity: idempotent guard does not break the registered handlers.
        async def route() -> None:
            raise RunNotFoundError("missing", run_id="r-7")

        app = FastAPI()
        register_exception_handlers(app)
        register_exception_handlers(app)  # second call — should still work
        app.get("/raise")(route)
        client = TestClient(app)
        resp = client.get("/raise")
        assert resp.status_code == 404
        assert resp.json()["code"] == "workflow.run_not_found"

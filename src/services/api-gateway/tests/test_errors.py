"""Tests for the locked error taxonomy + Problem+JSON envelope (AGW-IMPL-003)."""

from __future__ import annotations

import uuid

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from custos_gateway.errors import (
    CORRELATION_ID_HEADER,
    LOCKED_CODE_TO_STATUS,
    PROBLEM_MEDIA_TYPE,
    PROBLEM_TYPE_PREFIX,
    GatewayError,
    GatewayErrorCode,
    ProblemDetail,
    problem_response,
    register_exception_handlers,
)


def _error_app(error: GatewayError) -> FastAPI:
    """Build a tiny app whose single route raises ``error``."""
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/boom")
    async def boom(request: Request) -> dict[str, str]:  # pragma: no cover - raises
        # Mimic the correlation middleware binding an id to the request.
        request.state.correlation_id = "01890d3e-0000-7000-8000-000000000001"
        raise error

    return app


def test_every_enum_member_is_in_the_locked_status_table() -> None:
    # The taxonomy is closed: every declared code has a status, and the status
    # table never carries a code the enum does not declare.
    assert set(LOCKED_CODE_TO_STATUS) == set(GatewayErrorCode)


@pytest.mark.parametrize("code", list(GatewayErrorCode))
def test_grid_every_code_has_stable_type_uri_and_status(code: GatewayErrorCode) -> None:
    status = LOCKED_CODE_TO_STATUS[code]
    assert 100 <= status <= 599
    problem = ProblemDetail.from_code(
        code,
        detail="grid",
        instance="/v1/test",
        correlation_id="cid",
    )
    assert problem.type == f"{PROBLEM_TYPE_PREFIX}{code.value}"
    assert problem.type.startswith("https://custos.dev/errors/")
    assert problem.code == code.value
    assert problem.status == status
    assert problem.title  # non-empty human-readable summary


def test_gateway_error_status_property_reads_locked_table() -> None:
    err = GatewayError(GatewayErrorCode.RATE_LIMITED, detail="slow down")
    assert err.status == 429
    assert err.code is GatewayErrorCode.RATE_LIMITED
    assert str(err) == "slow down"


def test_from_code_serialises_alias_and_drops_none() -> None:
    problem = ProblemDetail.from_code(
        GatewayErrorCode.PERMISSION_DENIED,
        detail="nope",
        instance=None,
        correlation_id=None,
    )
    dumped = problem.model_dump(exclude_none=True, by_alias=True)
    assert "instance" not in dumped
    assert "correlationId" not in dumped
    assert dumped["code"] == "permission-denied"
    assert dumped["status"] == 403


def test_from_code_includes_non_null_extras() -> None:
    problem = ProblemDetail.from_code(
        GatewayErrorCode.PERMISSION_DENIED,
        detail="nope",
        instance="/v1/runs",
        correlation_id="cid",
        extras={"auditEventId": "evt_123", "ignored": None},
    )
    dumped = problem.model_dump(exclude_none=True, by_alias=True)
    assert dumped["auditEventId"] == "evt_123"
    assert "ignored" not in dumped


def test_handler_renders_problem_json_with_correlation_and_extras() -> None:
    app = _error_app(
        GatewayError(
            GatewayErrorCode.PERMISSION_DENIED,
            detail="you may not start runs here",
            extras={"auditEventId": "evt_42"},
        )
    )
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/boom")

    assert response.status_code == 403
    assert response.headers["content-type"].startswith(PROBLEM_MEDIA_TYPE)
    body = response.json()
    assert body["code"] == "permission-denied"
    assert body["type"] == "https://custos.dev/errors/permission-denied"
    assert body["status"] == 403
    assert body["instance"] == "/boom"
    assert body["correlationId"] == "01890d3e-0000-7000-8000-000000000001"
    assert body["auditEventId"] == "evt_42"


def test_handler_passes_through_response_headers() -> None:
    app = _error_app(
        GatewayError(
            GatewayErrorCode.RATE_LIMITED,
            detail="rate limited",
            headers={"Retry-After": "5"},
        )
    )
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/boom")

    assert response.status_code == 429
    assert response.headers["retry-after"] == "5"


def test_from_code_rejects_unknown_code() -> None:
    # A value outside the locked enum can never be rendered.
    with pytest.raises(KeyError):
        ProblemDetail.from_code(
            "totally-made-up",  # type: ignore[arg-type]
            detail="x",
            instance=None,
            correlation_id=None,
        )


def test_problem_detail_validates_status_range() -> None:
    with pytest.raises(ValueError):
        ProblemDetail(
            type="https://custos.dev/errors/x",
            title="x",
            status=42,
            detail="x",
            code="x",
        )


def test_problem_response_without_correlation_id() -> None:
    # A request with no correlation id bound (no middleware) yields an envelope
    # that omits the correlationId field rather than emitting null.
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/v1/x",
        "headers": [],
        "query_string": b"",
    }
    request = Request(scope)
    response = problem_response(request, code=GatewayErrorCode.INVALID_TOKEN, detail="bad")
    assert response.status_code == 401
    assert response.media_type == PROBLEM_MEDIA_TYPE
    assert b"correlationId" not in response.body


def test_correlation_header_constant_is_lowercase() -> None:
    assert CORRELATION_ID_HEADER == "x-correlation-id"


def test_correlation_id_field_is_omitted_when_unset() -> None:
    # The from_code path with a real uuid keeps the alias key present.
    cid = str(uuid.uuid4())
    problem = ProblemDetail.from_code(
        GatewayErrorCode.INVALID_TOKEN,
        detail="bad token",
        instance="/v1/x",
        correlation_id=cid,
    )
    assert problem.model_dump(by_alias=True)["correlationId"] == cid

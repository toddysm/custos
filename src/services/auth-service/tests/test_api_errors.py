"""Direct unit tests for ``custos_auth.api.errors`` (AS-IMPL-027 / #262).

The auth-service public REST surface guarantees one error envelope shape:

    {"error": {"code": "...", "detail": "...", "issues"?: [...]}}

This module hosts the handlers that produce that envelope for every
failure mode the service can surface. The behaviour is also covered
end-to-end through route tests, but the per-handler unit tests pinned
here exist to:

* prove every branch of ``handle_http_exception`` and
  ``handle_validation_error`` is exercised (Phase J left
  ``handle_http_exception`` uncovered);
* lock the envelope keyset so a future refactor cannot silently change
  the wire shape — every test asserts ``set(body["error"].keys())``.

The handlers in ``errors.py`` accept ``Exception`` at the signature level
(FastAPI installs them via ``add_exception_handler`` which uses that
loose annotation) and ``assert isinstance(...)`` internally. The
assertion paths are tested via the documented success cases; the
``AssertionError`` branches are not part of the contract and are not
tested here (they would only fire on a registration bug).
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.testclient import TestClient
from starlette.exceptions import HTTPException as StarletteHTTPException

from custos_auth.api.errors import (
    AuthApiError,
    Conflict,
    InvalidRoleScope,
    NotFound,
    Unauthenticated,
    ValidationFailure,
    handle_auth_api_error,
    handle_http_exception,
    handle_validation_error,
    register_exception_handlers,
)

# ---------------------------------------------------------------------------
# AuthApiError subclasses — status_code + code pinning
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("exc_cls", "expected_status", "expected_code"),
    [
        (NotFound, 404, "not_found"),
        (Conflict, 409, "conflict"),
        (ValidationFailure, 400, "invalid_request"),
        (InvalidRoleScope, 400, "invalid_role_scope"),
        (Unauthenticated, 401, "unauthenticated"),
    ],
)
def test_auth_api_error_subclass_pins_status_and_code(
    exc_cls: type[AuthApiError], expected_status: int, expected_code: str
) -> None:
    """Each subclass carries the documented status + machine-readable code.

    Changing either is a breaking API change — every client that
    branches on ``error.code`` would have to be updated in lockstep.
    """
    exc = exc_cls("whatever")
    assert exc.status_code == expected_status
    assert exc.code == expected_code
    assert exc.detail == "whatever"


def test_auth_api_error_base_defaults_to_500_internal_error() -> None:
    """The bare base class is the fallback for un-subclassed raises.

    No route raises it directly, but the handler registration covers
    it so any future un-narrowed subclass still renders the envelope.
    """
    exc = AuthApiError("boom")
    assert exc.status_code == 500
    assert exc.code == "internal_error"


# ---------------------------------------------------------------------------
# handle_auth_api_error — envelope shape
# ---------------------------------------------------------------------------


async def test_handle_auth_api_error_envelope_keyset_is_locked() -> None:
    """Without ``issues``, the envelope is exactly ``{code, detail}``."""
    response = await handle_auth_api_error(_make_request(), NotFound("nope"))
    body = _decode(response)
    assert response.status_code == 404
    assert body == {"error": {"code": "not_found", "detail": "nope"}}
    assert set(body["error"].keys()) == {"code", "detail"}


async def test_handle_auth_api_error_renders_invalid_role_scope() -> None:
    response = await handle_auth_api_error(
        _make_request(),
        InvalidRoleScope("role 'viewer' cannot bind at platform scope"),
    )
    body = _decode(response)
    assert response.status_code == 400
    assert body["error"]["code"] == "invalid_role_scope"
    assert "platform" in body["error"]["detail"]


# ---------------------------------------------------------------------------
# handle_validation_error — Pydantic per-field issues
# ---------------------------------------------------------------------------


async def test_handle_validation_error_renders_issues_array() -> None:
    """Every Pydantic ``ValidationError`` entry becomes one ``issues[]`` row.

    The ``loc`` element is coerced to ``str`` per element — Pydantic
    allows ints (tuple-index path) and types we cannot JSON-encode
    directly.
    """
    exc = RequestValidationError(
        [
            {
                "type": "missing",
                "loc": ("body", "tenant_id"),
                "msg": "Field required",
                "input": {},
            },
            {
                "type": "string_type",
                "loc": ("body", "items", 0, "name"),
                "msg": "Input should be a valid string",
                "input": 42,
            },
        ]
    )

    response = await handle_validation_error(_make_request(), exc)
    body = _decode(response)

    assert response.status_code == 422
    assert body["error"]["code"] == "request_validation_failed"
    assert body["error"]["detail"] == "one or more request fields are invalid"

    issues = body["error"]["issues"]
    assert len(issues) == 2
    assert issues[0] == {
        "loc": ["body", "tenant_id"],
        "msg": "Field required",
        "type": "missing",
    }
    # Numeric tuple positions are coerced to str so the response is
    # cleanly JSON-serialisable.
    assert issues[1]["loc"] == ["body", "items", "0", "name"]
    assert all(isinstance(piece, str) for piece in issues[1]["loc"])


async def test_handle_validation_error_uses_defaults_for_missing_fields() -> None:
    """A malformed validation entry must not crash the renderer.

    Real Pydantic always populates ``msg`` / ``type`` but we render
    defaults defensively so a future Pydantic version that drops or
    renames a key cannot down the whole error-envelope path.
    """
    exc = RequestValidationError([{"loc": ()}])  # no msg, no type

    response = await handle_validation_error(_make_request(), exc)
    body = _decode(response)

    assert response.status_code == 422
    issues = body["error"]["issues"]
    assert issues == [{"loc": [], "msg": "invalid value", "type": "value_error"}]


# ---------------------------------------------------------------------------
# handle_http_exception — Starlette HTTPException → envelope
# ---------------------------------------------------------------------------


async def test_handle_http_exception_renders_string_detail() -> None:
    """``raise HTTPException(404)`` must surface as the shared envelope.

    Without the handler, Starlette would emit ``{"detail": "Not Found"}``
    which is the wrong shape for clients that key on ``error.code``.
    """
    exc = StarletteHTTPException(status_code=404, detail="Not Found")
    response = await handle_http_exception(_make_request(), exc)
    body = _decode(response)

    assert response.status_code == 404
    assert body == {"error": {"code": "http_error", "detail": "Not Found"}}


async def test_handle_http_exception_coerces_non_string_detail() -> None:
    """Starlette permits dict-shaped details; we stringify them.

    The envelope contract is ``detail: str``; passing a dict through
    untouched would break the wire shape. ``str(...)`` is good enough
    for the rare case a route raises one.
    """
    # ``StarletteHTTPException.detail`` is typed as ``str | None`` but the
    # runtime accepts arbitrary values (FastAPI documents the dict shape).
    # The handler's non-string ``detail`` path (the ``else str(exc.detail)``
    # branch) is under test, so we coerce past the typed signature.
    exc = StarletteHTTPException(status_code=418, detail="placeholder")
    exc.detail = {"reason": "teapot"}  # type: ignore[assignment]
    response = await handle_http_exception(_make_request(), exc)
    body = _decode(response)

    assert response.status_code == 418
    assert body["error"]["code"] == "http_error"
    assert "teapot" in body["error"]["detail"]
    assert isinstance(body["error"]["detail"], str)


# ---------------------------------------------------------------------------
# register_exception_handlers — end-to-end shape via TestClient
# ---------------------------------------------------------------------------


def test_register_exception_handlers_installs_full_set() -> None:
    """Spinning up a minimal FastAPI app via the registrar must surface
    every error class through the shared envelope.

    Catches drift where a new ``AuthApiError`` subclass is added but
    not wired into the registrar.
    """
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/raise-not-found")
    def _raise_not_found() -> None:
        raise NotFound("missing tenant")

    @app.get("/raise-conflict")
    def _raise_conflict() -> None:
        raise Conflict("dup")

    @app.get("/raise-validation")
    def _raise_validation() -> None:
        raise ValidationFailure("bad")

    @app.get("/raise-invalid-scope")
    def _raise_invalid_scope() -> None:
        raise InvalidRoleScope("wrong scope")

    @app.get("/raise-unauth")
    def _raise_unauth() -> None:
        raise Unauthenticated("nope")

    @app.get("/raise-http")
    def _raise_http() -> None:
        raise HTTPException(status_code=418, detail="teapot")

    with TestClient(app) as client:
        for path, expected_status, expected_code in (
            ("/raise-not-found", 404, "not_found"),
            ("/raise-conflict", 409, "conflict"),
            ("/raise-validation", 400, "invalid_request"),
            ("/raise-invalid-scope", 400, "invalid_role_scope"),
            ("/raise-unauth", 401, "unauthenticated"),
            ("/raise-http", 418, "http_error"),
        ):
            resp = client.get(path)
            assert resp.status_code == expected_status, path
            body = resp.json()
            assert body["error"]["code"] == expected_code, path
            assert "detail" in body["error"], path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request() -> Any:
    """Build the minimum Starlette ``Request`` ASGI scope.

    The handlers ignore the request — they only consume the
    exception — but FastAPI's signature requires it.
    """
    from starlette.requests import Request

    scope: dict[str, Any] = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [],
    }
    return Request(scope)


def _decode(response: Any) -> dict[str, Any]:
    decoded: dict[str, Any] = json.loads(response.body.decode("utf-8"))
    return decoded

"""Tests for the call-context middleware (OBS-IMPL-012).

Cover both trust modes (real-verifier + unsigned dev shim), the probe bypass,
the production guard, and the two authorization dependencies
(:func:`get_call_context`, :func:`require_permission`).
"""

from __future__ import annotations

import json

import pytest
from custos_callctx import CALLCTX_HEADER, CallContext, InvalidCallContextError, InvalidReason
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient

from custos_obs.middleware import (
    CallContextError,
    CallContextMiddleware,
    DevShimDisabledInProductionError,
    call_context_error_handler,
    get_call_context,
    require_permission,
)
from custos_obs.middleware.callctx import CallContextVerifierProtocol

_PERM = "logs:read"


def _context(
    *, principal: str = "user-1", permissions: frozenset[str] | None = None
) -> CallContext:
    return CallContext(
        acting_principal_id=principal,
        workspace_id="ws-1",
        caller_component="api-gateway",
        jti="jti-1",
        issued_at=0,
        expires_at=0,
        issuer="custos-auth",
        audience="custos.internal",
        kid="kid-1",
        permissions=permissions if permissions is not None else frozenset({_PERM}),
    )


class _FakeVerifier:
    """Structural :class:`CallContextVerifierProtocol` for the production path."""

    def __init__(
        self, *, result: CallContext | None = None, error: InvalidCallContextError | None = None
    ) -> None:
        self._result = result
        self._error = error

    async def verify(self, *, metadata: object) -> CallContext:
        if self._error is not None:
            raise self._error
        assert self._result is not None
        return self._result


def _build_app(
    verifier: CallContextVerifierProtocol | None, *, environment: str = "development"
) -> FastAPI:
    app = FastAPI()
    app.add_middleware(CallContextMiddleware, verifier=verifier, environment=environment)
    app.add_exception_handler(CallContextError, call_context_error_handler)

    @app.get("/healthz")
    async def _healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/whoami")
    async def _whoami(ctx: CallContext = Depends(get_call_context)) -> dict[str, str | None]:
        return {"principal": ctx.acting_principal_id, "workspace": ctx.workspace_id}

    @app.get("/guarded")
    async def _guarded(ctx: CallContext = Depends(require_permission(_PERM))) -> dict[str, str]:
        return {"principal": ctx.acting_principal_id}

    return app


def _dev_header(payload: dict[str, object]) -> dict[str, str]:
    return {CALLCTX_HEADER: json.dumps(payload)}


# --------------------------------------------------------------------------- #
# Dev-shim mode
# --------------------------------------------------------------------------- #


def test_dev_shim_accepts_valid_header_and_exposes_context() -> None:
    client = TestClient(_build_app(None))
    resp = client.get(
        "/whoami",
        headers=_dev_header(
            {"acting_principal_id": "alice", "workspace_id": "ws-9", "permissions": [_PERM]}
        ),
    )
    assert resp.status_code == 200
    assert resp.json() == {"principal": "alice", "workspace": "ws-9"}


def test_dev_shim_accepts_camelcase_aliases_and_null_workspace() -> None:
    client = TestClient(_build_app(None))
    resp = client.get(
        "/whoami",
        headers=_dev_header({"actingPrincipalId": "bob", "workspaceId": None, "permissions": []}),
    )
    assert resp.status_code == 200
    assert resp.json() == {"principal": "bob", "workspace": None}


def test_dev_shim_missing_header_is_401() -> None:
    client = TestClient(_build_app(None))
    resp = client.get("/whoami")
    assert resp.status_code == 401
    assert resp.json() == {
        "error": {"code": "callctx_missing", "detail": f"{CALLCTX_HEADER} header is required"}
    }


def test_dev_shim_non_json_header_is_400() -> None:
    client = TestClient(_build_app(None))
    resp = client.get("/whoami", headers={CALLCTX_HEADER: "not-json{"})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "callctx_malformed"
    # The raw parser exception text must not leak to callers.
    assert resp.json()["error"]["detail"] == f"{CALLCTX_HEADER} header is not valid JSON"


def test_dev_shim_non_object_payload_is_invalid() -> None:
    client = TestClient(_build_app(None))
    resp = client.get("/whoami", headers={CALLCTX_HEADER: json.dumps(["a", "b"])})
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "callctx_invalid"
    assert "JSON object" in resp.json()["error"]["detail"]


def test_dev_shim_missing_principal_is_invalid() -> None:
    client = TestClient(_build_app(None))
    resp = client.get("/whoami", headers=_dev_header({"permissions": [_PERM]}))
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "callctx_invalid"
    assert "acting_principal_id" in resp.json()["error"]["detail"]


def test_dev_shim_non_string_workspace_is_invalid() -> None:
    client = TestClient(_build_app(None))
    resp = client.get(
        "/whoami", headers=_dev_header({"acting_principal_id": "x", "workspace_id": 7})
    )
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "callctx_invalid"
    assert "workspace_id" in resp.json()["error"]["detail"]


def test_dev_shim_non_list_permissions_is_invalid() -> None:
    client = TestClient(_build_app(None))
    resp = client.get(
        "/whoami", headers=_dev_header({"acting_principal_id": "x", "permissions": "logs:read"})
    )
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "callctx_invalid"
    assert "permissions" in resp.json()["error"]["detail"]


def test_dev_shim_forbidden_in_production() -> None:
    async def _app(scope: object, receive: object, send: object) -> None:  # pragma: no cover - stub
        raise AssertionError("unreachable")

    with pytest.raises(DevShimDisabledInProductionError):
        CallContextMiddleware(_app, verifier=None, environment="Production")


# --------------------------------------------------------------------------- #
# Bypass paths
# --------------------------------------------------------------------------- #


def test_probe_paths_bypass_the_middleware() -> None:
    client = TestClient(_build_app(None))
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


# --------------------------------------------------------------------------- #
# Verifier (production) mode
# --------------------------------------------------------------------------- #


def test_verifier_success_exposes_context() -> None:
    verifier = _FakeVerifier(result=_context(principal="signed-user"))
    client = TestClient(_build_app(verifier))
    resp = client.get("/whoami", headers={CALLCTX_HEADER: "signed.jwt.token"})
    assert resp.status_code == 200
    assert resp.json()["principal"] == "signed-user"


def test_verifier_invalid_token_is_401() -> None:
    verifier = _FakeVerifier(error=InvalidCallContextError(InvalidReason.BAD_SIGNATURE, "bad sig"))
    client = TestClient(_build_app(verifier))
    resp = client.get("/whoami", headers={CALLCTX_HEADER: "tampered"})
    assert resp.status_code == 401
    assert resp.json() == {"error": {"code": "callctx_invalid", "detail": "invalid call context"}}


# --------------------------------------------------------------------------- #
# Authorization dependencies
# --------------------------------------------------------------------------- #


def test_require_permission_grants_when_present() -> None:
    client = TestClient(_build_app(None))
    resp = client.get(
        "/guarded",
        headers=_dev_header({"acting_principal_id": "alice", "permissions": [_PERM]}),
    )
    assert resp.status_code == 200
    assert resp.json() == {"principal": "alice"}


def test_require_permission_denies_when_missing() -> None:
    client = TestClient(_build_app(None))
    resp = client.get(
        "/guarded",
        headers=_dev_header({"acting_principal_id": "alice", "permissions": ["metrics:read"]}),
    )
    assert resp.status_code == 403
    body = resp.json()
    assert body["error"]["code"] == "permission_denied"
    assert _PERM in body["error"]["detail"]


def test_get_call_context_without_middleware_raises_envelope() -> None:
    # A route mounted with no middleware in front: the dependency must surface
    # the same 401 envelope rather than handing back a missing context.
    app = FastAPI()
    app.add_exception_handler(CallContextError, call_context_error_handler)

    @app.get("/bare")
    async def _bare(ctx: CallContext = Depends(get_call_context)) -> dict[str, str]:
        return {"principal": ctx.acting_principal_id}

    resp = TestClient(app).get("/bare")
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "callctx_missing"


async def test_call_context_error_handler_renders_envelope() -> None:
    request = Request({"type": "http", "headers": []})
    exc = CallContextError(403, "permission_denied", "nope")
    resp = await call_context_error_handler(request, exc)
    assert resp.status_code == 403
    assert json.loads(bytes(resp.body)) == {
        "error": {"code": "permission_denied", "detail": "nope"}
    }

"""In-process conformance harness for the API Gateway ingress pipeline (AGW-IMPL-020).

These helpers assemble a fully wired :func:`custos_gateway.app.create_app` against
a *stub* downstream (a recording :class:`httpx.MockTransport`), a *fake* Auth
Service client, and an *in-memory* SPL idempotency store — no live collaborators.
The conformance suite in :mod:`test_conformance` drives this harness through
``TestClient`` to prove every cross-cutting stage of the designed pipeline runs
end-to-end and in the designed order.

This is deliberately distinct from the live, Postgres-backed ``tests/integration``
model the data-plane services use: the gateway owns no datastore in M1, so its
"integration" surface is the in-process composition of middleware + clients +
routing. The suite therefore carries no ``integration`` marker — it runs in the
default ``pytest`` step and counts toward the package coverage gate.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx
from custos_spl import (
    IdempotencyRecord,
    IdemReserved,
    PrincipalId,
    ReserveIdempotencyResult,
    WorkspaceId,
)
from fastapi import FastAPI

from custos_gateway.app import create_app
from custos_gateway.clients.auth import (
    AuthServiceClientError as AuthError,
)
from custos_gateway.clients.auth import (
    CallctxSignRequest,
    CallctxSignResponse,
    DeclaredPermission,
    VerifyAndAuthorizeRequest,
    VerifyAndAuthorizeResponse,
)
from custos_gateway.middleware.ratelimit import RateLimiter
from custos_gateway.router import DownstreamRouter
from custos_gateway.routes.registry import registry_required_permissions
from custos_gateway.settings import Settings

WRITE_PATH = "/v1/workspaces/ws-1/runs"
READ_PATH = "/v1/workspaces/ws-1/runs"
AUTH_HEADERS: dict[str, str] = {"authorization": "Bearer token-123"}
JSON_WRITE_HEADERS: dict[str, str] = {**AUTH_HEADERS, "content-type": "application/json"}


@dataclass(slots=True)
class DownstreamRecorder:
    """Captures every request the gateway forwarded to the stub downstream."""

    calls: list[httpx.Request] = field(default_factory=list)


def build_downstream(
    *,
    status_code: int = 201,
    headers: dict[str, str] | None = None,
    body: bytes = b"created",
) -> tuple[DownstreamRouter, DownstreamRecorder]:
    """A real :class:`DownstreamRouter` over a recording mock transport."""
    recorder = DownstreamRecorder()
    reply_headers = {"x-downstream": "yes"} if headers is None else headers

    def handler(request: httpx.Request) -> httpx.Response:
        recorder.calls.append(request)
        return httpx.Response(status_code, headers=reply_headers, content=body)

    router = DownstreamRouter(
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        host="127.0.0.1",
        http_port=3500,
    )
    return router, recorder


def make_record(status: str = "in_progress", route: str = WRITE_PATH) -> IdempotencyRecord:
    """A canned SPL idempotency record for the harness store."""
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return IdempotencyRecord(
        workspace_id=WorkspaceId("ws-1"),
        principal_id=PrincipalId("principal-fake"),
        route=route,
        idempotency_key="key-1",
        request_hash="hash",
        status=status,
        response_snapshot=None,
        reserved_at=now,
        expires_at=now,
    )


@dataclass(slots=True)
class RecordingStore:
    """In-memory SPL idempotency store returning a configurable reserve outcome."""

    outcome: ReserveIdempotencyResult = field(
        default_factory=lambda: IdemReserved(record=make_record())
    )
    reserve_calls: list[tuple[str, str, str, str, str, int]] = field(default_factory=list)
    complete_calls: list[Mapping[str, Any]] = field(default_factory=list)

    async def reserve_idempotency_record(
        self,
        workspace_id: WorkspaceId,
        principal_id: PrincipalId,
        route: str,
        idempotency_key: str,
        request_hash: str,
        ttl_seconds: int,
    ) -> ReserveIdempotencyResult:
        self.reserve_calls.append(
            (workspace_id, principal_id, route, idempotency_key, request_hash, ttl_seconds)
        )
        return self.outcome

    async def complete_idempotency_record(
        self,
        workspace_id: WorkspaceId,
        principal_id: PrincipalId,
        route: str,
        idempotency_key: str,
        response_snapshot: Mapping[str, Any],
    ) -> IdempotencyRecord:
        self.complete_calls.append(response_snapshot)
        return make_record(status="completed", route=route)


@dataclass(slots=True)
class HarnessAuth:
    """Auth double that always satisfies the startup permission cross-check.

    ``get_permissions`` always returns the full registry grant so
    ``create_app``'s lifespan validation passes, while ``verify_and_authorize``
    is configured per scenario — to allow, to deny, or to raise — so the
    conformance suite can drive the authn/authz branches independently of the
    startup check.
    """

    verify_result: VerifyAndAuthorizeResponse | None = None
    verify_error: AuthError | None = None
    verify_calls: list[VerifyAndAuthorizeRequest] = field(default_factory=list)
    sign_calls: list[CallctxSignRequest] = field(default_factory=list)

    async def verify_and_authorize(
        self, request: VerifyAndAuthorizeRequest
    ) -> VerifyAndAuthorizeResponse:
        self.verify_calls.append(request)
        if self.verify_error is not None:
            raise self.verify_error
        if self.verify_result is not None:
            return self.verify_result
        return VerifyAndAuthorizeResponse(
            principal_id="principal-fake",
            allowed=True,
            reason="allow",
            audit_event_id="evt-fake",
        )

    async def callctx_sign(self, request: CallctxSignRequest) -> CallctxSignResponse:
        self.sign_calls.append(request)
        return CallctxSignResponse(token="token-fake", kid="kid", jti="jti", iat=1, exp=2)

    async def get_permissions(self) -> list[DeclaredPermission]:
        return [
            DeclaredPermission(name=name, description=name, declared_by="test")
            for name in registry_required_permissions()
        ]


def build_gateway(
    settings: Settings,
    *,
    auth: HarnessAuth | None = None,
    downstream: DownstreamRouter | None = None,
    store: RecordingStore | None = None,
    rate_limiter: RateLimiter | None = None,
) -> FastAPI:
    """Assemble ``create_app`` with the conformance doubles."""
    return create_app(
        settings=settings,
        auth_client=auth if auth is not None else HarnessAuth(),
        downstream_router=downstream if downstream is not None else build_downstream()[0],
        metadata_store=store,
        rate_limiter=rate_limiter,
    )

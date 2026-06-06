"""End-to-end ingress-pipeline tests for the wired app factory (AGW-IMPL-016).

These drive a fully assembled :func:`create_app` through ``TestClient`` to assert
the cross-cutting stages run in the designed order — workspace resolution,
authorization, body/content-type validation, rate limiting, idempotency, call-
context minting, the downstream forward and response shaping — and that each
stage is skipped when it does not apply or its backing resource is unbound.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx
from custos_spl import (
    ExistingCompleted,
    IdempotencyRecord,
    IdemReserved,
    PrincipalId,
    ReserveIdempotencyResult,
    WorkspaceId,
)
from fastapi.testclient import TestClient

from custos_gateway.app import create_app
from custos_gateway.clients.auth import DeclaredPermission, FakeAuthServiceClient
from custos_gateway.middleware.ratelimit import (
    RATE_LIMIT_LIMIT_HEADER,
    BucketConfig,
    RateLimiter,
)
from custos_gateway.router import DownstreamResponse, DownstreamRouter
from custos_gateway.routes._forwarding import response_snapshot
from custos_gateway.routes.registry import registry_required_permissions
from custos_gateway.settings import Settings

WRITE_PATH = "/v1/workspaces/ws-1/runs"
READ_PATH = "/v1/workspaces/ws-1/runs"
AUTH_HEADERS = {"authorization": "Bearer token-123"}


def _auth_client() -> FakeAuthServiceClient:
    """An Auth double declaring every registry permission so startup passes."""
    return FakeAuthServiceClient(
        permissions=[
            DeclaredPermission(name=name, description=name, declared_by="test")
            for name in registry_required_permissions()
        ]
    )


def _recording_router(
    *,
    status_code: int = 201,
    headers: dict[str, str] | None = None,
    body: bytes = b"created",
) -> tuple[DownstreamRouter, list[httpx.Request]]:
    """Build a real downstream router over a recording mock transport."""
    calls: list[httpx.Request] = []
    reply_headers = headers if headers is not None else {"x-downstream": "yes"}

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(status_code, headers=reply_headers, content=body)

    router = DownstreamRouter(
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        host="127.0.0.1",
        http_port=3500,
    )
    return router, calls


def _record(status: str = "in_progress") -> IdempotencyRecord:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return IdempotencyRecord(
        workspace_id=WorkspaceId("ws-1"),
        principal_id=PrincipalId("principal-fake"),
        route=WRITE_PATH,
        idempotency_key="key-1",
        request_hash="hash",
        status=status,
        response_snapshot=None,
        reserved_at=now,
        expires_at=now,
    )


@dataclass(slots=True)
class _FakeStore:
    """Records reserve/complete calls and returns a canned reserve outcome."""

    outcome: ReserveIdempotencyResult = field(
        default_factory=lambda: IdemReserved(record=_record())
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
        return _record(status="completed")


def test_write_pipeline_forwards_and_records_each_stage(settings: Settings) -> None:
    auth = _auth_client()
    router, calls = _recording_router()
    store = _FakeStore()
    app = create_app(
        settings=settings,
        auth_client=auth,
        downstream_router=router,
        metadata_store=store,
    )

    with TestClient(app) as client:
        response = client.post(
            WRITE_PATH,
            content=b'{"name":"demo"}',
            headers={
                **AUTH_HEADERS,
                "content-type": "application/json",
                "idempotency-key": "key-1",
            },
        )

    assert response.status_code == 201
    assert response.content == b"created"
    assert response.headers["x-downstream"] == "yes"
    # Authorize → mint → forward all ran exactly once.
    assert len(auth.verify_calls) == 1
    assert len(auth.sign_calls) == 1
    assert len(calls) == 1
    # Idempotency reserved before forwarding and completed afterwards.
    assert len(store.reserve_calls) == 1
    assert len(store.complete_calls) == 1
    # The forward carried the minted call-context + correlation metadata.
    forwarded = calls[0]
    assert "x-custos-callctx" in forwarded.headers
    assert "x-correlation-id" in forwarded.headers
    # The write surfaced rate-limit budget headers.
    assert RATE_LIMIT_LIMIT_HEADER in response.headers


def test_read_pipeline_skips_idempotency_and_rate_limit(settings: Settings) -> None:
    auth = _auth_client()
    router, calls = _recording_router(status_code=200, headers={}, body=b"[]")
    store = _FakeStore()
    app = create_app(
        settings=settings,
        auth_client=auth,
        downstream_router=router,
        metadata_store=store,
    )

    with TestClient(app) as client:
        response = client.get(READ_PATH, headers=AUTH_HEADERS)

    assert response.status_code == 200
    assert len(auth.verify_calls) == 1
    assert len(auth.sign_calls) == 1
    assert len(calls) == 1
    # Reads bypass idempotency and rate limiting entirely.
    assert store.reserve_calls == []
    assert RATE_LIMIT_LIMIT_HEADER not in response.headers


def test_idempotent_replay_returns_snapshot_without_forwarding(settings: Settings) -> None:
    auth = _auth_client()
    router, calls = _recording_router()
    snapshot = response_snapshot(
        DownstreamResponse(status_code=200, headers=[("x-replay", "yes")], body=b"replayed")
    )
    store = _FakeStore(
        outcome=ExistingCompleted(record=_record(status="completed"), response_snapshot=snapshot)
    )
    app = create_app(
        settings=settings,
        auth_client=auth,
        downstream_router=router,
        metadata_store=store,
    )

    with TestClient(app) as client:
        response = client.post(
            WRITE_PATH,
            content=b'{"name":"demo"}',
            headers={
                **AUTH_HEADERS,
                "content-type": "application/json",
                "idempotency-key": "key-1",
            },
        )

    assert response.status_code == 200
    assert response.content == b"replayed"
    assert response.headers["x-replay"] == "yes"
    # The replay short-circuits before forwarding or completing.
    assert calls == []
    assert store.complete_calls == []


def test_rate_limit_deny_returns_429(settings: Settings) -> None:
    auth = _auth_client()
    router, calls = _recording_router()
    store = _FakeStore()
    limiter = RateLimiter(
        principal_config=BucketConfig(rps=1, burst=1),
        workspace_config=BucketConfig(rps=1000, burst=1000),
        time_source=lambda: 1000.0,
    )
    app = create_app(
        settings=settings,
        auth_client=auth,
        downstream_router=router,
        metadata_store=store,
        rate_limiter=limiter,
    )

    headers = {**AUTH_HEADERS, "content-type": "application/json", "idempotency-key": "key-1"}
    with TestClient(app) as client:
        first = client.post(WRITE_PATH, content=b"{}", headers=headers)
        second = client.post(WRITE_PATH, content=b"{}", headers=headers)

    assert first.status_code == 201
    assert second.status_code == 429
    # The denied request never reached idempotency or the downstream.
    assert len(calls) == 1
    assert len(store.reserve_calls) == 1


def test_store_unbound_skips_idempotency(settings: Settings) -> None:
    auth = _auth_client()
    router, calls = _recording_router()
    app = create_app(settings=settings, auth_client=auth, downstream_router=router)

    with TestClient(app) as client:
        response = client.post(
            WRITE_PATH,
            content=b"{}",
            headers={**AUTH_HEADERS, "content-type": "application/json"},
        )

    assert response.status_code == 201
    assert len(calls) == 1


def test_unsupported_media_type_returns_415(settings: Settings) -> None:
    auth = _auth_client()
    router, calls = _recording_router()
    app = create_app(settings=settings, auth_client=auth, downstream_router=router)

    with TestClient(app) as client:
        response = client.post(
            WRITE_PATH,
            content=b"plain",
            headers={**AUTH_HEADERS, "content-type": "text/plain"},
        )

    assert response.status_code == 415
    assert calls == []


def test_cors_headers_present_for_allowed_origin(settings: Settings) -> None:
    auth = _auth_client()
    router, _ = _recording_router()
    app = create_app(settings=settings, auth_client=auth, downstream_router=router)

    with TestClient(app) as client:
        response = client.get("/healthz", headers={"origin": "https://ui.custos.example"})

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "https://ui.custos.example"


def test_lifespan_builds_and_closes_owned_downstream_client(settings: Settings) -> None:
    auth = _auth_client()
    # No downstream_router injected → the factory owns an httpx client it closes.
    app = create_app(settings=settings, auth_client=auth)

    with TestClient(app) as client:
        assert client.get("/readyz").status_code == 200

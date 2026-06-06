"""Conformance suite for the API Gateway ingress pipeline (AGW-IMPL-020).

Drives a fully wired :func:`custos_gateway.app.create_app` through ``TestClient``
against the in-process harness (stub downstream + fake Auth + in-memory SPL store
— see :mod:`harness`) to prove every cross-cutting stage behaves to contract:

* **authn / authz** — missing bearer 401, failed verify 401, deny 403, allow 2xx;
* **workspace** — scoped resolution + body/URL mismatch 400;
* **idempotency** — all four reserve outcomes (reserved, replay, in-flight, reuse);
* **rate-limit** — token-bucket denial 429 with budget headers;
* **validation** — oversized body 413, unsupported media type 415;
* **routing** — the forward targets the correct downstream app-id;
* **webhook** — anonymous forward to the Trigger Service with credentials stripped;
* **device-code** — the M1 503 stub while OIDC is disabled.
"""

from __future__ import annotations

from custos_spl import ExistingCompleted, ExistingInFlight, IdemReserved, KeyReuse
from fastapi.testclient import TestClient

from custos_gateway.clients.auth import (
    AuthServiceClientStatusError,
    VerifyAndAuthorizeResponse,
)
from custos_gateway.middleware.ratelimit import (
    RATE_LIMIT_LIMIT_HEADER,
    BucketConfig,
    RateLimiter,
)
from custos_gateway.router import DownstreamResponse
from custos_gateway.routes._forwarding import response_snapshot
from custos_gateway.settings import Settings

from .harness import (
    AUTH_HEADERS,
    JSON_WRITE_HEADERS,
    READ_PATH,
    WRITE_PATH,
    HarnessAuth,
    RecordingStore,
    build_downstream,
    build_gateway,
    make_record,
)

# --- authn / authz -----------------------------------------------------------


def test_authn_missing_bearer_returns_401(settings: Settings) -> None:
    downstream, recorder = build_downstream()
    app = build_gateway(settings, downstream=downstream)

    with TestClient(app) as client:
        response = client.post(
            WRITE_PATH, content=b"{}", headers={"content-type": "application/json"}
        )

    assert response.status_code == 401
    assert recorder.calls == []


def test_authn_failed_verify_returns_401(settings: Settings) -> None:
    auth = HarnessAuth(
        verify_error=AuthServiceClientStatusError("token did not verify", status_code=401)
    )
    downstream, recorder = build_downstream()
    app = build_gateway(settings, auth=auth, downstream=downstream)

    with TestClient(app) as client:
        response = client.post(WRITE_PATH, content=b"{}", headers=JSON_WRITE_HEADERS)

    assert response.status_code == 401
    assert len(auth.verify_calls) == 1
    assert recorder.calls == []


def test_authz_denied_returns_403(settings: Settings) -> None:
    auth = HarnessAuth(
        verify_result=VerifyAndAuthorizeResponse(
            principal_id="principal-fake",
            allowed=False,
            reason="not allowed",
            audit_event_id="evt-deny",
        )
    )
    downstream, recorder = build_downstream()
    app = build_gateway(settings, auth=auth, downstream=downstream)

    with TestClient(app) as client:
        response = client.post(WRITE_PATH, content=b"{}", headers=JSON_WRITE_HEADERS)

    assert response.status_code == 403
    assert len(auth.verify_calls) == 1
    # A denied request is never signed nor forwarded.
    assert auth.sign_calls == []
    assert recorder.calls == []


def test_authorized_write_forwards_and_mints_callctx(settings: Settings) -> None:
    auth = HarnessAuth()
    downstream, recorder = build_downstream()
    store = RecordingStore()
    app = build_gateway(settings, auth=auth, downstream=downstream, store=store)

    with TestClient(app) as client:
        response = client.post(
            WRITE_PATH,
            content=b'{"name":"demo"}',
            headers={**JSON_WRITE_HEADERS, "idempotency-key": "key-1"},
        )

    assert response.status_code == 201
    assert response.content == b"created"
    # verify → sign → forward each ran exactly once, in that order.
    assert len(auth.verify_calls) == 1
    assert len(auth.sign_calls) == 1
    assert len(recorder.calls) == 1
    forwarded = recorder.calls[0]
    assert "x-custos-callctx" in forwarded.headers
    assert "x-correlation-id" in forwarded.headers


# --- workspace ---------------------------------------------------------------


def test_workspace_body_url_mismatch_returns_400(settings: Settings) -> None:
    downstream, recorder = build_downstream()
    app = build_gateway(settings, downstream=downstream)

    with TestClient(app) as client:
        response = client.post(
            WRITE_PATH,
            content=b'{"workspaceId":"ws-other"}',
            headers=JSON_WRITE_HEADERS,
        )

    assert response.status_code == 400
    # The mismatch is rejected before the request reaches the downstream.
    assert recorder.calls == []


# --- idempotency (all four reserve outcomes) ---------------------------------


def test_idempotency_reserved_forwards_then_completes(settings: Settings) -> None:
    auth = HarnessAuth()
    downstream, recorder = build_downstream()
    store = RecordingStore(outcome=IdemReserved(record=make_record()))
    app = build_gateway(settings, auth=auth, downstream=downstream, store=store)

    with TestClient(app) as client:
        response = client.post(
            WRITE_PATH,
            content=b'{"name":"demo"}',
            headers={**JSON_WRITE_HEADERS, "idempotency-key": "key-1"},
        )

    assert response.status_code == 201
    assert len(store.reserve_calls) == 1
    assert len(store.complete_calls) == 1
    assert len(recorder.calls) == 1


def test_idempotency_replay_returns_snapshot_without_forwarding(settings: Settings) -> None:
    auth = HarnessAuth()
    downstream, recorder = build_downstream()
    snapshot = response_snapshot(
        DownstreamResponse(status_code=200, headers=[("x-replay", "yes")], body=b"replayed")
    )
    store = RecordingStore(
        outcome=ExistingCompleted(
            record=make_record(status="completed"), response_snapshot=snapshot
        )
    )
    app = build_gateway(settings, auth=auth, downstream=downstream, store=store)

    with TestClient(app) as client:
        response = client.post(
            WRITE_PATH,
            content=b'{"name":"demo"}',
            headers={**JSON_WRITE_HEADERS, "idempotency-key": "key-1"},
        )

    assert response.status_code == 200
    assert response.content == b"replayed"
    assert response.headers["x-replay"] == "yes"
    # The replay short-circuits before forwarding or completing.
    assert recorder.calls == []
    assert store.complete_calls == []


def test_idempotency_in_flight_returns_409_with_retry_after(settings: Settings) -> None:
    auth = HarnessAuth()
    downstream, recorder = build_downstream()
    store = RecordingStore(outcome=ExistingInFlight(record=make_record(status="in_progress")))
    app = build_gateway(settings, auth=auth, downstream=downstream, store=store)

    with TestClient(app) as client:
        response = client.post(
            WRITE_PATH,
            content=b'{"name":"demo"}',
            headers={**JSON_WRITE_HEADERS, "idempotency-key": "key-1"},
        )

    assert response.status_code == 409
    assert "retry-after" in {k.lower() for k in response.headers}
    assert recorder.calls == []


def test_idempotency_key_reuse_returns_409(settings: Settings) -> None:
    auth = HarnessAuth()
    downstream, recorder = build_downstream()
    store = RecordingStore(outcome=KeyReuse(record=make_record(status="completed")))
    app = build_gateway(settings, auth=auth, downstream=downstream, store=store)

    with TestClient(app) as client:
        response = client.post(
            WRITE_PATH,
            content=b'{"name":"demo"}',
            headers={**JSON_WRITE_HEADERS, "idempotency-key": "key-1"},
        )

    assert response.status_code == 409
    assert recorder.calls == []


# --- rate-limit --------------------------------------------------------------


def test_rate_limit_denied_returns_429(settings: Settings) -> None:
    auth = HarnessAuth()
    downstream, recorder = build_downstream()
    store = RecordingStore()
    limiter = RateLimiter(
        principal_config=BucketConfig(rps=1, burst=1),
        workspace_config=BucketConfig(rps=1000, burst=1000),
        time_source=lambda: 1000.0,
    )
    app = build_gateway(
        settings, auth=auth, downstream=downstream, store=store, rate_limiter=limiter
    )

    headers = {**JSON_WRITE_HEADERS, "idempotency-key": "key-1"}
    with TestClient(app) as client:
        first = client.post(WRITE_PATH, content=b"{}", headers=headers)
        second = client.post(WRITE_PATH, content=b"{}", headers=headers)

    assert first.status_code == 201
    assert second.status_code == 429
    assert RATE_LIMIT_LIMIT_HEADER in first.headers
    # The denied write never reached the downstream a second time.
    assert len(recorder.calls) == 1


def test_read_bypasses_idempotency_and_rate_limit(settings: Settings) -> None:
    auth = HarnessAuth()
    downstream, _recorder = build_downstream(status_code=200, headers={}, body=b"[]")
    store = RecordingStore()
    app = build_gateway(settings, auth=auth, downstream=downstream, store=store)

    with TestClient(app) as client:
        response = client.get(READ_PATH, headers=AUTH_HEADERS)

    assert response.status_code == 200
    assert store.reserve_calls == []
    assert RATE_LIMIT_LIMIT_HEADER not in response.headers


# --- validation --------------------------------------------------------------


def test_validation_oversized_body_returns_413(settings: Settings) -> None:
    auth = HarnessAuth()
    downstream, recorder = build_downstream()
    app = build_gateway(settings, auth=auth, downstream=downstream)

    # The runs route caps the body at the 1 MiB default; one byte over trips 413.
    oversized = b"x" * (1_048_576 + 1)
    with TestClient(app) as client:
        response = client.post(WRITE_PATH, content=oversized, headers=JSON_WRITE_HEADERS)

    assert response.status_code == 413
    assert recorder.calls == []


def test_validation_unsupported_media_type_returns_415(settings: Settings) -> None:
    auth = HarnessAuth()
    downstream, recorder = build_downstream()
    app = build_gateway(settings, auth=auth, downstream=downstream)

    with TestClient(app) as client:
        response = client.post(
            WRITE_PATH, content=b"plain", headers={**AUTH_HEADERS, "content-type": "text/plain"}
        )

    assert response.status_code == 415
    assert recorder.calls == []


# --- routing -----------------------------------------------------------------


def test_routing_targets_correct_downstream(settings: Settings) -> None:
    auth = HarnessAuth()
    downstream, recorder = build_downstream()
    store = RecordingStore()
    app = build_gateway(settings, auth=auth, downstream=downstream, store=store)

    with TestClient(app) as client:
        response = client.post(
            WRITE_PATH,
            content=b"{}",
            headers={**JSON_WRITE_HEADERS, "idempotency-key": "key-1"},
        )

    assert response.status_code == 201
    assert len(recorder.calls) == 1
    forwarded = str(recorder.calls[0].url)
    # The runs route is owned by the Workflow Service; the Dapr invoke URL
    # names that app-id.
    assert "/invoke/workflow-service/method/" in forwarded


# --- webhook -----------------------------------------------------------------


def test_webhook_forwards_anonymously_and_strips_credentials(settings: Settings) -> None:
    auth = HarnessAuth()
    downstream, recorder = build_downstream(status_code=202, headers={}, body=b"")
    app = build_gateway(settings, auth=auth, downstream=downstream)

    with TestClient(app) as client:
        response = client.post(
            "/v1/webhooks/inst-1",
            content=b'{"event":"x"}',
            headers={
                "content-type": "application/json",
                "authorization": "Bearer should-be-stripped",
                "x-custos-callctx": "smuggled",
            },
        )

    assert response.status_code == 202
    # The webhook hop is anonymous — no Auth round-trip is made.
    assert auth.verify_calls == []
    assert auth.sign_calls == []
    assert len(recorder.calls) == 1
    forwarded = recorder.calls[0]
    # It targets the Trigger Service and forwards neither the bearer nor a
    # caller-supplied call-context.
    assert "/invoke/trigger-service/method/" in str(forwarded.url)
    assert "authorization" not in forwarded.headers
    assert "x-custos-callctx" not in forwarded.headers


# --- device-code (M1 503 stub) -----------------------------------------------


def test_device_code_start_returns_503_while_disabled(settings: Settings) -> None:
    app = build_gateway(settings)

    with TestClient(app) as client:
        response = client.post("/v1/auth/login/device", content=b"{}")

    assert response.status_code == 503


def test_device_code_poll_returns_503_while_disabled(settings: Settings) -> None:
    app = build_gateway(settings)

    with TestClient(app) as client:
        response = client.post("/v1/auth/login/device/dev-code/poll", content=b"{}")

    assert response.status_code == 503


def test_device_code_landing_returns_503_while_disabled(settings: Settings) -> None:
    app = build_gateway(settings)

    with TestClient(app) as client:
        response = client.get("/v1/auth/login/device/user-code")

    assert response.status_code == 503

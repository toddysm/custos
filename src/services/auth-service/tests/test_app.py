"""Integration smoke for :func:`custos_auth.create_app` (AS-IMPL-004)."""

from __future__ import annotations

import pytest
from custos_spl import MigrationRequired
from fastapi.testclient import TestClient

from custos_auth import create_app
from custos_auth.providers import Providers
from custos_auth.settings import load_settings
from tests._fakes import FakeAuthAdapter, FakeMetadataAdapter

_ENV = {
    "CUSTOS_AUTH_STORE_DSN": "postgresql://u:p@h:5432/custos_auth",
    "CUSTOS_AUTH_METADATA_STORE_DSN": "postgresql://u:p@h:5432/custos_meta",
}


def _providers(
    *,
    auth_revs: set[int] | None = None,
    meta_revs: set[int] | None = None,
) -> Providers:
    return Providers(
        auth_store=FakeAuthAdapter(applied_revisions=auth_revs),  # type: ignore[arg-type]
        metadata_store=FakeMetadataAdapter(applied_revisions=meta_revs),  # type: ignore[arg-type]
    )


def test_create_app_returns_a_fastapi_instance() -> None:
    app = create_app(settings=load_settings(_ENV), providers=_providers())
    from fastapi import FastAPI

    assert isinstance(app, FastAPI)


def test_healthz_returns_200_independent_of_lifespan_state() -> None:
    # /healthz is a flat liveness probe: it must not depend on
    # app.state.ready, app.state.providers, or anything else the
    # lifespan sets. Exercise that by mounting the health router on a
    # bare FastAPI app without ever entering the create_app lifespan.
    from fastapi import FastAPI

    from custos_auth.health import router as health_router

    bare = FastAPI()
    bare.include_router(health_router)
    with TestClient(bare) as client:
        resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_readyz_returns_200_when_schema_gate_passes() -> None:
    app = create_app(settings=load_settings(_ENV), providers=_providers())
    with TestClient(app) as client:
        resp = client.get("/readyz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ready"}


def test_readyz_returns_503_when_schema_gate_fails() -> None:
    # When the schema gate fails, the lifespan re-raises MigrationRequired
    # before the app serves any request, so /readyz is unreachable from a
    # successfully-entered TestClient. Verify the abort path here; the
    # readyz-during-startup branch is covered by the dedicated test below
    # via direct app.state manipulation.
    app = create_app(
        settings=load_settings(_ENV),
        providers=_providers(auth_revs=set(), meta_revs={1, 2, 3, 4}),
    )
    with pytest.raises(MigrationRequired) as exc_info, TestClient(app):
        pass
    assert ("AuthStoreProvider", 1) in exc_info.value.gaps


def test_readyz_returns_503_during_lifespan_startup_before_ready() -> None:
    # Defensive coverage for the brief window between the lifespan setting
    # app.state.ready = False at the top of startup and the gate flipping
    # it to True. We simulate that window by handing /readyz a request
    # against an app whose state.ready is False but whose lifespan has
    # not yet been entered.
    from fastapi import FastAPI

    bare = FastAPI()
    from custos_auth.health import router as health_router

    bare.include_router(health_router)
    bare.state.ready = False
    with TestClient(bare) as client:
        resp = client.get("/readyz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "not_ready"
    assert body["detail"] == "auth-service has not finished startup"


def test_app_state_carries_schema_gate_error_on_failure() -> None:
    app = create_app(
        settings=load_settings(_ENV),
        providers=_providers(auth_revs=set(), meta_revs={1, 2, 3, 4}),
    )
    with pytest.raises(MigrationRequired), TestClient(app):
        pass
    # The lifespan stashes the gap on app.state before re-raising so
    # forensic inspection (tests, post-mortem `kubectl exec` into a
    # crash-looped pod, etc.) can confirm the diagnostic.
    assert isinstance(app.state.schema_gate_error, MigrationRequired)
    assert app.state.ready is False


def test_app_state_carries_providers_after_startup() -> None:
    providers = _providers()
    app = create_app(settings=load_settings(_ENV), providers=providers)
    with TestClient(app):
        assert app.state.providers is providers


def test_app_state_carries_settings_after_startup() -> None:
    settings = load_settings(_ENV)
    app = create_app(settings=settings, providers=_providers())
    with TestClient(app):
        assert app.state.settings is settings


@pytest.mark.parametrize("path", ["/healthz", "/readyz"])
def test_probes_do_not_require_authentication(path: str) -> None:
    app = create_app(settings=load_settings(_ENV), providers=_providers())
    with TestClient(app) as client:
        resp = client.get(path)
    assert resp.status_code in (200, 503)


def test_lifespan_subscribes_authz_cache_to_local_bus() -> None:
    # AS-IMPL-012: when the publisher is LocalBindingChangedBus
    # (default), the lifespan subscribes the authz cache's
    # on_binding_changed handler so binding-changed events on the
    # local replica invalidate the cache synchronously.
    from custos_auth.binding_events import LocalBindingChangedBus

    providers = _providers()
    assert isinstance(providers.binding_changed_publisher, LocalBindingChangedBus)
    app = create_app(settings=load_settings(_ENV), providers=providers)
    with TestClient(app):
        bus = providers.binding_changed_publisher
        assert isinstance(bus, LocalBindingChangedBus)
        # The handler list contains the cache invalidator. Bound
        # methods compare by ``__self__`` + ``__func__`` so equality
        # is the right check, not identity.
        assert any(
            getattr(h, "__self__", None) is providers.authz_cache
            and getattr(h, "__func__", None) is type(providers.authz_cache).on_binding_changed
            for h in bus.handlers
        )


def test_lifespan_starts_and_stops_binding_changed_subscriber() -> None:
    # The subscriber Protocol is started with the cache's handler
    # and stopped cleanly on shutdown so background tasks (in a real
    # Redis subscriber) do not leak.
    from custos_auth.binding_events import NoOpBindingChangedSubscriber

    providers = _providers()
    sub = providers.binding_changed_subscriber
    assert isinstance(sub, NoOpBindingChangedSubscriber)
    app = create_app(settings=load_settings(_ENV), providers=providers)
    with TestClient(app):
        assert sub.started is True
        # Bound methods compare by ``__self__`` + ``__func__``.
        assert sub.handler is not None
        assert getattr(sub.handler, "__self__", None) is providers.authz_cache
        assert (
            getattr(sub.handler, "__func__", None) is type(providers.authz_cache).on_binding_changed
        )
    # Shutdown ran the subscriber's stop().
    assert sub.stopped is True


def test_revoke_then_recheck_evicts_cache_in_one_round_trip() -> None:
    # AS-IMPL-012 acceptance criterion: a revoke (publish on the
    # local bus) followed by a recheck must see the new decision —
    # the cache row must be gone after the publish.
    import asyncio

    from custos_spl.interfaces.auth_store import WorkspaceScope

    from custos_auth.binding_events import (
        BindingChangedEvent,
        LocalBindingChangedBus,
    )

    providers = _providers()
    app = create_app(settings=load_settings(_ENV), providers=providers)
    with TestClient(app):
        cache = providers.authz_cache
        bus = providers.binding_changed_publisher
        assert isinstance(bus, LocalBindingChangedBus)
        # Prime the cache with a stale "allow" decision.
        cache.put("user-1", "ws-1", "workflow:read", allowed=True, reason="allow-bound")
        assert cache.get("user-1", "ws-1", "workflow:read") is not None
        # Publish a revoke; the in-process bus invokes the cache
        # invalidator synchronously.
        event = BindingChangedEvent(
            principal_id="user-1",
            role_id="role:workspace.viewer",
            scope=WorkspaceScope(workspace_id="ws-1"),  # type: ignore[arg-type]
            action="revoked",
            binding_id="rb-1",
        )
        asyncio.run(bus.publish(event))
        # Cache row evicted — recheck path will go to the auth store
        # and observe the new decision.
        assert cache.get("user-1", "ws-1", "workflow:read") is None


# ---------------------------------------------------------------------------
# Phase G (AS-IMPL-017 / AS-IMPL-018) lifespan wiring
# ---------------------------------------------------------------------------


def test_lifespan_builds_call_context_signer_in_dev_mode() -> None:
    # No CUSTOS_AUTH_CALL_CONTEXT_KEY_REF set + environment defaults
    # to "development" — the lifespan generates an ephemeral key.
    env = dict(_ENV, CUSTOS_AUTH_CALL_CONTEXT_KEY_ROTATION="0")
    app = create_app(settings=load_settings(env), providers=_providers())
    with TestClient(app):
        from custos_auth.callctx_keyring import KeyRing
        from custos_auth.callctx_signer import (
            CallContextSigner,
            StaticSigningKeyResolver,
        )

        assert isinstance(app.state.call_context_key_ring, KeyRing)
        assert isinstance(app.state.call_context_signing_key_resolver, StaticSigningKeyResolver)
        assert isinstance(app.state.call_context_signer, CallContextSigner)
        # Rotation disabled => no rotation task spawned.
        assert app.state.call_context_rotation_task is None


def test_lifespan_spawns_and_cancels_rotation_task() -> None:
    # When rotation > 0, the lifespan creates an asyncio task and
    # cancels it cleanly on shutdown.
    env = dict(_ENV, CUSTOS_AUTH_CALL_CONTEXT_KEY_ROTATION="600")
    app = create_app(settings=load_settings(env), providers=_providers())
    with TestClient(app):
        task = app.state.call_context_rotation_task
        assert task is not None
        assert not task.done()
    # After the TestClient exit, the lifespan shutdown should have
    # cancelled the task.
    task = app.state.call_context_rotation_task
    assert task is not None
    assert task.done()
    assert task.cancelled() or task.exception() is None


def test_lifespan_refuses_to_start_in_production_without_key_ref() -> None:
    # Outside development the lifespan demands an explicit Dapr key
    # reference; without it the helper raises a RuntimeError that
    # crash-loops the pod with an operator-actionable message.
    env = dict(
        _ENV,
        ENVIRONMENT="staging",
        # Provide a non-empty verifier URL so the call-context
        # middleware accepts the non-dev environment.
        CUSTOS_AUTH_CALLCTX_VERIFIER_URL="http://auth-service/.well-known/jwks.json",
    )
    app = create_app(settings=load_settings(env), providers=_providers())
    with pytest.raises(RuntimeError, match="CUSTOS_AUTH_CALL_CONTEXT_KEY_REF"), TestClient(app):
        pass


def test_lifespan_loads_signing_key_from_dapr_secret_reference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # When CUSTOS_AUTH_CALL_CONTEXT_KEY_REF is set, the lifespan
    # builds a DaprSecretsSigningKeyResolver and fetches the PEM via
    # the injected HTTP client. We swap httpx.AsyncClient for an
    # in-memory fake so the test is hermetic.
    from custos_auth.callctx_signer import SigningKey

    pem = SigningKey.generate().private_pem().decode("utf-8")

    class _FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, str]:
            return {"call-context-key": pem}

    class _FakeAsyncClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        async def __aenter__(self) -> _FakeAsyncClient:
            return self

        async def __aexit__(self, *exc_info: object) -> None:
            return None

        async def get(self, _url: str) -> _FakeResponse:
            return _FakeResponse()

    import httpx

    monkeypatch.setattr(httpx, "AsyncClient", _FakeAsyncClient)

    env = dict(
        _ENV,
        CUSTOS_AUTH_CALL_CONTEXT_KEY_REF="call-context-key",
    )
    app = create_app(settings=load_settings(env), providers=_providers())
    with TestClient(app):
        from custos_auth.callctx_signer import SigningKey as _SigningKey

        active = app.state.call_context_key_ring.active
        assert isinstance(active, _SigningKey)

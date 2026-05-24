"""Tests for :mod:`custos_auth.authn` and ``POST /v1/auth/verify`` (AS-IMPL-014, #249)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from custos_spl.ids import PrincipalId, ServiceTokenId, WorkspaceId
from custos_spl.interfaces.auth_store import ServiceAccount, ServiceToken
from fastapi.testclient import TestClient

from custos_auth.audit import (
    EVENT_AUTHN_FAILURE,
    EVENT_AUTHN_SUCCESS,
    EVENT_TOKEN_USED,
    PLATFORM_WORKSPACE_ID,
)
from custos_auth.authn import (
    REASON_EXPIRED,
    REASON_MALFORMED,
    REASON_REVOKED,
    REASON_SA_DISABLED,
    REASON_SA_MISSING,
    REASON_UNKNOWN,
    verify_token,
)
from custos_auth.authn_cache import AuthnCache
from custos_auth.tokens import hash_token, mint_token
from tests._fakes import FakeAuthAdapter, FakeMetadataAdapter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_service_account(
    store: FakeAuthAdapter,
    principal_id: str,
    workspace_id: str,
    *,
    disabled: bool = False,
) -> ServiceAccount:
    now = datetime.now(UTC)
    sa = ServiceAccount(
        kind="serviceAccount",
        principal_id=PrincipalId(principal_id),
        workspace_id=WorkspaceId(workspace_id),
        display_name=principal_id,
        disabled_at=now if disabled else None,
        disabled_reason="rotation" if disabled else None,
        created_at=now,
    )
    store.principals[principal_id] = sa
    return sa


def _mint_and_seed(
    store: FakeAuthAdapter,
    service_account_id: str,
    *,
    token_id: str = "tok-1",
    ttl: timedelta = timedelta(days=30),
    revoked: bool = False,
    expired: bool = False,
) -> str:
    """Mint a real bearer + insert the matching SPL row. Returns plaintext."""
    plaintext, h = mint_token()
    now = datetime.now(UTC)
    if expired:
        issued = now - timedelta(days=10)
        expires = now - timedelta(seconds=1)
    else:
        issued = now
        expires = now + ttl
    store.service_tokens[token_id] = ServiceToken(
        token_id=ServiceTokenId(token_id),
        service_account_id=PrincipalId(service_account_id),
        hash=h,
        issued_at=issued,
        expires_at=expires,
        revoked_at=now if revoked else None,
        revoked_by=PrincipalId("admin") if revoked else None,
        revoked_reason="compromised" if revoked else None,
    )
    return plaintext


# ---------------------------------------------------------------------------
# verify_token — unit tests against the fakes directly
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_verify_token_returns_sa_on_happy_path_and_audits_token_used() -> None:
    auth = FakeAuthAdapter()
    meta = FakeMetadataAdapter()
    cache = AuthnCache(ttl_seconds=30)
    _seed_service_account(auth, "sa-1", "ws-1")
    plaintext = _mint_and_seed(auth, "sa-1", token_id="tok-1")

    principal = await verify_token(
        plaintext,
        auth_store=auth,  # type: ignore[arg-type]
        metadata_store=meta,  # type: ignore[arg-type]
        authn_cache=cache,
    )
    assert isinstance(principal, ServiceAccount)
    assert principal.principal_id == "sa-1"

    events = [event.event_type for _ws, event in meta.append_audit_calls]
    # First-use after rotation → token.used + authn.success in that order.
    assert events == [EVENT_TOKEN_USED, EVENT_AUTHN_SUCCESS]
    success = next(e for _ws, e in meta.append_audit_calls if e.event_type == EVENT_AUTHN_SUCCESS)
    assert success.payload == {"cache_hit": False}

    # The cache must now carry the row so the next verify hits.
    cached = cache.get(hash_token(plaintext))
    assert cached is not None
    assert cached.token_id == "tok-1"


@pytest.mark.asyncio
async def test_verify_token_cache_hit_skips_token_used() -> None:
    # AS-IMPL-014 acceptance criterion: ``token.used`` fires on
    # first use after rotation, NOT on every request. The 30 s
    # cache rate-limits the row.
    auth = FakeAuthAdapter()
    meta = FakeMetadataAdapter()
    cache = AuthnCache(ttl_seconds=30)
    _seed_service_account(auth, "sa-1", "ws-1")
    plaintext = _mint_and_seed(auth, "sa-1", token_id="tok-1")

    # First call → primes the cache.
    await verify_token(
        plaintext,
        auth_store=auth,  # type: ignore[arg-type]
        metadata_store=meta,  # type: ignore[arg-type]
        authn_cache=cache,
    )
    meta.append_audit_calls.clear()

    # Second call → cache hit → only authn.success(cache_hit=True).
    principal = await verify_token(
        plaintext,
        auth_store=auth,  # type: ignore[arg-type]
        metadata_store=meta,  # type: ignore[arg-type]
        authn_cache=cache,
    )
    assert isinstance(principal, ServiceAccount)
    events = [event.event_type for _ws, event in meta.append_audit_calls]
    assert events == [EVENT_AUTHN_SUCCESS]
    success = meta.append_audit_calls[0][1]
    assert success.payload == {"cache_hit": True}


@pytest.mark.asyncio
async def test_verify_token_returns_none_for_malformed_input() -> None:
    auth = FakeAuthAdapter()
    meta = FakeMetadataAdapter()
    cache = AuthnCache(ttl_seconds=30)

    principal = await verify_token(
        "not-a-custos-token",
        auth_store=auth,  # type: ignore[arg-type]
        metadata_store=meta,  # type: ignore[arg-type]
        authn_cache=cache,
    )
    assert principal is None
    ws, event = meta.append_audit_calls[0]
    assert event.event_type == EVENT_AUTHN_FAILURE
    assert event.payload == {"reason": REASON_MALFORMED}
    # Malformed input cannot be attributed to a workspace.
    assert ws == PLATFORM_WORKSPACE_ID


@pytest.mark.asyncio
async def test_verify_token_returns_none_for_unknown_token() -> None:
    auth = FakeAuthAdapter()
    meta = FakeMetadataAdapter()
    cache = AuthnCache(ttl_seconds=30)
    # No SPL row seeded → hash lookup misses.
    plaintext, _ = mint_token()

    principal = await verify_token(
        plaintext,
        auth_store=auth,  # type: ignore[arg-type]
        metadata_store=meta,  # type: ignore[arg-type]
        authn_cache=cache,
    )
    assert principal is None
    ws, event = meta.append_audit_calls[0]
    assert event.event_type == EVENT_AUTHN_FAILURE
    assert event.payload == {"reason": REASON_UNKNOWN}
    assert ws == PLATFORM_WORKSPACE_ID
    # The hash MUST NOT be carried on the failure row; we audit by
    # token_id (which we don't have for unknown tokens) but never
    # by hash.
    assert "hash" not in event.subject
    assert "hash" not in event.payload


@pytest.mark.asyncio
async def test_verify_token_returns_none_for_revoked_token() -> None:
    auth = FakeAuthAdapter()
    meta = FakeMetadataAdapter()
    cache = AuthnCache(ttl_seconds=30)
    _seed_service_account(auth, "sa-1", "ws-1")
    plaintext = _mint_and_seed(auth, "sa-1", token_id="tok-1", revoked=True)

    principal = await verify_token(
        plaintext,
        auth_store=auth,  # type: ignore[arg-type]
        metadata_store=meta,  # type: ignore[arg-type]
        authn_cache=cache,
    )
    assert principal is None
    _ws, event = meta.append_audit_calls[0]
    assert event.event_type == EVENT_AUTHN_FAILURE
    assert event.payload == {"reason": REASON_REVOKED}
    assert event.subject == {"token_id": "tok-1", "service_account_id": "sa-1"}


@pytest.mark.asyncio
async def test_verify_token_returns_none_for_expired_token() -> None:
    auth = FakeAuthAdapter()
    meta = FakeMetadataAdapter()
    cache = AuthnCache(ttl_seconds=30)
    _seed_service_account(auth, "sa-1", "ws-1")
    plaintext = _mint_and_seed(auth, "sa-1", token_id="tok-1", expired=True)

    principal = await verify_token(
        plaintext,
        auth_store=auth,  # type: ignore[arg-type]
        metadata_store=meta,  # type: ignore[arg-type]
        authn_cache=cache,
    )
    assert principal is None
    _ws, event = meta.append_audit_calls[0]
    assert event.event_type == EVENT_AUTHN_FAILURE
    assert event.payload == {"reason": REASON_EXPIRED}


@pytest.mark.asyncio
async def test_verify_token_returns_none_when_owning_sa_is_disabled() -> None:
    auth = FakeAuthAdapter()
    meta = FakeMetadataAdapter()
    cache = AuthnCache(ttl_seconds=30)
    _seed_service_account(auth, "sa-1", "ws-1", disabled=True)
    plaintext = _mint_and_seed(auth, "sa-1", token_id="tok-1")

    principal = await verify_token(
        plaintext,
        auth_store=auth,  # type: ignore[arg-type]
        metadata_store=meta,  # type: ignore[arg-type]
        authn_cache=cache,
    )
    assert principal is None
    ws, event = meta.append_audit_calls[0]
    assert event.event_type == EVENT_AUTHN_FAILURE
    assert event.payload == {"reason": REASON_SA_DISABLED}
    # SA-disabled audits should key under the SA's workspace.
    assert ws == "ws-1"


@pytest.mark.asyncio
async def test_verify_token_returns_none_when_owning_sa_is_missing() -> None:
    # Defensive: SPL contract says SAs are not hard-deleted, but the
    # verifier still distinguishes ``sa-missing`` from
    # ``sa-disabled`` so operators reading the audit pipeline can
    # tell a data-integrity violation from an operator-driven
    # disable.
    auth = FakeAuthAdapter()
    meta = FakeMetadataAdapter()
    cache = AuthnCache(ttl_seconds=30)
    plaintext = _mint_and_seed(auth, "sa-1", token_id="tok-1")
    # Notice: no SA row was seeded.

    principal = await verify_token(
        plaintext,
        auth_store=auth,  # type: ignore[arg-type]
        metadata_store=meta,  # type: ignore[arg-type]
        authn_cache=cache,
    )
    assert principal is None
    _ws, event = meta.append_audit_calls[0]
    assert event.payload == {"reason": REASON_SA_MISSING}


@pytest.mark.asyncio
async def test_verify_token_bypass_mode_still_works_without_cache() -> None:
    # ``CUSTOS_AUTH_AUTHN_CACHE_TTL=0`` is the documented bypass
    # knob: every verify performs a full SPL lookup.
    auth = FakeAuthAdapter()
    meta = FakeMetadataAdapter()
    cache = AuthnCache(ttl_seconds=0)
    _seed_service_account(auth, "sa-1", "ws-1")
    plaintext = _mint_and_seed(auth, "sa-1", token_id="tok-1")

    # Two consecutive calls in bypass mode → two ``token.used``
    # rows because the cache is never primed.
    await verify_token(
        plaintext,
        auth_store=auth,  # type: ignore[arg-type]
        metadata_store=meta,  # type: ignore[arg-type]
        authn_cache=cache,
    )
    await verify_token(
        plaintext,
        auth_store=auth,  # type: ignore[arg-type]
        metadata_store=meta,  # type: ignore[arg-type]
        authn_cache=cache,
    )
    used = [e for _ws, e in meta.append_audit_calls if e.event_type == EVENT_TOKEN_USED]
    assert len(used) == 2


# ---------------------------------------------------------------------------
# POST /v1/auth/verify — HTTP layer
# ---------------------------------------------------------------------------


def test_verify_endpoint_returns_200_with_principal_envelope(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    plaintext = _mint_and_seed(fake_auth_store, "sa-1", token_id="tok-1")

    # No call-context header — the verify endpoint is on the bypass
    # list because it *produces* call-context for downstream
    # services rather than consuming it.
    resp = client.post("/v1/auth/verify", json={"token": plaintext})
    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "serviceAccount"
    assert body["principal_id"] == "sa-1"
    assert body["workspace_id"] == "ws-1"


def test_verify_endpoint_returns_401_for_unknown_token(client: TestClient) -> None:
    plaintext, _ = mint_token()
    resp = client.post("/v1/auth/verify", json={"token": plaintext})
    assert resp.status_code == 401
    # Standard call-context error envelope so callers can branch on
    # ``error.code``. Generic ``"unauthenticated"`` keeps the
    # endpoint from acting as an oracle.
    assert resp.json()["error"]["code"] == "unauthenticated"


def test_verify_endpoint_returns_401_for_revoked_token(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    plaintext = _mint_and_seed(fake_auth_store, "sa-1", token_id="tok-1", revoked=True)
    resp = client.post("/v1/auth/verify", json={"token": plaintext})
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "unauthenticated"


def test_verify_endpoint_returns_401_for_expired_token(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    plaintext = _mint_and_seed(fake_auth_store, "sa-1", token_id="tok-1", expired=True)
    resp = client.post("/v1/auth/verify", json={"token": plaintext})
    assert resp.status_code == 401


def test_verify_endpoint_returns_401_for_disabled_sa(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1", disabled=True)
    plaintext = _mint_and_seed(fake_auth_store, "sa-1", token_id="tok-1")
    resp = client.post("/v1/auth/verify", json={"token": plaintext})
    assert resp.status_code == 401


def test_verify_endpoint_returns_401_for_malformed_token(client: TestClient) -> None:
    resp = client.post("/v1/auth/verify", json={"token": "not-a-token"})
    assert resp.status_code == 401


def test_verify_endpoint_rejects_empty_body(client: TestClient) -> None:
    resp = client.post("/v1/auth/verify", json={})
    assert resp.status_code == 422


def test_verify_endpoint_rejects_extra_fields(client: TestClient) -> None:
    # extra="forbid" on VerifyRequest — typos surface immediately
    # rather than being silently ignored.
    plaintext, _ = mint_token()
    resp = client.post("/v1/auth/verify", json={"token": plaintext, "extra": "x"})
    assert resp.status_code == 422


def test_verify_endpoint_caches_across_requests(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
    fake_metadata_store: FakeMetadataAdapter,
) -> None:
    # First request primes the cache → token.used row.
    # Second request inside the TTL window must NOT emit token.used.
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    plaintext = _mint_and_seed(fake_auth_store, "sa-1", token_id="tok-1")

    r1 = client.post("/v1/auth/verify", json={"token": plaintext})
    r2 = client.post("/v1/auth/verify", json={"token": plaintext})
    assert r1.status_code == 200
    assert r2.status_code == 200
    used = [
        e for _ws, e in fake_metadata_store.append_audit_calls if e.event_type == EVENT_TOKEN_USED
    ]
    assert len(used) == 1


def test_revoke_event_evicts_authn_cache_immediately(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
    providers: object,
) -> None:
    """AS-IMPL-014 acceptance criterion: a revoke event evicts the
    authn cache so the very next verify on the same replica returns
    401.

    The lifespan code in :mod:`custos_auth.__init__` subscribes the
    per-pod authn cache to the in-process
    :class:`LocalTokenRevokedBus`. Once AS-IMPL-015 lands, the
    revoke endpoint will publish a :class:`TokenRevokedEvent` on
    that bus right after marking the SPL row revoked. This test
    simulates the publish side directly.
    """
    import anyio

    from custos_auth.providers import Providers
    from custos_auth.token_revoked_events import (
        LocalTokenRevokedBus,
        TokenRevokedEvent,
    )

    assert isinstance(providers, Providers)

    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    plaintext = _mint_and_seed(fake_auth_store, "sa-1", token_id="tok-1")

    # Prime the cache.
    r1 = client.post("/v1/auth/verify", json={"token": plaintext})
    assert r1.status_code == 200

    # Simulate AS-IMPL-015's revoke commit + bus publish.
    bus = providers.token_revoked_publisher
    assert isinstance(bus, LocalTokenRevokedBus)
    revoked = fake_auth_store.service_tokens["tok-1"]
    fake_auth_store.service_tokens["tok-1"] = ServiceToken(
        token_id=revoked.token_id,
        service_account_id=revoked.service_account_id,
        hash=revoked.hash,
        issued_at=revoked.issued_at,
        expires_at=revoked.expires_at,
        revoked_at=datetime.now(UTC),
        revoked_by=PrincipalId("admin"),
        revoked_reason="compromised",
    )
    anyio.run(
        bus.publish,
        TokenRevokedEvent(
            token_id="tok-1",
            token_hash=revoked.hash,
            service_account_id="sa-1",
        ),
    )

    # Immediate re-verify must return 401 — the cache eviction +
    # the SPL ``revoked_at`` collapse the outcome.
    r2 = client.post("/v1/auth/verify", json={"token": plaintext})
    assert r2.status_code == 401

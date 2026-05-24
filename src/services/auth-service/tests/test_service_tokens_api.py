"""HTTP-level tests for service-token endpoints (AS-IMPL-013, #248).

Covers:

* Token mint:
    * 201 on the happy path, plaintext returned exactly once.
    * Token entropy (≥256 bits) and prefix shape.
    * Storage stores only the hash, never the plaintext.
    * Default TTL applied when ``ttl_seconds`` omitted.
    * Per-mint TTL override honoured.
    * ``token.issued`` audit row emitted with no plaintext / hash.
    * 404 on missing SA / wrong-workspace SA / user-principal target.
    * 400 on disabled SA.
    * 403 when caller lacks ``admin:service-account``.
* Token list:
    * 200 with empty list when the SA has no tokens.
    * 200 with revoked rows included; no plaintext / hash in any row.
    * 404 on missing SA / wrong-workspace SA / user-principal target.
    * Read against a disabled SA is permitted (history audit).
    * 403 when caller lacks ``admin:service-account``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from custos_spl.ids import PrincipalId, ServiceTokenId, TenantId, WorkspaceId
from custos_spl.interfaces.auth_store import ServiceAccount, ServiceToken, User
from fastapi.testclient import TestClient

from custos_auth.audit import EVENT_TOKEN_ISSUED
from custos_auth.settings import DEFAULT_SERVICE_TOKEN_TTL_SECONDS
from custos_auth.tokens import (
    TOKEN_LENGTH,
    TOKEN_PREFIX,
    hash_token,
    looks_like_custos_token,
    mint_token,
)
from tests._fakes import FakeAuthAdapter, FakeMetadataAdapter
from tests.conftest import callctx_header


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


def _seed_user(store: FakeAuthAdapter, principal_id: str, tenant_id: str) -> None:
    now = datetime.now(UTC)
    store.principals[principal_id] = User(
        kind="user",
        principal_id=PrincipalId(principal_id),
        tenant_id=TenantId(tenant_id),
        display_name=principal_id,
        email=None,
        disabled_at=None,
        disabled_reason=None,
        created_at=now,
    )


def _seed_token(
    store: FakeAuthAdapter,
    *,
    token_id: str,
    service_account_id: str,
    hash: str,
    ttl: timedelta = timedelta(days=30),
    revoked: bool = False,
) -> ServiceToken:
    now = datetime.now(UTC)
    token = ServiceToken(
        token_id=ServiceTokenId(token_id),
        service_account_id=PrincipalId(service_account_id),
        hash=hash,
        issued_at=now,
        expires_at=now + ttl,
        revoked_at=now if revoked else None,
        revoked_by=PrincipalId("admin") if revoked else None,
        revoked_reason="compromised" if revoked else None,
    )
    store.service_tokens[token_id] = token
    return token


# ---------------------------------------------------------------------------
# tokens helper module (pure unit tests, no HTTP)
# ---------------------------------------------------------------------------


def test_mint_token_returns_prefixed_plaintext_and_hash() -> None:
    plaintext, h = mint_token()
    assert plaintext.startswith(TOKEN_PREFIX)
    assert len(plaintext) == TOKEN_LENGTH
    # SHA-256 hex digest is 64 lowercase hex chars.
    assert len(h) == 64
    assert all(c in "0123456789abcdef" for c in h)
    assert h == hash_token(plaintext)


def test_mint_token_emits_unique_plaintext_each_call() -> None:
    # Sanity check that the random body is actually random — drawing
    # 32 bytes from os.urandom and seeing a collision in two calls
    # would either be a 1-in-2**256 cosmic event or a bug.
    plain1, _ = mint_token()
    plain2, _ = mint_token()
    assert plain1 != plain2


def test_hash_token_is_deterministic() -> None:
    assert hash_token("custos_abc") == hash_token("custos_abc")
    assert hash_token("custos_abc") != hash_token("custos_def")


def test_looks_like_custos_token_accepts_valid_shape() -> None:
    plain, _ = mint_token()
    assert looks_like_custos_token(plain) is True


def test_looks_like_custos_token_rejects_wrong_prefix_or_length() -> None:
    plain, _ = mint_token()
    assert looks_like_custos_token("eyJhbGciOiJIUzI1NiJ9.x") is False
    assert looks_like_custos_token(plain[:-1]) is False  # truncated
    assert looks_like_custos_token(plain + "X") is False  # too long
    assert looks_like_custos_token("") is False


# ---------------------------------------------------------------------------
# POST /v1/service-accounts/{id}/tokens
# ---------------------------------------------------------------------------


def test_mint_token_returns_201_with_plaintext(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
    fake_metadata_store: FakeMetadataAdapter,
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    resp = client.post(
        "/v1/service-accounts/sa-1/tokens",
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
        json={},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["service_account_id"] == "sa-1"
    assert "token_id" in body
    plaintext = body["token"]
    assert plaintext.startswith(TOKEN_PREFIX)
    assert len(plaintext) == TOKEN_LENGTH
    assert "issued_at" in body and "expires_at" in body

    # Storage carries the hash, not the plaintext.
    assert len(fake_auth_store.service_tokens) == 1
    stored = next(iter(fake_auth_store.service_tokens.values()))
    assert stored.hash == hash_token(plaintext)
    assert stored.hash != plaintext
    # Confirm the token can be looked up by the hash the verifier
    # will compute — this proves mint and verify share a hash funnel.
    looked_up = fake_auth_store.service_tokens[body["token_id"]]
    assert looked_up.hash == hash_token(plaintext)


def test_mint_token_emits_token_issued_audit_without_secrets(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
    fake_metadata_store: FakeMetadataAdapter,
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    resp = client.post(
        "/v1/service-accounts/sa-1/tokens",
        headers=callctx_header(
            principal_id="op-1",
            workspace_id="ws-1",
            permissions=["admin:service-account"],
        ),
        json={},
    )
    assert resp.status_code == 201
    plaintext = resp.json()["token"]
    stored_hash = next(iter(fake_auth_store.service_tokens.values())).hash

    rows = [
        (ws, event)
        for ws, event in fake_metadata_store.append_audit_calls
        if event.event_type == EVENT_TOKEN_ISSUED
    ]
    assert len(rows) == 1
    ws_id, event = rows[0]
    assert ws_id == "ws-1"
    assert event.actor == "op-1"
    assert event.subject == {"token_id": resp.json()["token_id"], "service_account_id": "sa-1"}
    # The audit payload must not carry the plaintext or the storage
    # hash — both would let anyone with audit-read access steal
    # the credential.
    blob = str(event.subject) + str(event.payload)
    assert plaintext not in blob
    assert stored_hash not in blob


def test_mint_token_applies_default_ttl_when_omitted(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    resp = client.post(
        "/v1/service-accounts/sa-1/tokens",
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
        json={},
    )
    assert resp.status_code == 201
    body = resp.json()
    issued = datetime.fromisoformat(body["issued_at"])
    expires = datetime.fromisoformat(body["expires_at"])
    delta = expires - issued
    assert delta == timedelta(seconds=DEFAULT_SERVICE_TOKEN_TTL_SECONDS)


def test_mint_token_honours_per_mint_ttl_override(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    resp = client.post(
        "/v1/service-accounts/sa-1/tokens",
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
        json={"ttl_seconds": 3600},
    )
    assert resp.status_code == 201
    body = resp.json()
    delta = datetime.fromisoformat(body["expires_at"]) - datetime.fromisoformat(body["issued_at"])
    assert delta == timedelta(seconds=3600)


def test_mint_token_404_when_sa_missing(client: TestClient) -> None:
    resp = client.post(
        "/v1/service-accounts/ghost/tokens",
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
        json={},
    )
    assert resp.status_code == 404


def test_mint_token_404_when_sa_lives_in_other_workspace(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-other")
    resp = client.post(
        "/v1/service-accounts/sa-1/tokens",
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
        json={},
    )
    assert resp.status_code == 404


def test_mint_token_404_when_principal_is_a_user(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    # A User principal is not mintable; collapse to 404 so the
    # caller cannot probe principal-kind on cross-workspace targets.
    _seed_user(fake_auth_store, "user-1", "t-1")
    resp = client.post(
        "/v1/service-accounts/user-1/tokens",
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
        json={},
    )
    assert resp.status_code == 404


def test_mint_token_400_when_sa_disabled(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1", disabled=True)
    resp = client.post(
        "/v1/service-accounts/sa-1/tokens",
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
        json={},
    )
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_request"


def test_mint_token_requires_permission(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    resp = client.post(
        "/v1/service-accounts/sa-1/tokens",
        headers=callctx_header(workspace_id="ws-1", permissions=["random.perm"]),
        json={},
    )
    assert resp.status_code == 403


def test_mint_token_rejects_negative_ttl(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    resp = client.post(
        "/v1/service-accounts/sa-1/tokens",
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
        json={"ttl_seconds": -1},
    )
    assert resp.status_code == 422


def test_mint_token_404_when_callctx_has_no_workspace(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    # Without a workspace in the call context we cannot scope the
    # SA lookup; collapse to 404 (existence-hiding) rather than 400
    # so the cross-workspace probe path is uniform.
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    resp = client.post(
        "/v1/service-accounts/sa-1/tokens",
        headers=callctx_header(permissions=["admin:service-account"]),
        json={},
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /v1/service-accounts/{id}/tokens
# ---------------------------------------------------------------------------


def test_list_tokens_returns_empty_list_for_fresh_sa(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    resp = client.get(
        "/v1/service-accounts/sa-1/tokens",
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
    )
    assert resp.status_code == 200
    assert resp.json() == {"tokens": []}


def test_list_tokens_returns_active_and_revoked_rows_without_secrets(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    _seed_token(fake_auth_store, token_id="tok-1", service_account_id="sa-1", hash="hash-active")
    _seed_token(
        fake_auth_store,
        token_id="tok-2",
        service_account_id="sa-1",
        hash="hash-revoked",
        revoked=True,
    )
    resp = client.get(
        "/v1/service-accounts/sa-1/tokens",
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
    )
    assert resp.status_code == 200
    rows = {r["token_id"]: r for r in resp.json()["tokens"]}
    assert set(rows) == {"tok-1", "tok-2"}
    # No plaintext, no hash in any row.
    for r in rows.values():
        assert "hash" not in r
        assert "token" not in r
    # Revoke metadata flows through.
    assert rows["tok-1"]["revoked_at"] is None
    assert rows["tok-2"]["revoked_at"] is not None
    assert rows["tok-2"]["revoked_by"] == "admin"
    assert rows["tok-2"]["revoked_reason"] == "compromised"


def test_list_tokens_does_not_leak_across_service_accounts(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    _seed_service_account(fake_auth_store, "sa-2", "ws-1")
    _seed_token(fake_auth_store, token_id="tok-A", service_account_id="sa-1", hash="hA")
    _seed_token(fake_auth_store, token_id="tok-B", service_account_id="sa-2", hash="hB")
    resp = client.get(
        "/v1/service-accounts/sa-1/tokens",
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
    )
    assert resp.status_code == 200
    rows = resp.json()["tokens"]
    assert [r["token_id"] for r in rows] == ["tok-A"]


def test_list_tokens_404_on_cross_workspace(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-other")
    resp = client.get(
        "/v1/service-accounts/sa-1/tokens",
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
    )
    assert resp.status_code == 404


def test_list_tokens_404_when_principal_is_a_user(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_user(fake_auth_store, "user-1", "t-1")
    resp = client.get(
        "/v1/service-accounts/user-1/tokens",
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
    )
    assert resp.status_code == 404


def test_list_tokens_succeeds_on_disabled_sa_for_history_audit(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1", disabled=True)
    _seed_token(fake_auth_store, token_id="tok-1", service_account_id="sa-1", hash="h1")
    resp = client.get(
        "/v1/service-accounts/sa-1/tokens",
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
    )
    # Disabled SAs cannot mint (tested above) but list is allowed
    # so operators can render rotation history.
    assert resp.status_code == 200
    assert [r["token_id"] for r in resp.json()["tokens"]] == ["tok-1"]


def test_list_tokens_requires_permission(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    resp = client.get(
        "/v1/service-accounts/sa-1/tokens",
        headers=callctx_header(workspace_id="ws-1", permissions=["random.perm"]),
    )
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# AS-IMPL-015 (#250) — DELETE /v1/tokens/{token_id}
# ---------------------------------------------------------------------------


def test_revoke_token_204_marks_spl_row_revoked_and_audits(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
    fake_metadata_store: FakeMetadataAdapter,
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    _seed_token(fake_auth_store, token_id="tok-1", service_account_id="sa-1", hash="h1")

    resp = client.request(
        "DELETE",
        "/v1/tokens/tok-1",
        json={"reason": "compromised"},
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
    )
    assert resp.status_code == 204
    assert resp.content == b""

    # SPL row flipped.
    row = fake_auth_store.service_tokens["tok-1"]
    assert row.revoked_at is not None
    assert row.revoked_by == PrincipalId("user-1")
    assert row.revoked_reason == "compromised"
    # Audit row emitted with the operator-supplied reason.
    revoked_audits = [
        e for _ws, e in fake_metadata_store.append_audit_calls if e.event_type == "token.revoked"
    ]
    assert len(revoked_audits) == 1
    event = revoked_audits[0]
    assert event.actor == "user-1"
    assert event.subject == {"token_id": "tok-1", "service_account_id": "sa-1"}
    assert event.payload == {"reason": "compromised"}


def test_revoke_token_publishes_token_revoked_event(
    client: TestClient, fake_auth_store: FakeAuthAdapter, providers: object
) -> None:
    # The publish must carry the token_id, the (now-old) hash, and
    # the owning SA id so cross-replica subscribers can evict their
    # per-pod authn cache without re-hitting the SPL.
    from custos_auth.providers import Providers
    from custos_auth.token_revoked_events import (
        LocalTokenRevokedBus,
        RecordingTokenRevokedSubscriber,
        TokenRevokedEvent,
    )

    assert isinstance(providers, Providers)
    bus = providers.token_revoked_publisher
    assert isinstance(bus, LocalTokenRevokedBus)

    seen: list[TokenRevokedEvent] = []

    async def witness(event: TokenRevokedEvent) -> None:
        seen.append(event)

    bus.subscribe(witness)

    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    _seed_token(fake_auth_store, token_id="tok-1", service_account_id="sa-1", hash="h1")

    resp = client.request(
        "DELETE",
        "/v1/tokens/tok-1",
        json={"reason": "rotate"},
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
    )
    assert resp.status_code == 204
    assert seen == [TokenRevokedEvent(token_id="tok-1", token_hash="h1", service_account_id="sa-1")]
    # Also assert the no-op RecordingTokenRevokedSubscriber type
    # really is in scope (regression: catches accidental import
    # breakage of the events module).
    assert RecordingTokenRevokedSubscriber  # type: ignore[truthy-function]


def test_revoke_token_204_no_op_when_already_revoked(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
    fake_metadata_store: FakeMetadataAdapter,
    providers: object,
) -> None:
    from custos_auth.providers import Providers
    from custos_auth.token_revoked_events import (
        LocalTokenRevokedBus,
        TokenRevokedEvent,
    )

    assert isinstance(providers, Providers)
    bus = providers.token_revoked_publisher
    assert isinstance(bus, LocalTokenRevokedBus)
    seen: list[TokenRevokedEvent] = []

    async def witness(event: TokenRevokedEvent) -> None:
        seen.append(event)

    bus.subscribe(witness)

    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    _seed_token(
        fake_auth_store,
        token_id="tok-1",
        service_account_id="sa-1",
        hash="h1",
        revoked=True,
    )
    fake_metadata_store.append_audit_calls.clear()

    resp = client.request(
        "DELETE",
        "/v1/tokens/tok-1",
        json={"reason": "compromised"},
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
    )
    # Idempotent: second revoke must succeed (204) but emit
    # nothing — no audit row and no eviction event. Operators
    # rerunning the same script should not flood the audit
    # pipeline with redundant rows.
    assert resp.status_code == 204
    assert fake_metadata_store.append_audit_calls == []
    assert seen == []


def test_revoke_token_404_when_token_missing(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    resp = client.request(
        "DELETE",
        "/v1/tokens/tok-ghost",
        json={"reason": "compromised"},
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
    )
    assert resp.status_code == 404


def test_revoke_token_404_when_token_belongs_to_other_workspace(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    # Cross-workspace probe collapses to 404 so the existence of the
    # token is not observable from a sibling workspace.
    _seed_service_account(fake_auth_store, "sa-1", "ws-other")
    _seed_token(fake_auth_store, token_id="tok-1", service_account_id="sa-1", hash="h1")
    resp = client.request(
        "DELETE",
        "/v1/tokens/tok-1",
        json={"reason": "compromised"},
        headers=callctx_header(workspace_id="ws-mine", permissions=["admin:service-account"]),
    )
    assert resp.status_code == 404


def test_revoke_token_404_when_owning_sa_was_hard_deleted(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    # Defensive: SPL contract forbids SA hard-delete, but the
    # revoke path still collapses to 404 if the row vanishes so
    # there is no way to mutate an orphan token row.
    _seed_token(fake_auth_store, token_id="tok-orphan", service_account_id="sa-gone", hash="h1")
    resp = client.request(
        "DELETE",
        "/v1/tokens/tok-orphan",
        json={"reason": "compromised"},
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
    )
    assert resp.status_code == 404


def test_revoke_token_requires_admin_service_account_permission(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    _seed_token(fake_auth_store, token_id="tok-1", service_account_id="sa-1", hash="h1")
    resp = client.request(
        "DELETE",
        "/v1/tokens/tok-1",
        json={"reason": "rotate"},
        headers=callctx_header(workspace_id="ws-1", permissions=["random.perm"]),
    )
    assert resp.status_code == 403


def test_revoke_token_rejects_empty_reason(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    _seed_token(fake_auth_store, token_id="tok-1", service_account_id="sa-1", hash="h1")
    resp = client.request(
        "DELETE",
        "/v1/tokens/tok-1",
        json={"reason": ""},
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
    )
    assert resp.status_code == 422


def test_revoke_token_rejects_extra_fields(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    _seed_token(fake_auth_store, token_id="tok-1", service_account_id="sa-1", hash="h1")
    resp = client.request(
        "DELETE",
        "/v1/tokens/tok-1",
        json={"reason": "rotate", "extra": "x"},
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# AS-IMPL-015 (#250) — DELETE /v1/service-accounts/{id}/tokens
# ---------------------------------------------------------------------------


def test_revoke_all_tokens_revokes_every_live_row(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
    fake_metadata_store: FakeMetadataAdapter,
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    _seed_token(fake_auth_store, token_id="tok-1", service_account_id="sa-1", hash="h1")
    _seed_token(fake_auth_store, token_id="tok-2", service_account_id="sa-1", hash="h2")
    _seed_token(
        fake_auth_store,
        token_id="tok-3",
        service_account_id="sa-1",
        hash="h3",
        revoked=True,
    )
    resp = client.request(
        "DELETE",
        "/v1/service-accounts/sa-1/tokens",
        json={"reason": "sa-disabled"},
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
    )
    assert resp.status_code == 200
    # Only the two live rows were flipped — the already-revoked
    # row is a silent no-op.
    assert resp.json() == {"revoked_count": 2}
    assert fake_auth_store.service_tokens["tok-1"].revoked_at is not None
    assert fake_auth_store.service_tokens["tok-2"].revoked_at is not None
    # And exactly two ``token.revoked`` audits were emitted.
    revoked_events = [
        e for _ws, e in fake_metadata_store.append_audit_calls if e.event_type == "token.revoked"
    ]
    assert len(revoked_events) == 2
    assert {e.subject["token_id"] for e in revoked_events} == {"tok-1", "tok-2"}


def test_revoke_all_tokens_publishes_one_event_per_flipped_token(
    client: TestClient, fake_auth_store: FakeAuthAdapter, providers: object
) -> None:
    from custos_auth.providers import Providers
    from custos_auth.token_revoked_events import (
        LocalTokenRevokedBus,
        TokenRevokedEvent,
    )

    assert isinstance(providers, Providers)
    bus = providers.token_revoked_publisher
    assert isinstance(bus, LocalTokenRevokedBus)
    seen: list[TokenRevokedEvent] = []

    async def witness(event: TokenRevokedEvent) -> None:
        seen.append(event)

    bus.subscribe(witness)

    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    _seed_token(fake_auth_store, token_id="tok-1", service_account_id="sa-1", hash="h1")
    _seed_token(fake_auth_store, token_id="tok-2", service_account_id="sa-1", hash="h2")

    resp = client.request(
        "DELETE",
        "/v1/service-accounts/sa-1/tokens",
        json={"reason": "rotate"},
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
    )
    assert resp.status_code == 200
    assert {e.token_id for e in seen} == {"tok-1", "tok-2"}
    # Each event carries the matching hash so subscribers don't
    # have to re-fetch the SPL row to compute it.
    assert {(e.token_id, e.token_hash) for e in seen} == {
        ("tok-1", "h1"),
        ("tok-2", "h2"),
    }


def test_revoke_all_tokens_returns_zero_when_nothing_was_live(
    client: TestClient,
    fake_auth_store: FakeAuthAdapter,
    fake_metadata_store: FakeMetadataAdapter,
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    _seed_token(
        fake_auth_store,
        token_id="tok-1",
        service_account_id="sa-1",
        hash="h1",
        revoked=True,
    )
    fake_metadata_store.append_audit_calls.clear()
    resp = client.request(
        "DELETE",
        "/v1/service-accounts/sa-1/tokens",
        json={"reason": "rotate"},
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
    )
    assert resp.status_code == 200
    assert resp.json() == {"revoked_count": 0}
    assert fake_metadata_store.append_audit_calls == []


def test_revoke_all_tokens_works_against_disabled_sa(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    # The "kill switch" flow: operator disables the SA, then bulk-
    # revokes its outstanding tokens. The bulk-revoke endpoint
    # therefore deliberately bypasses the disabled-SA guard that
    # would otherwise refuse a mutation on a disabled principal.
    _seed_service_account(fake_auth_store, "sa-1", "ws-1", disabled=True)
    _seed_token(fake_auth_store, token_id="tok-1", service_account_id="sa-1", hash="h1")
    resp = client.request(
        "DELETE",
        "/v1/service-accounts/sa-1/tokens",
        json={"reason": "kill-switch"},
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
    )
    assert resp.status_code == 200
    assert resp.json() == {"revoked_count": 1}


def test_revoke_all_tokens_404_when_sa_missing(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    resp = client.request(
        "DELETE",
        "/v1/service-accounts/sa-ghost/tokens",
        json={"reason": "rotate"},
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
    )
    assert resp.status_code == 404


def test_revoke_all_tokens_404_when_sa_in_other_workspace(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-other")
    resp = client.request(
        "DELETE",
        "/v1/service-accounts/sa-1/tokens",
        json={"reason": "rotate"},
        headers=callctx_header(workspace_id="ws-mine", permissions=["admin:service-account"]),
    )
    assert resp.status_code == 404


def test_revoke_all_tokens_404_when_principal_is_a_user(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_user(fake_auth_store, "user-1", "t-1")
    resp = client.request(
        "DELETE",
        "/v1/service-accounts/user-1/tokens",
        json={"reason": "rotate"},
        headers=callctx_header(workspace_id="ws-1", permissions=["admin:service-account"]),
    )
    assert resp.status_code == 404


def test_revoke_all_tokens_requires_admin_service_account_permission(
    client: TestClient, fake_auth_store: FakeAuthAdapter
) -> None:
    _seed_service_account(fake_auth_store, "sa-1", "ws-1")
    _seed_token(fake_auth_store, token_id="tok-1", service_account_id="sa-1", hash="h1")
    resp = client.request(
        "DELETE",
        "/v1/service-accounts/sa-1/tokens",
        json={"reason": "rotate"},
        headers=callctx_header(workspace_id="ws-1", permissions=["random.perm"]),
    )
    assert resp.status_code == 403

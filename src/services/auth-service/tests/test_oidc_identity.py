"""Tests for OIDC identity helpers (AS-IMPL-007, #242)."""

from __future__ import annotations

from typing import cast

import pytest
from custos_spl import AuthStoreProvider, MetadataStoreProvider

from custos_auth.audit import EVENT_OIDC_IDENTITY_LINKED, PLATFORM_WORKSPACE_ID
from custos_auth.oidc_identity import (
    OidcIdentityAlreadyBound,
    find_user_by_oidc,
    link_oidc_identity,
)
from tests._fakes import FakeAuthAdapter, FakeMetadataAdapter


def _as_auth(store: FakeAuthAdapter) -> AuthStoreProvider:
    return cast(AuthStoreProvider, store)


def _as_meta(store: FakeMetadataAdapter) -> MetadataStoreProvider:
    return cast(MetadataStoreProvider, store)


async def test_link_oidc_identity_writes_binding_and_emits_audit() -> None:
    auth_store = FakeAuthAdapter()
    metadata_store = FakeMetadataAdapter()

    await link_oidc_identity(
        _as_auth(auth_store),
        _as_meta(metadata_store),
        user_id="user-1",
        issuer="https://idp.example.com",
        subject="oidc-sub-1",
    )

    assert auth_store.oidc_identities[("https://idp.example.com", "oidc-sub-1")] == "user-1"
    assert len(metadata_store.append_audit_calls) == 1
    ws_id, event = metadata_store.append_audit_calls[0]
    assert ws_id == PLATFORM_WORKSPACE_ID
    assert event.event_type == EVENT_OIDC_IDENTITY_LINKED
    assert event.actor == "system"
    assert event.subject == {
        "user_id": "user-1",
        "issuer": "https://idp.example.com",
        "oidc_subject": "oidc-sub-1",
    }


async def test_link_oidc_identity_skips_audit_when_metadata_store_is_none() -> None:
    auth_store = FakeAuthAdapter()

    await link_oidc_identity(
        _as_auth(auth_store),
        None,
        user_id="user-1",
        issuer="https://idp.example.com",
        subject="sub-1",
    )

    assert auth_store.oidc_identities[("https://idp.example.com", "sub-1")] == "user-1"


async def test_link_oidc_identity_honours_actor_and_workspace_overrides() -> None:
    auth_store = FakeAuthAdapter()
    metadata_store = FakeMetadataAdapter()

    await link_oidc_identity(
        _as_auth(auth_store),
        _as_meta(metadata_store),
        user_id="user-1",
        issuer="https://idp.example.com",
        subject="sub-1",
        actor="admin",
        audit_workspace_id="ws-1",
    )

    ws_id, event = metadata_store.append_audit_calls[0]
    assert ws_id == "ws-1"
    assert event.actor == "admin"


async def test_link_oidc_identity_raises_already_bound_on_duplicate() -> None:
    auth_store = FakeAuthAdapter()
    metadata_store = FakeMetadataAdapter()

    await link_oidc_identity(
        _as_auth(auth_store),
        _as_meta(metadata_store),
        user_id="user-1",
        issuer="https://idp.example.com",
        subject="sub-1",
    )

    with pytest.raises(OidcIdentityAlreadyBound) as exc_info:
        await link_oidc_identity(
            _as_auth(auth_store),
            _as_meta(metadata_store),
            user_id="user-2",
            issuer="https://idp.example.com",
            subject="sub-1",
        )

    assert exc_info.value.issuer == "https://idp.example.com"
    assert exc_info.value.subject == "sub-1"
    # Second attempt must not have emitted a second audit row
    assert len(metadata_store.append_audit_calls) == 1


async def test_find_user_by_oidc_returns_user_id_when_bound() -> None:
    auth_store = FakeAuthAdapter()
    await link_oidc_identity(
        _as_auth(auth_store),
        None,
        user_id="user-1",
        issuer="https://idp.example.com",
        subject="sub-1",
    )

    result = await find_user_by_oidc(
        _as_auth(auth_store),
        issuer="https://idp.example.com",
        subject="sub-1",
    )

    assert result == "user-1"


async def test_find_user_by_oidc_returns_none_when_unbound() -> None:
    auth_store = FakeAuthAdapter()

    result = await find_user_by_oidc(
        _as_auth(auth_store),
        issuer="https://idp.example.com",
        subject="missing",
    )

    assert result is None

"""Tests for :mod:`custos_auth.audit`."""

from __future__ import annotations

import logging

import pytest
from custos_spl import AuditEvent

from custos_auth.audit import (
    EVENT_AUTHN_FAILURE,
    EVENT_AUTHN_SUCCESS,
    EVENT_CALL_CONTEXT_INVALID,
    EVENT_OIDC_IDENTITY_LINKED,
    EVENT_PRINCIPAL_CREATED,
    EVENT_PRINCIPAL_DISABLED,
    EVENT_TENANT_CREATED,
    EVENT_TOKEN_EXPIRED,
    EVENT_TOKEN_ISSUED,
    EVENT_TOKEN_REVOKED,
    EVENT_TOKEN_USED,
    EVENT_WORKSPACE_CREATED,
    PLATFORM_WORKSPACE_ID,
    audit_authn_failure,
    audit_authn_success,
    audit_call_context_invalid,
    audit_oidc_identity_linked,
    audit_principal_created,
    audit_principal_disabled,
    audit_tenant_created,
    audit_token_expired,
    audit_token_issued,
    audit_token_revoked,
    audit_token_used,
    audit_workspace_created,
)
from tests._fakes import FakeMetadataAdapter


@pytest.mark.asyncio
async def test_audit_tenant_created_appends_under_platform_sentinel() -> None:
    meta = FakeMetadataAdapter()
    await audit_tenant_created(
        meta,  # type: ignore[arg-type]
        actor="user-1",
        tenant_id="t1",
        name="Acme",
    )
    assert len(meta.append_audit_calls) == 1
    ws_id, event = meta.append_audit_calls[0]
    assert ws_id == PLATFORM_WORKSPACE_ID
    assert isinstance(event, AuditEvent)
    assert event.event_type == EVENT_TENANT_CREATED
    assert event.actor == "user-1"
    assert event.subject == {"tenant_id": "t1"}
    assert event.payload == {"name": "Acme"}


@pytest.mark.asyncio
async def test_audit_workspace_created_keys_under_new_workspace() -> None:
    meta = FakeMetadataAdapter()
    await audit_workspace_created(
        meta,  # type: ignore[arg-type]
        actor="user-1",
        tenant_id="t1",
        workspace_id="ws-1",
        name="Default",
    )
    ws_id, event = meta.append_audit_calls[0]
    assert ws_id == "ws-1"
    assert event.event_type == EVENT_WORKSPACE_CREATED
    assert event.subject == {"workspace_id": "ws-1"}
    assert event.payload == {"tenant_id": "t1", "name": "Default"}


@pytest.mark.asyncio
async def test_audit_principal_created_carries_kind_and_display_name() -> None:
    meta = FakeMetadataAdapter()
    await audit_principal_created(
        meta,  # type: ignore[arg-type]
        actor="user-1",
        workspace_id="ws-1",
        principal_id="sa-1",
        kind="serviceAccount",
        display_name="ci-runner",
    )
    ws_id, event = meta.append_audit_calls[0]
    assert ws_id == "ws-1"
    assert event.event_type == EVENT_PRINCIPAL_CREATED
    assert event.subject == {"principal_id": "sa-1"}
    assert event.payload == {"kind": "serviceAccount", "display_name": "ci-runner"}


@pytest.mark.asyncio
async def test_audit_principal_disabled_includes_reason() -> None:
    meta = FakeMetadataAdapter()
    await audit_principal_disabled(
        meta,  # type: ignore[arg-type]
        actor="user-1",
        workspace_id="ws-1",
        principal_id="sa-1",
        reason="left-the-company",
    )
    ws_id, event = meta.append_audit_calls[0]
    assert ws_id == "ws-1"
    assert event.event_type == EVENT_PRINCIPAL_DISABLED
    assert event.payload == {"reason": "left-the-company"}


@pytest.mark.asyncio
async def test_audit_oidc_identity_linked_payload_empty_subject_carries_keys() -> None:
    meta = FakeMetadataAdapter()
    await audit_oidc_identity_linked(
        meta,  # type: ignore[arg-type]
        actor="system",
        workspace_id=PLATFORM_WORKSPACE_ID,
        user_id="user-1",
        issuer="https://idp.example.com",
        subject="sub-42",
    )
    ws_id, event = meta.append_audit_calls[0]
    assert ws_id == PLATFORM_WORKSPACE_ID
    assert event.event_type == EVENT_OIDC_IDENTITY_LINKED
    assert event.subject == {
        "user_id": "user-1",
        "issuer": "https://idp.example.com",
        "oidc_subject": "sub-42",
    }
    assert event.payload == {}


@pytest.mark.asyncio
async def test_audit_token_issued_keys_under_sa_workspace_and_omits_secrets() -> None:
    import datetime as _dt

    meta = FakeMetadataAdapter()
    issued = _dt.datetime(2026, 5, 24, 12, 0, 0, tzinfo=_dt.UTC)
    expires = issued + _dt.timedelta(days=90)
    await audit_token_issued(
        meta,  # type: ignore[arg-type]
        actor="op-1",
        workspace_id="ws-1",
        token_id="tok-1",
        service_account_id="sa-1",
        issued_at=issued,
        expires_at=expires,
    )
    ws_id, event = meta.append_audit_calls[0]
    assert ws_id == "ws-1"
    assert event.event_type == EVENT_TOKEN_ISSUED
    assert event.actor == "op-1"
    assert event.subject == {"token_id": "tok-1", "service_account_id": "sa-1"}
    # The audit payload must carry timestamps as ISO 8601 strings
    # (not naive epoch ints) and must never carry the plaintext or
    # the storage hash.
    assert event.payload == {
        "issued_at": issued.isoformat(),
        "expires_at": expires.isoformat(),
    }
    for k, v in event.payload.items():
        assert isinstance(v, str), f"payload[{k}] must be ISO-format string"


# ---------------------------------------------------------------------------
# Best-effort: emission failures are swallowed and counter-incremented.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_audit_emission_failure_is_swallowed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    meta = FakeMetadataAdapter(append_audit_should_fail=True)
    with caplog.at_level(logging.WARNING, logger="custos_auth.audit"):
        await audit_tenant_created(
            meta,  # type: ignore[arg-type]
            actor="user-1",
            tenant_id="t1",
            name="Acme",
        )
    assert meta.append_audit_calls == []
    assert any("audit emission failed" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# AS-IMPL-014: token.used / authn.success / authn.failure helpers.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_audit_token_used_keys_to_sa_workspace_with_sa_as_actor() -> None:
    meta = FakeMetadataAdapter()
    await audit_token_used(
        meta,  # type: ignore[arg-type]
        workspace_id="ws-1",
        token_id="tok-1",
        service_account_id="sa-1",
    )
    ws_id, event = meta.append_audit_calls[0]
    assert ws_id == "ws-1"
    assert event.event_type == EVENT_TOKEN_USED
    # The verify path runs before call-context middleware so the
    # bearer (the SA itself) is the only available actor.
    assert event.actor == "sa-1"
    assert event.subject == {"token_id": "tok-1", "service_account_id": "sa-1"}
    # ``token.used`` carries no payload — the row is a presence
    # signal, not a data point. The 30 s authn-cache rate-limits
    # it to ~one per token per window after a rotation.
    assert event.payload == {}


@pytest.mark.asyncio
async def test_audit_authn_success_payload_carries_cache_hit_flag() -> None:
    meta = FakeMetadataAdapter()
    await audit_authn_success(
        meta,  # type: ignore[arg-type]
        workspace_id="ws-1",
        token_id="tok-1",
        service_account_id="sa-1",
        cache_hit=True,
    )
    _ws, event = meta.append_audit_calls[0]
    assert event.event_type == EVENT_AUTHN_SUCCESS
    assert event.actor == "sa-1"
    assert event.payload == {"cache_hit": True}


@pytest.mark.asyncio
async def test_audit_authn_failure_with_full_subject_keys_to_sa_workspace() -> None:
    meta = FakeMetadataAdapter()
    await audit_authn_failure(
        meta,  # type: ignore[arg-type]
        reason="revoked",
        workspace_id="ws-1",
        token_id="tok-1",
        service_account_id="sa-1",
    )
    ws_id, event = meta.append_audit_calls[0]
    assert ws_id == "ws-1"
    assert event.event_type == EVENT_AUTHN_FAILURE
    assert event.actor == "sa-1"
    assert event.subject == {"token_id": "tok-1", "service_account_id": "sa-1"}
    assert event.payload == {"reason": "revoked"}


@pytest.mark.asyncio
async def test_audit_authn_failure_unknown_token_falls_back_to_platform() -> None:
    # When no SPL row matched the input hash we have neither a
    # token_id nor a SA id; the failure row therefore lands in the
    # platform sentinel bucket so an unknown-token probe does not
    # appear (arbitrarily) under some workspace's audit pipeline.
    meta = FakeMetadataAdapter()
    await audit_authn_failure(
        meta,  # type: ignore[arg-type]
        reason="unknown-token",
    )
    ws_id, event = meta.append_audit_calls[0]
    assert ws_id == PLATFORM_WORKSPACE_ID
    assert event.actor == "anonymous"
    # Empty subject — we don't leak the input hash on the row.
    assert event.subject == {}
    assert event.payload == {"reason": "unknown-token"}


# ---------------------------------------------------------------------------
# AS-IMPL-015: token.revoked helper.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_audit_token_revoked_keys_under_sa_workspace() -> None:
    # The revoke row lands in the SA's owning workspace so an
    # operator filtering the audit pipeline by their workspace can
    # see every revoke they caused. The reason string is part of
    # the payload — subject carries only the structural ids.
    meta = FakeMetadataAdapter()
    await audit_token_revoked(
        meta,  # type: ignore[arg-type]
        actor="op-1",
        workspace_id="ws-1",
        token_id="tok-1",
        service_account_id="sa-1",
        reason="compromised",
    )
    ws_id, event = meta.append_audit_calls[0]
    assert ws_id == "ws-1"
    assert event.event_type == EVENT_TOKEN_REVOKED
    assert event.actor == "op-1"
    assert event.subject == {"token_id": "tok-1", "service_account_id": "sa-1"}
    assert event.payload == {"reason": "compromised"}


# ---------------------------------------------------------------------------
# AS-IMPL-016: token.expired helper.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_audit_token_expired_keys_under_sa_workspace() -> None:
    # Sweeper-driven expiry rows land in the SA's owning workspace
    # so the audit-query path is identical to ``token.revoked``.
    # ``actor`` is the SA itself (the system is the deleter; the
    # SA is the only meaningful actor since no human triggered the
    # row).
    from datetime import UTC, datetime

    meta = FakeMetadataAdapter()
    expires_at = datetime(2030, 1, 1, tzinfo=UTC)
    await audit_token_expired(
        meta,  # type: ignore[arg-type]
        workspace_id="ws-1",
        token_id="tok-1",
        service_account_id="sa-1",
        expires_at=expires_at,
    )
    ws_id, event = meta.append_audit_calls[0]
    assert ws_id == "ws-1"
    assert event.event_type == EVENT_TOKEN_EXPIRED
    assert event.actor == "sa-1"
    assert event.subject == {"token_id": "tok-1", "service_account_id": "sa-1"}
    assert event.payload == {"expires_at": expires_at.isoformat()}


# ---------------------------------------------------------------------------
# AS-IMPL-026: call-context.invalid helper.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_audit_call_context_invalid_keys_under_platform_sentinel() -> None:
    meta = FakeMetadataAdapter()
    await audit_call_context_invalid(
        meta,  # type: ignore[arg-type]
        reason="bad_signature",
    )
    ws_id, event = meta.append_audit_calls[0]
    assert ws_id == PLATFORM_WORKSPACE_ID
    assert event.event_type == EVENT_CALL_CONTEXT_INVALID
    # Default actor is the system because failed call-contexts have
    # no trusted principal binding.
    assert event.actor == "system"
    assert event.subject == {"reason": "bad_signature"}
    assert event.payload == {"reason": "bad_signature"}


@pytest.mark.asyncio
async def test_audit_call_context_invalid_includes_kid_when_present() -> None:
    meta = FakeMetadataAdapter()
    await audit_call_context_invalid(
        meta,  # type: ignore[arg-type]
        reason="unknown_kid",
        actor="dapr-app:catalog",
        kid="kid-1",
    )
    _, event = meta.append_audit_calls[0]
    assert event.actor == "dapr-app:catalog"
    assert event.payload == {"reason": "unknown_kid", "kid": "kid-1"}


@pytest.mark.asyncio
async def test_audit_call_context_invalid_never_carries_raw_token() -> None:
    """Security: the failing JWT must never enter the audit payload.

    The helper takes only the reason + optional ``kid``; there is no
    keyword that could smuggle the raw signed string. This guards
    against future refactors that might try to attach the offending
    token "for debugging".
    """
    meta = FakeMetadataAdapter()
    await audit_call_context_invalid(
        meta,  # type: ignore[arg-type]
        reason="expired",
        kid="kid-9",
    )
    _, event = meta.append_audit_calls[0]
    # No keyword on the helper accepts a raw token; the only data
    # that may travel is the reason code + the optional public kid.
    # Assert the payload keyset is exactly the contractually-allowed
    # set so a future refactor that smuggles a JWT under any new
    # key trips the gate.
    assert set(event.payload.keys()) <= {"reason", "kid"}
    assert set(event.subject.keys()) <= {"reason"}

"""Tests for :mod:`custos_auth.audit`."""

from __future__ import annotations

import logging

import pytest
from custos_spl import AuditEvent

from custos_auth.audit import (
    EVENT_OIDC_IDENTITY_LINKED,
    EVENT_PRINCIPAL_CREATED,
    EVENT_PRINCIPAL_DISABLED,
    EVENT_TENANT_CREATED,
    EVENT_TOKEN_ISSUED,
    EVENT_WORKSPACE_CREATED,
    PLATFORM_WORKSPACE_ID,
    audit_oidc_identity_linked,
    audit_principal_created,
    audit_principal_disabled,
    audit_tenant_created,
    audit_token_issued,
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

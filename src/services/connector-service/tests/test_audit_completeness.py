"""Audit completeness for CONN-IMPL-029 (Phase K, #312).

Verifies the typed audit helpers introduced in Phase K — registration
accept/reject, deprecation toggle, manifest-fallback used/ignored/
rejected, and authz decision — write through the
:class:`~custos_spl.interfaces.metadata_store.MetadataStoreProvider`
contract:

* Each helper appends exactly one audit row.
* Platform-scoped events (registration / deprecation / manifest
  fallback) land under :data:`PLATFORM_WORKSPACE_ID`.
* Workspace-scoped events (authz decision) land under the call-
  context workspace id.
* The subject/payload shape matches the issue contract.

These tests cover *just* the new typed surface added in Phase K. The
older event helpers (instance lifecycle, lease, cursor, binding) are
already covered by the pre-existing audit unit tests.
"""

from __future__ import annotations

import pytest

from custos_connector.audit import (
    EVENT_AUTHZ_DECISION,
    EVENT_DEPRECATION_TOGGLED,
    EVENT_MANIFEST_FALLBACK_IGNORED,
    EVENT_MANIFEST_FALLBACK_REJECTED,
    EVENT_MANIFEST_FALLBACK_USED,
    EVENT_REGISTRATION_ACCEPTED,
    EVENT_REGISTRATION_REJECTED,
    PLATFORM_WORKSPACE_ID,
    audit_authz_decision,
    audit_deprecation_toggled,
    audit_manifest_fallback_ignored,
    audit_manifest_fallback_rejected,
    audit_manifest_fallback_used,
    audit_registration_accepted,
    audit_registration_rejected,
)
from tests._fakes import FakeMetadataAdapter


@pytest.mark.asyncio
async def test_audit_registration_accepted_emits_platform_scope_event() -> None:
    metadata = FakeMetadataAdapter()
    await audit_registration_accepted(
        metadata,  # type: ignore[arg-type]
        type_name="oci-registry",
        version="1.2.0",
        image_ref="registry.example.com/team-a/oci-registry-conn:1.2.0",
        manifest_digest="sha256:" + "a" * 64,
    )
    assert len(metadata.append_audit_calls) == 1
    ws, evt = metadata.append_audit_calls[0]
    assert ws == PLATFORM_WORKSPACE_ID
    assert evt.event_type == EVENT_REGISTRATION_ACCEPTED
    assert evt.subject == {"type": "oci-registry", "version": "1.2.0"}
    assert evt.payload["image_ref"].endswith(":1.2.0")
    assert evt.payload["manifest_digest"].startswith("sha256:")


@pytest.mark.asyncio
async def test_audit_registration_rejected_omits_unknown_type_and_version() -> None:
    """Subject carries only ``image_ref`` when type/version are unknown.

    The loader rejects bad image refs before the manifest is parsed,
    so the typed helper allows ``type_name`` / ``version`` to remain
    ``None`` rather than synthesising placeholder strings.
    """
    metadata = FakeMetadataAdapter()
    await audit_registration_rejected(
        metadata,  # type: ignore[arg-type]
        image_ref="malformed:::ref",
        code="invalid-image-ref",
        detail="image reference is not parseable",
    )
    _, evt = metadata.append_audit_calls[0]
    assert evt.event_type == EVENT_REGISTRATION_REJECTED
    assert evt.subject == {"image_ref": "malformed:::ref"}
    assert evt.payload == {
        "code": "invalid-image-ref",
        "detail": "image reference is not parseable",
    }


@pytest.mark.asyncio
async def test_audit_deprecation_toggled_carries_flag_in_payload() -> None:
    metadata = FakeMetadataAdapter()
    await audit_deprecation_toggled(
        metadata,  # type: ignore[arg-type]
        type_name="oci-registry",
        version="*",
        deprecated=True,
    )
    ws, evt = metadata.append_audit_calls[0]
    assert ws == PLATFORM_WORKSPACE_ID
    assert evt.event_type == EVENT_DEPRECATION_TOGGLED
    assert evt.payload == {"deprecated": True}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("helper", "event_type"),
    [
        (audit_manifest_fallback_used, EVENT_MANIFEST_FALLBACK_USED),
        (audit_manifest_fallback_ignored, EVENT_MANIFEST_FALLBACK_IGNORED),
    ],
)
async def test_audit_manifest_fallback_used_ignored_emit_platform_scope(
    helper,
    event_type: str,
) -> None:
    """``used`` and ``ignored`` share the same call signature.

    Both helpers attach the fallback tag and a ``resolved_via``
    discriminator payload so dashboards can tell the two paths apart
    without parsing the event type.
    """
    metadata = FakeMetadataAdapter()
    await helper(
        metadata,
        repository="team-a/oci-registry-conn",
        subject_digest="sha256:" + "b" * 64,
        fallback_tag="manifest-sha256-bbbbbb",
    )
    ws, evt = metadata.append_audit_calls[0]
    assert ws == PLATFORM_WORKSPACE_ID
    assert evt.event_type == event_type
    assert evt.subject == {
        "repository": "team-a/oci-registry-conn",
        "subject_digest": "sha256:" + "b" * 64,
    }
    assert evt.payload["fallback_tag"] == "manifest-sha256-bbbbbb"
    assert evt.payload["resolved_via"] in {"referrers", "fallback-tag"}


@pytest.mark.asyncio
async def test_audit_manifest_fallback_rejected_carries_code_and_detail() -> None:
    metadata = FakeMetadataAdapter()
    await audit_manifest_fallback_rejected(
        metadata,  # type: ignore[arg-type]
        repository="team-a/oci-registry-conn",
        subject_digest="sha256:" + "c" * 64,
        code="no-manifest-found",
        detail="neither the Referrers API nor the fallback tag yielded a descriptor",
    )
    ws, evt = metadata.append_audit_calls[0]
    assert ws == PLATFORM_WORKSPACE_ID
    assert evt.event_type == EVENT_MANIFEST_FALLBACK_REJECTED
    assert evt.payload["code"] == "no-manifest-found"


@pytest.mark.asyncio
@pytest.mark.parametrize("allowed", [True, False])
async def test_audit_authz_decision_emits_workspace_scoped_row(allowed: bool) -> None:
    """Authz audit lands in the caller's workspace, not the platform bucket.

    Workspace-scoping matters because the audit consumer dashboards
    are scoped per-tenant; routing authz decisions to the
    ``__platform__`` bucket would invisibly leak permission checks
    across tenants.
    """
    metadata = FakeMetadataAdapter()
    await audit_authz_decision(
        metadata,  # type: ignore[arg-type]
        workspace_id="ws-tenant-a",
        actor="op:carol",
        principal_id="op:carol",
        path="/v1/workspaces/ws-tenant-a/leases/lx:revoke",
        method="POST",
        permission="lease.revoke",
        allowed=allowed,
    )
    ws, evt = metadata.append_audit_calls[0]
    assert ws == "ws-tenant-a"
    assert evt.event_type == EVENT_AUTHZ_DECISION
    assert evt.subject == {"principal_id": "op:carol"}
    assert evt.payload["permission"] == "lease.revoke"
    assert evt.payload["allowed"] is allowed

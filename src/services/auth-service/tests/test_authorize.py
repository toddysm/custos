"""Tests for the Phase E authorization decision engine (AS-IMPL-011).

Covers:

* Allow when a workspace binding grants the requested permission.
* Allow when a tenant-scope binding grants the requested permission.
* Allow when a platform-scope ``platform.admin`` binding short-circuits.
* Deny when no bindings exist for the principal in the relevant scopes.
* Deny when bindings exist but none grants the requested permission.
* Deny with ``workspace-not-found`` when the workspace doesn't exist.
* Audit emission: exactly one ``authz.decision`` event per call (allow + deny).
* Audit failure does not raise from :func:`authorize`.
* :class:`UnknownPermissionError` when ``declared_permissions`` is
  supplied and the permission is absent.
* :class:`Decision.audit_event_id` matches the emitted event id.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

import pytest
from custos_spl import AuthStoreProvider, MetadataStoreProvider
from custos_spl.ids import (
    PrincipalId,
    RoleBindingId,
    TenantId,
    WorkspaceId,
)
from custos_spl.interfaces.auth_store import (
    GlobalScope,
    RoleBinding,
    TenantScope,
    Workspace,
    WorkspaceScope,
)

from custos_auth.audit import EVENT_AUTHZ_DECISION
from custos_auth.authorize import (
    REASON_ALLOW_BOUND,
    REASON_ALLOW_PLATFORM_ADMIN,
    REASON_DENY_NO_BINDING,
    REASON_DENY_PERMISSION_NOT_GRANTED,
    REASON_DENY_WORKSPACE_NOT_FOUND,
    Decision,
    UnknownPermissionError,
    authorize,
)
from custos_auth.roles import (
    ROLE_PLATFORM_ADMIN,
    ROLE_TENANT_ADMIN,
    ROLE_WORKSPACE_VIEWER,
)
from tests._fakes import FakeAuthAdapter, FakeMetadataAdapter

TENANT = "tenant-1"
WORKSPACE = "ws-1"
PRINCIPAL = "user-1"


async def _call(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
    /,
    **kwargs: Any,
) -> Decision:
    """Cast wrapper around :func:`authorize`.

    The in-memory fakes implement the SPL Protocol surface structurally
    but mypy --strict cannot infer Protocol conformance for them, so
    cast at the call boundary and keep the test bodies clean.
    """
    return await authorize(
        cast(AuthStoreProvider, auth_store),
        cast(MetadataStoreProvider, metadata_store),
        **kwargs,
    )


@pytest.fixture
def auth_store() -> FakeAuthAdapter:
    store = FakeAuthAdapter()
    store.workspaces[WORKSPACE] = Workspace(
        workspace_id=WorkspaceId(WORKSPACE),
        tenant_id=TenantId(TENANT),
        display_name="ws-1",
        disabled_at=None,
        created_at=datetime.now(UTC),
    )
    return store


@pytest.fixture
def metadata_store() -> FakeMetadataAdapter:
    return FakeMetadataAdapter()


def _grant(
    store: FakeAuthAdapter,
    *,
    role_id: str,
    scope: WorkspaceScope | TenantScope | GlobalScope,
    principal_id: str = PRINCIPAL,
) -> str:
    binding_id = str(uuid4())
    store.role_bindings[binding_id] = RoleBinding(
        binding_id=RoleBindingId(binding_id),
        principal_id=PrincipalId(principal_id),
        role_id=role_id,  # type: ignore[arg-type]
        scope=scope,
        bound_at=datetime.now(UTC),
        bound_by=PrincipalId("seed"),
    )
    return binding_id


def _assert_one_audit(
    metadata_store: FakeMetadataAdapter,
    *,
    decision: str,
    reason: str,
    workspace_id: str | None,
) -> str:
    audits = [
        event
        for _, event in metadata_store.append_audit_calls
        if event.event_type == EVENT_AUTHZ_DECISION
    ]
    assert len(audits) == 1, f"expected 1 authz.decision row, got {len(audits)}"
    event = audits[0]
    assert event.payload["decision"] == decision
    assert event.payload["reason"] == reason
    assert event.subject["principal_id"] == PRINCIPAL
    assert event.subject["workspace_id"] == workspace_id
    return str(event.event_id)


# ---------------------------------------------------------------------------
# Allow paths
# ---------------------------------------------------------------------------


async def test_allow_workspace_binding_grants_permission(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
) -> None:
    _grant(
        auth_store,
        role_id=ROLE_WORKSPACE_VIEWER,
        scope=WorkspaceScope(workspace_id=WorkspaceId(WORKSPACE)),
    )

    decision = await _call(
        auth_store,
        metadata_store,
        principal_id=PRINCIPAL,
        permission="workflow:read",
        workspace_id=WORKSPACE,
        caller_component="api-gateway",
    )

    assert isinstance(decision, Decision)
    assert decision.allowed is True
    assert decision.reason == REASON_ALLOW_BOUND
    audit_id = _assert_one_audit(
        metadata_store,
        decision="allow",
        reason=REASON_ALLOW_BOUND,
        workspace_id=WORKSPACE,
    )
    assert decision.audit_event_id == audit_id


async def test_allow_tenant_scope_binding(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
) -> None:
    _grant(
        auth_store,
        role_id=ROLE_TENANT_ADMIN,
        scope=TenantScope(tenant_id=TenantId(TENANT)),
    )

    decision = await _call(
        auth_store,
        metadata_store,
        principal_id=PRINCIPAL,
        permission="admin:workspace",
        workspace_id=WORKSPACE,
        caller_component="api-gateway",
    )

    assert decision.allowed is True
    assert decision.reason == REASON_ALLOW_BOUND


async def test_platform_admin_short_circuits(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
) -> None:
    _grant(
        auth_store,
        role_id=ROLE_PLATFORM_ADMIN,
        scope=GlobalScope(),
    )

    decision = await _call(
        auth_store,
        metadata_store,
        principal_id=PRINCIPAL,
        # A permission no role grants — platform.admin must still allow.
        permission="some:unrelated-permission",
        workspace_id=WORKSPACE,
        caller_component="api-gateway",
    )

    assert decision.allowed is True
    assert decision.reason == REASON_ALLOW_PLATFORM_ADMIN
    _assert_one_audit(
        metadata_store,
        decision="allow",
        reason=REASON_ALLOW_PLATFORM_ADMIN,
        workspace_id=WORKSPACE,
    )


# ---------------------------------------------------------------------------
# Deny paths
# ---------------------------------------------------------------------------


async def test_deny_no_binding(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
) -> None:
    decision = await _call(
        auth_store,
        metadata_store,
        principal_id=PRINCIPAL,
        permission="workflow:read",
        workspace_id=WORKSPACE,
        caller_component="api-gateway",
    )

    assert decision.allowed is False
    assert decision.reason == REASON_DENY_NO_BINDING
    _assert_one_audit(
        metadata_store,
        decision="deny",
        reason=REASON_DENY_NO_BINDING,
        workspace_id=WORKSPACE,
    )


async def test_deny_binding_does_not_grant_permission(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
) -> None:
    # Viewer can read workflows but cannot execute them.
    _grant(
        auth_store,
        role_id=ROLE_WORKSPACE_VIEWER,
        scope=WorkspaceScope(workspace_id=WorkspaceId(WORKSPACE)),
    )

    decision = await _call(
        auth_store,
        metadata_store,
        principal_id=PRINCIPAL,
        permission="workflow:execute",
        workspace_id=WORKSPACE,
        caller_component="api-gateway",
    )

    assert decision.allowed is False
    assert decision.reason == REASON_DENY_PERMISSION_NOT_GRANTED
    _assert_one_audit(
        metadata_store,
        decision="deny",
        reason=REASON_DENY_PERMISSION_NOT_GRANTED,
        workspace_id=WORKSPACE,
    )


async def test_deny_workspace_not_found(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
) -> None:
    decision = await _call(
        auth_store,
        metadata_store,
        principal_id=PRINCIPAL,
        permission="workflow:read",
        workspace_id="does-not-exist",
        caller_component="api-gateway",
    )

    assert decision.allowed is False
    assert decision.reason == REASON_DENY_WORKSPACE_NOT_FOUND
    # workspace_id on the audit row is None when the workspace was missing.
    _assert_one_audit(
        metadata_store,
        decision="deny",
        reason=REASON_DENY_WORKSPACE_NOT_FOUND,
        workspace_id=None,
    )


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


async def test_unknown_permission_raises_when_registry_supplied(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
) -> None:
    declared = frozenset({"workflow:read"})

    with pytest.raises(UnknownPermissionError):
        await _call(
            auth_store,
            metadata_store,
            principal_id=PRINCIPAL,
            permission="not:declared",
            workspace_id=WORKSPACE,
            caller_component="api-gateway",
            declared_permissions=declared,
        )

    # No audit row is written for the unknown-permission early-refuse:
    # the caller (HTTP layer) is responsible for surfacing the 500.
    assert metadata_store.append_audit_calls == []


async def test_audit_failure_does_not_raise(
    auth_store: FakeAuthAdapter,
) -> None:
    metadata_store = FakeMetadataAdapter(append_audit_should_fail=True)

    decision = await _call(
        auth_store,
        metadata_store,
        principal_id=PRINCIPAL,
        permission="workflow:read",
        workspace_id=WORKSPACE,
        caller_component="api-gateway",
    )

    # Decision is still produced and the event_id is a real (generated)
    # uuid-shaped string — emission failure is best-effort and bumps
    # EMIT_FAILURES_TOTAL out-of-band.
    assert decision.allowed is False
    assert decision.reason == REASON_DENY_NO_BINDING
    assert decision.audit_event_id != ""
    assert metadata_store.append_audit_calls == []


async def test_actor_defaults_to_principal_id(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
) -> None:
    await _call(
        auth_store,
        metadata_store,
        principal_id=PRINCIPAL,
        permission="workflow:read",
        workspace_id=WORKSPACE,
        caller_component="api-gateway",
    )

    audits = metadata_store.append_audit_calls
    assert len(audits) == 1
    _, event = audits[0]
    assert event.actor == PRINCIPAL


async def test_actor_override_recorded_on_audit_row(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
) -> None:
    await _call(
        auth_store,
        metadata_store,
        principal_id=PRINCIPAL,
        permission="workflow:read",
        workspace_id=WORKSPACE,
        caller_component="workflow-service",
        actor="component:workflow-service",
    )

    _, event = metadata_store.append_audit_calls[0]
    assert event.actor == "component:workflow-service"
    assert event.payload["caller_component"] == "workflow-service"


async def test_workspace_scope_audit_recorded_under_target_workspace(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
) -> None:
    # Audit row should be filed under the *targeted* workspace, not the
    # PLATFORM sentinel, so per-workspace audit feeds see the decision.
    await _call(
        auth_store,
        metadata_store,
        principal_id=PRINCIPAL,
        permission="workflow:read",
        workspace_id=WORKSPACE,
        caller_component="api-gateway",
    )

    workspace_id, _ = metadata_store.append_audit_calls[0]
    assert workspace_id == WORKSPACE

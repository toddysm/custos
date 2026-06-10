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
from custos_auth.authz_cache import AuthzDecisionCache
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

    Defaults ``caller_tenant_id`` to :data:`TENANT` so every test that
    does not explicitly exercise the cross-tenant existence-hiding
    path looks like a same-tenant call from the caller's perspective.
    Pass ``caller_tenant_id=...`` (or ``None``) to override.
    """
    kwargs.setdefault("caller_tenant_id", TENANT)
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
        permission="catalog:workflows:read",
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


# ---------------------------------------------------------------------------
# Cross-tenant existence-hiding (the design's
# "never disclose existence cross-tenant" rule)
# ---------------------------------------------------------------------------


async def test_cross_tenant_workspace_collapses_to_workspace_not_found(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
) -> None:
    # Workspace exists but belongs to ``tenant-2``; caller is in
    # ``tenant-1``. The result must be indistinguishable from a
    # genuine missing-workspace probe: deny-workspace-not-found with
    # workspace_id=None on the audit row.
    other_tenant = "tenant-2"
    other_ws = "ws-in-other-tenant"
    auth_store.workspaces[other_ws] = Workspace(
        workspace_id=WorkspaceId(other_ws),
        tenant_id=TenantId(other_tenant),
        display_name=other_ws,
        disabled_at=None,
        created_at=datetime.now(UTC),
    )
    # Even a workspace-scope binding on the *other* tenant's workspace
    # must not leak — the engine returns workspace-not-found before it
    # ever looks at the bindings.
    _grant(
        auth_store,
        role_id=ROLE_WORKSPACE_VIEWER,
        scope=WorkspaceScope(workspace_id=WorkspaceId(other_ws)),
    )

    decision = await _call(
        auth_store,
        metadata_store,
        principal_id=PRINCIPAL,
        permission="workflow:read",
        workspace_id=other_ws,
        caller_component="api-gateway",
        caller_tenant_id=TENANT,
    )

    assert decision.allowed is False
    assert decision.reason == REASON_DENY_WORKSPACE_NOT_FOUND
    _assert_one_audit(
        metadata_store,
        decision="deny",
        reason=REASON_DENY_WORKSPACE_NOT_FOUND,
        workspace_id=None,
    )


async def test_missing_caller_tenant_collapses_to_workspace_not_found(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
) -> None:
    # A caller with no resolved tenant context (``caller_tenant_id=None``)
    # cannot prove same-tenant standing for any workspace, so the
    # engine collapses every probe to workspace-not-found per the
    # design's strict default.
    decision = await _call(
        auth_store,
        metadata_store,
        principal_id=PRINCIPAL,
        permission="workflow:read",
        workspace_id=WORKSPACE,
        caller_component="api-gateway",
        caller_tenant_id=None,
    )

    assert decision.allowed is False
    assert decision.reason == REASON_DENY_WORKSPACE_NOT_FOUND
    _assert_one_audit(
        metadata_store,
        decision="deny",
        reason=REASON_DENY_WORKSPACE_NOT_FOUND,
        workspace_id=None,
    )


async def test_platform_admin_caller_bypasses_tenant_gate(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
) -> None:
    # A platform-admin caller may target any tenant's workspaces. The
    # decision still depends on bindings — here no binding exists for
    # the other tenant's workspace, so the result is
    # ``deny-no-binding`` (NOT ``workspace-not-found``). The audit row
    # is filed under the targeted workspace because the platform-admin
    # claim made the existence visible.
    other_tenant = "tenant-2"
    other_ws = "ws-in-other-tenant"
    auth_store.workspaces[other_ws] = Workspace(
        workspace_id=WorkspaceId(other_ws),
        tenant_id=TenantId(other_tenant),
        display_name=other_ws,
        disabled_at=None,
        created_at=datetime.now(UTC),
    )

    decision = await _call(
        auth_store,
        metadata_store,
        principal_id=PRINCIPAL,
        permission="workflow:read",
        workspace_id=other_ws,
        caller_component="api-gateway",
        caller_tenant_id=TENANT,
        caller_is_platform_admin=True,
    )

    assert decision.allowed is False
    assert decision.reason == REASON_DENY_NO_BINDING
    _assert_one_audit(
        metadata_store,
        decision="deny",
        reason=REASON_DENY_NO_BINDING,
        workspace_id=other_ws,
    )


# ---------------------------------------------------------------------------
# Decision-cache integration (AS-IMPL-012)
# ---------------------------------------------------------------------------


async def test_cache_hit_short_circuits_binding_resolution(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
) -> None:
    # Prime the cache with an "allow-bound" entry. The auth store has
    # no bindings for the principal, so the only way ``authorize`` can
    # return allow is if it consulted the cache and skipped the binding
    # resolution.
    cache = AuthzDecisionCache(ttl_seconds=60)
    cache.put(
        PRINCIPAL,
        WORKSPACE,
        "workflow:read",
        allowed=True,
        reason=REASON_ALLOW_BOUND,
    )

    decision = await _call(
        auth_store,
        metadata_store,
        principal_id=PRINCIPAL,
        permission="workflow:read",
        workspace_id=WORKSPACE,
        caller_component="api-gateway",
        cache=cache,
    )

    assert decision.allowed is True
    assert decision.reason == REASON_ALLOW_BOUND
    # Cache-hit counter bumped; miss counter untouched.
    assert cache.hits == 1
    assert cache.misses == 0
    # Audit row still emitted (the cache short-cuts resolution, never
    # the audit trail).
    audit_id = _assert_one_audit(
        metadata_store,
        decision="allow",
        reason=REASON_ALLOW_BOUND,
        workspace_id=WORKSPACE,
    )
    # Each call generates its own audit event id.
    assert decision.audit_event_id == audit_id


async def test_cache_miss_populates_the_cache(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
) -> None:
    _grant(
        auth_store,
        role_id=ROLE_WORKSPACE_VIEWER,
        scope=WorkspaceScope(workspace_id=WorkspaceId(WORKSPACE)),
    )
    cache = AuthzDecisionCache(ttl_seconds=60)

    decision = await _call(
        auth_store,
        metadata_store,
        principal_id=PRINCIPAL,
        permission="catalog:workflows:read",
        workspace_id=WORKSPACE,
        caller_component="api-gateway",
        cache=cache,
    )

    assert decision.allowed is True
    assert decision.reason == REASON_ALLOW_BOUND
    # First call missed and populated the cache.
    assert cache.misses == 1
    cached = cache.get(PRINCIPAL, WORKSPACE, "catalog:workflows:read")
    assert cached is not None
    assert cached.allowed is True
    assert cached.reason == REASON_ALLOW_BOUND


async def test_cache_disabled_does_not_store_decisions(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
) -> None:
    # CUSTOS_AUTH_AUTHZ_CACHE_TTL=0 bypass mode: every call resolves
    # from the auth store; nothing is ever written to or read from
    # the cache.
    _grant(
        auth_store,
        role_id=ROLE_WORKSPACE_VIEWER,
        scope=WorkspaceScope(workspace_id=WorkspaceId(WORKSPACE)),
    )
    cache = AuthzDecisionCache(ttl_seconds=0)

    await _call(
        auth_store,
        metadata_store,
        principal_id=PRINCIPAL,
        permission="workflow:read",
        workspace_id=WORKSPACE,
        caller_component="api-gateway",
        cache=cache,
    )

    assert cache.hits == 0
    assert cache.misses == 0  # disabled get() short-circuits before counters
    assert cache.get(PRINCIPAL, WORKSPACE, "workflow:read") is None


async def test_cross_tenant_deny_is_not_cached(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
) -> None:
    # Existence-hiding denials depend on caller_tenant_id, not on the
    # cache-key triple alone. Caching them would let a same-tenant
    # follow-up call read a poisoned entry. The engine must therefore
    # never write a cross-tenant deny to the cache.
    other_tenant = "tenant-2"
    other_ws = "ws-in-other-tenant"
    auth_store.workspaces[other_ws] = Workspace(
        workspace_id=WorkspaceId(other_ws),
        tenant_id=TenantId(other_tenant),
        display_name=other_ws,
        disabled_at=None,
        created_at=datetime.now(UTC),
    )
    cache = AuthzDecisionCache(ttl_seconds=60)

    decision = await _call(
        auth_store,
        metadata_store,
        principal_id=PRINCIPAL,
        permission="workflow:read",
        workspace_id=other_ws,
        caller_component="api-gateway",
        caller_tenant_id=TENANT,
        cache=cache,
    )

    assert decision.reason == REASON_DENY_WORKSPACE_NOT_FOUND
    # Cache was never consulted on the existence-hiding path — the
    # gate runs before the lookup. Check counters *before* probing
    # the cache because ``get`` itself bumps the miss counter.
    assert cache.hits == 0
    assert cache.misses == 0
    assert cache.get(PRINCIPAL, other_ws, "workflow:read") is None


async def test_missing_workspace_deny_is_not_cached(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
) -> None:
    cache = AuthzDecisionCache(ttl_seconds=60)

    decision = await _call(
        auth_store,
        metadata_store,
        principal_id=PRINCIPAL,
        permission="workflow:read",
        workspace_id="does-not-exist",
        caller_component="api-gateway",
        cache=cache,
    )

    assert decision.reason == REASON_DENY_WORKSPACE_NOT_FOUND
    # ``get`` bumps the miss counter, so check counters first.
    assert cache.hits == 0
    assert cache.misses == 0
    assert cache.get(PRINCIPAL, "does-not-exist", "workflow:read") is None


async def test_cache_miss_then_hit_skips_auth_store_on_second_call(
    auth_store: FakeAuthAdapter,
    metadata_store: FakeMetadataAdapter,
) -> None:
    _grant(
        auth_store,
        role_id=ROLE_WORKSPACE_VIEWER,
        scope=WorkspaceScope(workspace_id=WorkspaceId(WORKSPACE)),
    )
    cache = AuthzDecisionCache(ttl_seconds=60)

    # First call \u2014 cache miss, populates from the auth store.
    first = await _call(
        auth_store,
        metadata_store,
        principal_id=PRINCIPAL,
        permission="catalog:workflows:read",
        workspace_id=WORKSPACE,
        caller_component="api-gateway",
        cache=cache,
    )
    assert first.allowed is True
    assert cache.misses == 1
    assert cache.hits == 0

    # Drop the binding to prove the second call did NOT consult the
    # auth store \u2014 if it did, the result would flip to deny-no-binding.
    auth_store.role_bindings.clear()

    second = await _call(
        auth_store,
        metadata_store,
        principal_id=PRINCIPAL,
        permission="catalog:workflows:read",
        workspace_id=WORKSPACE,
        caller_component="api-gateway",
        cache=cache,
    )
    assert second.allowed is True
    assert second.reason == REASON_ALLOW_BOUND
    assert cache.hits == 1
    # Each call emitted its own audit row \u2014 two total now.
    rows = [
        event
        for _, event in metadata_store.append_audit_calls
        if event.event_type == EVENT_AUTHZ_DECISION
    ]
    assert len(rows) == 2

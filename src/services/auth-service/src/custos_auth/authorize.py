"""Authorization decision engine (Phase E / AS-IMPL-011, GH-#246).

Implements the canonical :func:`authorize` entry point per
``design/components/auth-service/design.md`` § Authorization decision.

Decision shape
--------------

Every call returns a :class:`Decision` carrying ``allowed``, a human-
readable ``reason`` (one of the :data:`REASON_*` constants below), and
the ``audit_event_id`` written to the SPL audit outbox so callers can
correlate denials with the audit row. The shape mirrors the design's
``Decision{allowed, reason, auditEventId}`` triple.

Resolution
----------

1. Resolve the workspace. Missing-or-cross-tenant collapses to
   :data:`REASON_DENY_WORKSPACE_NOT_FOUND` with existence-hiding
   semantics — the audit row records ``workspace_id=None`` so
   per-workspace audit feeds for the targeted workspace do **not**
   leak the cross-tenant probe. The tenant check requires the caller
   to supply ``caller_tenant_id`` (the tenant component of the
   :class:`CallContext`); when the caller is a platform admin the
   tenant gate is skipped via ``caller_is_platform_admin=True``.
   Callers that pass neither (``caller_tenant_id=None`` and
   ``caller_is_platform_admin=False``) are treated as tenant-less and
   collapse to ``workspace-not-found`` — the design's "never disclose
   existence cross-tenant" rule is the strict default.
2. Look up the principal's bindings at workspace, tenant, and
   platform-global scopes in a single :meth:`list_role_bindings_for_principal`
   round-trip.
3. Resolve each binding's role to its permission tuple via the
   in-process :data:`BUILTIN_ROLES_BY_ID` registry. Unknown roles
   contribute no permissions (custom roles land in M2+ and will
   resolve via :meth:`get_role` once that surface is live).
4. ``role:platform.admin`` short-circuits to allow regardless of the
   requested permission.
5. Allow if the requested permission is in the union of resolved
   permission names; deny otherwise.

Auditing
--------

Every decision (allow and deny) emits exactly one ``authz.decision``
row via :func:`custos_auth.audit.audit_authz_decision`. Emission is
best-effort post-commit (the SPL ``with_transaction`` primitive is
intra-provider only — auth-store and metadata-store run in independent
transaction domains). The generated event id is returned on the
:class:`Decision` whether or not the outbox write succeeded; drops
bump :data:`custos_auth.audit.EMIT_FAILURES_TOTAL` and page
Observability.

Caching
-------

:func:`authorize` accepts an optional
:class:`~custos_auth.authz_cache.AuthzDecisionCache`. The cache sits
*after* the tenant existence-hiding gate and *before* the binding
resolution — a cache hit short-cuts only the
:meth:`list_role_bindings_for_principal` round trip, never the
tenant check and never the audit row. The cache is keyed by
``(principal_id, workspace_id, permission)`` so it is callera-state
independent; only post-tenant-gate decisions are stored. Cross-tenant
probes and missing-workspace probes are *not* cached because the
existence-hiding outcome depends on the caller's tenant id, not on
``(principal, workspace, permission)`` alone.

Every cache hit still emits a fresh :data:`~custos_auth.audit.EVENT_AUTHZ_DECISION`
audit row with a new ``audit_event_id`` so the audit ledger remains
the source of truth even when the cache serves the answer. When the
cache is disabled (``CUSTOS_AUTH_AUTHZ_CACHE_TTL=0``) the
``cache.get`` / ``cache.put`` calls short-circuit and the resolution
always runs.

Unknown permissions
-------------------

The startup gate in AS-IMPL-008 refuses to start the service when any
*built-in role* references an undeclared permission. The runtime
:func:`authorize` call is therefore guaranteed to see only declared
permissions for the built-in role set. If a caller passes a
permission name that no service has declared — i.e. an operator typo
or a programming error — :func:`authorize` raises
:class:`UnknownPermissionError`. The HTTP/RPC surface translates that
to ``500 unknown-permission``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from custos_spl.ids import PrincipalId, WorkspaceId
from custos_spl.interfaces.auth_store import (
    GlobalScope,
    RoleBindingScope,
    TenantScope,
    WorkspaceScope,
)

from custos_auth.audit import audit_authz_decision
from custos_auth.roles import BUILTIN_ROLES_BY_ID, ROLE_PLATFORM_ADMIN

if TYPE_CHECKING:
    from custos_spl import AuthStoreProvider, MetadataStoreProvider

    from custos_auth.authz_cache import AuthzDecisionCache

_LOGGER = logging.getLogger("custos_auth.authorize")


# ---------------------------------------------------------------------------
# Decision shape + reason codes
# ---------------------------------------------------------------------------

#: Decision-side reason codes. Strings, not enums, so the audit row
#: payload renders as plain JSON and operators can grep the audit log.
REASON_ALLOW_PLATFORM_ADMIN: Final[str] = "allow-platform-admin"
REASON_ALLOW_BOUND: Final[str] = "allow-bound"
REASON_DENY_NO_BINDING: Final[str] = "deny-no-binding"
REASON_DENY_PERMISSION_NOT_GRANTED: Final[str] = "deny-permission-not-granted"
REASON_DENY_WORKSPACE_NOT_FOUND: Final[str] = "deny-workspace-not-found"

#: ``decision`` field values on the ``authz.decision`` audit row.
_DECISION_ALLOW: Final[str] = "allow"
_DECISION_DENY: Final[str] = "deny"


@dataclass(frozen=True, slots=True)
class Decision:
    """Result of an :func:`authorize` call.

    The triple matches the design's
    ``Decision{allowed, reason, auditEventId}`` so the HTTP/RPC
    surface can flatten it onto the wire envelope without
    re-projection.
    """

    allowed: bool
    reason: str
    audit_event_id: str


class UnknownPermissionError(KeyError):
    """Raised when :func:`authorize` is asked about a permission name
    that no service has declared.

    Startup (AS-IMPL-008) refuses the boot if any built-in role
    references an undeclared permission, so reaching this exception
    means an operator typed the wrong permission name or a programming
    error in a new service. The HTTP/RPC surface returns ``500
    unknown-permission``.
    """


# ---------------------------------------------------------------------------
# Resolution helpers
# ---------------------------------------------------------------------------


def _resolve_permission_set(bindings: tuple[object, ...]) -> set[str]:
    """Union the permission names contributed by ``bindings``.

    Each binding's :attr:`role_id` is looked up in
    :data:`BUILTIN_ROLES_BY_ID`. Unknown role ids contribute nothing
    so the union grows monotonically (custom roles are an M2+ feature
    and currently never appear here).
    """
    granted: set[str] = set()
    for binding in bindings:
        role_id = getattr(binding, "role_id", None)
        if role_id is None:
            continue
        role = BUILTIN_ROLES_BY_ID.get(role_id)
        if role is None:
            continue
        granted.update(role.permission_names)
    return granted


def _is_platform_admin(bindings: tuple[object, ...]) -> bool:
    """``True`` if any binding grants the platform-admin role."""
    return any(getattr(b, "role_id", None) == ROLE_PLATFORM_ADMIN for b in bindings)


def _build_scope_set(
    workspace_id: str,
    tenant_id: str | None,
) -> tuple[RoleBindingScope, ...]:
    """Return the canonical (workspace, tenant, global) scope triple.

    ``tenant_id`` may be ``None`` for tests / call paths that haven't
    resolved the workspace's parent tenant; in that case only the
    workspace + global scopes are read.
    """
    scopes: list[RoleBindingScope] = [
        WorkspaceScope(workspace_id=WorkspaceId(workspace_id)),
        GlobalScope(),
    ]
    if tenant_id is not None:
        from custos_spl.ids import TenantId  # local: avoid cycle on import

        scopes.insert(1, TenantScope(tenant_id=TenantId(tenant_id)))
    return tuple(scopes)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


async def authorize(
    auth_store: AuthStoreProvider,
    metadata_store: MetadataStoreProvider,
    *,
    principal_id: str,
    permission: str,
    workspace_id: str,
    caller_component: str,
    caller_tenant_id: str | None = None,
    caller_is_platform_admin: bool = False,
    actor: str | None = None,
    declared_permissions: frozenset[str] | None = None,
    cache: AuthzDecisionCache | None = None,
) -> Decision:
    """Decide whether ``principal_id`` may perform ``permission`` in
    ``workspace_id``.

    Args:
        auth_store: SPL :class:`AuthStoreProvider` used to look up the
            workspace and the principal's bindings.
        metadata_store: SPL :class:`MetadataStoreProvider` used to
            append the ``authz.decision`` audit row.
        principal_id: Subject of the authorization check.
        permission: Permission name (e.g. ``"workflow:execute"``).
        workspace_id: Workspace the action targets. ``"__platform__"``
            and similar sentinels are not valid here — the call-site
            should target a real workspace.
        caller_component: Component name that initiated the check
            (``"api-gateway"``, ``"workflow-service"``, …). Recorded
            on the audit row's ``caller_component`` payload field.
        caller_tenant_id: Tenant component of the call-context. When
            the workspace exists but belongs to a different tenant
            (or ``caller_tenant_id`` is ``None``), :func:`authorize`
            collapses the result to
            :data:`REASON_DENY_WORKSPACE_NOT_FOUND` and audits with
            ``workspace_id=None`` so the cross-tenant probe is not
            disclosed by the per-workspace audit feed of the targeted
            workspace. Pass the tenant id resolved by the call-context
            verifier.
        caller_is_platform_admin: When ``True`` skips the tenant gate
            — a platform admin may target any tenant's workspaces.
            The verifier/middleware derives this flag from the
            call-context's platform-admin claim. The binding-side
            ``role:platform.admin`` short-circuit (Resolution step 4)
            remains in effect for principals whose admin status is
            not yet projected onto the call-context.
        actor: Optional override for the audit row's ``actor`` field.
            Defaults to ``principal_id``; internal components that
            re-check on behalf of a user can pass their own component
            id here.
        declared_permissions: Optional registry snapshot from
            :func:`seed_permissions_and_validate_roles`. When supplied
            and the requested permission is missing,
            :class:`UnknownPermissionError` is raised instead of
            silently denying. The HTTP/RPC surface passes the snapshot
            from ``app.state.declared_permissions``; pure-library
            callers may omit it.
        cache: Optional per-replica
            :class:`~custos_auth.authz_cache.AuthzDecisionCache`
            consulted *after* the tenant existence-hiding gate and
            *before* the binding resolution. The HTTP/RPC surface
            passes ``providers.authz_cache``. A hit short-cuts the
            :meth:`list_role_bindings_for_principal` round trip;
            audit emission still runs (a new ``audit_event_id`` is
            generated for every call). Cross-tenant and missing-
            workspace probes are never cached. When ``None`` the
            cache machinery is bypassed entirely — useful for
            diagnostic call-sites that must always hit the auth
            store.

    Returns:
        A :class:`Decision`. The ``audit_event_id`` is set whether or
        not the outbox write succeeded; emission failure does not
        propagate.

    Raises:
        UnknownPermissionError: When ``declared_permissions`` is
            supplied and ``permission`` is not in it.
    """
    if declared_permissions is not None and permission not in declared_permissions:
        # Refuse early — startup already gated declared references, so
        # reaching this branch means an operator typed an unknown name
        # at call time.
        raise UnknownPermissionError(permission)

    audit_actor = actor if actor is not None else principal_id

    workspace = await auth_store.get_workspace(WorkspaceId(workspace_id))
    # Existence-hiding: a missing workspace and a workspace that exists
    # in a *different* tenant from the caller must be indistinguishable
    # on the wire. Both paths collapse to ``deny-workspace-not-found``
    # and audit with ``workspace_id=None`` so the per-workspace audit
    # feed of the targeted workspace does not leak the probe. These
    # caller-state-dependent denials are *not* cached — a later same-
    # tenant caller targeting the same key must reach the normal
    # binding-resolution path.
    if workspace is None or (
        not caller_is_platform_admin
        and (caller_tenant_id is None or caller_tenant_id != str(workspace.tenant_id))
    ):
        return await _emit_decision(
            metadata_store,
            audit_actor=audit_actor,
            workspace_id=None,
            principal_id=principal_id,
            permission=permission,
            allowed=False,
            reason=REASON_DENY_WORKSPACE_NOT_FOUND,
            caller_component=caller_component,
        )

    # Cache lookup happens *after* the tenant gate so cross-tenant
    # probes can never read a poisoned entry. The cache key is
    # ``(principal_id, workspace_id, permission)`` — caller-state
    # independent — so it is safe to share across callers within the
    # tenant.
    if cache is not None:
        hit = cache.get(principal_id, workspace_id, permission)
        if hit is not None:
            return await _emit_decision(
                metadata_store,
                audit_actor=audit_actor,
                workspace_id=workspace_id,
                principal_id=principal_id,
                permission=permission,
                allowed=hit.allowed,
                reason=hit.reason,
                caller_component=caller_component,
            )

    scopes = _build_scope_set(workspace_id, str(workspace.tenant_id))
    bindings = await auth_store.list_role_bindings_for_principal(
        PrincipalId(principal_id),
        scopes,
    )

    # Resolve the binding set to a single (allowed, reason) tuple. The
    # platform.admin role's permission tuple is intentionally empty —
    # the engine treats the role marker itself as "allow everything"
    # so we don't have to enumerate every permission the platform
    # ever declares.
    if _is_platform_admin(bindings):
        allowed, reason = True, REASON_ALLOW_PLATFORM_ADMIN
    elif not bindings:
        allowed, reason = False, REASON_DENY_NO_BINDING
    else:
        granted = _resolve_permission_set(bindings)
        if permission in granted:
            allowed, reason = True, REASON_ALLOW_BOUND
        else:
            allowed, reason = False, REASON_DENY_PERMISSION_NOT_GRANTED

    # Populate the cache *before* emitting the audit row so a write
    # failure on the outbox does not prevent the next call from hitting
    # the cached answer (audit drops are accounted out-of-band via
    # EMIT_FAILURES_TOTAL).
    if cache is not None:
        cache.put(
            principal_id,
            workspace_id,
            permission,
            allowed=allowed,
            reason=reason,
        )

    return await _emit_decision(
        metadata_store,
        audit_actor=audit_actor,
        workspace_id=workspace_id,
        principal_id=principal_id,
        permission=permission,
        allowed=allowed,
        reason=reason,
        caller_component=caller_component,
    )


async def _emit_decision(
    metadata_store: MetadataStoreProvider,
    *,
    audit_actor: str,
    workspace_id: str | None,
    principal_id: str,
    permission: str,
    allowed: bool,
    reason: str,
    caller_component: str,
) -> Decision:
    """Emit the ``authz.decision`` audit row and shape the
    :class:`Decision` return value.

    Factored out so each return site — existence-hiding deny, cache
    hit, and the resolved tail — shares a single emission
    implementation. Keeping the audit emission funnel narrow makes
    the "exactly one row per call" invariant trivially auditable.
    """
    event_id = await audit_authz_decision(
        metadata_store,
        actor=audit_actor,
        workspace_id=workspace_id,
        principal_id=principal_id,
        permission=permission,
        decision=_DECISION_ALLOW if allowed else _DECISION_DENY,
        reason=reason,
        caller_component=caller_component,
    )
    return Decision(allowed=allowed, reason=reason, audit_event_id=event_id)


__all__ = [
    "REASON_ALLOW_BOUND",
    "REASON_ALLOW_PLATFORM_ADMIN",
    "REASON_DENY_NO_BINDING",
    "REASON_DENY_PERMISSION_NOT_GRANTED",
    "REASON_DENY_WORKSPACE_NOT_FOUND",
    "Decision",
    "UnknownPermissionError",
    "authorize",
]

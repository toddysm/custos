"""OIDC identity binding helpers (AS-IMPL-007, GH-#242).

Auth Service stores ``(issuer, subject) → user_id`` bindings in the
``oidc_identity`` table via :class:`AuthStoreProvider.put_oidc_identity`.
The binding is **write-once**: re-binding the same ``(issuer, subject)``
pair to a different user requires an explicit delete + re-put workflow
(out of scope for Phase C).

Phase C ships **storage-side helpers only**. There are no HTTP routes
on the wire yet; the full OIDC verifier path that consumes these
bindings ships in Phase H (AS-IMPL-020 / AS-IMPL-021). Until then, the
helpers are used by:

* M1 bootstrap tooling that pre-binds the initial admin's OIDC subject.
* Tests for the verifier path.
* Phase H's invitation-accept flow (which will call
  :func:`link_oidc_identity` after Phase H provisions the user).

Audit
-----

:func:`link_oidc_identity` emits ``oidc.identity-linked`` against the
provided ``audit_workspace_id`` (or the platform sentinel when the
caller does not know a workspace context yet, e.g. during bootstrap).
The audit emission is best-effort post-write; the binding write itself
is the atomic operation the caller cares about.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from custos_spl.errors import ImmutableViolation
from custos_spl.ids import PrincipalId

from custos_auth.audit import PLATFORM_WORKSPACE_ID, audit_oidc_identity_linked

if TYPE_CHECKING:
    from custos_spl import AuthStoreProvider, MetadataStoreProvider


class OidcIdentityAlreadyBound(RuntimeError):
    """Raised when ``(issuer, subject)`` is already linked to a user.

    Mirrors the SPL ``ImmutableViolation`` but carries the original
    user_id (when the adapter can supply it) so callers can render an
    operator-actionable message. Callers that want a 4xx HTTP response
    should map this to :class:`custos_auth.api.errors.Conflict`.
    """

    def __init__(self, issuer: str, subject: str) -> None:
        super().__init__(
            f"OIDC identity (issuer='{issuer}', subject='{subject}') is "
            "already bound; rebinding requires explicit delete + re-put"
        )
        self.issuer = issuer
        self.subject = subject


async def link_oidc_identity(
    auth_store: AuthStoreProvider,
    metadata_store: MetadataStoreProvider | None,
    *,
    user_id: str,
    issuer: str,
    subject: str,
    actor: str | None = None,
    audit_workspace_id: str | None = None,
) -> None:
    """Bind ``(issuer, subject)`` to ``user_id``.

    Args:
        auth_store: SPL :class:`AuthStoreProvider`.
        metadata_store: Optional :class:`MetadataStoreProvider` for
            audit emission. ``None`` skips the audit write — used by
            bootstrap tooling that runs before the metadata provider
            is wired.
        user_id: Internal :class:`PrincipalId` (as a plain string).
        issuer: OIDC issuer URL (e.g. ``"https://login.example.com"``).
        subject: OIDC ``sub`` claim.
        actor: Optional principal id of the operator performing the
            bind. Defaults to ``"system"`` (bootstrap path) when
            omitted.
        audit_workspace_id: Workspace to attribute the audit row to.
            Defaults to :data:`PLATFORM_WORKSPACE_ID` because OIDC
            binds happen at tenant / platform scope, not inside a
            workspace.

    Raises:
        OidcIdentityAlreadyBound: When the SPL adapter rejects the
            put because the ``(issuer, subject)`` row already exists.
            Adapters surface this through :class:`ImmutableViolation`.
    """
    try:
        await auth_store.put_oidc_identity(
            issuer,
            subject,
            PrincipalId(user_id),
        )
    except ImmutableViolation as exc:
        raise OidcIdentityAlreadyBound(issuer, subject) from exc

    if metadata_store is not None:
        await audit_oidc_identity_linked(
            metadata_store,
            actor=actor or "system",
            workspace_id=audit_workspace_id or PLATFORM_WORKSPACE_ID,
            user_id=user_id,
            issuer=issuer,
            subject=subject,
        )


async def find_user_by_oidc(
    auth_store: AuthStoreProvider,
    *,
    issuer: str,
    subject: str,
) -> str | None:
    """Resolve ``(issuer, subject)`` to an internal user id.

    Returns the user id as a plain string (the SPL ``PrincipalId``
    newtype is a ``NewType`` alias for ``str``, so the cast is purely
    a runtime narrowing — at the typing layer, callers that want the
    typed alias re-wrap via :class:`custos_spl.ids.PrincipalId`).

    Returns ``None`` when no binding exists; callers treat that as
    "unknown OIDC identity, fall through to the unauthenticated
    path".
    """
    user_id = await auth_store.get_oidc_identity(issuer, subject)
    if user_id is None:
        return None
    return str(user_id)


__all__ = [
    "OidcIdentityAlreadyBound",
    "find_user_by_oidc",
    "link_oidc_identity",
]

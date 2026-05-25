"""OIDC provisioning policy (AS-IMPL-023).

After a successful OIDC verification, the platform has a verified
``(issuer, subject)`` pair but does not yet know the internal
:class:`custos_spl.User` it maps to. The provisioning policy bridges
that gap:

1. Look up ``(issuer, subject) → user_id`` via
   :func:`custos_auth.oidc_identity.find_user_by_oidc`.
2. **Existing user** — return the :class:`User` row.
3. **Unknown identity** — create a new :class:`User` with **zero
   workspace bindings**, link the OIDC identity to it, and emit
   ``oidc.identity-linked`` for the audit feed. This is policy (a)
   from the design's § Identity Sources: least-surprising, no
   implicit grants. Operators flip auto-onboarding via separate
   automation against the role-binding endpoint.

Per AS-IMPL-022, presets that publish a group claim can additionally
seed role bindings at link time. Phase H ships the data flow for
group-binding application but the link-time grant itself is gated
behind a TODO marker — the role-binding write path is owned by
AS-IMPL-010 and a v1-strict implementation cannot duplicate that
logic here. The verifier surfaces the matched bindings via
:attr:`ProvisionResult.matched_group_bindings` so the callback
handler can apply them via the existing role-binding API.

The provisioner is intentionally narrow: it owns the *identity*
linkage and one-shot User creation, nothing else. Tenant binding
defaults to the platform tenant (operators wire a default-tenant
policy via the issuer config in M2+).
"""

from __future__ import annotations

import logging
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from custos_spl import User
from custos_spl.ids import PrincipalId, TenantId

from custos_auth.audit import PLATFORM_WORKSPACE_ID
from custos_auth.oidc_identity import OidcIdentityAlreadyBound, link_oidc_identity

if TYPE_CHECKING:
    from custos_spl import AuthStoreProvider, MetadataStoreProvider

    from custos_auth.oidc.config import GroupBinding
    from custos_auth.oidc.verifier import VerifiedOidcIdentity

_LOG = logging.getLogger(__name__)

#: Tenant ID assigned to first-time OIDC users when the issuer
#: config does not pin a default tenant. ``platform`` is the
#: sentinel tenant Custos uses for cross-tenant principals; a
#: zero-binding user against ``platform`` cannot perform any
#: workspace operation until an admin grants a binding (which
#: simultaneously moves the principal to the granting tenant via
#: the role-binding API).
DEFAULT_PROVISION_TENANT_ID: str = "platform"

#: Prefix on auto-generated user IDs so they sort distinctly from
#: hand-provisioned IDs (``usr-`` for human-onboarded users from
#: an admin UI; ``oidc-usr-`` for OIDC zero-binding provisioning).
PROVISIONED_USER_ID_PREFIX: str = "oidc-usr-"


@dataclass(frozen=True, slots=True)
class ProvisionResult:
    """Outcome of a provisioning call.

    Used by the callback handler to render the response body
    (Principal id + display name) and to apply any group-binding
    grants surfaced by the issuer's preset.
    """

    user: User
    """The (newly created or pre-existing) internal User row."""

    newly_provisioned: bool
    """``True`` when this call created the User; ``False`` when the
    OIDC binding already existed. Drives whether the callback emits
    the ``oidc.identity-linked`` audit row (already done inside
    :func:`OidcProvisioner.provision`) and any first-login flows
    the gateway wants to inject.
    """

    matched_group_bindings: tuple[GroupBinding, ...] = ()
    """Group-binding rules the verified token's group claim matched.

    The provisioner surfaces matches; the callback handler is
    responsible for applying them via the role-binding API so the
    write goes through the same audit + permission-check path as
    an admin-driven grant. Empty tuple when the issuer has no
    ``group_claim`` set or the verified claim carried no matching
    values.
    """


def _generate_user_id() -> str:
    """Mint a fresh provisioned-user id."""
    # 16 bytes of entropy is overkill for a user id but matches the
    # service-token style used elsewhere in the auth-service code and
    # keeps the IDs unguessable on any reasonable input distribution.
    return PROVISIONED_USER_ID_PREFIX + secrets.token_hex(8)


def _match_group_bindings(
    identity: VerifiedOidcIdentity,
) -> tuple[GroupBinding, ...]:
    """Resolve group-binding matches from the verified token's claims.

    Returns the configured :class:`GroupBinding` entries whose
    ``claim_value`` appears in the claim named by
    :attr:`OidcIssuerConfig.group_claim`. Returns an empty tuple
    when no group claim is configured or the claim is absent /
    not a list of strings.
    """
    config = identity.issuer_config
    if config.group_claim is None or not config.group_bindings:
        return ()
    claim_value = identity.claims.get(config.group_claim)
    if not isinstance(claim_value, list):
        return ()
    values: set[str] = {str(item) for item in claim_value if isinstance(item, str)}
    if not values:
        return ()
    return tuple(rule for rule in config.group_bindings if rule.claim_value in values)


class OidcProvisioner:
    """Bridge verified-OIDC-identities to internal users.

    Carries the SPL store handles + the actor identity used on
    audit rows for system-driven provisioning (``"system"`` by
    default). Tests can override ``actor_id`` to make audit
    assertions deterministic.
    """

    def __init__(
        self,
        auth_store: AuthStoreProvider,
        metadata_store: MetadataStoreProvider | None,
        *,
        default_tenant_id: str = DEFAULT_PROVISION_TENANT_ID,
        actor_id: str = "system",
    ) -> None:
        self._auth_store = auth_store
        self._metadata_store = metadata_store
        self._default_tenant_id = default_tenant_id
        self._actor_id = actor_id

    async def provision(self, identity: VerifiedOidcIdentity) -> ProvisionResult:
        """Resolve or create the internal User for ``identity``.

        Workflow:

        1. Look up ``(issuer, subject)`` in the auth store.
        2. If present: load the User row and return as ``newly_provisioned=False``.
           Group-binding matches are still surfaced — operators may
           use the same rules as a re-evaluation hook on re-login.
        3. If absent: mint a fresh User row, persist it, link the
           OIDC identity (which also emits ``oidc.identity-linked``),
           and return ``newly_provisioned=True``.

        The implementation is intentionally not transactional — the
        SPL ``put_principal`` and ``put_oidc_identity`` calls are
        separate writes, and a crash between them leaves a dangling
        User with no OIDC binding. Operators reconcile via the
        ``oidc.identity-linked`` audit feed. A concurrent provisioner
        can also win the identity-link race after our initial
        ``get_oidc_identity`` check; in that case the duplicate link
        attempt raises :class:`OidcIdentityAlreadyBound` out of
        :meth:`provision`, and callers must catch it and reconcile the
        orphaned freshly-created User row via the accepted manual/audit
        reconciliation flow.
        """
        config = identity.issuer_config
        existing_user_id = await self._auth_store.get_oidc_identity(
            config.issuer_url, identity.subject
        )
        if existing_user_id is not None:
            user = await self._auth_store.get_principal(existing_user_id)
            if user is None or user.kind != "user":  # pragma: no cover — defensive
                raise RuntimeError(
                    f"OIDC binding for {(config.issuer_url, identity.subject)!r} resolved "
                    f"to non-user principal {existing_user_id!r}"
                )
            return ProvisionResult(
                user=user,
                newly_provisioned=False,
                matched_group_bindings=_match_group_bindings(identity),
            )

        # Zero-binding new user creation.
        display_name = self._display_name_for(identity)
        email = self._email_for(identity)
        new_user = User(
            kind="user",
            principal_id=PrincipalId(_generate_user_id()),
            tenant_id=TenantId(self._default_tenant_id),
            display_name=display_name,
            email=email,
            disabled_at=None,
            disabled_reason=None,
            created_at=datetime.now(UTC),
        )
        await self._auth_store.put_principal(new_user)
        try:
            await link_oidc_identity(
                self._auth_store,
                self._metadata_store,
                user_id=str(new_user.principal_id),
                issuer=config.issuer_url,
                subject=identity.subject,
                actor=self._actor_id,
                audit_workspace_id=PLATFORM_WORKSPACE_ID,
            )
        except OidcIdentityAlreadyBound:
            # Race: a peer created the binding between our get_oidc_identity
            # check above and our put. Fall through — the peer's binding
            # now points to a different user row, and the freshly-minted
            # row we just wrote is orphaned. Reconcile manually via the
            # audit feed (the design's accepted reconciliation pattern for
            # OIDC link races).
            _LOG.warning(
                "OIDC link race: identity (%s, %s) already bound by a peer; "
                "orphan user row %s persisted (reconcile manually)",
                config.issuer_url,
                identity.subject,
                new_user.principal_id,
            )
            raise
        return ProvisionResult(
            user=new_user,
            newly_provisioned=True,
            matched_group_bindings=_match_group_bindings(identity),
        )

    @staticmethod
    def _display_name_for(identity: VerifiedOidcIdentity) -> str:
        """Derive a human-readable display name from the verified claims.

        Falls back to a synthetic ``"<preset> user <subject>"`` when
        the provider does not carry a human-readable claim. The display
        name is purely cosmetic — the stable identifier is always the
        ``(issuer, subject)`` pair.
        """
        for candidate in ("name", "preferred_username", "email", "login"):
            value = identity.claims.get(candidate)
            if isinstance(value, str) and value:
                return value
        preset = identity.issuer_config.preset or "oidc"
        return f"{preset} user {identity.subject}"

    @staticmethod
    def _email_for(identity: VerifiedOidcIdentity) -> str | None:
        """Extract an email when the provider verified it.

        We only persist the email when ``email_verified`` is true (or
        the claim is absent — providers like GitHub Actions do not
        publish ``email_verified`` for workload tokens). Otherwise
        ``None`` to avoid storing an unverified address.
        """
        email = identity.claims.get("email")
        if not isinstance(email, str) or not email:
            return None
        verified = identity.claims.get("email_verified")
        if verified is False:
            return None
        return email


__all__ = [
    "DEFAULT_PROVISION_TENANT_ID",
    "PROVISIONED_USER_ID_PREFIX",
    "OidcProvisioner",
    "ProvisionResult",
]

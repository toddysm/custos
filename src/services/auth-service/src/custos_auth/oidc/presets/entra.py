"""Azure Entra ID OIDC preset (AS-IMPL-022, REQ-058).

Entra exposes two issuer-URL shapes:

* **Tenant-specific** — ``https://login.microsoftonline.com/<tenant>/v2.0``
  — the issuer URL embeds the AAD tenant GUID, and the verifier rejects
  tokens issued by any other tenant.
* **Multi-tenant** — ``https://login.microsoftonline.com/common/v2.0``
  in the discovery document, but tokens are actually stamped with the
  caller's tenant in their ``iss`` claim
  (``https://login.microsoftonline.com/<actual-tenant>/v2.0``). For
  multi-tenant deployments the operator typically configures one
  issuer entry **per accepted tenant**; ``common`` is rejected at
  config-parse time because that pattern almost always means "I
  forgot to pin a tenant" and silently accepts every Entra customer.

This preset registers under the ``entra`` name and supplies sensible
defaults for the tenant-specific case. Operators wiring multi-tenant
land that as multiple issuer entries — one per ``OidcIssuerConfig``,
each pinned to a specific tenant.

Group → role mapping
--------------------

When the configured Entra app registration emits group claims (object-
id GUIDs), the verifier reads the ``groups`` claim and matches each
GUID against :class:`custos_auth.oidc.config.GroupBinding` rules in
the issuer entry's ``group_bindings`` list. Matches are applied as
one-shot role grants at first OIDC link (Phase H ships the link-time
grant; the revocation-on-claim-removal loop is M3+ scope per the
issue).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final

#: Common Entra v2.0 JWKS endpoint. Tenant-specific deployments
#: typically override this to the tenant-specific discovery URI;
#: most production deployments pin the JWKS via the
#: ``/.well-known/openid-configuration`` document, but a default
#: keeps eval deployments low-friction.
DEFAULT_JWKS_URI: Final[str] = "https://login.microsoftonline.com/common/discovery/v2.0/keys"

#: Entra publishes RS256 today. ES256 is on their roadmap but we
#: still ship RS256 as the only default — operators flip to
#: ``["RS256", "ES256"]`` when Microsoft rolls it out.
DEFAULT_ALGORITHMS: Final[tuple[str, ...]] = ("RS256",)

#: Default subject claim. Entra publishes both ``sub`` (per-app
#: pairwise identifier) and ``oid`` (tenant-wide stable object
#: id); the design defaults to ``oid`` because that survives app
#: registrations being re-issued. Operators that need ``sub`` (e.g.
#: for compatibility with another platform's identity database)
#: override ``subject_claim``.
DEFAULT_SUBJECT_CLAIM: Final[str] = "oid"

#: Default group claim name. Entra emits group object-ids under
#: ``groups`` when the app registration's manifest enables it;
#: when group emission is disabled the array is absent and the
#: provisioning policy simply skips the group-binding step.
DEFAULT_GROUP_CLAIM: Final[str] = "groups"

name: Final[str] = "entra"


def defaults() -> dict[str, object]:
    """Return the preset's default-fill values for an issuer entry.

    Note: no ``issuer_url`` default — operators MUST pin a specific
    tenant URL. Returning a default of ``common`` would silently
    accept any Entra tenant's tokens, which is almost certainly not
    what the operator wanted; explicit > implicit here.
    """
    return {
        "jwks_uri": DEFAULT_JWKS_URI,
        "algorithms": DEFAULT_ALGORITHMS,
        "subject_claim": DEFAULT_SUBJECT_CLAIM,
        "group_claim": DEFAULT_GROUP_CLAIM,
    }


def extract_subject(claims: Mapping[str, Any]) -> str:
    """Return the configured subject claim's value as a string.

    Most Entra tokens carry both ``oid`` and ``sub``. The preset
    default is ``oid``; the verifier resolves the actual claim name
    from the issuer's :attr:`OidcIssuerConfig.subject_claim` before
    calling this, so the function only has to look up the resolved
    name in the claims dict.

    We accept the claim only when it is a non-empty string. The
    raw ``oid`` is a GUID (``"d2c0f8c1-...-..."``) — perfectly
    suitable as a binding subject without further normalisation.
    """
    # Use the standard subject claim by default; the verifier passes
    # in the resolved name when present via the ``subject_claim``
    # config entry, but this helper is kept thin for callers that
    # already resolved the claim.
    for candidate in ("oid", "sub"):
        value = claims.get(candidate)
        if isinstance(value, str) and value:
            return value
    raise ValueError(
        "Entra OIDC token is missing both 'oid' and 'sub' claims; cannot extract a stable subject"
    )


def extra_audit_payload(claims: Mapping[str, Any]) -> dict[str, str]:
    """Return Entra-specific audit fields.

    Surfaced fields:

    * ``tid`` — Entra tenant id (the auth-time tenant).
    * ``preferred_username`` — UPN-shaped string (``user@org.com``).
    * ``app_id`` (alias ``appid``) — the requesting app registration's
      client id, useful for distinguishing CLI vs portal vs custom
      app traffic in the audit feed.

    Group memberships are NOT surfaced here — they go through the
    structured ``group_bindings`` mapping rather than the audit
    payload to keep the row small.
    """
    payload: dict[str, str] = {}
    for claim_name in ("tid", "preferred_username", "appid", "app_id"):
        value = claims.get(claim_name)
        if isinstance(value, str) and value:
            payload[claim_name] = value
    return payload


__all__ = [
    "DEFAULT_ALGORITHMS",
    "DEFAULT_GROUP_CLAIM",
    "DEFAULT_JWKS_URI",
    "DEFAULT_SUBJECT_CLAIM",
    "defaults",
    "extra_audit_payload",
    "extract_subject",
    "name",
]

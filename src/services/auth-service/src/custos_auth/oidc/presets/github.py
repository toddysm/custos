"""GitHub OIDC preset (AS-IMPL-021, REQ-057).

GitHub publishes **two** OIDC issuers Custos cares about:

* ``https://token.actions.githubusercontent.com`` — workload tokens
  minted by GitHub Actions runners. The ``sub`` claim encodes the
  triggering repository + ref (e.g.
  ``repo:acme/sandbox:ref:refs/heads/main``). No code flow.
* ``https://github.com/login/oauth`` — human OAuth login via GitHub's
  OAuth Apps. Carries a numeric ``sub`` (the GitHub user id) and
  goes through the OAuth ``authorization_code`` grant against
  ``https://github.com/login/oauth/access_token``.

The single ``github`` preset covers both — operators pick the
right ``issuer_url`` and ``token_endpoint`` per-entry. The preset's
defaults align with the workload-token variant (the v1 P0 case);
operators wiring human login override ``issuer_url`` /
``token_endpoint`` to the OAuth values.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final

#: Default ``aud`` value GitHub Actions runners request when the
#: workflow does not override it. Operators MUST pin a custom
#: audience in production (``permissions.id-token: write`` +
#: ``actions/configure-aws-credentials`` style flows) so the token
#: cannot be replayed against unrelated OIDC verifiers.
DEFAULT_AUDIENCE: Final[str] = "custos"

#: GitHub Actions workload-token issuer.
DEFAULT_ISSUER_URL: Final[str] = "https://token.actions.githubusercontent.com"

#: GitHub Actions JWKS endpoint. Cache-control headers honour the
#: response's ``max-age``; GitHub typically returns a 1h TTL.
DEFAULT_JWKS_URI: Final[str] = "https://token.actions.githubusercontent.com/.well-known/jwks"

name: Final[str] = "github"


def defaults() -> dict[str, object]:
    """Return the preset's default-fill values for an issuer entry.

    Only fields the operator commonly omits are populated. ``client_id``,
    ``client_secret_env``, ``token_endpoint`` are intentionally absent
    — they are required for the human-login OAuth flow but meaningless
    for workload tokens, so the operator MUST set them explicitly when
    wiring human login. The parser surfaces a fail-loud error if a
    workload-only entry is used for the OAuth callback.
    """
    return {
        "issuer_url": DEFAULT_ISSUER_URL,
        "jwks_uri": DEFAULT_JWKS_URI,
        "audiences": (DEFAULT_AUDIENCE,),
        "algorithms": ("RS256",),
        "subject_claim": "sub",
    }


def extract_subject(claims: Mapping[str, Any]) -> str:
    """Extract the OIDC-link subject from a verified token's claims.

    For both flows the ``sub`` claim is the stable identifier:

    * Workload tokens: ``repo:<org>/<repo>:ref:refs/heads/<branch>``
      etc. — the full ``sub`` string is the binding key (we do not
      strip the ref because that would collapse different branches
      onto the same identity).
    * Human login: numeric GitHub user id (``"12345"``) stringified.

    Returns the raw ``sub`` claim — callers that want to surface the
    structured workload subject (``repository`` / ``workflow``) use
    :func:`extra_audit_payload`.
    """
    sub = claims.get("sub")
    if not isinstance(sub, str) or not sub:
        raise ValueError("GitHub OIDC token is missing a non-empty 'sub' claim")
    return sub


def extra_audit_payload(claims: Mapping[str, Any]) -> dict[str, str]:
    """Return preset-specific audit-payload fields.

    For workload tokens we surface ``repository``, ``repository_id``,
    ``workflow``, ``ref`` so the audit feed makes the originating
    pipeline obvious. For human login the same call returns ``{}``
    — none of those claims are present on the human-flow token.
    """
    payload: dict[str, str] = {}
    for claim_name in ("repository", "repository_id", "workflow", "ref", "event_name"):
        value = claims.get(claim_name)
        if isinstance(value, str) and value:
            payload[claim_name] = value
        elif isinstance(value, int):
            payload[claim_name] = str(value)
    return payload


__all__ = [
    "DEFAULT_AUDIENCE",
    "DEFAULT_ISSUER_URL",
    "DEFAULT_JWKS_URI",
    "defaults",
    "extra_audit_payload",
    "extract_subject",
    "name",
]

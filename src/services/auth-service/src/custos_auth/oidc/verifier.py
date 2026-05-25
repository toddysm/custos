"""Generic OIDC ID-token verifier (AS-IMPL-020).

Verifies a JSON Web Token against the issuer's published JWKS and the
configured claim constraints:

* ``iss`` matches the configured ``issuer_url`` exactly.
* ``aud`` (string or array) intersects the configured ``audiences``.
* ``alg`` is in the configured allow-list.
* ``exp`` is in the future, ``nbf`` (when present) is in the past,
  ``iat`` (when present) is not too far in the future. ``leeway``
  absorbs small clock skew.

The verifier returns a ``VerifiedOidcIdentity`` on success and raises
an appropriate verification error on failure. Audit emission for OIDC
authentication outcomes is handled by the caller/auth route rather
than by this module.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

import jwt
from jwt import (
    ExpiredSignatureError,
    ImmatureSignatureError,
    InvalidAlgorithmError,
    InvalidAudienceError,
    InvalidIssuerError,
    InvalidSignatureError,
    InvalidTokenError,
    PyJWTError,
)
from jwt import (
    PyJWK as _PyJWK,
)

if TYPE_CHECKING:
    from custos_auth.oidc.config import OidcIssuerConfig
    from custos_auth.oidc.jwks_cache import JwksCache

_LOG = logging.getLogger(__name__)

#: Default clock-skew leeway (seconds) the verifier accepts on
#: ``exp`` / ``nbf`` / ``iat`` checks. 60s covers the realistic
#: skew between two NTP-synced hosts; anything larger increases the
#: replay window after revocation.
DEFAULT_LEEWAY_SECONDS: Final[int] = 60

#: Closed-set ``authn.failure`` reason codes. Surfaced verbatim in
#: the audit row so dashboards can group by failure mode. Keep this
#: set narrow — adding a new code is an audit-schema change.
REASON_MALFORMED: Final[str] = "malformed"
REASON_UNKNOWN_KID: Final[str] = "unknown_kid"
REASON_BAD_SIGNATURE: Final[str] = "bad_signature"
REASON_EXPIRED: Final[str] = "expired"
REASON_IMMATURE: Final[str] = "immature"
REASON_WRONG_AUDIENCE: Final[str] = "wrong_audience"
REASON_WRONG_ISSUER: Final[str] = "wrong_issuer"
REASON_WRONG_ALGORITHM: Final[str] = "wrong_algorithm"
REASON_MISSING_CLAIM: Final[str] = "missing_claim"
REASON_JWKS_FETCH_FAILED: Final[str] = "jwks_fetch_failed"

#: Set of all valid reason codes, exposed so other modules
#: (audit emission, tests) can validate against the closed set.
FAILURE_REASONS: Final[frozenset[str]] = frozenset(
    {
        REASON_MALFORMED,
        REASON_UNKNOWN_KID,
        REASON_BAD_SIGNATURE,
        REASON_EXPIRED,
        REASON_IMMATURE,
        REASON_WRONG_AUDIENCE,
        REASON_WRONG_ISSUER,
        REASON_WRONG_ALGORITHM,
        REASON_MISSING_CLAIM,
        REASON_JWKS_FETCH_FAILED,
    }
)


class OidcVerificationError(RuntimeError):
    """Raised when an OIDC token fails verification.

    Always carries a :attr:`reason` from :data:`FAILURE_REASONS` so
    callers (the callback handler, audit emitter, RPC layer) can
    branch on a closed-set code rather than parsing the message.
    """

    def __init__(self, reason: str, detail: str) -> None:
        if reason not in FAILURE_REASONS:  # pragma: no cover — defensive guard
            raise ValueError(f"unknown OIDC verification reason {reason!r}")
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True, slots=True)
class VerifiedOidcIdentity:
    """Result of a successful OIDC verification.

    Carries the issuer-config that handled the verify (so callers
    can look up preset-specific claim extractors), the canonical
    ``subject`` value (already resolved against the issuer's
    ``subject_claim``), and the raw verified claim set for
    downstream consumers (provisioning, group binding, audit).
    """

    issuer_config: OidcIssuerConfig
    subject: str
    claims: Mapping[str, Any]


class OidcVerifier:
    """Verify OIDC ID tokens against the configured issuer set.

    Constructed once per app from the lifespan, takes the parsed
    issuer config + a :class:`JwksCache`. Per-token verifies are
    coroutine-safe and do not mutate verifier state — the cache
    handles concurrent JWKS refreshes internally.
    """

    def __init__(
        self,
        issuers: tuple[OidcIssuerConfig, ...],
        jwks_cache: JwksCache,
        *,
        leeway_seconds: int = DEFAULT_LEEWAY_SECONDS,
    ) -> None:
        self._issuers_by_id: dict[str, OidcIssuerConfig] = {entry.id: entry for entry in issuers}
        self._jwks_cache = jwks_cache
        self._leeway = leeway_seconds

    def issuer_for(self, issuer_id: str) -> OidcIssuerConfig | None:
        """Look up the issuer-config entry by id."""
        return self._issuers_by_id.get(issuer_id)

    async def verify(self, issuer_id: str, token: str) -> VerifiedOidcIdentity:
        """Verify ``token`` against the configured issuer.

        Raises :class:`OidcVerificationError` on every failure mode;
        the ``reason`` attribute carries the closed-set failure code
        from :data:`FAILURE_REASONS`.
        """
        entry = self._issuers_by_id.get(issuer_id)
        if entry is None:
            raise OidcVerificationError(
                REASON_MISSING_CLAIM,
                f"unknown OIDC issuer id {issuer_id!r}",
            )

        # Step 1 — parse the unverified header so we can look up the JWK
        # by kid. PyJWT's get_unverified_header validates the JWT shape
        # but does not check the signature — that happens in step 3.
        try:
            header = jwt.get_unverified_header(token)
        except PyJWTError as exc:
            raise OidcVerificationError(REASON_MALFORMED, f"invalid JWT header: {exc}") from exc
        kid = header.get("kid")
        if not isinstance(kid, str) or not kid:
            raise OidcVerificationError(REASON_MALFORMED, "JWT header missing 'kid'")
        alg = header.get("alg")
        if not isinstance(alg, str) or alg not in entry.algorithms:
            raise OidcVerificationError(
                REASON_WRONG_ALGORITHM,
                f"JWT alg {alg!r} not in configured allow-list {list(entry.algorithms)!r}",
            )

        # Step 2 — resolve the signing key from the JWKS cache.
        try:
            jwk = await self._jwks_cache.get_key(entry.jwks_uri, kid)
        except Exception as exc:
            # Prefer structured cache error metadata over exception
            # message parsing when deciding which closed-set audit
            # reason to emit. Unknown / unstructured cache failures
            # still map conservatively to jwks_fetch_failed.
            from custos_auth.oidc.jwks_cache import JwksCacheError

            if isinstance(exc, JwksCacheError):
                cache_error_code = (
                    getattr(exc, "reason", None)
                    or getattr(exc, "code", None)
                    or getattr(exc, "error_code", None)
                )
                if isinstance(cache_error_code, str) and cache_error_code.lower() in {
                    "unknown_kid",
                    "kid_missing",
                    "missing_kid",
                }:
                    raise OidcVerificationError(REASON_UNKNOWN_KID, str(exc)) from exc
                raise OidcVerificationError(REASON_JWKS_FETCH_FAILED, str(exc)) from exc
            raise OidcVerificationError(REASON_JWKS_FETCH_FAILED, str(exc)) from exc

        # Step 3 — verify signature + standard claims via PyJWT. We
        # let PyJWT do iss / aud / exp / nbf / iat / alg checks so
        # there is exactly one implementation of the OIDC claim
        # contract; the closed-set reason mapping below distills
        # PyJWT's exception hierarchy into the audit codes.
        try:
            pyjwk = _PyJWK(jwk)
            claims = jwt.decode(
                token,
                key=pyjwk.key,
                algorithms=list(entry.algorithms),
                audience=list(entry.audiences),
                issuer=entry.issuer_url,
                leeway=self._leeway,
                options={"require": ["exp", "iss", "aud"]},
            )
        except ExpiredSignatureError as exc:
            raise OidcVerificationError(REASON_EXPIRED, str(exc)) from exc
        except ImmatureSignatureError as exc:
            raise OidcVerificationError(REASON_IMMATURE, str(exc)) from exc
        except InvalidAudienceError as exc:
            raise OidcVerificationError(REASON_WRONG_AUDIENCE, str(exc)) from exc
        except InvalidIssuerError as exc:
            raise OidcVerificationError(REASON_WRONG_ISSUER, str(exc)) from exc
        except InvalidAlgorithmError as exc:
            raise OidcVerificationError(REASON_WRONG_ALGORITHM, str(exc)) from exc
        except InvalidSignatureError as exc:
            raise OidcVerificationError(REASON_BAD_SIGNATURE, str(exc)) from exc
        except InvalidTokenError as exc:
            # PyJWT's base-class — covers "missing required claim",
            # malformed claim shapes, etc.
            detail = str(exc)
            if "missing" in detail.lower() or "required" in detail.lower():
                raise OidcVerificationError(REASON_MISSING_CLAIM, detail) from exc
            raise OidcVerificationError(REASON_MALFORMED, detail) from exc

        # Step 4 — resolve the issuer-specific subject. The default
        # ``subject_claim`` is ``sub``, but presets like Entra use
        # ``oid`` so the actual stable identity survives app re-
        # registration.
        subject_value = claims.get(entry.subject_claim)
        if not isinstance(subject_value, str) or not subject_value:
            raise OidcVerificationError(
                REASON_MISSING_CLAIM,
                f"verified token missing required subject claim {entry.subject_claim!r}",
            )

        _LOG.debug(
            "oidc verify ok issuer=%s subject=%s iat=%s exp=%s",
            entry.issuer_url,
            subject_value,
            claims.get("iat"),
            claims.get("exp"),
        )
        return VerifiedOidcIdentity(
            issuer_config=entry,
            subject=subject_value,
            claims=claims,
        )


def now_seconds() -> int:
    """Return the current epoch second-count.

    Wrapped so tests can monkey-patch ``time.time`` in one spot;
    real code paths call ``time.time`` directly via this indirection.
    """
    return int(time.time())


__all__ = [
    "DEFAULT_LEEWAY_SECONDS",
    "FAILURE_REASONS",
    "REASON_BAD_SIGNATURE",
    "REASON_EXPIRED",
    "REASON_IMMATURE",
    "REASON_JWKS_FETCH_FAILED",
    "REASON_MALFORMED",
    "REASON_MISSING_CLAIM",
    "REASON_UNKNOWN_KID",
    "REASON_WRONG_ALGORITHM",
    "REASON_WRONG_AUDIENCE",
    "REASON_WRONG_ISSUER",
    "OidcVerificationError",
    "OidcVerifier",
    "VerifiedOidcIdentity",
    "now_seconds",
]

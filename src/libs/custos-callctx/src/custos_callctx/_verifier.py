"""High-level call-context verifier (AS-IMPL-019).

Bundles :class:`JwksCache` with the JWT decode + claim validation so
consuming components do one ``await verifier.verify(metadata=...)`` call
per request.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final

import jwt
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from custos_callctx._context import CallContext
from custos_callctx._errors import InvalidCallContextError, InvalidReason
from custos_callctx._jwks import JwksCache

#: Canonical wire header carrying the call-context JWT. Lowercase form
#: matches the design note in the API Gateway component
#: (``design/components/api-gateway/design.md``).
CALLCTX_HEADER: Final[str] = "x-custos-callctx"

#: Default ``aud`` claim expected on every minted call-context token.
#: Matches :data:`custos_auth.callctx_signer.DEFAULT_AUDIENCE`.
DEFAULT_AUDIENCE: Final[str] = "custos.internal"

#: Default ``iss`` claim expected on every minted call-context token.
#: Matches :data:`custos_auth.callctx_signer.ISSUER`.
DEFAULT_ISSUER: Final[str] = "custos-auth"

#: Only EdDSA over Ed25519 is accepted. Receivers must reject any other
#: algorithm — in particular ``"none"`` and any HMAC variant — to keep
#: the trust boundary on the JWKS public key alone.
_ALLOWED_ALGORITHMS: Final[tuple[str, ...]] = ("EdDSA",)


def _extract_header_value(metadata: Mapping[str, str], header: str) -> str | None:
    """Case-insensitive lookup of ``header`` in ``metadata``."""
    target = header.lower()
    for key, value in metadata.items():
        if key.lower() == target:
            return value
    return None


class CallContextVerifier:
    """Verifies inbound call-context JWTs against the auth-service JWKS.

    Args:
        jwks_url: Auth-service JWKS endpoint (typically
            ``http://auth-service.custos.svc/.well-known/jwks.json``).
        audience: Expected ``aud`` claim. Defaults to
            :data:`DEFAULT_AUDIENCE`. Verifiers and the auth-service
            signer must agree on this value.
        issuer: Expected ``iss`` claim. Defaults to
            :data:`DEFAULT_ISSUER`.
        leeway_seconds: Allowed clock skew when validating
            ``iat`` / ``exp``. Defaults to ``0`` (the JWKS-cache
            TTL plus the auth-service overlap window already absorb
            in-flight drift; a non-zero leeway can mask clock-skew
            misconfigurations).
        header: Wire header carrying the token. Defaults to
            :data:`CALLCTX_HEADER`. Override only when integrating with
            a non-default gateway.
        jwks_cache: Pre-built :class:`JwksCache`. Optional; when
            ``None`` the verifier builds one with the production
            HTTP fetcher.

    The verifier is cheap to construct and safe to share between
    request handlers; every :meth:`verify` call is independent.
    """

    def __init__(
        self,
        *,
        jwks_url: str | None = None,
        audience: str = DEFAULT_AUDIENCE,
        issuer: str = DEFAULT_ISSUER,
        leeway_seconds: int = 0,
        header: str = CALLCTX_HEADER,
        jwks_cache: JwksCache | None = None,
    ) -> None:
        if jwks_cache is None:
            if not jwks_url:
                raise ValueError("CallContextVerifier requires either jwks_url or jwks_cache")
            jwks_cache = JwksCache(jwks_url=jwks_url)
        if leeway_seconds < 0:
            raise ValueError(f"leeway_seconds must be non-negative; got {leeway_seconds!r}")
        self._jwks = jwks_cache
        self._audience = audience
        self._issuer = issuer
        self._leeway_seconds = leeway_seconds
        self._header = header

    @property
    def jwks(self) -> JwksCache:
        return self._jwks

    async def verify(self, *, metadata: Mapping[str, str]) -> CallContext:
        """Validate the call-context JWT and return its decoded claims.

        Args:
            metadata: Per-request header map (case-insensitive). Either
                raw ASGI headers or a gRPC metadata dict are accepted.

        Raises:
            InvalidCallContextError: with a populated
                :class:`InvalidReason` describing why the token failed
                verification. Callers map the reason onto their audit
                outbox (``call-context.invalid`` event) before
                rendering a 401 to the upstream caller.
        """
        raw = _extract_header_value(metadata, self._header)
        if raw is None or raw == "":
            raise InvalidCallContextError(
                InvalidReason.MISSING_HEADER,
                f"{self._header} header is required",
            )
        token = raw.strip()
        try:
            header = jwt.get_unverified_header(token)
        except jwt.InvalidTokenError as exc:
            raise InvalidCallContextError(
                InvalidReason.MALFORMED_TOKEN,
                f"call-context JWT header is unreadable: {exc}",
            ) from exc
        alg = header.get("alg")
        if alg not in _ALLOWED_ALGORITHMS:
            raise InvalidCallContextError(
                InvalidReason.BAD_ALGORITHM,
                f"call-context JWT must use EdDSA; got alg={alg!r}",
            )
        kid = header.get("kid")
        if not isinstance(kid, str) or not kid:
            raise InvalidCallContextError(
                InvalidReason.MISSING_KID,
                "call-context JWT header is missing 'kid'",
            )
        try:
            public_key = await self._jwks.get_key(kid)
        except InvalidCallContextError:
            raise
        claims = self._decode_claims(token, public_key=public_key, kid=kid)
        return self._build_context(claims, kid=kid)

    def _decode_claims(
        self,
        token: str,
        *,
        public_key: Ed25519PublicKey,
        kid: str,
    ) -> dict[str, Any]:
        try:
            claims = jwt.decode(
                token,
                key=public_key,
                algorithms=list(_ALLOWED_ALGORITHMS),
                audience=self._audience,
                issuer=self._issuer,
                leeway=self._leeway_seconds,
                options={"require": ["exp", "iat", "iss", "aud"]},
            )
        except jwt.ExpiredSignatureError as exc:
            raise InvalidCallContextError(
                InvalidReason.EXPIRED,
                "call-context JWT has expired",
                kid=kid,
            ) from exc
        except jwt.ImmatureSignatureError as exc:
            raise InvalidCallContextError(
                InvalidReason.NOT_YET_VALID,
                "call-context JWT is not yet valid",
                kid=kid,
            ) from exc
        except jwt.InvalidAudienceError as exc:
            raise InvalidCallContextError(
                InvalidReason.WRONG_AUDIENCE,
                f"call-context JWT carries the wrong 'aud' claim: {exc}",
                kid=kid,
            ) from exc
        except jwt.InvalidIssuerError as exc:
            raise InvalidCallContextError(
                InvalidReason.WRONG_ISSUER,
                f"call-context JWT carries the wrong 'iss' claim: {exc}",
                kid=kid,
            ) from exc
        except jwt.MissingRequiredClaimError as exc:
            raise InvalidCallContextError(
                InvalidReason.MISSING_CLAIM,
                f"call-context JWT is missing required claim: {exc.claim!r}",
                kid=kid,
            ) from exc
        except jwt.InvalidSignatureError as exc:
            raise InvalidCallContextError(
                InvalidReason.BAD_SIGNATURE,
                "call-context JWT signature failed verification",
                kid=kid,
            ) from exc
        except jwt.InvalidTokenError as exc:
            raise InvalidCallContextError(
                InvalidReason.MALFORMED_TOKEN,
                f"call-context JWT is invalid: {exc}",
                kid=kid,
            ) from exc
        if not isinstance(claims, dict):  # pragma: no cover — pyjwt always returns dict
            raise InvalidCallContextError(
                InvalidReason.MALFORMED_TOKEN,
                "decoded call-context claims are not a JSON object",
                kid=kid,
            )
        return claims

    def _build_context(self, claims: dict[str, Any], *, kid: str) -> CallContext:
        try:
            principal = claims["actingPrincipalId"]
            caller = claims["callerComponent"]
            jti = claims["jti"]
            iat = int(claims["iat"])
            exp = int(claims["exp"])
            iss = claims["iss"]
            aud = claims["aud"]
        except KeyError as exc:
            raise InvalidCallContextError(
                InvalidReason.MISSING_CLAIM,
                f"call-context JWT is missing required claim: {exc.args[0]!r}",
                kid=kid,
            ) from exc
        except (TypeError, ValueError) as exc:
            raise InvalidCallContextError(
                InvalidReason.MALFORMED_TOKEN,
                f"call-context JWT has a malformed claim value: {exc}",
                kid=kid,
            ) from exc
        workspace = claims.get("workspaceId")
        if workspace is not None and not isinstance(workspace, str):
            raise InvalidCallContextError(
                InvalidReason.MALFORMED_TOKEN,
                f"workspaceId must be a string or null; got {type(workspace).__name__}",
                kid=kid,
            )
        if not isinstance(principal, str) or not principal:
            raise InvalidCallContextError(
                InvalidReason.MISSING_CLAIM,
                "actingPrincipalId must be a non-empty string",
                kid=kid,
            )
        if not isinstance(caller, str) or not caller:
            raise InvalidCallContextError(
                InvalidReason.MISSING_CLAIM,
                "callerComponent must be a non-empty string",
                kid=kid,
            )
        if not isinstance(jti, str) or not jti:
            raise InvalidCallContextError(
                InvalidReason.MISSING_CLAIM,
                "jti must be a non-empty string",
                kid=kid,
            )
        # ``aud`` from the JWT envelope can be a string or list; pyjwt
        # has already enforced that the configured audience matches at
        # least one entry. Normalise to a string for the public
        # CallContext shape.
        if isinstance(aud, list):
            aud = self._audience
        permissions = self._extract_permissions(claims, kid=kid)
        return CallContext(
            acting_principal_id=principal,
            workspace_id=workspace,
            caller_component=caller,
            jti=jti,
            issued_at=iat,
            expires_at=exp,
            issuer=str(iss),
            audience=str(aud),
            kid=kid,
            permissions=permissions,
        )

    @staticmethod
    def _extract_permissions(
        claims: dict[str, Any],
        *,
        kid: str,
    ) -> frozenset[str]:
        """Parse the optional ``permissions`` claim.

        The claim is an enabler for Option D of AS-IMPL-030 (fat call
        context): the API Gateway embeds the principal's RBAC grant
        into the signed token so downstream services do not need a
        per-request Auth Service round-trip to enforce
        ``require_permission``. Tokens minted without the claim
        (e.g. internal callctx for platform-global RPCs) yield an
        empty :class:`frozenset`.

        Validation rules:

        * Missing claim → empty set (back-compat with AS-IMPL-017 tokens).
        * ``null`` → empty set (signer is allowed to emit ``None`` to
          signal "no embedded grant").
        * List of strings → de-duplicated frozenset.
        * Anything else (non-list, non-null) is rejected as
          :data:`InvalidReason.MALFORMED_TOKEN` so a downstream that
          relies on ``has_permission`` cannot be fooled by an
          attacker-controlled claim type.
        """
        raw = claims.get("permissions")
        if raw is None:
            return frozenset()
        if not isinstance(raw, list):
            raise InvalidCallContextError(
                InvalidReason.MALFORMED_TOKEN,
                "permissions claim must be a JSON array of strings",
                kid=kid,
            )
        out: set[str] = set()
        for item in raw:
            if not isinstance(item, str) or not item:
                raise InvalidCallContextError(
                    InvalidReason.MALFORMED_TOKEN,
                    "permissions claim entries must be non-empty strings",
                    kid=kid,
                )
            out.add(item)
        return frozenset(out)


__all__ = [
    "CALLCTX_HEADER",
    "DEFAULT_AUDIENCE",
    "DEFAULT_ISSUER",
    "CallContextVerifier",
]

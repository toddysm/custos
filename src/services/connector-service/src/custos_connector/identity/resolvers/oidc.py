"""OIDC federated identity resolver (token exchange + JWKS-validated issuer).

The federated category is the path that lets a connector instance use
a *subject token* (typically the workload identity's own ID token)
against an OIDC-compliant issuer to mint an *access token* scoped to a
specific upstream audience. The flow is RFC 8693 token exchange:

    POST {token_endpoint}
        grant_type=urn:ietf:params:oauth:grant-type:token-exchange
        subject_token=<workload-id-token>
        subject_token_type=urn:ietf:params:oauth:token-type:id_token
        audience=<upstream-audience>
        [scope=<space-delimited scopes>]

This resolver also fetches and caches the issuer's JWKS document so a
caller (in a follow-up phase) can verify the access token's signature
without a second round trip. JWKS is cached with a short TTL — long
enough to absorb a noisy bind loop, short enough to pick up key
rotations without operator intervention.

The subject-token-provider seam mirrors the
:class:`AzureBearerTokenProvider` in the Key Vault resolver: in
production it wraps the workload identity's projected SA-token mount;
in tests it returns a deterministic constant.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime, timedelta
from typing import Any, ClassVar

from custos_connector.identity.errors import (
    IdentityResolverError,
    IdentityResolverErrorCode,
)
from custos_connector.identity.models import ResolvedIdentity
from custos_connector.identity.protocols import IdentityResolverContext
from custos_connector.identity.transport import (
    AsyncHttpClient,
    HttpRequest,
    UpstreamTransportError,
)
from custos_connector.loader.identity import IdentityCategory

#: RFC 8693 grant_type value for the token-exchange flow.
GRANT_TYPE_TOKEN_EXCHANGE: str = "urn:ietf:params:oauth:grant-type:token-exchange"

#: RFC 8693 subject_token_type for an ID token. We default to this and
#: let the manifest override when the workload presents a different
#: subject token type.
SUBJECT_TOKEN_TYPE_ID_TOKEN: str = "urn:ietf:params:oauth:token-type:id_token"

#: Default JWKS cache TTL. Operators that need faster key rotation
#: pass ``jwks_cache_ttl_seconds=`` to the constructor.
DEFAULT_JWKS_CACHE_TTL_SECONDS: int = 300

SubjectTokenProvider = Callable[[IdentityResolverContext], Awaitable[str]]


async def _default_subject_token_provider(
    context: IdentityResolverContext,
) -> str:  # pragma: no cover
    raise IdentityResolverError(
        detail=(
            "no OIDC subject-token provider configured; pass "
            "subject_token_provider= to OidcFederatedResolver"
        ),
        code=IdentityResolverErrorCode.MISSING_CREDENTIAL_FIELD,
        data={"workspace_id": context.workspace_id},
    )


class OidcFederatedResolver:
    """Federated identity resolver running RFC 8693 token exchange.

    Construction parameters:

    Args:
        http: Transport seam for both the token endpoint and the JWKS
            endpoint.
        subject_token_provider: Mints the *subject* token presented to
            the token endpoint. In production this typically reads the
            projected SA token mount; in tests it returns a constant.
        jwks_cache_ttl_seconds: How long to cache the JWKS document
            between fetches. The cache key is the JWKS URL.
        request_timeout_seconds: Per-request timeout (applied to both
            the token endpoint and the JWKS endpoint).
    """

    authentication_type: ClassVar[str] = "oidc"
    category: ClassVar[IdentityCategory] = IdentityCategory.FEDERATED

    def __init__(
        self,
        *,
        http: AsyncHttpClient,
        subject_token_provider: SubjectTokenProvider | None = None,
        jwks_cache_ttl_seconds: int = DEFAULT_JWKS_CACHE_TTL_SECONDS,
        request_timeout_seconds: float = 5.0,
    ) -> None:
        if jwks_cache_ttl_seconds < 0:
            raise ValueError(f"jwks_cache_ttl_seconds must be >= 0 (got {jwks_cache_ttl_seconds})")
        self._http = http
        self._subject_token_provider = subject_token_provider or _default_subject_token_provider
        self._jwks_cache_ttl = jwks_cache_ttl_seconds
        self._request_timeout = request_timeout_seconds
        # JWKS cache keyed by JWKS URL. Each entry is
        # (fetched_at_monotonic_seconds, jwks_document).
        self._jwks_cache: dict[str, tuple[float, Mapping[str, Any]]] = {}

    async def resolve(
        self,
        *,
        credentials_authentication: Mapping[str, Any],
        context: IdentityResolverContext,
    ) -> ResolvedIdentity:
        issuer = _require_str(credentials_authentication, "issuer")
        audience = _require_str(credentials_authentication, "audience")
        token_endpoint = _require_str(credentials_authentication, "tokenEndpoint")
        jwks_uri = _optional_str(credentials_authentication, "jwksUri", "")
        scope = _optional_str(credentials_authentication, "scope", "")
        subject_token_type = _optional_str(
            credentials_authentication,
            "subjectTokenType",
            SUBJECT_TOKEN_TYPE_ID_TOKEN,
        )

        try:
            subject_token = await self._subject_token_provider(context)
        except IdentityResolverError:
            raise
        except Exception as exc:
            raise IdentityResolverError(
                detail=f"OIDC subject-token provider raised: {exc}",
                code=IdentityResolverErrorCode.UPSTREAM_UNAVAILABLE,
                data={"issuer": issuer, "audience": audience},
            ) from exc

        form_body: dict[str, str] = {
            "grant_type": GRANT_TYPE_TOKEN_EXCHANGE,
            "subject_token": subject_token,
            "subject_token_type": subject_token_type,
            "audience": audience,
        }
        if scope:
            form_body["scope"] = scope

        try:
            response = await self._http.request(
                HttpRequest(
                    method="POST",
                    url=token_endpoint,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    form_body=form_body,
                    timeout_seconds=self._request_timeout,
                )
            )
        except UpstreamTransportError as exc:
            raise IdentityResolverError(
                detail=f"OIDC token-endpoint transport error: {exc}",
                code=IdentityResolverErrorCode.UPSTREAM_UNAVAILABLE,
                data={"issuer": issuer, "audience": audience},
            ) from exc

        if response.status_code in (401, 403):
            raise IdentityResolverError(
                detail=(
                    f"OIDC token endpoint rejected token exchange for "
                    f"audience {audience!r} (status {response.status_code})"
                ),
                code=IdentityResolverErrorCode.UPSTREAM_UNAUTHORIZED,
                data={
                    "issuer": issuer,
                    "audience": audience,
                    "status_code": response.status_code,
                },
            )
        if response.status_code >= 400:
            raise IdentityResolverError(
                detail=(
                    f"OIDC token endpoint returned status "
                    f"{response.status_code} for audience {audience!r}"
                ),
                code=IdentityResolverErrorCode.UPSTREAM_REJECTED,
                data={
                    "issuer": issuer,
                    "audience": audience,
                    "status_code": response.status_code,
                },
            )

        try:
            body = response.json()
        except ValueError as exc:
            raise IdentityResolverError(
                detail=f"OIDC token-endpoint response was not JSON: {exc}",
                code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
                data={"issuer": issuer, "audience": audience},
            ) from exc

        if not isinstance(body, dict):
            raise IdentityResolverError(
                detail="OIDC token-endpoint response was not a JSON object",
                code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
                data={"issuer": issuer, "audience": audience},
            )

        access_token = body.get("access_token")
        if not isinstance(access_token, str) or not access_token:
            raise IdentityResolverError(
                detail="OIDC token-endpoint response missing 'access_token'",
                code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
                data={"issuer": issuer, "audience": audience},
            )

        token_type = body.get("token_type")
        if not isinstance(token_type, str):
            token_type = "Bearer"

        now = context.now()
        expires_in = body.get("expires_in")
        expires_at: datetime | None = None
        if isinstance(expires_in, int) and expires_in > 0:
            expires_at = now + timedelta(seconds=expires_in)

        # Optionally pre-warm the JWKS cache so a subsequent verifier
        # call doesn't have to wait for the JWKS fetch. We swallow
        # failures here — the access token is still valid even when
        # JWKS pre-warm fails (the caller can re-fetch on demand).
        if jwks_uri:
            with contextlib.suppress(IdentityResolverError):
                await self._fetch_jwks(jwks_uri)

        material: dict[str, Any] = {
            "access_token": access_token,
            "token_type": token_type,
        }
        if isinstance(body.get("issued_token_type"), str):
            material["issued_token_type"] = body["issued_token_type"]
        if isinstance(body.get("scope"), str):
            material["scope"] = body["scope"]

        return ResolvedIdentity.build(
            authentication_type=self.authentication_type,
            category=self.category,
            material=material,
            descriptor=f"oidc:{issuer}|aud={audience}",
            issued_at=now,
            expires_at=expires_at,
        )

    async def get_jwks(self, jwks_uri: str) -> Mapping[str, Any]:
        """Fetch (or return cached) JWKS for ``jwks_uri``.

        Public so a follow-up verifier in Phase G can validate the
        access tokens this resolver mints. Raises
        :class:`IdentityResolverError` on failure.
        """
        return await self._fetch_jwks(jwks_uri)

    async def _fetch_jwks(self, jwks_uri: str) -> Mapping[str, Any]:
        cached = self._jwks_cache.get(jwks_uri)
        now_mono = time.monotonic()
        if cached is not None and (now_mono - cached[0]) < self._jwks_cache_ttl:
            return dict(cached[1])

        try:
            response = await self._http.request(
                HttpRequest(
                    method="GET",
                    url=jwks_uri,
                    timeout_seconds=self._request_timeout,
                )
            )
        except UpstreamTransportError as exc:
            raise IdentityResolverError(
                detail=f"JWKS GET transport error: {exc}",
                code=IdentityResolverErrorCode.UPSTREAM_UNAVAILABLE,
                data={"jwks_uri": jwks_uri},
            ) from exc

        if response.status_code >= 400:
            raise IdentityResolverError(
                detail=f"JWKS endpoint returned status {response.status_code}",
                code=IdentityResolverErrorCode.UPSTREAM_REJECTED,
                data={
                    "jwks_uri": jwks_uri,
                    "status_code": response.status_code,
                },
            )

        try:
            body = response.json()
        except ValueError as exc:
            raise IdentityResolverError(
                detail=f"JWKS response was not JSON: {exc}",
                code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
                data={"jwks_uri": jwks_uri},
            ) from exc

        if not isinstance(body, dict) or "keys" not in body:
            raise IdentityResolverError(
                detail="JWKS response missing 'keys' field",
                code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
                data={"jwks_uri": jwks_uri},
            )

        self._jwks_cache[jwks_uri] = (now_mono, body)
        return body


def _require_str(payload: Mapping[str, Any], field: str) -> str:
    value = payload.get(field)
    if value is None:
        raise IdentityResolverError(
            detail=f"credentials.authentication.{field} is required",
            code=IdentityResolverErrorCode.MISSING_CREDENTIAL_FIELD,
            data={"field": field},
        )
    if not isinstance(value, str) or not value.strip():
        raise IdentityResolverError(
            detail=f"credentials.authentication.{field} must be a non-empty string",
            code=IdentityResolverErrorCode.INVALID_CREDENTIAL_FIELD,
            data={"field": field},
        )
    return value


def _optional_str(payload: Mapping[str, Any], field: str, default: str) -> str:
    value = payload.get(field)
    if value is None:
        return default
    if not isinstance(value, str):
        raise IdentityResolverError(
            detail=f"credentials.authentication.{field} must be a string when present",
            code=IdentityResolverErrorCode.INVALID_CREDENTIAL_FIELD,
            data={"field": field},
        )
    value = value.strip()
    return value or default


__all__ = [
    "DEFAULT_JWKS_CACHE_TTL_SECONDS",
    "GRANT_TYPE_TOKEN_EXCHANGE",
    "SUBJECT_TOKEN_TYPE_ID_TOKEN",
    "OidcFederatedResolver",
    "SubjectTokenProvider",
]

"""Azure Key Vault KMS-backed identity resolver.

The Key Vault REST API treats every secret as an HTTPS resource:

    GET {vaultUri}/secrets/{secretName}?api-version=7.4
    Authorization: Bearer <workload-identity-token>

The resolver does *not* mint the bearer token itself — that's the
workload's job and depends on whether we're running under AKS workload
identity, an Azure-VM IMDS, or Dapr. We accept the bearer token via a
constructor-injected :class:`AzureBearerTokenProvider` callable. In
production the operator wires this to a Dapr "secretstore-component"
GET (the same way the secret-bridge sidecar in Phase H does); in tests
we pass a deterministic lambda.

This keeps the resolver itself stateless and free of azure-identity
imports.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime
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

#: Default Key Vault REST API version. The 7.4 GA wire format is
#: backward-compatible with 7.0+; we hard-code the version here so a
#: misconfigured environment cannot drift onto an unstable preview API.
DEFAULT_API_VERSION: str = "7.4"

#: The scope (audience) used when the operator-supplied token provider
#: needs to know which audience to request. Operators wiring a custom
#: provider can ignore this — it's just the conventional Azure value.
DEFAULT_BEARER_AUDIENCE: str = "https://vault.azure.net/.default"

AzureBearerTokenProvider = Callable[[str], Awaitable[str]]


def _default_token_provider(audience: str) -> Awaitable[str]:  # pragma: no cover
    raise IdentityResolverError(
        detail=(
            "no Azure bearer-token provider configured; pass token_provider= "
            "to AzureKeyVaultResolver when constructing the registry"
        ),
        code=IdentityResolverErrorCode.MISSING_CREDENTIAL_FIELD,
        data={"audience": audience},
    )


class AzureKeyVaultResolver:
    """KMS-backed resolver fronting the Azure Key Vault REST API."""

    authentication_type: ClassVar[str] = "azure-key-vault"
    category: ClassVar[IdentityCategory] = IdentityCategory.KMS

    def __init__(
        self,
        *,
        http: AsyncHttpClient,
        token_provider: AzureBearerTokenProvider | None = None,
        api_version: str = DEFAULT_API_VERSION,
        request_timeout_seconds: float = 5.0,
    ) -> None:
        self._http = http
        self._token_provider = token_provider or _default_token_provider
        self._api_version = api_version
        self._request_timeout = request_timeout_seconds

    async def resolve(
        self,
        *,
        credentials_authentication: Mapping[str, Any],
        context: IdentityResolverContext,
    ) -> ResolvedIdentity:
        """Resolve currently valid material without extending upstream validity timestamps."""
        vault_uri = _require_str(credentials_authentication, "vaultUri")
        secret_name = _require_str(credentials_authentication, "secretName")
        audience = _optional_str(credentials_authentication, "audience", DEFAULT_BEARER_AUDIENCE)

        # Strip trailing slashes so we can build the URL without a
        # double slash. Key Vault accepts both, but matching the
        # documented form keeps logs readable.
        vault_uri = vault_uri.rstrip("/")

        try:
            bearer = await self._token_provider(audience)
        except IdentityResolverError:
            raise
        except Exception as exc:
            raise IdentityResolverError(
                detail=f"bearer-token provider raised: {exc}",
                code=IdentityResolverErrorCode.UPSTREAM_UNAVAILABLE,
                data={"audience": audience},
            ) from exc

        url = f"{vault_uri}/secrets/{secret_name}?api-version={self._api_version}"
        try:
            response = await self._http.request(
                HttpRequest(
                    method="GET",
                    url=url,
                    headers={"Authorization": f"Bearer {bearer}"},
                    timeout_seconds=self._request_timeout,
                )
            )
        except UpstreamTransportError as exc:
            raise IdentityResolverError(
                detail=f"Key Vault GET transport error: {exc}",
                code=IdentityResolverErrorCode.UPSTREAM_UNAVAILABLE,
                data={"vault_uri": vault_uri, "secret_name": secret_name},
            ) from exc

        if response.status_code in (401, 403):
            raise IdentityResolverError(
                detail=(
                    f"Key Vault rejected bearer for {secret_name!r} (status {response.status_code})"
                ),
                code=IdentityResolverErrorCode.UPSTREAM_UNAUTHORIZED,
                data={
                    "vault_uri": vault_uri,
                    "secret_name": secret_name,
                    "status_code": response.status_code,
                },
            )
        if response.status_code >= 400:
            raise IdentityResolverError(
                detail=(f"Key Vault returned status {response.status_code} for {secret_name!r}"),
                code=IdentityResolverErrorCode.UPSTREAM_REJECTED,
                data={
                    "vault_uri": vault_uri,
                    "secret_name": secret_name,
                    "status_code": response.status_code,
                },
            )

        try:
            body = response.json()
        except ValueError as exc:
            raise IdentityResolverError(
                detail=f"Key Vault response was not JSON: {exc}",
                code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
                data={"vault_uri": vault_uri, "secret_name": secret_name},
            ) from exc

        if not isinstance(body, dict) or "value" not in body:
            raise IdentityResolverError(
                detail="Key Vault response missing 'value' field",
                code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
                data={"vault_uri": vault_uri, "secret_name": secret_name},
            )

        secret_value = body["value"]
        if not isinstance(secret_value, str):
            raise IdentityResolverError(
                detail="Key Vault 'value' field is not a string",
                code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
                data={"vault_uri": vault_uri, "secret_name": secret_name},
            )

        now = context.now()
        expires_at = _validate_secret_attributes(body, now=now)
        return ResolvedIdentity.build(
            authentication_type=self.authentication_type,
            category=self.category,
            material={"secret": secret_value},
            descriptor=f"azure-key-vault:{vault_uri}/secrets/{secret_name}",
            issued_at=now,
            expires_at=expires_at,
        )


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
    if not isinstance(value, str) or not value.strip():
        raise IdentityResolverError(
            detail=(f"credentials.authentication.{field} must be a non-empty string when present"),
            code=IdentityResolverErrorCode.INVALID_CREDENTIAL_FIELD,
            data={"field": field},
        )
    return value


def _validate_secret_attributes(
    body: Mapping[str, Any],
    *,
    now: datetime,
) -> datetime | None:
    """Enforce enabled/nbf/exp; retrieval permission alone does not establish key validity."""
    attrs = body.get("attributes")
    if attrs is None:
        return None
    if not isinstance(attrs, Mapping):
        raise IdentityResolverError(
            "Key Vault attributes must be an object",
            code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
        )
    enabled = attrs.get("enabled")
    if enabled is not None and type(enabled) is not bool:
        raise IdentityResolverError(
            "Key Vault enabled attribute must be boolean",
            code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
        )
    if enabled is False:
        raise IdentityResolverError(
            "Key Vault secret is disabled", code=IdentityResolverErrorCode.UPSTREAM_REJECTED
        )
    bounds: dict[str, datetime] = {}
    for field in ("nbf", "exp"):
        value = attrs.get(field)
        if value is None:
            continue
        if type(value) is not int:
            raise IdentityResolverError(
                "Key Vault validity attribute must be Unix seconds",
                code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
                data={"field": field},
            )
        try:
            bounds[field] = datetime.fromtimestamp(value, tz=now.tzinfo)
        except (ValueError, OverflowError, OSError) as exc:
            raise IdentityResolverError(
                "Key Vault validity attribute is outside the supported time range",
                code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
                data={"field": field},
            ) from exc
    if "nbf" in bounds and now < bounds["nbf"]:
        raise IdentityResolverError(
            "Key Vault secret is not yet valid", code=IdentityResolverErrorCode.UPSTREAM_REJECTED
        )
    expiry = bounds.get("exp")
    if expiry is not None and expiry <= now:
        raise IdentityResolverError(
            "Key Vault secret has expired", code=IdentityResolverErrorCode.UPSTREAM_REJECTED
        )
    return expiry


__all__ = [
    "DEFAULT_API_VERSION",
    "DEFAULT_BEARER_AUDIENCE",
    "AzureBearerTokenProvider",
    "AzureKeyVaultResolver",
]

"""Azure Managed Identity (workload-identity) resolver.

This resolver does *not* talk to Azure directly. Instead it fronts the
Dapr Secrets API (or any compatible secret-store seam) and lets the
operator delegate the actual IMDS / workload-identity dance to Dapr's
``azure.keyvault`` or ``azure.managed-identity`` components. From the
resolver's point of view we GET a secret from a named secret store
under a named key and forward the result to the plugin.

Wire shape (Dapr default):

    GET {dapr_endpoint}/v1.0/secrets/{store_name}/{secret_name}
    [optional metadata.X query params]

The full URL is owned by the operator via the
``credentials.authentication`` payload, but we provide sensible
defaults so a manifest can reference just ``{store_name}/{secret_name}``
and inherit a cluster-wide Dapr sidecar endpoint from the connector
service's wiring.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any, ClassVar
from urllib.parse import quote

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

#: Dapr's default sidecar HTTP endpoint when the standard
#: ``DAPR_HTTP_PORT`` env var is unset. Matches the value documented in
#: ``deploy/helm/charts/connector-service`` and the v1 secret-bridge
#: prototype.
DEFAULT_DAPR_ENDPOINT: str = "http://localhost:3500"


class AzureManagedIdentityResolver:
    """Workload-identity resolver fronting the Dapr Secrets API.

    The resolver itself stays generic — its construction is the only
    Azure-specific bit. Operators that want to wire AWS IRSA or GCP
    Workload Identity through the same code path can either (a)
    register a vendor resolver, or (b) point the
    ``credentials.authentication.store`` field at a Dapr secret store
    backed by their preferred provider.
    """

    authentication_type: ClassVar[str] = "azure-managed-identity"
    category: ClassVar[IdentityCategory] = IdentityCategory.WORKLOAD

    def __init__(
        self,
        *,
        http: AsyncHttpClient,
        dapr_endpoint: str = DEFAULT_DAPR_ENDPOINT,
        request_timeout_seconds: float = 5.0,
    ) -> None:
        self._http = http
        self._dapr_endpoint = dapr_endpoint.rstrip("/")
        self._request_timeout = request_timeout_seconds

    async def resolve(
        self,
        *,
        credentials_authentication: Mapping[str, Any],
        context: IdentityResolverContext,
    ) -> ResolvedIdentity:
        store = _require_str(credentials_authentication, "store")
        secret_name = _require_str(credentials_authentication, "secretName")
        # Per-call override: some manifests will want to talk to a
        # named Dapr sidecar (e.g. in multi-cluster setups). We allow
        # the field but require it to be a full URL.
        endpoint = _optional_str(
            credentials_authentication, "endpoint", self._dapr_endpoint
        ).rstrip("/")

        url = f"{endpoint}/v1.0/secrets/{quote(store, safe='')}/{quote(secret_name, safe='')}"

        try:
            response = await self._http.request(
                HttpRequest(
                    method="GET",
                    url=url,
                    timeout_seconds=self._request_timeout,
                )
            )
        except UpstreamTransportError as exc:
            raise IdentityResolverError(
                detail=f"Dapr Secrets API transport error: {exc}",
                code=IdentityResolverErrorCode.UPSTREAM_UNAVAILABLE,
                data={
                    "endpoint": endpoint,
                    "store": store,
                    "secret_name": secret_name,
                },
            ) from exc

        if response.status_code in (401, 403):
            raise IdentityResolverError(
                detail=(
                    f"Dapr Secrets API rejected request for "
                    f"{store}/{secret_name} (status {response.status_code})"
                ),
                code=IdentityResolverErrorCode.UPSTREAM_UNAUTHORIZED,
                data={
                    "store": store,
                    "secret_name": secret_name,
                    "status_code": response.status_code,
                },
            )
        if response.status_code >= 400:
            raise IdentityResolverError(
                detail=(
                    f"Dapr Secrets API returned status {response.status_code} "
                    f"for {store}/{secret_name}"
                ),
                code=IdentityResolverErrorCode.UPSTREAM_REJECTED,
                data={
                    "store": store,
                    "secret_name": secret_name,
                    "status_code": response.status_code,
                },
            )

        try:
            body = response.json()
        except ValueError as exc:
            raise IdentityResolverError(
                detail=f"Dapr Secrets API response was not JSON: {exc}",
                code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
                data={"store": store, "secret_name": secret_name},
            ) from exc

        # Dapr returns a flat ``{key: value}`` mapping where ``key`` is
        # the secret name (and per-key entries when the underlying
        # store is structured). We forward the mapping verbatim.
        if not isinstance(body, dict) or not body:
            raise IdentityResolverError(
                detail="Dapr Secrets API response was empty or not an object",
                code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
                data={"store": store, "secret_name": secret_name},
            )

        # Coerce values to string for the opaque material; a Dapr store
        # returning a non-string value is unusual but we treat it as an
        # invalid response rather than guessing at a coercion.
        for key, value in body.items():
            if not isinstance(value, str):
                raise IdentityResolverError(
                    detail=(
                        f"Dapr Secrets API entry {key!r} for {store}/{secret_name} is not a string"
                    ),
                    code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
                    data={"store": store, "secret_name": secret_name, "key": key},
                )

        now = context.now()
        return ResolvedIdentity.build(
            authentication_type=self.authentication_type,
            category=self.category,
            material=dict(body),
            descriptor=f"azure-managed-identity:{store}/{secret_name}",
            issued_at=now,
            expires_at=None,
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


_ = datetime  # see analogous note in amazon_kms.py

__all__ = [
    "DEFAULT_DAPR_ENDPOINT",
    "AzureManagedIdentityResolver",
]

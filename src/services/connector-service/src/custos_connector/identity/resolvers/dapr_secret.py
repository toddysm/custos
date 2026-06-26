"""Dapr-secret-store resolver for the ``x-dapr-secret`` vendor token.

Some upstream targets — notably container registries such as Docker Hub
and GHCR — authenticate with a static username + Personal Access Token
(PAT) rather than a KMS, workload identity, or OIDC federation. This
resolver sources such a credential from a **Kubernetes Secret** (or any
other Dapr secret-store backend) read through the **Dapr Secrets API**,
exactly the same seam :class:`AzureManagedIdentityResolver` uses:

    GET {dapr_endpoint}/v1.0/secrets/{store}/{secretName}
    [?metadata.namespace={namespace}]

The connector instance carries only a *reference* (store name + secret
name + the keys that hold the username and token); the secret value
never appears in the manifest or instance config. The resolver extracts
``{username, token}`` and forwards it to the plugin as opaque material.
It is store-agnostic: the same ``authenticationType`` works against the
Kubernetes secret store (eval default ``custos-secretstore``), an
external backend such as Vault for HA, or a local file/env store for
off-cluster development, by pointing ``authentication.store`` at it.

Secret values are never logged: the raised :class:`IdentityResolverError`
payloads and the resolved ``descriptor`` carry only the store name, the
secret name, and key *names* — never their values.
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

#: Dapr's default sidecar HTTP endpoint when ``DAPR_HTTP_PORT`` is unset.
#: Matches :data:`custos_connector.identity.resolvers.azure_managed_identity.DEFAULT_DAPR_ENDPOINT`.
DEFAULT_DAPR_ENDPOINT: str = "http://localhost:3500"

#: The vendor ``authenticationType`` token this resolver answers to.
AUTHENTICATION_TYPE: str = "x-dapr-secret"

#: Default keys read from the resolved Secret when the instance does not
#: override them via ``usernameKey`` / ``tokenKey``.
DEFAULT_USERNAME_KEY: str = "username"
DEFAULT_TOKEN_KEY: str = "token"


class DaprSecretResolver:
    """Resolve a static ``{username, token}`` credential from a Dapr secret store.

    The resolver is the runtime counterpart to the ``x-dapr-secret``
    vendor token: the Loader records its identity category
    (:attr:`IdentityCategory.KMS`) at connector-type registration, and
    the :class:`~custos_connector.identity.registry.IdentityResolverRegistry`
    invokes :meth:`resolve` at bind time. It stays generic — its only
    binding to Kubernetes is the *default* store name the operator wires
    in; any Dapr secret-store component works.
    """

    authentication_type: ClassVar[str] = AUTHENTICATION_TYPE
    category: ClassVar[IdentityCategory] = IdentityCategory.KMS

    def __init__(
        self,
        *,
        http: AsyncHttpClient,
        default_store: str,
        dapr_endpoint: str = DEFAULT_DAPR_ENDPOINT,
        request_timeout_seconds: float = 5.0,
    ) -> None:
        if not default_store or not default_store.strip():
            raise ValueError("default_store must be a non-empty Dapr secret-store name")
        self._http = http
        self._default_store = default_store
        self._dapr_endpoint = dapr_endpoint.rstrip("/")
        self._request_timeout = request_timeout_seconds

    async def resolve(
        self,
        *,
        credentials_authentication: Mapping[str, Any],
        context: IdentityResolverContext,
    ) -> ResolvedIdentity:
        secret_name = _require_str(credentials_authentication, "secretName")
        store = _optional_str(credentials_authentication, "store", self._default_store)
        username_key = _optional_str(
            credentials_authentication, "usernameKey", DEFAULT_USERNAME_KEY
        )
        token_key = _optional_str(credentials_authentication, "tokenKey", DEFAULT_TOKEN_KEY)
        namespace = _optional_str(credentials_authentication, "namespace", "")

        quoted_store = quote(store, safe="")
        quoted_secret = quote(secret_name, safe="")
        url = f"{self._dapr_endpoint}/v1.0/secrets/{quoted_store}/{quoted_secret}"
        if namespace:
            url = f"{url}?metadata.namespace={quote(namespace, safe='')}"

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
                data=_diag(store, secret_name, namespace),
            ) from exc

        if response.status_code in (401, 403):
            raise IdentityResolverError(
                detail=(
                    f"Dapr Secrets API rejected request for {store}/{secret_name} "
                    f"(status {response.status_code})"
                ),
                code=IdentityResolverErrorCode.UPSTREAM_UNAUTHORIZED,
                data=_diag(store, secret_name, namespace, status_code=response.status_code),
            )
        if response.status_code >= 400:
            raise IdentityResolverError(
                detail=(
                    f"Dapr Secrets API returned status {response.status_code} "
                    f"for {store}/{secret_name}"
                ),
                code=IdentityResolverErrorCode.UPSTREAM_REJECTED,
                data=_diag(store, secret_name, namespace, status_code=response.status_code),
            )

        try:
            body = response.json()
        except ValueError as exc:
            raise IdentityResolverError(
                detail=f"Dapr Secrets API response was not JSON: {exc}",
                code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
                data=_diag(store, secret_name, namespace),
            ) from exc

        if not isinstance(body, dict) or not body:
            raise IdentityResolverError(
                detail="Dapr Secrets API response was empty or not an object",
                code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
                data=_diag(store, secret_name, namespace),
            )

        username = _extract_secret_value(body, username_key, store, secret_name, namespace)
        token = _extract_secret_value(body, token_key, store, secret_name, namespace)

        return ResolvedIdentity.build(
            authentication_type=self.authentication_type,
            category=self.category,
            material={"username": username, "token": token},
            descriptor=_descriptor(store, secret_name, namespace),
            issued_at=context.now(),
            expires_at=None,
        )


def _diag(
    store: str,
    secret_name: str,
    namespace: str,
    *,
    status_code: int | None = None,
) -> dict[str, Any]:
    """Build a redacted diagnostics payload — never carries secret values."""
    data: dict[str, Any] = {"store": store, "secret_name": secret_name}
    if namespace:
        data["namespace"] = namespace
    if status_code is not None:
        data["status_code"] = status_code
    return data


def _descriptor(store: str, secret_name: str, namespace: str) -> str:
    if namespace:
        return f"dapr-secret://{store}/{namespace}/{secret_name}"
    return f"dapr-secret://{store}/{secret_name}"


def _extract_secret_value(
    body: Mapping[str, Any],
    key: str,
    store: str,
    secret_name: str,
    namespace: str,
) -> str:
    """Return ``body[key]`` as a non-empty string, or raise INVALID_UPSTREAM_RESPONSE.

    The error never embeds the secret value — only the missing/invalid
    key *name* — so a malformed Secret can be diagnosed without leaking
    credential material into logs or audit payloads.
    """
    if key not in body:
        raise IdentityResolverError(
            detail=(
                f"Dapr Secret {store}/{secret_name} has no key {key!r}; "
                "expected the configured username/token keys"
            ),
            code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
            data={**_diag(store, secret_name, namespace), "key": key},
        )
    value = body[key]
    if not isinstance(value, str) or not value:
        raise IdentityResolverError(
            detail=f"Dapr Secret {store}/{secret_name} key {key!r} is not a non-empty string",
            code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
            data={**_diag(store, secret_name, namespace), "key": key},
        )
    return value


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


_ = datetime  # imported for the IdentityResolverContext.now() return type in docs/tests

__all__ = [
    "AUTHENTICATION_TYPE",
    "DEFAULT_DAPR_ENDPOINT",
    "DEFAULT_TOKEN_KEY",
    "DEFAULT_USERNAME_KEY",
    "DaprSecretResolver",
]

"""Amazon Secrets Manager (KMS-backed) identity resolver.

We talk to Secrets Manager rather than KMS-proper because that is the
service that returns *credential material* (an opaque string, JSON, or
binary blob). KMS-proper hands out data keys and is the right tool for
client-side encryption but the wrong shape for the
"credentials.authentication" v1 contract.

Wire protocol:

    POST {endpoint}/        # default: https://secretsmanager.{region}.amazonaws.com/
    Host: secretsmanager.{region}.amazonaws.com
    X-Amz-Target: secretsmanager.GetSecretValue
    Content-Type: application/x-amz-json-1.1
    Authorization: AWS4-HMAC-SHA256 ... (signed by sigv4)

    body: {"SecretId": "<arn-or-name>"}

The sigv4 signing step is delegated to an injected
:class:`AwsSigner` callable so the resolver itself does not depend on
botocore. Tests pass a no-op signer; production wires a small,
focussed sigv4 helper.
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

#: AWS Secrets Manager API target header value for the GetSecretValue
#: operation. Carried in ``X-Amz-Target`` on every request.
SECRETS_MANAGER_TARGET: str = "secretsmanager.GetSecretValue"

#: Wire content-type for the JSON-RPC 1.1 surface AWS Secrets Manager
#: speaks.
SECRETS_MANAGER_CONTENT_TYPE: str = "application/x-amz-json-1.1"


class AwsSignedHeaders:
    """The opaque headers an :class:`AwsSigner` returns.

    Stored as a small wrapper rather than a bare ``Mapping`` so the
    type signature is explicit at the resolver call site.
    """

    __slots__ = ("headers",)

    def __init__(self, headers: Mapping[str, str]) -> None:
        self.headers = dict(headers)


AwsSigner = Callable[[HttpRequest], Awaitable[AwsSignedHeaders]]


async def _identity_signer(request: HttpRequest) -> AwsSignedHeaders:
    """Default signer: returns the request headers unchanged.

    Production deployments must inject a real sigv4 signer. We use
    "identity" here so unit tests that simply check the request shape
    are not forced to provide a stub.
    """
    return AwsSignedHeaders(request.headers)


class AmazonKmsResolver:
    """KMS-backed resolver fronting AWS Secrets Manager."""

    authentication_type: ClassVar[str] = "amazon-kms"
    category: ClassVar[IdentityCategory] = IdentityCategory.KMS

    def __init__(
        self,
        *,
        http: AsyncHttpClient,
        signer: AwsSigner | None = None,
        request_timeout_seconds: float = 5.0,
    ) -> None:
        self._http = http
        self._signer = signer or _identity_signer
        self._request_timeout = request_timeout_seconds

    async def resolve(
        self,
        *,
        credentials_authentication: Mapping[str, Any],
        context: IdentityResolverContext,
    ) -> ResolvedIdentity:
        region = _require_str(credentials_authentication, "region")
        secret_id = _require_str(credentials_authentication, "secretId")
        endpoint = _optional_str(
            credentials_authentication,
            "endpoint",
            f"https://secretsmanager.{region}.amazonaws.com/",
        )

        request = HttpRequest(
            method="POST",
            url=endpoint,
            headers={
                "Host": _host_from_endpoint(endpoint, region),
                "X-Amz-Target": SECRETS_MANAGER_TARGET,
                "Content-Type": SECRETS_MANAGER_CONTENT_TYPE,
            },
            json_body={"SecretId": secret_id},
            timeout_seconds=self._request_timeout,
        )

        try:
            signed = await self._signer(request)
        except Exception as exc:
            raise IdentityResolverError(
                detail=f"AWS sigv4 signer raised: {exc}",
                code=IdentityResolverErrorCode.UPSTREAM_UNAVAILABLE,
                data={"region": region, "secret_id": secret_id},
            ) from exc

        signed_request = HttpRequest(
            method=request.method,
            url=request.url,
            headers=signed.headers,
            json_body=request.json_body,
            timeout_seconds=request.timeout_seconds,
        )

        try:
            response = await self._http.request(signed_request)
        except UpstreamTransportError as exc:
            raise IdentityResolverError(
                detail=f"Secrets Manager POST transport error: {exc}",
                code=IdentityResolverErrorCode.UPSTREAM_UNAVAILABLE,
                data={"region": region, "secret_id": secret_id},
            ) from exc

        if response.status_code in (401, 403):
            raise IdentityResolverError(
                detail=(
                    f"Secrets Manager rejected request for {secret_id!r} "
                    f"(status {response.status_code})"
                ),
                code=IdentityResolverErrorCode.UPSTREAM_UNAUTHORIZED,
                data={
                    "region": region,
                    "secret_id": secret_id,
                    "status_code": response.status_code,
                },
            )
        if response.status_code >= 400:
            raise IdentityResolverError(
                detail=(
                    f"Secrets Manager returned status {response.status_code} for {secret_id!r}"
                ),
                code=IdentityResolverErrorCode.UPSTREAM_REJECTED,
                data={
                    "region": region,
                    "secret_id": secret_id,
                    "status_code": response.status_code,
                },
            )

        try:
            body = response.json()
        except ValueError as exc:
            raise IdentityResolverError(
                detail=f"Secrets Manager response was not JSON: {exc}",
                code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
                data={"region": region, "secret_id": secret_id},
            ) from exc

        if not isinstance(body, dict):
            raise IdentityResolverError(
                detail="Secrets Manager response was not a JSON object",
                code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
                data={"region": region, "secret_id": secret_id},
            )

        material = _extract_secret_material(body)
        if material is None:
            raise IdentityResolverError(
                detail=(
                    "Secrets Manager response missing both SecretString and SecretBinary fields"
                ),
                code=IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE,
                data={"region": region, "secret_id": secret_id},
            )

        now = context.now()
        return ResolvedIdentity.build(
            authentication_type=self.authentication_type,
            category=self.category,
            material=material,
            descriptor=f"amazon-kms:{region}:{secret_id}",
            issued_at=now,
            expires_at=None,
        )


def _host_from_endpoint(endpoint: str, region: str) -> str:
    # Strip scheme + trailing slash so the ``Host`` header stays clean.
    cleaned = endpoint.removeprefix("https://").removeprefix("http://")
    cleaned = cleaned.rstrip("/")
    return cleaned or f"secretsmanager.{region}.amazonaws.com"


def _extract_secret_material(body: Mapping[str, Any]) -> Mapping[str, Any] | None:
    secret_string = body.get("SecretString")
    if isinstance(secret_string, str):
        return {"secret": secret_string}
    secret_binary = body.get("SecretBinary")
    if isinstance(secret_binary, str):
        # SecretBinary is base64-encoded on the wire. We forward it
        # verbatim — the plugin is responsible for any decoding.
        return {"secret_binary_b64": secret_binary}
    return None


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


# Keep the unused-import guard happy: ``datetime`` is part of the
# resolver context typing surface even though the resolver derives its
# timestamps from ``context.now()``.
_ = datetime

__all__ = [
    "SECRETS_MANAGER_CONTENT_TYPE",
    "SECRETS_MANAGER_TARGET",
    "AmazonKmsResolver",
    "AwsSignedHeaders",
    "AwsSigner",
]

"""Unit tests for the four built-in identity resolvers.

Each resolver is exercised via a stub :class:`AsyncHttpClient` that
records the outbound request and returns a canned response, so we
cover the URL shape, the field-extraction logic, the error-mapping
table, and the response-parsing edge cases without standing up real
upstream identity providers.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

import pytest

from custos_connector.identity import (
    AmazonKmsResolver,
    AzureKeyVaultResolver,
    AzureManagedIdentityResolver,
    HttpRequest,
    HttpResponse,
    IdentityResolverError,
    OidcFederatedResolver,
)
from custos_connector.identity.errors import IdentityResolverErrorCode
from custos_connector.identity.protocols import IdentityResolverContext
from custos_connector.identity.resolvers.amazon_kms import AwsSignedHeaders
from custos_connector.identity.transport import UpstreamTransportError
from custos_connector.loader.identity import IdentityCategory

_FIXED_NOW = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)


def _context() -> IdentityResolverContext:
    return IdentityResolverContext(
        workspace_id="ws-1",
        instance_id="inst-A",
        lease_ttl_seconds=600,
        now=lambda: _FIXED_NOW,
    )


class _StubHttp:
    """Records each outbound request; returns a canned response (or raises)."""

    def __init__(
        self,
        *,
        responses: list[HttpResponse | Exception] | None = None,
    ) -> None:
        # When the list is short the last entry is repeated. A bare
        # 200 / empty-body response is the default so resolvers under
        # test don't have to provide responses for boilerplate calls.
        self.responses: list[HttpResponse | Exception] = list(responses or [])
        self.requests: list[HttpRequest] = []

    async def request(self, request: HttpRequest) -> HttpResponse:
        self.requests.append(request)
        if not self.responses:
            return HttpResponse(status_code=200, body=b"{}")
        result = self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]
        if isinstance(result, Exception):
            raise result
        return result

    async def aclose(self) -> None:  # pragma: no cover - not exercised
        pass


def _json_body(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(payload).encode("utf-8")


# ---------------------------------------------------------------------------
# AzureKeyVaultResolver
# ---------------------------------------------------------------------------


class TestAzureKeyVaultResolver:
    @pytest.mark.asyncio
    async def test_happy_path_forwards_secret_value(self) -> None:
        http = _StubHttp(
            responses=[
                HttpResponse(
                    status_code=200,
                    body=_json_body({"value": "the-secret"}),
                )
            ]
        )

        async def _token_provider(audience: str) -> str:
            assert audience == "https://vault.azure.net/.default"
            return "bearer-xyz"

        resolver = AzureKeyVaultResolver(http=http, token_provider=_token_provider)
        identity = await resolver.resolve(
            credentials_authentication={
                "vaultUri": "https://example.vault.azure.net",
                "secretName": "db-password",
            },
            context=_context(),
        )
        assert identity.authentication_type == "azure-key-vault"
        assert identity.category is IdentityCategory.KMS
        assert identity.material["secret"] == "the-secret"
        assert identity.descriptor == (
            "azure-key-vault:https://example.vault.azure.net/secrets/db-password"
        )
        # The request shape matches the documented Key Vault REST.
        outbound = http.requests[0]
        assert outbound.method == "GET"
        assert outbound.url == (
            "https://example.vault.azure.net/secrets/db-password?api-version=7.4"
        )
        assert outbound.headers["Authorization"] == "Bearer bearer-xyz"

    @pytest.mark.asyncio
    async def test_strips_trailing_slash_from_vault_uri(self) -> None:
        http = _StubHttp(responses=[HttpResponse(200, body=_json_body({"value": "s"}))])

        async def _token_provider(_audience: str) -> str:
            return "tk"

        resolver = AzureKeyVaultResolver(http=http, token_provider=_token_provider)
        await resolver.resolve(
            credentials_authentication={
                "vaultUri": "https://example.vault.azure.net/",
                "secretName": "foo",
            },
            context=_context(),
        )
        assert "//secrets/" not in http.requests[0].url

    @pytest.mark.asyncio
    async def test_401_maps_to_upstream_unauthorized(self) -> None:
        http = _StubHttp(responses=[HttpResponse(401, body=b"{}")])

        async def _tk(_audience: str) -> str:
            return "tk"

        resolver = AzureKeyVaultResolver(http=http, token_provider=_tk)
        with pytest.raises(IdentityResolverError) as info:
            await resolver.resolve(
                credentials_authentication={
                    "vaultUri": "https://v",
                    "secretName": "foo",
                },
                context=_context(),
            )
        assert info.value.code is IdentityResolverErrorCode.UPSTREAM_UNAUTHORIZED

    @pytest.mark.asyncio
    async def test_transport_error_maps_to_unavailable(self) -> None:
        http = _StubHttp(responses=[UpstreamTransportError("connection refused")])

        async def _tk(_audience: str) -> str:
            return "tk"

        resolver = AzureKeyVaultResolver(http=http, token_provider=_tk)
        with pytest.raises(IdentityResolverError) as info:
            await resolver.resolve(
                credentials_authentication={
                    "vaultUri": "https://v",
                    "secretName": "foo",
                },
                context=_context(),
            )
        assert info.value.code is IdentityResolverErrorCode.UPSTREAM_UNAVAILABLE

    @pytest.mark.asyncio
    async def test_missing_field_is_caught_before_request(self) -> None:
        http = _StubHttp()

        async def _tk(_audience: str) -> str:  # pragma: no cover - unreachable
            return "tk"

        resolver = AzureKeyVaultResolver(http=http, token_provider=_tk)
        with pytest.raises(IdentityResolverError) as info:
            await resolver.resolve(
                credentials_authentication={"secretName": "foo"},
                context=_context(),
            )
        assert info.value.code is IdentityResolverErrorCode.MISSING_CREDENTIAL_FIELD
        assert http.requests == []

    @pytest.mark.asyncio
    async def test_invalid_response_payload_is_caught(self) -> None:
        http = _StubHttp(responses=[HttpResponse(200, body=b"{}")])

        async def _tk(_audience: str) -> str:
            return "tk"

        resolver = AzureKeyVaultResolver(http=http, token_provider=_tk)
        with pytest.raises(IdentityResolverError) as info:
            await resolver.resolve(
                credentials_authentication={
                    "vaultUri": "https://v",
                    "secretName": "foo",
                },
                context=_context(),
            )
        assert info.value.code is (IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE)

    @pytest.mark.asyncio
    async def test_attributes_exp_propagates_to_expires_at(self) -> None:
        future_unix = int(_FIXED_NOW.timestamp()) + 3600
        http = _StubHttp(
            responses=[
                HttpResponse(
                    200,
                    body=_json_body({"value": "s", "attributes": {"exp": future_unix}}),
                )
            ]
        )

        async def _tk(_audience: str) -> str:
            return "tk"

        resolver = AzureKeyVaultResolver(http=http, token_provider=_tk)
        identity = await resolver.resolve(
            credentials_authentication={
                "vaultUri": "https://v",
                "secretName": "foo",
            },
            context=_context(),
        )
        assert identity.expires_at is not None
        assert identity.expires_at > _FIXED_NOW


# ---------------------------------------------------------------------------
# AmazonKmsResolver
# ---------------------------------------------------------------------------


class TestAmazonKmsResolver:
    @pytest.mark.asyncio
    async def test_happy_path_forwards_secret_string(self) -> None:
        http = _StubHttp(responses=[HttpResponse(200, body=_json_body({"SecretString": "abc"}))])

        signer_calls: list[HttpRequest] = []

        async def _signer(req: HttpRequest) -> AwsSignedHeaders:
            signer_calls.append(req)
            headers = dict(req.headers)
            headers["Authorization"] = "AWS4-HMAC-SHA256 Credential=test/..."
            return AwsSignedHeaders(headers)

        resolver = AmazonKmsResolver(http=http, signer=_signer)
        identity = await resolver.resolve(
            credentials_authentication={
                "region": "us-east-1",
                "secretId": "arn:aws:secretsmanager:us-east-1:1:secret:foo",
            },
            context=_context(),
        )
        assert identity.material["secret"] == "abc"
        outbound = http.requests[0]
        assert outbound.headers["X-Amz-Target"] == "secretsmanager.GetSecretValue"
        assert outbound.headers["Authorization"].startswith("AWS4-HMAC-SHA256")
        assert outbound.json_body == {"SecretId": "arn:aws:secretsmanager:us-east-1:1:secret:foo"}
        parsed = urlparse(outbound.url)
        assert parsed.hostname is not None
        assert parsed.hostname == "amazonaws.com" or parsed.hostname.endswith(".amazonaws.com")
        assert signer_calls

    @pytest.mark.asyncio
    async def test_binary_secret_is_forwarded_under_b64_key(self) -> None:
        body = _json_body({"SecretBinary": base64.b64encode(b"raw").decode("ascii")})
        http = _StubHttp(responses=[HttpResponse(200, body=body)])

        resolver = AmazonKmsResolver(http=http)
        identity = await resolver.resolve(
            credentials_authentication={
                "region": "us-east-1",
                "secretId": "x",
            },
            context=_context(),
        )
        assert "secret_binary_b64" in identity.material

    @pytest.mark.asyncio
    async def test_missing_string_and_binary_is_rejected(self) -> None:
        http = _StubHttp(responses=[HttpResponse(200, body=_json_body({}))])
        resolver = AmazonKmsResolver(http=http)
        with pytest.raises(IdentityResolverError) as info:
            await resolver.resolve(
                credentials_authentication={
                    "region": "us-east-1",
                    "secretId": "x",
                },
                context=_context(),
            )
        assert info.value.code is (IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE)

    @pytest.mark.asyncio
    async def test_403_maps_to_upstream_unauthorized(self) -> None:
        http = _StubHttp(responses=[HttpResponse(403, body=b"{}")])
        resolver = AmazonKmsResolver(http=http)
        with pytest.raises(IdentityResolverError) as info:
            await resolver.resolve(
                credentials_authentication={"region": "us-east-1", "secretId": "x"},
                context=_context(),
            )
        assert info.value.code is IdentityResolverErrorCode.UPSTREAM_UNAUTHORIZED


# ---------------------------------------------------------------------------
# AzureManagedIdentityResolver (Dapr Secrets API)
# ---------------------------------------------------------------------------


class TestAzureManagedIdentityResolver:
    @pytest.mark.asyncio
    async def test_happy_path_passes_through_secret_mapping(self) -> None:
        http = _StubHttp(responses=[HttpResponse(200, body=_json_body({"password": "pw"}))])
        resolver = AzureManagedIdentityResolver(http=http)
        identity = await resolver.resolve(
            credentials_authentication={
                "store": "kv-prod",
                "secretName": "db-password",
            },
            context=_context(),
        )
        assert identity.category is IdentityCategory.WORKLOAD
        assert identity.material == {"password": "pw"}
        outbound = http.requests[0]
        assert outbound.url.endswith("/v1.0/secrets/kv-prod/db-password")

    @pytest.mark.asyncio
    async def test_url_quoting_handles_path_separators(self) -> None:
        http = _StubHttp(responses=[HttpResponse(200, body=_json_body({"v": "ok"}))])
        resolver = AzureManagedIdentityResolver(http=http)
        await resolver.resolve(
            credentials_authentication={
                "store": "kv/prod",
                "secretName": "ns/db",
            },
            context=_context(),
        )
        assert "kv%2Fprod" in http.requests[0].url
        assert "ns%2Fdb" in http.requests[0].url

    @pytest.mark.asyncio
    async def test_non_string_value_is_rejected(self) -> None:
        http = _StubHttp(responses=[HttpResponse(200, body=_json_body({"v": 42}))])
        resolver = AzureManagedIdentityResolver(http=http)
        with pytest.raises(IdentityResolverError) as info:
            await resolver.resolve(
                credentials_authentication={"store": "s", "secretName": "n"},
                context=_context(),
            )
        assert info.value.code is (IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE)


# ---------------------------------------------------------------------------
# OidcFederatedResolver
# ---------------------------------------------------------------------------


class TestOidcFederatedResolver:
    @pytest.mark.asyncio
    async def test_token_exchange_happy_path(self) -> None:
        token_body = _json_body(
            {
                "access_token": "tok",
                "token_type": "Bearer",
                "expires_in": 3600,
                "scope": "api://upstream/.default",
                "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
            }
        )
        http = _StubHttp(responses=[HttpResponse(200, body=token_body)])

        async def _subject(context: IdentityResolverContext) -> str:
            return f"workload-id-token-for-{context.workspace_id}"

        resolver = OidcFederatedResolver(http=http, subject_token_provider=_subject)
        identity = await resolver.resolve(
            credentials_authentication={
                "issuer": "https://issuer.example",
                "audience": "api://upstream",
                "tokenEndpoint": "https://issuer.example/oauth/token",
                "scope": "api://upstream/.default",
            },
            context=_context(),
        )
        assert identity.material["access_token"] == "tok"
        assert identity.material["token_type"] == "Bearer"
        assert identity.material["scope"] == "api://upstream/.default"
        assert identity.expires_at is not None
        outbound = http.requests[0]
        assert outbound.method == "POST"
        assert outbound.headers["Content-Type"] == "application/x-www-form-urlencoded"
        assert outbound.form_body is not None
        assert outbound.form_body["grant_type"] == "urn:ietf:params:oauth:grant-type:token-exchange"
        assert outbound.form_body["audience"] == "api://upstream"
        assert outbound.form_body["subject_token"] == "workload-id-token-for-ws-1"

    @pytest.mark.asyncio
    async def test_jwks_pre_warm_is_optional_and_failure_safe(self) -> None:
        token_body = _json_body({"access_token": "tok", "token_type": "Bearer", "expires_in": 60})
        # Token exchange succeeds; JWKS fetch fails. Resolver should still
        # return the access token.
        http = _StubHttp(
            responses=[
                HttpResponse(200, body=token_body),
                HttpResponse(500, body=b""),
            ]
        )

        async def _subject(_c: IdentityResolverContext) -> str:
            return "subject"

        resolver = OidcFederatedResolver(http=http, subject_token_provider=_subject)
        identity = await resolver.resolve(
            credentials_authentication={
                "issuer": "https://issuer",
                "audience": "aud",
                "tokenEndpoint": "https://issuer/oauth/token",
                "jwksUri": "https://issuer/.well-known/jwks.json",
            },
            context=_context(),
        )
        assert identity.material["access_token"] == "tok"

    @pytest.mark.asyncio
    async def test_jwks_cache_serves_repeated_calls(self) -> None:
        jwks_body = _json_body({"keys": [{"kty": "RSA", "kid": "k1"}]})
        http = _StubHttp(responses=[HttpResponse(200, body=jwks_body)])

        async def _subject(_c: IdentityResolverContext) -> str:  # pragma: no cover
            return "subject"

        resolver = OidcFederatedResolver(
            http=http,
            subject_token_provider=_subject,
            jwks_cache_ttl_seconds=300,
        )
        first = await resolver.get_jwks("https://issuer/.well-known/jwks.json")
        second = await resolver.get_jwks("https://issuer/.well-known/jwks.json")
        assert first == second
        # Only one upstream call thanks to the cache.
        assert len(http.requests) == 1

    @pytest.mark.asyncio
    async def test_missing_access_token_is_rejected(self) -> None:
        http = _StubHttp(responses=[HttpResponse(200, body=_json_body({"token_type": "Bearer"}))])

        async def _subject(_c: IdentityResolverContext) -> str:
            return "subject"

        resolver = OidcFederatedResolver(http=http, subject_token_provider=_subject)
        with pytest.raises(IdentityResolverError) as info:
            await resolver.resolve(
                credentials_authentication={
                    "issuer": "https://issuer",
                    "audience": "aud",
                    "tokenEndpoint": "https://issuer/oauth/token",
                },
                context=_context(),
            )
        assert info.value.code is (IdentityResolverErrorCode.INVALID_UPSTREAM_RESPONSE)

    @pytest.mark.asyncio
    async def test_401_maps_to_unauthorized(self) -> None:
        http = _StubHttp(responses=[HttpResponse(401, body=b"{}")])

        async def _subject(_c: IdentityResolverContext) -> str:
            return "subject"

        resolver = OidcFederatedResolver(http=http, subject_token_provider=_subject)
        with pytest.raises(IdentityResolverError) as info:
            await resolver.resolve(
                credentials_authentication={
                    "issuer": "https://issuer",
                    "audience": "aud",
                    "tokenEndpoint": "https://issuer/oauth/token",
                },
                context=_context(),
            )
        assert info.value.code is IdentityResolverErrorCode.UPSTREAM_UNAUTHORIZED

    @pytest.mark.asyncio
    async def test_negative_cache_ttl_rejected(self) -> None:
        with pytest.raises(ValueError, match=">= 0"):
            OidcFederatedResolver(http=_StubHttp(), jwks_cache_ttl_seconds=-1)

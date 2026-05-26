"""Unit tests for the resolver value objects + transport seam."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, ClassVar

import pytest

from custos_connector.identity import (
    HttpRequest,
    HttpResponse,
    ResolvedIdentity,
)
from custos_connector.identity.errors import (
    IdentityResolverError,
    IdentityResolverErrorCode,
)
from custos_connector.identity.transport import HttpxAsyncHttpClient


class TestResolvedIdentity:
    def test_build_freezes_material_into_mapping_proxy(self) -> None:
        material = {"secret": "value"}
        identity = ResolvedIdentity.build(
            authentication_type="azure-key-vault",
            category=__import__(
                "custos_connector.loader.identity", fromlist=["IdentityCategory"]
            ).IdentityCategory.KMS,
            material=material,
            descriptor="azure-key-vault:https://vault/secrets/foo",
            issued_at=datetime(2026, 1, 1, tzinfo=UTC),
            expires_at=None,
        )
        assert isinstance(identity.material, MappingProxyType)
        # Mutating the source dict does not leak into the frozen snapshot.
        material["secret"] = "tampered"
        assert identity.material["secret"] == "value"
        assert identity.plugin_envelope_keys == ("secret",)

    def test_plugin_envelope_keys_is_sorted_tuple(self) -> None:
        from custos_connector.loader.identity import IdentityCategory

        identity = ResolvedIdentity.build(
            authentication_type="oidc",
            category=IdentityCategory.FEDERATED,
            material={"token_type": "Bearer", "access_token": "abc"},
            descriptor="oidc:issuer|aud=foo",
            issued_at=datetime(2026, 1, 1, tzinfo=UTC),
            expires_at=datetime(2026, 1, 1, 1, tzinfo=UTC),
        )
        assert identity.plugin_envelope_keys == ("access_token", "token_type")


class TestIdentityResolverError:
    def test_default_code_is_upstream_rejected(self) -> None:
        err = IdentityResolverError("nope")
        assert err.code is IdentityResolverErrorCode.UPSTREAM_REJECTED
        assert err.detail == "nope"
        assert err.data == {}

    def test_explicit_code_and_data_are_recorded(self) -> None:
        err = IdentityResolverError(
            "boom",
            code=IdentityResolverErrorCode.MISSING_CREDENTIAL_FIELD,
            data={"field": "vaultUri"},
        )
        assert err.code is IdentityResolverErrorCode.MISSING_CREDENTIAL_FIELD
        assert err.data == {"field": "vaultUri"}
        assert str(err) == "boom"

    def test_data_is_frozen(self) -> None:
        payload = {"field": "secretName"}
        err = IdentityResolverError("missing", data=payload)
        # Mutating the source dict does not leak.
        payload["field"] = "tampered"
        assert err.data["field"] == "secretName"


class TestHttpResponse:
    def test_json_decodes_utf8_body(self) -> None:
        body = json.dumps({"foo": "bar"}).encode("utf-8")
        response = HttpResponse(status_code=200, body=body)
        assert response.json() == {"foo": "bar"}

    def test_json_raises_on_empty_body(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            HttpResponse(status_code=204, body=b"").json()

    def test_json_raises_on_malformed_body(self) -> None:
        with pytest.raises(ValueError):
            HttpResponse(status_code=200, body=b"not json").json()


class TestHttpxAsyncHttpClient:
    @pytest.mark.asyncio
    async def test_request_rejects_dual_payload(self) -> None:
        # We deliberately do not require an httpx.AsyncClient here:
        # the validation runs before any network call.
        adapter = HttpxAsyncHttpClient(client=object(), owns_client=False)
        with pytest.raises(ValueError, match="both json_body and form_body"):
            await adapter.request(
                HttpRequest(
                    method="POST",
                    url="https://example.invalid",
                    json_body={"a": 1},
                    form_body={"a": "1"},
                )
            )

    @pytest.mark.asyncio
    async def test_request_lowercases_response_headers(self) -> None:
        class _StubResponse:
            status_code = 200
            content = b'{"ok": true}'
            headers: ClassVar[dict[str, str]] = {
                "Content-Type": "application/json",
                "X-Foo": "bar",
            }

        class _StubClient:
            def __init__(self) -> None:
                self.last_kwargs: Mapping[str, Any] | None = None

            async def request(self, **kwargs: Any) -> _StubResponse:
                self.last_kwargs = kwargs
                return _StubResponse()

            async def aclose(self) -> None:  # pragma: no cover - not exercised
                pass

        stub = _StubClient()
        adapter = HttpxAsyncHttpClient(client=stub, owns_client=False)
        response = await adapter.request(
            HttpRequest(
                method="GET",
                url="https://example.invalid/api",
                headers={"Authorization": "Bearer t"},
            )
        )
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/json"
        assert response.headers["x-foo"] == "bar"
        assert stub.last_kwargs is not None
        assert stub.last_kwargs["method"] == "GET"
        assert stub.last_kwargs["headers"] == {"Authorization": "Bearer t"}

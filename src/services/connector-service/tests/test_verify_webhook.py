"""Private webhook authentication, exact scope, rotation and indexed batch contracts."""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import replace

import pytest
from custos_spl.errors import BackendUnavailable
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from fastapi.testclient import TestClient

from custos_connector import create_app
from custos_connector.identity import IdentityResolverError
from custos_connector.listen.verification import (
    MAX_ENCODED_REQUEST_BYTES,
    MAX_HEADER_BYTES,
    MAX_RAW_BYTES,
)
from custos_connector.providers import Providers
from tests._webhook import (
    MESH_HEADERS,
    RAW,
    RotatingResolver,
    encoded_envelope,
    envelope,
    memory_providers,
    seed,
    settings,
    signature,
)

Configured = tuple[TestClient, RotatingResolver, Providers]


@pytest.fixture
def configured() -> Iterator[Configured]:
    """Open real middleware/routes over scoped providers and the real credential registry."""
    resolver = RotatingResolver()
    providers = memory_providers(resolver)
    with TestClient(create_app(settings=settings(), providers=providers)) as client:
        assert client.portal is not None
        client.portal.call(seed, providers, "a")
        client.portal.call(seed, providers, "b")
        yield client, resolver, providers


def test_verified_mixed_batch_has_exact_indexed_partition(configured: Configured) -> None:
    """No payload, credential or arbitrary exception data appears in rejection metadata."""
    client, resolver, providers = configured
    raw = json.dumps(
        {
            "events": [
                {"eventId": "first", "eventType": "registry.push", "value": 1},
                {"eventType": "registry.push", "private": "payload-not-in-rejection"},
                {"eventId": "bad", "eventType": "unknown-private-event"},
                {"eventId": "last", "eventType": "registry.push", "value": 2},
                "malformed-private-payload",
            ]
        }
    ).encode()
    response = client.post("/VerifyWebhook", json=envelope(raw), headers=MESH_HEADERS)
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["receivedCount"] == 5
    assert [item["index"] for item in result["events"]] == [0, 3]
    assert result["rejections"] == [
        {"index": 1, "code": "missing-event-id"},
        {"index": 2, "code": "unknown-event-type"},
        {"index": 4, "code": "malformed"},
    ]
    assert sorted(item["index"] for item in result["events"] + result["rejections"]) == list(
        range(5)
    )
    assert [item["event"]["eventId"] for item in result["events"]] == ["first", "last"]
    assert result["events"][0]["event"]["source"]["workspaceId"] == "a"
    assert result["events"][0]["event"]["source"]["instanceId"] == "same"
    assert result["events"][0]["event"]["payload"] == {"value": 1}
    assert resolver.calls == [("a", "same", "a")]
    assert "unit-key" not in response.text and "private" not in response.text
    assert providers.event_publisher is None


def test_colliding_instance_ids_and_wrong_workspace(configured: Configured) -> None:
    """A valid signature for one workspace never authenticates its identically named sibling."""
    client, resolver, _ = configured
    assert (
        client.post(
            "/VerifyWebhook", json=envelope(workspace="missing"), headers=MESH_HEADERS
        ).status_code
        == 404
    )
    assert not resolver.calls
    assert (
        client.post(
            "/VerifyWebhook", json=envelope(workspace="b"), headers=MESH_HEADERS
        ).status_code
        == 401
    )
    response = client.post(
        "/VerifyWebhook",
        json=envelope(workspace="b", headers={"x-custos-signature": signature(RAW, "unit-key-b")}),
        headers=MESH_HEADERS,
    )
    assert response.status_code == 200
    assert response.json()["events"][0]["event"]["source"]["workspaceId"] == "b"
    assert all(call == ("b", "same", "b") for call in resolver.calls)


@pytest.mark.parametrize(
    "raw",
    [
        b"{",
        b'{"events":[]}',
        b'{"events":{}}',
        b"[]",
        b'{"events":[NaN]}',
        b'{"events":[],"events":[{}]}',
        b'{"events":[{"x":"\\ud800"}]}',
    ],
)
def test_signature_precedes_vendor_parsing(configured: Configured, raw: bytes) -> None:
    """Malformed vendor JSON is not parsed for an unauthenticated request."""
    client, _, _ = configured
    rejected = client.post(
        "/VerifyWebhook",
        json=envelope(raw, headers={"x-custos-signature": "sha256=00"}),
        headers=MESH_HEADERS,
    )
    assert rejected.status_code == 401
    verified = client.post("/VerifyWebhook", json=envelope(raw), headers=MESH_HEADERS)
    assert verified.status_code == 400
    assert verified.json()["error"]["code"] == "webhook.malformed"


def test_all_invalid_batch_is_still_an_indexed_success(configured: Configured) -> None:
    """Verification success remains distinct from per-event normalization acceptance."""
    client, _, _ = configured
    raw = b'{"events":[{},{"eventId":"a"}]}'
    response = client.post("/VerifyWebhook", json=envelope(raw), headers=MESH_HEADERS)
    assert response.status_code == 200
    assert response.json() == {
        "schemaVersion": "1",
        "receivedCount": 2,
        "events": [],
        "rejections": [
            {"index": 0, "code": "missing-event-id"},
            {"index": 1, "code": "missing-event-type"},
        ],
    }


def test_rotation_does_not_accept_previously_cached_material(configured: Configured) -> None:
    """Even a live registry cache entry cannot extend the acceptance of a rotated signing key."""
    client, resolver, providers = configured
    assert client.portal is not None

    async def warm_cache() -> None:
        """Populate the same registry key with an ordinary long-lived binding read."""
        await providers.identity_registry.resolve(
            workspace_id="a",
            actor="test",
            instance_id="same",
            authentication_type="azure-key-vault",
            credentials_authentication={"vaultUri": "https://vault.example", "secretName": "a"},
            lease_ttl_seconds=300,
        )

    client.portal.call(warm_cache)
    resolver.secrets["a"] = "unit-rotated"
    assert client.post("/VerifyWebhook", json=envelope(), headers=MESH_HEADERS).status_code == 401
    new = envelope(headers={"x-custos-signature": signature(RAW, "unit-rotated")})
    assert client.post("/VerifyWebhook", json=new, headers=MESH_HEADERS).status_code == 200
    assert len(resolver.calls) == 3


def test_bearer_is_only_private_verification_material() -> None:
    """Ambient Authorization cannot authenticate either the mesh or the external webhook."""
    resolver = RotatingResolver()
    providers = memory_providers(resolver)
    with TestClient(create_app(settings=settings(), providers=providers)) as client:
        assert client.portal is not None
        client.portal.call(seed, providers, "a", "bearer")
        body = envelope(headers={"Authorization": "Bearer unit-key-a"})
        assert (
            client.post(
                "/VerifyWebhook", json=body, headers={"Authorization": "Bearer unit-key-a"}
            ).status_code
            == 401
        )
        assert (
            client.post(
                "/VerifyWebhook",
                json=envelope(headers={}),
                headers={**MESH_HEADERS, "Authorization": "Bearer unit-key-a"},
            ).status_code
            == 401
        )
        accepted = client.post("/VerifyWebhook", json=body, headers=MESH_HEADERS)
        assert accepted.status_code == 200
        assert "unit-key-a" not in accepted.text and "Authorization" not in accepted.text
        resolver.secrets["a"] = "unit-next"
        assert client.post("/VerifyWebhook", json=body, headers=MESH_HEADERS).status_code == 401
        body["headers"]["Authorization"] = "Bearer unit-next"
        assert client.post("/VerifyWebhook", json=body, headers=MESH_HEADERS).status_code == 200


@pytest.mark.parametrize(
    "change,status",
    [
        ({}, 401),
        (
            {
                "dapr-api-token": "wrong",
                "dapr-caller-app-id": "trigger-service",
                "dapr-caller-namespace": "custos",
            },
            401,
        ),
        ({**MESH_HEADERS, "dapr-caller-app-id": "other"}, 403),
        ({**MESH_HEADERS, "dapr-caller-namespace": "other"}, 403),
    ],
)
def test_only_configured_service_identity_is_allowed(
    configured: Configured, change: dict[str, str], status: int
) -> None:
    """Unverified or foreign service headers cannot authorize a tenant lookup."""
    client, resolver, _ = configured
    response = client.post("/VerifyWebhook", json=envelope(), headers=change)
    assert response.status_code == status
    assert not resolver.calls


def test_duplicate_mesh_headers_are_rejected(configured: Configured) -> None:
    """Duplicate token or identity headers never collapse into a trusted single value."""
    client, resolver, _ = configured
    for header in ("dapr-api-token", "dapr-caller-app-id", "dapr-caller-namespace"):
        response = client.post(
            "/VerifyWebhook",
            json=envelope(),
            headers=[*MESH_HEADERS.items(), (header, MESH_HEADERS[header])],
        )
        assert response.status_code == (401 if header == "dapr-api-token" else 403)
    assert not resolver.calls


@pytest.mark.parametrize("token", [None, "", "   "])
def test_missing_sidecar_configuration_never_uses_development_shim(token: str | None) -> None:
    """The new private verifier fails closed even when other development routes use a shim."""
    with TestClient(
        create_app(
            settings=replace(settings(), app_api_token=token),
            providers=memory_providers(RotatingResolver()),
        )
    ) as client:
        response = client.post("/VerifyWebhook", json=envelope(), headers=MESH_HEADERS)
    assert response.status_code == 503


def test_sidecar_token_is_not_in_configuration_repr() -> None:
    """Diagnostic configuration rendering must not reveal the private app-hop credential."""
    assert MESH_HEADERS["dapr-api-token"] not in repr(settings())


@pytest.mark.parametrize(
    "headers",
    [
        {"Authorization": "one", "authorization": "two"},
        {"x-custos-callctx": "forged"},
        {"x-custos-webhook-authorization": "forged"},
        {"dapr-caller-app-id": "trigger-service"},
        {"bad name": "x"},
        {"x-valid": "line\ninjection"},
    ],
)
def test_forwarded_identity_and_ambiguous_headers_are_rejected(
    configured: Configured, headers: dict[str, str]
) -> None:
    """Private identity metadata is not accepted as external vendor verification data."""
    client, resolver, _ = configured
    response = client.post("/VerifyWebhook", json=envelope(headers=headers), headers=MESH_HEADERS)
    assert response.status_code == 400
    assert not resolver.calls


def test_header_byte_limit_is_exact(configured: Configured) -> None:
    """Exactly 16 KiB passes; a single additional byte is rejected before resolving credentials."""
    client, resolver, _ = configured
    headers = {"x-custos-signature": signature(RAW), "x-padding": ""}
    used = sum(len(key.encode()) + len(value.encode()) + 4 for key, value in headers.items())
    headers["x-padding"] = "x" * (MAX_HEADER_BYTES - used)
    assert (
        client.post(
            "/VerifyWebhook", json=envelope(headers=headers), headers=MESH_HEADERS
        ).status_code
        == 200
    )
    before = len(resolver.calls)
    headers["x-padding"] += "x"
    assert (
        client.post(
            "/VerifyWebhook", json=envelope(headers=headers), headers=MESH_HEADERS
        ).status_code
        == 413
    )
    assert len(resolver.calls) == before


def test_raw_and_encoded_body_boundaries(configured: Configured) -> None:
    """Measure raw and encoded byte limits independently of Content-Length."""
    client, _, _ = configured
    prefix = b'{"events":[{"eventId":"a","eventType":"registry.push","padding":"'
    suffix = b'"}]}'
    raw = prefix + b"x" * (MAX_RAW_BYTES - len(prefix) - len(suffix)) + suffix
    assert len(raw) == MAX_RAW_BYTES
    assert (
        client.post("/VerifyWebhook", json=envelope(raw), headers=MESH_HEADERS).status_code == 200
    )
    assert (
        client.post("/VerifyWebhook", json=envelope(raw + b" "), headers=MESH_HEADERS).status_code
        == 413
    )
    small = encoded_envelope(RAW)
    encoded = small + b" " * (MAX_ENCODED_REQUEST_BYTES - len(small))
    assert client.post("/VerifyWebhook", content=encoded, headers=MESH_HEADERS).status_code == 200
    assert (
        client.post(
            "/VerifyWebhook",
            content=iter([encoded, b" "]),
            headers={**MESH_HEADERS, "content-length": "1"},
        ).status_code
        == 413
    )


def test_missing_credentials_and_unavailable_resolver_fail_closed(configured: Configured) -> None:
    """Neither absence nor resolver failure becomes a verification success or leaked detail."""
    client, resolver, _ = configured
    resolver.secrets.clear()
    assert client.post("/VerifyWebhook", json=envelope(), headers=MESH_HEADERS).status_code == 503
    resolver.failure = IdentityResolverError("private-upstream-detail")
    response = client.post("/VerifyWebhook", json=envelope(), headers=MESH_HEADERS)
    assert response.status_code == 503 and "private-upstream-detail" not in response.text


def test_signature_material_is_not_logged(
    configured: Configured, caplog: pytest.LogCaptureFixture
) -> None:
    """Malformed signature values are never copied into logs, responses or normalized envelopes."""
    client, _, _ = configured
    value = "unsupported-sensitive-signature"
    response = client.post(
        "/VerifyWebhook", json=envelope(headers={"x-custos-signature": value}), headers=MESH_HEADERS
    )
    assert response.status_code == 401
    assert value not in caplog.text and value not in response.text


def test_production_mesh_route_does_not_authorize_neighbor_routes() -> None:
    """Production verification uses authenticated mesh identity, not the user-context shim."""
    providers = memory_providers(RotatingResolver())
    production = replace(
        settings(), environment="production", authz_endpoint="https://auth.example"
    )
    with TestClient(create_app(settings=production, providers=providers)) as client:
        assert client.portal is not None
        client.portal.call(seed, providers, "a")
        response = client.post("/VerifyWebhook", json=envelope(), headers=MESH_HEADERS)
        assert response.status_code == 200
        assert client.get("/VerifyWebhook", headers=MESH_HEADERS).status_code == 401
        assert client.post("/VerifyWebhook/other", headers=MESH_HEADERS).status_code == 401
        assert client.post("/BindForStep", headers=MESH_HEADERS).status_code == 401


def test_configured_vendor_signature_header(configured: Configured) -> None:
    """Header selection comes from stored policy, never a request-selected algorithm."""
    client, resolver, providers = configured
    resolver.secrets["vendor"] = "unit-vendor"
    assert client.portal is not None
    client.portal.call(seed, providers, "vendor", "hmac-sha256", "X-Hub-Signature-256")
    supplied = signature(RAW, "unit-vendor")
    valid = envelope(workspace="vendor", headers={"X-Hub-Signature-256": supplied})
    assert client.post("/VerifyWebhook", json=valid, headers=MESH_HEADERS).status_code == 200
    wrong = envelope(workspace="vendor", headers={"x-custos-signature": supplied})
    assert client.post("/VerifyWebhook", json=wrong, headers=MESH_HEADERS).status_code == 401


def test_disabled_instance_and_storage_outage(
    configured: Configured, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Disabled pairs look absent; infrastructure failures remain retryable."""
    client, resolver, providers = configured
    assert client.portal is not None

    async def disable() -> None:
        """Persist lifecycle state through the existing provider mutation."""
        await providers.instance_store.patch_connector_instance(
            WorkspaceId("a"), ConnectorInstanceId("same"), {"enabled": False, "status": "disabled"}
        )

    client.portal.call(disable)
    assert client.post("/VerifyWebhook", json=envelope(), headers=MESH_HEADERS).status_code == 404
    assert not resolver.calls

    async def unavailable(workspace_id: WorkspaceId, instance_id: ConnectorInstanceId) -> None:
        """Simulate an explicit provider outage without weakening lookup scoping."""
        raise BackendUnavailable("unit outage")

    monkeypatch.setattr(providers.instance_store, "get_connector_instance", unavailable)
    assert client.post("/VerifyWebhook", json=envelope(), headers=MESH_HEADERS).status_code == 503


def test_missing_catalog_is_not_reported_as_an_unknown_pair(
    configured: Configured, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A known active pair with unavailable metadata is a retryable verifier failure."""
    client, resolver, providers = configured

    async def missing(connector_type: str, version: str) -> None:
        """Return the catalog's explicit absent-version result."""
        return None

    monkeypatch.setattr(providers.catalog_store, "get_connector_type_version", missing)
    assert client.post("/VerifyWebhook", json=envelope(), headers=MESH_HEADERS).status_code == 503
    assert not resolver.calls


@pytest.mark.parametrize(
    "patch",
    [
        {"schemaVersion": "2"},
        {"bodyBase64": "not-base64!"},
        {"workspaceId": " "},
        {"unexpected": True},
    ],
)
def test_invalid_private_wire_request(configured: Configured, patch: dict[str, object]) -> None:
    """Malformed wrappers fail before any credential or event processing."""
    client, resolver, _ = configured
    response = client.post("/VerifyWebhook", json={**envelope(), **patch}, headers=MESH_HEADERS)
    assert response.status_code == 400
    assert not resolver.calls

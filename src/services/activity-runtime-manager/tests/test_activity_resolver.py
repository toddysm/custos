"""Tests for the Activity Resolver (ARM-IMPL-007)."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

import httpx
import pytest

from custos_arm.contract import ErrorClass
from custos_arm.manifest import IsolationTier
from custos_arm.resolve import (
    ActivityRef,
    ActivityResolver,
    ActivityTypeVersion,
    ActivityUnresolvedError,
    CatalogActivityResolver,
    CatalogUnavailableError,
)

_CATALOG = "http://catalog.svc:8080"
_DIGEST = "sha256:" + "ab" * 32


def _manifest(*, version: str = "2.1.0", min_tier: str | None = "vm") -> dict[str, Any]:
    isolation: dict[str, Any] = {}
    if min_tier is not None:
        isolation["minTier"] = min_tier
    runtime: dict[str, Any] = {
        "kind": "oci-container",
        "image": "ghcr.io/acme/scan:2.1.0",
        "digest": "sha256:" + "cd" * 32,
    }
    if isolation:
        runtime["isolation"] = isolation
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "ActivityManifest",
        "metadata": {
            "type": "scan-image",
            "version": version,
            "namespace": "security",
            "description": "Scans an OCI image.",
            "owner": "team-sec",
        },
        "spec": {
            "contractVersion": "1",
            "runtime": runtime,
            "inputs": {"schema": {"type": "object", "properties": {"ref": {"type": "string"}}}},
            "outputs": {
                "schema": {"type": "object"},
                "artifacts": [{"name": "sbom", "mediaType": "application/json", "required": True}],
            },
            "connectors": [
                {
                    "name": "registry",
                    "type": "oci-registry",
                    "required": True,
                    "capabilities": ["oci.pull"],
                }
            ],
            "resources": {"timeout": "PT5M"},
        },
    }


def _body(*, version: str = "2.1.0", min_tier: str | None = "vm") -> dict[str, Any]:
    return {
        "namespace": "security",
        "type": "scan-image",
        "version": version,
        "digest": _DIGEST,
        "normalizedManifest": _manifest(version=version, min_tier=min_tier),
        "parentDeprecated": False,
        "publishedAt": "2025-01-01T12:00:00Z",
    }


_Handler = Callable[[httpx.Request], httpx.Response]


class _Counter:
    """Captures every request a MockTransport handler observes."""

    def __init__(self, handler: _Handler) -> None:
        self._handler = handler
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self._handler(request)


def _client(handler: _Handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _resolver(handler: _Handler) -> CatalogActivityResolver:
    return CatalogActivityResolver(_client(handler), catalog_endpoint=_CATALOG)


# ---------------------------------------------------------------------------
# ActivityRef grammar
# ---------------------------------------------------------------------------


def test_ref_parse_exact_pin() -> None:
    ref = ActivityRef.parse("security/scan-image@2.1.0")
    assert (ref.namespace, ref.type, ref.version) == ("security", "scan-image", "2.1.0")
    assert ref.is_exact_pin is True
    assert str(ref) == "security/scan-image@2.1.0"


def test_ref_parse_major() -> None:
    ref = ActivityRef.parse("security/scan-image@2")
    assert ref.version == "2"
    assert ref.is_exact_pin is False


@pytest.mark.parametrize(
    "bad",
    [
        "scan-image@2.1.0",  # missing namespace
        "security/scan-image",  # missing version
        "security/scan-image@latest",  # non-numeric version
        "security/scan-image@2.1",  # partial semver
        "a/b/c@1.0.0",  # too many segments
        "security/scan-image@01.0.0",  # leading zero
    ],
)
def test_ref_parse_invalid(bad: str) -> None:
    with pytest.raises(ValueError):
        ActivityRef.parse(bad)


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


async def test_resolve_exact_pin_returns_pinned_version() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_body())

    resolver = _resolver(handler)
    version = await resolver.resolve(
        workspace_id="security", activity_ref="security/scan-image@2.1.0"
    )

    assert isinstance(version, ActivityTypeVersion)
    assert version.digest == _DIGEST
    assert version.version == "2.1.0"
    assert version.runtime.digest == "sha256:" + "cd" * 32
    assert version.input_schema["properties"]["ref"]["type"] == "string"
    assert [a.name for a in version.outputs.artifacts] == ["sbom"]
    assert [c.name for c in version.connectors] == ["registry"]
    assert version.isolation_floor is IsolationTier.VM
    assert version.published_at == datetime.fromisoformat("2025-01-01T12:00:00+00:00")


async def test_resolve_builds_catalog_url() -> None:
    counter = _Counter(lambda _r: httpx.Response(200, json=_body()))
    resolver = _resolver(counter)
    await resolver.resolve(workspace_id="security", activity_ref="security/scan-image@2.1.0")

    assert len(counter.requests) == 1
    request = counter.requests[0]
    assert request.method == "GET"
    assert str(request.url) == (
        "http://catalog.svc:8080/v1/workspaces/security/activity-types/security/scan-image@2.1.0"
    )


async def test_unknown_ref_raises_activity_unresolved() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": {"code": "catalog.activity_type_not_found"}})

    resolver = _resolver(handler)
    with pytest.raises(ActivityUnresolvedError) as exc:
        await resolver.resolve(workspace_id="security", activity_ref="security/scan-image@9.9.9")
    assert exc.value.activity_ref == "security/scan-image@9.9.9"
    assert exc.value.code == "activity.unresolved"
    assert exc.value.error_class is ErrorClass.PERMANENT


async def test_malformed_ref_raises_activity_unresolved_without_http() -> None:
    counter = _Counter(lambda _r: httpx.Response(200, json=_body()))
    resolver = _resolver(counter)
    with pytest.raises(ActivityUnresolvedError):
        await resolver.resolve(workspace_id="security", activity_ref="not-a-ref")
    assert counter.requests == []


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------


async def test_exact_pin_is_cached() -> None:
    counter = _Counter(lambda _r: httpx.Response(200, json=_body()))
    resolver = _resolver(counter)
    first = await resolver.resolve(
        workspace_id="security", activity_ref="security/scan-image@2.1.0"
    )
    second = await resolver.resolve(
        workspace_id="security", activity_ref="security/scan-image@2.1.0"
    )

    assert first is second
    assert len(counter.requests) == 1


async def test_major_ref_is_not_cached() -> None:
    counter = _Counter(lambda _r: httpx.Response(200, json=_body()))
    resolver = _resolver(counter)
    await resolver.resolve(workspace_id="security", activity_ref="security/scan-image@2")
    await resolver.resolve(workspace_id="security", activity_ref="security/scan-image@2")

    assert len(counter.requests) == 2


# ---------------------------------------------------------------------------
# Transient failures
# ---------------------------------------------------------------------------


async def test_catalog_5xx_raises_unavailable() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="unavailable")

    resolver = _resolver(handler)
    with pytest.raises(CatalogUnavailableError):
        await resolver.resolve(workspace_id="security", activity_ref="security/scan-image@2.1.0")


async def test_transport_error_raises_unavailable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    resolver = _resolver(handler)
    with pytest.raises(CatalogUnavailableError):
        await resolver.resolve(workspace_id="security", activity_ref="security/scan-image@2.1.0")


async def test_invalid_manifest_raises_unavailable() -> None:
    bad = _body()
    bad["normalizedManifest"] = {"kind": "ActivityManifest"}  # missing apiVersion/metadata/spec

    resolver = _resolver(lambda _r: httpx.Response(200, json=bad))
    with pytest.raises(CatalogUnavailableError):
        await resolver.resolve(workspace_id="security", activity_ref="security/scan-image@2.1.0")


async def test_missing_field_raises_unavailable() -> None:
    bad = _body()
    del bad["digest"]

    resolver = _resolver(lambda _r: httpx.Response(200, json=bad))
    with pytest.raises(CatalogUnavailableError):
        await resolver.resolve(workspace_id="security", activity_ref="security/scan-image@2.1.0")


async def test_invalid_json_body_raises_unavailable() -> None:
    resolver = _resolver(lambda _r: httpx.Response(200, text="not-json{"))
    with pytest.raises(CatalogUnavailableError):
        await resolver.resolve(workspace_id="security", activity_ref="security/scan-image@2.1.0")


async def test_empty_digest_raises_unavailable() -> None:
    bad = _body()
    bad["digest"] = ""

    resolver = _resolver(lambda _r: httpx.Response(200, json=bad))
    with pytest.raises(CatalogUnavailableError):
        await resolver.resolve(workspace_id="security", activity_ref="security/scan-image@2.1.0")


async def test_invalid_type_version_raises_unavailable() -> None:
    bad = _body()
    bad["publishedAt"] = "not-a-timestamp"

    resolver = _resolver(lambda _r: httpx.Response(200, json=bad))
    with pytest.raises(CatalogUnavailableError):
        await resolver.resolve(workspace_id="security", activity_ref="security/scan-image@2.1.0")


async def test_non_object_payload_raises_unavailable() -> None:
    resolver = _resolver(lambda _r: httpx.Response(200, json=["not", "an", "object"]))
    with pytest.raises(CatalogUnavailableError):
        await resolver.resolve(workspace_id="security", activity_ref="security/scan-image@2.1.0")


# ---------------------------------------------------------------------------
# Resolved record + Protocol
# ---------------------------------------------------------------------------


async def test_isolation_floor_defaults_to_process_when_silent() -> None:
    resolver = _resolver(lambda _r: httpx.Response(200, json=_body(min_tier=None)))
    version = await resolver.resolve(
        workspace_id="security", activity_ref="security/scan-image@2.1.0"
    )
    assert version.isolation_floor is IsolationTier.PROCESS
    assert version.ref == ActivityRef.parse("security/scan-image@2.1.0")
    assert version.resources.timeout == "PT5M"


def test_resolver_satisfies_protocol() -> None:
    resolver = _resolver(lambda _r: httpx.Response(200, json=_body()))
    assert isinstance(resolver, ActivityResolver)

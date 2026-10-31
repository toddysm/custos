"""Content-addressed image launch resolution without mutable tags or credential redirects."""

from __future__ import annotations

import hashlib
import json

import httpx
import pytest

from custos_connector.hook_images import RegistryHookImageResolver
from custos_connector.runtime import PluginInvocationFailed


def document(value: object) -> tuple[str, bytes]:
    """Create an actual content-addressed OCI test document."""
    body = json.dumps(value, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(body).hexdigest(), body


@pytest.mark.parametrize("architecture", ["amd64", "arm64"])
@pytest.mark.parametrize(
    "origin,authority",
    [
        ("https://registry.example", "registry.example"),
        ("https://registry-internal", "registry-internal:443"),
        ("https://localhost", "localhost"),
        ("https://registry-internal:5443", "registry-internal:5443"),
        ("http://registry-internal", "registry-internal:80"),
    ],
)
async def test_resolves_platform_digest_and_original_entrypoint(
    architecture: str,
    origin: str,
    authority: str,
) -> None:
    """Verify every document while preserving ENTRYPOINT and overriding image CMD with the hook."""
    config_digest, config = document(
        {
            "architecture": architecture,
            "os": "linux",
            "config": {"Entrypoint": ["/plugin", "--safe"], "Cmd": ["ignored-default"]},
        }
    )
    image_digest, image = document(
        {
            "schemaVersion": 2,
            "config": {"digest": config_digest},
        }
    )
    index_digest, index = document(
        {
            "schemaVersion": 2,
            "manifests": [
                {"digest": image_digest, "platform": {"os": "linux", "architecture": architecture}}
            ],
        }
    )
    documents = {config_digest: config, image_digest: image, index_digest: index}

    def registry(request: httpx.Request) -> httpx.Response:
        """Serve only the configured origin's verified objects."""
        assert request.url.host == httpx.URL(origin).host
        assert request.headers["accept-encoding"] == "identity"
        assert request.url.path.startswith("/v2/team/plugin/")
        body = documents[request.url.path.rsplit("/", 1)[-1]]
        return httpx.Response(200, stream=httpx.ByteStream(body))

    async with httpx.AsyncClient(
        base_url=origin,
        transport=httpx.MockTransport(registry),
    ) as client:
        result = await RegistryHookImageResolver(client, architecture=architecture).resolve(
            "team/plugin@" + index_digest
        )
    assert result.reference == authority + "/team/plugin@" + image_digest
    assert result.entrypoint == ("/plugin", "--safe") and result.architecture == architecture


@pytest.mark.parametrize("failure", ["digest", "redirect", "encoding", "oversize", "status"])
async def test_registry_failures_do_not_change_trust_or_expose_payloads(failure: str) -> None:
    """Reject credential redirects, changed content and oversized responses."""
    digest, _body = document({"schemaVersion": 2, "config": {"digest": "sha256:" + "1" * 64}})

    def registry(request: httpx.Request) -> httpx.Response:
        """Return one controlled boundary failure."""
        assert request.url.host == "registry.example"
        if failure == "redirect":
            return httpx.Response(307, headers={"location": "https://other.invalid/private"})
        if failure == "status":
            return httpx.Response(403, content="private diagnostic")
        if failure == "encoding":
            return httpx.Response(200, headers={"content-encoding": "gzip"}, content=b"")
        response = b"private diagnostic" if failure == "digest" else b"x" * (1024 * 1024 + 1)
        return httpx.Response(200, stream=httpx.ByteStream(response))

    async with httpx.AsyncClient(
        base_url="https://registry.example",
        transport=httpx.MockTransport(registry),
    ) as client:
        with pytest.raises(PluginInvocationFailed) as error:
            await RegistryHookImageResolver(client, architecture="amd64").resolve(
                "plugin@" + digest
            )
    assert "private diagnostic" not in str(error.value)


async def test_invalid_platform_and_launch_configuration_fail_closed() -> None:
    """Do not treat invalid falsy ENTRYPOINT values as an absent entrypoint."""
    config_digest, config = document(
        {
            "architecture": "amd64",
            "os": "linux",
            "config": {"Entrypoint": False},
        }
    )
    digest, manifest = document({"schemaVersion": 2, "config": {"digest": config_digest}})

    def registry(request: httpx.Request) -> httpx.Response:
        """Serve a correctly hashed but invalid launch configuration."""
        return httpx.Response(
            200,
            stream=httpx.ByteStream(config if "/blobs/" in request.url.path else manifest),
        )

    async with httpx.AsyncClient(
        base_url="https://registry.example",
        transport=httpx.MockTransport(registry),
    ) as client:
        resolver = RegistryHookImageResolver(client, architecture="amd64")
        with pytest.raises(PluginInvocationFailed):
            await resolver.resolve("plugin@" + digest)
        with pytest.raises(PluginInvocationFailed):
            await resolver.resolve("plugin:mutable")

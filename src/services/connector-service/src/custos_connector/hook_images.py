"""Resolve immutable OCI hook launch metadata through the configured registry client."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any
from urllib.parse import quote

import httpx

from custos_connector.hook_sandbox import HookImage
from custos_connector.loader.errors import LoaderError
from custos_connector.loader.registry import Loader
from custos_connector.runtime import PluginInvocationFailed

_DIGEST = re.compile(r"sha256:[a-f0-9]{64}")
_ACCEPT = ", ".join(
    (
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
    )
)


class RegistryHookImageResolver:
    """Caller-owned registry transport; no arbitrary-origin authentication forwarding."""

    def __init__(self, client: httpx.AsyncClient, *, architecture: str) -> None:
        """Bind the same registry origin/authentication used for connector registration."""
        if architecture not in {"amd64", "arm64"}:
            raise ValueError("hook architecture must be amd64 or arm64")
        if not client.base_url.host or client.base_url.scheme not in {"https", "http"}:
            raise ValueError("hook image resolution requires a configured registry origin")
        if client.base_url.username or client.base_url.password:
            raise ValueError("registry authentication must not be embedded in its URL")
        if client.base_url.path not in ("", "/"):
            raise ValueError("hook registry must be an origin without a path prefix")
        self.client, self.architecture = client, architecture

    async def _document(
        self, repository: str, kind: str, digest: str, maximum: int
    ) -> dict[str, Any]:
        """Fetch bounded bytes, reject redirects and verify their content-addressed identity."""
        if _DIGEST.fullmatch(digest) is None:
            raise PluginInvocationFailed("unsupported hook image digest")
        path = f"/v2/{quote(repository, safe='/')}/{kind}/{digest}"
        try:
            async with self.client.stream(
                "GET",
                path,
                headers={
                    "Accept-Encoding": "identity",
                    **({"Accept": _ACCEPT} if kind == "manifests" else {}),
                },
                follow_redirects=False,
            ) as response:
                if response.status_code != 200:
                    raise PluginInvocationFailed("hook image metadata is unavailable")
                if response.headers.get("content-encoding", "identity") != "identity":
                    raise PluginInvocationFailed("encoded hook image metadata is unsupported")
                body = bytearray()
                async for chunk in response.aiter_raw(chunk_size=64 * 1024):
                    if len(body) + len(chunk) > maximum:
                        raise PluginInvocationFailed("hook image metadata exceeds its limit")
                    body.extend(chunk)
            if "sha256:" + hashlib.sha256(body).hexdigest() != digest:
                raise PluginInvocationFailed("hook image digest verification failed")
            value = json.loads(body)
            if not isinstance(value, dict):
                raise PluginInvocationFailed("invalid hook image metadata")
            return value
        except (httpx.HTTPError, ValueError, RecursionError) as exc:
            raise PluginInvocationFailed("hook image metadata could not be verified") from exc

    async def resolve(self, image_ref: str) -> HookImage:
        """Resolve the image, preserving ENTRYPOINT and replacing CMD with the hook."""
        try:
            repository, digest = Loader._parse_image_ref(image_ref)
        except LoaderError as exc:
            raise PluginInvocationFailed(
                "hook image requires an immutable registered reference"
            ) from exc
        host = self.client.base_url.host
        port = self.client.base_url.port
        authority = f"[{host}]" if ":" in host else host
        if self.client.base_url.scheme == "http":
            authority += f":{port or 80}"
        elif port not in (None, 443):
            authority += f":{port}"
        if not repository or any(part in {"", ".", ".."} for part in repository.split("/")):
            raise PluginInvocationFailed("invalid hook image repository")
        manifest = await self._document(repository, "manifests", digest, 1024 * 1024)
        if manifest.get("schemaVersion") != 2:
            raise PluginInvocationFailed("unsupported hook image manifest")
        if "manifests" in manifest:
            choices = manifest["manifests"]
            if not isinstance(choices, list):
                raise PluginInvocationFailed("invalid hook image index")
            candidates = [
                row
                for row in choices
                if isinstance(row, dict)
                and isinstance(row.get("platform"), dict)
                and row["platform"].get("os") == "linux"
                and row["platform"].get("architecture") == self.architecture
                and row["platform"].get("variant")
                in ((None, "", "v8") if self.architecture == "arm64" else (None, ""))
            ]
            if len(candidates) != 1 or not isinstance(candidates[0].get("digest"), str):
                raise PluginInvocationFailed("hook image platform is absent or ambiguous")
            digest = candidates[0]["digest"]
            manifest = await self._document(repository, "manifests", digest, 1024 * 1024)
        config = manifest.get("config")
        if not isinstance(config, dict) or not isinstance(config.get("digest"), str):
            raise PluginInvocationFailed("hook image has no verifiable launch configuration")
        document = await self._document(repository, "blobs", config["digest"], 4 * 1024 * 1024)
        if document.get("os") != "linux" or document.get("architecture") != self.architecture:
            raise PluginInvocationFailed("hook image configuration has the wrong platform")
        launch = document.get("config")
        if not isinstance(launch, dict):
            raise PluginInvocationFailed("hook image launch configuration is invalid")
        entrypoint = launch.get("Entrypoint")
        if entrypoint is None:
            entrypoint = []
        if not isinstance(entrypoint, list) or not all(
            isinstance(value, str) for value in entrypoint
        ):
            raise PluginInvocationFailed("hook image entrypoint is invalid")
        try:
            return HookImage(
                f"{authority}/{repository}@{digest}", tuple(entrypoint), self.architecture
            )
        except ValueError as exc:
            raise PluginInvocationFailed(
                "hook image launch configuration exceeds supported bounds"
            ) from exc

"""Activity Resolver — Catalog-backed ``activityRef`` resolution (ARM-IMPL-007).

The Scheduler hands the resolver a fully-qualified ``activityRef`` and the
caller's workspace; the resolver returns the pinned
:class:`~custos_arm.resolve.models.ActivityTypeVersion` read from the Catalog
Service over Dapr Service-Invocation.

The concrete :class:`CatalogActivityResolver` speaks the Catalog's public
``GET /v1/workspaces/{ws}/activity-types/{ref}`` contract through an injected,
lifespan-owned :class:`httpx.AsyncClient` (mirroring the Workflow Service's
outbound-RPC adapter precedent — the client is *not* owned here). The Catalog
base URL is the ``ARM_CATALOG_ENDPOINT`` value, which in production points at
the local Dapr sidecar's invoke path for the ``catalog`` app.

Immutability and caching: an exact-pin ref (``…@MAJOR.MINOR.PATCH``) names one
content-addressed version that can never change, so its resolution is cached
forever. A major ref (``…@MAJOR``) is a moving pointer and is always re-read.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

import httpx

from custos_arm.manifest import ManifestError, parse_manifest
from custos_arm.resolve.errors import (
    ActivityUnresolvedError,
    CatalogUnavailableError,
)
from custos_arm.resolve.models import ActivityRef, ActivityTypeVersion

__all__ = [
    "DEFAULT_RESOLVE_TIMEOUT_SECONDS",
    "ActivityResolver",
    "CatalogActivityResolver",
]

#: Default per-request timeout (seconds) against the Catalog. Matches the
#: Workflow Service outbound-RPC envelope (``10s``) — a Catalog read is bounded
#: by the same expected sidecar-latency budget.
DEFAULT_RESOLVE_TIMEOUT_SECONDS: float = 10.0


@runtime_checkable
class ActivityResolver(Protocol):
    """Resolves an ``activityRef`` to a pinned :class:`ActivityTypeVersion`."""

    async def resolve(self, *, workspace_id: str, activity_ref: str) -> ActivityTypeVersion:
        """Resolve ``activity_ref`` within ``workspace_id``.

        :raises ActivityUnresolvedError: the ref is unknown or malformed
            (permanent).
        :raises CatalogUnavailableError: the Catalog was unreachable or
            returned an unexpected status (transient).
        """
        ...


class CatalogActivityResolver:
    """Resolves activity refs against the Catalog Service over HTTP/Dapr.

    :param http_client: A lifespan-owned async client; not closed here.
    :param catalog_endpoint: The Catalog base URL (``ARM_CATALOG_ENDPOINT``).
    :param timeout: Per-request timeout in seconds.
    """

    def __init__(
        self,
        http_client: httpx.AsyncClient,
        *,
        catalog_endpoint: str,
        timeout: float = DEFAULT_RESOLVE_TIMEOUT_SECONDS,
    ) -> None:
        self._http = http_client
        self._base = catalog_endpoint.rstrip("/")
        self._timeout = timeout
        self._cache: dict[tuple[str, str], ActivityTypeVersion] = {}

    async def resolve(self, *, workspace_id: str, activity_ref: str) -> ActivityTypeVersion:
        try:
            ref = ActivityRef.parse(activity_ref)
        except ValueError as exc:
            # A malformed ref can never resolve — permanent.
            raise ActivityUnresolvedError(activity_ref, str(exc)) from exc

        cache_key = (workspace_id, str(ref))
        if ref.is_exact_pin:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached

        resolved = await self._fetch(workspace_id=workspace_id, ref=ref)

        # Only exact-pin resolutions are immutable; a major ref is a moving
        # pointer and must never be cached under the requested ref.
        if ref.is_exact_pin:
            self._cache[cache_key] = resolved
        return resolved

    async def _fetch(self, *, workspace_id: str, ref: ActivityRef) -> ActivityTypeVersion:
        url = f"{self._base}/v1/workspaces/{workspace_id}/activity-types/{ref}"
        try:
            response = await self._http.get(url, timeout=self._timeout)
        except httpx.HTTPError as exc:
            raise CatalogUnavailableError(
                str(ref), f"catalog request for {ref} failed: {exc}"
            ) from exc

        if response.status_code == httpx.codes.NOT_FOUND:
            raise ActivityUnresolvedError(str(ref))
        if response.status_code != httpx.codes.OK:
            raise CatalogUnavailableError(
                str(ref),
                f"catalog returned unexpected status {response.status_code} for {ref}",
            )

        return self._parse(ref=ref, payload=response.json())

    def _parse(self, *, ref: ActivityRef, payload: object) -> ActivityTypeVersion:
        if not isinstance(payload, dict):
            raise CatalogUnavailableError(
                str(ref), f"catalog response for {ref} is not a JSON object"
            )
        try:
            raw_manifest = payload["normalizedManifest"]
            digest = payload["digest"]
        except KeyError as exc:
            raise CatalogUnavailableError(
                str(ref), f"catalog response for {ref} is missing required field {exc}"
            ) from exc
        try:
            manifest = parse_manifest(raw_manifest)
        except ManifestError as exc:
            raise CatalogUnavailableError(
                str(ref), f"catalog returned an invalid manifest for {ref}: {exc}"
            ) from exc

        return ActivityTypeVersion(
            namespace=str(payload.get("namespace", ref.namespace)),
            type=str(payload.get("type", ref.type)),
            version=str(payload.get("version", ref.version)),
            digest=str(digest),
            manifest=manifest,
            parent_deprecated=bool(payload.get("parentDeprecated", False)),
            published_at=payload.get("publishedAt"),
        )

"""Connector-type catalog commands (DEVCLI-IMPL-005).

`register` reads a connector's `connector-manifest.json`, resolves a
digest-pinned GHCR image reference, and posts both to the gateway's
`POST /v1/catalog/connector-types`. `list_versions` walks
`GET /v1/catalog/connector-types?type=<type>` (per-type; the catalog lists
versions of a connector type, not all types). Both accept an injected
:class:`~custosctl.api.ApiClient` so the CLI tests can drive them over an
`httpx.MockTransport` without a live gateway.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from custosctl import imageref
from custosctl.api import ApiClient, build_client
from custosctl.config import Settings

_MANIFEST_NAME = "connector-manifest.json"
_CONNECTOR_TYPES_PATH = "/v1/catalog/connector-types"


def register(
    settings: Settings,
    *,
    path: str,
    image_ref: str | None = None,
    client: ApiClient | None = None,
) -> dict[str, Any]:
    """Register the connector-type described by ``path``.

    ``path`` is an extension directory (containing ``connector-manifest.json``)
    or the manifest file itself. ``image_ref`` overrides the derived image; it
    must be digest-pinned. Returns the registered ``{type, version, digest}``.
    """
    name, manifest = _load_manifest(Path(path))
    _, version = _manifest_meta(manifest)
    _, ref = imageref.resolve_image_ref(settings, name=name, version=version, image_ref=image_ref)

    owns = client is None
    client = client or build_client(settings)
    try:
        body = client.post(
            _CONNECTOR_TYPES_PATH,
            json={"imageRef": ref, "manifest": manifest},
        )
    finally:
        if owns:
            client.close()
    if not isinstance(body, dict):
        raise RuntimeError("unexpected register response (expected a JSON object)")
    return body


def list_versions(
    settings: Settings,
    *,
    connector_type: str,
    client: ApiClient | None = None,
) -> list[dict[str, Any]]:
    """Return all registered versions of ``connector_type`` (following pages)."""
    owns = client is None
    client = client or build_client(settings)
    items: list[dict[str, Any]] = []
    cursor: str | None = None
    try:
        while True:
            params: dict[str, Any] = {"type": connector_type}
            if cursor:
                params["cursor"] = cursor
            page = client.get(_CONNECTOR_TYPES_PATH, params=params)
            if not isinstance(page, dict):
                raise RuntimeError("unexpected list response (expected a JSON object)")
            page_items = page.get("items", [])
            if not isinstance(page_items, list):
                raise RuntimeError("unexpected list response ('items' is not a list)")
            for item in page_items:
                if not isinstance(item, dict):
                    raise RuntimeError("unexpected list response (item is not an object)")
                items.append(item)
            next_cursor = page.get("nextCursor")
            cursor = next_cursor if isinstance(next_cursor, str) else None
            if not cursor:
                break
    finally:
        if owns:
            client.close()
    return items


def _load_manifest(path: Path) -> tuple[str, dict[str, Any]]:
    if path.is_dir():
        manifest_file = path / _MANIFEST_NAME
        name = path.name
    else:
        manifest_file = path
        name = path.parent.name
    if not manifest_file.is_file():
        raise RuntimeError(f"connector manifest not found: {manifest_file}")
    try:
        data = json.loads(manifest_file.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"could not read connector manifest {manifest_file}: {exc}") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"connector manifest {manifest_file} is not a JSON object")
    return name, data


def _manifest_meta(manifest: dict[str, Any]) -> tuple[str, str]:
    meta = manifest.get("metadata", {})
    type_ = meta.get("type") if isinstance(meta, dict) else None
    version = meta.get("version") if isinstance(meta, dict) else None
    if not type_ or not version:
        raise RuntimeError("connector manifest is missing metadata.type / metadata.version")
    return str(type_), str(version)


__all__ = ["list_versions", "register"]

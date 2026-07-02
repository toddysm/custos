"""Activity-type catalog commands (DEVCLI-IMPL-006).

`register` reads an activity's YAML `activity-manifest.yaml`, resolves a
digest-pinned GHCR reference, injects the resolved image + digest into
`spec.runtime` (the on-disk manifest carries a placeholder digest), and posts
`{manifest, referrerRef}` to `POST /v1/workspaces/<ws>/activity-types`.
`list_versions` walks `GET /v1/workspaces/<ws>/activity-types?namespace=&type=`.

Both accept an injected :class:`~custosctl.api.ApiClient` so the tests drive
them over an `httpx.MockTransport` without a live gateway. The workspace
defaults to the manifest's `metadata.namespace` (e.g. `custos.builtin`).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from custosctl import imageref
from custosctl.api import ApiClient, build_client
from custosctl.config import Settings

_MANIFEST_NAME = "activity-manifest.yaml"


def _types_path(workspace: str) -> str:
    return f"/v1/workspaces/{workspace}/activity-types"


def register(
    settings: Settings,
    *,
    path: str,
    image_ref: str | None = None,
    workspace: str | None = None,
    client: ApiClient | None = None,
) -> dict[str, Any]:
    """Register the activity-type described by ``path``.

    ``path`` is an extension directory (containing ``activity-manifest.yaml``)
    or the manifest file. Returns the registered
    ``{namespace, type, version, digest}``.
    """
    name, manifest = _load_manifest(Path(path))
    namespace, _, version = _manifest_meta(manifest)
    image, ref = imageref.resolve_image_ref(
        settings, name=name, version=version, image_ref=image_ref
    )
    _inject_runtime(manifest, image=image, ref=ref)

    ws = workspace or namespace
    owns = client is None
    client = client or build_client(settings)
    try:
        body = client.post(
            _types_path(ws),
            json={"manifest": manifest, "referrerRef": ref},
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
    namespace: str,
    activity_type: str,
    workspace: str | None = None,
    client: ApiClient | None = None,
) -> list[dict[str, Any]]:
    """Return all registered versions of ``namespace/activity_type``."""
    ws = workspace or namespace
    owns = client is None
    client = client or build_client(settings)
    items: list[dict[str, Any]] = []
    cursor: str | None = None
    try:
        while True:
            params: dict[str, Any] = {"namespace": namespace, "type": activity_type}
            if cursor:
                params["cursor"] = cursor
            page = client.get(_types_path(ws), params=params)
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
        raise RuntimeError(f"activity manifest not found: {manifest_file}")
    try:
        data = yaml.safe_load(manifest_file.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise RuntimeError(f"could not read activity manifest {manifest_file}: {exc}") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"activity manifest {manifest_file} is not a mapping")
    return name, data


def _manifest_meta(manifest: dict[str, Any]) -> tuple[str, str, str]:
    meta = manifest.get("metadata", {})
    if not isinstance(meta, dict):
        raise RuntimeError("activity manifest metadata is not a mapping")
    namespace = meta.get("namespace")
    type_ = meta.get("type")
    version = meta.get("version")
    if not namespace or not type_ or not version:
        raise RuntimeError(
            "activity manifest is missing metadata.namespace / metadata.type / metadata.version"
        )
    return str(namespace), str(type_), str(version)


def _inject_runtime(manifest: dict[str, Any], *, image: str, ref: str) -> None:
    """Set the resolved image + digest in ``spec.runtime`` (placeholder on disk)."""
    spec = manifest.get("spec")
    if not isinstance(spec, dict):
        raise RuntimeError("activity manifest is missing a 'spec' mapping")
    runtime = spec.get("runtime")
    if not isinstance(runtime, dict):
        raise RuntimeError("activity manifest is missing 'spec.runtime'")
    runtime["image"] = image
    runtime["digest"] = ref.split("@", 1)[1]


__all__ = ["list_versions", "register"]

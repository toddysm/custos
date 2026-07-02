"""Tests for the connector-type catalog commands (DEVCLI-IMPL-005)."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

from custosctl import connectors, shell
from custosctl.api import ApiClient
from custosctl.config import Settings, Target

Handler = Callable[[httpx.Request], httpx.Response]
_DIGEST = "sha256:" + "a" * 64


def _client(handler: Handler) -> ApiClient:
    return ApiClient(
        base_url="https://gw.example",
        token="cst_x",
        transport=httpx.MockTransport(handler),
    )


def _settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "target": Target.REMOTE,
        "gateway": "https://gw.example",
        "token": "cst_x",
        "image_prefix": "ghcr.io/acme/custos",
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


def _connector_dir(tmp_path: Path, *, name: str = "dockerhub", version: str = "1.2.0") -> Path:
    d = tmp_path / name
    d.mkdir()
    (d / "connector-manifest.json").write_text(
        json.dumps(
            {
                "apiVersion": "custos.dev/connector-manifest/v1",
                "kind": "ConnectorManifest",
                "metadata": {"type": name, "version": version},
                "spec": {"capabilities": ["oci.pull"]},
            }
        )
    )
    return d


def test_register_derives_ref_and_posts_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(shell, "resolve_image_digest", lambda ref: _DIGEST)
    d = _connector_dir(tmp_path)
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content)
        return httpx.Response(
            201, json={"type": "dockerhub", "version": "1.2.0", "digest": _DIGEST}
        )

    with _client(handler) as client:
        ref = connectors.register(_settings(), path=str(d), client=client)

    assert ref == {"type": "dockerhub", "version": "1.2.0", "digest": _DIGEST}
    assert seen["path"] == "/v1/catalog/connector-types"
    body = seen["body"]
    assert isinstance(body, dict)
    assert body["imageRef"] == f"ghcr.io/acme/custos/dockerhub:v1.2.0@{_DIGEST}"
    assert body["manifest"]["metadata"]["type"] == "dockerhub"


def test_register_accepts_manifest_file_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(shell, "resolve_image_digest", lambda ref: _DIGEST)
    d = _connector_dir(tmp_path, name="ghcr", version="0.1.0")
    manifest_file = d / "connector-manifest.json"
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = json.loads(request.content)
        return httpx.Response(201, json={"type": "ghcr", "version": "0.1.0", "digest": _DIGEST})

    with _client(handler) as client:
        connectors.register(_settings(), path=str(manifest_file), client=client)
    # name is derived from the parent directory when a file path is given.
    assert seen["body"]["imageRef"].startswith("ghcr.io/acme/custos/ghcr:v0.1.0@")  # type: ignore[index]


def test_register_explicit_image_ref_skips_digest_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _boom(ref: str) -> str:
        raise AssertionError("resolve_image_digest should not be called")

    monkeypatch.setattr(shell, "resolve_image_digest", _boom)
    d = _connector_dir(tmp_path)
    pinned = f"ghcr.io/acme/custos/dockerhub:v1.2.0@{_DIGEST}"
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = json.loads(request.content)
        return httpx.Response(
            201, json={"type": "dockerhub", "version": "1.2.0", "digest": _DIGEST}
        )

    with _client(handler) as client:
        connectors.register(_settings(), path=str(d), image_ref=pinned, client=client)
    assert seen["body"]["imageRef"] == pinned  # type: ignore[index]


def test_register_rejects_untagged_image_ref(tmp_path: Path) -> None:
    d = _connector_dir(tmp_path)

    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - not reached
        return httpx.Response(201, json={})

    with _client(handler) as client, pytest.raises(RuntimeError, match="digest-pinned"):
        connectors.register(
            _settings(),
            path=str(d),
            image_ref="ghcr.io/acme/custos/dockerhub:v1.2.0",
            client=client,
        )


def test_register_rejects_malformed_digest(tmp_path: Path) -> None:
    d = _connector_dir(tmp_path)

    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - not reached
        return httpx.Response(201, json={})

    with _client(handler) as client, pytest.raises(RuntimeError, match="digest-pinned"):
        connectors.register(
            _settings(),
            path=str(d),
            image_ref="ghcr.io/acme/x@sha256:abc",
            client=client,
        )


def test_register_manifest_not_found(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - not reached
        return httpx.Response(201, json={})

    with (
        _client(handler) as client,
        pytest.raises(RuntimeError, match="connector manifest not found"),
    ):
        connectors.register(_settings(), path=str(tmp_path / "missing"), client=client)


def test_register_manifest_missing_metadata(tmp_path: Path) -> None:
    d = tmp_path / "bad"
    d.mkdir()
    (d / "connector-manifest.json").write_text(json.dumps({"metadata": {}}))

    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - not reached
        return httpx.Response(201, json={})

    with _client(handler) as client, pytest.raises(RuntimeError, match=r"metadata\.type"):
        connectors.register(_settings(), path=str(d), client=client)


def test_list_versions_follows_pages() -> None:
    calls: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        calls.append(params)
        if "cursor" not in params:
            return httpx.Response(
                200,
                json={
                    "items": [{"type": "dockerhub", "version": "1.0.0", "digest": "sha256:1"}],
                    "nextCursor": "c2",
                },
            )
        return httpx.Response(
            200,
            json={
                "items": [{"type": "dockerhub", "version": "1.1.0", "digest": "sha256:2"}],
                "nextCursor": None,
            },
        )

    with _client(handler) as client:
        items = connectors.list_versions(_settings(), connector_type="dockerhub", client=client)

    assert [i["version"] for i in items] == ["1.0.0", "1.1.0"]
    assert calls[0]["type"] == "dockerhub"
    assert calls[1]["cursor"] == "c2"


def test_list_versions_rejects_non_list_items() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"items": "not-a-list"})

    with _client(handler) as client, pytest.raises(RuntimeError, match="not a list"):
        connectors.list_versions(_settings(), connector_type="dockerhub", client=client)

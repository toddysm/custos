"""Tests for the activity-type catalog commands (DEVCLI-IMPL-006)."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import httpx
import pytest
import yaml

from custosctl import activities, shell
from custosctl.api import ApiClient
from custosctl.config import Settings, Target

Handler = Callable[[httpx.Request], httpx.Response]
_DIGEST = "sha256:" + "b" * 64


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


def _activity_dir(
    tmp_path: Path,
    *,
    name: str = "copy-image",
    version: str = "0.1.0",
    namespace: str = "custos.builtin",
) -> Path:
    d = tmp_path / name
    d.mkdir()
    (d / "activity-manifest.yaml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "custos.dev/v1",
                "kind": "ActivityManifest",
                "metadata": {"type": name, "version": version, "namespace": namespace},
                "spec": {
                    "runtime": {
                        "kind": "oci-container",
                        "image": f"ghcr.io/x/{name}:{version}",
                        "digest": "sha256:" + "0" * 64,
                    }
                },
            }
        )
    )
    return d


def test_register_injects_runtime_and_posts_to_namespace_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(shell, "resolve_image_digest", lambda ref: _DIGEST)
    d = _activity_dir(tmp_path)
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content)
        return httpx.Response(
            201,
            json={
                "namespace": "custos.builtin",
                "type": "copy-image",
                "version": "0.1.0",
                "digest": _DIGEST,
            },
        )

    with _client(handler) as client:
        ref = activities.register(_settings(), path=str(d), client=client)

    assert ref["type"] == "copy-image"
    # workspace defaults to the manifest namespace.
    assert seen["path"] == "/v1/workspaces/custos.builtin/activity-types"
    body = seen["body"]
    assert isinstance(body, dict)
    assert body["referrerRef"] == f"ghcr.io/acme/custos/copy-image:v0.1.0@{_DIGEST}"
    # the resolved image + digest are injected into spec.runtime.
    assert body["manifest"]["spec"]["runtime"]["image"] == "ghcr.io/acme/custos/copy-image:v0.1.0"
    assert body["manifest"]["spec"]["runtime"]["digest"] == _DIGEST


def test_register_workspace_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shell, "resolve_image_digest", lambda ref: _DIGEST)
    d = _activity_dir(tmp_path)
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        return httpx.Response(
            201,
            json={
                "namespace": "custos.builtin",
                "type": "copy-image",
                "version": "0.1.0",
                "digest": _DIGEST,
            },
        )

    with _client(handler) as client:
        activities.register(_settings(), path=str(d), workspace="ws-1", client=client)
    assert seen["path"] == "/v1/workspaces/ws-1/activity-types"


def test_register_explicit_image_ref(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(ref: str) -> str:
        raise AssertionError("resolve_image_digest should not be called")

    monkeypatch.setattr(shell, "resolve_image_digest", _boom)
    d = _activity_dir(tmp_path)
    pinned = f"ghcr.io/acme/custos/copy-image:v0.1.0@{_DIGEST}"
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        seen["body"] = json.loads(request.content)
        return httpx.Response(
            201,
            json={
                "namespace": "custos.builtin",
                "type": "copy-image",
                "version": "0.1.0",
                "digest": _DIGEST,
            },
        )

    with _client(handler) as client:
        activities.register(_settings(), path=str(d), image_ref=pinned, client=client)
    body = seen["body"]
    assert isinstance(body, dict)
    assert body["referrerRef"] == pinned
    assert body["manifest"]["spec"]["runtime"]["digest"] == _DIGEST


def test_register_manifest_not_found(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - not reached
        return httpx.Response(201, json={})

    with (
        _client(handler) as client,
        pytest.raises(RuntimeError, match="activity manifest not found"),
    ):
        activities.register(_settings(), path=str(tmp_path / "missing"), client=client)


def test_register_missing_metadata(tmp_path: Path) -> None:
    d = tmp_path / "bad"
    d.mkdir()
    (d / "activity-manifest.yaml").write_text(
        yaml.safe_dump({"metadata": {"type": "x"}, "spec": {}})
    )

    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - not reached
        return httpx.Response(201, json={})

    with _client(handler) as client, pytest.raises(RuntimeError, match=r"metadata\.namespace"):
        activities.register(_settings(), path=str(d), client=client)


def test_register_missing_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shell, "resolve_image_digest", lambda ref: _DIGEST)
    d = tmp_path / "copy-image"
    d.mkdir()
    (d / "activity-manifest.yaml").write_text(
        yaml.safe_dump(
            {
                "metadata": {
                    "type": "copy-image",
                    "version": "0.1.0",
                    "namespace": "custos.builtin",
                },
                "spec": {},
            }
        )
    )

    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - not reached
        return httpx.Response(201, json={})

    with _client(handler) as client, pytest.raises(RuntimeError, match=r"spec\.runtime"):
        activities.register(_settings(), path=str(d), client=client)


def test_list_versions_queries_namespace_and_type(tmp_path: Path) -> None:
    calls: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(dict(request.url.params))
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "namespace": "custos.builtin",
                        "type": "copy-image",
                        "version": "0.1.0",
                        "digest": "sha256:1",
                    }
                ],
                "nextCursor": None,
            },
        )

    with _client(handler) as client:
        items = activities.list_versions(
            _settings(), namespace="custos.builtin", activity_type="copy-image", client=client
        )

    assert [i["version"] for i in items] == ["0.1.0"]
    assert calls[0]["namespace"] == "custos.builtin"
    assert calls[0]["type"] == "copy-image"

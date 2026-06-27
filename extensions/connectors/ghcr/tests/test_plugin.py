"""Unit tests for the GHCR connector plugin hook handlers.

Exercises the handlers directly through :func:`handle` so they run fast
without the Dockerized entry point (covered in :mod:`test_main_entry`)
and without live network I/O (``health`` delegates to ``probe``, which is
monkeypatched here and tested for real in :mod:`test_probe`).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import jsonschema
import pytest

from ghcr_plugin import handle
from ghcr_plugin.plugin import PluginError

_PLUGIN_DIR = Path(__file__).resolve().parents[1]
_REPO_ROOT = Path(__file__).resolve().parents[4]
_SCHEMA = (
    _REPO_ROOT
    / "design"
    / "components"
    / "connector-service"
    / "schemas"
    / "connector-manifest.v1.schema.json"
)


def _request(hook: str, *, hook_input: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build a v1 request envelope matching the runtime's wire format."""
    return {
        "apiVersion": 1,
        "hook": hook,
        "connector": {
            "type": "custos-ghcr",
            "version": "0.1.0",
            "imageRef": "ghcr.io/example/custos-ghcr:0.1.0",
            "digest": "sha256:" + "a" * 64,
            "manifest": {
                "spec": {
                    "target": {
                        "kind": "oci-registry",
                        "endpoint": "https://ghcr.io",
                        "verifyTls": True,
                    }
                }
            },
        },
        "instance": {
            "workspaceId": "ws-1",
            "instanceId": "inst-1",
            "type": "custos-ghcr",
            "version": "0.1.0",
            "name": "prod",
            "enabled": True,
            "status": "active",
            "healthStatus": "unknown",
            "leaseTtlSeconds": 3600,
            "targetConfig": {"repositoryNamespace": "acme"},
            "credentialsAuthentication": {
                "secretName": "ghcr-pat",
                "usernameKey": "username",
                "tokenKey": "token",
                "namespace": "custos-connectors",
            },
            "usedCapabilities": ["oci.pull", "oci.push"],
        },
        "input": hook_input or {},
    }


# ---------------------------------------------------------------------------
# manifest
# ---------------------------------------------------------------------------


def test_manifest_validates_against_connector_manifest_v1() -> None:
    manifest = json.loads((_PLUGIN_DIR / "connector-manifest.json").read_text())
    schema = json.loads(_SCHEMA.read_text())
    jsonschema.validate(instance=manifest, schema=schema)


def test_manifest_declares_x_dapr_secret_and_ghcr_target() -> None:
    manifest = json.loads((_PLUGIN_DIR / "connector-manifest.json").read_text())
    spec = manifest["spec"]
    assert manifest["metadata"]["type"] == "custos-ghcr"
    assert spec["credentials"]["authenticationType"] == "x-dapr-secret"
    assert spec["target"]["endpoint"] == "https://ghcr.io"
    assert set(spec["capabilities"]) == {
        "oci.pull",
        "oci.push",
        "oci.list-tags",
        "oci.list-referrers",
    }


# ---------------------------------------------------------------------------
# bind
# ---------------------------------------------------------------------------


def test_bind_returns_v2_endpoint_under_namespace() -> None:
    response = handle(
        "bind", _request("bind", hook_input={"slot": "source", "capability": "oci.pull"})
    )
    assert response["ok"] is True
    result = response["result"]
    assert result["endpoint"] == "https://ghcr.io/v2/acme"
    assert result["tokenTypeHint"] == "basic"
    assert result["handle"]["slot"] == "source"
    assert result["handle"]["capability"] == "oci.pull"
    assert result["handle"]["instanceId"] == "inst-1"
    assert result["extras"]["registryKind"] == "oci-registry"
    assert result["extras"]["registryProvider"] == "ghcr"
    assert result["extras"]["tokenEndpoint"] == "https://ghcr.io/token"
    assert result["extras"]["service"] == "ghcr.io"
    assert result["extras"]["verifyTls"] is True


def test_bind_without_namespace_falls_back_to_bare_v2() -> None:
    request = _request("bind", hook_input={"slot": "sink", "capability": "oci.push"})
    request["instance"]["targetConfig"] = {}
    result = handle("bind", request)["result"]
    assert result["endpoint"] == "https://ghcr.io/v2"


def test_bind_rejects_unadvertised_capability() -> None:
    with pytest.raises(PluginError) as excinfo:
        handle("bind", _request("bind", hook_input={"slot": "source", "capability": "s3.read"}))
    assert excinfo.value.code == "invalid-response"
    assert "s3.read" in excinfo.value.detail


def test_bind_requires_slot_and_capability() -> None:
    with pytest.raises(PluginError):
        handle("bind", _request("bind", hook_input={"capability": "oci.pull"}))
    with pytest.raises(PluginError):
        handle("bind", _request("bind", hook_input={"slot": "source"}))


def test_bind_missing_endpoint_is_upstream_unreachable() -> None:
    request = _request("bind", hook_input={"slot": "source", "capability": "oci.pull"})
    request["connector"]["manifest"]["spec"]["target"]["endpoint"] = ""
    with pytest.raises(PluginError) as excinfo:
        handle("bind", request)
    assert excinfo.value.code == "upstream-unreachable"


def test_bind_rejects_non_ghcr_endpoint() -> None:
    request = _request("bind", hook_input={"slot": "source", "capability": "oci.pull"})
    request["connector"]["manifest"]["spec"]["target"]["endpoint"] = "https://evil.example.com"
    with pytest.raises(PluginError) as excinfo:
        handle("bind", request)
    assert excinfo.value.code == "invalid-response"
    assert excinfo.value.detail.startswith("ghcr connector only targets")
    assert "refusing endpoint" in excinfo.value.detail


def test_bind_rejects_non_https_endpoint() -> None:
    request = _request("bind", hook_input={"slot": "source", "capability": "oci.pull"})
    request["connector"]["manifest"]["spec"]["target"]["endpoint"] = "http://ghcr.io"
    with pytest.raises(PluginError) as excinfo:
        handle("bind", request)
    assert excinfo.value.code == "invalid-response"


def test_bind_rejects_endpoint_with_path_or_userinfo() -> None:
    for bad in ("https://ghcr.io/foo", "https://user@ghcr.io", "https://ghcr.io:8443"):
        request = _request("bind", hook_input={"slot": "source", "capability": "oci.pull"})
        request["connector"]["manifest"]["spec"]["target"]["endpoint"] = bad
        with pytest.raises(PluginError) as excinfo:
            handle("bind", request)
        assert excinfo.value.code == "invalid-response", bad


def test_bind_normalizes_trailing_slash_endpoint() -> None:
    request = _request("bind", hook_input={"slot": "source", "capability": "oci.pull"})
    request["connector"]["manifest"]["spec"]["target"]["endpoint"] = "https://ghcr.io/"
    result = handle("bind", request)["result"]
    assert result["endpoint"] == "https://ghcr.io/v2/acme"


# ---------------------------------------------------------------------------
# health
# ---------------------------------------------------------------------------


def test_health_delegates_to_probe_and_wraps_result(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_probe(endpoint: str, *, verify_tls: bool = True, **_: Any) -> dict[str, Any]:
        captured["endpoint"] = endpoint
        captured["verify_tls"] = verify_tls
        return {
            "healthy": True,
            "detail": "ok",
            "registryEndpoint": "https://ghcr.io/v2/",
            "tokenEndpoint": "https://ghcr.io/token",
            "service": "ghcr.io",
        }

    monkeypatch.setattr("ghcr_plugin.plugin.probe.check_reachability", fake_probe)
    result = handle("health", _request("health"))["result"]
    assert result["healthy"] is True
    assert result["detail"] == "ok"
    assert "checkedAt" in result
    assert result["extras"]["instanceId"] == "inst-1"
    assert result["extras"]["tokenEndpoint"] == "https://ghcr.io/token"
    assert captured == {"endpoint": "https://ghcr.io", "verify_tls": True}


def test_health_unhealthy_when_probe_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "ghcr_plugin.plugin.probe.check_reachability",
        lambda *_a, **_k: {"healthy": False, "detail": "registry unreachable: ConnectError"},
    )
    result = handle("health", _request("health"))["result"]
    assert result["healthy"] is False
    assert "unreachable" in result["detail"]


def test_health_missing_endpoint_is_unhealthy_without_probe() -> None:
    request = _request("health")
    request["connector"]["manifest"]["spec"]["target"]["endpoint"] = ""
    result = handle("health", request)["result"]
    assert result["healthy"] is False
    assert "endpoint" in result["detail"]


def test_health_rejects_non_ghcr_endpoint_without_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(*_a: Any, **_k: Any) -> dict[str, Any]:
        raise AssertionError("probe must not be called for a non-GHCR endpoint")

    monkeypatch.setattr("ghcr_plugin.plugin.probe.check_reachability", boom)
    request = _request("health")
    request["connector"]["manifest"]["spec"]["target"]["endpoint"] = "https://evil.example.com"
    result = handle("health", request)["result"]
    assert result["healthy"] is False
    assert "refusing endpoint" in result["detail"]


# ---------------------------------------------------------------------------
# dispatch
# ---------------------------------------------------------------------------


def test_unknown_hook_is_rejected() -> None:
    with pytest.raises(PluginError) as excinfo:
        handle("listen", _request("listen"))
    assert excinfo.value.code == "invalid-response"


def test_unsupported_api_version_is_rejected() -> None:
    request = _request("bind", hook_input={"slot": "source", "capability": "oci.pull"})
    request["apiVersion"] = 2
    with pytest.raises(PluginError):
        handle("bind", request)

"""Unit tests for the reference OCI-registry plugin.

These exercise the hook handlers directly through :func:`handle` so
they run fast without going through the Dockerized entry point. The
``__main__`` entry point is covered separately in
:mod:`test_main_entry` to assert the stdin/stdout JSON contract is
preserved end-to-end.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from oci_registry_plugin import handle
from oci_registry_plugin.plugin import PluginError


def _request(hook: str, *, hook_input: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build a v1 request envelope matching the runtime's wire format."""
    return {
        "apiVersion": 1,
        "hook": hook,
        "connector": {
            "type": "custos-oci-registry",
            "version": "1.0.0",
            "imageRef": "ghcr.io/example/custos-oci-registry:1.0.0",
            "digest": "sha256:" + "a" * 64,
            "manifest": {
                "spec": {
                    "target": {
                        "kind": "oci-registry",
                        "endpoint": "https://registry.example.com",
                        "verifyTls": True,
                    }
                }
            },
        },
        "instance": {
            "workspaceId": "ws-1",
            "instanceId": "inst-1",
            "type": "custos-oci-registry",
            "version": "1.0.0",
            "name": "prod",
            "enabled": True,
            "status": "active",
            "healthStatus": "unknown",
            "leaseTtlSeconds": 600,
            "targetConfig": {"repositoryNamespace": "team-a"},
            "credentialsAuthentication": {
                "vaultUri": "https://sample-akv.vault.azure.net",
                "secretName": "registry-token",
            },
            "usedCapabilities": ["oci.pull", "oci.push"],
        },
        "input": hook_input or {},
    }


def test_bind_returns_v2_endpoint_under_namespace() -> None:
    response = handle(
        "bind", _request("bind", hook_input={"slot": "source", "capability": "oci.pull"})
    )
    assert response["ok"] is True
    result = response["result"]
    assert result["endpoint"] == "https://registry.example.com/v2/team-a"
    assert result["tokenTypeHint"] == "bearer"
    assert result["handle"]["slot"] == "source"
    assert result["extras"]["registryKind"] == "oci-registry"
    assert result["extras"]["verifyTls"] is True


def test_bind_strips_trailing_slash_when_namespace_absent() -> None:
    request = _request("bind", hook_input={"slot": "sink", "capability": "oci.push"})
    request["instance"]["targetConfig"] = {}
    response = handle("bind", request)
    assert response["result"]["endpoint"] == "https://registry.example.com/v2"


def test_bind_requires_slot_and_capability() -> None:
    with pytest.raises(PluginError) as exc:
        handle("bind", _request("bind", hook_input={"slot": "", "capability": "oci.pull"}))
    assert exc.value.code == "invalid-response"

    with pytest.raises(PluginError) as exc:
        handle("bind", _request("bind", hook_input={"slot": "source", "capability": ""}))
    assert exc.value.code == "invalid-response"


def test_bind_rejects_unadvertised_capability() -> None:
    """A bind request for a capability the manifest does not advertise
    must surface as an invalid-response error rather than silently
    handing back a registry endpoint.
    """
    request = _request("bind", hook_input={"slot": "source", "capability": "s3.read"})
    with pytest.raises(PluginError) as exc:
        handle("bind", request)
    assert exc.value.code == "invalid-response"
    assert "s3.read" in exc.value.detail


def test_bind_fails_when_manifest_endpoint_missing() -> None:
    request = _request("bind", hook_input={"slot": "source", "capability": "oci.pull"})
    request["connector"]["manifest"]["spec"]["target"]["endpoint"] = ""
    with pytest.raises(PluginError) as exc:
        handle("bind", request)
    assert exc.value.code == "upstream-unreachable"


def test_listen_pull_initial_cursor() -> None:
    response = handle("listen", _request("listen", hook_input={"mode": "pull", "cursor": None}))
    result = response["result"]
    assert len(result["events"]) == 1
    event = result["events"][0]
    assert event["eventType"] == "oci.image.pushed"
    assert event["subject"]["tag"] == "v0"
    assert result["nextCursor"] == {
        "encoding": "oci-list-tags-v1",
        "value": {"tag": "v0"},
    }


def test_listen_pull_advances_cursor() -> None:
    cursor = {"encoding": "oci-list-tags-v1", "value": {"tag": "v3"}}
    response = handle("listen", _request("listen", hook_input={"mode": "pull", "cursor": cursor}))
    result = response["result"]
    assert result["events"][0]["subject"]["tag"] == "v3-next"
    assert result["nextCursor"]["value"] == {"tag": "v3-next"}


def test_listen_pull_rejects_encoding_mismatch() -> None:
    cursor = {"encoding": "oci-list-tags-v2", "value": {"tag": "v0"}}
    with pytest.raises(PluginError) as exc:
        handle("listen", _request("listen", hook_input={"mode": "pull", "cursor": cursor}))
    assert exc.value.code == "cursor-encoding-mismatch"
    assert exc.value.data == {
        "persistedEncoding": "oci-list-tags-v2",
        "pluginEncoding": "oci-list-tags-v1",
    }


def test_listen_push_returns_receiver_endpoint() -> None:
    response = handle("listen", _request("listen", hook_input={"mode": "push", "cursor": None}))
    result = response["result"]
    assert result["events"] == []
    assert result["receiverEndpoint"] == "https://example.com/webhooks/oci-registry"


def test_listen_rejects_unknown_mode() -> None:
    with pytest.raises(PluginError) as exc:
        handle("listen", _request("listen", hook_input={"mode": "stream", "cursor": None}))
    assert exc.value.code == "invalid-response"


def test_health_healthy_when_endpoint_present() -> None:
    response = handle("health", _request("health"))
    result = response["result"]
    assert result["healthy"] is True
    # checkedAt must be RFC3339 with a trailing Z.
    assert result["checkedAt"].endswith("Z")
    assert result["extras"]["instanceId"] == "inst-1"


def test_health_unhealthy_when_endpoint_blank() -> None:
    request = _request("health")
    request["connector"]["manifest"]["spec"]["target"]["endpoint"] = ""
    response = handle("health", request)
    assert response["result"]["healthy"] is False


def test_handle_rejects_wrong_api_version() -> None:
    request = _request("bind", hook_input={"slot": "source", "capability": "oci.pull"})
    request["apiVersion"] = 2
    with pytest.raises(PluginError) as exc:
        handle("bind", request)
    assert exc.value.code == "invalid-response"


def test_handle_rejects_unknown_hook() -> None:
    with pytest.raises(PluginError) as exc:
        handle("rewind", _request("bind"))
    assert exc.value.code == "invalid-response"


def test_handle_rejects_non_object_connector() -> None:
    request = _request("bind")
    request["connector"] = "not-an-object"
    with pytest.raises(PluginError) as exc:
        handle("bind", request)
    assert exc.value.code == "invalid-response"


def test_request_envelope_serializes_to_json() -> None:
    """Sanity check: the helper request envelope is JSON-serializable."""
    assert json.loads(json.dumps(_request("bind"))) == _request("bind")

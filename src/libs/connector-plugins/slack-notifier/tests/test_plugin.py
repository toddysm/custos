"""Unit tests for the reference slack-notifier sink plugin."""

from __future__ import annotations

from typing import Any

import pytest

from slack_notifier_plugin import handle
from slack_notifier_plugin.plugin import PluginError


def _request(hook: str, *, hook_input: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "apiVersion": 1,
        "hook": hook,
        "connector": {
            "type": "custos-slack-notifier",
            "version": "1.0.0",
            "imageRef": "ghcr.io/example/custos-slack-notifier:1.0.0",
            "digest": "sha256:" + "c" * 64,
            "manifest": {
                "spec": {
                    "target": {
                        "kind": "slack-webhook",
                        "endpoint": "https://hooks.slack.com",
                        "verifyTls": True,
                    }
                }
            },
        },
        "instance": {
            "workspaceId": "ws-1",
            "instanceId": "inst-1",
            "type": "custos-slack-notifier",
            "version": "1.0.0",
            "name": "prod",
            "enabled": True,
            "status": "active",
            "healthStatus": "unknown",
            "leaseTtlSeconds": 600,
            "targetConfig": {"channel": "#deploys"},
            "credentialsAuthentication": {
                "identityRef": "azure://managed-identity/custos-connector-mi",
                "audience": "api://hooks.slack.com",
            },
            "usedCapabilities": ["slack.post"],
        },
        "input": hook_input or {},
    }


def test_bind_returns_webhook_endpoint_and_stripped_channel() -> None:
    response = handle(
        "bind", _request("bind", hook_input={"slot": "notification", "capability": "slack.post"})
    )
    result = response["result"]
    assert response["ok"] is True
    assert result["endpoint"] == "https://hooks.slack.com"
    assert result["tokenTypeHint"] == "webhook"
    assert result["handle"] == {
        "slot": "notification",
        "capability": "slack.post",
        "channel": "deploys",  # leading '#' stripped
    }
    assert result["extras"] == {"connectorKind": "sink"}


def test_bind_rejects_other_capabilities() -> None:
    with pytest.raises(PluginError) as exc:
        handle(
            "bind",
            _request("bind", hook_input={"slot": "notification", "capability": "slack.broadcast"}),
        )
    assert exc.value.code == "invalid-response"


def test_bind_requires_channel() -> None:
    request = _request("bind", hook_input={"slot": "notification", "capability": "slack.post"})
    request["instance"]["targetConfig"] = {}
    with pytest.raises(PluginError) as exc:
        handle("bind", request)
    assert exc.value.code == "invalid-response"


def test_bind_requires_non_empty_slot() -> None:
    with pytest.raises(PluginError) as exc:
        handle("bind", _request("bind", hook_input={"slot": "", "capability": "slack.post"}))
    assert exc.value.code == "invalid-response"


def test_listen_is_unsupported_on_sinks() -> None:
    with pytest.raises(PluginError) as exc:
        handle("listen", _request("listen", hook_input={"mode": "pull", "cursor": None}))
    assert exc.value.code == "invalid-response"
    assert "sink connector" in exc.value.detail


def test_health_succeeds_when_endpoint_present() -> None:
    response = handle("health", _request("health"))
    assert response["result"]["healthy"] is True


def test_health_unhealthy_when_endpoint_blank() -> None:
    request = _request("health")
    request["connector"]["manifest"]["spec"]["target"]["endpoint"] = ""
    response = handle("health", request)
    assert response["result"]["healthy"] is False


def test_handle_rejects_wrong_api_version() -> None:
    request = _request("bind", hook_input={"slot": "x", "capability": "slack.post"})
    request["apiVersion"] = 9
    with pytest.raises(PluginError) as exc:
        handle("bind", request)
    assert exc.value.code == "invalid-response"


def test_handle_rejects_unknown_hook() -> None:
    with pytest.raises(PluginError) as exc:
        handle("notify", _request("bind"))
    assert exc.value.code == "invalid-response"

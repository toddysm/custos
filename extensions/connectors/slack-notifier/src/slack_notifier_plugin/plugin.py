"""Hook handlers for the slack-notifier sink plugin.

The plugin advertises:

* ``slack.post`` — the only capability, exercised at bind time.
* No ``events`` block — sink connectors don't emit events back into
  the platform. The Connector Service treats ``listen`` as unsupported
  for this type and never invokes it, but the plugin still implements
  a safe stub so the runtime contract is uniform across plugins.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Final

_API_VERSION: Final[int] = 1


class PluginError(Exception):
    def __init__(self, code: str, detail: str, *, data: dict[str, Any] | None = None) -> None:
        self.code = code
        self.detail = detail
        self.data = data
        super().__init__(detail)


def _require_object(value: Any, *, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PluginError("invalid-response", f"{name} must be a JSON object")
    return value


def handle(hook: str, request: dict[str, Any]) -> dict[str, Any]:
    api_version = request.get("apiVersion")
    if api_version != _API_VERSION:
        raise PluginError(
            "invalid-response",
            f"unsupported apiVersion {api_version!r}; plugin speaks v{_API_VERSION}",
        )
    connector = _require_object(request.get("connector"), name="connector")
    instance = _require_object(request.get("instance"), name="instance")
    hook_input = _require_object(request.get("input", {}), name="input")

    if hook == "bind":
        return {"ok": True, "result": _bind(connector, instance, hook_input)}
    if hook == "listen":
        # Sink connectors don't deliver events. The runtime should
        # never invoke listen because the manifest's spec.events block
        # is absent, but if it does we surface a typed error rather
        # than crashing.
        raise PluginError(
            "invalid-response",
            "slack-notifier is a sink connector and does not implement listen",
        )
    if hook == "health":
        return {"ok": True, "result": _health(connector, instance)}
    raise PluginError("invalid-response", f"unknown hook {hook!r}")


def _bind(
    connector: dict[str, Any],
    instance: dict[str, Any],
    hook_input: dict[str, Any],
) -> dict[str, Any]:
    """Return a ConnectorContext pointing at the configured webhook channel.

    The plugin advertises a single capability (``slack.post``); any
    other capability requested at bind time is treated as an
    integration mismatch.
    """
    slot = hook_input.get("slot")
    capability = hook_input.get("capability")
    if not isinstance(slot, str) or not slot:
        raise PluginError("invalid-response", "bind input requires non-empty slot")
    if capability != "slack.post":
        raise PluginError(
            "invalid-response",
            f"slack-notifier only advertises 'slack.post'; got {capability!r}",
        )
    manifest = connector.get("manifest") or {}
    target = manifest.get("spec", {}).get("target", {}) if isinstance(manifest, dict) else {}
    endpoint = str(target.get("endpoint") or "")
    if not endpoint:
        raise PluginError("upstream-unreachable", "manifest spec.target.endpoint is missing")
    target_config = instance.get("targetConfig") or {}
    if not isinstance(target_config, dict):
        raise PluginError("invalid-response", "instance.targetConfig must be a JSON object")
    channel = str(target_config.get("channel") or "")
    if not channel:
        raise PluginError(
            "invalid-response",
            "targetConfig.channel is required for the slack-notifier sink",
        )
    return {
        "endpoint": endpoint,
        "tokenTypeHint": "webhook",
        "handle": {
            "slot": slot,
            "capability": capability,
            "channel": channel.lstrip("#"),
        },
        "extras": {
            "connectorKind": "sink",
        },
    }


def _health(connector: dict[str, Any], instance: dict[str, Any]) -> dict[str, Any]:
    manifest = connector.get("manifest") or {}
    target = manifest.get("spec", {}).get("target", {}) if isinstance(manifest, dict) else {}
    endpoint = target.get("endpoint") if isinstance(target, dict) else None
    healthy = bool(endpoint)
    return {
        "healthy": healthy,
        "detail": "webhook endpoint configured" if healthy else "missing endpoint",
        "checkedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "extras": {"instanceId": instance.get("instanceId")},
    }

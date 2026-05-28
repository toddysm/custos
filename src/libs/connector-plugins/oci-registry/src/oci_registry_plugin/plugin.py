"""Hook handlers for the reference OCI-registry plugin.

The handlers implement the three v1 hooks documented in
``docs/developers/connector-plugin-author.md``:

* ``bind``   — return a :class:`ConnectorContext`-shaped JSON object so
  the workflow step can reach the registry through the secret-bridge
  sidecar.
* ``listen`` — return the normalized event batch and the next pull
  cursor (or a push-mode receiver endpoint).
* ``health`` — return a synchronous health probe.

The handlers are deterministic and side-effect free: they exercise the
*shape* of the wire contract so the integration suite can run against
them without a live registry. A production plugin replaces the
hard-coded bodies with calls to the registry's actual API while keeping
the JSON envelope shape unchanged.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Final

_API_VERSION: Final[int] = 1


class PluginError(Exception):
    """Typed error raised by hook handlers.

    The runtime maps the :attr:`code` value onto its own
    :class:`PluginErrorCode` enum so cursor-encoding mismatches, upstream
    auth failures, and unreachable upstreams all surface as the matching
    structured exception on the service side.
    """

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
    """Dispatch a request envelope to the matching hook handler.

    Validates the apiVersion contract and the top-level envelope shape,
    then delegates. Wrapped errors propagate as :class:`PluginError`;
    everything else is the caller's problem (``__main__`` converts
    unhandled exceptions into ``unknown-plugin-error``).
    """
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
        return {"ok": True, "result": _listen(connector, instance, hook_input)}
    if hook == "health":
        return {"ok": True, "result": _health(connector, instance)}
    raise PluginError("invalid-response", f"unknown hook {hook!r}")


# ---------------------------------------------------------------------------
# bind
# ---------------------------------------------------------------------------


def _bind(
    connector: dict[str, Any],
    instance: dict[str, Any],
    hook_input: dict[str, Any],
) -> dict[str, Any]:
    """Return a ConnectorContext for the requested ``(slot, capability)``.

    The reference plugin produces a registry endpoint URL derived from
    ``target.endpoint`` + the per-instance ``repositoryNamespace`` so
    activities can build pull/push URLs directly. ``handle`` is a JWT
    placeholder that the secret-bridge sidecar substitutes at runtime;
    ``extras`` carries the registry kind so downstream activities can
    pick the right client library.
    """
    slot = hook_input.get("slot")
    capability = hook_input.get("capability")
    if not isinstance(slot, str) or not slot:
        raise PluginError("invalid-response", "bind input requires non-empty slot")
    if not isinstance(capability, str) or not capability:
        raise PluginError("invalid-response", "bind input requires non-empty capability")
    target_config = instance.get("targetConfig") or {}
    if not isinstance(target_config, dict):
        raise PluginError("invalid-response", "instance.targetConfig must be a JSON object")
    repo_ns = target_config.get("repositoryNamespace", "")
    manifest = connector.get("manifest") or {}
    target = manifest.get("spec", {}).get("target", {}) if isinstance(manifest, dict) else {}
    endpoint = str(target.get("endpoint") or "")
    if not endpoint:
        raise PluginError(
            "upstream-unreachable",
            "manifest spec.target.endpoint is missing — cannot derive bind endpoint",
        )
    full_endpoint = (
        f"{endpoint.rstrip('/')}/v2/{repo_ns}".rstrip("/")
        if repo_ns
        else f"{endpoint.rstrip('/')}/v2"
    )
    return {
        "endpoint": full_endpoint,
        "tokenTypeHint": "bearer",
        "handle": {
            "slot": slot,
            "capability": capability,
            "instanceId": instance.get("instanceId"),
        },
        "extras": {
            "registryKind": "oci-registry",
            "verifyTls": bool(target.get("verifyTls", True)),
        },
    }


# ---------------------------------------------------------------------------
# listen
# ---------------------------------------------------------------------------


def _listen(
    connector: dict[str, Any],
    instance: dict[str, Any],
    hook_input: dict[str, Any],
) -> dict[str, Any]:
    """Return one tick of normalized events.

    Cursor contract:

    * ``cursorEncoding`` is ``oci-list-tags-v1``. The cursor value is a
      JSON object with a single ``"tag"`` key holding the last tag
      we've already emitted; a missing or null cursor means "start from
      the beginning of the tag list per ``initialCursorBehavior``".
    * On every tick we synthesize one ``oci.image.pushed`` event and
      advance the cursor to a deterministic next-tag value
      (``"<previous>-next"``). A real plugin would page through the
      registry's ``GET /v2/<repo>/tags/list`` response instead.
    """
    mode = hook_input.get("mode")
    if mode not in ("pull", "push"):
        raise PluginError("invalid-response", f"listen mode must be pull|push (got {mode!r})")
    if mode == "push":
        # The reference plugin advertises both delivery modes; for push
        # mode it returns a deterministic receiver endpoint so the
        # Listen Manager can wire its webhook receiver.
        return {
            "events": [],
            "receiverEndpoint": "https://example.com/webhooks/oci-registry",
        }

    cursor = hook_input.get("cursor")
    persisted_encoding: str | None = None
    last_tag = ""
    if cursor is not None:
        if not isinstance(cursor, dict):
            raise PluginError("invalid-response", "cursor must be a JSON object when present")
        persisted_encoding = (
            cursor.get("encoding") if isinstance(cursor.get("encoding"), str) else None
        )
        if persisted_encoding is not None and persisted_encoding != "oci-list-tags-v1":
            raise PluginError(
                "cursor-encoding-mismatch",
                f"persisted encoding {persisted_encoding!r} != plugin encoding 'oci-list-tags-v1'",
                data={
                    "persistedEncoding": persisted_encoding,
                    "pluginEncoding": "oci-list-tags-v1",
                },
            )
        value = cursor.get("value") or {}
        if not isinstance(value, dict):
            raise PluginError("invalid-response", "cursor.value must be a JSON object")
        last_tag = str(value.get("tag") or "")

    next_tag = "v0" if not last_tag else f"{last_tag}-next"
    event_id = f"{instance.get('instanceId', 'unknown')}:{next_tag}"
    repo_ns = (instance.get("targetConfig") or {}).get("repositoryNamespace", "")
    event = {
        "eventId": event_id,
        "eventType": "oci.image.pushed",
        "occurredAt": "2026-05-27T00:00:00Z",
        "subject": {
            "repository": f"{repo_ns}/sample-image" if repo_ns else "sample-image",
            "tag": next_tag,
        },
    }
    return {
        "events": [event],
        "nextCursor": {
            "encoding": "oci-list-tags-v1",
            "value": {"tag": next_tag},
        },
    }


# ---------------------------------------------------------------------------
# health
# ---------------------------------------------------------------------------


def _health(connector: dict[str, Any], instance: dict[str, Any]) -> dict[str, Any]:
    """Synchronous health probe.

    The reference plugin returns ``healthy=True`` if the manifest
    declares a non-empty ``spec.target.endpoint``; otherwise it returns
    ``healthy=False`` so an integration test can assert the unhealthy
    branch without having to break the upstream.
    """
    manifest = connector.get("manifest") or {}
    target = manifest.get("spec", {}).get("target", {}) if isinstance(manifest, dict) else {}
    endpoint = target.get("endpoint") if isinstance(target, dict) else None
    healthy = bool(endpoint)
    return {
        "healthy": healthy,
        "detail": "endpoint reachable (stub)" if healthy else "missing endpoint",
        "checkedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "extras": {
            "instanceId": instance.get("instanceId"),
            "registryEndpoint": endpoint,
        },
    }

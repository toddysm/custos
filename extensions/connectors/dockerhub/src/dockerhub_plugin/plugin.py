"""Hook handlers for the out-of-the-box Docker Hub connector plugin.

The handlers implement the v1 hooks documented in
``docs/developers/connector-plugin-author.md``:

* ``bind``   — return a :class:`ConnectorContext`-shaped JSON object so a
  workflow step can reach Docker Hub through the connector sidecar. The
  context advertises ``tokenTypeHint: "basic"``: Docker Hub authenticates
  the Layer-2 token exchange with the PAT presented as HTTP Basic
  credentials, and the per-repository bearer is minted by the *consuming
  activity* (the plugin never mints it because the repository scope is
  unknown at bind time).
* ``health`` — a live, *unauthenticated* ``GET /v2/`` reachability probe.

The plugin never receives the resolved Personal Access Token. It is
handed the credential *reference* (the ``x-dapr-secret`` ``authentication``
block); ``bind`` returns a binding handle the sidecar maps onto the leased
credential at the data plane. Two-layer token model and rationale live in
``README.md``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Final

from . import probe

_API_VERSION: Final[int] = 1

# Capabilities advertised by ``connector-manifest.json``. Keep the two
# lists in lock-step: the plugin only knows how to bind for this exact
# set, so a bind request for any other token is an integration mismatch
# we want to surface at runtime rather than silently producing a registry
# endpoint for (e.g.) ``s3.read``.
_ADVERTISED_CAPABILITIES: Final[frozenset[str]] = frozenset(
    {
        "oci.pull",
        "oci.push",
        "oci.list-tags",
        "oci.list-referrers",
    }
)

# Docker Hub's Layer-2 token endpoint and auth service, surfaced to the
# consuming activity via ``bind`` extras so it can run the PAT -> bearer
# exchange without re-deriving these constants.
_TOKEN_ENDPOINT: Final[str] = "https://auth.docker.io/token"
_AUTH_SERVICE: Final[str] = "registry.docker.io"


class PluginError(Exception):
    """Typed error raised by hook handlers.

    The runtime maps the :attr:`code` value onto its own
    :class:`PluginErrorCode` enum so capability mismatches and unreachable
    upstreams surface as the matching structured exception on the service
    side.
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
    if hook == "health":
        return {"ok": True, "result": _health(connector, instance)}
    raise PluginError("invalid-response", f"unknown hook {hook!r}")


# ---------------------------------------------------------------------------
# bind
# ---------------------------------------------------------------------------


def _manifest_target(connector: dict[str, Any]) -> dict[str, Any]:
    manifest = connector.get("manifest") or {}
    if not isinstance(manifest, dict):
        return {}
    spec = manifest.get("spec", {})
    target = spec.get("target", {}) if isinstance(spec, dict) else {}
    return target if isinstance(target, dict) else {}


def _bind(
    connector: dict[str, Any],
    instance: dict[str, Any],
    hook_input: dict[str, Any],
) -> dict[str, Any]:
    """Return a ConnectorContext for the requested ``(slot, capability)``.

    Derives the Docker Hub data-plane endpoint
    (``https://registry-1.docker.io/v2/<namespace>``) from the manifest
    endpoint plus the per-instance ``repositoryNamespace``. ``handle`` is
    the binding the sidecar maps onto the leased PAT; ``extras`` carries
    the Layer-2 token endpoint so the consuming activity can run the
    PAT -> per-repository bearer exchange. No token is minted here.
    """
    slot = hook_input.get("slot")
    capability = hook_input.get("capability")
    if not isinstance(slot, str) or not slot:
        raise PluginError("invalid-response", "bind input requires non-empty slot")
    if not isinstance(capability, str) or not capability:
        raise PluginError("invalid-response", "bind input requires non-empty capability")
    if capability not in _ADVERTISED_CAPABILITIES:
        raise PluginError(
            "invalid-response",
            f"dockerhub plugin does not advertise capability {capability!r}; "
            f"supported: {sorted(_ADVERTISED_CAPABILITIES)}",
        )
    target_config = instance.get("targetConfig") or {}
    if not isinstance(target_config, dict):
        raise PluginError("invalid-response", "instance.targetConfig must be a JSON object")
    repo_ns = str(target_config.get("repositoryNamespace", "") or "")
    target = _manifest_target(connector)
    endpoint = str(target.get("endpoint") or "")
    if not endpoint:
        raise PluginError(
            "upstream-unreachable",
            "manifest spec.target.endpoint is missing — cannot derive bind endpoint",
        )
    base = endpoint.rstrip("/")
    full_endpoint = f"{base}/v2/{repo_ns}".rstrip("/") if repo_ns else f"{base}/v2"
    return {
        "endpoint": full_endpoint,
        "tokenTypeHint": "basic",
        "handle": {
            "slot": slot,
            "capability": capability,
            "instanceId": instance.get("instanceId"),
        },
        "extras": {
            "registryKind": "dockerhub",
            "tokenEndpoint": _TOKEN_ENDPOINT,
            "service": _AUTH_SERVICE,
            "verifyTls": bool(target.get("verifyTls", True)),
        },
    }


# ---------------------------------------------------------------------------
# health
# ---------------------------------------------------------------------------


def _health(connector: dict[str, Any], instance: dict[str, Any]) -> dict[str, Any]:
    """Synchronous, unauthenticated reachability probe.

    Delegates to :func:`probe.check_reachability`, which performs a live
    ``GET /v2/`` and verifies the Docker Hub Bearer challenge. The plugin
    has no resolved PAT, so this is the strongest signal it can produce
    without credentials.
    """
    target = _manifest_target(connector)
    endpoint = str(target.get("endpoint") or "")
    if not endpoint:
        return {
            "healthy": False,
            "detail": "manifest spec.target.endpoint is missing",
            "checkedAt": _now(),
            "extras": {"instanceId": instance.get("instanceId")},
        }
    verify_tls = bool(target.get("verifyTls", True))
    result = probe.check_reachability(endpoint, verify_tls=verify_tls)
    extras: dict[str, Any] = {"instanceId": instance.get("instanceId")}
    for key in ("registryEndpoint", "tokenEndpoint", "service"):
        if key in result:
            extras[key] = result[key]
    return {
        "healthy": bool(result["healthy"]),
        "detail": str(result["detail"]),
        "checkedAt": _now(),
        "extras": extras,
    }


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")

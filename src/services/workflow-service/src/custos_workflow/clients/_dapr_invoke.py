"""Dapr Service-Invocation HTTP transport primitives (WF-IMPL-073).

Shared building blocks for the production ``ActivityRuntimeClient``
and ``ConnectorClient`` adapters that land in WF-IMPL-076..078.
Both adapters speak to the local Dapr sidecar's Service Invocation
HTTP API (``POST /v1.0/invoke/<app-id>/method/<method>``), so the
URL builder, default constants, and env parser belong in one
place. This module deliberately holds **no** :class:`httpx.AsyncClient`
— clients receive an already-built (lifespan-owned) client by
injection, mirroring the
:class:`custos_workflow.runs.events.DaprPubSubLifecyclePublisher`
precedent.

The exported names are sibling-only; they are deliberately not
re-exported from :mod:`custos_workflow.clients` because nothing
outside the ``clients`` package should construct
:class:`DaprInvokeEndpoint` instances directly — the
:mod:`custos_workflow.providers` factory builds them from the
environment.

Acceptance criteria (mirrored from #484):

* :func:`build_invoke_url` matches the canonical Dapr
  Service-Invocation HTTP shape verbatim.
* :class:`DaprInvokeEndpoint` is frozen, hashable, and rejects an
  empty ``app_id`` / ``host`` at construction.
* :func:`read_dapr_env` raises :class:`RuntimeError` whose message
  names the missing env var.
* 100 % unit-test coverage on this module.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

__all__ = [
    "DEFAULT_DAPR_HOST",
    "DEFAULT_DAPR_HTTP_PORT",
    "DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS",
    "ENV_DAPR_HTTP_HOST",
    "ENV_DAPR_HTTP_PORT",
    "DaprInvokeEndpoint",
    "build_invoke_url",
    "read_dapr_env",
]


# ---------------------------------------------------------------------------
# Defaults & env-var names
# ---------------------------------------------------------------------------

#: Default host the Dapr sidecar is reachable on from inside the
#: same pod. Matches the
#: :class:`custos_workflow.runs.events.DaprPubSubLifecyclePublisher`
#: precedent (``http://127.0.0.1:3500``) and the Dapr documented
#: sidecar contract — the sidecar always binds to localhost from
#: the app container's perspective.
DEFAULT_DAPR_HOST: Final[str] = "127.0.0.1"

#: Default Dapr sidecar HTTP port. Mirrors Dapr's documented
#: default (``DAPR_HTTP_PORT=3500``) and the existing default
#: baked into
#: :data:`custos_workflow.providers.ENV_DAPR_ENDPOINT`'s fallback
#: (``http://127.0.0.1:3500``).
DEFAULT_DAPR_HTTP_PORT: Final[int] = 3500

#: Default request timeout (seconds) the production outbound-RPC
#: adapters use against the Dapr sidecar. Matches the spirit of
#: :data:`custos_workflow.runs.events.DEFAULT_DAPR_PUBLISH_TIMEOUT_SECONDS`
#: (10s) — outbound RPC to ARM / Connector Service is bounded by
#: the same expected sidecar-latency envelope as Pub/Sub publish.
DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS: Final[float] = 10.0

#: Env var name for an override of :data:`DEFAULT_DAPR_HOST`.
#: Kept outside the ``WF_`` prefix because the Dapr-side conventions
#: (``DAPR_HTTP_HOST`` / ``DAPR_HTTP_PORT``) are what the rest of the
#: ecosystem documents — see
#: https://docs.dapr.io/operations/configuration/environment-variables-reference/.
ENV_DAPR_HTTP_HOST: Final[str] = "DAPR_HTTP_HOST"

#: Env var name for an override of :data:`DEFAULT_DAPR_HTTP_PORT`.
#: See :data:`ENV_DAPR_HTTP_HOST` for naming rationale.
ENV_DAPR_HTTP_PORT: Final[str] = "DAPR_HTTP_PORT"


# ---------------------------------------------------------------------------
# Endpoint dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DaprInvokeEndpoint:
    """Resolved Dapr Service-Invocation target for one upstream app.

    The triple ``(host, http_port, app_id)`` is everything
    :func:`build_invoke_url` needs to build the canonical
    ``…/v1.0/invoke/<app-id>/method/<method>`` URL the production
    outbound-RPC adapters POST against. Instances are frozen +
    hashable so an adapter can stash one alongside its tracing
    span without defensive copying.

    :raises ValueError: If :attr:`host` or :attr:`app_id` is empty,
        or :attr:`http_port` is not a positive int.
    """

    host: str
    http_port: int
    app_id: str

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("DaprInvokeEndpoint.host must be a non-empty string")
        if not self.app_id:
            raise ValueError("DaprInvokeEndpoint.app_id must be a non-empty string")
        # Reject ``bool`` explicitly: ``True`` and ``False`` would
        # otherwise sneak past the ``int`` check below thanks to
        # ``isinstance(True, int) is True``. The same defensive
        # pattern is used in
        # :class:`custos_workflow.clients.activity_runtime.ActivityResultEnvelope`.
        if isinstance(self.http_port, bool) or not isinstance(self.http_port, int):
            raise ValueError(
                f"DaprInvokeEndpoint.http_port must be an int, got {type(self.http_port).__name__}"
            )
        if self.http_port <= 0:
            raise ValueError(f"DaprInvokeEndpoint.http_port must be positive, got {self.http_port}")


# ---------------------------------------------------------------------------
# URL builder
# ---------------------------------------------------------------------------


def build_invoke_url(endpoint: DaprInvokeEndpoint, method: str) -> str:
    """Render the canonical Dapr Service-Invocation HTTP URL.

    Returns ``http://{host}:{port}/v1.0/invoke/{app_id}/method/{method}``
    verbatim — no path normalisation beyond stripping a single
    leading ``/`` from ``method`` so callers can pass either
    ``"ScheduleActivity"`` or ``"/ScheduleActivity"`` without
    double-slashing the URL.

    :raises ValueError: If ``method`` is empty.
    """
    if not method:
        raise ValueError("build_invoke_url requires a non-empty method name")
    cleaned = method.lstrip("/")
    if not cleaned:
        raise ValueError("build_invoke_url method must contain non-slash characters")
    return (
        f"http://{endpoint.host}:{endpoint.http_port}"
        f"/v1.0/invoke/{endpoint.app_id}/method/{cleaned}"
    )


# ---------------------------------------------------------------------------
# Env parser
# ---------------------------------------------------------------------------


def read_dapr_env(env: Mapping[str, str], app_id_var: str) -> DaprInvokeEndpoint:
    """Build a :class:`DaprInvokeEndpoint` from an environment mapping.

    Resolves the upstream app-id from ``env[app_id_var]`` (the
    workflow-service-specific env var, e.g. ``WF_ARM_ENDPOINT`` or
    ``WF_CONNECTOR_ENDPOINT``), then resolves the sidecar host +
    port from the Dapr-side conventions
    :data:`ENV_DAPR_HTTP_HOST` / :data:`ENV_DAPR_HTTP_PORT`,
    falling back to :data:`DEFAULT_DAPR_HOST` /
    :data:`DEFAULT_DAPR_HTTP_PORT` when unset.

    :raises RuntimeError: If ``env[app_id_var]`` is missing or
        empty. The message names ``app_id_var`` so the operator
        knows exactly which env var to set.
    :raises ValueError: If :data:`ENV_DAPR_HTTP_PORT` is set but
        not parseable as a positive int.
    """
    raw_app_id = env.get(app_id_var, "").strip()
    if not raw_app_id:
        raise RuntimeError(
            f"{app_id_var} must be set to the upstream Dapr app-id "
            f"(e.g. 'activity-runtime-manager' or 'connector-service')"
        )
    host = env.get(ENV_DAPR_HTTP_HOST, DEFAULT_DAPR_HOST).strip() or DEFAULT_DAPR_HOST
    raw_port = env.get(ENV_DAPR_HTTP_PORT, "").strip()
    if not raw_port:
        port = DEFAULT_DAPR_HTTP_PORT
    else:
        try:
            port = int(raw_port)
        except ValueError as exc:
            raise ValueError(
                f"{ENV_DAPR_HTTP_PORT} must be a positive integer, got {raw_port!r}"
            ) from exc
    return DaprInvokeEndpoint(host=host, http_port=port, app_id=raw_app_id)

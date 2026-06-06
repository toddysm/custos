"""OIDC device-code session manager — M1 503 stub (AGW-IMPL-015).

The CLI authenticates via the OIDC device-code flow (RFC 8628). The gateway owns
the *session-state* endpoints (the actual OIDC token verification stays in Auth
Service / the configured issuer — the gateway is purely a relay):

- ``POST /v1/auth/login/device``                   — start a device-code session
- ``POST /v1/auth/login/device/{deviceCode}/poll`` — CLI polls for completion
- ``GET  /v1/auth/login/device/{userCode}``        — browser landing page

All three are **auth-bootstrap** routes: they are hit *before* the caller holds a
bearer token, so they bypass AuthN/AuthZ and the gateway mints **no call
context** for them (their paths live under
:data:`~custos_gateway.middleware.auth.AUTH_BOOTSTRAP_BYPASS_PREFIX`, which the
bypass classifier already excludes).

Per the design's M1 note the flow is gated on a configured OIDC issuer
(:attr:`~custos_gateway.settings.Settings.device_code_enabled`). M1 ships with
OIDC disabled, so every handler returns ``503`` — the issuer the gateway relays
to is not available. The persistence seam (:class:`DeviceCodeStore` +
:data:`DEVICE_CODE_STORE_STATE_ATTR`) and the TTL config
(``CUSTOS_GATEWAY_DEVICE_CODE_TTL``, already parsed into
:attr:`Settings.device_code_ttl_seconds`) are declared here so M3 can activate
the flow by configuring an issuer and filling in the handler bodies — without
touching the routing or middleware ordering.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final, Protocol, cast

from fastapi import APIRouter, Request, Response

from custos_gateway.errors import GatewayError, GatewayErrorCode
from custos_gateway.middleware.auth import AUTH_BOOTSTRAP_BYPASS_PREFIX, is_auth_bypass_path

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime
    from typing import Any

    from custos_spl import DeviceCodeSession, WorkspaceId

    from custos_gateway.settings import Settings

__all__ = [
    "DEVICE_CODE_LANDING_PATH",
    "DEVICE_CODE_POLL_PATH",
    "DEVICE_CODE_START_PATH",
    "DEVICE_CODE_STORE_STATE_ATTR",
    "DeviceCodeStore",
    "build_device_code_router",
    "device_code_flow_unavailable",
    "get_device_code_store",
]

#: Start a device-code session. Returns ``{deviceCode, userCode, verificationUri,
#: interval, expiresIn}`` and stores a pending session keyed by ``deviceCode``.
DEVICE_CODE_START_PATH: Final[str] = "/v1/auth/login/device"

#: CLI polling endpoint. Returns ``authorization_pending`` / ``slow_down`` until
#: the browser flow completes, then the minted token bundle.
DEVICE_CODE_POLL_PATH: Final[str] = "/v1/auth/login/device/{deviceCode}/poll"

#: Browser-facing landing page keyed by the short ``userCode``; delegates the
#: actual login to the configured OIDC issuer.
DEVICE_CODE_LANDING_PATH: Final[str] = "/v1/auth/login/device/{userCode}"

#: ``app.state`` attribute holding the lifespan-owned device-code store. Bound by
#: the application factory (AGW-IMPL-016); ``None`` until the flow is activated.
DEVICE_CODE_STORE_STATE_ATTR: Final[str] = "device_code_store"


class DeviceCodeStore(Protocol):
    """The narrow SPL metadata-store surface the device-code flow depends on.

    The full :class:`custos_spl.MetadataStoreProvider` structurally satisfies
    this protocol; depending on only the five device-code methods keeps the flow
    decoupled from the rest of the store and trivially fakeable in M3. The seam
    exists in M1 so activation is a configuration + handler-body change rather
    than a routing change.
    """

    async def put_device_code_session(
        self, workspace_id: WorkspaceId, session: DeviceCodeSession
    ) -> DeviceCodeSession: ...

    async def get_device_code_session_by_device_code(
        self, workspace_id: WorkspaceId, device_code: str
    ) -> DeviceCodeSession | None: ...

    async def get_device_code_session_by_user_code(
        self, workspace_id: WorkspaceId, user_code: str
    ) -> DeviceCodeSession | None: ...

    async def complete_device_code_session(
        self,
        workspace_id: WorkspaceId,
        device_code: str,
        token_bundle: Mapping[str, Any],
    ) -> DeviceCodeSession: ...

    async def delete_expired_device_code_sessions(self, before: datetime) -> int: ...


def _device_code_settings(request: Request) -> Settings:
    """Return the gateway settings the factory bound to ``app.state``."""
    return cast("Settings", request.app.state.settings)


def get_device_code_store(request: Request) -> DeviceCodeStore:
    """Return the lifespan-owned device-code store, or fail with 503.

    The store is bound to ``app.state`` once the device-code flow is activated
    (AGW-IMPL-016 / M3); its absence means the flow cannot service the request.
    """
    store = getattr(request.app.state, DEVICE_CODE_STORE_STATE_ATTR, None)
    if store is None:
        raise device_code_flow_unavailable()
    return cast("DeviceCodeStore", store)


def device_code_flow_unavailable() -> GatewayError:
    """Build the ``503`` raised while the device-code flow is disabled.

    The gateway is a relay for the OIDC issuer that mints tokens; when no issuer
    is configured that upstream dependency is unavailable, so the locked ``503
    downstream-unavailable`` code is the honest answer (M1 ships OIDC disabled).
    """
    return GatewayError(
        GatewayErrorCode.DOWNSTREAM_UNAVAILABLE,
        detail="The OIDC device-code login flow is not enabled.",
    )


def _require_active_flow(request: Request) -> DeviceCodeStore:
    """Resolve the device-code backend or fail with ``503``.

    The flow needs *both* a configured OIDC issuer
    (:attr:`Settings.device_code_enabled`) *and* a bound
    :class:`DeviceCodeStore`. M1 ships with neither, so every request gets the
    locked ``503``. Gating on both — rather than the issuer flag alone — means a
    half-configured deployment (e.g. ``device_code_enabled`` flipped on before
    the store is wired in M3) still returns ``503`` instead of leaking the
    not-yet-implemented handler body as a misleading ``500``.
    """
    if not _device_code_settings(request).device_code_enabled:
        raise device_code_flow_unavailable()
    return get_device_code_store(request)


async def _start_device_code_session(request: Request) -> Response:
    """Start a device-code session (M3); M1 returns ``503`` (OIDC disabled)."""
    _require_active_flow(request)
    raise NotImplementedError  # pragma: no cover - M3 activation


async def _poll_device_code_session(request: Request) -> Response:
    """Poll a device-code session (M3); M1 returns ``503`` (OIDC disabled)."""
    _require_active_flow(request)
    raise NotImplementedError  # pragma: no cover - M3 activation


async def _device_code_landing(request: Request) -> Response:
    """Render the browser landing page (M3); M1 returns ``503`` (OIDC disabled)."""
    _require_active_flow(request)
    raise NotImplementedError  # pragma: no cover - M3 activation


def build_device_code_router() -> APIRouter:
    """Materialize the auth-bootstrap device-code routes onto a FastAPI router.

    None of the routes declare a ``require_permission`` dependency — they are
    anonymous auth-bootstrap routes under
    :data:`~custos_gateway.middleware.auth.AUTH_BOOTSTRAP_BYPASS_PREFIX`, so the
    bypass classifier already excludes them from authentication. The handlers
    return ``503`` in M1 while OIDC is disabled.
    """
    for path in (DEVICE_CODE_START_PATH, DEVICE_CODE_POLL_PATH, DEVICE_CODE_LANDING_PATH):
        if not is_auth_bypass_path(path):  # pragma: no cover - invariant
            msg = (
                f"device-code path {path!r} must live under the auth-bootstrap prefix "
                f"{AUTH_BOOTSTRAP_BYPASS_PREFIX!r} or it would require authentication"
            )
            raise RuntimeError(msg)
    router = APIRouter()
    router.add_api_route(
        DEVICE_CODE_START_PATH,
        _start_device_code_session,
        methods=["POST"],
        name="device-code-start",
    )
    router.add_api_route(
        DEVICE_CODE_POLL_PATH,
        _poll_device_code_session,
        methods=["POST"],
        name="device-code-poll",
    )
    router.add_api_route(
        DEVICE_CODE_LANDING_PATH,
        _device_code_landing,
        methods=["GET"],
        name="device-code-landing",
    )
    return router

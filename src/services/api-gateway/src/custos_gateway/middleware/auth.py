"""AuthN/AuthZ enforcement for the Custos API Gateway (AGW-IMPL-005).

Every non-bypass request must present a bearer token. The gateway never makes
an authentication or authorization decision itself: it extracts the bearer,
resolves the workspace, and asks the Auth Service to *verify-and-authorize* the
token against the route's declared ``required_permission`` in a single round
trip (see ``design/components/api-gateway/design.md`` § "AuthN / AuthZ Path").

The mechanism is a FastAPI dependency factory, :func:`require_permission`: a
route declares ``Depends(require_permission("catalog:workflows:read"))`` and the
returned dependency performs the verify-and-authorize call, raising the locked
gateway taxonomy on failure and attaching the authorized caller to
``request.state`` on success. Routes that should be anonymous at the gateway
boundary — webhook ingress and the auth-bootstrap login routes — simply omit
the dependency and therefore never call ``verify_and_authorize``;
:func:`is_auth_bypass_path` classifies those families for the request router
(AGW-IMPL-016) and is exercised here so the contract is pinned.

The required permission for a route must exist in the Auth Service permission
registry; that cross-check happens at startup (AGW-IMPL-008), not here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, cast

from fastapi import Request

from custos_gateway.clients.auth import (
    AuthServiceClient,
    AuthServiceClientError,
    AuthServiceClientStatusError,
    VerifyAndAuthorizeRequest,
)
from custos_gateway.errors import GatewayError, GatewayErrorCode

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

__all__ = [
    "AUTH_BOOTSTRAP_BYPASS_PREFIX",
    "AUTH_CLIENT_STATE_ATTR",
    "AUTH_STATE_ATTR",
    "BEARER_SCHEME",
    "PLATFORM_WORKSPACE_ID",
    "WEBHOOK_BYPASS_PREFIX",
    "AuthorizedCaller",
    "get_auth_client",
    "is_auth_bypass_path",
    "require_permission",
]

#: ``Authorization: Bearer <token>`` scheme label (case-insensitive on input).
BEARER_SCHEME: Final[str] = "Bearer"

#: Webhook ingress is anonymous at the gateway boundary — signature validation
#: belongs to Trigger Service / the connector plugin, not the gateway.
WEBHOOK_BYPASS_PREFIX: Final[str] = "/v1/webhooks/"

#: Auth-bootstrap login routes (OIDC callback + device-code flow) are hit
#: *before* a caller holds a bearer token, so they bypass authn entirely.
AUTH_BOOTSTRAP_BYPASS_PREFIX: Final[str] = "/v1/auth/login"

#: Sentinel workspace id for unscoped / platform-scoped routes, mirroring the
#: Auth Service ``PLATFORM_WORKSPACE_ID``. The Workspace Resolver (AGW-IMPL-006)
#: refines path/body resolution; until then unscoped routes authorize against
#: this platform bucket so the wire contract's non-empty ``workspace_id`` holds.
PLATFORM_WORKSPACE_ID: Final[str] = "__platform__"

#: ``app.state`` attribute holding the lifespan-owned Auth Service client.
AUTH_CLIENT_STATE_ATTR: Final[str] = "auth_client"

#: ``request.state`` attribute the dependency binds the authorized caller to.
AUTH_STATE_ATTR: Final[str] = "auth"


@dataclass(frozen=True, slots=True)
class AuthorizedCaller:
    """The decision the gateway carries forward after a successful authorize.

    Bound to ``request.state.auth`` so downstream pipeline stages (idempotency,
    rate limiting, call-context minting) and the route handler can read the
    principal and the audit trail id without re-querying Auth Service.
    """

    principal_id: str
    audit_event_id: str
    permission: str
    workspace_id: str


def is_auth_bypass_path(path: str) -> bool:
    """Return ``True`` when ``path`` belongs to an anonymous route family.

    Two families bypass AuthN/AuthZ by design: webhook ingress
    (``/v1/webhooks/...``) and the auth-bootstrap login routes
    (``/v1/auth/login...``). Everything else requires a verified bearer.
    """
    return path.startswith(WEBHOOK_BYPASS_PREFIX) or path.startswith(AUTH_BOOTSTRAP_BYPASS_PREFIX)


def get_auth_client(request: Request) -> AuthServiceClient:
    """Return the Auth Service client the lifespan attached to ``app.state``.

    Raises:
        RuntimeError: When the client is absent — a wiring bug, since a
            protected route can never authorize without it.
    """
    client = getattr(request.app.state, AUTH_CLIENT_STATE_ATTR, None)
    if client is None:
        raise RuntimeError(
            "Auth Service client is not attached to app.state; did the lifespan run?"
        )
    return cast(AuthServiceClient, client)


def _extract_bearer(request: Request) -> str:
    """Extract the bearer token, or raise ``invalid-token`` (401).

    Accepts the scheme case-insensitively but requires exactly
    ``<scheme> <token>`` with a non-empty token.
    """
    header = request.headers.get("authorization")
    if not header:
        raise GatewayError(
            GatewayErrorCode.INVALID_TOKEN,
            detail="Missing Authorization header.",
        )
    scheme, _, token = header.partition(" ")
    if scheme.lower() != BEARER_SCHEME.lower() or not token.strip():
        raise GatewayError(
            GatewayErrorCode.INVALID_TOKEN,
            detail="Authorization header must be a non-empty Bearer token.",
        )
    return token.strip()


def _resolve_workspace_id(request: Request) -> str:
    """Resolve the workspace the request authorizes against.

    The Workspace Resolver (AGW-IMPL-006) will bind ``request.state.workspace_id``
    from the path (URL authoritative) and reject URL-vs-body divergence; until
    then this honours a pre-bound value, falls back to the ``workspaceId`` path
    parameter, and finally to the platform sentinel for unscoped routes.
    """
    bound = getattr(request.state, "workspace_id", None)
    if isinstance(bound, str) and bound:
        return bound
    from_path = request.path_params.get("workspaceId")
    if isinstance(from_path, str) and from_path:
        return from_path
    return PLATFORM_WORKSPACE_ID


def require_permission(permission: str) -> Callable[[Request], Awaitable[AuthorizedCaller]]:
    """Build the per-route dependency that enforces ``permission``.

    The returned coroutine extracts the bearer, resolves the workspace, and
    delegates the decision to ``verify_and_authorize``. It raises:

    * ``invalid-token`` (401) — missing/malformed bearer, or Auth Service
      reports the token did not verify (HTTP 401);
    * ``permission-denied`` (403) — token verified but the principal lacks the
      permission; the envelope carries the ``auditEventId``;
    * ``downstream-unavailable`` (503) — Auth Service was unreachable or
      returned an unexpected status / undecodable body.

    On success it binds an :class:`AuthorizedCaller` to ``request.state.auth``
    and returns it so the handler can declare it as a typed dependency.
    """
    if not permission:
        raise ValueError("require_permission needs a non-empty permission name")

    async def _dependency(request: Request) -> AuthorizedCaller:
        token = _extract_bearer(request)
        workspace_id = _resolve_workspace_id(request)
        client = get_auth_client(request)

        try:
            decision = await client.verify_and_authorize(
                VerifyAndAuthorizeRequest(
                    token=token,
                    permission=permission,
                    workspace_id=workspace_id,
                )
            )
        except AuthServiceClientStatusError as exc:
            if exc.status_code == 401:
                raise GatewayError(
                    GatewayErrorCode.INVALID_TOKEN,
                    detail="Auth Service rejected the bearer token.",
                ) from exc
            raise GatewayError(
                GatewayErrorCode.DOWNSTREAM_UNAVAILABLE,
                detail="Auth Service returned an unexpected status.",
            ) from exc
        except AuthServiceClientError as exc:
            raise GatewayError(
                GatewayErrorCode.DOWNSTREAM_UNAVAILABLE,
                detail="Auth Service is unavailable.",
            ) from exc

        if not decision.allowed:
            raise GatewayError(
                GatewayErrorCode.PERMISSION_DENIED,
                detail=decision.reason or "Permission denied.",
                extras={"auditEventId": decision.audit_event_id},
            )

        caller = AuthorizedCaller(
            principal_id=decision.principal_id,
            audit_event_id=decision.audit_event_id,
            permission=permission,
            workspace_id=workspace_id,
        )
        setattr(request.state, AUTH_STATE_ATTR, caller)
        return caller

    return _dependency

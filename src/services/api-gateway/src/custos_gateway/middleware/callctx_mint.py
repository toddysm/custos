"""Call-context minting for the Custos API Gateway (AGW-IMPL-007).

For every *authenticated* request the gateway mints a fresh signed call-context
(``x-custos-callctx``) by asking the Auth Service to sign one for the authorized
principal, and propagates it — together with the request correlation id
(``x-correlation-id``) — through the Dapr service-invocation metadata of the
downstream call (see ``design/components/api-gateway/design.md`` § "Call-Context
Minting"). Downstream components verify the signature locally against the Auth
Service JWKS; they never call back to Auth Service per request.

The mechanism is a FastAPI dependency, :func:`mint_call_context`, that runs
*after* :func:`custos_gateway.middleware.auth.require_permission` (it reads the
:class:`~custos_gateway.middleware.auth.AuthorizedCaller` that dependency bound
to ``request.state``) and *after* the correlation middleware (it reads
``request.state.correlation_id``). It signs exactly one context per request and
binds the outbound metadata to ``request.state`` for the downstream router
(AGW-IMPL-016) to attach. Bypass routes declare neither dependency, so they mint
no context.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from custos_callctx import CALLCTX_HEADER
from fastapi import Request

from custos_gateway.clients.auth import (
    AuthServiceClientError,
    CallctxSignRequest,
)
from custos_gateway.errors import CORRELATION_ID_HEADER, GatewayError, GatewayErrorCode
from custos_gateway.middleware.auth import AUTH_STATE_ATTR, AuthorizedCaller, get_auth_client

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = [
    "CALLER_COMPONENT",
    "CALL_CONTEXT_STATE_ATTR",
    "OUTBOUND_METADATA_STATE_ATTR",
    "MintedCallContext",
    "mint_call_context",
]

#: ``callerComponent`` recorded in the minted call-context — identifies the
#: gateway as the issuer so downstream audit trails attribute the hop correctly.
CALLER_COMPONENT: Final[str] = "api-gateway"

#: ``request.state`` attribute the minted context is bound to.
CALL_CONTEXT_STATE_ATTR: Final[str] = "call_context"

#: ``request.state`` attribute holding the outbound Dapr invocation metadata
#: (``x-custos-callctx`` + ``x-correlation-id``) the downstream router attaches.
OUTBOUND_METADATA_STATE_ATTR: Final[str] = "outbound_metadata"


@dataclass(frozen=True, slots=True)
class MintedCallContext:
    """The signed call-context the gateway forwards downstream.

    ``metadata`` is the exact header map to attach to the Dapr service-invocation
    call: the signed ``x-custos-callctx`` token plus the request
    ``x-correlation-id`` so the whole chain shares one trace id.
    """

    token: str
    correlation_id: str
    metadata: Mapping[str, str]


async def mint_call_context(request: Request) -> MintedCallContext:
    """Sign one call-context for the authorized principal and stage its metadata.

    Reads the :class:`AuthorizedCaller` the authz dependency bound to
    ``request.state`` and the ingress correlation id, asks Auth Service to sign a
    context for ``(principal, workspace, "api-gateway")``, and binds both the
    :class:`MintedCallContext` and the outbound metadata map to ``request.state``
    so the downstream router can attach them.

    Raises:
        GatewayError: ``downstream-unavailable`` (503) when no authorized caller
            is present (the minter was wired without the authz dependency) or the
            Auth Service ``callctx.sign`` call fails.
    """
    caller = getattr(request.state, AUTH_STATE_ATTR, None)
    if not isinstance(caller, AuthorizedCaller):
        raise GatewayError(
            GatewayErrorCode.DOWNSTREAM_UNAVAILABLE,
            detail="Cannot mint a call-context before the request is authorized.",
        )

    client = get_auth_client(request)
    correlation_id: str = request.state.correlation_id

    try:
        signed = await client.callctx_sign(
            CallctxSignRequest(
                principal_id=caller.principal_id,
                caller_component=CALLER_COMPONENT,
                workspace_id=caller.workspace_id,
            )
        )
    except AuthServiceClientError as exc:
        raise GatewayError(
            GatewayErrorCode.DOWNSTREAM_UNAVAILABLE,
            detail="Auth Service could not mint a call-context.",
        ) from exc

    metadata: dict[str, str] = {
        CALLCTX_HEADER: signed.token,
        CORRELATION_ID_HEADER: correlation_id,
    }
    # Bind the single mutable mapping to ``request.state`` for the downstream
    # router to attach, and expose a read-only view of the *same* backing data
    # on the value object so the two can never silently diverge.
    minted = MintedCallContext(
        token=signed.token,
        correlation_id=correlation_id,
        metadata=MappingProxyType(metadata),
    )
    setattr(request.state, CALL_CONTEXT_STATE_ATTR, minted)
    setattr(request.state, OUTBOUND_METADATA_STATE_ATTR, metadata)
    return minted

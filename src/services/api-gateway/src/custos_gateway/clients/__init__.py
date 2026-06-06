"""Outbound clients for the components the API Gateway delegates to.

Each client in this package adapts a Custos Internal RPC surface onto a typed
async method the gateway request pipeline depends on, transported over the local
Dapr sidecar. The lifespan owns the shared :class:`httpx.AsyncClient`; the
clients are constructed onto ``app.state`` and never open sockets at import time.
"""

from __future__ import annotations

from custos_gateway.clients.auth import (
    AuthServiceClient,
    CallctxSignRequest,
    CallctxSignResponse,
    DaprAuthServiceClient,
    DaprEndpoint,
    DeclaredPermission,
    FakeAuthServiceClient,
    NoopAuthServiceClient,
    VerifyAndAuthorizeRequest,
    VerifyAndAuthorizeResponse,
    build_invoke_url,
    read_dapr_endpoint,
)

__all__ = [
    "AuthServiceClient",
    "CallctxSignRequest",
    "CallctxSignResponse",
    "DaprAuthServiceClient",
    "DaprEndpoint",
    "DeclaredPermission",
    "FakeAuthServiceClient",
    "NoopAuthServiceClient",
    "VerifyAndAuthorizeRequest",
    "VerifyAndAuthorizeResponse",
    "build_invoke_url",
    "read_dapr_endpoint",
]

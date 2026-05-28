"""Outbound service clients used by the Catalog Service runtime.

Each sub-module wraps one external dependency in a narrow protocol the
managers can depend on, so unit tests can pass hand-rolled fakes while
the live ASGI app wires the real HTTP / Dapr-invocation implementation.
"""

from __future__ import annotations

from custos_catalog.clients.connector import (
    ConnectorClient,
    ConnectorClientFactory,
    ConnectorServiceUnavailable,
    HttpConnectorClient,
    StubConnectorClient,
    build_connector_client_factory,
)

__all__ = [
    "ConnectorClient",
    "ConnectorClientFactory",
    "ConnectorServiceUnavailable",
    "HttpConnectorClient",
    "StubConnectorClient",
    "build_connector_client_factory",
]

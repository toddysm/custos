"""Verify sidecar tokens before trusting Dapr >=1.18.6 mutual-TLS caller metadata."""

from __future__ import annotations

import hmac
from typing import Protocol


class HeaderValues(Protocol):
    """Duplicate-preserving, case-insensitive HTTP header access."""

    def getlist(self, key: str) -> list[str]:
        """Return every value for a header, retaining duplicates."""
        ...


class MeshAuthenticationError(Exception):
    """A sanitized mesh authentication failure, independent of an HTTP framework."""

    def __init__(self, status_code: int, code: str, detail: str) -> None:
        """Retain only stable error metadata, never token or caller-header values."""
        super().__init__(detail)
        self.status_code, self.code, self.detail = status_code, code, detail


def require_sidecar_token(headers: HeaderValues, token: str | None) -> None:
    """Authenticate the local app hop; missing configuration fails closed."""
    if token is None or not token.strip():
        raise MeshAuthenticationError(
            503, "mesh_unavailable", "sidecar authentication is not configured"
        )
    values = headers.getlist("dapr-api-token")
    if len(values) != 1 or not hmac.compare_digest(values[0].encode(), token.encode()):
        raise MeshAuthenticationError(401, "mesh_unauthenticated", "invalid sidecar authentication")


def require_service_caller(
    headers: HeaderValues, *, token: str | None, app_id: str, namespace: str | None
) -> None:
    """Require the exact deployment-owned caller after authenticating the sidecar hop."""
    require_sidecar_token(headers, token)
    if (
        not app_id.strip()
        or "://" in app_id
        or "/" in app_id
        or not namespace
        or not namespace.strip()
    ):
        raise MeshAuthenticationError(503, "mesh_unavailable", "service identity is not configured")
    if headers.getlist("dapr-caller-app-id") != [app_id] or headers.getlist(
        "dapr-caller-namespace"
    ) != [namespace]:
        raise MeshAuthenticationError(
            403, "mesh_caller_denied", "permitted service identity required"
        )

"""Bounded local Sidecar HTTP transport without platform or third-party dependencies."""

from __future__ import annotations

import http.client
import json
import socket
import time
from typing import TYPE_CHECKING, Any

from copy_image.contract import ActivityError, Sandbox

if TYPE_CHECKING:
    from _typeshed import ReadableBuffer, WriteableBuffer

_TIMEOUT_SECONDS = 15.0
_MAX_RESPONSE_BYTES = 1024 * 1024


class _DeadlineSocket(socket.socket):
    """Apply one elapsed-time budget even when HTTP headers/body arrive a byte at a time."""

    def __init__(self) -> None:
        """Create an owned Unix socket with a fixed deadline for the complete exchange."""
        super().__init__(socket.AF_UNIX, socket.SOCK_STREAM)
        self.deadline = time.monotonic() + _TIMEOUT_SECONDS
        self._remaining()

    def _remaining(self) -> None:
        """Set the next blocking operation's timeout without extending the total budget."""
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("credential exchange deadline exceeded")
        self.settimeout(remaining)

    def sendall(self, data: ReadableBuffer, flags: int = 0) -> None:
        """Bound each HTTP write by the original deadline."""
        self._remaining()
        super().sendall(data, flags)

    def recv_into(self, buffer: WriteableBuffer, nbytes: int = 0, flags: int = 0) -> int:
        """Bound the reads performed by HTTPResponse's buffered socket file."""
        self._remaining()
        return super().recv_into(buffer, nbytes, flags)


def request(
    sandbox: Sandbox,
    slot: str,
    path: str,
    *,
    payload: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Perform one authenticated local exchange; never expose response bodies or causes.

    Authorization failures are permanent; transport/service failures are retryable.
    Successful responses must be bounded JSON objects. Redirects are never followed.
    """
    try:
        token = sandbox.sidecar_token()
        if not token:
            raise ActivityError(f"{slot}.unauthorized", "permanent", "missing Sidecar authority")
        with _DeadlineSocket() as sock:
            sock.connect(str(sandbox.base / "run/connector.sock"))
            connection = http.client.HTTPConnection("localhost")
            connection.sock = sock
            try:
                connection.request(
                    "GET" if payload is None else "POST",
                    path,
                    body=None if payload is None else json.dumps(payload).encode(),
                    headers={
                        "Custos-Sidecar-Token": token,
                        "Content-Type": "application/json",
                    },
                )
                with connection.getresponse() as response:
                    if response.status in {401, 403, 404}:
                        raise ActivityError(
                            f"{slot}.unauthorized",
                            "permanent",
                            "credential request was denied",
                        )
                    if response.status != 200:
                        raise ActivityError(
                            "activity.unexpected_error",
                            "retryable",
                            f"credential broker unavailable for {slot}",
                        )
                    raw = response.read(_MAX_RESPONSE_BYTES + 1)
                    if len(raw) > _MAX_RESPONSE_BYTES:
                        raise ValueError
                    result = json.loads(raw)
                    if not isinstance(result, dict):
                        raise ValueError
                    return result
            finally:
                connection.close()
    except (OSError, http.client.HTTPException):
        raise ActivityError(
            "activity.unexpected_error",
            "retryable",
            f"credential broker exchange failed for {slot}",
        ) from None
    except (ValueError, RecursionError):
        raise ActivityError(
            "activity.contract_violation",
            "permanent",
            f"invalid credential broker response for {slot}",
        ) from None

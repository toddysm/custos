"""Issue existing-format signed bootstrap tokens using independent per-workspace/attempt keys.

The active registry is process-local, not restart recovery or remote revocation.
Private delivery and durable execution ownership must be wired before activation.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable
from datetime import datetime
from secrets import token_bytes as random_bytes
from typing import ClassVar, Final

from custos_callctx.bootstrap import BoundTriple, mint_bootstrap_token

from custos_arm.contract import StepRef
from custos_arm.contract.errors import ErrorClass
from custos_arm.secrets.models import SidecarToken

DEFAULT_TOKEN_BYTES: Final[int] = 32
DEFAULT_BOOTSTRAP_TTL_SECONDS: Final[int] = 3600


class BootstrapDeadlineExceeded(ValueError):
    """A consumed execution deadline is cancellation, not a retryable sandbox failure."""

    code: ClassVar[str] = "activity.timeout"
    error_class: ClassVar[ErrorClass] = ErrorClass.CANCELLED


class BootstrapAuthorityExpired(ValueError):
    """Expired authority cannot be silently replaced or classified as an automatic retry."""

    code: ClassVar[str] = "activity.bootstrap_expired"
    error_class: ClassVar[ErrorClass] = ErrorClass.PERMANENT


class SidecarTokenMinter:
    """Keep one unexpired bootstrap per workspace/attempt without extending it on replay."""

    def __init__(
        self,
        *,
        token_bytes: int = DEFAULT_TOKEN_BYTES,
        ttl_seconds: int = DEFAULT_BOOTSTRAP_TTL_SECONDS,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """Require at least 256 bits of private key entropy and a positive configured lifetime."""
        if type(token_bytes) is not int or token_bytes < DEFAULT_TOKEN_BYTES:
            raise ValueError("token_bytes must provide at least 32 bytes of entropy")
        if type(ttl_seconds) is not int or ttl_seconds < 1:
            raise ValueError("ttl_seconds must be a positive integer")
        self._token_bytes, self._ttl_seconds, self._clock = token_bytes, ttl_seconds, clock
        self._active: dict[tuple[str | None, str, str, int], SidecarToken] = {}

    def mint(
        self,
        *,
        step: StepRef,
        workspace_id: str | None = None,
        deadline: datetime | None = None,
    ) -> SidecarToken:
        """Mint a signed token without rebinding expired or shortened-deadline replays."""
        if workspace_id is not None and not workspace_id.strip():
            raise ValueError("workspace_id must be nonempty when supplied")
        timestamp = self._clock()
        if not math.isfinite(timestamp):
            raise ValueError("bootstrap issuance clock must be finite")
        issued = int(timestamp)
        expires = issued + self._ttl_seconds
        if deadline is not None:
            if deadline.tzinfo is None or deadline.utcoffset() is None:
                raise ValueError("bootstrap deadline must be timezone aware")
            expires = min(expires, int(deadline.timestamp()))
        if expires <= timestamp:
            raise BootstrapDeadlineExceeded("bootstrap deadline has expired")
        scope = (workspace_id, step.run_id, step.step_id, step.attempt)
        existing = self._active.get(scope)
        if existing is not None:
            if existing.expires_at is None or existing.expires_at <= timestamp:
                raise BootstrapAuthorityExpired("existing bootstrap token has expired")
            if existing.expires_at > expires:
                raise ValueError("existing bootstrap lifetime exceeds the requested deadline")
            return existing
        key = random_bytes(self._token_bytes)
        token = SidecarToken(
            value=mint_bootstrap_token(
                key=key,
                triple=BoundTriple(step.run_id, step.step_id, step.attempt),
                ttl_sec=expires - issued,
                now=issued,
            ),
            run_id=step.run_id,
            step_id=step.step_id,
            attempt=step.attempt,
            workspace_id=workspace_id,
            verification_key=key,
            expires_at=expires,
        )
        self._active[scope] = token
        return token

    def revoke(self, token: SidecarToken) -> None:
        """Remove only the matching active generation; remote verification still obeys expiry."""
        key = (token.workspace_id, *token.scope)
        if self._active.get(key) == token:
            self._active.pop(key)

    def is_active(self, token: SidecarToken) -> bool:
        """Check the local registry and expiry, not a remote sidecar's revocation state."""
        return (
            self._active.get((token.workspace_id, *token.scope)) == token
            and token.expires_at is not None
            and self._clock() < token.expires_at
        )


__all__ = ["DEFAULT_TOKEN_BYTES", "SidecarTokenMinter"]

"""Sidecar bootstrap-token minting and revocation (design § Authentication).

ARM mints a fresh bootstrap token for every step attempt, writes it to the
``0400`` ``/custos/in/sidecar-token`` file, and revokes it when the attempt
reaches a terminal state (completion, retry, or cancellation). The token is an
opaque, high-entropy bearer string; the sidecar verifies it against the scope
ARM handed it at pod start. v1 deliberately uses ARM-issued bootstrap tokens —
SPIFFE/SPIRE workload identity is deferred to M3 (REQ-059).

:class:`SidecarTokenMinter` keeps an in-memory registry of the active token per
``(runId, stepId, attempt)`` scope so that:

* a replayed attempt (idempotent ``ScheduleActivity``) re-uses the same token
  rather than minting a second one, and
* :meth:`~SidecarTokenMinter.revoke` can drop the token at step terminal,
  after which :meth:`~SidecarTokenMinter.is_active` reports it dead.
"""

from __future__ import annotations

from secrets import token_urlsafe
from typing import Final

from custos_arm.contract import StepRef
from custos_arm.secrets.models import SidecarToken

#: Number of random bytes behind each minted token. 32 bytes → a 43-char
#: URL-safe base64 string (~256 bits of entropy), comfortably beyond brute
#: force for the lifetime of one step attempt.
DEFAULT_TOKEN_BYTES: Final[int] = 32


class SidecarTokenMinter:
    """Mints and revokes scope-bound sidecar bootstrap tokens.

    One instance is shared across the Scheduler; it is not safe for concurrent
    use across event loops, matching the single-loop ARM runtime.

    :param token_bytes: Entropy (in bytes) behind each minted token value.
    :raises ValueError: If ``token_bytes`` is not strictly positive.
    """

    def __init__(self, *, token_bytes: int = DEFAULT_TOKEN_BYTES) -> None:
        if token_bytes <= 0:
            raise ValueError("token_bytes must be strictly positive")
        self._token_bytes = token_bytes
        self._active: dict[tuple[str, str, int], SidecarToken] = {}

    def mint(self, *, step: StepRef) -> SidecarToken:
        """Return the active token for ``step``, minting one on first call.

        Idempotent per scope: a second call for the same
        ``(runId, stepId, attempt)`` returns the existing token, so a replayed
        attempt does not orphan a live credential.
        """
        scope = (step.run_id, step.step_id, step.attempt)
        existing = self._active.get(scope)
        if existing is not None:
            return existing
        token = SidecarToken(
            value=token_urlsafe(self._token_bytes),
            run_id=step.run_id,
            step_id=step.step_id,
            attempt=step.attempt,
        )
        self._active[scope] = token
        return token

    def revoke(self, token: SidecarToken) -> None:
        """Drop ``token`` from the active registry. Idempotent.

        Revoking a token that was never minted (or already revoked) is a no-op,
        matching the best-effort revocation contract — the caller only cares
        that the token is dead afterwards.
        """
        self._active.pop(token.scope, None)

    def is_active(self, token: SidecarToken) -> bool:
        """Report whether ``token`` is the live token for its scope."""
        return self._active.get(token.scope) == token


__all__ = [
    "DEFAULT_TOKEN_BYTES",
    "SidecarTokenMinter",
]

"""Verify ARM bootstrap tokens using the shared version-one codec.

The public sidecar API and error envelope remain unchanged. The shared HMAC
key belongs only to the bound sidecar, never the activity holding its token.
"""

from __future__ import annotations

import time
from collections.abc import Callable

from custos_callctx.bootstrap import (
    BoundTriple,
    InvalidBootstrapToken,
    mint_bootstrap_token,
    verify_bootstrap_token,
)

from custos_sidecar.errors import SidecarError, SidecarErrorCode

__all__ = ["BootstrapTokenVerifier", "BoundTriple", "mint_bootstrap_token"]


class BootstrapTokenVerifier:
    """Map shared signature/scope/expiry rejection into the sidecar's existing 401 contract."""

    def __init__(
        self, *, key: bytes, triple: BoundTriple, clock: Callable[[], float] | None = None
    ) -> None:
        """Bind one private verification key and immutable attempt to this sidecar."""
        if not isinstance(key, bytes) or not key:
            raise ValueError("BootstrapTokenVerifier requires a non-empty HMAC key")
        self._key = key
        self._triple = triple
        self._clock = clock or time.time

    def verify(self, token: str | None) -> None:
        """Raise the standard private bootstrap error; never bypass a failed verification."""
        try:
            verify_bootstrap_token(token, key=self._key, triple=self._triple, now=self._clock())
        except InvalidBootstrapToken as exc:
            raise SidecarError(SidecarErrorCode.BOOTSTRAP_INVALID, str(exc)) from None

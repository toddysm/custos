"""Typed errors surfaced by :class:`custos_callctx.CallContextVerifier`."""

from __future__ import annotations

from enum import StrEnum


class InvalidReason(StrEnum):
    """Stable categorisation of why a call-context JWT was rejected.

    Callers (middleware in each consuming component) map this enum to
    their own audit-event payloads. Keeping the categories stable lets
    operators write alerts against a small, bounded set of failure
    modes rather than free-form messages.
    """

    MISSING_HEADER = "missing_header"
    MALFORMED_TOKEN = "malformed_token"
    MISSING_KID = "missing_kid"
    UNKNOWN_KID = "unknown_kid"
    BAD_ALGORITHM = "bad_algorithm"
    BAD_SIGNATURE = "bad_signature"
    EXPIRED = "expired"
    NOT_YET_VALID = "not_yet_valid"
    WRONG_AUDIENCE = "wrong_audience"
    WRONG_ISSUER = "wrong_issuer"
    MISSING_CLAIM = "missing_claim"
    JWKS_UNAVAILABLE = "jwks_unavailable"


class InvalidCallContextError(Exception):
    """Raised when an inbound call-context JWT fails verification.

    Args:
        reason: Stable categorisation of the failure (see
            :class:`InvalidReason`).
        detail: Human-readable description, safe to log but **not**
            safe to echo back to external callers as-is — middleware
            should map this to a generic "invalid call context"
            response and emit the verbose form into the audit outbox.
        kid: Optional key id pulled from the JWT header; useful for
            JWKS-cache forensic logging when ``reason`` is
            :attr:`InvalidReason.UNKNOWN_KID`.
    """

    def __init__(
        self,
        reason: InvalidReason,
        detail: str,
        *,
        kid: str | None = None,
    ) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail
        self.kid = kid


__all__ = ["InvalidCallContextError", "InvalidReason"]

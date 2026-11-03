"""Shared version-one bootstrap codec for ARM issuance and sidecar verification.

This is the existing internal HMAC-SHA256 payload format, not a call-context JWT.
Keys stay outside tokens and must be delivered separately to the bound sidecar.
"""

from __future__ import annotations

import base64
import hmac
import json
import math
import time
from dataclasses import dataclass
from typing import Final

MAX_BOOTSTRAP_TOKEN_BYTES: Final[int] = 16 * 1024
_FIELDS = frozenset({"v", "run_id", "step_id", "attempt", "iat", "exp"})


class InvalidBootstrapToken(ValueError):
    """A closed-format, signature, scope or lifetime failure without private token material."""


@dataclass(frozen=True, slots=True)
class BoundTriple:
    """The exact opaque run/step identity and positive attempt served by one sidecar."""

    run_id: str
    step_id: str
    attempt: int

    def __post_init__(self) -> None:
        """Reject impossible scopes without normalizing or splitting opaque identifiers."""
        if any(
            not isinstance(value, str) or not value or "\0" in value
            for value in (self.run_id, self.step_id)
        ):
            raise ValueError("bootstrap scope requires nonempty run and step identifiers")
        if type(self.attempt) is not int or self.attempt < 1:
            raise ValueError("bootstrap scope requires a positive integer attempt")


def _encode(value: bytes) -> str:
    """Encode bytes using the established unpadded base64url representation."""
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _decode(value: str) -> bytes:
    """Reject invalid alphabet/encoding instead of silently discarding input bytes."""
    try:
        return base64.b64decode(
            value + "=" * ((4 - len(value) % 4) % 4), altchars=b"-_", validate=True
        )
    except (ValueError, TypeError):
        raise InvalidBootstrapToken("bootstrap token is not valid base64url") from None


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Prevent duplicate claims from being interpreted differently by different readers."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise InvalidBootstrapToken("bootstrap token has duplicate fields")
        result[key] = value
    return result


def mint_bootstrap_token(
    *, key: bytes, triple: BoundTriple, ttl_sec: int = 3600, now: float | None = None
) -> str:
    """Sign one bounded token using the supplied per-attempt key and lifetime."""
    if not isinstance(key, bytes) or not key:
        raise ValueError("bootstrap signing requires a nonempty key")
    if type(ttl_sec) is not int or ttl_sec < 1:
        raise ValueError("bootstrap lifetime must be a positive integer")
    timestamp = time.time() if now is None else now
    if not math.isfinite(timestamp):
        raise ValueError("bootstrap issuance clock must be finite")
    issued = int(timestamp)
    payload = {
        "v": 1,
        "run_id": triple.run_id,
        "step_id": triple.step_id,
        "attempt": triple.attempt,
        "iat": issued,
        "exp": issued + ttl_sec,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    token = f"{_encode(raw)}.{_encode(hmac.digest(key, raw, 'sha256'))}"
    if len(token) > MAX_BOOTSTRAP_TOKEN_BYTES:
        raise ValueError("bootstrap scope exceeds the token size bound")
    return token


def verify_bootstrap_token(
    token: str | None, *, key: bytes, triple: BoundTriple, now: float | None = None
) -> None:
    """Verify the actual version-one signature, closed claims, exact scope and expiry."""
    if not isinstance(key, bytes) or not key:
        raise ValueError("bootstrap verification requires a nonempty key")
    if not token:
        raise InvalidBootstrapToken("missing Custos-Sidecar-Token header")
    if len(token) > MAX_BOOTSTRAP_TOKEN_BYTES or token.count(".") != 1:
        raise InvalidBootstrapToken("bootstrap token has an invalid envelope")
    payload_part, signature_part = token.split(".", 1)
    raw, signature = _decode(payload_part), _decode(signature_part)
    if not hmac.compare_digest(signature, hmac.digest(key, raw, "sha256")):
        raise InvalidBootstrapToken("bootstrap token signature mismatch")
    try:
        payload = json.loads(raw, object_pairs_hook=_unique_object)
    except (ValueError, RecursionError):
        raise InvalidBootstrapToken("bootstrap token payload is not valid JSON") from None
    if not isinstance(payload, dict) or set(payload) != _FIELDS:
        raise InvalidBootstrapToken("bootstrap token has invalid claim fields")
    if type(payload["v"]) is not int or payload["v"] != 1:
        raise InvalidBootstrapToken("unsupported bootstrap token version")
    if (
        payload["run_id"] != triple.run_id
        or payload["step_id"] != triple.step_id
        or type(payload["attempt"]) is not int
        or payload["attempt"] != triple.attempt
    ):
        raise InvalidBootstrapToken("bootstrap token triple does not match sidecar binding")
    issued, expires = payload["iat"], payload["exp"]
    if type(issued) is not int or type(expires) is not int or expires <= issued:
        raise InvalidBootstrapToken("bootstrap token has an invalid lifetime")
    timestamp = time.time() if now is None else now
    if not math.isfinite(timestamp) or issued > timestamp:
        raise InvalidBootstrapToken("bootstrap token issuance is in the future")
    if timestamp >= expires:
        raise InvalidBootstrapToken("bootstrap token expired")

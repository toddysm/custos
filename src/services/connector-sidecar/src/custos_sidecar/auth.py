"""Bootstrap-token verifier (CONN-IMPL-019).

The Activity Runtime Manager (ARM) mints a short-lived bootstrap token
at sidecar start, drops it into the activity container's
``/custos/in/sidecar-token`` (tmpfs, mode ``0400``), and binds it to
the specific ``(runId, stepId, attempt)`` triple the sidecar is
serving. Every activity request to the sidecar UDS must carry the
token in the ``Custos-Sidecar-Token`` header; the sidecar verifies the
signature against a shared HMAC key + the bound triple.

This module is intentionally small: token format is
``<b64url-payload>.<b64url-sig>``; the payload is a compact JSON
object; the signature is HMAC-SHA256 over the raw payload bytes; and
verification is constant-time. No JWT library because the format is
internal-only and the trade-off (one less dep, no JWS spec footguns)
is worth the bespoke parser.

The verifier is constructed with the sidecar's bound triple and the
shared key; tests inject both via the constructor without touching
the filesystem.

CONN-IMPL-020 will replace this in-band header with a stronger
ed25519-signed token shipped over the mTLS control channel; for #019
the HMAC scheme matches the design's "signed by ARM with a key the
sidecar has been issued" wording and gives us a complete unit-testable
verifier in one file.
"""

from __future__ import annotations

import base64
import hmac
import json
import time
from dataclasses import dataclass
from typing import Any, Final

from custos_sidecar.errors import SidecarError, SidecarErrorCode

#: Token format version. Bumped if the payload shape changes; verifier
#: rejects tokens with a different ``v`` so old/new sidecars do not
#: silently accept each other's tokens during a roll.
_TOKEN_VERSION: Final[int] = 1


def _b64url_decode(value: str) -> bytes:
    """Decode unpadded base64url; raise ``SidecarError`` on malformed input."""
    padding = "=" * ((4 - len(value) % 4) % 4)
    try:
        return base64.urlsafe_b64decode(value + padding)
    except (ValueError, TypeError) as exc:
        raise SidecarError(
            SidecarErrorCode.BOOTSTRAP_INVALID,
            "bootstrap token is not valid base64url",
        ) from exc


def _b64url_encode(value: bytes) -> str:
    """Encode bytes as unpadded base64url."""
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


@dataclass(frozen=True, slots=True)
class BoundTriple:
    """The ``(runId, stepId, attempt)`` the sidecar serves.

    Set once at sidecar start from ARM-supplied environment input and
    immutable for the sidecar's lifetime. Every bootstrap token must
    decode to this exact triple or it is rejected.
    """

    run_id: str
    step_id: str
    attempt: int


def mint_bootstrap_token(
    *,
    key: bytes,
    triple: BoundTriple,
    ttl_sec: int = 3600,
    now: float | None = None,
) -> str:
    """Mint a bootstrap token bound to ``triple``.

    Pure helper exposed for tests and for the integration harness;
    production minting happens out-of-band in ARM, which speaks the
    same payload schema.
    """
    issued_at = now if now is not None else time.time()
    payload: dict[str, Any] = {
        "v": _TOKEN_VERSION,
        "run_id": triple.run_id,
        "step_id": triple.step_id,
        "attempt": triple.attempt,
        "iat": int(issued_at),
        "exp": int(issued_at) + int(ttl_sec),
    }
    payload_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    sig = hmac.new(key, payload_bytes, "sha256").digest()
    return f"{_b64url_encode(payload_bytes)}.{_b64url_encode(sig)}"


class BootstrapTokenVerifier:
    """Verify ``Custos-Sidecar-Token`` headers against the bound triple.

    The verifier holds the shared HMAC key + the sidecar's bound triple
    and rejects any token that:

    * Is malformed (missing ``.`` separator, non-base64 segment).
    * Has an invalid signature (wrong key or tampered payload).
    * Decodes to a different ``(runId, stepId, attempt)`` than the
      sidecar was started with.
    * Has an ``exp`` claim in the past (clock-skew tolerance defaults
      to zero; in practice the token TTL absorbs sub-second drift).

    All rejection paths raise :class:`SidecarError(BOOTSTRAP_INVALID)`
    so the router can render the canonical 401 problem document
    without leaking *why* a given token was rejected (the audit log
    captures the precise reason).
    """

    def __init__(
        self,
        *,
        key: bytes,
        triple: BoundTriple,
        clock: Any = None,
    ) -> None:
        if not key:
            raise ValueError("BootstrapTokenVerifier requires a non-empty HMAC key")
        self._key = key
        self._triple = triple
        self._clock = clock if clock is not None else time.time

    def verify(self, token: str | None) -> None:
        """Raise :class:`SidecarError(BOOTSTRAP_INVALID)` on any failure.

        Returns ``None`` on success so callers can treat verification
        as a guard. Constant-time signature comparison via
        :func:`hmac.compare_digest`.
        """
        if not token:
            raise SidecarError(
                SidecarErrorCode.BOOTSTRAP_INVALID,
                "missing Custos-Sidecar-Token header",
            )
        if "." not in token:
            raise SidecarError(
                SidecarErrorCode.BOOTSTRAP_INVALID,
                "bootstrap token missing '.' separator",
            )
        payload_b64, sig_b64 = token.rsplit(".", 1)
        payload_bytes = _b64url_decode(payload_b64)
        sig = _b64url_decode(sig_b64)
        expected = hmac.new(self._key, payload_bytes, "sha256").digest()
        if not hmac.compare_digest(sig, expected):
            raise SidecarError(
                SidecarErrorCode.BOOTSTRAP_INVALID,
                "bootstrap token signature mismatch",
            )
        try:
            payload = json.loads(payload_bytes)
        except json.JSONDecodeError as exc:
            raise SidecarError(
                SidecarErrorCode.BOOTSTRAP_INVALID,
                "bootstrap token payload is not valid JSON",
            ) from exc
        if not isinstance(payload, dict):
            raise SidecarError(
                SidecarErrorCode.BOOTSTRAP_INVALID,
                "bootstrap token payload is not a JSON object",
            )
        if payload.get("v") != _TOKEN_VERSION:
            raise SidecarError(
                SidecarErrorCode.BOOTSTRAP_INVALID,
                f"unsupported bootstrap token version {payload.get('v')!r}",
            )
        run_id = payload.get("run_id")
        step_id = payload.get("step_id")
        attempt = payload.get("attempt")
        if (
            run_id != self._triple.run_id
            or step_id != self._triple.step_id
            or attempt != self._triple.attempt
        ):
            raise SidecarError(
                SidecarErrorCode.BOOTSTRAP_INVALID,
                "bootstrap token triple does not match sidecar binding",
            )
        exp = payload.get("exp")
        if not isinstance(exp, int):
            raise SidecarError(
                SidecarErrorCode.BOOTSTRAP_INVALID,
                "bootstrap token missing or non-integer exp",
            )
        if self._clock() >= exp:
            raise SidecarError(
                SidecarErrorCode.BOOTSTRAP_INVALID,
                "bootstrap token expired",
            )


__all__ = ["BootstrapTokenVerifier", "BoundTriple", "mint_bootstrap_token"]

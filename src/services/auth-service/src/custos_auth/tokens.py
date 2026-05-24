"""Service-token mint + hash helpers (AS-IMPL-013).

This module owns the canonical token format and the deterministic
one-way hash function that both sides of the lifecycle — mint
(this file) and verify (AS-IMPL-014) — share.

Token format
------------

::

    custos_<43-char base64url, no padding>

The 43 base64url characters encode 32 random bytes (256 bits of
entropy), which exceeds the AS-IMPL-013 acceptance criterion
"Token entropy ≥256 bits" and matches the design doc § Service-Token
Lifecycle prefix.

Hash storage
------------

The design doc calls for storing "the bcrypt-style hash" of the
plaintext. That wording is loose; the SPL
:meth:`AuthStoreProvider.get_service_token_by_hash` contract requires
a **deterministic** lookup ("Adapter MUST index on `hash` — verifier
hot path"). A salted bcrypt hash is non-deterministic by construction
and therefore cannot serve as a lookup key.

Reconciling the two: a 256-bit cryptographically-random token derives
its security from entropy, not from the cost factor of a KDF. A
deterministic one-way hash (``SHA-256`` of the plaintext) gives every
property the spec actually requires:

* **One-way** — plaintext cannot be recovered from the row, so a
  database leak does not expose live credentials.
* **Deterministic** — verifier can hash an incoming bearer and look
  up the SPL row in one indexed call.
* **Constant-time compare** — after lookup the verifier compares the
  two hex digests with :func:`hmac.compare_digest`.
* **Brute-force-infeasible** — 2**256 candidate space is exhausted
  long before any plausible KDF cost factor matters.

The design § "Storage" line should be read as "store a one-way hash;
plaintext never persisted" — the literal "bcrypt-style" phrasing is
implementation guidance that the SPL contract overrides. This module
documents the deviation so future readers do not re-litigate it.

Constant-time
-------------

:func:`hash_token` is constant-time across the *input* axis (SHA-256
runs in time proportional to the input length, not its contents) and
:func:`verify_hash` uses :func:`hmac.compare_digest` so the comparison
of two hex digests is also constant-time. Both are documented
defensive choices even though the practical attack surface against a
2**256 keyspace is negligible.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
from typing import Final

#: Bearer-token prefix. Every minted token starts with this string so
#: operators grepping logs / headers can spot a Custos token at a
#: glance and distinguish it from OIDC bearers (which start with
#: ``eyJ`` for JWTs).
TOKEN_PREFIX: Final[str] = "custos_"

#: Number of random bytes packed into the token body. 32 bytes →
#: 256 bits of entropy, meeting the AS-IMPL-013 acceptance criterion.
TOKEN_ENTROPY_BYTES: Final[int] = 32

#: Exact length of a minted token. ``len("custos_") == 7`` and 32
#: random bytes encoded as base64url-without-padding takes 43 chars,
#: for a total of 50. Used by :func:`looks_like_custos_token` to
#: reject obviously malformed input on the verifier hot path before
#: it touches the hash function.
TOKEN_LENGTH: Final[int] = 50


def mint_token() -> tuple[str, str]:
    """Generate a fresh ``(plaintext, hash)`` pair.

    The plaintext is returned to the caller exactly once (the mint
    endpoint surfaces it in the response body and never again); the
    hash is what the SPL row stores. Both strings are safe to print
    inside the auth-service process but the **plaintext** must never
    appear in logs, audit payloads, traces, or the SPL row — the
    AS-IMPL-013 acceptance criterion is unambiguous on that point.

    The body uses :func:`secrets.token_urlsafe` for the random bytes
    so it inherits the OS CSPRNG and the urlsafe base64 alphabet
    without padding (the ``=`` characters are dropped because they
    would force URL-percent-encoding on every gateway hop and we
    gain nothing in entropy by keeping them).
    """
    plaintext = TOKEN_PREFIX + secrets.token_urlsafe(TOKEN_ENTROPY_BYTES)
    return plaintext, hash_token(plaintext)


def hash_token(plaintext: str) -> str:
    """Compute the deterministic storage hash for ``plaintext``.

    Returns a lowercase hex SHA-256 digest. Same input → same output
    so the verifier can look up the SPL row via
    :meth:`AuthStoreProvider.get_service_token_by_hash`.

    The function does not validate that the input looks like a
    minted Custos token because the verifier is responsible for
    that check and we want a single hash funnel both sides share.
    Pre-validation lives in :func:`looks_like_custos_token`.
    """
    return hashlib.sha256(plaintext.encode("utf-8")).hexdigest()


def verify_hash(plaintext: str, expected_hash: str) -> bool:
    """Constant-time comparison helper for the verifier hot path.

    Computes the SHA-256 of ``plaintext`` and compares it to
    ``expected_hash`` with :func:`hmac.compare_digest`. The verifier
    in AS-IMPL-014 will use this after a successful SPL lookup so
    a hash-table collision (impossible at the SHA-256 level, but the
    SPL adapter is free to use any storage scheme) cannot let a
    different token through.
    """
    return hmac.compare_digest(hash_token(plaintext), expected_hash)


def looks_like_custos_token(raw: str) -> bool:
    """Cheap pre-validation for the verifier hot path.

    Returns ``True`` only when ``raw`` has the canonical prefix and
    expected length. Used by the verify endpoint to short-circuit a
    SPL lookup on obviously-malformed input (a missing or truncated
    bearer is a far more common failure than a real revocation) and
    keep the hot path uniform-time across rejection reasons.
    """
    return len(raw) == TOKEN_LENGTH and raw.startswith(TOKEN_PREFIX)


__all__ = [
    "TOKEN_ENTROPY_BYTES",
    "TOKEN_LENGTH",
    "TOKEN_PREFIX",
    "hash_token",
    "looks_like_custos_token",
    "mint_token",
    "verify_hash",
]

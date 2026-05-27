"""Webhook signature verification (CONN-IMPL-025, #308).

The push receiver (:func:`custos_connector.listen.router.post_events`)
verifies every inbound webhook through a pluggable
:class:`SignatureVerifier`. Three implementations ship in this slice:

* :class:`RejectAllSignatureVerifier` — production-safe default. Until
  a follow-up plumbs per-instance HMAC secret resolution through the
  Identity Resolver Registry, every request is denied so the surface
  cannot be exploited by an unauthenticated caller. Operators that
  need the push receiver enabled today configure
  :class:`HmacSignatureVerifier` explicitly.
* :class:`HmacSignatureVerifier` — verifies ``X-Custos-Signature: sha256=<hex>``
  where ``<hex>`` is the lowercase hex HMAC-SHA256 of the raw request
  body using a per-instance shared secret. The verifier is wired by
  the test suite today; production wiring (where the secret comes
  from instance config / identity registry) is a follow-up.
* :class:`AllowAllSignatureVerifier` — **test only**. Disabled in
  production by the constructor's explicit ``__test_only__`` marker
  pattern (see the assertion in :meth:`__init__`).

The Protocol is intentionally request-scoped: every verifier call
receives the raw bytes body and headers from the FastAPI request, so
implementations can pick their own canonicalisation scheme without
the router having to know about it. Verifiers are stateless apart
from any constructor-supplied secret material; they are safe to
share across coroutines.

The design's "Plugin signature verification out of scope" line refers
to supply-chain (container-image signing); inbound webhook
verification is in scope for CONN-IMPL-025 because push receivers
that don't authenticate the caller are a textbook OWASP A07
(broken authentication) issue.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Final, Protocol

_LOGGER: Final[logging.Logger] = logging.getLogger("custos_connector.listen.signature")

__all__ = [
    "AllowAllSignatureVerifier",
    "HmacSignatureVerifier",
    "RejectAllSignatureVerifier",
    "SignatureVerificationFailure",
    "SignatureVerifier",
]


#: Header the platform expects the upstream webhook signer to use.
#: Mirrors the GitHub / Stripe convention (``sha256=<hex>``) so most
#: upstreams require no custom signing code on the integrator side.
SIGNATURE_HEADER: Final[str] = "X-Custos-Signature"
#: HMAC algorithm prefix on the header value. Future algorithm
#: rotations (e.g. ``sha512=...``) land as additional prefixes
#: the verifier accepts, gated on the connector type's manifest.
SIGNATURE_ALG_PREFIX_SHA256: Final[str] = "sha256="


class SignatureVerificationFailure(Exception):
    """Raised by verifiers that prefer explicit failure over a bool.

    The Protocol contract is "return False on failure"; this exception
    exists so :class:`HmacSignatureVerifier` (and future asymmetric
    verifiers) can distinguish "header malformed" from "header
    valid but signature wrong" in the WARNING log line. The router
    catches both into a single 401 response regardless.
    """


class SignatureVerifier(Protocol):
    """Verify the signature on an inbound webhook POST.

    Implementations MUST be constant-time on the cryptographic
    compare so a timing-side-channel attacker cannot probe the
    shared secret.
    """

    async def verify(
        self,
        *,
        body: bytes,
        headers: Mapping[str, str],
        instance_id: str,
    ) -> bool:
        """Return ``True`` if the request is authenticated.

        Args:
            body: The raw bytes of the request body. Implementations
                MUST sign over the raw bytes, not the decoded JSON,
                because re-encoding loses canonical ordering and
                breaks the HMAC.
            headers: Lower-cased request headers (FastAPI's
                normalized form). Implementations look up
                :data:`SIGNATURE_HEADER` (case-insensitive) themselves.
            instance_id: Used by implementations that key the shared
                secret by instance (so one connector instance's
                secret rotation does not affect siblings).
        """
        ...


class RejectAllSignatureVerifier:
    """Production-safe default — denies every request.

    Until follow-up wiring resolves the per-instance HMAC secret out
    of instance config / the identity registry, leaving the push
    receiver in this state is the correct safety posture: the route
    answers ``401 webhook.signature_invalid`` and no plugin event is
    ever ingested off an unauthenticated request.
    """

    async def verify(
        self,
        *,
        body: bytes,
        headers: Mapping[str, str],
        instance_id: str,
    ) -> bool:
        _LOGGER.info(
            "rejecting webhook (RejectAllSignatureVerifier) instance=%s",
            instance_id,
        )
        return False


class AllowAllSignatureVerifier:
    """Test-only verifier that accepts every request.

    Constructor takes a mandatory ``test_only`` flag to prevent
    accidental production wiring. The flag has no runtime effect
    beyond the assertion; the comment is a guard for code review.
    Production deployments MUST use :class:`HmacSignatureVerifier`
    or :class:`RejectAllSignatureVerifier`.
    """

    def __init__(self, *, test_only: bool) -> None:
        # ``test_only`` is mandatory and must be True. The keyword-only
        # form forces every call site to spell out "yes, this is a
        # test"; a search for ``AllowAllSignatureVerifier`` in a code
        # review of a production wiring change is then a stop-the-line
        # signal.
        assert test_only, (
            "AllowAllSignatureVerifier must not be used in production. "
            "Use RejectAllSignatureVerifier (default) or HmacSignatureVerifier."
        )

    async def verify(
        self,
        *,
        body: bytes,
        headers: Mapping[str, str],
        instance_id: str,
    ) -> bool:
        return True


class HmacSignatureVerifier:
    """Verifies ``sha256=<hex>`` HMAC over the raw request body.

    Constructor takes a ``secret_lookup`` callable so the secret
    plumbing (instance config? identity registry? sealed-secret on
    disk?) is policy on the caller side. The callable receives the
    ``instance_id`` and returns the secret bytes, or ``None`` if no
    secret is configured for that instance (in which case the
    verifier returns ``False`` — fail-closed). The callable is
    invoked on every request so secret rotations take effect
    immediately without re-wiring.

    Verification is constant-time via :func:`hmac.compare_digest`
    so the secret cannot be probed via timing attacks.
    """

    __slots__ = ("_secret_lookup",)

    def __init__(
        self,
        *,
        secret_lookup: _SecretLookup,
    ) -> None:
        self._secret_lookup = secret_lookup

    async def verify(
        self,
        *,
        body: bytes,
        headers: Mapping[str, str],
        instance_id: str,
    ) -> bool:
        header_value = _lookup_header(headers, SIGNATURE_HEADER)
        if header_value is None:
            _LOGGER.warning(
                "webhook rejected: missing %s header instance=%s",
                SIGNATURE_HEADER,
                instance_id,
            )
            return False
        if not header_value.startswith(SIGNATURE_ALG_PREFIX_SHA256):
            _LOGGER.warning(
                "webhook rejected: unsupported signature algorithm header=%r instance=%s",
                header_value[:32],
                instance_id,
            )
            return False
        provided_hex = header_value[len(SIGNATURE_ALG_PREFIX_SHA256) :]
        try:
            provided = bytes.fromhex(provided_hex)
        except ValueError:
            _LOGGER.warning(
                "webhook rejected: malformed hex in %s instance=%s",
                SIGNATURE_HEADER,
                instance_id,
            )
            return False
        secret = await self._secret_lookup(instance_id)
        if secret is None:
            _LOGGER.warning(
                "webhook rejected: no signing secret configured instance=%s",
                instance_id,
            )
            return False
        expected = hmac.new(secret, body, hashlib.sha256).digest()
        if not hmac.compare_digest(provided, expected):
            _LOGGER.warning("webhook rejected: signature mismatch instance=%s", instance_id)
            return False
        return True


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


#: Signature of the secret-lookup callable
#: :class:`HmacSignatureVerifier` calls per request. Returns ``None``
#: when no secret is configured (fail-closed).
_SecretLookup = Callable[[str], Awaitable[bytes | None]]


def _lookup_header(headers: Mapping[str, str], name: str) -> str | None:
    """Case-insensitive header lookup.

    FastAPI's :class:`starlette.datastructures.Headers` is already
    case-insensitive but the Protocol takes a plain ``Mapping[str, str]``
    so tests can pass a regular dict. This helper smooths that over.
    """
    lowered = name.lower()
    for key, value in headers.items():
        if key.lower() == lowered:
            return value
    return None

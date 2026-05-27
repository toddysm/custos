"""Unit tests for :mod:`custos_connector.listen.signature` (CONN-IMPL-025, #308).

Covers:

* :class:`RejectAllSignatureVerifier` denies every request.
* :class:`AllowAllSignatureVerifier` requires the ``test_only`` flag
  and accepts every request when set.
* :class:`HmacSignatureVerifier` accepts a valid ``sha256=<hex>``
  signature, rejects missing / wrong-algorithm / malformed-hex /
  mismatched signatures, and is fail-closed when no secret is
  configured for the instance.
"""

from __future__ import annotations

import hashlib
import hmac

import pytest

from custos_connector.listen import (
    AllowAllSignatureVerifier,
    HmacSignatureVerifier,
    RejectAllSignatureVerifier,
)

pytestmark = pytest.mark.asyncio


_BODY = b'{"events":[{"eventId":"e1","eventType":"oci.image.pushed"}]}'
_SECRET = b"shhhh"
_INSTANCE = "inst-1"


def _hmac_header(body: bytes, secret: bytes) -> str:
    digest = hmac.new(secret, body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


async def test_reject_all_denies_everything() -> None:
    verifier = RejectAllSignatureVerifier()
    ok = await verifier.verify(body=_BODY, headers={}, instance_id=_INSTANCE)
    assert ok is False


async def test_allow_all_requires_test_only_flag() -> None:
    # Use ``raise RuntimeError`` (not ``assert``) so the guard fires
    # even under ``python -O`` / ``PYTHONOPTIMIZE=1``. Any of those
    # environments would silently strip an ``assert`` and accept
    # every webhook in production.
    with pytest.raises(RuntimeError, match="must not be used in production"):
        AllowAllSignatureVerifier(test_only=False)
    verifier = AllowAllSignatureVerifier(test_only=True)
    ok = await verifier.verify(body=_BODY, headers={}, instance_id=_INSTANCE)
    assert ok is True


async def test_hmac_valid_signature_accepted() -> None:
    async def lookup(_instance_id: str) -> bytes | None:
        return _SECRET

    verifier = HmacSignatureVerifier(secret_lookup=lookup)
    headers = {"X-Custos-Signature": _hmac_header(_BODY, _SECRET)}
    ok = await verifier.verify(body=_BODY, headers=headers, instance_id=_INSTANCE)
    assert ok is True


async def test_hmac_valid_signature_case_insensitive_header() -> None:
    async def lookup(_instance_id: str) -> bytes | None:
        return _SECRET

    verifier = HmacSignatureVerifier(secret_lookup=lookup)
    headers = {"x-custos-signature": _hmac_header(_BODY, _SECRET)}
    assert await verifier.verify(body=_BODY, headers=headers, instance_id=_INSTANCE) is True


async def test_hmac_missing_header_denied() -> None:
    async def lookup(_instance_id: str) -> bytes | None:
        return _SECRET

    verifier = HmacSignatureVerifier(secret_lookup=lookup)
    assert await verifier.verify(body=_BODY, headers={}, instance_id=_INSTANCE) is False


async def test_hmac_unsupported_algorithm_denied() -> None:
    async def lookup(_instance_id: str) -> bytes | None:
        return _SECRET

    verifier = HmacSignatureVerifier(secret_lookup=lookup)
    headers = {"X-Custos-Signature": "md5=deadbeef"}
    assert await verifier.verify(body=_BODY, headers=headers, instance_id=_INSTANCE) is False


async def test_hmac_malformed_hex_denied() -> None:
    async def lookup(_instance_id: str) -> bytes | None:
        return _SECRET

    verifier = HmacSignatureVerifier(secret_lookup=lookup)
    headers = {"X-Custos-Signature": "sha256=not-hex"}
    assert await verifier.verify(body=_BODY, headers=headers, instance_id=_INSTANCE) is False


async def test_hmac_no_secret_configured_fail_closed() -> None:
    async def lookup(_instance_id: str) -> bytes | None:
        return None

    verifier = HmacSignatureVerifier(secret_lookup=lookup)
    headers = {"X-Custos-Signature": _hmac_header(_BODY, _SECRET)}
    assert await verifier.verify(body=_BODY, headers=headers, instance_id=_INSTANCE) is False


async def test_hmac_signature_mismatch_denied() -> None:
    async def lookup(_instance_id: str) -> bytes | None:
        return _SECRET

    verifier = HmacSignatureVerifier(secret_lookup=lookup)
    headers = {"X-Custos-Signature": _hmac_header(b"other-body", _SECRET)}
    assert await verifier.verify(body=_BODY, headers=headers, instance_id=_INSTANCE) is False


async def test_hmac_body_tampering_detected() -> None:
    """Bit-flip in body must invalidate signature."""

    async def lookup(_instance_id: str) -> bytes | None:
        return _SECRET

    verifier = HmacSignatureVerifier(secret_lookup=lookup)
    sig = _hmac_header(_BODY, _SECRET)
    tampered = _BODY[:-1] + b"X"
    ok = await verifier.verify(
        body=tampered,
        headers={"X-Custos-Signature": sig},
        instance_id=_INSTANCE,
    )
    assert ok is False

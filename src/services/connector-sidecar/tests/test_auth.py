"""Unit tests for :mod:`custos_sidecar.auth`."""

from __future__ import annotations

import base64
import json

import pytest

from custos_sidecar.auth import (
    BootstrapTokenVerifier,
    BoundTriple,
    mint_bootstrap_token,
)
from custos_sidecar.errors import SidecarError, SidecarErrorCode

KEY = b"k" * 32
TRIPLE = BoundTriple(run_id="r1", step_id="s1", attempt=1)


def _verifier(now: float = 1_000.0) -> BootstrapTokenVerifier:
    return BootstrapTokenVerifier(key=KEY, triple=TRIPLE, clock=lambda: now)


def test_round_trip_verifies():
    token = mint_bootstrap_token(key=KEY, triple=TRIPLE, ttl_sec=60, now=1_000.0)
    _verifier(now=1_001.0).verify(token)


def test_missing_token_rejected():
    with pytest.raises(SidecarError) as info:
        _verifier().verify(None)
    assert info.value.code is SidecarErrorCode.BOOTSTRAP_INVALID


def test_missing_separator_rejected():
    with pytest.raises(SidecarError) as info:
        _verifier().verify("nopointhere")
    assert info.value.code is SidecarErrorCode.BOOTSTRAP_INVALID


def test_bad_base64_rejected():
    with pytest.raises(SidecarError) as info:
        _verifier().verify("***not-base64***.also")
    assert info.value.code is SidecarErrorCode.BOOTSTRAP_INVALID


def test_signature_mismatch_rejected():
    token = mint_bootstrap_token(key=KEY, triple=TRIPLE, ttl_sec=60, now=1_000.0)
    other = BootstrapTokenVerifier(
        key=b"DIFFERENTKEYDIFFERENTKEYDIFFERENTKEY",
        triple=TRIPLE,
        clock=lambda: 1_001.0,
    )
    with pytest.raises(SidecarError) as info:
        other.verify(token)
    assert info.value.code is SidecarErrorCode.BOOTSTRAP_INVALID


def test_triple_mismatch_rejected():
    token = mint_bootstrap_token(
        key=KEY,
        triple=BoundTriple(run_id="other", step_id="s1", attempt=1),
        ttl_sec=60,
        now=1_000.0,
    )
    with pytest.raises(SidecarError) as info:
        _verifier(now=1_001.0).verify(token)
    assert info.value.code is SidecarErrorCode.BOOTSTRAP_INVALID


def test_expired_rejected():
    token = mint_bootstrap_token(key=KEY, triple=TRIPLE, ttl_sec=60, now=1_000.0)
    with pytest.raises(SidecarError) as info:
        _verifier(now=2_000.0).verify(token)
    assert info.value.code is SidecarErrorCode.BOOTSTRAP_INVALID


def _resign(payload: dict[str, object], *, key: bytes = KEY) -> str:
    import hmac

    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    sig = hmac.new(key, raw, "sha256").digest()
    return (
        base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
        + "."
        + base64.urlsafe_b64encode(sig).rstrip(b"=").decode("ascii")
    )


def test_wrong_version_rejected():
    token = _resign({"v": 99, "run_id": "r1", "step_id": "s1", "attempt": 1, "iat": 1, "exp": 9999})
    with pytest.raises(SidecarError) as info:
        _verifier().verify(token)
    assert info.value.code is SidecarErrorCode.BOOTSTRAP_INVALID


def test_non_object_payload_rejected():
    import hmac

    raw = b"[1,2,3]"
    sig = hmac.new(KEY, raw, "sha256").digest()
    token = (
        base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
        + "."
        + base64.urlsafe_b64encode(sig).rstrip(b"=").decode("ascii")
    )
    with pytest.raises(SidecarError) as info:
        _verifier().verify(token)
    assert info.value.code is SidecarErrorCode.BOOTSTRAP_INVALID


def test_missing_exp_rejected():
    token = _resign({"v": 1, "run_id": "r1", "step_id": "s1", "attempt": 1, "iat": 1})
    with pytest.raises(SidecarError) as info:
        _verifier().verify(token)
    assert info.value.code is SidecarErrorCode.BOOTSTRAP_INVALID


def test_non_int_exp_rejected():
    token = _resign(
        {
            "v": 1,
            "run_id": "r1",
            "step_id": "s1",
            "attempt": 1,
            "iat": 1,
            "exp": "soon",
        }
    )
    with pytest.raises(SidecarError) as info:
        _verifier().verify(token)
    assert info.value.code is SidecarErrorCode.BOOTSTRAP_INVALID


def test_empty_key_rejected_at_construction():
    with pytest.raises(ValueError):
        BootstrapTokenVerifier(key=b"", triple=TRIPLE)

"""Compatibility and negative cases for the existing version-one bootstrap wire format."""

import base64
import hmac
import json

import pytest

from custos_callctx.bootstrap import (
    MAX_BOOTSTRAP_TOKEN_BYTES,
    BoundTriple,
    InvalidBootstrapToken,
    mint_bootstrap_token,
    verify_bootstrap_token,
)

KEY = b"k" * 32
SCOPE = BoundTriple("run/opaque", "step", 1)


def signed(raw: bytes) -> str:
    """Construct an independent version-one fixture, including malformed signed JSON."""
    return ".".join(
        base64.urlsafe_b64encode(part).rstrip(b"=").decode()
        for part in (raw, hmac.digest(KEY, raw, "sha256"))
    )


def test_existing_wire_format_and_exact_expiry() -> None:
    """The producer retains the original field names, encoding, algorithm and expiry boundary."""
    token = mint_bootstrap_token(key=KEY, triple=SCOPE, ttl_sec=60, now=1000)
    raw = json.dumps(
        {
            "v": 1,
            "run_id": "run/opaque",
            "step_id": "step",
            "attempt": 1,
            "iat": 1000,
            "exp": 1060,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    assert token == signed(raw)
    verify_bootstrap_token(token, key=KEY, triple=SCOPE, now=1059.999)
    with pytest.raises(InvalidBootstrapToken, match="expired"):
        verify_bootstrap_token(token, key=KEY, triple=SCOPE, now=1060)


@pytest.mark.parametrize("token", [None, "", "opaque", "a.b.c", "***.a", "\u2603.a", "x" * 16385])
def test_malformed_envelopes_fail_closed(token: str | None) -> None:
    """Malformed encoding and oversized headers cannot become accepted bootstrap authority."""
    with pytest.raises(InvalidBootstrapToken):
        verify_bootstrap_token(token, key=KEY, triple=SCOPE, now=1000)


@pytest.mark.parametrize(
    "change",
    [
        {"v": True},
        {"v": 2},
        {"attempt": True},
        {"attempt": 2},
        {"run_id": "other"},
        {"step_id": "other"},
        {"iat": True},
        {"iat": 1001},
        {"exp": True},
        {"exp": "later"},
        {"exp": 1000},
        {"extra": "denied"},
    ],
)
def test_signed_invalid_claims_are_rejected(change: dict[str, object]) -> None:
    """A trusted signature cannot excuse a widened or malformed scope/lifetime contract."""
    payload = {
        "v": 1,
        "run_id": SCOPE.run_id,
        "step_id": SCOPE.step_id,
        "attempt": 1,
        "iat": 1000,
        "exp": 1060,
        **change,
    }
    with pytest.raises(InvalidBootstrapToken):
        verify_bootstrap_token(
            signed(json.dumps(payload).encode()), key=KEY, triple=SCOPE, now=1000
        )


@pytest.mark.parametrize("raw", [b"[]", b"{bad", b"\xff", b'{"v":1,"v":1}'])
def test_noncanonical_signed_payloads_are_rejected(raw: bytes) -> None:
    """Non-object, duplicate, malformed and invalid UTF-8 payloads have explicit closed failures."""
    with pytest.raises(InvalidBootstrapToken):
        verify_bootstrap_token(signed(raw), key=KEY, triple=SCOPE, now=1000)


def test_wrong_key_fails_without_exposing_material() -> None:
    """Verification errors contain no token or private key value."""
    token = mint_bootstrap_token(key=KEY, triple=SCOPE, now=1000)
    with pytest.raises(InvalidBootstrapToken) as failed:
        verify_bootstrap_token(token, key=b"z" * 32, triple=SCOPE, now=1000)
    assert token not in str(failed.value) and str(KEY) not in str(failed.value)


@pytest.mark.parametrize("ttl", [0, -1, True])
def test_invalid_issuance_lifetime(ttl: int) -> None:
    """The issuer cannot mint a zero, negative or boolean lifetime."""
    with pytest.raises(ValueError):
        mint_bootstrap_token(key=KEY, triple=SCOPE, ttl_sec=ttl, now=1000)


def test_invalid_issuance_configuration() -> None:
    """Missing keys, non-finite clocks and oversized scopes fail before a token is returned."""
    with pytest.raises(ValueError):
        mint_bootstrap_token(key=b"", triple=SCOPE)
    with pytest.raises(ValueError):
        verify_bootstrap_token("a.b", key=b"", triple=SCOPE)
    with pytest.raises(ValueError):
        mint_bootstrap_token(key=KEY, triple=SCOPE, now=float("nan"))
    with pytest.raises(ValueError):
        mint_bootstrap_token(key=KEY, triple=BoundTriple("x" * MAX_BOOTSTRAP_TOKEN_BYTES, "s", 1))
    for run, step, attempt in [("", "s", 1), ("r", "\0", 1), ("r", "s", 0), ("r", "s", True)]:
        with pytest.raises(ValueError):
            BoundTriple(run, step, attempt)

"""Tests for ``custos_auth.oidc.verifier`` (AS-IMPL-020).

Mint real RS256 tokens against an in-process RSA keypair and feed the
corresponding JWK through a mocked ``httpx`` transport. The verifier
sees a real PyJWT decode path; the only thing faked is the HTTP
transport for JWKS fetches.
"""

from __future__ import annotations

import time
from typing import Any

import httpx
import jwt
import pytest
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm

from custos_auth.oidc.config import OidcIssuerConfig
from custos_auth.oidc.jwks_cache import JwksCache
from custos_auth.oidc.verifier import (
    FAILURE_REASONS,
    REASON_BAD_SIGNATURE,
    REASON_EXPIRED,
    REASON_JWKS_FETCH_FAILED,
    REASON_MALFORMED,
    REASON_MISSING_CLAIM,
    REASON_UNKNOWN_KID,
    REASON_WRONG_ALGORITHM,
    REASON_WRONG_AUDIENCE,
    REASON_WRONG_ISSUER,
    OidcVerificationError,
    OidcVerifier,
    VerifiedOidcIdentity,
)

_ISSUER_URL = "https://issuer.example.com"
_AUDIENCE = "api://custos"
_JWKS_URI = "https://issuer.example.com/.well-known/jwks.json"


def _make_issuer_entry(**overrides: Any) -> OidcIssuerConfig:
    defaults: dict[str, Any] = dict(
        id="primary",
        preset=None,
        issuer_url=_ISSUER_URL,
        jwks_uri=_JWKS_URI,
        audiences=(_AUDIENCE,),
        algorithms=("RS256",),
        subject_claim="sub",
        provisioning_policy="zero-binding",
        group_claim=None,
        group_bindings=(),
        token_endpoint=None,
        client_id=None,
        client_secret_env=None,
    )
    defaults.update(overrides)
    return OidcIssuerConfig(**defaults)


def _generate_rsa_keypair() -> tuple[Any, Any]:
    private_key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )
    return private_key, private_key.public_key()


def _jwk_from_public_key(public_key: Any, kid: str) -> dict[str, Any]:
    pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    jwk_str = RSAAlgorithm.to_jwk(RSAAlgorithm(RSAAlgorithm.SHA256).prepare_key(pem))
    import json

    jwk: dict[str, Any] = json.loads(jwk_str)
    jwk["kid"] = kid
    jwk["alg"] = "RS256"
    jwk["use"] = "sig"
    return jwk


def _mint_token(
    private_key: Any,
    *,
    kid: str = "kid-1",
    alg: str = "RS256",
    iss: str = _ISSUER_URL,
    aud: str | list[str] = _AUDIENCE,
    sub: str | None = "user-42",
    extra: dict[str, Any] | None = None,
    exp_delta: int = 300,
    nbf_delta: int = -60,
) -> str:
    now = int(time.time())
    claims: dict[str, Any] = {
        "iss": iss,
        "aud": aud,
        "iat": now,
        "nbf": now + nbf_delta,
        "exp": now + exp_delta,
    }
    if sub is not None:
        claims["sub"] = sub
    if extra:
        claims.update(extra)
    return jwt.encode(claims, private_key, algorithm=alg, headers={"kid": kid})


def _make_client(jwk: dict[str, Any], *, kid_in_response: str | None = None) -> httpx.AsyncClient:
    response_jwk = dict(jwk)
    if kid_in_response is not None:
        response_jwk["kid"] = kid_in_response

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"keys": [response_jwk]})

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


async def test_verify_returns_identity_on_valid_token() -> None:
    priv, pub = _generate_rsa_keypair()
    jwk = _jwk_from_public_key(pub, kid="kid-1")
    token = _mint_token(priv, sub="user-42", extra={"name": "Verified User"})

    entry = _make_issuer_entry()
    async with _make_client(jwk) as client:
        cache = JwksCache(client)
        verifier = OidcVerifier((entry,), cache)
        identity = await verifier.verify("primary", token)

    assert isinstance(identity, VerifiedOidcIdentity)
    assert identity.subject == "user-42"
    assert identity.claims["name"] == "Verified User"
    assert identity.issuer_config is entry


async def test_verify_uses_configured_subject_claim() -> None:
    # Entra-style: subject_claim="oid".
    priv, pub = _generate_rsa_keypair()
    jwk = _jwk_from_public_key(pub, kid="kid-1")
    token = _mint_token(priv, sub="pairwise", extra={"oid": "stable-guid"})

    entry = _make_issuer_entry(subject_claim="oid")
    async with _make_client(jwk) as client:
        verifier = OidcVerifier((entry,), JwksCache(client))
        identity = await verifier.verify("primary", token)
    assert identity.subject == "stable-guid"


# ---------------------------------------------------------------------------
# Failure modes (closed-set reason codes)
# ---------------------------------------------------------------------------


async def test_verify_unknown_issuer_id_raises_missing_claim() -> None:
    entry = _make_issuer_entry()
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(404)))
    async with client:
        verifier = OidcVerifier((entry,), JwksCache(client))
        with pytest.raises(OidcVerificationError) as exc_info:
            await verifier.verify("unknown", "x.y.z")
    assert exc_info.value.reason == REASON_MISSING_CLAIM


async def test_verify_malformed_token_raises_malformed() -> None:
    entry = _make_issuer_entry()
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(404)))
    async with client:
        verifier = OidcVerifier((entry,), JwksCache(client))
        with pytest.raises(OidcVerificationError) as exc_info:
            await verifier.verify("primary", "not-a-jwt")
    assert exc_info.value.reason == REASON_MALFORMED


async def test_verify_token_missing_kid_raises_malformed() -> None:
    priv, _ = _generate_rsa_keypair()
    # PyJWT does not let us omit kid through the public API easily —
    # craft a token with empty kid via custom header.
    token = jwt.encode(
        {"iss": _ISSUER_URL, "aud": _AUDIENCE, "sub": "u", "exp": int(time.time()) + 60},
        priv,
        algorithm="RS256",
        headers={"kid": ""},
    )
    entry = _make_issuer_entry()
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(404)))
    async with client:
        verifier = OidcVerifier((entry,), JwksCache(client))
        with pytest.raises(OidcVerificationError) as exc_info:
            await verifier.verify("primary", token)
    assert exc_info.value.reason == REASON_MALFORMED


async def test_verify_wrong_algorithm_raises() -> None:
    priv, pub = _generate_rsa_keypair()
    jwk = _jwk_from_public_key(pub, kid="kid-1")
    token = _mint_token(priv)

    # Verifier allows only ES256; the RS256-signed token is rejected
    # at the header-allow-list check before any signature decode.
    entry = _make_issuer_entry(algorithms=("ES256",))
    async with _make_client(jwk) as client:
        verifier = OidcVerifier((entry,), JwksCache(client))
        with pytest.raises(OidcVerificationError) as exc_info:
            await verifier.verify("primary", token)
    assert exc_info.value.reason == REASON_WRONG_ALGORITHM


async def test_verify_unknown_kid_raises_unknown_kid() -> None:
    priv, pub = _generate_rsa_keypair()
    token = _mint_token(priv, kid="kid-1")
    # JWKS publishes a different kid only.
    other_jwk = _jwk_from_public_key(pub, kid="kid-2")
    entry = _make_issuer_entry()
    async with _make_client(other_jwk) as client:
        verifier = OidcVerifier((entry,), JwksCache(client))
        with pytest.raises(OidcVerificationError) as exc_info:
            await verifier.verify("primary", token)
    assert exc_info.value.reason == REASON_UNKNOWN_KID


async def test_verify_jwks_fetch_failed_raises() -> None:
    priv, _ = _generate_rsa_keypair()
    token = _mint_token(priv)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    entry = _make_issuer_entry()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        verifier = OidcVerifier((entry,), JwksCache(client))
        with pytest.raises(OidcVerificationError) as exc_info:
            await verifier.verify("primary", token)
    assert exc_info.value.reason == REASON_JWKS_FETCH_FAILED


async def test_verify_expired_token_raises_expired() -> None:
    priv, pub = _generate_rsa_keypair()
    jwk = _jwk_from_public_key(pub, kid="kid-1")
    token = _mint_token(priv, exp_delta=-3600)  # expired 1h ago
    entry = _make_issuer_entry()
    async with _make_client(jwk) as client:
        verifier = OidcVerifier((entry,), JwksCache(client), leeway_seconds=0)
        with pytest.raises(OidcVerificationError) as exc_info:
            await verifier.verify("primary", token)
    assert exc_info.value.reason == REASON_EXPIRED


async def test_verify_wrong_audience_raises() -> None:
    priv, pub = _generate_rsa_keypair()
    jwk = _jwk_from_public_key(pub, kid="kid-1")
    token = _mint_token(priv, aud="some-other-audience")
    entry = _make_issuer_entry()
    async with _make_client(jwk) as client:
        verifier = OidcVerifier((entry,), JwksCache(client))
        with pytest.raises(OidcVerificationError) as exc_info:
            await verifier.verify("primary", token)
    assert exc_info.value.reason == REASON_WRONG_AUDIENCE


async def test_verify_wrong_issuer_raises() -> None:
    priv, pub = _generate_rsa_keypair()
    jwk = _jwk_from_public_key(pub, kid="kid-1")
    token = _mint_token(priv, iss="https://attacker.example.com")
    entry = _make_issuer_entry()
    async with _make_client(jwk) as client:
        verifier = OidcVerifier((entry,), JwksCache(client))
        with pytest.raises(OidcVerificationError) as exc_info:
            await verifier.verify("primary", token)
    assert exc_info.value.reason == REASON_WRONG_ISSUER


async def test_verify_bad_signature_raises() -> None:
    priv_legit, _ = _generate_rsa_keypair()
    _, pub_other = _generate_rsa_keypair()
    # Token signed by the legit key, but JWKS serves a different
    # public key under the same kid → signature mismatch.
    other_jwk = _jwk_from_public_key(pub_other, kid="kid-1")
    token = _mint_token(priv_legit, kid="kid-1")
    entry = _make_issuer_entry()
    async with _make_client(other_jwk) as client:
        verifier = OidcVerifier((entry,), JwksCache(client))
        with pytest.raises(OidcVerificationError) as exc_info:
            await verifier.verify("primary", token)
    assert exc_info.value.reason == REASON_BAD_SIGNATURE


async def test_verify_missing_subject_claim_raises_missing_claim() -> None:
    priv, pub = _generate_rsa_keypair()
    jwk = _jwk_from_public_key(pub, kid="kid-1")
    token = _mint_token(priv, sub=None, extra={})
    entry = _make_issuer_entry()
    async with _make_client(jwk) as client:
        verifier = OidcVerifier((entry,), JwksCache(client))
        with pytest.raises(OidcVerificationError) as exc_info:
            await verifier.verify("primary", token)
    # Missing required claim — PyJWT raises MissingRequiredClaimError
    # mapped through InvalidTokenError → REASON_MISSING_CLAIM.
    assert exc_info.value.reason == REASON_MISSING_CLAIM


async def test_failure_reasons_set_size_matches_design_contract() -> None:
    # Closed-set guarantee: 11 reason codes (10 verifier-side + 1
    # token-exchange). Adding one is an audit-schema change.
    assert len(FAILURE_REASONS) == 11


async def test_issuer_for_lookup() -> None:
    entry = _make_issuer_entry()
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(404)))
    async with client:
        verifier = OidcVerifier((entry,), JwksCache(client))
        assert verifier.issuer_for("primary") is entry
        assert verifier.issuer_for("missing") is None

"""Tests for the call-context EdDSA signer (AS-IMPL-017).

Covers:

* :class:`SigningKey` generation + PEM round-trip.
* :class:`CallContextSigner.sign` claim shape, JWT round-trip
  through PyJWT (the "standard JWT library" half of the
  acceptance criterion), and ``ttl_seconds`` / ``jti`` overrides.
* :class:`StaticSigningKeyResolver` swap-in behaviour (the
  "rotation does not require a caller restart" acceptance
  criterion).
* :class:`DaprSecretsSigningKeyResolver` URL construction, secret
  extraction, caching, and error envelopes.
* :func:`decode_claims_unverified` / :func:`decode_header`
  diagnostic helpers.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import jwt
import pytest
from cryptography.hazmat.primitives import serialization

from custos_auth.callctx_signer import (
    ALGORITHM,
    DEFAULT_AUDIENCE,
    DEFAULT_TTL_SECONDS,
    ISSUER,
    CallContextSigner,
    DaprSecretsResolutionError,
    DaprSecretsSigningKeyResolver,
    SignedContext,
    SigningKey,
    StaticSigningKeyResolver,
    decode_claims_unverified,
    decode_header,
)

# ---------------------------------------------------------------------------
# SigningKey
# ---------------------------------------------------------------------------


def test_signing_key_generate_produces_unique_kid_per_call() -> None:
    key_a = SigningKey.generate()
    key_b = SigningKey.generate()
    assert key_a.kid != key_b.kid
    assert len(key_a.kid) == 16


def test_signing_key_generate_uses_supplied_created_at() -> None:
    moment = datetime(2026, 5, 24, 12, 0, 0, tzinfo=UTC)
    key = SigningKey.generate(created_at=moment)
    assert key.created_at == moment


def test_signing_key_private_pem_round_trips_through_from_private_pem() -> None:
    original = SigningKey.generate()
    pem = original.private_pem()
    restored = SigningKey.from_private_pem(pem)
    # Same key material → same derived kid.
    assert restored.kid == original.kid
    # Public PEM survives the round-trip too.
    assert restored.public_pem() == original.public_pem()


def test_signing_key_from_private_pem_accepts_str_input() -> None:
    original = SigningKey.generate()
    pem_str = original.private_pem().decode("ascii")
    restored = SigningKey.from_private_pem(pem_str)
    assert restored.kid == original.kid


def test_signing_key_from_private_pem_rejects_non_ed25519() -> None:
    from cryptography.hazmat.primitives.asymmetric.rsa import generate_private_key

    rsa_pem = generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    with pytest.raises(ValueError, match="must be Ed25519"):
        SigningKey.from_private_pem(rsa_pem)


# ---------------------------------------------------------------------------
# CallContextSigner — claim shape + JWT round-trip
# ---------------------------------------------------------------------------


async def test_signer_mints_jwt_with_documented_claim_shape() -> None:
    key = SigningKey.generate()
    resolver = StaticSigningKeyResolver(key=key)
    signer = CallContextSigner(resolver, clock=lambda: 1_700_000_000.0)
    signed = await signer.sign(
        principal_id="user-123",
        workspace_id="ws-1",
        caller_component="api-gateway",
    )
    assert isinstance(signed, SignedContext)
    assert signed.kid == key.kid
    assert signed.iat == 1_700_000_000
    assert signed.exp == 1_700_000_000 + DEFAULT_TTL_SECONDS
    # JWT verifies through PyJWT (the "standard JWT library" half of the
    # AS-IMPL-017 acceptance criterion).
    claims = jwt.decode(
        signed.token,
        key.public_key,
        algorithms=[ALGORITHM],
        audience=DEFAULT_AUDIENCE,
        issuer=ISSUER,
        options={"verify_exp": False},
    )
    assert claims["actingPrincipalId"] == "user-123"
    assert claims["workspaceId"] == "ws-1"
    assert claims["callerComponent"] == "api-gateway"
    assert claims["aud"] == DEFAULT_AUDIENCE
    assert claims["iss"] == ISSUER
    assert claims["iat"] == signed.iat
    assert claims["exp"] == signed.exp
    assert claims["jti"] == signed.jti


async def test_signer_emits_kid_in_jwt_header() -> None:
    key = SigningKey.generate()
    signer = CallContextSigner(StaticSigningKeyResolver(key=key))
    signed = await signer.sign(
        principal_id="user-1",
        workspace_id=None,
        caller_component="workflow-service",
    )
    header = jwt.get_unverified_header(signed.token)
    assert header["kid"] == key.kid
    assert header["alg"] == ALGORITHM
    assert header["typ"] == "JWT"


async def test_signer_allows_workspace_id_none_for_platform_global_calls() -> None:
    key = SigningKey.generate()
    signer = CallContextSigner(StaticSigningKeyResolver(key=key))
    signed = await signer.sign(
        principal_id="platform-admin",
        workspace_id=None,
        caller_component="api-gateway",
    )
    claims = jwt.decode(
        signed.token,
        key.public_key,
        algorithms=[ALGORITHM],
        audience=DEFAULT_AUDIENCE,
    )
    assert claims["workspaceId"] is None


async def test_signer_uses_explicit_ttl_seconds_override() -> None:
    key = SigningKey.generate()
    signer = CallContextSigner(
        StaticSigningKeyResolver(key=key),
        clock=lambda: 1_000_000.0,
    )
    signed = await signer.sign(
        principal_id="user-1",
        workspace_id="ws-1",
        caller_component="api-gateway",
        ttl_seconds=10,
    )
    assert signed.exp - signed.iat == 10


async def test_signer_uses_explicit_jti_override() -> None:
    key = SigningKey.generate()
    signer = CallContextSigner(StaticSigningKeyResolver(key=key))
    signed = await signer.sign(
        principal_id="user-1",
        workspace_id="ws-1",
        caller_component="api-gateway",
        jti="fixed-jti-1234",
    )
    assert signed.jti == "fixed-jti-1234"


async def test_signer_supports_custom_audience_and_issuer() -> None:
    key = SigningKey.generate()
    signer = CallContextSigner(
        StaticSigningKeyResolver(key=key),
        audience="custos.test",
        issuer="custos-auth-test",
    )
    signed = await signer.sign(
        principal_id="user-1",
        workspace_id="ws-1",
        caller_component="api-gateway",
    )
    claims = jwt.decode(
        signed.token,
        key.public_key,
        algorithms=[ALGORITHM],
        audience="custos.test",
        issuer="custos-auth-test",
    )
    assert claims["aud"] == "custos.test"
    assert claims["iss"] == "custos-auth-test"


async def test_signer_rejects_empty_principal_id() -> None:
    signer = CallContextSigner(StaticSigningKeyResolver(key=SigningKey.generate()))
    with pytest.raises(ValueError, match="principal_id"):
        await signer.sign(
            principal_id="",
            workspace_id="ws-1",
            caller_component="api-gateway",
        )


async def test_signer_rejects_empty_caller_component() -> None:
    signer = CallContextSigner(StaticSigningKeyResolver(key=SigningKey.generate()))
    with pytest.raises(ValueError, match="caller_component"):
        await signer.sign(
            principal_id="user-1",
            workspace_id="ws-1",
            caller_component="",
        )


async def test_signer_rejects_non_positive_ttl_override() -> None:
    signer = CallContextSigner(StaticSigningKeyResolver(key=SigningKey.generate()))
    with pytest.raises(ValueError, match="ttl_seconds"):
        await signer.sign(
            principal_id="user-1",
            workspace_id="ws-1",
            caller_component="api-gateway",
            ttl_seconds=0,
        )


def test_signer_init_rejects_non_positive_default_ttl() -> None:
    with pytest.raises(ValueError, match="default_ttl_seconds"):
        CallContextSigner(
            StaticSigningKeyResolver(key=SigningKey.generate()),
            default_ttl_seconds=0,
        )


async def test_each_signed_context_has_unique_jti_by_default() -> None:
    signer = CallContextSigner(StaticSigningKeyResolver(key=SigningKey.generate()))
    a = await signer.sign(
        principal_id="user-1",
        workspace_id="ws-1",
        caller_component="api-gateway",
    )
    b = await signer.sign(
        principal_id="user-1",
        workspace_id="ws-1",
        caller_component="api-gateway",
    )
    assert a.jti != b.jti


# ---------------------------------------------------------------------------
# Rotation acceptance criterion — swap-in resolver
# ---------------------------------------------------------------------------


async def test_resolver_key_swap_takes_effect_without_signer_restart() -> None:
    key_a = SigningKey.generate()
    key_b = SigningKey.generate()
    resolver = StaticSigningKeyResolver(key=key_a)
    signer = CallContextSigner(resolver)

    signed_a = await signer.sign(
        principal_id="user-1",
        workspace_id="ws-1",
        caller_component="api-gateway",
    )
    assert signed_a.kid == key_a.kid

    resolver.set_key(key_b)
    signed_b = await signer.sign(
        principal_id="user-1",
        workspace_id="ws-1",
        caller_component="api-gateway",
    )
    assert signed_b.kid == key_b.kid
    # The earlier token still verifies against key_a; rotation only
    # affects new mints.
    jwt.decode(
        signed_a.token,
        key_a.public_key,
        algorithms=[ALGORITHM],
        audience=DEFAULT_AUDIENCE,
    )
    jwt.decode(
        signed_b.token,
        key_b.public_key,
        algorithms=[ALGORITHM],
        audience=DEFAULT_AUDIENCE,
    )


# ---------------------------------------------------------------------------
# DaprSecretsSigningKeyResolver
# ---------------------------------------------------------------------------


class _FakeFetcher:
    """Minimal recording stand-in for an ``httpx.AsyncClient.get(...).json()``."""

    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self._responses = list(responses)
        self.calls: list[str] = []

    async def __call__(self, url: str) -> dict[str, Any]:
        self.calls.append(url)
        if not self._responses:
            raise AssertionError(f"unexpected extra Dapr Secrets call to {url!r}")
        return self._responses.pop(0)


async def test_dapr_resolver_issues_documented_url_and_returns_signing_key() -> None:
    pem = SigningKey.generate().private_pem().decode("ascii")
    fetcher = _FakeFetcher([{"call-context-key": pem}])
    resolver = DaprSecretsSigningKeyResolver(
        secret_store="custos-secrets",
        secret_name="call-context-key",
        fetch_json=fetcher,
    )
    key = await resolver.active_signing_key()
    assert isinstance(key, SigningKey)
    assert fetcher.calls == ["http://localhost:3500/v1.0/secrets/custos-secrets/call-context-key"]


async def test_dapr_resolver_respects_custom_endpoint() -> None:
    pem = SigningKey.generate().private_pem().decode("ascii")
    fetcher = _FakeFetcher([{"key": pem}])
    resolver = DaprSecretsSigningKeyResolver(
        secret_store="vault",
        secret_name="cc",
        fetch_json=fetcher,
        dapr_endpoint="http://dapr-sidecar.svc:3500/",
    )
    await resolver.active_signing_key()
    assert fetcher.calls == ["http://dapr-sidecar.svc:3500/v1.0/secrets/vault/cc"]


async def test_dapr_resolver_caches_resolved_key_within_ttl() -> None:
    pem = SigningKey.generate().private_pem().decode("ascii")
    fetcher = _FakeFetcher([{"key": pem}])
    clock_value = [100.0]

    resolver = DaprSecretsSigningKeyResolver(
        secret_store="store",
        secret_name="cc",
        fetch_json=fetcher,
        cache_ttl_seconds=60.0,
    )
    resolver._clock = lambda: clock_value[0]

    first = await resolver.active_signing_key()
    clock_value[0] = 130.0  # 30s later — still within cache TTL
    second = await resolver.active_signing_key()
    assert first is second
    assert len(fetcher.calls) == 1


async def test_dapr_resolver_refreshes_after_ttl_expiry() -> None:
    pem_a = SigningKey.generate().private_pem().decode("ascii")
    pem_b = SigningKey.generate().private_pem().decode("ascii")
    fetcher = _FakeFetcher([{"key": pem_a}, {"key": pem_b}])
    clock_value = [100.0]

    resolver = DaprSecretsSigningKeyResolver(
        secret_store="store",
        secret_name="cc",
        fetch_json=fetcher,
        cache_ttl_seconds=60.0,
    )
    resolver._clock = lambda: clock_value[0]

    first = await resolver.active_signing_key()
    clock_value[0] = 200.0  # well past TTL
    second = await resolver.active_signing_key()
    assert first.kid != second.kid
    assert len(fetcher.calls) == 2


async def test_dapr_resolver_uses_named_secret_key_when_supplied() -> None:
    pem = SigningKey.generate().private_pem().decode("ascii")
    payload = {"other-junk": "ignore-me", "the-pem": pem}
    fetcher = _FakeFetcher([payload])
    resolver = DaprSecretsSigningKeyResolver(
        secret_store="store",
        secret_name="cc",
        fetch_json=fetcher,
        secret_key="the-pem",
    )
    key = await resolver.active_signing_key()
    assert isinstance(key, SigningKey)


async def test_dapr_resolver_raises_when_named_secret_key_is_missing() -> None:
    fetcher = _FakeFetcher([{"another-key": "not-the-one"}])
    resolver = DaprSecretsSigningKeyResolver(
        secret_store="store",
        secret_name="cc",
        fetch_json=fetcher,
        secret_key="the-pem",
    )
    with pytest.raises(DaprSecretsResolutionError, match="the-pem"):
        await resolver.active_signing_key()


async def test_dapr_resolver_raises_on_empty_payload() -> None:
    fetcher = _FakeFetcher([{}])
    resolver = DaprSecretsSigningKeyResolver(
        secret_store="store",
        secret_name="cc",
        fetch_json=fetcher,
    )
    with pytest.raises(DaprSecretsResolutionError, match="empty body"):
        await resolver.active_signing_key()


async def test_dapr_resolver_raises_on_non_object_payload() -> None:
    async def fetcher(url: str) -> dict[str, Any]:
        # Pretend the Dapr Secrets API returned something pathological.
        # We type-narrow in production via ``isinstance(payload, dict)``.
        return ["not", "a", "dict"]  # type: ignore[return-value]

    resolver = DaprSecretsSigningKeyResolver(
        secret_store="store",
        secret_name="cc",
        fetch_json=fetcher,
    )
    with pytest.raises(DaprSecretsResolutionError, match="non-object/empty body"):
        await resolver.active_signing_key()


async def test_dapr_resolver_raises_on_unparseable_pem() -> None:
    fetcher = _FakeFetcher([{"key": "not a real pem"}])
    resolver = DaprSecretsSigningKeyResolver(
        secret_store="store",
        secret_name="cc",
        fetch_json=fetcher,
    )
    with pytest.raises(DaprSecretsResolutionError, match="not a valid"):
        await resolver.active_signing_key()


async def test_dapr_resolver_wraps_transport_exceptions() -> None:
    async def boom(url: str) -> dict[str, Any]:
        raise RuntimeError("connection refused")

    resolver = DaprSecretsSigningKeyResolver(
        secret_store="store",
        secret_name="cc",
        fetch_json=boom,
    )
    with pytest.raises(DaprSecretsResolutionError, match="connection refused"):
        await resolver.active_signing_key()


async def test_dapr_resolver_raises_when_payload_has_no_string_values() -> None:
    fetcher = _FakeFetcher([{"x": 1, "y": [], "z": None}])
    resolver = DaprSecretsSigningKeyResolver(
        secret_store="store",
        secret_name="cc",
        fetch_json=fetcher,
    )
    with pytest.raises(DaprSecretsResolutionError, match="no non-empty string"):
        await resolver.active_signing_key()


# ---------------------------------------------------------------------------
# Diagnostic helpers
# ---------------------------------------------------------------------------


async def test_decode_claims_unverified_returns_payload_without_verification() -> None:
    key = SigningKey.generate()
    signer = CallContextSigner(StaticSigningKeyResolver(key=key))
    signed = await signer.sign(
        principal_id="user-1",
        workspace_id="ws-1",
        caller_component="api-gateway",
    )
    claims = decode_claims_unverified(signed.token)
    assert claims["actingPrincipalId"] == "user-1"
    assert claims["jti"] == signed.jti


async def test_decode_header_returns_kid_and_algorithm() -> None:
    key = SigningKey.generate()
    signer = CallContextSigner(StaticSigningKeyResolver(key=key))
    signed = await signer.sign(
        principal_id="user-1",
        workspace_id="ws-1",
        caller_component="api-gateway",
    )
    header = decode_header(signed.token)
    assert header["kid"] == key.kid
    assert header["alg"] == ALGORITHM

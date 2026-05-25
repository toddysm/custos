"""Unit tests for :class:`custos_callctx.CallContextVerifier`."""

from __future__ import annotations

import base64
import json
from typing import Any

import pytest

from custos_callctx import (
    CALLCTX_HEADER,
    CallContextVerifier,
    InvalidCallContextError,
    JwksCache,
)
from custos_callctx._errors import InvalidReason
from tests._helpers import SigningKeyFixture, jwks_from_keys


class _FakeFetcher:
    def __init__(self, body: dict[str, Any], *, max_age: int = 600) -> None:
        self._body = body
        self._max_age = max_age
        self.calls = 0

    def update_body(self, body: dict[str, Any]) -> None:
        self._body = body

    async def __call__(self, url: str) -> tuple[dict[str, str], dict[str, Any]]:
        self.calls += 1
        return (
            {"cache-control": f"public, max-age={self._max_age}"},
            self._body,
        )


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _craft_unsigned_token(*, kid: str, claims: dict[str, Any]) -> str:
    """Build an ``alg=none`` JWT by hand (PyJWT refuses to mint one)."""
    header = {"alg": "none", "kid": kid, "typ": "JWT"}
    h = _b64url(json.dumps(header, separators=(",", ":")).encode())
    p = _b64url(json.dumps(claims, separators=(",", ":")).encode())
    return f"{h}.{p}."


@pytest.fixture
def signing_key() -> SigningKeyFixture:
    return SigningKeyFixture.generate()


@pytest.fixture
def verifier(signing_key: SigningKeyFixture) -> CallContextVerifier:
    fetcher = _FakeFetcher(jwks_from_keys(signing_key.public_key))
    cache = JwksCache(
        jwks_url="https://auth/.well-known/jwks.json",
        fetcher=fetcher,
    )
    return CallContextVerifier(jwks_cache=cache)


async def test_verify_returns_decoded_context(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    token = signing_key.mint(
        acting_principal_id="user-42",
        workspace_id="ws-7",
        caller_component="catalog",
    )
    ctx = await verifier.verify(metadata={CALLCTX_HEADER: token})

    assert ctx.acting_principal_id == "user-42"
    assert ctx.workspace_id == "ws-7"
    assert ctx.caller_component == "catalog"
    assert ctx.kid == signing_key.kid
    assert ctx.audience == "custos.internal"
    assert ctx.issuer == "custos-auth"
    assert ctx.expires_at > ctx.issued_at


async def test_verify_is_case_insensitive_on_header_name(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    token = signing_key.mint()
    ctx = await verifier.verify(metadata={"X-Custos-CallCtx": token})
    assert ctx.acting_principal_id


async def test_verify_handles_optional_workspace_id(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    token = signing_key.mint(workspace_id=None)
    ctx = await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert ctx.workspace_id is None


async def test_verify_rejects_missing_header(verifier: CallContextVerifier) -> None:
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={})
    assert exc.value.reason is InvalidReason.MISSING_HEADER


async def test_verify_rejects_empty_header(verifier: CallContextVerifier) -> None:
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: ""})
    assert exc.value.reason is InvalidReason.MISSING_HEADER


async def test_verify_rejects_malformed_token(verifier: CallContextVerifier) -> None:
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: "not-a-jwt"})
    assert exc.value.reason is InvalidReason.MALFORMED_TOKEN


async def test_verify_rejects_unsigned_token(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    """Acceptance criterion AS-IMPL-019: reject unsigned JWTs unconditionally."""
    import time

    now = int(time.time())
    token = _craft_unsigned_token(
        kid=signing_key.kid,
        claims={
            "iss": "custos-auth",
            "aud": "custos.internal",
            "iat": now,
            "exp": now + 60,
            "jti": "j",
            "actingPrincipalId": "u",
            "workspaceId": "w",
            "callerComponent": "c",
        },
    )
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert exc.value.reason is InvalidReason.BAD_ALGORITHM


async def test_verify_rejects_hs256_token(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    """Algorithm-confusion defense: HS256 with the public key as MAC secret."""
    import jwt as _jwt

    now = 1_700_000_000
    token = _jwt.encode(
        {
            "iss": "custos-auth",
            "aud": "custos.internal",
            "iat": now,
            "exp": now + 60,
            "jti": "j",
            "actingPrincipalId": "u",
            "workspaceId": "w",
            "callerComponent": "c",
        },
        key="secret",
        algorithm="HS256",
        headers={"kid": signing_key.kid},
    )
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert exc.value.reason is InvalidReason.BAD_ALGORITHM


async def test_verify_rejects_missing_kid(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    token = signing_key.mint(override_headers={"kid": ""})
    # Empty-string kid is treated as missing.
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert exc.value.reason is InvalidReason.MISSING_KID


async def test_verify_rejects_unknown_kid(
    signing_key: SigningKeyFixture,
) -> None:
    other = SigningKeyFixture.generate()
    fetcher = _FakeFetcher(jwks_from_keys(signing_key.public_key))
    cache = JwksCache(jwks_url="https://auth/jwks", fetcher=fetcher)
    verifier = CallContextVerifier(jwks_cache=cache)
    token = other.mint()
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert exc.value.reason is InvalidReason.UNKNOWN_KID


async def test_verify_unknown_kid_resolves_after_jwks_rotation(
    signing_key: SigningKeyFixture,
) -> None:
    """A token minted with a rotated key must verify after the JWKS refresh."""
    rotated = SigningKeyFixture.generate()
    fetcher = _FakeFetcher(jwks_from_keys(signing_key.public_key))
    cache = JwksCache(jwks_url="https://auth/jwks", fetcher=fetcher)
    verifier = CallContextVerifier(jwks_cache=cache)

    # Pre-warm the cache so the next fetch is the rotation refresh.
    await cache.get_key(signing_key.kid)
    # Simulate auth-service rotation: JWKS now publishes both keys.
    fetcher.update_body(jwks_from_keys(signing_key.public_key, rotated.public_key))

    token = rotated.mint()
    ctx = await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert ctx.kid == rotated.kid


async def test_verify_rejects_wrong_signature(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    token = signing_key.mint()
    # Flip a byte in the signature half.
    head, _, sig = token.rpartition(".")
    tampered = head + "." + ("A" * len(sig))
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: tampered})
    assert exc.value.reason in {
        InvalidReason.BAD_SIGNATURE,
        InvalidReason.MALFORMED_TOKEN,
    }


async def test_verify_rejects_expired_token(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    token = signing_key.mint(now=1_700_000_000, ttl_seconds=60)
    # PyJWT computes "now" against time.time(); the minted exp is far
    # in the past so it MUST be considered expired.
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert exc.value.reason is InvalidReason.EXPIRED


async def test_verify_rejects_wrong_audience(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    token = signing_key.mint(audience="other.internal")
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert exc.value.reason is InvalidReason.WRONG_AUDIENCE


async def test_verify_rejects_wrong_issuer(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    token = signing_key.mint(issuer="evil-issuer")
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert exc.value.reason is InvalidReason.WRONG_ISSUER


async def test_verify_rejects_missing_required_claim(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    token = signing_key.mint(override_claims={"actingPrincipalId": None})
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert exc.value.reason is InvalidReason.MISSING_CLAIM


async def test_verify_rejects_workspace_id_of_wrong_type(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    token = signing_key.mint(override_claims={"workspaceId": 12345})
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert exc.value.reason is InvalidReason.MALFORMED_TOKEN


def test_verifier_requires_jwks_url_or_cache() -> None:
    with pytest.raises(ValueError, match="jwks_url or jwks_cache"):
        CallContextVerifier()


def test_verifier_builds_default_jwks_cache_from_url() -> None:
    """Passing ``jwks_url`` constructs a default :class:`JwksCache`."""
    verifier = CallContextVerifier(jwks_url="https://auth/jwks")
    assert isinstance(verifier.jwks, JwksCache)
    assert verifier.jwks.jwks_url == "https://auth/jwks"


def test_verifier_rejects_negative_leeway(signing_key: SigningKeyFixture) -> None:
    cache = JwksCache(jwks_url="https://auth/jwks", fetcher=_FakeFetcher({"keys": []}))
    with pytest.raises(ValueError, match="leeway_seconds"):
        CallContextVerifier(jwks_cache=cache, leeway_seconds=-1)


async def test_verify_rejects_token_not_yet_valid(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    """iat far in the future maps to NOT_YET_VALID."""
    import time

    token = signing_key.mint(now=time.time() + 10_000)
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert exc.value.reason is InvalidReason.NOT_YET_VALID


async def test_verify_rejects_missing_iat_via_pyjwt(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    """PyJWT enforces required claims (``iat``); we map that to MISSING_CLAIM."""
    token = signing_key.mint(override_claims={"iat": None})
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert exc.value.reason is InvalidReason.MISSING_CLAIM


async def test_verify_rejects_non_integer_iat(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    """A non-integer ``iat`` should be flagged as malformed before pyjwt parses it.

    Note: PyJWT itself raises on non-numeric iat with InvalidTokenError, which
    maps to MALFORMED_TOKEN.
    """
    token = signing_key.mint(override_claims={"iat": "not-a-number"})
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert exc.value.reason in {
        InvalidReason.MALFORMED_TOKEN,
        InvalidReason.MISSING_CLAIM,
    }


async def test_verify_rejects_empty_acting_principal(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    token = signing_key.mint(override_claims={"actingPrincipalId": ""})
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert exc.value.reason is InvalidReason.MISSING_CLAIM


async def test_verify_rejects_empty_caller_component(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    token = signing_key.mint(override_claims={"callerComponent": ""})
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert exc.value.reason is InvalidReason.MISSING_CLAIM


async def test_verify_rejects_missing_caller_component(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    token = signing_key.mint(override_claims={"callerComponent": None})
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert exc.value.reason is InvalidReason.MISSING_CLAIM


async def test_verify_rejects_missing_jti(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    token = signing_key.mint(override_claims={"jti": None})
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert exc.value.reason is InvalidReason.MISSING_CLAIM


async def test_verify_normalises_aud_list_claim(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    """A multi-audience JWT validates as long as our audience is in the list."""
    token = signing_key.mint(
        override_claims={"aud": ["custos.internal", "extra.audience"]},
    )
    ctx = await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert ctx.audience == "custos.internal"


# ---------------------------------------------------------------------------
# permissions claim (Option D fat call-context)
# ---------------------------------------------------------------------------


async def test_verify_extracts_permissions_claim(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    token = signing_key.mint(
        permissions=["catalog:workflows:read", "catalog:workflows:write"],
    )
    ctx = await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert ctx.permissions == frozenset(
        {"catalog:workflows:read", "catalog:workflows:write"},
    )
    assert ctx.has_permission("catalog:workflows:read") is True
    assert ctx.has_permission("catalog:workflows:delete") is False


async def test_verify_deduplicates_permissions_claim(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    """Duplicates in the JSON array collapse to a single set entry."""
    token = signing_key.mint(
        permissions=["catalog:read", "catalog:read", "catalog:write"],
    )
    ctx = await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert ctx.permissions == frozenset({"catalog:read", "catalog:write"})


async def test_verify_treats_missing_permissions_claim_as_empty(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    """Back-compat with AS-IMPL-017 tokens that predate the claim."""
    token = signing_key.mint()  # no permissions kwarg
    ctx = await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert ctx.permissions == frozenset()
    assert ctx.has_permission("catalog:read") is False


async def test_verify_treats_null_permissions_claim_as_empty(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    """Explicit ``null`` is a documented signer-side way to say 'no grant'."""
    token = signing_key.mint(extra_claims={"permissions": None})
    ctx = await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert ctx.permissions == frozenset()


async def test_verify_accepts_empty_permissions_list(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    token = signing_key.mint(permissions=[])
    ctx = await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert ctx.permissions == frozenset()


async def test_verify_rejects_non_list_permissions_claim(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    """A scalar permissions claim is a forgery signal — refuse the token."""
    token = signing_key.mint(extra_claims={"permissions": "catalog:read"})
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert exc.value.reason is InvalidReason.MALFORMED_TOKEN


async def test_verify_rejects_permissions_claim_with_non_string_entry(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    token = signing_key.mint(extra_claims={"permissions": ["catalog:read", 42]})
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert exc.value.reason is InvalidReason.MALFORMED_TOKEN


async def test_verify_rejects_permissions_claim_with_empty_string_entry(
    signing_key: SigningKeyFixture,
    verifier: CallContextVerifier,
) -> None:
    token = signing_key.mint(extra_claims={"permissions": ["catalog:read", ""]})
    with pytest.raises(InvalidCallContextError) as exc:
        await verifier.verify(metadata={CALLCTX_HEADER: token})
    assert exc.value.reason is InvalidReason.MALFORMED_TOKEN


def test_call_context_has_permission_is_exact_match() -> None:
    from custos_callctx import CallContext

    ctx = CallContext(
        acting_principal_id="p",
        workspace_id="w",
        caller_component="c",
        jti="j",
        issued_at=0,
        expires_at=0,
        issuer="i",
        audience="a",
        kid="k",
        permissions=frozenset({"catalog:workflows:read"}),
    )
    # Wildcard expansion is not the library's job.
    assert ctx.has_permission("catalog:workflows:read") is True
    assert ctx.has_permission("catalog:*") is False
    assert ctx.has_permission("catalog:workflows:*") is False


def test_call_context_defaults_permissions_to_empty_frozenset() -> None:
    from custos_callctx import CallContext

    ctx = CallContext(
        acting_principal_id="p",
        workspace_id=None,
        caller_component="c",
        jti="j",
        issued_at=0,
        expires_at=0,
        issuer="i",
        audience="a",
        kid="k",
    )
    assert ctx.permissions == frozenset()
    assert ctx.has_permission("anything") is False


def test_module_exports_canonical_constants() -> None:
    from custos_callctx import DEFAULT_AUDIENCE, DEFAULT_ISSUER

    assert DEFAULT_AUDIENCE == "custos.internal"
    assert DEFAULT_ISSUER == "custos-auth"
    assert CALLCTX_HEADER == "x-custos-callctx"


async def test_default_http_fetcher_parses_jwks_response(
    signing_key: SigningKeyFixture,
) -> None:
    """Smoke test the production fetcher with an ``httpx.MockTransport``."""
    import json as _json

    import httpx

    from custos_callctx._jwks import default_http_fetcher

    body = jwks_from_keys(signing_key.public_key)

    async def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Cache-Control": "public, max-age=120"},
            content=_json.dumps(body),
        )

    transport = httpx.MockTransport(_handler)
    # Patch httpx.AsyncClient to use the mock transport for this call only.
    original_init = httpx.AsyncClient.__init__

    def _patched_init(self: httpx.AsyncClient, *args: Any, **kwargs: Any) -> None:
        kwargs["transport"] = transport
        original_init(self, *args, **kwargs)

    httpx.AsyncClient.__init__ = _patched_init  # type: ignore[method-assign]
    try:
        headers, parsed = await default_http_fetcher("https://auth/jwks")
    finally:
        httpx.AsyncClient.__init__ = original_init  # type: ignore[method-assign]

    assert "cache-control" in headers
    assert parsed["keys"][0]["kid"] == signing_key.kid

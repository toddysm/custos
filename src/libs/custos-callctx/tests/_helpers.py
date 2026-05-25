"""Test helpers: in-process Ed25519 signer + RFC 8037 OKP JWK builder.

Mirrors the auth-service ``custos_auth.callctx_signer`` shape so tests
exercise the same wire format that production receivers will see.
"""

from __future__ import annotations

import base64
import hashlib
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _raw_public_bytes(public_key: Ed25519PublicKey) -> bytes:
    return public_key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )


def derive_kid(public_key: Ed25519PublicKey) -> str:
    """Matches the auth-service ``kid = sha256(raw_pubkey).hexdigest()[:16]``."""
    digest = hashlib.sha256(_raw_public_bytes(public_key)).hexdigest()
    return digest[:16]


def public_key_to_jwk(public_key: Ed25519PublicKey, *, kid: str) -> dict[str, str]:
    return {
        "kty": "OKP",
        "crv": "Ed25519",
        "alg": "EdDSA",
        "use": "sig",
        "kid": kid,
        "x": _b64url(_raw_public_bytes(public_key)),
    }


def jwks_from_keys(*public_keys: Ed25519PublicKey) -> dict[str, Any]:
    return {"keys": [public_key_to_jwk(pk, kid=derive_kid(pk)) for pk in public_keys]}


@dataclass(frozen=True)
class SigningKeyFixture:
    """Bundle of (private_key, public_key, kid) for hermetic tests."""

    private_key: Ed25519PrivateKey
    public_key: Ed25519PublicKey
    kid: str

    @classmethod
    def generate(cls) -> SigningKeyFixture:
        private_key = Ed25519PrivateKey.generate()
        public_key = private_key.public_key()
        return cls(private_key=private_key, public_key=public_key, kid=derive_kid(public_key))

    def private_pem(self) -> bytes:
        return self.private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    def mint(
        self,
        *,
        acting_principal_id: str = "user-123",
        workspace_id: str | None = "ws-1",
        caller_component: str = "test-runner",
        audience: str = "custos.internal",
        issuer: str = "custos-auth",
        ttl_seconds: int = 300,
        now: float | None = None,
        permissions: list[str] | None = None,
        extra_claims: Mapping[str, Any] | None = None,
        override_headers: Mapping[str, Any] | None = None,
        override_claims: Mapping[str, Any] | None = None,
    ) -> str:
        """Mint a JWT mirroring the production ``CallContextSigner`` shape."""
        issued = int(now if now is not None else time.time())
        claims: dict[str, Any] = {
            "iss": issuer,
            "aud": audience,
            "iat": issued,
            "exp": issued + ttl_seconds,
            "jti": uuid.uuid4().hex,
            "actingPrincipalId": acting_principal_id,
            "workspaceId": workspace_id,
            "callerComponent": caller_component,
        }
        if permissions is not None:
            claims["permissions"] = permissions
        if extra_claims:
            claims.update(extra_claims)
        if override_claims:
            for key, value in override_claims.items():
                if value is None and key in claims:
                    del claims[key]
                else:
                    claims[key] = value
        headers: dict[str, Any] = {
            "alg": "EdDSA",
            "kid": self.kid,
            "typ": "JWT",
        }
        if override_headers:
            headers.update(override_headers)
        return jwt.encode(
            claims,
            self.private_pem(),
            algorithm=str(headers["alg"]) if headers.get("alg") else "EdDSA",
            headers=headers,
        )

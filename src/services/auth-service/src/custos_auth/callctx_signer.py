"""Internal call-context JWT signer (AS-IMPL-017).

Mints the **signed call context** that every internal RPC carries between
Custos components. The receiving component verifies the JWT locally via
the published JWKS (AS-IMPL-018) using the
:mod:`custos_callctx` helper (AS-IMPL-019), so internal hops never make
an Auth Service round-trip for authentication.

Design reference:
``design/components/auth-service/design.md`` § Internal vs External Auth —
Trust Model. Phase G fixes the JWT claim shape that the design left as a
TODO:

============================  ==============================================
Claim                         Meaning
============================  ==============================================
``actingPrincipalId``         Principal UUID minted by the API Gateway's
                              authentication step (User or ServiceAccount).
``workspaceId``               Workspace scope, or ``None`` for platform-
                              global calls (e.g. ``POST /v1/tenants``).
``callerComponent``           Component label of the signer
                              (``"api-gateway"``, ``"workflow-service"``,
                              ``"auth-service"``, …). Receivers may log it.
``iat`` / ``exp``             Issued-at / expiry as integer Unix seconds.
                              Default ``exp = iat + 300`` (5 min).
``jti``                       UUID4 per token; lets receivers debounce
                              replays within the 5-min window.
``aud``                       ``"custos.internal"`` by default.
``iss``                       ``"custos-auth"`` so verifiers can refuse
                              tokens minted by other authorities (e.g.
                              an OIDC IdP) by mistake.
============================  ==============================================

Algorithm is fixed at ``EdDSA`` over Ed25519. Header carries the
``kid`` so the verifier can pick the right public key from the JWKS
during the rotation overlap window (AS-IMPL-018 ships rotation +
JWKS endpoint).

The signing-key material is owned by a pluggable
:class:`SigningKeyResolver`:

* :class:`StaticSigningKeyResolver` — returns a fixed in-memory
  :class:`SigningKey`. The fallback for tests and (when a PEM is
  provisioned via env) for non-production deployments without a Dapr
  Secrets sidecar.
* :class:`DaprSecretsSigningKeyResolver` — resolves a PEM-encoded
  private key from the Dapr Secrets HTTP API (per
  ``CUSTOS_AUTH_CALL_CONTEXT_KEY_REF``). Caches the materialised
  :class:`SigningKey` for a short TTL so the rotation scheduler
  (AS-IMPL-018) can swap the secret server-side without restarting
  every caller.

The signer consults the resolver on every :meth:`CallContextSigner.sign`
call (the resolver is responsible for whatever caching it wants to
do), which is the acceptance criterion that rotation does not require
a caller restart.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Final, Protocol

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

#: JWT ``alg`` header value used for every call-context token.
ALGORITHM: Final[str] = "EdDSA"

#: Default JWT ``aud`` claim. Verifiers MUST refuse tokens whose
#: ``aud`` does not match the audience they were configured with;
#: ``custos.internal`` distinguishes call-context JWTs from any
#: end-user OIDC token an attacker might try to replay.
DEFAULT_AUDIENCE: Final[str] = "custos.internal"

#: Default JWT ``iss`` claim. Stamps every internal token with the
#: signing authority so verifiers can refuse tokens minted by other
#: authorities (e.g. an OIDC IdP) even when audience matches.
ISSUER: Final[str] = "custos-auth"

#: Default lifetime applied when the caller does not pass an explicit
#: ``ttl_seconds``. Short enough to absorb realistic clock skew without
#: opening a wide replay window, long enough to cover even slow
#: downstream RPCs.
DEFAULT_TTL_SECONDS: Final[int] = 300


def _kid_for_public_key(public_key: Ed25519PublicKey) -> str:
    """Derive a stable, short ``kid`` from the raw Ed25519 public key.

    The kid is the first 16 hex chars of the SHA-256 of the RFC 8032
    raw public key bytes. 64 bits of identity is plenty for rotation
    overlap (we ever hold 2 active keys at a time) and is short enough
    to fit comfortably in a JWT header without bloating every token.
    """
    raw = public_key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    return hashlib.sha256(raw).hexdigest()[:16]


@dataclass(frozen=True, slots=True)
class SigningKey:
    """Materialised Ed25519 keypair ready to sign call-context JWTs.

    Carries both halves of the keypair because:

    * the signer needs the private half to mint tokens,
    * the JWKS endpoint (AS-IMPL-018) needs the public half to
      publish, and
    * the rotation scheduler tracks ``created_at`` to age out old
      keys after ``2x CUSTOS_AUTH_CALL_CONTEXT_KEY_ROTATION``.

    The dataclass is frozen so it can be shared between coroutines
    without copy-on-write churn.

    Args:
        kid: Opaque key id stamped into the JWT header.
        private_key: Ed25519 private key used for signing.
        public_key: Matching public key used for verification + JWKS.
        created_at: UTC instant the keypair was generated. The
            rotation scheduler in AS-IMPL-018 reads this to decide
            when to retire the key.
    """

    kid: str
    private_key: Ed25519PrivateKey
    public_key: Ed25519PublicKey
    created_at: datetime

    @classmethod
    def generate(cls, *, created_at: datetime | None = None) -> SigningKey:
        """Generate a fresh Ed25519 keypair.

        Args:
            created_at: Override the timestamp stamped into the
                returned key (used in tests). Defaults to ``now()``
                in UTC.
        """
        private = Ed25519PrivateKey.generate()
        public = private.public_key()
        return cls(
            kid=_kid_for_public_key(public),
            private_key=private,
            public_key=public,
            created_at=created_at if created_at is not None else datetime.now(UTC),
        )

    @classmethod
    def from_private_pem(
        cls,
        pem: bytes | str,
        *,
        created_at: datetime | None = None,
    ) -> SigningKey:
        """Reconstruct a :class:`SigningKey` from a PEM-encoded private key.

        Accepts both bytes and ``str`` because the Dapr Secrets API
        returns string values. The PEM must encode an Ed25519 private
        key in PKCS#8 (the format emitted by
        :meth:`Ed25519PrivateKey.private_bytes` with
        ``PrivateFormat.PKCS8`` + ``NoEncryption``); other shapes
        raise :class:`ValueError`.
        """
        pem_bytes = pem.encode("utf-8") if isinstance(pem, str) else pem
        loaded = serialization.load_pem_private_key(pem_bytes, password=None)
        if not isinstance(loaded, Ed25519PrivateKey):
            raise ValueError(
                f"call-context signing key must be Ed25519; got {type(loaded).__name__}"
            )
        public = loaded.public_key()
        return cls(
            kid=_kid_for_public_key(public),
            private_key=loaded,
            public_key=public,
            created_at=created_at if created_at is not None else datetime.now(UTC),
        )

    def private_pem(self) -> bytes:
        """Serialise the private key to PKCS#8 PEM bytes (unencrypted).

        Used by the rotation scheduler (AS-IMPL-018) when staging a
        freshly generated key back to the Dapr Secrets API.
        """
        return self.private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    def public_pem(self) -> bytes:
        """Serialise the public key to SPKI PEM bytes (used for testing/diagnostics)."""
        return self.public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )


@dataclass(frozen=True, slots=True)
class SignedContext:
    """Minted call-context JWT plus its useful metadata.

    Returned by :meth:`CallContextSigner.sign`. Callers typically only
    propagate :attr:`token` through Dapr service-invocation metadata;
    the metadata fields are exposed for tests, audit, and so the
    rotation logic can correlate freshly minted tokens with their
    issuing key.

    Args:
        token: Serialised compact JWT (``header.payload.signature``).
        kid: Key id that signed the token (mirrors the JWT header).
        jti: Unique token id (mirrors the JWT ``jti`` claim).
        iat: Issued-at instant as Unix seconds.
        exp: Expiry instant as Unix seconds.
    """

    token: str
    kid: str
    jti: str
    iat: int
    exp: int


class SigningKeyResolver(Protocol):
    """Async source of the *currently active* call-context signing key.

    Implementations decide their own caching policy. The signer calls
    :meth:`active_signing_key` on every :meth:`CallContextSigner.sign`
    invocation so an updated key takes effect on the next minted token
    without restarting the signing process (AS-IMPL-017 acceptance
    criterion).
    """

    async def active_signing_key(self) -> SigningKey: ...


@dataclass(slots=True)
class StaticSigningKeyResolver:
    """In-memory resolver returning a fixed :class:`SigningKey`.

    Used by tests, by the dev-mode non-production startup path (when an
    inline PEM is provided in lieu of a Dapr Secrets sidecar), and by
    AS-IMPL-018's in-process rotation logic during the warm-up phase.

    Args:
        key: The signing key to return for every resolve.
    """

    key: SigningKey

    async def active_signing_key(self) -> SigningKey:
        return self.key

    def set_key(self, key: SigningKey) -> None:
        """Replace the active key.

        AS-IMPL-018's rotation scheduler calls this when a brand-new
        keypair is staged. Because :meth:`active_signing_key` is consulted
        on every :meth:`CallContextSigner.sign` call, the new key is
        picked up by all in-flight signers without restart.
        """
        self.key = key


#: Async HTTP fetcher signature accepted by
#: :class:`DaprSecretsSigningKeyResolver`. Receives the fully-qualified
#: URL and returns the JSON body as a dict.
#:
#: Production wiring injects an ``httpx.AsyncClient.get(...).json()``
#: equivalent; tests can pass a pure-Python coroutine to avoid
#: standing up a real HTTP transport.
HttpJsonFetcher = Callable[[str], Awaitable[dict[str, Any]]]


class DaprSecretsResolutionError(RuntimeError):
    """Raised when the Dapr Secrets API does not return a usable PEM.

    Surfaced as a 500 by the signer so an unconfigured Dapr secret
    crash-loops the pod with an operator-actionable message rather
    than minting invalid JWTs.
    """


@dataclass(slots=True)
class DaprSecretsSigningKeyResolver:
    """Resolves the call-context signing key from the Dapr Secrets API.

    Args:
        secret_store: Dapr secret-store component name (e.g.
            ``"custos-secrets"``).
        secret_name: Name of the secret holding the PEM-encoded
            Ed25519 private key. Comes from
            ``CUSTOS_AUTH_CALL_CONTEXT_KEY_REF``.
        fetch_json: Async callable that GETs a URL and returns the
            decoded JSON body. Injected so the resolver remains
            transport-agnostic and trivially mockable.
        dapr_endpoint: Base URL of the Dapr sidecar (default
            ``"http://localhost:3500"`` per Dapr defaults). The
            resolver issues
            ``{dapr_endpoint}/v1.0/secrets/{store}/{name}``.
        secret_key: Optional explicit field name to read inside the
            Dapr secret payload. When omitted, the resolver picks the
            first non-empty string value, which matches both the
            common ``{name: value}`` Dapr shape and single-entry
            Kubernetes-secret-backed stores.
        cache_ttl_seconds: How long the resolved key is reused before
            re-fetching from Dapr. Short enough that AS-IMPL-018's
            rotation propagates within roughly one TTL; long enough
            that bulk-signing workloads do not hammer the sidecar.

    The resolver is goroutine-safe in the asyncio sense: an in-flight
    refresh blocks concurrent callers from also issuing fetches, so a
    burst of :meth:`CallContextSigner.sign` calls after a TTL expiry
    triggers exactly one Dapr round-trip.
    """

    secret_store: str
    secret_name: str
    fetch_json: HttpJsonFetcher
    dapr_endpoint: str = "http://localhost:3500"
    secret_key: str | None = None
    cache_ttl_seconds: float = 60.0
    _cached_key: SigningKey | None = field(default=None, init=False)
    _cached_at: float = field(default=0.0, init=False)
    _clock: Callable[[], float] = field(default=time.monotonic, init=False)
    #: Serialises refreshes so a burst of cache-miss callers triggers
    #: exactly one Dapr round-trip. Created lazily on first refresh so
    #: the dataclass can still be constructed off the event loop
    #: (``asyncio.Lock()`` instantiated at import time used to bind to
    #: a non-existent loop on older Pythons; ``None``-then-lazy keeps
    #: us compatible with every supported runtime).
    _refresh_lock: asyncio.Lock | None = field(default=None, init=False, repr=False)

    async def active_signing_key(self) -> SigningKey:
        if self._is_cache_fresh():
            assert self._cached_key is not None  # narrowed by _is_cache_fresh
            return self._cached_key
        lock = self._refresh_lock
        if lock is None:
            lock = asyncio.Lock()
            self._refresh_lock = lock
        async with lock:
            # Double-checked locking: a sibling coroutine may have
            # already refreshed the cache while we were waiting on
            # the lock. Re-read ``self._cached_key`` / ``_cached_at``
            # under the lock before issuing another Dapr call.
            if self._is_cache_fresh():
                assert self._cached_key is not None
                return self._cached_key
            key = await self._fetch()
            self._cached_key = key
            self._cached_at = self._clock()
            return key

    def _is_cache_fresh(self) -> bool:
        if self._cached_key is None:
            return False
        return (self._clock() - self._cached_at) < self.cache_ttl_seconds

    async def _fetch(self) -> SigningKey:
        url = (
            f"{self.dapr_endpoint.rstrip('/')}/v1.0/secrets/{self.secret_store}/{self.secret_name}"
        )
        try:
            payload = await self.fetch_json(url)
        except asyncio.CancelledError:
            # Preserve task-cancellation semantics. asyncio.CancelledError
            # derives from BaseException on Python 3.8+, so the broader
            # ``except Exception`` below already misses it — but an
            # injected ``fetch_json`` could (incorrectly) raise a custom
            # Exception-derived cancellation marker, and being explicit
            # makes the intent obvious to readers and to future
            # maintainers swapping the fetcher.
            raise
        except Exception as exc:  # network/transport failures
            raise DaprSecretsResolutionError(
                f"Dapr Secrets API call to {url!r} failed: {exc}"
            ) from exc
        if not isinstance(payload, dict) or not payload:
            raise DaprSecretsResolutionError(
                f"Dapr Secrets API returned a non-object/empty body for "
                f"{self.secret_store}/{self.secret_name!r}"
            )
        pem_value: str | None = None
        if self.secret_key is not None:
            raw = payload.get(self.secret_key)
            if not isinstance(raw, str) or not raw:
                raise DaprSecretsResolutionError(
                    f"Dapr Secrets payload for "
                    f"{self.secret_store}/{self.secret_name!r} does not "
                    f"contain a non-empty string at key {self.secret_key!r}"
                )
            pem_value = raw
        else:
            for value in payload.values():
                if isinstance(value, str) and value.strip():
                    pem_value = value
                    break
            if pem_value is None:
                raise DaprSecretsResolutionError(
                    f"Dapr Secrets payload for "
                    f"{self.secret_store}/{self.secret_name!r} carries "
                    f"no non-empty string value"
                )
        try:
            return SigningKey.from_private_pem(pem_value)
        except (ValueError, TypeError) as exc:
            raise DaprSecretsResolutionError(
                f"Dapr Secrets payload for "
                f"{self.secret_store}/{self.secret_name!r} is not a valid "
                f"PEM-encoded Ed25519 private key: {exc}"
            ) from exc


class CallContextSigner:
    """Mints EdDSA-signed call-context JWTs.

    Wires together a :class:`SigningKeyResolver` and the fixed JWT
    claim shape. The signer is cheap to construct and safe to share
    between request handlers; each :meth:`sign` call is independent.

    Args:
        resolver: Source of the active signing key. Consulted on
            every :meth:`sign` so a rotated key takes effect on the
            next mint.
        audience: ``aud`` claim stamped into every minted token.
            Defaults to :data:`DEFAULT_AUDIENCE`. Verifiers should
            be configured with the same value.
        issuer: ``iss`` claim stamped into every minted token.
            Defaults to :data:`ISSUER`. Operators rarely need to
            override this; it exists so test fixtures can mint
            tokens with a non-matching ``iss`` to confirm verifiers
            reject them.
        default_ttl_seconds: ``exp - iat`` applied when the caller
            does not pass ``ttl_seconds`` to :meth:`sign`.
            :data:`DEFAULT_TTL_SECONDS` (5 min) matches the design.
        clock: Override for the wall-clock used to stamp ``iat`` /
            ``exp``. Tests pass a fixed callable so the produced JWT
            is byte-stable.
    """

    def __init__(
        self,
        resolver: SigningKeyResolver,
        *,
        audience: str = DEFAULT_AUDIENCE,
        issuer: str = ISSUER,
        default_ttl_seconds: int = DEFAULT_TTL_SECONDS,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if default_ttl_seconds <= 0:
            raise ValueError(
                f"default_ttl_seconds must be a positive integer; got {default_ttl_seconds!r}"
            )
        self._resolver = resolver
        self._audience = audience
        self._issuer = issuer
        self._default_ttl_seconds = default_ttl_seconds
        self._clock = clock

    async def sign(
        self,
        *,
        principal_id: str,
        workspace_id: str | None,
        caller_component: str,
        ttl_seconds: int | None = None,
        jti: str | None = None,
    ) -> SignedContext:
        """Mint a signed call-context JWT.

        Args:
            principal_id: Acting principal id stamped into
                ``actingPrincipalId``. Required and non-empty.
            workspace_id: Workspace scope for the call, or ``None``
                for platform-global RPCs.
            caller_component: Component label of the signer
                (``"api-gateway"``, ``"workflow-service"``, …).
                Required and non-empty.
            ttl_seconds: Override for the JWT lifetime. Must be
                strictly positive; ``None`` falls back to
                :attr:`default_ttl_seconds`.
            jti: Override for the unique-token-id claim (tests use
                this to make the produced JWT byte-stable). ``None``
                generates a fresh UUID4.

        Returns:
            A :class:`SignedContext` holding the compact JWT and its
            decoded metadata.

        Raises:
            ValueError: When required arguments are missing/empty
                or ``ttl_seconds`` is non-positive.
        """
        if not principal_id:
            raise ValueError("principal_id is required and must be non-empty")
        if not caller_component:
            raise ValueError("caller_component is required and must be non-empty")
        effective_ttl = ttl_seconds if ttl_seconds is not None else self._default_ttl_seconds
        if effective_ttl <= 0:
            raise ValueError(f"ttl_seconds must be a positive integer; got {effective_ttl!r}")
        iat = int(self._clock())
        exp = iat + effective_ttl
        token_id = jti if jti is not None else str(uuid.uuid4())
        payload: dict[str, Any] = {
            "iss": self._issuer,
            "aud": self._audience,
            "iat": iat,
            "exp": exp,
            "jti": token_id,
            "actingPrincipalId": principal_id,
            "workspaceId": workspace_id,
            "callerComponent": caller_component,
        }
        key = await self._resolver.active_signing_key()
        token = jwt.encode(
            payload,
            key.private_key,
            algorithm=ALGORITHM,
            headers={"kid": key.kid, "typ": "JWT"},
        )
        return SignedContext(token=token, kid=key.kid, jti=token_id, iat=iat, exp=exp)


def decode_claims_unverified(token: str) -> dict[str, Any]:
    """Best-effort decode of a JWT payload **without** signature verification.

    Diagnostic helper exposed for audit and admin tooling so an
    operator can inspect a minted or received call-context token
    without standing up the verifier helper from AS-IMPL-019. **Do
    not** use this in any request-handling code path — it explicitly
    skips signature, ``exp``, ``aud``, and ``iss`` validation.
    """
    claims = jwt.decode(token, options={"verify_signature": False})
    if not isinstance(claims, dict):  # pragma: no cover — pyjwt guarantee
        raise ValueError("decoded JWT payload is not a JSON object")
    return claims


def decode_header(token: str) -> dict[str, Any]:
    """Decode the JWT header so the verifier can pick the right ``kid``.

    The full verifier in AS-IMPL-019 needs this to look up the right
    key in the JWKS before validating the signature. Exposed at module
    level so other auth-service modules (and tests) can reuse the same
    parser instead of reaching into PyJWT.
    """
    header = jwt.get_unverified_header(token)
    if not isinstance(header, dict):  # pragma: no cover — pyjwt guarantee
        raise ValueError("decoded JWT header is not a JSON object")
    return header


__all__ = [
    "ALGORITHM",
    "DEFAULT_AUDIENCE",
    "DEFAULT_TTL_SECONDS",
    "ISSUER",
    "CallContextSigner",
    "DaprSecretsResolutionError",
    "DaprSecretsSigningKeyResolver",
    "HttpJsonFetcher",
    "SignedContext",
    "SigningKey",
    "SigningKeyResolver",
    "StaticSigningKeyResolver",
    "decode_claims_unverified",
    "decode_header",
]

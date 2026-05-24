"""In-memory call-context signing-key ring + rotation scheduler (AS-IMPL-018).

The signing key minted by :class:`custos_auth.callctx_signer.CallContextSigner`
must rotate on a fixed schedule so the blast radius of an exfiltrated key
is bounded to the rotation period. The receivers — every other Custos
component — verify against the JWKS endpoint shipped by
:data:`custos_auth.api.routes.jwks.router`; that endpoint publishes
the **current** key plus any **retired** keys still within the
:math:`2\\times` overlap window, so a JWT minted just before rotation
still verifies for up to its 5-minute ``exp`` even after the JWKS rolls
(AS-IMPL-018 acceptance criterion).

Design reference:
``design/components/auth-service/design.md`` § Internal vs External Auth —
Trust Model: *"the signing key is rotated weekly; old keys remain in the
JWKS for 2x rotation period to absorb in-flight requests."*

Scope of this module
--------------------

* :class:`JwksEntry` — public-half + metadata wire shape, ready to be
  serialised into the RFC 8037 OKP JWK payload by the JWKS route.
* :class:`KeyRing` — holds the active :class:`SigningKey` plus any
  retired entries still within the overlap window. Threadsafe in the
  asyncio sense (every mutation goes through :meth:`KeyRing.rotate`
  which is called only by the scheduler).
* :func:`run_rotation_loop` — async coroutine the lifespan runs as a
  background task. Generates a fresh keypair on schedule, calls
  :meth:`KeyRing.rotate`, and pushes the new active key into the
  supplied :class:`StaticSigningKeyResolver` so existing
  :class:`CallContextSigner` instances pick it up on the next sign
  call without a restart.
* :func:`install_key_age_metric` — registers the OTel observable
  gauge for the active key's age in seconds.

Multi-replica caveat
--------------------

For M1, auth-service is single-replica per the design's deployment
guidance, so each pod owns its own ring. Multi-replica HA (M3 +
SPIFFE/SPIRE in AS-IMPL-031) is the future home of cross-pod key
sharing; until then a pod restart issues a brand-new active key and
the previous pod's signatures fail at the next call. That is a
deliberate M1 trade-off captured here so the M3 cutover is unambiguous.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import random
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final, TypeAlias

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, Observation

from custos_auth.callctx_signer import SigningKey, StaticSigningKeyResolver

logger = logging.getLogger(__name__)

#: Wall-clock signature shared by :class:`KeyRing` and
#: :func:`run_rotation_loop`. Pulled out as a type alias so tests can
#: pass a deterministic callable without inheriting from the protocol.
TimeFunc: TypeAlias = Callable[[], float]

#: Sleep injection accepted by :func:`run_rotation_loop`. Defaults to
#: :func:`asyncio.sleep`; tests pass a recording fake.
SleepFunc: TypeAlias = Callable[[float], Awaitable[None]]


#: Default rotation interval (7 days, in seconds). Mirrors the design's
#: ``CUSTOS_AUTH_CALL_CONTEXT_KEY_ROTATION`` default.
DEFAULT_ROTATION_PERIOD_SECONDS: Final[int] = 7 * 24 * 60 * 60

#: Overlap multiplier: a retired key is kept in the JWKS for
#: ``OVERLAP_FACTOR * rotation_period`` after retirement. The design
#: fixes this at 2x so a JWT minted just before rotation survives for
#: up to its 5-min ``exp`` even if the rotation completes the instant
#: after the mint.
OVERLAP_FACTOR: Final[int] = 2

#: Fraction of the rotation interval consumed by HTTP caching headers on
#: the JWKS response. Half the rotation period keeps the cache fresh
#: enough that verifiers pick up a new key before retired ones age out.
JWKS_CACHE_FRACTION: Final[float] = 0.5

_INSTRUMENTATION_NAME: Final[str] = "custos_auth.callctx"
_INSTRUMENTATION_VERSION: Final[str] = "0.1.0"


@dataclass(frozen=True, slots=True)
class JwksEntry:
    """Public half of a :class:`SigningKey`, plus retirement metadata.

    The JWKS route serialises one of these per entry into the RFC 8037
    OKP JWK shape (``kty=OKP`` / ``crv=Ed25519`` / ``alg=EdDSA`` /
    ``use=sig`` / ``kid`` / ``x``).

    Args:
        kid: Key id stamped into the matching JWT header.
        public_key: Ed25519 public key used for verification.
        created_at: UTC instant the keypair was generated.
        retired_at: UTC instant the key left active rotation, or
            ``None`` while the key is the currently active signer.
    """

    kid: str
    public_key: Ed25519PublicKey
    created_at: datetime
    retired_at: datetime | None = None

    def public_raw_bytes(self) -> bytes:
        """Return the raw Ed25519 public-key bytes (32 bytes)."""
        return self.public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )

    def to_jwk(self) -> dict[str, str]:
        """Serialise into the RFC 8037 OKP JWK shape.

        The base64url encoding is RFC 7515 §2 — no padding, URL-safe
        alphabet. Receivers using PyJWT / jose / node-jose decode the
        ``x`` field with the same scheme to reconstruct the public key.
        """
        x = base64.urlsafe_b64encode(self.public_raw_bytes()).rstrip(b"=").decode("ascii")
        return {
            "kty": "OKP",
            "crv": "Ed25519",
            "alg": "EdDSA",
            "use": "sig",
            "kid": self.kid,
            "x": x,
        }


def _entry_from_key(
    key: SigningKey,
    *,
    retired_at: datetime | None = None,
) -> JwksEntry:
    return JwksEntry(
        kid=key.kid,
        public_key=key.public_key,
        created_at=key.created_at,
        retired_at=retired_at,
    )


class KeyRing:
    """Holds the active call-context signing key plus retired entries.

    The ring's invariant is:

    * exactly one :attr:`active` key at any time,
    * zero or more retired entries, each retired within the past
      :math:`OVERLAP\\_FACTOR \\times rotation\\_period\\_seconds`.

    The constructor seeds the ring from an initial :class:`SigningKey`;
    callers (the lifespan in :mod:`custos_auth`) obtain that key from
    a :class:`SigningKeyResolver` — typically Dapr in production or
    ephemeral generation in dev.

    Args:
        active: Initial active signing key.
        rotation_period_seconds: How long an active key stays active
            before the next :meth:`rotate`. Determines the retirement
            window via :data:`OVERLAP_FACTOR`.
        clock: Wall-clock callable returning Unix seconds. Override
            in tests for byte-stable behaviour.
    """

    def __init__(
        self,
        active: SigningKey,
        *,
        rotation_period_seconds: int = DEFAULT_ROTATION_PERIOD_SECONDS,
        clock: TimeFunc = time.time,
    ) -> None:
        if rotation_period_seconds <= 0:
            raise ValueError(
                "rotation_period_seconds must be a positive integer; "
                f"got {rotation_period_seconds!r}"
            )
        self._active: SigningKey = active
        self._retired: list[JwksEntry] = []
        self._rotation_period_seconds = rotation_period_seconds
        self._clock = clock

    @property
    def rotation_period_seconds(self) -> int:
        return self._rotation_period_seconds

    @property
    def overlap_window_seconds(self) -> int:
        """Total time a retired key remains advertised in JWKS."""
        return OVERLAP_FACTOR * self._rotation_period_seconds

    @property
    def active(self) -> SigningKey:
        return self._active

    @property
    def active_entry(self) -> JwksEntry:
        return _entry_from_key(self._active)

    def retired_entries(self) -> list[JwksEntry]:
        """Snapshot of the retired entries currently inside the overlap window."""
        return list(self._retired)

    def all_public_entries(self) -> list[JwksEntry]:
        """Active first, then retired (newest first) — the JWKS body order.

        Listing the active key first lets verifiers that scan linearly
        for ``kid`` succeed faster on the common case (current key).
        """
        return [self.active_entry, *self._retired]

    def current_key_age_seconds(self) -> float:
        """Age of the active key in seconds, used by the OTel gauge."""
        now = self._clock()
        created = self._active.created_at.timestamp()
        return max(0.0, now - created)

    def rotate(self, new_key: SigningKey, *, now: datetime | None = None) -> None:
        """Promote ``new_key`` to active and retire the previous active.

        The retirement marker is the supplied ``now`` (or
        :data:`datetime.utcnow` in UTC). After the swap, any retired
        entries whose ``retired_at`` is older than
        :attr:`overlap_window_seconds` are dropped from the ring; that
        is what keeps JWKS growth bounded across an arbitrary number
        of rotations.

        Args:
            new_key: Freshly generated keypair that becomes the new
                active signer.
            now: Override the retirement timestamp (used in tests).
        """
        if new_key.kid == self._active.kid:
            raise ValueError(
                "rotate() called with the currently active key; "
                "generate a fresh keypair before rotating."
            )
        retired_at = now if now is not None else datetime.now(UTC)
        retiring = _entry_from_key(self._active, retired_at=retired_at)
        self._retired.insert(0, retiring)
        self._active = new_key
        self._prune_expired_retired(now=retired_at)
        logger.info(
            "call-context signing key rotated: new kid=%s retiring kid=%s retired keys in ring=%d",
            new_key.kid,
            retiring.kid,
            len(self._retired),
        )

    def _prune_expired_retired(self, *, now: datetime) -> None:
        cutoff = now.timestamp() - self.overlap_window_seconds
        kept: list[JwksEntry] = []
        for entry in self._retired:
            if entry.retired_at is None:
                kept.append(entry)
                continue
            if entry.retired_at.timestamp() >= cutoff:
                kept.append(entry)
        if len(kept) != len(self._retired):
            dropped = [e.kid for e in self._retired if e not in kept]
            logger.info(
                "dropped %d retired call-context signing key(s) past the overlap window: %s",
                len(self._retired) - len(kept),
                dropped,
            )
        self._retired = kept


def install_key_age_metric(ring: KeyRing) -> None:
    """Register the OTel observable gauge for the active key's age.

    Production deployments pair this with the
    OpenTelemetry SDK shipped via the Helm umbrella chart; the gauge
    surfaces as ``custos_auth_callctx_signing_key_age_seconds`` and is
    the AS-IMPL-018 acceptance-criterion knob for alerting on stuck
    rotation.

    Safe to call multiple times — the OTel meter de-dupes by name.
    """
    meter = metrics.get_meter(_INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION)

    def _observe(_options: CallbackOptions) -> Iterable[Observation]:
        return [Observation(ring.current_key_age_seconds())]

    meter.create_observable_gauge(
        name="custos_auth_callctx_signing_key_age_seconds",
        callbacks=[_observe],
        unit="s",
        description=(
            "Age of the call-context signing key currently advertised as "
            "active in the JWKS. Alerts on stuck-rotation should page when "
            "this exceeds 1.5x CUSTOS_AUTH_CALL_CONTEXT_KEY_ROTATION."
        ),
    )


async def run_rotation_loop(
    *,
    key_ring: KeyRing,
    resolver: StaticSigningKeyResolver,
    rotation_period_seconds: int,
    jitter_ratio: float = 0.05,
    sleep: SleepFunc | None = None,
    clock: TimeFunc = time.time,
) -> None:
    """Background task that rotates the call-context signing key.

    Loops forever until cancelled. Each iteration:

    1. Sleeps ``rotation_period_seconds`` (±``jitter_ratio`` per side)
       so a fleet of replicas does not all rotate in lockstep — although
       M1 is single-replica, the jitter is cheap insurance for M3.
    2. Generates a fresh :class:`SigningKey`.
    3. Calls :meth:`KeyRing.rotate` (which prunes retired keys past
       the overlap window).
    4. Calls :meth:`StaticSigningKeyResolver.set_key` so every
       :class:`CallContextSigner` consulting the resolver picks up
       the new key on its next mint — the AS-IMPL-017 acceptance
       criterion (rotation requires no signer restart) carried
       forward into AS-IMPL-018.

    Setting ``rotation_period_seconds`` to ``0`` disables rotation
    entirely (the loop exits immediately) — used by lifespan startup
    when the operator wants to manage rotation externally and by
    tests that drive :meth:`KeyRing.rotate` directly.

    Args:
        key_ring: The shared ring whose entries are mutated.
        resolver: The signer's resolver, which gets the new active
            key pushed on each rotation.
        rotation_period_seconds: Base sleep between rotations. ``0``
            disables the loop.
        jitter_ratio: Per-side jitter applied to the sleep interval.
            Default 5 %.
        sleep: Sleep injection for tests. Defaults to
            :func:`asyncio.sleep`. The callable receives the duration
            in seconds.
        clock: Override for the wall-clock used to stamp the new
            key's ``created_at`` (tests use this to make rotation
            byte-stable).
    """
    if rotation_period_seconds < 0:
        raise ValueError(
            f"rotation_period_seconds must be non-negative; got {rotation_period_seconds!r}"
        )
    if rotation_period_seconds == 0:
        logger.info(
            "call-context key rotation loop disabled "
            "(CUSTOS_AUTH_CALL_CONTEXT_KEY_ROTATION=0); operator manages "
            "rotation externally."
        )
        return
    effective_sleep = asyncio.sleep if sleep is None else sleep
    while True:
        delay = _jittered_delay(rotation_period_seconds, jitter_ratio)
        try:
            await effective_sleep(delay)
        except asyncio.CancelledError:
            raise
        try:
            new_key = SigningKey.generate(created_at=datetime.fromtimestamp(clock(), UTC))
            key_ring.rotate(new_key)
            resolver.set_key(new_key)
        except Exception:  # pragma: no cover — defensive logging
            logger.exception(
                "call-context key rotation cycle failed; continuing with the previously active key"
            )


def _jittered_delay(base_seconds: int, jitter_ratio: float) -> float:
    """Return ``base_seconds`` perturbed by up to ±``jitter_ratio``."""
    if jitter_ratio <= 0:
        return float(base_seconds)
    spread = base_seconds * jitter_ratio
    return base_seconds + random.uniform(-spread, spread)


__all__ = [
    "DEFAULT_ROTATION_PERIOD_SECONDS",
    "JWKS_CACHE_FRACTION",
    "OVERLAP_FACTOR",
    "JwksEntry",
    "KeyRing",
    "TimeFunc",
    "install_key_age_metric",
    "run_rotation_loop",
]

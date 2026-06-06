"""Coarse token-bucket rate limiter for the Custos API Gateway (AGW-IMPL-010).

Write endpoints (`POST`/`PUT`/`PATCH`/`DELETE`) are rate limited on two
independent buckets — one per *principal* and one per *workspace*; a request is
admitted only when **both** buckets can afford its cost. Reads are unlimited in
v1 (:func:`is_rate_limited_method`).

The limiter is in-memory and per replica: an N-replica deployment grants up to
N times the configured limit in the worst case. That is acceptable because the
limit exists to shield downstream components (Workflow Service, Connector
Service) from runaway clients, not to bill or strictly enforce quotas. Switching
to a
Dapr-state-backed or Redis-backed coordinated limiter is a drop-in replacement:
the :meth:`RateLimiter.try_consume` interface (``(bucketKey, cost) -> Allow |
Deny``) stays the same (design.md § Rate Limiter, deferred to M2).

Each bucket is a classic token bucket: it holds up to ``burst`` tokens and
refills at ``rps`` tokens per second. A request costs one token by default.
On admission the limiter emits the IETF ``RateLimit-*`` headers; on rejection it
raises ``429 rate-limited`` carrying ``Retry-After`` plus the ``RateLimit-*``
headers (:func:`rate_limit_headers`).
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

from custos_gateway.errors import GatewayError, GatewayErrorCode

if TYPE_CHECKING:
    from collections.abc import Callable

    from custos_gateway.settings import Settings

__all__ = [
    "DEFAULT_COST",
    "RATE_LIMIT_LIMIT_HEADER",
    "RATE_LIMIT_REMAINING_HEADER",
    "RATE_LIMIT_RESET_HEADER",
    "RETRY_AFTER_HEADER",
    "WRITE_METHODS",
    "Allow",
    "BucketConfig",
    "Decision",
    "Deny",
    "RateLimiter",
    "is_rate_limited_method",
    "rate_limit_denied_error",
    "rate_limit_headers",
]

#: ``RateLimit-Limit`` — the bucket's quota (its burst capacity).
RATE_LIMIT_LIMIT_HEADER: Final[str] = "ratelimit-limit"

#: ``RateLimit-Remaining`` — tokens left in the binding bucket.
RATE_LIMIT_REMAINING_HEADER: Final[str] = "ratelimit-remaining"

#: ``RateLimit-Reset`` — seconds until the binding bucket refills to full.
RATE_LIMIT_RESET_HEADER: Final[str] = "ratelimit-reset"

#: ``Retry-After`` — seconds the client should wait before retrying a deny.
RETRY_AFTER_HEADER: Final[str] = "retry-after"

#: Methods that mutate state and are therefore rate limited. Reads are
#: unlimited in v1 and skip the limiter entirely.
WRITE_METHODS: Final[frozenset[str]] = frozenset({"POST", "PUT", "PATCH", "DELETE"})

#: Default token cost charged for a single request.
DEFAULT_COST: Final[int] = 1


def is_rate_limited_method(method: str) -> bool:
    """Return whether ``method`` is a write method subject to rate limiting."""
    return method.upper() in WRITE_METHODS


@dataclass(frozen=True, slots=True)
class BucketConfig:
    """Static token-bucket parameters for one limiter dimension."""

    rps: float
    burst: int

    def __post_init__(self) -> None:
        if self.rps <= 0:
            msg = f"rps must be positive, got {self.rps}"
            raise ValueError(msg)
        if self.burst < 1:
            msg = f"burst must be at least 1, got {self.burst}"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class Allow:
    """A request admitted by the limiter, with the binding bucket's quota view."""

    limit: int
    remaining: int
    reset_seconds: int


@dataclass(frozen=True, slots=True)
class Deny:
    """A request rejected by the limiter, with the most restrictive bucket's view."""

    limit: int
    remaining: int
    retry_after_seconds: int
    reset_seconds: int


#: The outcome of a limiter check.
Decision = Allow | Deny


def rate_limit_headers(decision: Decision) -> dict[str, str]:
    """Return the ``RateLimit-*`` (and, for a deny, ``Retry-After``) headers."""
    headers = {
        RATE_LIMIT_LIMIT_HEADER: str(decision.limit),
        RATE_LIMIT_REMAINING_HEADER: str(decision.remaining),
        RATE_LIMIT_RESET_HEADER: str(decision.reset_seconds),
    }
    if isinstance(decision, Deny):
        headers[RETRY_AFTER_HEADER] = str(decision.retry_after_seconds)
    return headers


def rate_limit_denied_error(decision: Deny) -> GatewayError:
    """Build the ``429 rate-limited`` error for a rejected request."""
    return GatewayError(
        GatewayErrorCode.RATE_LIMITED,
        detail="Rate limit exceeded; retry after the indicated delay.",
        headers=rate_limit_headers(decision),
    )


def _reset_seconds(config: BucketConfig, tokens: float) -> int:
    """Return seconds until ``config``'s bucket refills from ``tokens`` to full."""
    return math.ceil((config.burst - tokens) / config.rps)


def _retry_after_seconds(config: BucketConfig, tokens: float, cost: int) -> int:
    """Return seconds until ``config``'s bucket holds ``cost`` tokens (at least 1)."""
    return max(1, math.ceil((cost - tokens) / config.rps))


def _allow_from(config: BucketConfig, tokens: float) -> Allow:
    return Allow(
        limit=config.burst,
        remaining=int(tokens),
        reset_seconds=_reset_seconds(config, tokens),
    )


def _deny_from(config: BucketConfig, tokens: float, cost: int) -> Deny:
    return Deny(
        limit=config.burst,
        remaining=int(tokens),
        retry_after_seconds=_retry_after_seconds(config, tokens, cost),
        reset_seconds=_reset_seconds(config, tokens),
    )


@dataclass(slots=True)
class _BucketState:
    """Mutable per-key token-bucket state."""

    tokens: float
    updated_at: float


@dataclass(slots=True)
class RateLimiter:
    """In-memory per-replica token-bucket limiter over principal + workspace buckets.

    ``time_source`` is injectable (defaults to :func:`time.monotonic`) so tests
    can drive the clock deterministically.
    """

    principal_config: BucketConfig
    workspace_config: BucketConfig
    time_source: Callable[[], float] = time.monotonic
    _buckets: dict[str, _BucketState] = field(default_factory=dict, init=False, repr=False)

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        *,
        time_source: Callable[[], float] = time.monotonic,
    ) -> RateLimiter:
        """Build a limiter from the parsed ``CUSTOS_GATEWAY_RATE_LIMIT_*`` settings."""
        return cls(
            principal_config=BucketConfig(
                rps=settings.rate_limit_principal_writes_rps,
                burst=settings.rate_limit_principal_writes_burst,
            ),
            workspace_config=BucketConfig(
                rps=settings.rate_limit_workspace_writes_rps,
                burst=settings.rate_limit_workspace_writes_burst,
            ),
            time_source=time_source,
        )

    def try_consume(
        self,
        bucket_key: str,
        config: BucketConfig,
        cost: int = DEFAULT_COST,
    ) -> Decision:
        """Charge ``cost`` tokens to a single bucket and return the decision.

        A fresh bucket starts full (``config.burst`` tokens). The bucket is
        refilled for the elapsed time before the charge is attempted; an
        insufficient balance is left untouched (only refilled) and rejected.
        """
        now = self.time_source()
        state = self._bucket(bucket_key, config, now)
        tokens = self._refilled(state, config, now)
        state.updated_at = now
        if tokens < cost:
            state.tokens = tokens
            return _deny_from(config, tokens, cost)
        state.tokens = tokens - cost
        return _allow_from(config, state.tokens)

    def check(
        self,
        *,
        principal_id: str,
        workspace_id: str,
        cost: int = DEFAULT_COST,
    ) -> Decision:
        """Atomically charge the principal and workspace buckets.

        The request is admitted only when **both** buckets can afford ``cost``;
        tokens are consumed solely on admission. A rejection reports the most
        restrictive bucket (the longest ``Retry-After``) and consumes nothing.
        """
        now = self.time_source()
        p_state = self._bucket(_principal_key(principal_id), self.principal_config, now)
        w_state = self._bucket(_workspace_key(workspace_id), self.workspace_config, now)
        p_tokens = self._refilled(p_state, self.principal_config, now)
        w_tokens = self._refilled(w_state, self.workspace_config, now)
        p_state.updated_at = w_state.updated_at = now

        blocked = [
            (config, tokens)
            for config, tokens in (
                (self.principal_config, p_tokens),
                (self.workspace_config, w_tokens),
            )
            if tokens < cost
        ]
        if blocked:
            p_state.tokens = p_tokens
            w_state.tokens = w_tokens
            config, tokens = max(blocked, key=lambda ct: _retry_after_seconds(ct[0], ct[1], cost))
            return _deny_from(config, tokens, cost)

        p_state.tokens = p_tokens - cost
        w_state.tokens = w_tokens - cost
        config, tokens = min(
            (
                (self.principal_config, p_state.tokens),
                (self.workspace_config, w_state.tokens),
            ),
            key=lambda ct: ct[1],
        )
        return _allow_from(config, tokens)

    def _bucket(self, key: str, config: BucketConfig, now: float) -> _BucketState:
        return self._buckets.setdefault(
            key, _BucketState(tokens=float(config.burst), updated_at=now)
        )

    @staticmethod
    def _refilled(state: _BucketState, config: BucketConfig, now: float) -> float:
        elapsed = max(0.0, now - state.updated_at)
        return min(float(config.burst), state.tokens + elapsed * config.rps)


def _principal_key(principal_id: str) -> str:
    return f"principal:{principal_id}"


def _workspace_key(workspace_id: str) -> str:
    return f"workspace:{workspace_id}"

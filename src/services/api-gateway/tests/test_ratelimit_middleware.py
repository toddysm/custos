"""Tests for the token-bucket Rate Limiter (AGW-IMPL-010)."""

from __future__ import annotations

import pytest

from custos_gateway.errors import GatewayError, GatewayErrorCode
from custos_gateway.middleware.ratelimit import (
    RATE_LIMIT_LIMIT_HEADER,
    RATE_LIMIT_REMAINING_HEADER,
    RATE_LIMIT_RESET_HEADER,
    RETRY_AFTER_HEADER,
    Allow,
    BucketConfig,
    Deny,
    RateLimiter,
    is_rate_limited_method,
    rate_limit_denied_error,
    rate_limit_headers,
)


class _Clock:
    """A manually advanced monotonic clock."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _limiter(
    *,
    principal: BucketConfig | None = None,
    workspace: BucketConfig | None = None,
    clock: _Clock | None = None,
) -> tuple[RateLimiter, _Clock]:
    clock = clock or _Clock()
    limiter = RateLimiter(
        principal_config=principal or BucketConfig(rps=10, burst=5),
        workspace_config=workspace or BucketConfig(rps=100, burst=50),
        time_source=clock,
    )
    return limiter, clock


# --- is_rate_limited_method --------------------------------------------------


@pytest.mark.parametrize("method", ["POST", "put", "Patch", "DELETE"])
def test_write_methods_are_rate_limited(method: str) -> None:
    assert is_rate_limited_method(method) is True


@pytest.mark.parametrize("method", ["GET", "head", "OPTIONS", "TRACE"])
def test_read_methods_are_not_rate_limited(method: str) -> None:
    assert is_rate_limited_method(method) is False


# --- BucketConfig validation -------------------------------------------------


@pytest.mark.parametrize("rps", [0, -1, -0.5])
def test_bucket_config_rejects_non_positive_rps(rps: float) -> None:
    with pytest.raises(ValueError, match="rps must be positive"):
        BucketConfig(rps=rps, burst=10)


@pytest.mark.parametrize("burst", [0, -1])
def test_bucket_config_rejects_burst_below_one(burst: int) -> None:
    with pytest.raises(ValueError, match="burst must be at least 1"):
        BucketConfig(rps=10, burst=burst)


# --- try_consume (single bucket) ---------------------------------------------


def test_try_consume_starts_full_and_admits() -> None:
    limiter, _ = _limiter()
    config = BucketConfig(rps=10, burst=3)
    decision = limiter.try_consume("k", config)
    assert isinstance(decision, Allow)
    assert decision.limit == 3
    assert decision.remaining == 2


def test_try_consume_denies_when_drained() -> None:
    limiter, _ = _limiter()
    config = BucketConfig(rps=10, burst=2)
    assert isinstance(limiter.try_consume("k", config), Allow)
    assert isinstance(limiter.try_consume("k", config), Allow)
    decision = limiter.try_consume("k", config)
    assert isinstance(decision, Deny)
    assert decision.remaining == 0
    assert decision.retry_after_seconds >= 1


def test_try_consume_refills_over_time() -> None:
    limiter, clock = _limiter()
    config = BucketConfig(rps=10, burst=1)
    assert isinstance(limiter.try_consume("k", config), Allow)
    assert isinstance(limiter.try_consume("k", config), Deny)
    clock.advance(0.1)  # 0.1s * 10rps == 1 token
    assert isinstance(limiter.try_consume("k", config), Allow)


def test_try_consume_caps_refill_at_burst() -> None:
    limiter, clock = _limiter()
    config = BucketConfig(rps=10, burst=2)
    assert isinstance(limiter.try_consume("k", config), Allow)
    clock.advance(100)  # would overfill, but capped at burst
    decision = limiter.try_consume("k", config)
    assert isinstance(decision, Allow)
    assert decision.remaining == 1  # 2 (capped) - 1 consumed


def test_try_consume_independent_keys() -> None:
    limiter, _ = _limiter()
    config = BucketConfig(rps=10, burst=1)
    assert isinstance(limiter.try_consume("a", config), Allow)
    assert isinstance(limiter.try_consume("b", config), Allow)
    assert isinstance(limiter.try_consume("a", config), Deny)


def test_try_consume_honours_cost() -> None:
    limiter, _ = _limiter()
    config = BucketConfig(rps=10, burst=5)
    decision = limiter.try_consume("k", config, cost=3)
    assert isinstance(decision, Allow)
    assert decision.remaining == 2
    assert isinstance(limiter.try_consume("k", config, cost=3), Deny)


@pytest.mark.parametrize("cost", [0, -1])
def test_try_consume_rejects_non_positive_cost(cost: int) -> None:
    limiter, _ = _limiter()
    with pytest.raises(ValueError, match="cost must be at least 1"):
        limiter.try_consume("k", BucketConfig(rps=10, burst=5), cost=cost)


def test_try_consume_rejects_cost_above_burst() -> None:
    limiter, _ = _limiter()
    with pytest.raises(ValueError, match="exceeds the bucket burst"):
        limiter.try_consume("k", BucketConfig(rps=10, burst=5), cost=6)


# --- bucket-tracking cap (memory-DoS guard) ----------------------------------


def test_limiter_rejects_non_positive_bucket_cap() -> None:
    with pytest.raises(ValueError, match="max_tracked_buckets must be at least 1"):
        RateLimiter(
            principal_config=BucketConfig(rps=10, burst=5),
            workspace_config=BucketConfig(rps=100, burst=50),
            max_tracked_buckets=0,
        )


def test_limiter_evicts_least_recently_used_bucket() -> None:
    clock = _Clock()
    limiter = RateLimiter(
        principal_config=BucketConfig(rps=1, burst=1),
        workspace_config=BucketConfig(rps=1, burst=1),
        time_source=clock,
        # Cap of 2 buckets: one check() touches a principal + a workspace bucket.
        max_tracked_buckets=2,
    )
    assert isinstance(limiter.check(principal_id="p1", workspace_id="w1"), Allow)
    clock.advance(100)  # let buckets refill so eviction does not change outcomes
    # A second principal/workspace pair overflows the cap and evicts the oldest.
    assert isinstance(limiter.check(principal_id="p2", workspace_id="w2"), Allow)
    assert len(limiter._buckets) == 2


# --- check (combined principal + workspace) ----------------------------------


def test_check_admits_when_both_buckets_afford() -> None:
    limiter, _ = _limiter()
    decision = limiter.check(principal_id="p", workspace_id="w")
    assert isinstance(decision, Allow)
    # principal (burst 5) is the binding (smaller) bucket.
    assert decision.limit == 5
    assert decision.remaining == 4


def test_check_rejects_cost_above_smaller_burst() -> None:
    limiter, _ = _limiter(
        principal=BucketConfig(rps=10, burst=3),
        workspace=BucketConfig(rps=100, burst=50),
    )
    with pytest.raises(ValueError, match="exceeds the bucket burst"):
        limiter.check(principal_id="p", workspace_id="w", cost=4)


@pytest.mark.parametrize("cost", [0, -2])
def test_check_rejects_non_positive_cost(cost: int) -> None:
    limiter, _ = _limiter()
    with pytest.raises(ValueError, match="cost must be at least 1"):
        limiter.check(principal_id="p", workspace_id="w", cost=cost)


def test_check_denied_by_principal_bucket() -> None:
    limiter, _ = _limiter(principal=BucketConfig(rps=10, burst=1))
    assert isinstance(limiter.check(principal_id="p", workspace_id="w"), Allow)
    decision = limiter.check(principal_id="p", workspace_id="w")
    assert isinstance(decision, Deny)
    assert decision.limit == 1
    assert decision.retry_after_seconds >= 1


def test_check_denied_by_workspace_bucket() -> None:
    limiter, _ = _limiter(
        principal=BucketConfig(rps=100, burst=50),
        workspace=BucketConfig(rps=10, burst=1),
    )
    assert isinstance(limiter.check(principal_id="p", workspace_id="w"), Allow)
    # A different principal still shares the same workspace bucket.
    decision = limiter.check(principal_id="other", workspace_id="w")
    assert isinstance(decision, Deny)
    assert decision.limit == 1


def test_check_deny_does_not_charge_the_other_bucket() -> None:
    # The principal bucket admits one request per principal then denies; the
    # workspace bucket holds 3 tokens. A principal-caused deny must NOT consume
    # a workspace token, so exactly three distinct principals can be admitted.
    limiter, _ = _limiter(
        principal=BucketConfig(rps=0.001, burst=1),
        workspace=BucketConfig(rps=0.001, burst=3),
    )
    assert isinstance(limiter.check(principal_id="p1", workspace_id="w"), Allow)
    # p1 is now exhausted: this deny must leave the workspace bucket untouched.
    assert isinstance(limiter.check(principal_id="p1", workspace_id="w"), Deny)
    # Two more fresh principals fit within the workspace's remaining capacity.
    assert isinstance(limiter.check(principal_id="p2", workspace_id="w"), Allow)
    assert isinstance(limiter.check(principal_id="p3", workspace_id="w"), Allow)
    # The workspace bucket is now drained; a fourth principal is denied by it.
    assert isinstance(limiter.check(principal_id="p4", workspace_id="w"), Deny)


def test_check_reports_most_restrictive_on_deny() -> None:
    limiter, _ = _limiter(
        principal=BucketConfig(rps=1, burst=1),
        workspace=BucketConfig(rps=2, burst=1),
    )
    assert isinstance(limiter.check(principal_id="p", workspace_id="w"), Allow)
    decision = limiter.check(principal_id="p", workspace_id="w")
    assert isinstance(decision, Deny)
    # Slower refill (principal, rps=1) has the longer Retry-After.
    assert decision.retry_after_seconds == 1
    assert decision.limit == 1


# --- from_settings -----------------------------------------------------------


def test_from_settings_builds_configured_buckets() -> None:
    class _Settings:
        rate_limit_principal_writes_rps = 7
        rate_limit_principal_writes_burst = 9
        rate_limit_workspace_writes_rps = 70
        rate_limit_workspace_writes_burst = 90

    clock = _Clock()
    limiter = RateLimiter.from_settings(_Settings(), time_source=clock)  # type: ignore[arg-type]
    assert limiter.principal_config == BucketConfig(rps=7, burst=9)
    assert limiter.workspace_config == BucketConfig(rps=70, burst=90)
    decision = limiter.check(principal_id="p", workspace_id="w")
    assert isinstance(decision, Allow)
    assert decision.limit == 9  # principal is the smaller (binding) bucket


# --- headers + error ---------------------------------------------------------


def test_rate_limit_headers_for_allow_omit_retry_after() -> None:
    headers = rate_limit_headers(Allow(limit=5, remaining=3, reset_seconds=2))
    assert headers == {
        RATE_LIMIT_LIMIT_HEADER: "5",
        RATE_LIMIT_REMAINING_HEADER: "3",
        RATE_LIMIT_RESET_HEADER: "2",
    }
    assert RETRY_AFTER_HEADER not in headers


def test_rate_limit_headers_for_deny_include_retry_after() -> None:
    headers = rate_limit_headers(
        Deny(limit=5, remaining=0, retry_after_seconds=4, reset_seconds=6),
    )
    assert headers[RATE_LIMIT_LIMIT_HEADER] == "5"
    assert headers[RATE_LIMIT_REMAINING_HEADER] == "0"
    assert headers[RATE_LIMIT_RESET_HEADER] == "6"
    assert headers[RETRY_AFTER_HEADER] == "4"


def test_rate_limit_denied_error_carries_status_and_headers() -> None:
    deny = Deny(limit=5, remaining=0, retry_after_seconds=4, reset_seconds=6)
    error = rate_limit_denied_error(deny)
    assert isinstance(error, GatewayError)
    assert error.code is GatewayErrorCode.RATE_LIMITED
    assert error.status == 429
    assert error.headers is not None
    assert error.headers[RETRY_AFTER_HEADER] == "4"
    assert error.headers[RATE_LIMIT_LIMIT_HEADER] == "5"

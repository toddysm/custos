"""Tests for the Idempotency Coordinator (AGW-IMPL-009)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest
from custos_spl import (
    ExistingCompleted,
    ExistingInFlight,
    IdempotencyRecord,
    IdemReserved,
    KeyReuse,
    PrincipalId,
    ReserveIdempotencyResult,
    WorkspaceId,
)

from custos_gateway.errors import GatewayError, GatewayErrorCode
from custos_gateway.middleware.idempotency import (
    HASHED_REQUEST_HEADERS,
    RETRY_AFTER_HEADER,
    IdempotencyCoordinator,
    IdempotencyKey,
    ProceedReservation,
    ReplayReservation,
    compute_request_hash,
    is_idempotent_method,
    resolve_idempotency_key,
)


def _record(request_hash: str = "hash", status: str = "in_progress") -> IdempotencyRecord:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return IdempotencyRecord(
        workspace_id=WorkspaceId("ws_1"),
        principal_id=PrincipalId("sa_1"),
        route="/v1/things",
        idempotency_key="key-1",
        request_hash=request_hash,
        status=status,
        response_snapshot=None,
        reserved_at=now,
        expires_at=now,
    )


@dataclass(slots=True)
class _ReserveCall:
    workspace_id: str
    principal_id: str
    route: str
    idempotency_key: str
    request_hash: str
    ttl_seconds: int


@dataclass(slots=True)
class _CompleteCall:
    workspace_id: str
    principal_id: str
    route: str
    idempotency_key: str
    response_snapshot: Mapping[str, Any]


@dataclass(slots=True)
class _FakeStore:
    """Records calls and returns a canned reserve outcome."""

    outcome: ReserveIdempotencyResult = field(
        default_factory=lambda: IdemReserved(record=_record())
    )
    reserve_calls: list[_ReserveCall] = field(default_factory=list)
    complete_calls: list[_CompleteCall] = field(default_factory=list)

    async def reserve_idempotency_record(
        self,
        workspace_id: WorkspaceId,
        principal_id: PrincipalId,
        route: str,
        idempotency_key: str,
        request_hash: str,
        ttl_seconds: int,
    ) -> ReserveIdempotencyResult:
        self.reserve_calls.append(
            _ReserveCall(
                workspace_id=workspace_id,
                principal_id=principal_id,
                route=route,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                ttl_seconds=ttl_seconds,
            )
        )
        return self.outcome

    async def complete_idempotency_record(
        self,
        workspace_id: WorkspaceId,
        principal_id: PrincipalId,
        route: str,
        idempotency_key: str,
        response_snapshot: Mapping[str, Any],
    ) -> IdempotencyRecord:
        self.complete_calls.append(
            _CompleteCall(
                workspace_id=workspace_id,
                principal_id=principal_id,
                route=route,
                idempotency_key=idempotency_key,
                response_snapshot=response_snapshot,
            )
        )
        return _record(status="completed")


_KEY = IdempotencyKey(
    workspace_id="ws_1",
    principal_id="sa_1",
    route="/v1/things",
    idempotency_key="key-1",
)


# --- is_idempotent_method ----------------------------------------------------


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE", "post", "Patch"])
def test_write_methods_are_idempotent(method: str) -> None:
    assert is_idempotent_method(method) is True


@pytest.mark.parametrize("method", ["GET", "HEAD", "OPTIONS", "get"])
def test_read_methods_skip_coordinator(method: str) -> None:
    assert is_idempotent_method(method) is False


# --- resolve_idempotency_key -------------------------------------------------


def test_resolve_idempotency_key_honours_header_verbatim() -> None:
    assert resolve_idempotency_key("  opaque-key  ") == "  opaque-key  "


def test_resolve_idempotency_key_generates_when_absent() -> None:
    first = resolve_idempotency_key(None)
    second = resolve_idempotency_key("   ")
    assert first and second
    assert first != second


# --- compute_request_hash ----------------------------------------------------


def _hash(**overrides: Any) -> str:
    base: dict[str, Any] = {
        "method": "POST",
        "route": "/v1/things",
        "workspace_id": "ws_1",
        "headers": {"content-type": "application/json"},
        "body": b'{"a":1}',
    }
    base.update(overrides)
    return compute_request_hash(**base)


def test_request_hash_is_deterministic_sha256_hex() -> None:
    digest = _hash()
    assert digest == _hash()
    assert len(digest) == 64
    assert all(c in "0123456789abcdef" for c in digest)


@pytest.mark.parametrize(
    "field_name,value",
    [
        ("method", "PUT"),
        ("route", "/v1/widgets"),
        ("workspace_id", "ws_2"),
        ("headers", {"content-type": "application/cbor"}),
        ("body", b'{"a":2}'),
    ],
)
def test_request_hash_changes_with_each_component(field_name: str, value: Any) -> None:
    assert _hash() != _hash(**{field_name: value})


def test_request_hash_ignores_headers_outside_subset() -> None:
    assert _hash(headers={"content-type": "application/json", "x-trace": "abc"}) == _hash()
    # The content-type header is the only one that participates.
    assert "content-type" in HASHED_REQUEST_HEADERS


def test_request_hash_handles_missing_content_type() -> None:
    # No content-type header → still a stable, distinct digest (no crash).
    assert _hash(headers={}) == _hash(headers={})
    assert _hash(headers={}) != _hash()


# --- IdempotencyCoordinator.reserve ------------------------------------------


async def test_reserve_proceeds_when_newly_reserved() -> None:
    store = _FakeStore(outcome=IdemReserved(record=_record()))
    coordinator = IdempotencyCoordinator(store=store, ttl_seconds=3600)

    outcome = await coordinator.reserve(_KEY, "req-hash")

    assert isinstance(outcome, ProceedReservation)
    assert outcome.key is _KEY
    assert outcome.request_hash == "req-hash"
    assert len(store.reserve_calls) == 1
    call = store.reserve_calls[0]
    assert call.workspace_id == "ws_1"
    assert call.principal_id == "sa_1"
    assert call.route == "/v1/things"
    assert call.idempotency_key == "key-1"
    assert call.request_hash == "req-hash"
    assert call.ttl_seconds == 3600


async def test_reserve_replays_existing_completed_snapshot() -> None:
    snapshot = {"status": 201, "body": {"id": "thing_1"}}
    store = _FakeStore(
        outcome=ExistingCompleted(record=_record(status="completed"), response_snapshot=snapshot)
    )
    coordinator = IdempotencyCoordinator(store=store, ttl_seconds=3600)

    outcome = await coordinator.reserve(_KEY, "req-hash")

    assert isinstance(outcome, ReplayReservation)
    assert outcome.response_snapshot == snapshot


async def test_reserve_in_flight_raises_with_retry_after() -> None:
    store = _FakeStore(outcome=ExistingInFlight(record=_record()))
    coordinator = IdempotencyCoordinator(store=store, ttl_seconds=3600, retry_after_seconds=5)

    with pytest.raises(GatewayError) as exc_info:
        await coordinator.reserve(_KEY, "req-hash")

    assert exc_info.value.code is GatewayErrorCode.IDEMPOTENCY_IN_FLIGHT
    assert exc_info.value.headers == {RETRY_AFTER_HEADER: "5"}


async def test_reserve_key_reuse_raises_without_retry_after() -> None:
    store = _FakeStore(outcome=KeyReuse(record=_record(request_hash="other")))
    coordinator = IdempotencyCoordinator(store=store, ttl_seconds=3600)

    with pytest.raises(GatewayError) as exc_info:
        await coordinator.reserve(_KEY, "req-hash")

    assert exc_info.value.code is GatewayErrorCode.IDEMPOTENCY_KEY_REUSE
    assert exc_info.value.headers is None


async def test_reserve_uses_default_retry_after() -> None:
    store = _FakeStore(outcome=ExistingInFlight(record=_record()))
    coordinator = IdempotencyCoordinator(store=store, ttl_seconds=3600)

    with pytest.raises(GatewayError) as exc_info:
        await coordinator.reserve(_KEY, "req-hash")

    assert exc_info.value.headers == {RETRY_AFTER_HEADER: "1"}


# --- IdempotencyCoordinator.complete -----------------------------------------


async def test_complete_records_response_snapshot() -> None:
    store = _FakeStore()
    coordinator = IdempotencyCoordinator(store=store, ttl_seconds=3600)
    snapshot = {"status": 200, "body": {"ok": True}}

    await coordinator.complete(_KEY, snapshot)

    assert len(store.complete_calls) == 1
    call = store.complete_calls[0]
    assert call.workspace_id == "ws_1"
    assert call.principal_id == "sa_1"
    assert call.route == "/v1/things"
    assert call.idempotency_key == "key-1"
    assert call.response_snapshot == snapshot

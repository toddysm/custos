"""Replay input privacy, serialization, bounds and provider-independent fencing."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from custos_spl.errors import ImmutableViolation, LeaseBusy, LeaseExpired
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.dead_letters import (
    MAX_PAYLOAD_BYTES,
    DeadLetterData,
    DeadLetterFilter,
    DispatchAttempt,
    claim_replay,
    decode_dead_letter,
    discard_dead_letter,
    encode_dead_letter,
    finish_replay,
    json_value,
    new_dead_letter,
    recover_replay,
)

NOW = datetime(2026, 10, 29, tzinfo=UTC)


@pytest.fixture
def data() -> DeadLetterData:
    """Provide one resolved start capture without a transport frame."""
    return DeadLetterData(
        workspace_id=WorkspaceId("workspace"),
        entry_id="entry",
        subscription_id="sub",
        idempotency_key="original-key",
        event={
            "schemaVersion": "1",
            "eventId": "evt",
            "kind": "custom.test",
            "source": {"type": "manual", "occurredAt": NOW.isoformat()},
            "data": {"x": [1]},
        },
        target={"kind": "start", "workflowVersionId": "wf-v1", "inputs": {}},
        history=(DispatchAttempt(NOW, "transient", 503),),
    )


def test_roundtrip_and_deep_immutability(data: DeadLetterData) -> None:
    """Input aliases and returned nested collections cannot mutate captured replay inputs."""
    values: dict[str, Any] = {"value": [{"nested": "original"}]}
    item = replace(data, event={**data.event, "data": values})
    values["value"][0]["nested"] = "changed"
    stored = new_dead_letter(item, NOW)
    assert "original" in encode_dead_letter(stored) and "changed" not in encode_dead_letter(stored)
    nested = item.event["data"]
    assert isinstance(nested, Mapping)
    with pytest.raises(TypeError):
        nested["value"] = "mutated"  # type: ignore[index]
    claimed = claim_replay(stored, "worker", 30, NOW)
    assert decode_dead_letter(encode_dead_letter(claimed)) == claimed
    assert claimed.claim is not None
    finished = finish_replay(claimed, claimed.claim, NOW, result_id="run")
    assert decode_dead_letter(encode_dead_letter(finished)) == finished


@pytest.mark.parametrize(
    "key",
    [
        "Authorization",
        "proxy-authorization",
        "Cookie",
        "Set-Cookie",
        "signature",
        "signing_key",
        "privateKey",
        "credentials",
        "access_token",
        "X-Api-Key",
        "headers",
        "raw",
        "x-custos-call-context",
        "X-Custos-Callctx",
        "token",
        "X-Hub-Signature-256",
    ],
)
def test_private_fields_fail_closed(data: DeadLetterData, key: str) -> None:
    """Even a mapped nested copy of private verification data is refused before persistence."""
    with pytest.raises(ValueError, match="private transport"):
        replace(
            data,
            target={
                "kind": "start",
                "workflowVersionId": "v",
                "inputs": {"nested": [{key: "private"}]},
            },
        )


@pytest.mark.parametrize("value", [float("nan"), float("inf"), b"bytes", object(), "\0", "\ud800"])
def test_non_json_values_rejected(data: DeadLetterData, value: Any) -> None:
    """Invalid database JSON is rejected in memory too, not only after reaching PostgreSQL."""
    with pytest.raises((ValueError, UnicodeError)):
        replace(data, event={**data.event, "data": {"bad": value}})


@pytest.mark.parametrize(
    "changes",
    [
        {"schema_version": 2},
        {"schema_version": True},
        {"retention_seconds": 0},
        {"retention_seconds": True},
        {"retention_seconds": 31_536_001},
        {"max_replay_attempts": 0},
        {"max_replay_attempts": 101},
        {"history": ()},
        {"workspace_id": ""},
        {"entry_id": "x" * 513},
        {"target": {"kind": "start", "workflowVersionId": "v", "inputs": {}, "headers": {}}},
    ],
)
def test_record_bounds(data: DeadLetterData, changes: dict[str, Any]) -> None:
    """Validate identities, closed versions, attempt limits and finite retention."""
    with pytest.raises(ValueError):
        replace(data, **changes)


def test_payload_and_depth_thresholds(data: DeadLetterData) -> None:
    """Enforce the actual encoded-byte bound, including multibyte content and nesting."""
    base = replace(data, event={**data.event, "data": {"text": ""}})
    empty_bytes = len(json.dumps(json_value(base.event), ensure_ascii=False).encode())
    exact = replace(
        base, event={**base.event, "data": {"text": "a" * (MAX_PAYLOAD_BYTES - empty_bytes)}}
    )
    assert (
        len(json.dumps(json_value(exact.event), ensure_ascii=False).encode()) == MAX_PAYLOAD_BYTES
    )
    with pytest.raises(ValueError, match="256 KiB"):
        replace(
            base,
            event={**base.event, "data": {"text": "a" * (MAX_PAYLOAD_BYTES - empty_bytes + 1)}},
        )
    with pytest.raises(ValueError, match="256 KiB"):
        replace(
            data, event={**data.event, "data": {"text": "\U0001f600" * (MAX_PAYLOAD_BYTES // 4)}}
        )
    nested: Any = "value"
    for _ in range(34):
        nested = [nested]
    with pytest.raises(ValueError, match="depth"):
        replace(data, event={**data.event, "data": {"nested": nested}})
    with pytest.raises(ValueError, match=r"1\.\.64"):
        replace(data, history=data.history * 65)


def test_claim_attempt_limit_and_interruption(data: DeadLetterData) -> None:
    """Crashes consume attempts; recovery discards the final abandoned attempt."""
    entry = new_dead_letter(replace(data, max_replay_attempts=1), NOW)
    claimed = claim_replay(entry, "worker", 30, NOW)
    assert claimed.claim is not None
    with pytest.raises(LeaseBusy):
        claim_replay(claimed, "worker", 30, NOW)
    recovered = recover_replay(claimed, NOW + timedelta(seconds=30))
    assert recovered.status == "discarded"
    assert recovered.replay_history[0].outcome == "interrupted"
    with pytest.raises(ImmutableViolation):
        claim_replay(recovered, "new", 30, NOW + timedelta(seconds=30))
    with pytest.raises(LeaseExpired):
        finish_replay(claimed, claimed.claim, NOW + timedelta(seconds=30))


def test_retention_and_active_claim_boundaries(data: DeadLetterData) -> None:
    """Live completion may cross retention, but a new claim and active discard are prohibited."""
    entry = new_dead_letter(replace(data, retention_seconds=1), NOW)
    with pytest.raises(ImmutableViolation):
        claim_replay(entry, "worker", 30, entry.expires_at)
    claimed = claim_replay(entry, "worker", 30, NOW)
    assert claimed.claim is not None
    with pytest.raises(LeaseBusy):
        discard_dead_letter(claimed, entry.expires_at)
    finished = finish_replay(claimed, claimed.claim, entry.expires_at)
    assert finished.status == "replayed"
    with pytest.raises(ImmutableViolation):
        discard_dead_letter(finished, entry.expires_at)


def test_filter_and_error_validation() -> None:
    """Reject malformed filters and unbounded/free-form exception details."""
    with pytest.raises(ValueError, match="since"):
        DeadLetterFilter(since=NOW, until=NOW - timedelta(seconds=1)).scope(WorkspaceId("ws"))
    with pytest.raises(ValueError, match="timezone-aware"):
        DeadLetterFilter(since=datetime(2026, 1, 1)).scope(WorkspaceId("ws"))
    with pytest.raises(ValueError, match="classification"):
        DispatchAttempt(NOW, "Bearer private")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="HTTP status"):
        DispatchAttempt(NOW, "transient", 999)

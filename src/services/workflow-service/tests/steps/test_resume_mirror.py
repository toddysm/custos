"""Tests for ``ResumeSubscriptionMirror`` + repository (WF-IMPL-102)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from custos_workflow.steps.resume import (
    InMemoryResumeSubscriptionMirrorRepository,
    ResumeSubscriptionMirror,
    ResumeSubscriptionMirrorRepository,
)
from custos_workflow.steps.resume.mirror import (
    InMemoryResumeSubscriptionMirrorRepository as MirrorRepoFromModule,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_REGISTERED_AT = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
_EXPIRES_AT = datetime(2025, 1, 2, 12, 0, 0, tzinfo=UTC)


def _mirror(
    *,
    mirror_id: str = "mir-1",
    run_id: str = "run-1",
    step_id: str = "step-a",
    event_key: str = "evt-1",
    ts_subscription_id: str = "ts-sub-1",
    selector: str | None = None,
    registered_at: datetime = _REGISTERED_AT,
    expires_at: datetime = _EXPIRES_AT,
) -> ResumeSubscriptionMirror:
    return ResumeSubscriptionMirror(
        mirror_id=mirror_id,
        run_id=run_id,
        step_id=step_id,
        event_key=event_key,
        ts_subscription_id=ts_subscription_id,
        selector=selector,
        registered_at=registered_at,
        expires_at=expires_at,
    )


# ---------------------------------------------------------------------------
# Entity: construction + invariants
# ---------------------------------------------------------------------------


def test_mirror_is_frozen_and_hashable() -> None:
    mirror = _mirror()
    with pytest.raises(AttributeError):
        mirror.mirror_id = "other"  # type: ignore[misc]
    # Hashable -> usable as a set / dict key.
    assert {mirror, _mirror()} == {mirror}


def test_mirror_uses_slots() -> None:
    mirror = _mirror()
    assert not hasattr(mirror, "__dict__")


def test_mirror_default_selector_is_none() -> None:
    assert _mirror().selector is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("mirror_id", ""),
        ("run_id", ""),
        ("step_id", ""),
        ("event_key", ""),
        ("ts_subscription_id", ""),
    ],
)
def test_mirror_rejects_empty_required_strings(field: str, value: str) -> None:
    with pytest.raises(ValueError, match=f"{field} must be a non-empty string"):
        _mirror(**{field: value})  # type: ignore[arg-type]


def test_mirror_rejects_empty_selector() -> None:
    with pytest.raises(ValueError, match="selector must be None or a non-empty string"):
        _mirror(selector="")


def test_mirror_accepts_non_empty_selector() -> None:
    assert _mirror(selector="$.payload.id").selector == "$.payload.id"


def test_mirror_rejects_naive_registered_at() -> None:
    with pytest.raises(ValueError, match="registered_at must be timezone-aware"):
        _mirror(registered_at=datetime(2025, 1, 1, 12, 0, 0))


def test_mirror_rejects_naive_expires_at() -> None:
    with pytest.raises(ValueError, match="expires_at must be timezone-aware"):
        _mirror(expires_at=datetime(2025, 1, 2, 12, 0, 0))


# ---------------------------------------------------------------------------
# Entity: serialization round-trip (byte-stable)
# ---------------------------------------------------------------------------


def test_to_dict_uses_camelcase_wire_keys() -> None:
    data = _mirror(selector="$.id").to_dict()
    assert data == {
        "mirrorId": "mir-1",
        "runId": "run-1",
        "stepId": "step-a",
        "eventKey": "evt-1",
        "selector": "$.id",
        "tsSubscriptionId": "ts-sub-1",
        "registeredAt": "2025-01-01T12:00:00+00:00",
        "expiresAt": "2025-01-02T12:00:00+00:00",
    }


def test_to_dict_emits_null_selector() -> None:
    assert _mirror().to_dict()["selector"] is None


def test_from_dict_round_trip_is_exact() -> None:
    mirror = _mirror(selector="$.id")
    assert ResumeSubscriptionMirror.from_dict(mirror.to_dict()) == mirror


def test_from_dict_round_trip_with_none_selector() -> None:
    mirror = _mirror()
    assert ResumeSubscriptionMirror.from_dict(mirror.to_dict()) == mirror


def test_to_json_is_byte_stable() -> None:
    # Two independently-built equal mirrors must serialize identically.
    assert _mirror(selector="$.id").to_json() == _mirror(selector="$.id").to_json()
    # Keys are sorted + separators tight.
    assert _mirror().to_json() == (
        '{"eventKey":"evt-1",'
        '"expiresAt":"2025-01-02T12:00:00+00:00",'
        '"mirrorId":"mir-1",'
        '"registeredAt":"2025-01-01T12:00:00+00:00",'
        '"runId":"run-1",'
        '"selector":null,'
        '"stepId":"step-a",'
        '"tsSubscriptionId":"ts-sub-1"}'
    )


def test_from_json_round_trip_is_exact() -> None:
    mirror = _mirror(selector="$.id")
    assert ResumeSubscriptionMirror.from_json(mirror.to_json()) == mirror


def test_to_dict_canonicalizes_datetimes_to_utc() -> None:
    # An instant expressed in a non-UTC offset serializes to the
    # equivalent UTC instant, not the literal offset.
    plus_five = timezone(timedelta(hours=5))
    mirror = _mirror(
        registered_at=datetime(2025, 1, 1, 17, 0, 0, tzinfo=plus_five),
        expires_at=datetime(2025, 1, 2, 17, 0, 0, tzinfo=plus_five),
    )
    data = mirror.to_dict()
    assert data["registeredAt"] == "2025-01-01T12:00:00+00:00"
    assert data["expiresAt"] == "2025-01-02T12:00:00+00:00"


def test_to_json_is_byte_stable_across_equal_offsets() -> None:
    # Two instant-equal mirrors built with different tz offsets are
    # `==` (aware datetime equality is by instant) and MUST serialize
    # byte-identically per the canonical-serialization contract.
    plus_five = timezone(timedelta(hours=5))
    utc_mirror = _mirror(
        registered_at=datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
        expires_at=datetime(2025, 1, 2, 12, 0, 0, tzinfo=UTC),
    )
    offset_mirror = _mirror(
        registered_at=datetime(2025, 1, 1, 17, 0, 0, tzinfo=plus_five),
        expires_at=datetime(2025, 1, 2, 17, 0, 0, tzinfo=plus_five),
    )
    assert utc_mirror == offset_mirror
    assert utc_mirror.to_json() == offset_mirror.to_json()


def test_from_dict_accepts_zulu_suffix() -> None:
    # Rows minted by other services may carry a trailing `Z`.
    data = _mirror().to_dict()
    data["registeredAt"] = "2025-01-01T12:00:00Z"
    data["expiresAt"] = "2025-01-02T12:00:00Z"
    restored = ResumeSubscriptionMirror.from_dict(data)
    assert restored == _mirror()
    assert restored.registered_at == datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)


def test_from_dict_missing_field_raises_key_error() -> None:
    data = _mirror().to_dict()
    del data["runId"]
    with pytest.raises(KeyError):
        ResumeSubscriptionMirror.from_dict(data)


# ---------------------------------------------------------------------------
# Repository Protocol
# ---------------------------------------------------------------------------


def test_in_memory_repo_satisfies_protocol() -> None:
    repo = InMemoryResumeSubscriptionMirrorRepository()
    assert isinstance(repo, ResumeSubscriptionMirrorRepository)


def test_package_and_module_export_same_class() -> None:
    assert InMemoryResumeSubscriptionMirrorRepository is MirrorRepoFromModule


# ---------------------------------------------------------------------------
# In-memory adapter behavior
# ---------------------------------------------------------------------------


async def test_put_returns_the_mirror_and_is_observable_via_list_open() -> None:
    repo = InMemoryResumeSubscriptionMirrorRepository()
    mirror = _mirror()
    # The mirror is written *before* any Trigger Service call; it must
    # be immediately observable (the crash-safety invariant).
    returned = await repo.put(mirror)
    assert returned is mirror
    assert await repo.list_open("run-1") == (mirror,)


async def test_put_upserts_on_mirror_id() -> None:
    repo = InMemoryResumeSubscriptionMirrorRepository()
    await repo.put(_mirror(ts_subscription_id="ts-sub-1"))
    # Replay re-registration returns a fresh subscription id; the
    # mirror row is replaced, not duplicated.
    updated = _mirror(ts_subscription_id="ts-sub-2")
    await repo.put(updated)
    rows = await repo.list_open("run-1")
    assert rows == (updated,)
    assert rows[0].ts_subscription_id == "ts-sub-2"


async def test_list_open_filters_by_run_id_and_sorts_by_mirror_id() -> None:
    repo = InMemoryResumeSubscriptionMirrorRepository()
    await repo.put(_mirror(mirror_id="mir-2", run_id="run-1"))
    await repo.put(_mirror(mirror_id="mir-1", run_id="run-1"))
    await repo.put(_mirror(mirror_id="mir-3", run_id="run-2"))
    rows = await repo.list_open("run-1")
    assert [row.mirror_id for row in rows] == ["mir-1", "mir-2"]


async def test_list_open_empty_for_unknown_run() -> None:
    repo = InMemoryResumeSubscriptionMirrorRepository()
    assert await repo.list_open("nope") == ()


async def test_list_open_for_step_filters_by_step() -> None:
    repo = InMemoryResumeSubscriptionMirrorRepository()
    await repo.put(_mirror(mirror_id="mir-1", run_id="run-1", step_id="step-a"))
    await repo.put(_mirror(mirror_id="mir-2", run_id="run-1", step_id="step-b"))
    await repo.put(_mirror(mirror_id="mir-3", run_id="run-2", step_id="step-a"))
    rows = await repo.list_open_for_step("run-1", "step-a")
    assert [row.mirror_id for row in rows] == ["mir-1"]


async def test_delete_removes_the_row() -> None:
    repo = InMemoryResumeSubscriptionMirrorRepository()
    await repo.put(_mirror(mirror_id="mir-1"))
    await repo.delete("mir-1")
    assert await repo.list_open("run-1") == ()


async def test_delete_unknown_id_is_a_no_op() -> None:
    repo = InMemoryResumeSubscriptionMirrorRepository()
    await repo.put(_mirror(mirror_id="mir-1"))
    await repo.delete("does-not-exist")
    assert len(await repo.list_open("run-1")) == 1


async def test_list_expired_honors_expires_at() -> None:
    repo = InMemoryResumeSubscriptionMirrorRepository()
    early = _mirror(mirror_id="mir-early", expires_at=_REGISTERED_AT + timedelta(hours=1))
    late = _mirror(mirror_id="mir-late", expires_at=_REGISTERED_AT + timedelta(hours=10))
    await repo.put(early)
    await repo.put(late)

    cutoff = _REGISTERED_AT + timedelta(hours=5)
    expired = await repo.list_expired(cutoff)
    assert expired == (early,)


async def test_list_expired_is_inclusive_of_the_cutoff() -> None:
    repo = InMemoryResumeSubscriptionMirrorRepository()
    boundary = _mirror(mirror_id="mir-boundary", expires_at=_EXPIRES_AT)
    await repo.put(boundary)
    # expires_at == before -> expired.
    assert await repo.list_expired(_EXPIRES_AT) == (boundary,)
    # A cutoff one microsecond earlier excludes it.
    assert await repo.list_expired(_EXPIRES_AT - timedelta(microseconds=1)) == ()


async def test_list_expired_sorts_by_mirror_id() -> None:
    repo = InMemoryResumeSubscriptionMirrorRepository()
    await repo.put(_mirror(mirror_id="mir-2", expires_at=_REGISTERED_AT))
    await repo.put(_mirror(mirror_id="mir-1", expires_at=_REGISTERED_AT))
    rows = await repo.list_expired(_EXPIRES_AT)
    assert [row.mirror_id for row in rows] == ["mir-1", "mir-2"]

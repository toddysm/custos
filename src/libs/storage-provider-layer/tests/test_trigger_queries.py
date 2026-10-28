"""Pin the additive trigger-query protocol and shared cursor validation contract."""

import base64
import inspect
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from custos_spl.errors import ImmutableViolation, WorkspaceMismatch
from custos_spl.ids import SubscriptionId, WorkflowId, WorkspaceId
from custos_spl.interfaces import ResumeRegistrationProvider, TriggerQueryProvider
from custos_spl.interfaces.metadata_store import Schedule, Subscription, SubscriptionSelector
from custos_spl.interfaces.trigger_queries import SubscriptionSnapshot, prepare_subscription_write
from custos_spl.pagination import (
    Cursor,
    decode_query_cursor,
    encode_query_cursor,
    query_page_limit,
)


@pytest.mark.parametrize(
    "name",
    [
        "get_subscription",
        "query_subscriptions",
        "get_resume_subscription",
        "query_resume_subscriptions",
        "get_schedule",
        "query_due_schedules",
    ],
)
def test_trigger_queries_are_async_and_workspace_scoped(name: str) -> None:
    """Each provider query requires an explicit workspace before filters."""
    method = getattr(TriggerQueryProvider, name)
    assert inspect.iscoroutinefunction(method)
    assert list(inspect.signature(method).parameters)[1] == "workspace_id"


def test_cursor_scope_round_trip() -> None:
    """Opaque tokens round-trip IDs and reject other tenants, filters and versions."""
    scope = ("subscriptions", "workspace", None)
    token = encode_query_cursor(scope, "sub-1")
    assert decode_query_cursor(None, scope) is None
    assert decode_query_cursor(token, scope) == "sub-1"
    with pytest.raises(ValueError, match="cursor"):
        decode_query_cursor(token, ("subscriptions", "other", None))
    with pytest.raises(ValueError, match="cursor"):
        decode_query_cursor(token, ("subscriptions", "workspace", "active"))
    invalid_payloads: tuple[object, ...] = (
        {},
        [2, list(scope), "id"],
        [True, list(scope), "id"],
        [1, list(scope), 3],
    )
    for data in invalid_payloads:
        malformed = Cursor(base64.urlsafe_b64encode(json.dumps(data).encode()).decode())
        with pytest.raises(ValueError, match="cursor"):
            decode_query_cursor(malformed, scope)


def test_query_page_size_bounds() -> None:
    """Page sizes are bounded, but invalid values never silently become defaults."""
    assert query_page_limit(None) == 50
    assert query_page_limit(1) == 1
    assert query_page_limit(999) == 200
    for value in (0, -1, True):
        with pytest.raises(ValueError, match="positive"):
            query_page_limit(value)


def test_atomic_registration_is_an_explicit_workspace_scoped_capability() -> None:
    """Atomic registration supplements, rather than weakens, immutable metadata writes."""
    method = ResumeRegistrationProvider.register_resume_subscription
    assert inspect.iscoroutinefunction(method)
    parameters = inspect.signature(method).parameters
    assert list(parameters)[1] == "workspace_id"
    assert parameters["as_of"].kind is inspect.Parameter.KEYWORD_ONLY


def test_subscription_write_revision_and_schedule_ownership() -> None:
    """Private revision keys are provider-owned; resets cannot adopt a foreign schedule."""
    now = datetime(2026, 1, 1, tzinfo=UTC)
    ws, identifier, workflow = WorkspaceId("ws"), SubscriptionId("sub"), WorkflowId("wf")
    row = Subscription(ws, identifier, workflow, "active", now, now)
    selector = SubscriptionSelector(
        ws, identifier, {"_revision": 999, "_scheduleId": "foreign"}, now
    )
    schedule = Schedule(ws, str(identifier), workflow, "* * * * *", now, True)
    created, managed = prepare_subscription_write(
        ws,
        row,
        selector,
        None,
        None,
        expected_revision=None,
        schedule=schedule,
        reset_schedule=True,
    )
    assert created.selector is not None
    assert created.selector.selector == {"_revision": 1, "_scheduleId": "sub"}
    assert managed == schedule
    for existing in (
        replace(schedule, workflow_id=WorkflowId("other")),
        replace(schedule, workspace_id=WorkspaceId("other")),
        replace(schedule, schedule_id="other"),
    ):
        with pytest.raises(ImmutableViolation, match="schedule"):
            prepare_subscription_write(
                ws,
                row,
                selector,
                created,
                existing,
                expected_revision=1,
                schedule=schedule,
                reset_schedule=True,
            )
    for revision in (None, 0, 2):
        with pytest.raises(ImmutableViolation):
            prepare_subscription_write(
                ws,
                row,
                selector,
                created,
                schedule,
                expected_revision=revision,
                schedule=schedule,
                reset_schedule=False,
            )
    with pytest.raises(WorkspaceMismatch):
        prepare_subscription_write(
            "other",
            row,
            selector,
            None,
            None,
            expected_revision=None,
            schedule=None,
            reset_schedule=False,
        )
    progressed = replace(schedule, next_fire_at=now + timedelta(hours=2))
    _, kept = prepare_subscription_write(
        ws,
        row,
        selector,
        created,
        progressed,
        expected_revision=1,
        schedule=schedule,
        reset_schedule=False,
    )
    assert kept == progressed
    _, disabled = prepare_subscription_write(
        ws,
        replace(row, state="paused"),
        selector,
        created,
        progressed,
        expected_revision=1,
        schedule=None,
        reset_schedule=False,
    )
    assert disabled == replace(progressed, enabled=False)


@pytest.mark.parametrize("revision", [True, -1])
def test_subscription_write_rejects_invalid_revision(revision: int) -> None:
    """Invalid caller revisions fail before any state can be mutated."""
    now = datetime(2026, 1, 1, tzinfo=UTC)
    row = Subscription(
        WorkspaceId("ws"), SubscriptionId("sub"), WorkflowId("wf"), "active", now, now
    )
    selector = SubscriptionSelector(row.workspace_id, row.subscription_id, {}, now)
    with pytest.raises(ValueError, match="expected_revision"):
        prepare_subscription_write(
            "ws",
            row,
            selector,
            SubscriptionSnapshot(row, selector),
            None,
            expected_revision=revision,
            schedule=None,
            reset_schedule=False,
        )


def test_subscription_creation_anchor_is_an_immutable_instant() -> None:
    """Repeated local hours cannot disguise a change to the interval's creation anchor."""
    first = datetime(2026, 11, 1, 1, 30, tzinfo=ZoneInfo("America/New_York"), fold=0)
    row = Subscription(
        WorkspaceId("ws"), SubscriptionId("sub"), WorkflowId("wf"), "active", first, first
    )
    selector = SubscriptionSelector(row.workspace_id, row.subscription_id, {}, first)
    current = SubscriptionSnapshot(row, selector)
    for changed in (
        replace(row, created_at=first.replace(fold=1)),
        replace(row, workflow_id=WorkflowId("other")),
    ):
        with pytest.raises(ImmutableViolation, match="identity"):
            prepare_subscription_write(
                "ws",
                changed,
                selector,
                current,
                None,
                expected_revision=0,
                schedule=None,
                reset_schedule=False,
            )
    with pytest.raises(ValueError, match="timezone-aware"):
        prepare_subscription_write(
            "ws",
            replace(row, created_at=first.replace(tzinfo=None)),
            selector,
            current,
            None,
            expected_revision=0,
            schedule=None,
            reset_schedule=False,
        )

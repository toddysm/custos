"""Pin the additive trigger-query protocol and shared cursor validation contract."""

import base64
import inspect
import json

import pytest

from custos_spl.interfaces import ResumeRegistrationProvider, TriggerQueryProvider
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

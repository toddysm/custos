"""Tests for LogQueryProvider Protocol and its data shapes."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from custos_spl.ids import RunId, StepId
from custos_spl.interfaces import (
    LogFilter,
    LogPage,
    LogQueryProvider,
    LogRecord,
)
from custos_spl.pagination import Cursor, Page


def _now() -> datetime:
    return datetime(2026, 1, 1, tzinfo=UTC)


# ----- Data shape -----


def test_log_record_is_frozen() -> None:
    r = LogRecord(
        timestamp=_now(),
        severity="info",
        message="hello",
        run_id=RunId("r-1"),
        step_id=None,
    )
    with pytest.raises(FrozenInstanceError):
        r.message = "other"  # type: ignore[misc]


def test_log_record_accepts_step_id_and_attributes() -> None:
    r = LogRecord(
        timestamp=_now(),
        severity="warn",
        message="hello",
        run_id=RunId("r-1"),
        step_id=StepId("s-1"),
        attributes={"trace.id": "abcd"},
    )
    assert r.step_id == StepId("s-1")
    assert r.attributes["trace.id"] == "abcd"


def test_log_filter_defaults_are_all_none() -> None:
    f = LogFilter()
    assert f.step_id is None
    assert f.start is None
    assert f.end is None
    assert f.severity_at_least is None
    assert f.message_contains is None


def test_log_page_is_an_alias_for_page_of_log_record() -> None:
    """`LogPage` is a type alias over `Page[LogRecord]`."""
    page: LogPage = Page(items=(), next_cursor=None)
    assert isinstance(page, Page)
    cursor = Cursor(token="opaque")
    page2: LogPage = Page(
        items=(
            LogRecord(
                timestamp=_now(),
                severity="info",
                message="m",
                run_id=RunId("r-1"),
                step_id=None,
            ),
        ),
        next_cursor=cursor,
    )
    assert page2.next_cursor is cursor


# ----- Protocol shape -----


def test_protocol_declares_schema_revision_zero() -> None:
    """Query facades own no schema; SCHEMA_REVISION is 0."""
    assert LogQueryProvider.SCHEMA_REVISION == 0


REQUIRED_METHODS = ["query_run_logs", "tail_run_logs", "query_step_logs"]


@pytest.mark.parametrize("method", REQUIRED_METHODS)
def test_protocol_exposes_method(method: str) -> None:
    assert hasattr(LogQueryProvider, method)


_NON_COROUTINE_METHODS = {"tail_run_logs"}


@pytest.mark.parametrize(
    "method", [m for m in REQUIRED_METHODS if m not in _NON_COROUTINE_METHODS]
)
def test_protocol_methods_are_async(method: str) -> None:
    fn = getattr(LogQueryProvider, method)
    assert inspect.iscoroutinefunction(fn), f"{method} must be async"


def test_tail_run_logs_is_not_a_coroutine_function() -> None:
    """`tail_run_logs` must return an `AsyncIterator` directly, not a coroutine.

    Pinned so a future refactor that accidentally writes
    `async def tail_run_logs` is caught — adapters expect to implement
    it as a plain `def` returning an async generator.
    """
    assert not inspect.iscoroutinefunction(LogQueryProvider.tail_run_logs)


@pytest.mark.parametrize("method", REQUIRED_METHODS)
def test_methods_take_workspace_id_first(method: str) -> None:
    sig = inspect.signature(getattr(LogQueryProvider, method))
    params = list(sig.parameters)
    # params[0] is self
    assert params[1] == "workspace_id", (
        f"{method} must take workspace_id as the first non-self argument"
    )


# ----- runtime_checkable conformance -----


class _MinimalLogQuery:
    """Just enough of the Protocol to satisfy isinstance() at runtime."""

    SCHEMA_REVISION = 0

    async def query_run_logs(self, *a: object, **kw: object) -> None: ...

    def tail_run_logs(self, *a: object, **kw: object) -> AsyncIterator[LogRecord]:  # type: ignore[empty-body]
        ...

    async def query_step_logs(self, *a: object, **kw: object) -> None: ...


def test_runtime_checkable_recognizes_duck_typed_impl() -> None:
    assert isinstance(_MinimalLogQuery(), LogQueryProvider)


def test_runtime_checkable_rejects_partial_impl() -> None:
    class Partial:
        async def query_run_logs(self, *a: object, **kw: object) -> None: ...

    assert not isinstance(Partial(), LogQueryProvider)

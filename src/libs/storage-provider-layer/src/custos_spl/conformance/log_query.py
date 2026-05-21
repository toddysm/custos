"""Conformance tests for LogQueryProvider adapters.

Tests that any LogQuery implementation must pass:
- Workspace scoping enforcement
- Filtering (severity, time range, message)
- Pagination (cursor-based)
- Streaming (tail_run_logs)
- Error classification
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from custos_spl.errors import BackendUnavailable, WorkspaceMismatch
from custos_spl.ids import RunId, WorkspaceId
from custos_spl.interfaces.log_query import LogFilter, LogQueryProvider

from .base import AdapterConformanceBase


class LogQueryConformanceTests(AdapterConformanceBase):
    """Base conformance tests for LogQueryProvider adapters.

    Subclasses MUST provide these pytest fixtures:
    - `adapter` → LogQueryProvider instance, configured and ready
    - `workspace_id` → WorkspaceId for testing
    - `other_workspace_id` → different WorkspaceId for cross-workspace tests
    - `run_id` → RunId with available logs for testing

    Tests will skip if required fixtures are not provided.

    Example:
        class TestMyLokiAdapter(LogQueryConformanceTests):
            @pytest.fixture
            def adapter(self):
                return MyLokiAdapter(base_url="http://loki:3100")

            @pytest.fixture
            def workspace_id(self):
                return WorkspaceId("ws-test")
    """

    @pytest.fixture
    def adapter(self) -> LogQueryProvider:
        """Adapter fixture (must be overridden by subclass)."""
        pytest.skip("adapter fixture not provided by subclass")

    @pytest.fixture
    def workspace_id(self) -> WorkspaceId:
        """Primary workspace ID fixture (must be overridden by subclass)."""
        pytest.skip("workspace_id fixture not provided by subclass")

    @pytest.fixture
    def other_workspace_id(self) -> WorkspaceId:
        """Secondary workspace ID for cross-workspace tests (must be overridden by subclass)."""
        pytest.skip("other_workspace_id fixture not provided by subclass")

    @pytest.fixture
    def run_id(self) -> RunId:
        """Run ID with logs (must be overridden by subclass)."""
        pytest.skip("run_id fixture not provided by subclass")

    @pytest.mark.asyncio
    async def test_empty_query_result_returns_empty_page(
        self,
        adapter: LogQueryProvider,
        workspace_id: WorkspaceId,
        run_id: RunId,
    ) -> None:
        """Query with no matches returns empty LogPage.

        No error raised; items tuple is empty, next_cursor is None.
        """
        # Query with filter that should match nothing
        log_filter = LogFilter(message_contains="NONEXISTENT_STRING_XYZ_ABC")
        page = await adapter.query_run_logs(workspace_id, run_id, log_filter)

        assert len(page.items) == 0
        assert page.next_cursor is None

    @pytest.mark.asyncio
    async def test_cursor_pagination_is_idempotent(
        self,
        adapter: LogQueryProvider,
        workspace_id: WorkspaceId,
        run_id: RunId,
    ) -> None:
        """Cursor pagination is idempotent.

        Passing the same cursor twice returns the same page.
        Cursor is stateless and opaque.
        """
        now = datetime.utcnow()
        log_filter = LogFilter(
            start=now - timedelta(hours=1),
            end=now,
        )

        # Get first page
        page1 = await adapter.query_run_logs(workspace_id, run_id, log_filter)

        # If there's a next_cursor, fetch same page again
        if page1.next_cursor:
            page2 = await adapter.query_run_logs(
                workspace_id, run_id, log_filter, page1.next_cursor
            )
            page3 = await adapter.query_run_logs(
                workspace_id, run_id, log_filter, page1.next_cursor
            )

            # Same cursor should yield same results
            assert len(page2.items) == len(page3.items)
            if page2.items:
                assert page2.items[0].timestamp == page3.items[0].timestamp

    @pytest.mark.asyncio
    async def test_workspace_scoping_blocks_cross_workspace_access(
        self,
        adapter: LogQueryProvider,
        workspace_id: WorkspaceId,
        other_workspace_id: WorkspaceId,
        run_id: RunId,
    ) -> None:
        """Cross-workspace queries are blocked or return empty.

        Adapter must prevent callers from accessing logs from workspace B
        when querying as workspace A.
        """
        log_filter = LogFilter()

        # Query from workspace A should work (may be empty if no logs)
        page_a = await adapter.query_run_logs(workspace_id, run_id, log_filter)
        assert isinstance(page_a.items, tuple)

        # Query from workspace B for same run should return empty or raise
        try:
            page_b = await adapter.query_run_logs(
                other_workspace_id, run_id, log_filter
            )
            # If it returns, should be empty
            assert len(page_b.items) == 0
        except WorkspaceMismatch:
            # Also acceptable to raise WorkspaceMismatch
            pass

    @pytest.mark.asyncio
    async def test_tail_run_logs_returns_async_generator(
        self,
        adapter: LogQueryProvider,
        workspace_id: WorkspaceId,
        run_id: RunId,
    ) -> None:
        """tail_run_logs() returns async generator (not coroutine).

        Caller iterates: async for record in tail_logs().
        """
        result = adapter.tail_run_logs(workspace_id, run_id)

        # Should be an async iterator, not a coroutine
        assert hasattr(result, "__aiter__"), "tail_run_logs must return async generator"
        assert hasattr(result, "__anext__"), "tail_run_logs must return async generator"

    @pytest.mark.asyncio
    async def test_time_range_filtering_respects_bounds(
        self,
        adapter: LogQueryProvider,
        workspace_id: WorkspaceId,
        run_id: RunId,
    ) -> None:
        """Time range filter respects start (inclusive) and end (exclusive).

        start <= timestamp < end for all returned logs.
        """
        now = datetime.utcnow()
        start = now - timedelta(hours=1)
        end = now - timedelta(minutes=30)

        log_filter = LogFilter(start=start, end=end)
        page = await adapter.query_run_logs(workspace_id, run_id, log_filter)

        # All returned logs should be within bounds
        for record in page.items:
            assert start <= record.timestamp < end, (
                f"Log timestamp {record.timestamp} outside bounds "
                f"[{start}, {end})"
            )

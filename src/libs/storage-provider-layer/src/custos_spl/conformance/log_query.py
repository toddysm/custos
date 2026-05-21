"""Conformance tests for LogQueryProvider adapters.

Tests that any LogQuery implementation must pass:
- Workspace scoping enforcement
- Filtering (severity, time range, message)
- Pagination (cursor-based)
- Streaming (tail_run_logs)
- Error classification
"""

from __future__ import annotations

import pytest

from custos_spl.errors import BackendUnavailable

from .base import AdapterConformanceBase


class LogQueryConformanceTests(AdapterConformanceBase):
    """Base conformance tests for LogQueryProvider adapters.

    Subclasses MUST provide an 'adapter' fixture that returns a configured
    LogQueryProvider implementation ready for testing.

    Example:
        @pytest.fixture
        def adapter(self) -> LogQueryProvider:
            return MyLokiAdapter(base_url="http://loki:3100")
    """

    def test_workspace_scoping_query_run_logs(self) -> None:
        """query_run_logs() filters to workspace.

        Cross-workspace queries return empty page or WorkspaceMismatch.

        Subclasses MUST implement:
        1. Query logs from workspace A
        2. Query same run from workspace B
        3. Assert returns empty or raises WorkspaceMismatch
        """
        pytest.skip("Adapter must implement workspace scoping for query_run_logs test")

    def test_workspace_scoping_query_step_logs(self) -> None:
        """query_step_logs() filters to workspace.

        Cross-workspace queries return empty page or WorkspaceMismatch.

        Subclasses MUST implement:
        1. Query step logs from workspace A
        2. Query same step from workspace B
        3. Assert returns empty or raises WorkspaceMismatch
        """
        pytest.skip("Adapter must implement workspace scoping for query_step_logs test")

    def test_workspace_scoping_tail_run_logs(self) -> None:
        """tail_run_logs() streams only workspace logs.

        Cross-workspace access raises WorkspaceMismatch.

        Subclasses MUST implement:
        1. Call tail_run_logs() on run in workspace B
        2. Assert raises WorkspaceMismatch
        """
        pytest.skip("Adapter must implement workspace scoping for tail_run_logs test")

    def test_cursor_pagination_idempotency(self) -> None:
        """Passing same cursor twice yields same page.

        Cursor is stateless and opaque; idempotent across calls.

        Subclasses MUST implement:
        1. Query and get first page with cursor A
        2. Query again with same cursor A
        3. Assert both calls return identical page
        """
        pytest.skip("Adapter must implement cursor pagination idempotency test")

    def test_severity_filtering(self) -> None:
        """Severity filter returns matching log level and above.

        severity_at_least='warn' returns warn, error, fatal but not debug/info.

        Subclasses MUST implement:
        1. Store logs at various severity levels
        2. Query with severity_at_least='warn'
        3. Assert returns only warn and above
        """
        pytest.skip("Adapter must implement severity filtering test")

    def test_time_range_filtering(self) -> None:
        """Time range filter respects start/end bounds.

        start is inclusive, end is exclusive (Prometheus-style).

        Subclasses MUST implement:
        1. Store logs at specific timestamps
        2. Query with time range
        3. Assert start <= timestamp < end for all results
        """
        pytest.skip("Adapter must implement time range filtering test")

    def test_message_content_filtering(self) -> None:
        """Message filter matches substring in log message.

        Case-sensitive substring match against normalized message.

        Subclasses MUST implement:
        1. Store logs with specific messages
        2. Query with message_contains filter
        3. Assert all returned logs contain the filter string
        """
        pytest.skip("Adapter must implement message content filtering test")

    def test_tail_run_logs_returns_async_generator(self) -> None:
        """tail_run_logs() returns async generator (not coroutine).

        Caller can iterate logs as they arrive: async for record in tail_logs().

        Subclasses MUST implement:
        1. Call tail_run_logs()
        2. Assert result is async iterable
        3. Iterate and collect results
        """
        pytest.skip("Adapter must implement async generator test for tail_run_logs")

    def test_empty_query_result(self) -> None:
        """Query with no matches returns empty LogPage.

        No error raised; next_cursor is None.

        Subclasses MUST implement:
        1. Query with filter that matches no logs
        2. Assert returns LogPage with empty items
        3. Assert next_cursor is None
        """
        pytest.skip("Adapter must implement empty result handling test")

    def test_error_classification(self) -> None:
        """Backend connection errors classified as BackendUnavailable.

        Transient failures raise BackendUnavailable; caller retries.

        Subclasses MUST implement:
        1. Simulate backend unavailable
        2. Call adapter query method
        3. Assert raises BackendUnavailable (not other exception type)
        """
        pytest.skip("Adapter must implement error classification test")

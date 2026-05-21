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

    Subclasses MUST provide:
    - `adapter` fixture: LogQueryProvider instance
    - `workspace_id` fixture: test workspace ID
    - `run_id` fixture: test run ID

    Example:
        @pytest.fixture
        def adapter(self) -> LogQueryProvider:
            return MyLokiAdapter(base_url="http://loki:3100")
    """

    def test_workspace_scoping_query_run_logs(self) -> None:
        """query_run_logs() enforces workspace scoping.

        Cross-workspace queries must return empty page or raise WorkspaceMismatch.
        """
        pytest.skip(
            "Adapter must implement: test workspace scoping for query_run_logs"
        )

    def test_workspace_scoping_query_step_logs(self) -> None:
        """query_step_logs() enforces workspace scoping.

        Cross-workspace queries must return empty page or raise WorkspaceMismatch.
        """
        pytest.skip(
            "Adapter must implement: test workspace scoping for query_step_logs"
        )

    def test_workspace_scoping_tail_run_logs(self) -> None:
        """tail_run_logs() enforces workspace scoping.

        Cross-workspace access must raise WorkspaceMismatch.
        """
        pytest.skip(
            "Adapter must implement: test workspace scoping for tail_run_logs"
        )

    def test_cursor_pagination_is_idempotent(self) -> None:
        """Cursor pagination is idempotent.

        Passing the same cursor twice returns the same page.
        Cursor is stateless and opaque.
        """
        pytest.skip(
            "Adapter must implement: test cursor pagination idempotency"
        )

    def test_severity_filtering_includes_and_above(self) -> None:
        """Severity filter returns matching level and above.

        severity_at_least='warn' returns warn, error, fatal but NOT debug/info.
        """
        pytest.skip(
            "Adapter must implement: test severity filtering (warn and above)"
        )

    def test_time_range_filtering_respects_bounds(self) -> None:
        """Time range filter respects start (inclusive) and end (exclusive).

        start <= timestamp < end for all returned logs.
        """
        pytest.skip(
            "Adapter must implement: test time range filtering bounds"
        )

    def test_message_content_filtering_substring_match(self) -> None:
        """Message filter matches substring in log message.

        Case-sensitive substring match against log message.
        """
        pytest.skip(
            "Adapter must implement: test message content filtering"
        )

    def test_tail_run_logs_returns_async_generator(self) -> None:
        """tail_run_logs() returns async generator (not coroutine).

        Caller iterates: async for record in tail_logs().
        """
        pytest.skip(
            "Adapter must implement: test tail_run_logs returns async generator"
        )

    def test_empty_query_result_returns_empty_page(self) -> None:
        """Query with no matches returns empty LogPage.

        No error raised; items tuple is empty, next_cursor is None.
        """
        pytest.skip(
            "Adapter must implement: test empty result handling"
        )

    def test_error_classification_transient_failures(self) -> None:
        """Network/transient errors raise BackendUnavailable.

        Connection refused, timeout, HTTP 503 → BackendUnavailable.
        Caller retries with backoff.
        """
        pytest.skip(
            "Adapter must implement: test transient errors raise BackendUnavailable"
        )

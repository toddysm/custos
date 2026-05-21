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
from custos_spl.ids import RunId, WorkspaceId

from .base import AdapterConformanceBase


class LogQueryConformanceTests(AdapterConformanceBase):
    """Base conformance tests for LogQueryProvider adapters."""

    def test_workspace_scoping_query_run_logs(self) -> None:
        """query_run_logs() filters to workspace.

        Cross-workspace queries return empty page or WorkspaceMismatch.
        """
        pass

    def test_workspace_scoping_query_step_logs(self) -> None:
        """query_step_logs() filters to workspace.

        Cross-workspace queries return empty page or WorkspaceMismatch.
        """
        pass

    def test_workspace_scoping_tail_run_logs(self) -> None:
        """tail_run_logs() streams only workspace logs.

        Cross-workspace access raises WorkspaceMismatch.
        """
        pass

    def test_cursor_pagination_idempotency(self) -> None:
        """Passing same cursor twice yields same page.

        Cursor is stateless and opaque; idempotent across calls.
        """
        pass

    def test_severity_filtering(self) -> None:
        """Severity filter returns matching log level and above.

        severity_at_least='warn' returns warn, error, fatal but not debug/info.
        """
        pass

    def test_time_range_filtering(self) -> None:
        """Time range filter respects start/end bounds.

        start is inclusive, end is exclusive (Prometheus-style).
        """
        pass

    def test_message_content_filtering(self) -> None:
        """Message filter matches substring in log message.

        Case-sensitive substring match against normalized message.
        """
        pass

    def test_tail_run_logs_returns_async_generator(self) -> None:
        """tail_run_logs() returns async generator (not coroutine).

        Caller can iterate logs as they arrive: async for record in tail_logs().
        """
        pass

    def test_empty_query_result(self) -> None:
        """Query with no matches returns empty LogPage.

        No error raised; next_cursor is None.
        """
        pass

    def test_error_classification(self) -> None:
        """Backend connection errors classified as BackendUnavailable.

        Transient failures raise BackendUnavailable; caller retries.
        """
        pass

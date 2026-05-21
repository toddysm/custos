"""Loki LogQueryProvider adapter.

Read-only query facade over Loki log storage. Normalizes Loki's native API
and response format to the LogQueryProvider interface contract.
"""

from __future__ import annotations

import base64
import json
import os
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Literal

import httpx

from custos_spl.errors import BackendUnavailable, WorkspaceMismatch
from custos_spl.ids import RunId, StepId, WorkspaceId
from custos_spl.interfaces.log_query import (
    LogFilter,
    LogPage,
    LogRecord,
    Severity,
)
from custos_spl.pagination import Cursor

LokiSeverity = Literal[
    "TRACE",
    "DEBUG",
    "INFO",
    "WARN",
    "ERROR",
    "FATAL",
]
"""Loki severity levels (case-insensitive in Loki labels)."""

SEVERITY_MAP: dict[str, Severity] = {
    "TRACE": "trace",
    "DEBUG": "debug",
    "INFO": "info",
    "WARN": "warn",
    "ERROR": "error",
    "FATAL": "fatal",
}
"""Map Loki severity labels to OTel severity values."""


class LokiLogQueryAdapter:
    """Loki adapter for LogQueryProvider.

    Queries logs from Loki using label selectors and returns paged LogRecords.
    All queries are workspace-scoped via label filter.
    """

    def __init__(
        self,
        base_url: str,
        tenant_id: str | None = None,
        tls_verify: bool | str = True,
    ) -> None:
        """Initialize Loki adapter.

        Args:
            base_url: Base URL of Loki instance (e.g., http://loki:3100)
            tenant_id: Optional Loki tenant ID for multi-tenant deployments
            tls_verify: True to verify TLS, False to skip, or path to CA bundle

        Raises:
            ValueError: if base_url is empty or invalid
        """
        if not base_url:
            raise ValueError("base_url cannot be empty")
        self.base_url = base_url.rstrip("/")
        self.tenant_id = tenant_id
        self.tls_verify = tls_verify

    def _escape_label_value(self, value: str) -> str:
        """Escape a label value for safe inclusion in LogQL selectors.

        Escapes backslashes and double-quotes per LogQL string rules.
        """
        return value.replace("\\", "\\\\").replace('"', '\\"')

    def _build_selector(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId | None = None,
        step_id: StepId | None = None,
    ) -> str:
        """Build Loki label selector for workspace + optional run/step.

        Returns a Loki label matcher string, e.g.:
        {workspace_id="ws-123", run_id="r-456"}
        """
        escaped_workspace_id = self._escape_label_value(str(workspace_id))
        labels = [f'workspace_id="{escaped_workspace_id}"']
        if run_id:
            escaped_run_id = self._escape_label_value(str(run_id))
            labels.append(f'run_id="{escaped_run_id}"')
        if step_id:
            escaped_step_id = self._escape_label_value(str(step_id))
            labels.append(f'step_id="{escaped_step_id}"')
        return "{" + ", ".join(labels) + "}"

    def _normalize_severity(self, severity_str: str) -> Severity:
        """Normalize a Loki severity string to OTel severity value.

        Handles case-insensitive matching and defaults to 'info'.
        """
        normalized = severity_str.upper()
        return SEVERITY_MAP.get(normalized, "info")  # type: ignore

    def _encode_cursor(self, token: str) -> Cursor:
        """Encode opaque Loki pagination token as base64 cursor."""
        encoded = base64.b64encode(token.encode()).decode()
        return Cursor(token=encoded)

    def _decode_cursor(self, cursor: Cursor) -> str:
        """Decode base64 cursor back to Loki pagination token."""
        return base64.b64decode(cursor.token.encode()).decode()

    async def query_run_logs(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        filter: LogFilter,
        cursor: Cursor | None = None,
    ) -> LogPage:
        """Paged historical query for one run's logs."""
        try:
            selector = self._build_selector(workspace_id, run_id=run_id)
            query = selector

            # Add severity filter if specified
            if filter.severity_at_least:
                severity_levels: list[Severity] = [
                    "trace",
                    "debug",
                    "info",
                    "warn",
                    "error",
                    "fatal",
                ]
                idx = severity_levels.index(filter.severity_at_least)
                matching = severity_levels[idx:]
                severity_regex = "|".join(matching)
                query += f' | severity=~"{severity_regex}"'

            # Add message filter if specified
            if filter.message_contains:
                query += f' | "{filter.message_contains}"'

            params: dict = {"query": query}

            # Add time range
            if filter.start:
                params["start"] = int(filter.start.timestamp() * 1e9)
            if filter.end:
                params["end"] = int(filter.end.timestamp() * 1e9)

            # Add pagination token if provided
            if cursor:
                params["_shards"] = self._decode_cursor(cursor)

            async with httpx.AsyncClient(verify=self.tls_verify) as client:
                headers = {}
                if self.tenant_id:
                    headers["X-Scope-OrgID"] = self.tenant_id

                response = await client.get(
                    f"{self.base_url}/loki/api/v1/query_range",
                    params=params,
                    headers=headers,
                )
                response.raise_for_status()

            data = response.json()
            records = self._parse_log_records(workspace_id, run_id, None, data)

            # Extract pagination token if present
            next_cursor = None
            if "_shards" in response.headers:
                next_cursor = self._encode_cursor(response.headers["_shards"])

            return LogPage(items=tuple(records), next_cursor=next_cursor)
        except httpx.HTTPError as exc:
            raise BackendUnavailable(f"Loki query failed: {exc}") from exc
        except Exception as exc:
            raise BackendUnavailable(f"Loki query failed: {exc}") from exc

    def tail_run_logs(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        from_cursor: Cursor | None = None,
    ) -> AsyncIterator[LogRecord]:
        """Live tail of a run's logs (async generator)."""
        return self._tail_impl(workspace_id, run_id, from_cursor)

    async def _tail_impl(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        from_cursor: Cursor | None = None,
    ) -> AsyncIterator[LogRecord]:
        """Implementation of tail as async generator."""
        try:
            selector = self._build_selector(workspace_id, run_id=run_id)
            params: dict = {"query": selector}

            if from_cursor:
                params["start"] = self._decode_cursor(from_cursor)

            async with httpx.AsyncClient(verify=self.tls_verify) as client:
                headers = {}
                if self.tenant_id:
                    headers["X-Scope-OrgID"] = self.tenant_id

                async with client.stream(
                    "GET",
                    f"{self.base_url}/loki/api/v1/tail",
                    params=params,
                    headers=headers,
                ) as response:
                    response.raise_for_status()

                    async for line in response.aiter_lines():
                        if not line or line.startswith("{"):
                            continue
                        try:
                            data = json.loads(line)
                            records = self._parse_log_records(
                                workspace_id, run_id, None, data
                            )
                            for record in records:
                                yield record
                        except (json.JSONDecodeError, ValueError):
                            continue
        except httpx.HTTPError as exc:
            raise BackendUnavailable(f"Loki tail failed: {exc}") from exc
        except Exception as exc:
            raise BackendUnavailable(f"Loki tail failed: {exc}") from exc

    async def query_step_logs(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        step_id: StepId,
        filter: LogFilter,
        cursor: Cursor | None = None,
    ) -> LogPage:
        """Paged historical query scoped to a single step within a run."""
        try:
            selector = self._build_selector(
                workspace_id, run_id=run_id, step_id=step_id
            )
            query = selector

            # Add severity filter if specified
            if filter.severity_at_least:
                severity_levels: list[Severity] = [
                    "trace",
                    "debug",
                    "info",
                    "warn",
                    "error",
                    "fatal",
                ]
                idx = severity_levels.index(filter.severity_at_least)
                matching = severity_levels[idx:]
                severity_regex = "|".join(matching)
                query += f' | severity=~"{severity_regex}"'

            # Add message filter if specified
            if filter.message_contains:
                query += f' | "{filter.message_contains}"'

            params: dict = {"query": query}

            # Add time range
            if filter.start:
                params["start"] = int(filter.start.timestamp() * 1e9)
            if filter.end:
                params["end"] = int(filter.end.timestamp() * 1e9)

            # Add pagination token if provided
            if cursor:
                params["_shards"] = self._decode_cursor(cursor)

            async with httpx.AsyncClient(verify=self.tls_verify) as client:
                headers = {}
                if self.tenant_id:
                    headers["X-Scope-OrgID"] = self.tenant_id

                response = await client.get(
                    f"{self.base_url}/loki/api/v1/query_range",
                    params=params,
                    headers=headers,
                )
                response.raise_for_status()

            data = response.json()
            records = self._parse_log_records(workspace_id, run_id, step_id, data)

            # Extract pagination token if present
            next_cursor = None
            if "_shards" in response.headers:
                next_cursor = self._encode_cursor(response.headers["_shards"])

            return LogPage(items=tuple(records), next_cursor=next_cursor)
        except httpx.HTTPError as exc:
            raise BackendUnavailable(f"Loki query failed: {exc}") from exc
        except Exception as exc:
            raise BackendUnavailable(f"Loki query failed: {exc}") from exc

    def _parse_log_records(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        step_id: StepId | None,
        data: dict,
    ) -> list[LogRecord]:
        """Parse Loki response into LogRecord list.

        Validates workspace ownership before returning records.
        """
        records: list[LogRecord] = []

        # Loki response format: {"result": [{"stream": {...}, "values": [...]}]}
        result = data.get("result", [])
        for stream in result:
            stream_labels = stream.get("stream", {})

            # Verify workspace match
            if stream_labels.get("workspace_id") != str(workspace_id):
                raise WorkspaceMismatch(
                    f"stream workspace_id mismatch: {stream_labels.get('workspace_id')} != {workspace_id}"
                )

            # Parse log entries
            for timestamp_ns_str, message in stream.get("values", []):
                try:
                    timestamp_ns = int(timestamp_ns_str)
                    timestamp = datetime.utcfromtimestamp(timestamp_ns / 1e9)
                    severity_str = stream_labels.get("severity", "info")
                    severity = self._normalize_severity(severity_str)

                    record = LogRecord(
                        timestamp=timestamp,
                        severity=severity,
                        message=message,
                        run_id=run_id,
                        step_id=step_id or None,
                        attributes=dict(stream_labels),
                    )
                    records.append(record)
                except (ValueError, KeyError, TypeError) as exc:
                    raise BackendUnavailable(
                        f"Failed to parse log record: {exc}"
                    ) from exc

        return records

    SCHEMA_REVISION = 0


def make_adapter() -> LokiLogQueryAdapter:
    """Factory: create adapter from environment variables.

    Required:
        CUSTOS_LOKI_URL: Loki base URL

    Optional:
        CUSTOS_LOKI_TENANT_ID: Loki tenant ID (for multi-tenant)
        CUSTOS_LOKI_TLS_VERIFY: TLS verification (true, false, or CA path)
    """
    url = os.getenv("CUSTOS_LOKI_URL")
    if not url:
        raise RuntimeError(
            "CUSTOS_LOKI_URL environment variable not set; required for Loki adapter"
        )

    tenant_id = os.getenv("CUSTOS_LOKI_TENANT_ID")

    tls_verify: bool | str = True
    tls_str = os.getenv("CUSTOS_LOKI_TLS_VERIFY", "true").lower()
    if tls_str == "false":
        tls_verify = False
    elif tls_str != "true":
        tls_verify = tls_str

    return LokiLogQueryAdapter(url, tenant_id=tenant_id, tls_verify=tls_verify)

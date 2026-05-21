"""Unit tests for custos-loki adapter — using httpx mocking."""

import base64
import os
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custos_loki.adapters import (
    LokiLogQueryAdapter,
    make_adapter,
)
from custos_spl.errors import BackendUnavailable, WorkspaceMismatch
from custos_spl.ids import RunId, StepId, WorkspaceId
from custos_spl.interfaces.log_query import LogFilter


def test_adapter_requires_base_url() -> None:
    """Adapter must be initialized with non-empty base_url."""
    with pytest.raises(ValueError, match="base_url cannot be empty"):
        LokiLogQueryAdapter(base_url="")


def test_adapter_strips_trailing_slash() -> None:
    """Adapter strips trailing slash from base_url."""
    adapter = LokiLogQueryAdapter(base_url="http://loki:3100/")
    assert adapter.base_url == "http://loki:3100"


def test_severity_normalization() -> None:
    """Test severity string normalization to OTel values."""
    adapter = LokiLogQueryAdapter(base_url="http://loki:3100")

    assert adapter._normalize_severity("DEBUG") == "debug"
    assert adapter._normalize_severity("info") == "info"
    assert adapter._normalize_severity("WARN") == "warn"
    assert adapter._normalize_severity("ERROR") == "error"
    assert adapter._normalize_severity("FATAL") == "fatal"
    assert adapter._normalize_severity("unknown") == "info"


def test_selector_builder_run() -> None:
    """Test label selector construction for run-scoped query."""
    adapter = LokiLogQueryAdapter(base_url="http://loki:3100")
    workspace_id = WorkspaceId("ws-123")
    run_id = RunId("r-456")

    selector = adapter._build_selector(workspace_id, run_id=run_id)
    assert 'workspace_id="ws-123"' in selector
    assert 'run_id="r-456"' in selector
    assert "step_id" not in selector


def test_selector_builder_step() -> None:
    """Test label selector construction for step-scoped query."""
    adapter = LokiLogQueryAdapter(base_url="http://loki:3100")
    workspace_id = WorkspaceId("ws-123")
    run_id = RunId("r-456")
    step_id = StepId("s-789")

    selector = adapter._build_selector(
        workspace_id, run_id=run_id, step_id=step_id
    )
    assert 'workspace_id="ws-123"' in selector
    assert 'run_id="r-456"' in selector
    assert 'step_id="s-789"' in selector


def test_cursor_encoding_decoding() -> None:
    """Test cursor encoding and decoding round-trip."""
    adapter = LokiLogQueryAdapter(base_url="http://loki:3100")
    token = "some-loki-pagination-token"

    cursor = adapter._encode_cursor(token)
    expected_token = base64.b64encode(token.encode()).decode()
    assert cursor.token == expected_token

    decoded = adapter._decode_cursor(cursor)
    assert decoded == token


@pytest.mark.asyncio
async def test_query_run_logs_success() -> None:
    """Test successful query_run_logs with mock Loki response."""
    adapter = LokiLogQueryAdapter(base_url="http://loki:3100")
    workspace_id = WorkspaceId("ws-123")
    run_id = RunId("r-456")

    now = datetime.utcnow()
    timestamp_ns = int(now.timestamp() * 1e9)

    loki_response = {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {
                    "stream": {
                        "workspace_id": "ws-123",
                        "run_id": "r-456",
                        "severity": "info",
                    },
                    "values": [
                        [str(timestamp_ns), "log message 1"],
                        [str(timestamp_ns + 1000000000), "log message 2"],
                    ],
                }
            ],
        },
    }

    with patch("httpx.AsyncClient") as mock_client_class:
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = loki_response
        mock_response.headers = {}
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client.get.return_value = mock_response
        mock_client_class.return_value = mock_client

        page = await adapter.query_run_logs(
            workspace_id, run_id, LogFilter()
        )

    assert len(page.items) == 2
    assert page.items[0].message == "log message 1"
    assert page.items[0].severity == "info"
    assert page.items[0].run_id == run_id
    assert page.next_cursor is None


@pytest.mark.asyncio
async def test_query_step_logs() -> None:
    """Test query_step_logs with step_id in selector."""
    adapter = LokiLogQueryAdapter(base_url="http://loki:3100")
    workspace_id = WorkspaceId("ws-123")
    run_id = RunId("r-456")
    step_id = StepId("s-789")

    loki_response = {
        "status": "success",
        "data": {"resultType": "matrix", "result": []},
    }

    with patch("httpx.AsyncClient") as mock_client_class:
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = loki_response
        mock_response.headers = {}
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client.get.return_value = mock_response
        mock_client_class.return_value = mock_client

        await adapter.query_step_logs(
            workspace_id, run_id, step_id, LogFilter()
        )

        call_kwargs = mock_client.get.call_args[1]
        assert 'step_id="s-789"' in call_kwargs["params"]["query"]


@pytest.mark.asyncio
async def test_tail_run_logs() -> None:
    """Test tail_run_logs returns async generator."""
    adapter = LokiLogQueryAdapter(base_url="http://loki:3100")
    workspace_id = WorkspaceId("ws-123")
    run_id = RunId("r-456")

    tail_iter = adapter.tail_run_logs(workspace_id, run_id)
    assert hasattr(tail_iter, "__aiter__")
    assert hasattr(tail_iter, "__anext__")


@pytest.mark.asyncio
async def test_workspace_mismatch_rejected() -> None:
    """Test that records from different workspace are rejected."""
    adapter = LokiLogQueryAdapter(base_url="http://loki:3100")
    workspace_id = WorkspaceId("ws-123")
    run_id = RunId("r-456")

    loki_response = {
        "status": "success",
        "data": {
            "resultType": "matrix",
            "result": [
                {
                    "stream": {
                        "workspace_id": "ws-999",
                        "run_id": "r-456",
                    },
                    "values": [[str(int(datetime.utcnow().timestamp() * 1e9)), "msg"]],
                }
            ],
        },
    }

    with patch("httpx.AsyncClient") as mock_client_class:
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.json.return_value = loki_response
        mock_response.headers = {}
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client.get.return_value = mock_response
        mock_client_class.return_value = mock_client

        with pytest.raises(WorkspaceMismatch):
            await adapter.query_run_logs(
                workspace_id, run_id, LogFilter()
            )


@pytest.mark.asyncio
async def test_network_error_handling() -> None:
    """Test that network errors convert to BackendUnavailable."""
    adapter = LokiLogQueryAdapter(base_url="http://loki:3100")
    workspace_id = WorkspaceId("ws-123")
    run_id = RunId("r-456")

    with patch("httpx.AsyncClient") as mock_client_class:
        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client.get.side_effect = Exception("Connection timeout")
        mock_client_class.return_value = mock_client

        with pytest.raises(BackendUnavailable, match="Connection timeout"):
            await adapter.query_run_logs(
                workspace_id, run_id, LogFilter()
            )


def test_factory_requires_env_var() -> None:
    """Factory raises error if CUSTOS_LOKI_URL not set."""
    old_value = os.environ.pop("CUSTOS_LOKI_URL", None)
    try:
        with pytest.raises(RuntimeError, match="CUSTOS_LOKI_URL"):
            make_adapter()
    finally:
        if old_value:
            os.environ["CUSTOS_LOKI_URL"] = old_value


def test_factory_returns_adapter() -> None:
    """Factory creates adapter from environment."""
    old_url = os.environ.get("CUSTOS_LOKI_URL")
    old_tenant = os.environ.get("CUSTOS_LOKI_TENANT_ID")

    try:
        os.environ["CUSTOS_LOKI_URL"] = "http://loki:3100"
        os.environ["CUSTOS_LOKI_TENANT_ID"] = "my-tenant"

        adapter = make_adapter()
        assert isinstance(adapter, LokiLogQueryAdapter)
        assert adapter.base_url == "http://loki:3100"
        assert adapter.tenant_id == "my-tenant"
    finally:
        if old_url:
            os.environ["CUSTOS_LOKI_URL"] = old_url
        else:
            os.environ.pop("CUSTOS_LOKI_URL", None)
        if old_tenant:
            os.environ["CUSTOS_LOKI_TENANT_ID"] = old_tenant
        else:
            os.environ.pop("CUSTOS_LOKI_TENANT_ID", None)


def test_factory_tls_verify_false() -> None:
    """Factory correctly parses CUSTOS_LOKI_TLS_VERIFY=false."""
    old_url = os.environ.get("CUSTOS_LOKI_URL")
    old_verify = os.environ.get("CUSTOS_LOKI_TLS_VERIFY")

    try:
        os.environ["CUSTOS_LOKI_URL"] = "https://loki:3100"
        os.environ["CUSTOS_LOKI_TLS_VERIFY"] = "false"

        adapter = make_adapter()
        assert adapter.tls_verify is False
    finally:
        if old_url:
            os.environ["CUSTOS_LOKI_URL"] = old_url
        else:
            os.environ.pop("CUSTOS_LOKI_URL", None)
        if old_verify:
            os.environ["CUSTOS_LOKI_TLS_VERIFY"] = old_verify
        else:
            os.environ.pop("CUSTOS_LOKI_TLS_VERIFY", None)

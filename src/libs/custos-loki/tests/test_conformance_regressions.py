"""Pin the escaped LogQL substring filter required by live conformance queries."""

import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from custos_spl.ids import RunId, StepId, WorkspaceId
from custos_spl.interfaces.log_query import LogFilter

from custos_loki.adapters import LokiLogQueryAdapter


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ["run", "step"])
async def test_message_filter_uses_literal_substring_operator(scope: str) -> None:
    """Both query surfaces must generate valid LogQL without interpreting message syntax."""
    message = 'quoted "value" with \\ slash'
    response = httpx.Response(
        200,
        json={"status": "success", "data": {"result": []}},
        request=httpx.Request("GET", "http://loki.test/loki/api/v1/query_range"),
    )
    with patch("custos_loki.adapters.httpx.AsyncClient") as factory:
        client = factory.return_value.__aenter__.return_value
        client.get = AsyncMock(return_value=response)
        adapter = LokiLogQueryAdapter("http://loki.test")
        query_filter = LogFilter(message_contains=message)
        if scope == "run":
            await adapter.query_run_logs(WorkspaceId("workspace"), RunId("run"), query_filter)
        else:
            await adapter.query_step_logs(
                WorkspaceId("workspace"), RunId("run"), StepId("step"), query_filter
            )
        query = client.get.call_args.kwargs["params"]["query"]
        assert query.endswith(f" |= {json.dumps(message)}")

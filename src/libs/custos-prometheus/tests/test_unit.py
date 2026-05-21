"""Unit tests for Prometheus and noop adapters."""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custos_prometheus.adapters import (
    NoopMetricsAdapter,
    PrometheusMetricsAdapter,
    make_noop_adapter,
    make_prometheus_adapter,
)
from custos_spl.errors import BackendUnavailable, QueryUnsupported
from custos_spl.ids import RunId, WorkspaceId
from custos_spl.interfaces.metrics_query import (
    MetricRange,
    MetricSample,
    MetricSelector,
    MetricSeries,
)


class TestPrometheusAdapter:
    """Tests for PrometheusMetricsAdapter."""

    @pytest.fixture
    def adapter(self) -> PrometheusMetricsAdapter:
        return PrometheusMetricsAdapter(base_url="http://prometheus:9090")

    def test_init_validates_base_url(self) -> None:
        """Base URL cannot be empty."""
        with pytest.raises(ValueError, match="base_url cannot be empty"):
            PrometheusMetricsAdapter(base_url="")

    def test_escape_label_value(self, adapter: PrometheusMetricsAdapter) -> None:
        """Label values are escaped for PromQL injection safety."""
        assert adapter._escape_label_value("normal-id") == "normal-id"
        assert adapter._escape_label_value('id"quotes') == 'id\\"quotes'
        assert adapter._escape_label_value("id\\backslash") == "id\\\\backslash"
        assert adapter._escape_label_value('both"and\\') == 'both\\"and\\\\'

    def test_build_matchers(self, adapter: PrometheusMetricsAdapter) -> None:
        """Build PromQL matchers with workspace + optional run."""
        workspace_id = WorkspaceId("ws-123")
        run_id = RunId("run-456")

        # Workspace only
        matchers = adapter._build_matchers(workspace_id)
        assert matchers == 'workspace_id="ws-123"'

        # Workspace + run
        matchers = adapter._build_matchers(workspace_id, run_id)
        assert matchers == 'workspace_id="ws-123",run_id="run-456"'

    def test_build_matchers_with_special_chars(
        self, adapter: PrometheusMetricsAdapter
    ) -> None:
        """Matchers escape special characters to prevent injection."""
        workspace_id = WorkspaceId('ws"inject')
        run_id = RunId("run\\escape")

        matchers = adapter._build_matchers(workspace_id, run_id)
        assert matchers == 'workspace_id="ws\\"inject",run_id="run\\\\escape"'

    def test_build_query(self, adapter: PrometheusMetricsAdapter) -> None:
        """Build PromQL query from selector + matchers."""
        workspace_id = WorkspaceId("ws-123")
        run_id = RunId("run-456")
        selector = MetricSelector(
            name="cpu_usage",
            label_matchers={"pod": "my-pod", "namespace": "default"},
        )

        query = adapter._build_query(selector, workspace_id, run_id)
        assert "cpu_usage" in query
        assert 'workspace_id="ws-123"' in query
        assert 'run_id="run-456"' in query
        assert 'pod="my-pod"' in query
        assert 'namespace="default"' in query

    @pytest.mark.asyncio
    async def test_query_run_metrics_success(
        self, adapter: PrometheusMetricsAdapter
    ) -> None:
        """Query run metrics returns MetricSeries."""
        workspace_id = WorkspaceId("ws-123")
        run_id = RunId("run-456")
        selector = MetricSelector(name="cpu_usage")
        range_query = MetricRange(
            start=datetime(2026, 5, 20, 0, 0),
            end=datetime(2026, 5, 20, 1, 0),
            step_seconds=60,
        )

        response_data = {
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [
                    {
                        "metric": {"__name__": "cpu_usage", "workspace_id": "ws-123"},
                        "values": [
                            ["1147483647", "1"],
                            ["1147483707", "2"],
                        ],
                    }
                ],
            },
        }

        with patch("custos_prometheus.adapters.httpx.AsyncClient") as mock_client:
            mock_response = MagicMock()
            mock_response.json.return_value = response_data
            mock_response.raise_for_status = MagicMock()
            mock_client.return_value.__aenter__.return_value.get.return_value = (
                mock_response
            )

            result = await adapter.query_run_metrics(
                workspace_id, run_id, selector, range_query
            )

            assert result.name == "cpu_usage"
            assert len(result.samples) == 2
            assert result.samples[0].value == 1.0
            assert result.samples[1].value == 2.0

    @pytest.mark.asyncio
    async def test_query_run_metrics_error_handling(
        self, adapter: PrometheusMetricsAdapter
    ) -> None:
        """Query run metrics handles network errors."""
        workspace_id = WorkspaceId("ws-123")
        run_id = RunId("run-456")
        selector = MetricSelector(name="cpu_usage")
        range_query = MetricRange(
            start=datetime(2026, 5, 20, 0, 0),
            end=datetime(2026, 5, 20, 1, 0),
            step_seconds=60,
        )

        with patch("custos_prometheus.adapters.httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__.return_value.get.side_effect = (
                ConnectionError("Connection refused")
            )

            with pytest.raises(BackendUnavailable):
                await adapter.query_run_metrics(
                    workspace_id, run_id, selector, range_query
                )

    @pytest.mark.asyncio
    async def test_query_instant_metric_success(
        self, adapter: PrometheusMetricsAdapter
    ) -> None:
        """Query instant metric returns MetricSample."""
        workspace_id = WorkspaceId("ws-123")
        selector = MetricSelector(name="up")
        at = datetime.utcnow()

        response_data = {
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {
                        "metric": {"__name__": "up", "job": "prometheus"},
                        "value": ["1147483647", "1"],
                    }
                ],
            },
        }

        with patch("custos_prometheus.adapters.httpx.AsyncClient") as mock_client:
            mock_response = MagicMock()
            mock_response.json.return_value = response_data
            mock_response.raise_for_status = MagicMock()
            mock_client.return_value.__aenter__.return_value.get.return_value = (
                mock_response
            )

            result = await adapter.query_instant_metric(workspace_id, selector, at)

            assert isinstance(result, MetricSample)
            assert result.value == 1.0

    @pytest.mark.asyncio
    async def test_query_workspace_metrics_success(
        self, adapter: PrometheusMetricsAdapter
    ) -> None:
        """Query workspace metrics (no run filter) returns MetricSeries."""
        workspace_id = WorkspaceId("ws-123")
        selector = MetricSelector(name="requests_total")
        range_query = MetricRange(
            start=datetime(2026, 5, 20, 0, 0),
            end=datetime(2026, 5, 20, 1, 0),
            step_seconds=60,
        )

        response_data = {
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [
                    {
                        "metric": {"__name__": "requests_total"},
                        "values": [["1147483647", "100"]],
                    }
                ],
            },
        }

        with patch("custos_prometheus.adapters.httpx.AsyncClient") as mock_client:
            mock_response = MagicMock()
            mock_response.json.return_value = response_data
            mock_response.raise_for_status = MagicMock()
            mock_client.return_value.__aenter__.return_value.get.return_value = (
                mock_response
            )

            result = await adapter.query_workspace_metrics(
                workspace_id, selector, range_query
            )

            assert result.name == "requests_total"
            assert len(result.samples) == 1


class TestNoopAdapter:
    """Tests for NoopMetricsAdapter."""

    @pytest.fixture
    def adapter(self) -> NoopMetricsAdapter:
        return NoopMetricsAdapter()

    @pytest.mark.asyncio
    async def test_query_run_metrics_raises_unsupported(
        self, adapter: NoopMetricsAdapter
    ) -> None:
        """Query run metrics raises QueryUnsupported."""
        workspace_id = WorkspaceId("ws-123")
        run_id = RunId("run-456")
        selector = MetricSelector(name="cpu_usage")
        range_query = MetricRange(
            start=datetime.utcnow() - timedelta(hours=1),
            end=datetime.utcnow(),
            step_seconds=60,
        )

        with pytest.raises(QueryUnsupported):
            await adapter.query_run_metrics(
                workspace_id, run_id, selector, range_query
            )

    @pytest.mark.asyncio
    async def test_query_workspace_metrics_raises_unsupported(
        self, adapter: NoopMetricsAdapter
    ) -> None:
        """Query workspace metrics raises QueryUnsupported."""
        workspace_id = WorkspaceId("ws-123")
        selector = MetricSelector(name="cpu_usage")
        range_query = MetricRange(
            start=datetime.utcnow() - timedelta(hours=1),
            end=datetime.utcnow(),
            step_seconds=60,
        )

        with pytest.raises(QueryUnsupported):
            await adapter.query_workspace_metrics(workspace_id, selector, range_query)

    @pytest.mark.asyncio
    async def test_query_instant_metric_raises_unsupported(
        self, adapter: NoopMetricsAdapter
    ) -> None:
        """Query instant metric raises QueryUnsupported."""
        workspace_id = WorkspaceId("ws-123")
        selector = MetricSelector(name="up")

        with pytest.raises(QueryUnsupported):
            await adapter.query_instant_metric(workspace_id, selector, datetime.utcnow())


class TestFactoryFunctions:
    """Tests for adapter factory functions."""

    def test_make_prometheus_adapter_requires_url(self) -> None:
        """Factory requires CUSTOS_PROMETHEUS_URL."""
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(RuntimeError, match="CUSTOS_PROMETHEUS_URL"):
                make_prometheus_adapter()

    def test_make_prometheus_adapter_with_env_vars(self) -> None:
        """Factory creates adapter from environment variables."""
        env_vars = {
            "CUSTOS_PROMETHEUS_URL": "http://prometheus:9090",
            "CUSTOS_PROMETHEUS_TENANT_ID": "tenant-123",
            "CUSTOS_PROMETHEUS_TLS_VERIFY": "false",
        }
        with patch.dict("os.environ", env_vars, clear=True):
            adapter = make_prometheus_adapter()

            assert adapter.base_url == "http://prometheus:9090"
            assert adapter.tenant_id == "tenant-123"
            assert adapter.tls_verify is False

    def test_make_noop_adapter(self) -> None:
        """Factory creates noop adapter."""
        adapter = make_noop_adapter()
        assert isinstance(adapter, NoopMetricsAdapter)

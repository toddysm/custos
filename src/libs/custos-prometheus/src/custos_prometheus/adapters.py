"""Prometheus and noop MetricsQueryProvider adapters."""

from __future__ import annotations

import os
import re
from datetime import datetime
from typing import ClassVar

import httpx

from custos_spl.errors import BackendUnavailable, QueryUnsupported, WorkspaceMismatch
from custos_spl.ids import RunId, WorkspaceId
from custos_spl.interfaces.metrics_query import (
    MetricRange,
    MetricSample,
    MetricSelector,
    MetricSeries,
)


class PrometheusMetricsAdapter:
    """Prometheus MetricsQueryProvider implementation.

    Supports Prometheus, Mimir, Thanos, Cortex, VictoriaMetrics, and
    any PromQL-speaking backend.
    """

    SCHEMA_REVISION: ClassVar[int] = 0

    def __init__(
        self,
        base_url: str,
        tenant_id: str | None = None,
        tls_verify: bool | str = True,
    ) -> None:
        """Initialize with Prometheus configuration.

        Args:
            base_url: Base URL of Prometheus instance.
            tenant_id: Tenant ID for multi-tenant deployments (optional).
            tls_verify: TLS verification (bool or path to CA bundle).

        Raises:
            ValueError: if base_url is empty or invalid.
        """
        if not base_url:
            raise ValueError("base_url cannot be empty")
        self.base_url = base_url.rstrip("/")
        self.tenant_id = tenant_id
        self.tls_verify = tls_verify

    def _escape_label_value(self, value: str) -> str:
        """Escape a label value for safe inclusion in PromQL matchers."""
        return value.replace("\\", "\\\\").replace('"', '\\"')

    def _validate_metric_name(self, name: str) -> None:
        """Validate metric name against Prometheus regex.

        Raises QueryUnsupported if name is invalid.
        Prometheus metric names must match: [a-zA-Z_:][a-zA-Z0-9_:]*
        """
        if not re.match(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$", name):
            raise QueryUnsupported(f"invalid metric name: {name}")

    def _validate_label_name(self, name: str) -> None:
        """Validate label name against Prometheus regex.

        Raises QueryUnsupported if name is invalid.
        Prometheus label names must match: [a-zA-Z_][a-zA-Z0-9_]*
        """
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
            raise QueryUnsupported(f"invalid label name: {name}")

    def _build_matchers(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId | None = None,
    ) -> str:
        """Build PromQL label matchers for workspace + optional run.

        Returns a PromQL matcher string, e.g.: workspace_id="ws-123",run_id="r-456"
        """
        escaped_workspace_id = self._escape_label_value(str(workspace_id))
        matchers = [f'workspace_id="{escaped_workspace_id}"']

        if run_id:
            escaped_run_id = self._escape_label_value(str(run_id))
            matchers.append(f'run_id="{escaped_run_id}"')

        return ",".join(matchers)

    def _build_query(
        self,
        selector: MetricSelector,
        workspace_id: WorkspaceId,
        run_id: RunId | None = None,
    ) -> str:
        """Build PromQL query from selector + workspace/run filters."""
        self._validate_metric_name(selector.name)
        metric_name = selector.name
        workspace_matchers = self._build_matchers(workspace_id, run_id)

        # Build label matchers from selector
        selector_matchers = []
        for key, value in selector.label_matchers.items():
            self._validate_label_name(key)
            escaped_value = self._escape_label_value(value)
            selector_matchers.append(f'{key}="{escaped_value}"')

        all_matchers = [workspace_matchers] + selector_matchers
        return f'{metric_name}{{{",".join(all_matchers)}}}'

    async def query_run_metrics(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        selector: MetricSelector,
        range: MetricRange,
    ) -> MetricSeries:
        """Time-bucketed range query scoped to one run."""
        try:
            query = self._build_query(selector, workspace_id, run_id)

            params: dict = {
                "query": query,
                "start": int(range.start.timestamp()),
                "end": int(range.end.timestamp()),
                "step": range.step_seconds,
            }

            async with httpx.AsyncClient(verify=self.tls_verify) as client:
                headers = {}
                if self.tenant_id:
                    headers["X-Scope-OrgID"] = self.tenant_id

                response = await client.get(
                    f"{self.base_url}/api/v1/query_range",
                    params=params,
                    headers=headers,
                )
                response.raise_for_status()

            data = response.json()
            return self._parse_range_response(selector.name, data, workspace_id)

        except httpx.HTTPStatusError as exc:
            raise BackendUnavailable(f"Prometheus query failed: {exc}") from exc
        except (httpx.ConnectError, httpx.TimeoutError) as exc:
            raise BackendUnavailable(f"Prometheus unavailable: {exc}") from exc
        except Exception as exc:
            raise BackendUnavailable(f"Prometheus query failed: {exc}") from exc

    async def query_workspace_metrics(
        self,
        workspace_id: WorkspaceId,
        selector: MetricSelector,
        range: MetricRange,
    ) -> MetricSeries:
        """Workspace-scoped range query (no run filter)."""
        try:
            query = self._build_query(selector, workspace_id, run_id=None)

            params: dict = {
                "query": query,
                "start": int(range.start.timestamp()),
                "end": int(range.end.timestamp()),
                "step": range.step_seconds,
            }

            async with httpx.AsyncClient(verify=self.tls_verify) as client:
                headers = {}
                if self.tenant_id:
                    headers["X-Scope-OrgID"] = self.tenant_id

                response = await client.get(
                    f"{self.base_url}/api/v1/query_range",
                    params=params,
                    headers=headers,
                )
                response.raise_for_status()

            data = response.json()
            return self._parse_range_response(selector.name, data, workspace_id)

        except httpx.HTTPStatusError:
            raise BackendUnavailable("Prometheus query failed") from None
        except (httpx.ConnectError, httpx.TimeoutError) as exc:
            raise BackendUnavailable(f"Prometheus unavailable: {exc}") from exc
        except Exception as exc:
            raise BackendUnavailable(f"Prometheus query failed: {exc}") from exc

    async def query_instant_metric(
        self,
        workspace_id: WorkspaceId,
        selector: MetricSelector,
        at: datetime,
    ) -> MetricSample:
        """Single-point lookup at a specific instant."""
        try:
            query = self._build_query(selector, workspace_id, run_id=None)

            params: dict = {
                "query": query,
                "time": int(at.timestamp()),
            }

            async with httpx.AsyncClient(verify=self.tls_verify) as client:
                headers = {}
                if self.tenant_id:
                    headers["X-Scope-OrgID"] = self.tenant_id

                response = await client.get(
                    f"{self.base_url}/api/v1/query",
                    params=params,
                    headers=headers,
                )
                response.raise_for_status()

            data = response.json()
            return self._parse_instant_response(data, at, workspace_id)

        except httpx.HTTPStatusError:
            raise BackendUnavailable("Prometheus instant query failed") from None
        except (httpx.ConnectError, httpx.TimeoutError) as exc:
            raise BackendUnavailable(f"Prometheus unavailable: {exc}") from exc
        except Exception as exc:
            raise BackendUnavailable(f"Prometheus instant query failed: {exc}") from exc

    def _parse_range_response(
        self,
        name: str,
        data: dict,
        workspace_id: WorkspaceId,
    ) -> MetricSeries:
        """Parse Prometheus range query response into MetricSeries."""
        if data.get("status") != "success":
            raise BackendUnavailable(f"Prometheus error: {data.get('error', 'unknown')}")

        result = data.get("data", {}).get("result", [])
        if not result:
            return MetricSeries(name=name, labels={}, samples=())

        # For simplicity, use first series (most range queries return one)
        first_result = result[0]
        labels = dict(first_result.get("metric", {}))

        samples = []
        for timestamp_str, value_str in first_result.get("values", []):
            try:
                timestamp = datetime.fromtimestamp(float(timestamp_str))
                value = float(value_str)
                sample = MetricSample(
                    timestamp=timestamp,
                    value=value,
                    labels=labels,
                )
                samples.append(sample)
            except (ValueError, TypeError):
                continue

        return MetricSeries(name=name, labels=labels, samples=tuple(samples))

    def _parse_instant_response(
        self,
        data: dict,
        at: datetime,
        workspace_id: WorkspaceId,
    ) -> MetricSample:
        """Parse Prometheus instant query response into MetricSample."""
        if data.get("status") != "success":
            raise BackendUnavailable(f"Prometheus error: {data.get('error', 'unknown')}")

        result = data.get("data", {})
        result_type = result.get("resultType")

        if result_type == "vector":
            values = result.get("result", [])
            if not values:
                raise BackendUnavailable("Prometheus returned no data")

            first_value = values[0]
            labels = dict(first_value.get("metric", {}))

            try:
                value_tuple = first_value.get("value", [None, None])
                value = float(value_tuple[1])
                return MetricSample(
                    timestamp=at,
                    value=value,
                    labels=labels,
                )
            except (ValueError, TypeError, IndexError) as exc:
                raise BackendUnavailable(f"Invalid Prometheus response: {exc}") from exc

        raise BackendUnavailable(f"Unexpected result type: {result_type}")


class NoopMetricsAdapter:
    """Noop MetricsQueryProvider that always returns QueryUnsupported.

    Used as a fallback when metrics are disabled. UI navigates to
    CUSTOS_METRICS_EXTERNAL_URL instead.
    """

    SCHEMA_REVISION: ClassVar[int] = 0

    async def query_run_metrics(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        selector: MetricSelector,
        range: MetricRange,
    ) -> MetricSeries:
        """Raise QueryUnsupported."""
        raise QueryUnsupported("metrics not configured")

    async def query_workspace_metrics(
        self,
        workspace_id: WorkspaceId,
        selector: MetricSelector,
        range: MetricRange,
    ) -> MetricSeries:
        """Raise QueryUnsupported."""
        raise QueryUnsupported("metrics not configured")

    async def query_instant_metric(
        self,
        workspace_id: WorkspaceId,
        selector: MetricSelector,
        at: datetime,
    ) -> MetricSample:
        """Raise QueryUnsupported."""
        raise QueryUnsupported("metrics not configured")


def make_prometheus_adapter() -> PrometheusMetricsAdapter:
    """Factory: create Prometheus adapter from environment variables.

    Required:
        CUSTOS_PROMETHEUS_URL: Prometheus base URL

    Optional:
        CUSTOS_PROMETHEUS_TENANT_ID: Tenant ID (for Cortex/Mimir)
        CUSTOS_PROMETHEUS_TLS_VERIFY: TLS verification (true/false or CA path)
    """
    base_url = os.getenv("CUSTOS_PROMETHEUS_URL")
    if not base_url:
        raise RuntimeError(
            "CUSTOS_PROMETHEUS_URL environment variable not set; "
            "required for Prometheus adapter"
        )

    tenant_id = os.getenv("CUSTOS_PROMETHEUS_TENANT_ID")
    tls_verify_str = os.getenv("CUSTOS_PROMETHEUS_TLS_VERIFY", "true")

    # Parse TLS verify setting
    if tls_verify_str.lower() in ("false", "0"):
        tls_verify: bool | str = False
    elif tls_verify_str.lower() in ("true", "1"):
        tls_verify = True
    else:
        # Treat as path to CA bundle
        tls_verify = tls_verify_str

    return PrometheusMetricsAdapter(
        base_url=base_url,
        tenant_id=tenant_id,
        tls_verify=tls_verify,
    )


def make_noop_adapter() -> NoopMetricsAdapter:
    """Factory: create noop adapter."""
    return NoopMetricsAdapter()

"""Tests for the run-metrics read-back route (OBS-IMPL-014).

Exercise the happy-path range query (selector + window mapping), parameter
validation, and the noop/unreachable/cross-workspace error mapping against fake
:class:`MetricsQueryProvider` implementations behind the real app + middleware.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from custos_spl import RunId, WorkspaceId
from custos_spl.errors import BackendUnavailable, QueryUnsupported, WorkspaceMismatch
from custos_spl.interfaces.metrics_query import (
    MetricRange,
    MetricSample,
    MetricSelector,
    MetricSeries,
)
from fastapi.testclient import TestClient

from custos_obs import create_app
from custos_obs.providers import Providers
from custos_obs.settings import Settings, load_settings

_TS = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)

_NOOP_ENV = {
    "CUSTOS_LOG_QUERY_PROVIDER": "noop",
    "CUSTOS_LOGS_EXTERNAL_URL": "https://logs.example.com",
    "CUSTOS_METRICS_QUERY_PROVIDER": "noop",
    "CUSTOS_METRICS_EXTERNAL_URL": "https://metrics.example.com",
    "CUSTOS_OBS_METADATA_STORE_DSN": "postgresql://noop/noop",
}

_NO_METRICS_PTR_ENV = {
    "CUSTOS_LOG_QUERY_PROVIDER": "noop",
    "CUSTOS_LOGS_EXTERNAL_URL": "https://logs.example.com",
    "CUSTOS_METRICS_QUERY_PROVIDER": "prometheus",
    "CUSTOS_PROMETHEUS_URL": "http://prom:9090",
    "CUSTOS_OBS_METADATA_STORE_DSN": "postgresql://noop/noop",
}


def _noop_settings() -> Settings:
    return load_settings(_NOOP_ENV)


def _series() -> MetricSeries:
    return MetricSeries(
        name="cpu",
        labels={"run": "run-1"},
        samples=(MetricSample(timestamp=_TS, value=1.5, labels={"q": "0.99"}),),
    )


class _FakeMetadataStore:
    async def query_audit(self, *args: object, **kwargs: object) -> object:  # pragma: no cover
        raise NotImplementedError


class _FakeLogQuery:
    async def query_run_logs(self, *args: object, **kwargs: object) -> object:  # pragma: no cover
        raise NotImplementedError


class _FakeMetricsProvider:
    def __init__(
        self,
        *,
        series: MetricSeries | None = None,
        raise_on_call: Exception | None = None,
    ) -> None:
        self._series = series
        self._raise_on_call = raise_on_call
        self.last_selector: MetricSelector | None = None
        self.last_range: MetricRange | None = None

    async def query_run_metrics(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        selector: MetricSelector,
        range: MetricRange,
    ) -> MetricSeries:
        self.last_selector = selector
        self.last_range = range
        if self._raise_on_call is not None:
            raise self._raise_on_call
        assert self._series is not None
        return self._series


def _providers(metrics: _FakeMetricsProvider) -> Providers:
    return Providers(
        metadata_store=_FakeMetadataStore(),  # type: ignore[arg-type]
        log_query=_FakeLogQuery(),  # type: ignore[arg-type]
        metrics_query=metrics,  # type: ignore[arg-type]
    )


def _client(metrics: _FakeMetricsProvider, settings: Settings | None = None) -> TestClient:
    app = create_app(
        settings=settings if settings is not None else _noop_settings(),
        providers=_providers(metrics),
        authz_jwks_url="",
    )
    return TestClient(app)


def _auth(*, workspace: str = "ws-1", perms: tuple[str, ...] = ("metrics:read",)) -> dict[str, str]:
    return {
        "x-custos-callctx": json.dumps(
            {"acting_principal_id": "u", "workspace_id": workspace, "permissions": list(perms)}
        )
    }


_URL = "/v1/workspaces/ws-1/runs/run-1/metrics"
_WINDOW = {"from": "2024-01-01T00:00:00Z", "to": "2024-01-02T00:00:00Z"}


def test_query_metrics_returns_series() -> None:
    provider = _FakeMetricsProvider(series=_series())
    with _client(provider) as client:
        resp = client.get(_URL, headers=_auth(), params={"metric": "cpu", **_WINDOW, "step": 30})
    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "cpu"
    assert body["samples"][0]["value"] == 1.5
    assert provider.last_selector == MetricSelector(name="cpu")
    assert provider.last_range is not None
    assert provider.last_range.start == datetime(2024, 1, 1, tzinfo=UTC)
    assert provider.last_range.end == datetime(2024, 1, 2, tzinfo=UTC)
    assert provider.last_range.step_seconds == 30


def test_query_metrics_default_step() -> None:
    provider = _FakeMetricsProvider(series=_series())
    with _client(provider) as client:
        resp = client.get(_URL, headers=_auth(), params={"metric": "cpu", **_WINDOW})
    assert resp.status_code == 200
    assert provider.last_range is not None
    assert provider.last_range.step_seconds == 60


def test_query_metrics_missing_metric_is_422() -> None:
    provider = _FakeMetricsProvider(series=_series())
    with _client(provider) as client:
        resp = client.get(_URL, headers=_auth(), params=_WINDOW)
    assert resp.status_code == 422


def test_query_metrics_missing_window_is_422() -> None:
    provider = _FakeMetricsProvider(series=_series())
    with _client(provider) as client:
        resp = client.get(_URL, headers=_auth(), params={"metric": "cpu"})
    assert resp.status_code == 422


def test_query_metrics_zero_step_is_422() -> None:
    provider = _FakeMetricsProvider(series=_series())
    with _client(provider) as client:
        resp = client.get(_URL, headers=_auth(), params={"metric": "cpu", **_WINDOW, "step": 0})
    assert resp.status_code == 422


def test_query_metrics_invalid_datetime_is_400() -> None:
    provider = _FakeMetricsProvider(series=_series())
    with _client(provider) as client:
        resp = client.get(
            _URL, headers=_auth(), params={"metric": "cpu", "from": "nope", "to": "nope"}
        )
    assert resp.status_code == 400


def test_query_metrics_naive_datetime_is_400() -> None:
    provider = _FakeMetricsProvider(series=_series())
    with _client(provider) as client:
        resp = client.get(
            _URL,
            headers=_auth(),
            params={"metric": "cpu", "from": "2024-01-01T00:00:00", "to": "2024-01-02T00:00:00Z"},
        )
    assert resp.status_code == 400


def test_query_metrics_noop_returns_503_with_pointer() -> None:
    provider = _FakeMetricsProvider(raise_on_call=QueryUnsupported("metrics not configured"))
    with _client(provider) as client:
        resp = client.get(_URL, headers=_auth(), params={"metric": "cpu", **_WINDOW})
    assert resp.status_code == 503
    assert resp.headers["content-type"].startswith("application/problem+json")
    body = resp.json()
    assert body["title"] == "Metrics query backend unavailable"
    assert body["externalUrl"] == "https://metrics.example.com"


def test_query_metrics_backend_unavailable_returns_503() -> None:
    provider = _FakeMetricsProvider(raise_on_call=BackendUnavailable("prom down"))
    with _client(provider) as client:
        resp = client.get(_URL, headers=_auth(), params={"metric": "cpu", **_WINDOW})
    assert resp.status_code == 503


def test_query_metrics_503_without_pointer_when_unset() -> None:
    provider = _FakeMetricsProvider(raise_on_call=QueryUnsupported("nope"))
    settings = load_settings(_NO_METRICS_PTR_ENV)
    with _client(provider, settings=settings) as client:
        resp = client.get(_URL, headers=_auth(), params={"metric": "cpu", **_WINDOW})
    assert resp.status_code == 503
    assert "externalUrl" not in resp.json()


def test_query_metrics_workspace_mismatch_returns_404() -> None:
    provider = _FakeMetricsProvider(raise_on_call=WorkspaceMismatch("other ws"))
    with _client(provider) as client:
        resp = client.get(_URL, headers=_auth(), params={"metric": "cpu", **_WINDOW})
    assert resp.status_code == 404


def test_query_metrics_requires_metrics_read_permission() -> None:
    provider = _FakeMetricsProvider(series=_series())
    with _client(provider) as client:
        resp = client.get(
            _URL, headers=_auth(perms=("logs:read",)), params={"metric": "cpu", **_WINDOW}
        )
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "permission_denied"


def test_query_metrics_path_workspace_mismatch_is_403() -> None:
    provider = _FakeMetricsProvider(series=_series())
    with _client(provider) as client:
        resp = client.get(
            _URL, headers=_auth(workspace="other-ws"), params={"metric": "cpu", **_WINDOW}
        )
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "workspace_mismatch"


def test_query_metrics_missing_callctx_is_401() -> None:
    provider = _FakeMetricsProvider(series=_series())
    with _client(provider) as client:
        resp = client.get(_URL, params={"metric": "cpu", **_WINDOW})
    assert resp.status_code == 401

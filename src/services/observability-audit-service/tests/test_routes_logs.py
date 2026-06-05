"""Tests for the log read-back routes (OBS-IMPL-013).

Exercise the paged history query (filters, cursor, step routing, error mapping)
and the SSE tail (framing, resume, pre-stream + in-stream unavailability) against
fake :class:`LogQueryProvider` implementations behind the real app + middleware.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime

from custos_spl import (
    Cursor,
    LogPage,
    LogRecord,
    Page,
    RunId,
    StepId,
    WorkspaceId,
)
from custos_spl.errors import BackendUnavailable, QueryUnsupported, WorkspaceMismatch
from custos_spl.interfaces.log_query import LogFilter
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

_LOKI_ENV = {
    "CUSTOS_LOG_QUERY_PROVIDER": "loki",
    "CUSTOS_LOKI_URL": "http://loki:3100",
    "CUSTOS_METRICS_QUERY_PROVIDER": "noop",
    "CUSTOS_METRICS_EXTERNAL_URL": "https://metrics.example.com",
    "CUSTOS_OBS_METADATA_STORE_DSN": "postgresql://noop/noop",
}


def _noop_settings() -> Settings:
    return load_settings(_NOOP_ENV)


def _loki_settings() -> Settings:
    return load_settings(_LOKI_ENV)


def _record(message: str = "hello", *, step: str | None = None) -> LogRecord:
    return LogRecord(
        timestamp=_TS,
        severity="info",
        message=message,
        run_id=RunId("run-1"),
        step_id=None if step is None else StepId(step),
        attributes={"k": "v"},
    )


class _FakeMetadataStore:
    async def query_audit(
        self, *args: object, **kwargs: object
    ) -> object:  # pragma: no cover - stub
        raise NotImplementedError


class _FakeMetricsQuery:
    async def query_run_metrics(
        self, *args: object, **kwargs: object
    ) -> object:  # pragma: no cover
        raise NotImplementedError


class _FakeLogProvider:
    """Configurable :class:`LogQueryProvider` double capturing call arguments."""

    def __init__(
        self,
        *,
        page: LogPage | None = None,
        tail_records: list[LogRecord] | None = None,
        raise_on_call: Exception | None = None,
        raise_in_stream: Exception | None = None,
    ) -> None:
        self._page = page
        self._tail_records = tail_records or []
        self._raise_on_call = raise_on_call
        self._raise_in_stream = raise_in_stream
        self.called: str | None = None
        self.last_filter: LogFilter | None = None
        self.last_cursor: Cursor | None = None
        self.last_from_cursor: Cursor | None = None
        self.last_step: StepId | None = None

    async def query_run_logs(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        filter: LogFilter,
        cursor: Cursor | None = None,
    ) -> LogPage:
        self.called = "query_run_logs"
        self.last_filter = filter
        self.last_cursor = cursor
        if self._raise_on_call is not None:
            raise self._raise_on_call
        assert self._page is not None
        return self._page

    async def query_step_logs(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        step_id: StepId,
        filter: LogFilter,
        cursor: Cursor | None = None,
    ) -> LogPage:
        self.called = "query_step_logs"
        self.last_step = step_id
        self.last_filter = filter
        self.last_cursor = cursor
        if self._raise_on_call is not None:
            raise self._raise_on_call
        assert self._page is not None
        return self._page

    def tail_run_logs(
        self,
        workspace_id: WorkspaceId,
        run_id: RunId,
        from_cursor: Cursor | None = None,
    ) -> AsyncIterator[LogRecord]:
        self.called = "tail_run_logs"
        self.last_from_cursor = from_cursor
        if self._raise_on_call is not None:
            raise self._raise_on_call
        records = list(self._tail_records)
        raise_in_stream = self._raise_in_stream

        async def _gen() -> AsyncIterator[LogRecord]:
            for record in records:
                yield record
            if raise_in_stream is not None:
                raise raise_in_stream

        return _gen()


def _providers(log_provider: _FakeLogProvider) -> Providers:
    return Providers(
        metadata_store=_FakeMetadataStore(),  # type: ignore[arg-type]
        log_query=log_provider,  # type: ignore[arg-type]
        metrics_query=_FakeMetricsQuery(),  # type: ignore[arg-type]
    )


def _client(log_provider: _FakeLogProvider, settings: Settings | None = None) -> TestClient:
    app = create_app(
        settings=settings if settings is not None else _noop_settings(),
        providers=_providers(log_provider),
        authz_jwks_url="",
    )
    return TestClient(app)


def _auth(*, workspace: str = "ws-1", perms: tuple[str, ...] = ("logs:read",)) -> dict[str, str]:
    return {
        "x-custos-callctx": json.dumps(
            {"acting_principal_id": "u", "workspace_id": workspace, "permissions": list(perms)}
        )
    }


_LOGS_URL = "/v1/workspaces/ws-1/runs/run-1/logs"
_TAIL_URL = "/v1/workspaces/ws-1/runs/run-1/logs/tail"


# --------------------------------------------------------------------------- #
# Paged history
# --------------------------------------------------------------------------- #


def test_query_logs_returns_page() -> None:
    provider = _FakeLogProvider(page=Page(items=(_record(),), next_cursor=Cursor(token="c1")))
    with _client(provider) as client:
        resp = client.get(_LOGS_URL, headers=_auth())
    assert resp.status_code == 200
    body = resp.json()
    assert provider.called == "query_run_logs"
    assert body["items"][0]["message"] == "hello"
    assert body["nextCursor"] == "c1"


def test_query_logs_routes_step_id_to_query_step_logs() -> None:
    provider = _FakeLogProvider(page=Page(items=(_record(step="s1"),), next_cursor=None))
    with _client(provider) as client:
        resp = client.get(_LOGS_URL, headers=_auth(), params={"stepId": "s1"})
    assert resp.status_code == 200
    assert provider.called == "query_step_logs"
    assert provider.last_step == StepId("s1")
    assert resp.json()["nextCursor"] is None


def test_query_logs_honors_all_filters_and_cursor() -> None:
    provider = _FakeLogProvider(page=Page(items=(), next_cursor=None))
    with _client(provider) as client:
        resp = client.get(
            _LOGS_URL,
            headers=_auth(),
            params={
                "from": "2024-01-01T00:00:00+00:00",
                "to": "2024-01-02T00:00:00+00:00",
                "severity": "error",
                "cursor": "tok",
            },
        )
    assert resp.status_code == 200
    assert provider.last_filter is not None
    assert provider.last_filter.start == datetime(2024, 1, 1, tzinfo=UTC)
    assert provider.last_filter.end == datetime(2024, 1, 2, tzinfo=UTC)
    assert provider.last_filter.severity_at_least == "error"
    assert provider.last_cursor == Cursor(token="tok")


def test_query_logs_invalid_severity_is_422() -> None:
    provider = _FakeLogProvider(page=Page(items=(), next_cursor=None))
    with _client(provider) as client:
        resp = client.get(_LOGS_URL, headers=_auth(), params={"severity": "bogus"})
    assert resp.status_code == 422


def test_query_logs_invalid_datetime_is_400() -> None:
    provider = _FakeLogProvider(page=Page(items=(), next_cursor=None))
    with _client(provider) as client:
        resp = client.get(_LOGS_URL, headers=_auth(), params={"from": "not-a-date"})
    assert resp.status_code == 400
    assert "invalid datetime" in resp.json()["detail"]


def test_query_logs_noop_returns_503_with_pointer() -> None:
    provider = _FakeLogProvider(raise_on_call=QueryUnsupported("logs not configured"))
    with _client(provider) as client:
        resp = client.get(_LOGS_URL, headers=_auth())
    assert resp.status_code == 503
    assert resp.headers["content-type"].startswith("application/problem+json")
    body = resp.json()
    assert body["title"] == "Log query backend unavailable"
    assert body["status"] == 503
    assert body["externalUrl"] == "https://logs.example.com"


def test_query_logs_backend_unavailable_returns_503() -> None:
    provider = _FakeLogProvider(raise_on_call=BackendUnavailable("loki down"))
    with _client(provider) as client:
        resp = client.get(_LOGS_URL, headers=_auth())
    assert resp.status_code == 503


def test_query_logs_503_without_external_url_omits_pointer() -> None:
    provider = _FakeLogProvider(raise_on_call=QueryUnsupported("nope"))
    with _client(provider, settings=_loki_settings()) as client:
        resp = client.get(_LOGS_URL, headers=_auth())
    assert resp.status_code == 503
    assert "externalUrl" not in resp.json()


def test_query_logs_workspace_mismatch_returns_404() -> None:
    provider = _FakeLogProvider(raise_on_call=WorkspaceMismatch("other ws"))
    with _client(provider) as client:
        resp = client.get(_LOGS_URL, headers=_auth())
    assert resp.status_code == 404
    assert "run-1" in resp.json()["detail"]


def test_query_logs_requires_logs_read_permission() -> None:
    provider = _FakeLogProvider(page=Page(items=(), next_cursor=None))
    with _client(provider) as client:
        resp = client.get(_LOGS_URL, headers=_auth(perms=("metrics:read",)))
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "permission_denied"


def test_query_logs_path_workspace_mismatch_is_403() -> None:
    provider = _FakeLogProvider(page=Page(items=(), next_cursor=None))
    with _client(provider) as client:
        resp = client.get(_LOGS_URL, headers=_auth(workspace="other-ws"))
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "workspace_mismatch"


def test_query_logs_missing_callctx_is_401() -> None:
    provider = _FakeLogProvider(page=Page(items=(), next_cursor=None))
    with _client(provider) as client:
        resp = client.get(_LOGS_URL)
    assert resp.status_code == 401


# --------------------------------------------------------------------------- #
# SSE tail
# --------------------------------------------------------------------------- #


def test_tail_streams_sse_frames() -> None:
    provider = _FakeLogProvider(tail_records=[_record("one"), _record("two")])
    with _client(provider) as client:
        resp = client.get(_TAIL_URL, headers=_auth())
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")
    frames = [f for f in resp.text.split("\n\n") if f]
    assert len(frames) == 2
    assert frames[0].startswith("data: ")
    assert json.loads(frames[0][len("data: ") :])["message"] == "one"
    assert json.loads(frames[1][len("data: ") :])["message"] == "two"


def test_tail_resume_uses_last_event_id_header() -> None:
    provider = _FakeLogProvider(tail_records=[_record()])
    with _client(provider) as client:
        resp = client.get(_TAIL_URL, headers={**_auth(), "Last-Event-ID": "resume-1"})
    assert resp.status_code == 200
    assert provider.last_from_cursor == Cursor(token="resume-1")


def test_tail_resume_uses_cursor_query_fallback() -> None:
    provider = _FakeLogProvider(tail_records=[_record()])
    with _client(provider) as client:
        resp = client.get(_TAIL_URL, headers=_auth(), params={"cursor": "resume-q"})
    assert resp.status_code == 200
    assert provider.last_from_cursor == Cursor(token="resume-q")


def test_tail_without_resume_passes_none_cursor() -> None:
    provider = _FakeLogProvider(tail_records=[_record()])
    with _client(provider) as client:
        resp = client.get(_TAIL_URL, headers=_auth())
    assert resp.status_code == 200
    assert provider.last_from_cursor is None


def test_tail_noop_returns_503() -> None:
    provider = _FakeLogProvider(raise_on_call=QueryUnsupported("logs not configured"))
    with _client(provider) as client:
        resp = client.get(_TAIL_URL, headers=_auth())
    assert resp.status_code == 503
    assert resp.json()["externalUrl"] == "https://logs.example.com"


def test_tail_backend_unavailable_pre_stream_returns_503() -> None:
    provider = _FakeLogProvider(raise_on_call=BackendUnavailable("loki down"))
    with _client(provider) as client:
        resp = client.get(_TAIL_URL, headers=_auth())
    assert resp.status_code == 503


def test_tail_workspace_mismatch_returns_404() -> None:
    provider = _FakeLogProvider(raise_on_call=WorkspaceMismatch("other ws"))
    with _client(provider) as client:
        resp = client.get(_TAIL_URL, headers=_auth())
    assert resp.status_code == 404


def test_tail_in_stream_failure_emits_error_frame() -> None:
    provider = _FakeLogProvider(
        tail_records=[_record("first")],
        raise_in_stream=BackendUnavailable("loki vanished mid-stream"),
    )
    with _client(provider) as client:
        resp = client.get(_TAIL_URL, headers=_auth())
    assert resp.status_code == 200
    frames = [f for f in resp.text.split("\n\n") if f]
    assert frames[0].startswith("data: ")
    assert json.loads(frames[0][len("data: ") :])["message"] == "first"
    assert frames[1].startswith("event: error")
    problem = json.loads(frames[1].split("data: ", 1)[1])
    assert problem["status"] == 503


def test_tail_requires_logs_read_permission() -> None:
    provider = _FakeLogProvider(tail_records=[_record()])
    with _client(provider) as client:
        resp = client.get(_TAIL_URL, headers=_auth(perms=()))
    assert resp.status_code == 403

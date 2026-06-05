"""Tests for the audit read-back routes (OBS-IMPL-014).

Exercise the paged search (filter mapping, subjectId post-filter, cursor
pagination), the single-event lookup (found / 404), and the unreachable-store
error mapping against a fake :class:`MetadataStoreProvider` behind the real app
+ middleware.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from custos_spl import AuditEvent, Cursor, Page, WorkspaceId
from custos_spl.errors import BackendUnavailable, QueryUnsupported
from custos_spl.interfaces.metadata_store import AuditFilter
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


def _noop_settings() -> Settings:
    return load_settings(_NOOP_ENV)


def _event(event_id: str, *, actor: str = "alice", subject_id: str = "sub-1") -> AuditEvent:
    return AuditEvent(
        workspace_id=WorkspaceId("ws-1"),
        event_id=event_id,
        event_type="run.started",
        actor=actor,
        subject={"id": subject_id, "type": "run"},
        payload={"k": "v"},
        occurred_at=_TS,
    )


class _FakeLogQuery:
    async def query_run_logs(self, *args: object, **kwargs: object) -> object:  # pragma: no cover
        raise NotImplementedError


class _FakeMetricsQuery:
    async def query_run_metrics(
        self, *args: object, **kwargs: object
    ) -> object:  # pragma: no cover
        raise NotImplementedError


class _FakeMetadataStore:
    """Fake store returning queued pages and recording call arguments.

    ``pages`` is consumed one per ``query_audit`` call so the single-event scan
    can be driven across multiple cursor-linked pages.
    """

    def __init__(
        self,
        *,
        pages: list[Page[AuditEvent]] | None = None,
        raise_on_call: Exception | None = None,
    ) -> None:
        self._pages = list(pages or [])
        self._raise_on_call = raise_on_call
        self.calls: list[tuple[AuditFilter | None, Cursor | None]] = []

    async def query_audit(
        self,
        workspace_id: WorkspaceId,
        filter: AuditFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[AuditEvent]:
        self.calls.append((filter, cursor))
        if self._raise_on_call is not None:
            raise self._raise_on_call
        if self._pages:
            return self._pages.pop(0)
        return Page(items=(), next_cursor=None)


def _providers(store: _FakeMetadataStore) -> Providers:
    return Providers(
        metadata_store=store,  # type: ignore[arg-type]
        log_query=_FakeLogQuery(),  # type: ignore[arg-type]
        metrics_query=_FakeMetricsQuery(),  # type: ignore[arg-type]
    )


def _client(store: _FakeMetadataStore, settings: Settings | None = None) -> TestClient:
    app = create_app(
        settings=settings if settings is not None else _noop_settings(),
        providers=_providers(store),
        authz_jwks_url="",
    )
    return TestClient(app)


def _auth(*, workspace: str = "ws-1", perms: tuple[str, ...] = ("audit:read",)) -> dict[str, str]:
    return {
        "x-custos-callctx": json.dumps(
            {"acting_principal_id": "u", "workspace_id": workspace, "permissions": list(perms)}
        )
    }


_SEARCH_URL = "/v1/workspaces/ws-1/audit"


# --------------------------------------------------------------------------- #
# Search
# --------------------------------------------------------------------------- #


def test_search_returns_page() -> None:
    store = _FakeMetadataStore(pages=[Page(items=(_event("e1"),), next_cursor=Cursor(token="c1"))])
    with _client(store) as client:
        resp = client.get(_SEARCH_URL, headers=_auth())
    assert resp.status_code == 200
    body = resp.json()
    assert body["items"][0]["eventId"] == "e1"
    assert body["nextCursor"] == "c1"


def test_search_maps_all_filters_and_cursor() -> None:
    store = _FakeMetadataStore(pages=[Page(items=(), next_cursor=None)])
    with _client(store) as client:
        resp = client.get(
            _SEARCH_URL,
            headers=_auth(),
            params={
                "actor": "bob",
                "eventName": "run.failed",
                "from": "2024-01-01T00:00:00Z",
                "to": "2024-01-02T00:00:00Z",
                "cursor": "tok",
            },
        )
    assert resp.status_code == 200
    audit_filter, cursor = store.calls[0]
    assert audit_filter == AuditFilter(
        event_type="run.failed",
        actor="bob",
        occurred_after=datetime(2024, 1, 1, tzinfo=UTC),
        occurred_before=datetime(2024, 1, 2, tzinfo=UTC),
    )
    assert cursor == Cursor(token="tok")


def test_search_subject_id_post_filters() -> None:
    store = _FakeMetadataStore(
        pages=[
            Page(
                items=(
                    _event("e1", subject_id="sub-1"),
                    _event("e2", subject_id="sub-2"),
                ),
                next_cursor=Cursor(token="next"),
            )
        ]
    )
    with _client(store) as client:
        resp = client.get(_SEARCH_URL, headers=_auth(), params={"subjectId": "sub-2"})
    assert resp.status_code == 200
    body = resp.json()
    assert [e["eventId"] for e in body["items"]] == ["e2"]
    # The opaque cursor is preserved across the post-filter.
    assert body["nextCursor"] == "next"


def test_search_invalid_datetime_is_400() -> None:
    store = _FakeMetadataStore(pages=[Page(items=(), next_cursor=None)])
    with _client(store) as client:
        resp = client.get(_SEARCH_URL, headers=_auth(), params={"from": "nope"})
    assert resp.status_code == 400


def test_search_store_unavailable_returns_503() -> None:
    store = _FakeMetadataStore(raise_on_call=BackendUnavailable("pg down"))
    with _client(store) as client:
        resp = client.get(_SEARCH_URL, headers=_auth())
    assert resp.status_code == 503
    assert resp.headers["content-type"].startswith("application/problem+json")
    body = resp.json()
    assert body["title"] == "Audit query backend unavailable"
    assert body["status"] == 503
    assert "externalUrl" not in body


def test_search_query_unsupported_returns_503() -> None:
    store = _FakeMetadataStore(raise_on_call=QueryUnsupported("nope"))
    with _client(store) as client:
        resp = client.get(_SEARCH_URL, headers=_auth())
    assert resp.status_code == 503
    assert resp.headers["content-type"].startswith("application/problem+json")


def test_search_requires_audit_read_permission() -> None:
    store = _FakeMetadataStore(pages=[Page(items=(), next_cursor=None)])
    with _client(store) as client:
        resp = client.get(_SEARCH_URL, headers=_auth(perms=("logs:read",)))
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "permission_denied"


def test_search_path_workspace_mismatch_is_403() -> None:
    store = _FakeMetadataStore(pages=[Page(items=(), next_cursor=None)])
    with _client(store) as client:
        resp = client.get(_SEARCH_URL, headers=_auth(workspace="other-ws"))
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "workspace_mismatch"


def test_search_missing_callctx_is_401() -> None:
    store = _FakeMetadataStore(pages=[Page(items=(), next_cursor=None)])
    with _client(store) as client:
        resp = client.get(_SEARCH_URL)
    assert resp.status_code == 401


# --------------------------------------------------------------------------- #
# Single-event lookup
# --------------------------------------------------------------------------- #


def test_lookup_found_on_first_page() -> None:
    store = _FakeMetadataStore(pages=[Page(items=(_event("e1"), _event("e2")), next_cursor=None)])
    with _client(store) as client:
        resp = client.get(f"{_SEARCH_URL}/e2", headers=_auth())
    assert resp.status_code == 200
    assert resp.json()["eventId"] == "e2"


def test_lookup_scans_multiple_pages() -> None:
    store = _FakeMetadataStore(
        pages=[
            Page(items=(_event("e1"),), next_cursor=Cursor(token="p2")),
            Page(items=(_event("e2"),), next_cursor=None),
        ]
    )
    with _client(store) as client:
        resp = client.get(f"{_SEARCH_URL}/e2", headers=_auth())
    assert resp.status_code == 200
    assert resp.json()["eventId"] == "e2"
    # Second page was fetched with the first page's cursor.
    assert store.calls[1][1] == Cursor(token="p2")


def test_lookup_absent_returns_404() -> None:
    store = _FakeMetadataStore(pages=[Page(items=(_event("e1"),), next_cursor=None)])
    with _client(store) as client:
        resp = client.get(f"{_SEARCH_URL}/missing", headers=_auth())
    assert resp.status_code == 404
    assert "missing" in resp.json()["detail"]


def test_lookup_store_unavailable_returns_503() -> None:
    store = _FakeMetadataStore(raise_on_call=BackendUnavailable("pg down"))
    with _client(store) as client:
        resp = client.get(f"{_SEARCH_URL}/e1", headers=_auth())
    assert resp.status_code == 503
    assert resp.headers["content-type"].startswith("application/problem+json")
    assert resp.json()["title"] == "Audit query backend unavailable"


def test_lookup_requires_audit_read_permission() -> None:
    store = _FakeMetadataStore(pages=[Page(items=(_event("e1"),), next_cursor=None)])
    with _client(store) as client:
        resp = client.get(f"{_SEARCH_URL}/e1", headers=_auth(perms=()))
    assert resp.status_code == 403

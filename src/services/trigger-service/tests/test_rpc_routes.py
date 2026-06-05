"""Tests for the internal resume-subscription RPC surface (TS-IMPL-016).

Drives :mod:`custos_trigger.api.routes.rpc` through a real
:class:`fastapi.testclient.TestClient` over the in-process metadata store. The
RPC routes are internal Dapr method invocations authenticated at the mesh
layer, so they carry **no** call-context header — the suite asserts the
call-context middleware bypasses them.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient

import custos_trigger.api.routes.rpc as rpc
from custos_trigger.api.errors import PROBLEM_MEDIA_TYPE
from custos_trigger.api.routes.rpc import (
    RESUME_WORKSPACE,
    compute_resume_id,
)
from custos_trigger.app import create_app
from custos_trigger.clients import FakeWorkflowServiceClient
from custos_trigger.dedup import Deduplicator
from custos_trigger.middleware.callctx import _BYPASS_PATHS
from custos_trigger.pipeline.dispatch import Dispatcher
from custos_trigger.providers import InMemoryTriggerMetadataStore, Providers
from custos_trigger.settings import DEFAULT_RESUME_DEFAULT_TTL_SECONDS

_T0 = datetime(2026, 6, 4, 12, 0, 0, tzinfo=UTC)

_REGISTER = "/RegisterResumeSubscription"
_CANCEL = "/CancelResumeSubscription"

_TRIPLE = {"runId": "run-1", "stepId": "step-1", "eventKey": "pr.merged"}
_SELECTOR_A = "event.data.region == 'emea'"
_SELECTOR_B = "event.data.region == 'apac'"


@dataclass
class RecordingAuditSink:
    """An audit sink that records every emitted event for assertions."""

    events: list[tuple[str, str, dict[str, Any]]] = field(default_factory=list)

    async def emit(self, event_name: str, *, workspace_id: str, attributes: Any) -> None:
        self.events.append((event_name, workspace_id, dict(attributes)))


@pytest.fixture
def audit() -> RecordingAuditSink:
    return RecordingAuditSink()


@pytest.fixture
def client(
    providers: Providers,
    metadata_store: InMemoryTriggerMetadataStore,
    audit: RecordingAuditSink,
) -> Iterator[TestClient]:
    dispatcher = Dispatcher(FakeWorkflowServiceClient(), Deduplicator(metadata_store))
    app = create_app(
        authz_endpoint="",
        providers=providers,
        dispatcher=dispatcher,
        audit_sink=audit,
    )
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def frozen_now(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin :func:`rpc._now` to a fixed instant for deterministic expiries."""
    monkeypatch.setattr(rpc, "_now", lambda: _T0)


def _register(client: TestClient, **overrides: object) -> dict[str, object]:
    body: dict[str, object] = {**_TRIPLE, "selector": None, "ttl": "PT24H"}
    body.update(overrides)
    response = client.post(_REGISTER, json=body)
    assert response.status_code == 200, response.text
    return cast(dict[str, object], response.json())


# --------------------------------------------------------------------------
# Register: shape + idempotency
# --------------------------------------------------------------------------


def test_register_returns_subscription_id(client: TestClient) -> None:
    response = _register(client)
    assert set(response.keys()) == {"subscriptionId"}
    expected = compute_resume_id("run-1", "step-1", "pr.merged")
    assert response["subscriptionId"] == expected


def test_register_persists_the_resume_row(
    client: TestClient,
    metadata_store: InMemoryTriggerMetadataStore,
    frozen_now: None,
) -> None:
    _register(client, selector=_SELECTOR_A, ttl="PT1H")
    resume_id = compute_resume_id("run-1", "step-1", "pr.merged")
    row = metadata_store.resume_subscription(RESUME_WORKSPACE, resume_id)
    assert row is not None
    assert str(row.run_id) == "run-1"
    assert str(row.step_id) == "step-1"
    assert row.payload["eventKey"] == "pr.merged"
    assert row.payload["selector"] == _SELECTOR_A
    assert row.expires_at == _T0 + timedelta(hours=1)


def test_register_is_idempotent_on_the_triple(
    client: TestClient,
    audit: RecordingAuditSink,
) -> None:
    first = _register(client, selector=_SELECTOR_A)
    second = _register(client, selector=_SELECTOR_A)
    assert first["subscriptionId"] == second["subscriptionId"]
    # No divergence -> no audit on an identical re-registration.
    assert audit.events == []


def test_register_requires_no_call_context(client: TestClient) -> None:
    # The route is in the middleware bypass set; a missing call-context header
    # must NOT 401 (internal Dapr invokes carry none).
    response = client.post(_REGISTER, json={**_TRIPLE, "selector": None, "ttl": "PT24H"})
    assert response.status_code == 200, response.text


def test_rpc_paths_are_in_the_callctx_bypass_set() -> None:
    assert _REGISTER in _BYPASS_PATHS
    assert _CANCEL in _BYPASS_PATHS


# --------------------------------------------------------------------------
# Register: divergence (original wins) + TTL refresh
# --------------------------------------------------------------------------


def test_divergent_selector_keeps_original_and_audits(
    client: TestClient,
    metadata_store: InMemoryTriggerMetadataStore,
    audit: RecordingAuditSink,
) -> None:
    first = _register(client, selector=_SELECTOR_A)
    second = _register(client, selector=_SELECTOR_B)

    # Same handle returned; original registration is untouched.
    assert second["subscriptionId"] == first["subscriptionId"]
    resume_id = str(first["subscriptionId"])
    row = metadata_store.resume_subscription(RESUME_WORKSPACE, resume_id)
    assert row is not None
    assert row.payload["selector"] == _SELECTOR_A

    assert len(audit.events) == 1
    name, workspace_id, attributes = audit.events[0]
    assert name == "resume.subscription.divergent"
    assert workspace_id == RESUME_WORKSPACE
    assert attributes["originalSelector"] == _SELECTOR_A
    assert attributes["replaySelector"] == _SELECTOR_B
    assert attributes["resumeId"] == resume_id


def test_reregistration_after_ttl_expiry_is_fresh(
    client: TestClient,
    metadata_store: InMemoryTriggerMetadataStore,
    audit: RecordingAuditSink,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(rpc, "_now", lambda: _T0)
    _register(client, selector=_SELECTOR_A, ttl="PT1H")

    # Advance past the original expiry; a re-register is a fresh registration.
    later = _T0 + timedelta(hours=2)
    monkeypatch.setattr(rpc, "_now", lambda: later)
    _register(client, selector=_SELECTOR_B, ttl="PT1H")

    resume_id = compute_resume_id("run-1", "step-1", "pr.merged")
    row = metadata_store.resume_subscription(RESUME_WORKSPACE, resume_id)
    assert row is not None
    assert row.payload["selector"] == _SELECTOR_B
    assert row.expires_at == later + timedelta(hours=1)
    # Fresh registration is not a divergence -> no audit.
    assert audit.events == []


# --------------------------------------------------------------------------
# Register: TTL resolution + selector validation
# --------------------------------------------------------------------------


def test_register_null_ttl_falls_back_to_default(
    client: TestClient,
    metadata_store: InMemoryTriggerMetadataStore,
    frozen_now: None,
) -> None:
    _register(client, ttl=None)
    resume_id = compute_resume_id("run-1", "step-1", "pr.merged")
    row = metadata_store.resume_subscription(RESUME_WORKSPACE, resume_id)
    assert row is not None
    assert row.expires_at == _T0 + timedelta(seconds=DEFAULT_RESUME_DEFAULT_TTL_SECONDS)


def test_register_unparseable_ttl_falls_back_to_default(
    client: TestClient,
    metadata_store: InMemoryTriggerMetadataStore,
    frozen_now: None,
) -> None:
    _register(client, ttl="not-a-duration")
    resume_id = compute_resume_id("run-1", "step-1", "pr.merged")
    row = metadata_store.resume_subscription(RESUME_WORKSPACE, resume_id)
    assert row is not None
    assert row.expires_at == _T0 + timedelta(seconds=DEFAULT_RESUME_DEFAULT_TTL_SECONDS)


def test_register_invalid_selector_is_rejected(client: TestClient) -> None:
    response = client.post(
        _REGISTER,
        json={**_TRIPLE, "selector": "event.data.region ==", "ttl": "PT24H"},
    )
    assert response.status_code == 422
    assert response.headers["content-type"].startswith(PROBLEM_MEDIA_TYPE)
    assert response.json()["type"].endswith("selector_invalid")


def test_register_invalid_selector_rejected_even_on_replay(client: TestClient) -> None:
    # An already-live wait must not let a malformed replay selector through:
    # the selector is compiled up front on every path.
    _register(client, selector=_SELECTOR_A)
    response = client.post(
        _REGISTER,
        json={**_TRIPLE, "selector": "event.data.region ==", "ttl": "PT24H"},
    )
    assert response.status_code == 422
    assert response.json()["type"].endswith("selector_invalid")


def test_register_missing_field_is_a_bad_request(client: TestClient) -> None:
    response = client.post(_REGISTER, json={"stepId": "step-1", "eventKey": "pr.merged"})
    assert response.status_code == 400
    assert response.headers["content-type"].startswith(PROBLEM_MEDIA_TYPE)


# --------------------------------------------------------------------------
# Cancel
# --------------------------------------------------------------------------


def test_cancel_removes_an_open_wait(
    client: TestClient,
    metadata_store: InMemoryTriggerMetadataStore,
) -> None:
    _register(client, selector=_SELECTOR_A)
    resume_id = compute_resume_id("run-1", "step-1", "pr.merged")
    assert metadata_store.resume_subscription(RESUME_WORKSPACE, resume_id) is not None

    response = client.post(_CANCEL, json=_TRIPLE)
    assert response.status_code == 204
    assert response.content == b""
    assert metadata_store.resume_subscription(RESUME_WORKSPACE, resume_id) is None


def test_cancel_unknown_key_is_a_clean_no_op(client: TestClient) -> None:
    response = client.post(
        _CANCEL,
        json={"runId": "ghost", "stepId": "nope", "eventKey": "never"},
    )
    assert response.status_code == 204
    assert response.content == b""


def test_cancel_then_reregister_mints_a_fresh_registration(
    client: TestClient,
    audit: RecordingAuditSink,
) -> None:
    _register(client, selector=_SELECTOR_A)
    assert client.post(_CANCEL, json=_TRIPLE).status_code == 204
    # After a cancel, the same triple with a different selector is fresh -> no
    # divergence audit (the prior row is gone).
    _register(client, selector=_SELECTOR_B)
    assert audit.events == []


# --------------------------------------------------------------------------
# Helpers: resume id + ISO-8601 duration parsing
# --------------------------------------------------------------------------


def test_compute_resume_id_is_deterministic_and_distinct() -> None:
    a = compute_resume_id("run-1", "step-1", "pr.merged")
    assert a == compute_resume_id("run-1", "step-1", "pr.merged")
    assert a.startswith("res_")
    # No concatenation collision across the triple boundary.
    assert compute_resume_id("run", "1step", "1pr.merged") != compute_resume_id(
        "run1", "step1", "pr.merged"
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("PT24H", 86_400),
        ("P7D", 604_800),
        ("PT1H", 3_600),
        ("PT30M", 1_800),
        ("PT45S", 45),
        ("P1W", 604_800),
        ("P1DT2H3M4S", 93_784),
        ("PT1.5S", 1),
    ],
)
def test_parse_iso8601_duration_seconds_valid(value: str, expected: int) -> None:
    assert rpc._parse_iso8601_duration_seconds(value) == expected


@pytest.mark.parametrize("value", ["", "P", "PT", "garbage", "24H", "PT24X"])
def test_parse_iso8601_duration_seconds_invalid(value: str) -> None:
    assert rpc._parse_iso8601_duration_seconds(value) is None


@pytest.mark.parametrize(
    ("ttl", "expected"),
    [
        ("PT1H", 3_600),
        (None, DEFAULT_RESUME_DEFAULT_TTL_SECONDS),
        ("", DEFAULT_RESUME_DEFAULT_TTL_SECONDS),
        ("nonsense", DEFAULT_RESUME_DEFAULT_TTL_SECONDS),
        ("PT0S", DEFAULT_RESUME_DEFAULT_TTL_SECONDS),
    ],
)
def test_resolve_ttl_seconds(ttl: str | None, expected: int) -> None:
    assert rpc._resolve_ttl_seconds(ttl, DEFAULT_RESUME_DEFAULT_TTL_SECONDS) == expected


# --------------------------------------------------------------------------
# Store read capability
# --------------------------------------------------------------------------


def test_resume_store_get_requires_readable_backend() -> None:
    import asyncio
    from typing import cast

    from custos_trigger.stores import ResumeReadUnsupportedError, ResumeSubscriptionStore
    from custos_trigger.stores.base import TriggerMetadataStore

    write_only = cast(TriggerMetadataStore, object())
    store = ResumeSubscriptionStore(write_only)
    with pytest.raises(ResumeReadUnsupportedError):
        asyncio.run(store.get("ws", "res_1"))


def test_resume_read_unsupported_renders_problem_501() -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient as _TestClient

    from custos_trigger.api import register_exception_handlers
    from custos_trigger.stores import ResumeReadUnsupportedError

    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/boom")
    async def _boom() -> dict[str, str]:
        raise ResumeReadUnsupportedError("backend has no resume read surface")

    with _TestClient(app) as test_client:
        response = test_client.get("/boom")
    assert response.status_code == 501
    assert response.headers["content-type"].startswith(PROBLEM_MEDIA_TYPE)
    assert response.json()["code"] == "trigger.api.resume_read_unsupported"

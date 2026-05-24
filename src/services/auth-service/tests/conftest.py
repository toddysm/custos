"""Shared fixtures for the auth-service test suite.

The Phase C route tests share a fairly chunky setup:

* Construct :func:`custos_auth.create_app` with the fake SPL providers
  (so no asyncpg dependency and no real network).
* Provide a TestClient that supplies a default ``x-custos-callctx``
  header so callers don't restate the JSON envelope in every test.

These fixtures wrap that boilerplate so the route tests stay focused
on the route behaviour rather than the test plumbing.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from custos_auth import create_app
from custos_auth.middleware.callctx import CALLCTX_HEADER
from custos_auth.providers import Providers
from custos_auth.settings import load_settings
from tests._fakes import FakeAuthAdapter, FakeMetadataAdapter

_DEFAULT_ENV = {
    "CUSTOS_AUTH_STORE_DSN": "postgresql://u:p@h:5432/custos_auth",
    "CUSTOS_AUTH_METADATA_STORE_DSN": "postgresql://u:p@h:5432/custos_meta",
}


@pytest.fixture
def fake_auth_store() -> FakeAuthAdapter:
    return FakeAuthAdapter()


@pytest.fixture
def fake_metadata_store() -> FakeMetadataAdapter:
    return FakeMetadataAdapter()


@pytest.fixture
def providers(
    fake_auth_store: FakeAuthAdapter,
    fake_metadata_store: FakeMetadataAdapter,
) -> Providers:
    return Providers(
        auth_store=fake_auth_store,  # type: ignore[arg-type]
        metadata_store=fake_metadata_store,  # type: ignore[arg-type]
    )


@pytest.fixture
def app(providers: Providers) -> FastAPI:
    return create_app(
        settings=load_settings(_DEFAULT_ENV),
        providers=providers,
    )


@pytest.fixture
def client(app: FastAPI) -> Iterator[TestClient]:
    with TestClient(app) as c:
        yield c


def callctx_header(
    *,
    principal_id: str = "user-1",
    tenant_id: str | None = None,
    workspace_id: str | None = None,
    permissions: list[str] | None = None,
) -> dict[str, str]:
    """Build a dev-shim call-context header for tests.

    By default the payload only includes ``principal_id="user-1"``.
    ``tenant_id``, ``workspace_id``, and ``permissions`` are added only
    when explicitly provided by an individual test.
    """
    payload: dict[str, Any] = {"principal_id": principal_id}
    if tenant_id is not None:
        payload["tenant_id"] = tenant_id
    if workspace_id is not None:
        payload["workspace_id"] = workspace_id
    if permissions is not None:
        payload["permissions"] = permissions
    return {CALLCTX_HEADER: json.dumps(payload)}

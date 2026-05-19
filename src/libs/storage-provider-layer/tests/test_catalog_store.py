"""Tests for CatalogStoreProvider Protocol and its data shapes."""

from __future__ import annotations

import inspect
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from custos_spl.interfaces import (
    ActivityTypeVersion,
    CatalogStoreProvider,
    ConnectorTypeVersion,
)


def _now() -> datetime:
    return datetime(2026, 1, 1, tzinfo=UTC)


# ----- Data shapes -----


def test_activity_type_version_is_frozen() -> None:
    atv = ActivityTypeVersion(
        namespace="acme",
        type="scan",
        version="1.0.0",
        digest="sha256:abc",
        normalized_manifest={"capabilities": []},
        deprecated=False,
        published_at=_now(),
    )
    with pytest.raises(FrozenInstanceError):
        atv.digest = "sha256:xyz"  # type: ignore[misc]


def test_connector_type_version_is_frozen() -> None:
    ctv = ConnectorTypeVersion(
        type="github",
        version="1.0.0",
        digest="sha256:abc",
        normalized_manifest={},
        deprecated=False,
        published_at=_now(),
    )
    with pytest.raises(FrozenInstanceError):
        ctv.deprecated = True  # type: ignore[misc]


# ----- Protocol shape -----


def test_protocol_declares_required_schema_revision() -> None:
    assert CatalogStoreProvider.SCHEMA_REVISION == 1


REQUIRED_METHODS = [
    "put_activity_type_version",
    "get_activity_type_version",
    "list_activity_type_versions",
    "set_activity_type_deprecated",
    "put_connector_type_version",
    "get_connector_type_version",
    "list_connector_type_versions",
    "set_connector_type_deprecated",
    "resolve",
]


@pytest.mark.parametrize("method", REQUIRED_METHODS)
def test_protocol_exposes_method(method: str) -> None:
    assert hasattr(CatalogStoreProvider, method)


@pytest.mark.parametrize("method", REQUIRED_METHODS)
def test_protocol_methods_are_async(method: str) -> None:
    fn = getattr(CatalogStoreProvider, method)
    assert inspect.iscoroutinefunction(fn), f"{method} must be async"


def test_catalog_methods_do_not_take_workspace_id() -> None:
    """Catalog is platform-wide, not workspace-scoped.

    Pinning this in a test so a future refactor that accidentally adds
    workspace-scoping to the catalog interface is caught immediately.
    """
    for name in REQUIRED_METHODS:
        sig = inspect.signature(getattr(CatalogStoreProvider, name))
        params = list(sig.parameters)
        assert "workspace_id" not in params, (
            f"{name} must not accept workspace_id — catalog is platform-wide"
        )


# ----- runtime_checkable conformance -----


class _MinimalCatalogStore:
    SCHEMA_REVISION = 1

    async def put_activity_type_version(self, *a: object, **kw: object) -> None: ...
    async def get_activity_type_version(self, *a: object, **kw: object) -> None: ...
    async def list_activity_type_versions(self, *a: object, **kw: object) -> None: ...
    async def set_activity_type_deprecated(self, *a: object, **kw: object) -> None: ...
    async def put_connector_type_version(self, *a: object, **kw: object) -> None: ...
    async def get_connector_type_version(self, *a: object, **kw: object) -> None: ...
    async def list_connector_type_versions(self, *a: object, **kw: object) -> None: ...
    async def set_connector_type_deprecated(self, *a: object, **kw: object) -> None: ...
    async def resolve(self, *a: object, **kw: object) -> None: ...


def test_runtime_checkable_recognizes_duck_typed_impl() -> None:
    assert isinstance(_MinimalCatalogStore(), CatalogStoreProvider)

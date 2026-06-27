"""Tests for the connector-type registration HTTP router (CONN-REG / #898).

Exercises ``POST /internal/v1/connectors:register`` end-to-end through
:func:`create_app` so the call-context middleware, the
:data:`CONNECTOR_REGISTER` permission gate, the Loader dispatch, and the
``LoaderError`` -> HTTP mapping are all under test.

The registration Loader is wired against an :class:`httpx.MockTransport`
registry (reusing the harness from :mod:`tests.test_loader_registry`) so
no live registry is needed. The headline case is the **#896 acceptance**:
a manifest declaring ``authenticationType: x-dapr-secret`` registers
cleanly (201) instead of failing with ``UNKNOWN_VENDOR_AUTH_TYPE``,
because the Loader is fed the ``x-dapr-secret -> KMS`` vendor override.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from typing import Any, cast

from custos_spl.interfaces.catalog_store import CatalogStoreProvider
from fastapi.testclient import TestClient

from custos_connector import create_app
from custos_connector.identity import IdentityResolverRegistry
from custos_connector.loader import IdentityCategory, Loader
from custos_connector.middleware import CALLCTX_HEADER
from custos_connector.permissions import CONNECTOR_READ, CONNECTOR_REGISTER
from custos_connector.providers import Providers
from custos_connector.settings import Settings
from tests._fakes import (
    FakeCatalogAdapter,
    FakeConnectorInstanceAdapter,
    FakeLeaseAdapter,
    FakeMetadataAdapter,
    build_bind_for_step_service,
    build_lease_manager,
)
from tests.test_loader_registry import (
    IMAGE_REF,
    _baseline_payload,
    _build_client,
    _make_handler,
)

_BASE_SETTINGS = Settings(
    catalog_store_dsn="postgresql://u:p@h:5432/cat",
    metadata_store_dsn="postgresql://u:p@h:5432/meta",
    catalog_endpoint="http://catalog-service:8080",
    authz_endpoint="",  # dev shim
    oci_referrers_timeout_ms=5000,
    publish_max_body_mb=4,
    sidecar_default_ttl_sec=600,
    lease_max_concurrent=16,
    pull_loop_min_interval_sec=10,
    health_cache_ttl_s=60,
    sidecar_mtls_issuer=None,
    environment="development",
)


def _x_dapr_secret_payload() -> dict[str, Any]:
    """Baseline connector manifest with the credentials block swapped to
    the first-party ``x-dapr-secret`` vendor authentication type."""
    payload = copy.deepcopy(_baseline_payload())
    payload["spec"]["credentials"] = {
        "authenticationType": "x-dapr-secret",
        "authentication": {
            "secretName": "dockerhub-pat",
            "usernameKey": "username",
            "tokenKey": "token",
            "namespace": "custos-connectors",
        },
    }
    return payload


def _ctx_header(*, permissions: list[str] | None = None) -> dict[str, str]:
    perms = permissions if permissions is not None else [CONNECTOR_REGISTER]
    return {
        CALLCTX_HEADER: json.dumps(
            {
                "workspace_id": "ws-1",
                "principal_id": "svc:control-plane",
                "permissions": perms,
            }
        )
    }


def _build_providers(*, loader: Loader | None) -> Providers:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    metadata = FakeMetadataAdapter(applied_revisions={1, 2, 3, 4})
    leases = FakeLeaseAdapter(applied_revisions={1})
    registry = IdentityResolverRegistry(resolvers=[])
    return Providers(
        catalog_store=cast(CatalogStoreProvider, catalog),
        instance_store=instances,  # type: ignore[arg-type]
        lease_store=leases,  # type: ignore[arg-type]
        metadata_store=metadata,  # type: ignore[arg-type]
        identity_registry=registry,
        bind_for_step_service=build_bind_for_step_service(
            catalog_store=catalog,
            instance_store=instances,
            metadata_store=metadata,
            identity_registry=registry,
        ),
        lease_manager=build_lease_manager(lease_store=leases, metadata_store=metadata),
        loader=loader,
    )


def _loader_for(
    payload: Mapping[str, Any],
    *,
    vendor_overrides: Mapping[str, IdentityCategory] | None,
) -> tuple[Loader, FakeCatalogAdapter]:
    catalog = FakeCatalogAdapter()
    client = _build_client(_make_handler(payload=dict(payload)))
    loader = Loader(
        catalog_store=cast(CatalogStoreProvider, catalog),
        registry_client=client,
        vendor_identity_categories=vendor_overrides,
    )
    return loader, catalog


# ---------------------------------------------------------------------------
# Happy path — the #896 acceptance
# ---------------------------------------------------------------------------


def test_register_x_dapr_secret_manifest_succeeds() -> None:
    """A manifest with authenticationType x-dapr-secret registers cleanly
    (201) — no UNKNOWN_VENDOR_AUTH_TYPE — because the Loader carries the
    x-dapr-secret -> KMS override (CONN-REG / #896)."""
    payload = _x_dapr_secret_payload()
    loader, _ = _loader_for(payload, vendor_overrides={"x-dapr-secret": IdentityCategory.KMS})
    app = create_app(settings=_BASE_SETTINGS, providers=_build_providers(loader=loader))
    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/connectors:register",
            json={"imageRef": IMAGE_REF},
            headers=_ctx_header(),
        )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["type"] == payload["metadata"]["type"]
    assert body["version"] == payload["metadata"]["version"]
    assert body["imageRef"] == IMAGE_REF
    assert body["digest"].startswith("sha256:")
    assert len(body["digest"]) == len("sha256:") + 64
    assert body["deprecated"] is False


def test_register_x_dapr_secret_without_override_is_422() -> None:
    """Without the vendor override the same manifest is rejected with the
    stable unknown-vendor-auth-type code — proving the override is what
    makes the happy path work."""
    payload = _x_dapr_secret_payload()
    loader, _ = _loader_for(payload, vendor_overrides=None)
    app = create_app(settings=_BASE_SETTINGS, providers=_build_providers(loader=loader))
    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/connectors:register",
            json={"imageRef": IMAGE_REF},
            headers=_ctx_header(),
        )
    assert response.status_code == 422, response.text
    assert response.json()["error"]["code"] == "unknown-vendor-auth-type"


# ---------------------------------------------------------------------------
# Error mapping + guards
# ---------------------------------------------------------------------------


def test_register_malformed_image_ref_is_400() -> None:
    payload = _x_dapr_secret_payload()
    loader, _ = _loader_for(payload, vendor_overrides={"x-dapr-secret": IdentityCategory.KMS})
    app = create_app(settings=_BASE_SETTINGS, providers=_build_providers(loader=loader))
    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/connectors:register",
            json={"imageRef": "no-digest-here"},
            headers=_ctx_header(),
        )
    assert response.status_code == 400, response.text
    assert response.json()["error"]["code"] == "invalid-image-ref"


def test_register_requires_connector_register_permission() -> None:
    payload = _x_dapr_secret_payload()
    loader, _ = _loader_for(payload, vendor_overrides={"x-dapr-secret": IdentityCategory.KMS})
    app = create_app(settings=_BASE_SETTINGS, providers=_build_providers(loader=loader))
    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/connectors:register",
            json={"imageRef": IMAGE_REF},
            headers=_ctx_header(permissions=[CONNECTOR_READ]),
        )
    assert response.status_code == 403, response.text


def test_register_when_loader_unwired_is_500() -> None:
    """When CONN_CONNECTOR_REGISTRY_URL is unset the Loader is None and the
    route surfaces a startup-wiring error."""
    app = create_app(settings=_BASE_SETTINGS, providers=_build_providers(loader=None))
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.post(
            "/internal/v1/connectors:register",
            json={"imageRef": IMAGE_REF},
            headers=_ctx_header(),
        )
    assert response.status_code == 500


def test_register_rejects_unknown_body_fields() -> None:
    payload = _x_dapr_secret_payload()
    loader, _ = _loader_for(payload, vendor_overrides={"x-dapr-secret": IdentityCategory.KMS})
    app = create_app(settings=_BASE_SETTINGS, providers=_build_providers(loader=loader))
    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/connectors:register",
            json={"imageRef": IMAGE_REF, "unexpected": "x"},
            headers=_ctx_header(),
        )
    assert response.status_code == 422, response.text
    # Unknown fields go through the service-wide RequestValidationError
    # handler, which renders the canonical envelope (not FastAPI's default
    # ``{"detail": [...]}``).
    assert response.json()["error"]["code"] == "invalid-request"

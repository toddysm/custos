"""CONN-IMPL-029 (Phase K, #312) — ``/metrics`` endpoint surface.

Boots the FastAPI app via :func:`custos_connector.create_app`, hits
the mounted ``/metrics`` endpoint, and asserts:

* The endpoint returns HTTP 200 with Prometheus text content.
* All five Phase K metrics (``custos_connector_bind_latency_seconds``,
  ``custos_connector_active_leases``,
  ``custos_connector_pull_tick_duration_seconds``,
  ``custos_connector_cursor_lag_seconds``,
  ``custos_connector_manifest_fallback_total``) appear in the body
  even before any production code path has exercised them.
* The endpoint is mounted, not routed: it does not appear in the
  generated OpenAPI snapshot.

The fixtures below mirror ``tests/test_api_routes.py``'s lightweight
provider wiring so the test exercises the real ``create_app`` factory
without standing up Postgres.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient

from custos_connector import create_app
from custos_connector._telemetry import (
    CURSOR_LAG_REGISTRY,
    observe_bind,
    observe_pull_tick,
    record_lease_closed,
    record_lease_issued,
    record_manifest_fallback,
)
from custos_connector.identity import IdentityResolverRegistry
from custos_connector.instances.service import InstanceService
from custos_connector.providers import Providers
from custos_connector.sidecar_admin import InMemorySidecarRegistry
from tests._fakes import (
    FakeCatalogAdapter,
    FakeConnectorInstanceAdapter,
    FakeLeaseAdapter,
    FakeMetadataAdapter,
    build_bind_for_step_service,
    build_lease_manager,
)
from tests.test_api_routes import _BASE_SETTINGS, _StubPluginInvoker

# Re-use the providers fixture pattern from ``tests/test_api_routes.py``
# but keep it private to this module to avoid a cross-file dependency
# on the helper signature.


def _build_providers() -> Providers:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    metadata = FakeMetadataAdapter(applied_revisions={1, 2, 3, 4})
    leases = FakeLeaseAdapter(applied_revisions={1})
    registry = IdentityResolverRegistry(resolvers=[])
    bind = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        identity_registry=registry,
    )
    invoker = _StubPluginInvoker()
    instance_service = InstanceService(
        instance_store=instances,  # type: ignore[arg-type]
        catalog_store=catalog,  # type: ignore[arg-type]
        metadata_store=metadata,  # type: ignore[arg-type]
        plugin_invoker=invoker,  # type: ignore[arg-type]
        health_cache_ttl_seconds=60,
    )
    return Providers(
        catalog_store=catalog,  # type: ignore[arg-type]
        instance_store=instances,  # type: ignore[arg-type]
        lease_store=leases,  # type: ignore[arg-type]
        metadata_store=metadata,  # type: ignore[arg-type]
        identity_registry=registry,
        bind_for_step_service=bind,
        lease_manager=build_lease_manager(
            lease_store=leases,
            metadata_store=metadata,
        ),
        instance_service=instance_service,
        sidecar_admin_client=None,
        sidecar_registry=InMemorySidecarRegistry(),
    )


@contextmanager
def _client() -> Iterator[TestClient]:
    app = create_app(settings=_BASE_SETTINGS, providers=_build_providers())
    with TestClient(app) as c:
        yield c


def _touch_all_instruments() -> None:
    """Drive one sample / observation through every Phase K instrument.

    Required because the Prometheus exporter omits a series until it
    has at least one observation. The acceptance criterion is that
    all five metrics appear in the body, so the test fixtures must
    poke every instrument before scraping.
    """
    with observe_bind():
        pass
    with observe_pull_tick():
        pass
    record_lease_issued("metrics-test-instance")
    record_lease_closed("metrics-test-instance")
    record_manifest_fallback("used")
    CURSOR_LAG_REGISTRY.record_advanced(
        workspace_id="ws-metrics-test",
        instance_id="inst-metrics-test",
        advanced_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
    )


def test_metrics_endpoint_returns_prometheus_text() -> None:
    """``GET /metrics`` returns HTTP 200 with Prometheus exposition text."""
    with _client() as client:
        # Touch each instrument once AFTER the app is created so the
        # writes go through the SDK provider installed by
        # ``install_otel_providers()`` rather than a pre-install
        # proxy/no-op meter. Without these writes the exporter would
        # omit instruments that have never been observed.
        _touch_all_instruments()
        resp = client.get("/metrics")
    assert resp.status_code == 200
    # ``prometheus_client.make_asgi_app`` returns ``text/plain;
    # version=0.0.4; charset=utf-8`` — pin the prefix rather than the
    # full string so a future content-type bump does not break the
    # test.
    assert resp.headers["content-type"].startswith("text/plain")


@pytest.mark.parametrize(
    "metric_name",
    [
        "custos_connector_bind_latency_seconds",
        "custos_connector_active_leases",
        "custos_connector_pull_tick_duration_seconds",
        "custos_connector_cursor_lag_seconds",
        "custos_connector_manifest_fallback_total",
    ],
)
def test_metrics_endpoint_exposes_phase_k_named_metrics(metric_name: str) -> None:
    """Every Phase K named metric appears in the ``/metrics`` body.

    The acceptance contract on #312 freezes the metric names. This
    test pins them so any rename triggers a loud failure during
    review rather than silently breaking the Helm-shipped
    Prometheus scrape rules.
    """
    with _client() as client:
        # Touch the instruments AFTER the SDK provider is installed by
        # ``create_app()`` so the samples are recorded by the real
        # meter, not by the pre-install proxy meter.
        _touch_all_instruments()
        resp = client.get("/metrics")
    body = resp.text
    assert metric_name in body, f"metric {metric_name!r} missing from /metrics body"


def test_metrics_endpoint_is_not_in_openapi_snapshot() -> None:
    """``/metrics`` is a Starlette mount, not a router; it MUST NOT appear in OpenAPI.

    Mounts are excluded from the OpenAPI schema by FastAPI. Pinning
    this here prevents an accidental conversion to a router (which
    would change ``openapi.json`` and force the snapshot test to
    regenerate, and would also drag the Prometheus exposition
    response into the public API surface).
    """
    with _client() as client:
        schema = client.get("/openapi.json").json()
    assert "/metrics" not in schema["paths"]

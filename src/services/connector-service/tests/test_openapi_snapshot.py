"""OpenAPI snapshot test for the public REST surface (CONN-IMPL-026, #309).

The connector-service ships a committed
``src/services/connector-service/openapi.json`` snapshot so that:

* downstream code-gen (TypeScript / Go clients, contract tests) has a
  stable reference;
* PR review surfaces drift in the public REST shape rather than letting
  it slip silently into the runtime spec.

This test does two things:

1. Validates the *generated* spec against the OpenAPI 3.1 schema using
   :mod:`openapi_spec_validator`. This catches accidental breakage of
   the spec (bad references, malformed responses) before it reaches
   the snapshot file.
2. Diffs the *generated* spec against the committed snapshot. The
   error message tells reviewers how to regenerate the snapshot when
   the change is intentional.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient
from openapi_spec_validator import validate

from custos_connector import create_app
from custos_connector.identity import IdentityResolverRegistry
from custos_connector.instances.service import InstanceService
from custos_connector.providers import Providers
from custos_connector.runtime import HealthResult
from custos_connector.settings import Settings
from tests._fakes import (
    FakeCatalogAdapter,
    FakeConnectorInstanceAdapter,
    FakeLeaseAdapter,
    FakeMetadataAdapter,
    build_bind_for_step_service,
    build_lease_manager,
)

_SNAPSHOT_PATH = Path(__file__).resolve().parent.parent / "openapi.json"

_REGENERATE_HINT = (
    "Regenerate with:\n"
    "    cd src/services/connector-service\n"
    "    python -m tests.regen_openapi > openapi.json\n"
    "(see tests/test_openapi_snapshot.py for the script)."
)


class _StubInvoker:
    async def health(self, *, connector: object, instance: object) -> HealthResult:
        del connector, instance
        return HealthResult(healthy=True, detail=None, checked_at=datetime.now(UTC), extras={})


def _settings() -> Settings:
    return Settings(
        catalog_store_dsn="postgresql://u:p@h:5432/cat",
        metadata_store_dsn="postgresql://u:p@h:5432/meta",
        catalog_endpoint="http://catalog-service:8080",
        authz_endpoint="",
        oci_referrers_timeout_ms=5000,
        publish_max_body_mb=4,
        sidecar_default_ttl_sec=600,
        lease_max_concurrent=16,
        pull_loop_min_interval_sec=10,
        health_cache_ttl_s=60,
        sidecar_mtls_issuer=None,
        environment="development",
    )


def _providers() -> Providers:
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    metadata = FakeMetadataAdapter(applied_revisions={1, 2, 3, 4})
    leases = FakeLeaseAdapter(applied_revisions={1})
    registry = IdentityResolverRegistry(resolvers=[])
    return Providers(
        catalog_store=catalog,  # type: ignore[arg-type]
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
        instance_service=InstanceService(
            instance_store=instances,  # type: ignore[arg-type]
            catalog_store=catalog,  # type: ignore[arg-type]
            metadata_store=metadata,  # type: ignore[arg-type]
            plugin_invoker=_StubInvoker(),  # type: ignore[arg-type]
            health_cache_ttl_seconds=60,
        ),
    )


def _generate_spec() -> dict[str, object]:
    app = create_app(settings=_settings(), providers=_providers())
    with TestClient(app) as client:
        resp = client.get("/openapi.json")
    assert resp.status_code == 200
    return resp.json()  # type: ignore[no-any-return]


def test_openapi_spec_is_valid_openapi_3_1() -> None:
    """The generated spec passes the OpenAPI 3.1 schema validator."""
    spec = _generate_spec()
    # ``validate`` raises ``OpenAPIValidationError`` on a malformed spec.
    validate(spec)


def test_openapi_snapshot_matches_committed_file() -> None:
    """The generated spec equals the committed ``openapi.json`` snapshot.

    On intentional changes, regenerate the snapshot with the command
    in :data:`_REGENERATE_HINT`.
    """
    assert _SNAPSHOT_PATH.is_file(), f"missing snapshot: {_SNAPSHOT_PATH}. {_REGENERATE_HINT}"
    generated = json.dumps(_generate_spec(), indent=2, sort_keys=True)
    committed = _SNAPSHOT_PATH.read_text(encoding="utf-8").rstrip("\n")
    if generated != committed:
        # Truncate the diff so test output stays readable. The reviewer
        # can compare the two files directly.
        head_gen = "\n".join(generated.splitlines()[:20])
        head_com = "\n".join(committed.splitlines()[:20])
        raise AssertionError(
            "OpenAPI snapshot drift detected.\n"
            f"--- committed ({_SNAPSHOT_PATH.name}) ---\n{head_com}\n"
            "--- generated ---\n"
            f"{head_gen}\n"
            f"\n{_REGENERATE_HINT}"
        )

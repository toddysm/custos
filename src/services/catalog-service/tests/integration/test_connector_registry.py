"""Integration test: connector-type registry round-trip (CS-IMPL-021).

Per issue #222 scope: register a connector type, fetch it, list
versions, and deprecate.

Connector types are *global* (workspace-less) — the design surfaces
them under ``/v1/catalog/connector-types`` and the call requires
``catalog:connector-types:*`` without a workspace match.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from tests.integration.conftest import (
    admin_header,
    minimal_connector_manifest,
)

pytestmark = pytest.mark.integration


def test_connector_register_resolve_deprecate_round_trip(client: TestClient) -> None:
    # Register ---------------------------------------------------------
    reg = client.post(
        "/v1/catalog/connector-types",
        json={"manifest": minimal_connector_manifest()},
        headers=admin_header(),
    )
    assert reg.status_code == 201, reg.text
    ref = reg.json()
    assert ref["type"] == "oci-registry"
    assert ref["version"] == "1.0.0"
    assert ref["digest"].startswith("sha256:")

    # Get --------------------------------------------------------------
    get_resp = client.get(
        "/v1/catalog/connector-types/oci-registry@1.0.0",
        headers=admin_header(),
    )
    assert get_resp.status_code == 200, get_resp.text
    body = get_resp.json()
    assert body["type"] == "oci-registry"
    assert body["version"] == "1.0.0"
    assert body["parentDeprecated"] is False
    assert body["normalizedManifest"]["spec"]["capabilities"] == ["oci.pull"]

    # List -------------------------------------------------------------
    list_resp = client.get(
        "/v1/catalog/connector-types",
        params={"type": "oci-registry"},
        headers=admin_header(),
    )
    assert list_resp.status_code == 200, list_resp.text
    items = list_resp.json()["items"]
    assert len(items) == 1
    assert items[0]["version"] == "1.0.0"

    # RPC resolve (internal surface) -----------------------------------
    rpc_resp = client.get(
        "/rpc/v1/connector-types/oci-registry@1.0.0",
        headers=admin_header(),  # admin bundle includes catalog:rpc:read
    )
    assert rpc_resp.status_code == 200, rpc_resp.text
    assert rpc_resp.json()["type"] == "oci-registry"

    # Deprecate --------------------------------------------------------
    dep = client.post(
        "/v1/catalog/connector-types/oci-registry@1.0.0:deprecate",
        json={"reason": "migrated to oci-registry v2"},
        headers=admin_header(),
    )
    assert dep.status_code == 200, dep.text

    # Version row remains, but parent_deprecated flips.
    after = client.get(
        "/v1/catalog/connector-types/oci-registry@1.0.0",
        headers=admin_header(),
    )
    assert after.status_code == 200
    assert after.json()["parentDeprecated"] is True


def test_connector_register_idempotent_on_same_digest(client: TestClient) -> None:
    first = client.post(
        "/v1/catalog/connector-types",
        json={"manifest": minimal_connector_manifest()},
        headers=admin_header(),
    )
    assert first.status_code == 201
    second = client.post(
        "/v1/catalog/connector-types",
        json={"manifest": minimal_connector_manifest()},
        headers=admin_header(),
    )
    assert second.status_code == 201
    assert second.json()["digest"] == first.json()["digest"]


def test_connector_register_conflict_on_different_digest(client: TestClient) -> None:
    first = client.post(
        "/v1/catalog/connector-types",
        json={"manifest": minimal_connector_manifest()},
        headers=admin_header(),
    )
    assert first.status_code == 201
    # Same (type, version) but a different capability set → different
    # canonical hash → 409 conflict.
    second_manifest = minimal_connector_manifest()
    second_manifest["spec"]["capabilities"] = ["oci.pull", "oci.push"]
    second = client.post(
        "/v1/catalog/connector-types",
        json={"manifest": second_manifest},
        headers=admin_header(),
    )
    assert second.status_code == 409, second.text
    assert second.json()["error"]["code"].startswith("catalog.connector_")

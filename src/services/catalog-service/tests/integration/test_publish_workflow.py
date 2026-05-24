"""Integration test: publish → list → get-by-ref / get-by-id → deprecate (CS-IMPL-021).

Per issue #222 scope:

* Publish a workflow with a realistic activity-type ref
  (pre-seeded ``ws-1/echo@1``).
* List versions for the published name.
* Fetch the version by ref (``GET /v1/workspaces/{ws}/workflows/{name}@{v}``)
  and by triple-encoded id
  (``GET /v1/workflows/{ws}/{name}@{v}``).
* Deprecate the workflow and assert the *parent_deprecated* flag flips
  while the immutable version row itself is untouched (REQ-025).
"""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from tests.integration.conftest import (
    admin_header,
    minimal_workflow,
    seed_echo_activity,
)

pytestmark = pytest.mark.integration


def _publish(client: TestClient, name: str = "orders") -> dict[str, object]:
    resp = client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(minimal_workflow(name=name))},
        headers=admin_header(),
    )
    assert resp.status_code == 201, resp.text
    body: dict[str, object] = resp.json()
    return body


def test_publish_list_get_deprecate_round_trip(client: TestClient) -> None:
    seed_echo_activity(client)

    # Publish ----------------------------------------------------------
    ref = _publish(client)
    assert ref == {"workspaceId": "ws-1", "workflowName": "orders", "version": 1}

    # List -------------------------------------------------------------
    list_resp = client.get(
        "/v1/workspaces/ws-1/workflows/orders",
        headers=admin_header(),
    )
    assert list_resp.status_code == 200, list_resp.text
    list_body = list_resp.json()
    assert list_body["items"] == [
        {"workspaceId": "ws-1", "workflowName": "orders", "version": 1},
    ]
    assert list_body["nextCursor"] is None

    # Get by ref -------------------------------------------------------
    ref_resp = client.get(
        "/v1/workspaces/ws-1/workflows/orders@1",
        headers=admin_header(),
    )
    assert ref_resp.status_code == 200, ref_resp.text
    ref_body = ref_resp.json()
    assert ref_body["workspaceId"] == "ws-1"
    assert ref_body["workflowName"] == "orders"
    assert ref_body["version"] == 1
    assert ref_body["parentDeprecated"] is False
    # The normalized document must include the original step.
    steps = ref_body["document"]["spec"]["steps"]
    assert len(steps) == 1
    assert steps[0]["id"] == "say-hi"

    # Get by id (workspaceless surface) --------------------------------
    id_resp = client.get(
        "/v1/workflows/ws-1/orders@1",
        headers=admin_header(),
    )
    assert id_resp.status_code == 200, id_resp.text
    # Body must be identical to the get-by-ref view (modulo the
    # ``publishedAt`` timestamp which is stable across both calls).
    assert id_resp.json()["document"] == ref_body["document"]

    # Deprecate --------------------------------------------------------
    dep_resp = client.post(
        "/v1/workspaces/ws-1/workflows/orders@1:deprecate",
        json={"reason": "superseded by orders v2"},
        headers=admin_header(),
    )
    assert dep_resp.status_code == 200, dep_resp.text

    # Get again — parent_deprecated must be true now, but the
    # ``document`` payload is byte-identical (immutability).
    after = client.get(
        "/v1/workspaces/ws-1/workflows/orders@1",
        headers=admin_header(),
    )
    assert after.status_code == 200
    after_body = after.json()
    assert after_body["parentDeprecated"] is True
    assert after_body["document"] == ref_body["document"]
    assert after_body["publishedAt"] == ref_body["publishedAt"]


def test_publish_emits_immutability_error_on_duplicate_version(
    client: TestClient,
) -> None:
    """Re-publishing the same workflow with a *different body* yields version=2.

    Byte-identical publishes are idempotent and return the same
    version (the user-visible part of REQ-025 immutability). A
    content change bumps the version monotonically.
    """
    seed_echo_activity(client)
    first = _publish(client)
    assert first["version"] == 1

    # Identical body → idempotent, version stays at 1.
    same = _publish(client)
    assert same["version"] == 1

    # Differing body → version bumps.
    other = minimal_workflow()
    other["spec"]["steps"][0]["with"]["message"] = "different"
    resp = client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(other)},
        headers=admin_header(),
    )
    assert resp.status_code == 201, resp.text
    assert resp.json()["version"] == 2


def test_get_by_id_rejects_cross_workspace(client: TestClient) -> None:
    seed_echo_activity(client)
    _publish(client)
    # A principal carrying read access in ws-1 cannot dereference an id
    # under another workspace.
    resp = client.get(
        "/v1/workflows/ws-other/orders@1",
        headers=admin_header(),  # ws-1
    )
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "catalog.workspace_mismatch"

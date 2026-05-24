"""Integration test: extract template → materialize → canonical-hash round-trip (CS-IMPL-021).

Per issue #222 scope: publish workflow ``A``, extract template ``T``
with selectors, materialize ``T`` using the original placeholder
values, and assert the canonical hash of the resulting workflow
equals that of ``A`` (round-trip property — design § Operation:
Extract Template).

Notes
-----
The round-trip property is byte-equality at the **normalized document**
level, not the wire-level body, since publish stamps ``publishedAt``
and other server-controlled fields onto the version row. The test
re-extracts the underlying ``normalized_doc`` from the catalog API
response and computes the canonical hash.
"""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from custos_catalog.normalize import canonical_hash
from tests.integration.conftest import (
    admin_header,
    minimal_workflow,
    seed_echo_activity,
)

pytestmark = pytest.mark.integration


def test_template_round_trip_preserves_canonical_hash(
    client: TestClient,
) -> None:
    seed_echo_activity(client)

    # Step 1 — Publish workflow A. Use a "with.message" value distinct
    # from the placeholder default so the materialise binding round-trip
    # is observable.
    workflow_doc = minimal_workflow(name="orders")
    workflow_doc["spec"]["steps"][0]["with"]["message"] = "hello-prod"
    publish_resp = client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(workflow_doc)},
        headers=admin_header(),
    )
    assert publish_resp.status_code == 201, publish_resp.text
    a_ref = publish_resp.json()

    get_a = client.get(
        f"/v1/workspaces/ws-1/workflows/orders@{a_ref['version']}",
        headers=admin_header(),
    )
    assert get_a.status_code == 200
    a_doc = get_a.json()["document"]
    a_hash = canonical_hash(a_doc["spec"])

    # Step 2 — Extract template T pointing at the message field.
    extract_resp = client.post(
        f"/v1/workspaces/ws-1/workflows/orders@{a_ref['version']}:extractTemplate",
        json={
            "templateName": "orders-template",
            "selectors": [
                {
                    "path": "spec.steps[0].with.message",
                    "placeholderName": "msg",
                    "placeholderType": "string",
                    "required": True,
                },
            ],
        },
        headers=admin_header(),
    )
    assert extract_resp.status_code == 201, extract_resp.text
    t_ref = extract_resp.json()
    assert t_ref["templateName"] == "orders-template"

    # Step 3 — Materialise T using the original value. Round-trip
    # requires the binding to equal what was in workflow A.
    mat_resp = client.post(
        f"/v1/workspaces/ws-1/templates/orders-template@{t_ref['version']}:materialize",
        json={
            "targetName": "orders-rehydrated",
            "bindings": {"msg": "hello-prod"},
        },
        headers=admin_header(),
    )
    assert mat_resp.status_code == 201, mat_resp.text
    rehydrated_ref = mat_resp.json()

    # Step 4 — Re-fetch the materialised workflow and assert
    # canonical-hash equality against A.
    get_b = client.get(
        f"/v1/workspaces/ws-1/workflows/orders-rehydrated@{rehydrated_ref['version']}",
        headers=admin_header(),
    )
    assert get_b.status_code == 200
    b_doc = get_b.json()["document"]

    # The metadata.name fields differ by construction (different
    # workflow names). The round-trip property is over the *body* of
    # ``spec`` — extract metadata.name for separate equality. Per
    # design § Round-trip: "the *workflow body* after materialise is
    # byte-identical to the source", which is exactly the spec section.
    assert b_doc["spec"] == a_doc["spec"]

    # Sanity: an off-by-one binding breaks the property — confirming
    # the test is actually exercising the round-trip and not a
    # tautology.
    mat_resp_bad = client.post(
        f"/v1/workspaces/ws-1/templates/orders-template@{t_ref['version']}:materialize",
        json={
            "targetName": "orders-rehydrated-mismatch",
            "bindings": {"msg": "hello-stage"},
        },
        headers=admin_header(),
    )
    assert mat_resp_bad.status_code == 201
    bad_ref = mat_resp_bad.json()
    get_bad = client.get(
        f"/v1/workspaces/ws-1/workflows/orders-rehydrated-mismatch@{bad_ref['version']}",
        headers=admin_header(),
    )
    bad_doc = get_bad.json()["document"]
    assert canonical_hash(bad_doc["spec"]) != canonical_hash(a_doc["spec"])
    assert canonical_hash(b_doc["spec"]) == a_hash or b_doc["spec"] == a_doc["spec"]

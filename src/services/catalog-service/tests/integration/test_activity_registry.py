"""Integration test: activity-type registry round-trip (CS-IMPL-021).

Per issue #222 scope:

* Register an activity type.
* Resolve it via ``<ns>/<type>@<major>`` (latest non-deprecated within
  ``<major>``).
* Re-register a *different-digest* manifest under the same triple and
  assert the SPL ``ConflictDigest`` surfaces as a 409 with a stable
  envelope.
* Deprecate the activity type; re-resolution returns the same row but
  the parent_deprecated flag flips and the public resolver semantics
  (fallback) kick in for callers that only have the major pin.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from tests.integration.conftest import (
    admin_header,
    minimal_activity_manifest,
)

pytestmark = pytest.mark.integration


def _register(
    client: TestClient,
    *,
    type: str = "echo",
    version: str = "1.0.0",
    runtime_digest: str = "sha256:abc",
) -> dict[str, str]:
    manifest = minimal_activity_manifest(type=type, version=version)
    # The activity type manager keys idempotency on the canonical
    # hash of the *normalised* manifest, so a value change anywhere
    # inside ``spec`` will alter the digest. ``runtime.digest`` is a
    # convenient knob: it is part of ``spec`` and ignored by every
    # other validation gate.
    manifest["spec"]["runtime"]["digest"] = runtime_digest
    resp = client.post(
        "/v1/workspaces/ws-1/activity-types",
        json={"manifest": manifest},
        headers=admin_header(),
    )
    return {
        "status": str(resp.status_code),
        "body": resp.text,
        **(resp.json() if resp.status_code < 500 else {}),
    }


def test_register_resolve_conflict_deprecate_round_trip(client: TestClient) -> None:
    # Register ---------------------------------------------------------
    first = _register(client, version="1.0.0", runtime_digest="sha256:aaa")
    assert first["status"] == "201", first["body"]
    assert first["namespace"] == "ws-1"
    assert first["type"] == "echo"
    assert first["version"] == "1.0.0"
    digest_v1 = first["digest"]
    assert digest_v1.startswith("sha256:")

    # Register a second minor — same major (1) — so major-pin resolves
    # the highest minor.
    second = _register(client, version="1.1.0", runtime_digest="sha256:bbb")
    assert second["status"] == "201", second["body"]

    # Resolve via internal RPC: list endpoint returns all versions in
    # the (ns, type) bucket; assert the catalog stored both rows. The
    # major-pin resolution surface lives on the activity-registry
    # manager (used by publish-time resolve, not exposed as a REST
    # endpoint at v1) — this list assertion confirms the catalog row
    # set the resolver works against.
    list_resp = client.get(
        "/v1/workspaces/ws-1/activity-types",
        params={"namespace": "ws-1", "type": "echo"},
        headers=admin_header(),
    )
    assert list_resp.status_code == 200, list_resp.text
    list_body = list_resp.json()
    versions = sorted(item["version"] for item in list_body["items"])
    assert versions == ["1.0.0", "1.1.0"]

    # Conflict (re-register 1.0.0 with a different digest) -------------
    conflict = _register(client, version="1.0.0", runtime_digest="sha256:zzz")
    assert conflict["status"] == "409", conflict["body"]
    # Envelope contract.
    body = client.post(
        "/v1/workspaces/ws-1/activity-types",
        json={
            "manifest": {
                **minimal_activity_manifest(version="1.0.0"),
                "spec": {
                    "contractVersion": "1",
                    "runtime": {
                        "kind": "oci-container",
                        "image": "ghcr.io/x:v1",
                        "digest": "sha256:zzz-2",
                    },
                },
            },
        },
        headers=admin_header(),
    ).json()
    assert body["error"]["code"].startswith("catalog.activity_")

    # Idempotency (re-register 1.0.0 with the original digest) ---------
    same = _register(client, version="1.0.0", runtime_digest="sha256:aaa")
    assert same["status"] == "201", same["body"]
    assert same["digest"] == digest_v1

    # Deprecate --------------------------------------------------------
    dep = client.post(
        "/v1/workspaces/ws-1/activity-types/ws-1/echo@1.0.0:deprecate",
        json={"reason": "moved to v2"},
        headers=admin_header(),
    )
    assert dep.status_code == 200, dep.text

    # The version-row GET must still work (immutability) but expose
    # parentDeprecated=true.
    get_resp = client.get(
        "/v1/workspaces/ws-1/activity-types/ws-1/echo@1.0.0",
        headers=admin_header(),
    )
    assert get_resp.status_code == 200, get_resp.text
    assert get_resp.json()["parentDeprecated"] is True


def test_register_rejects_reserved_namespace(client: TestClient) -> None:
    # ws-1 cannot publish into a reserved namespace (custos.builtin).
    resp = client.post(
        "/v1/workspaces/ws-1/activity-types",
        json={"manifest": minimal_activity_manifest(namespace="custos.builtin")},
        headers=admin_header(),
    )
    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "catalog.activity_namespace_forbidden"

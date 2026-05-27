"""Tests for the internal lease RPC router (CONN-IMPL-019, Phase H/1).

Exercises ``POST /internal/v1/leases:{issue,refresh,release}`` end-to-end
through ``create_app`` so the dev-shim call-context middleware, the
``connector:lease-mint`` permission gate, the wire-model translation,
the LeaseError -> HTTP status map, and the 429-Retry-After header are
all under test.

The router is what the Phase H sidecar's ``LeaseGateway`` will call;
these tests pin the wire contract.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Any
from uuid import uuid4

from custos_spl.ids import ConnectorInstanceId, RunId, StepId
from fastapi.testclient import TestClient

from custos_connector import create_app
from custos_connector.identity import IdentityResolverRegistry
from custos_connector.middleware import CALLCTX_HEADER
from custos_connector.permissions import (
    ADMIN_CONNECTOR,
    CONNECTOR_LEASE_MINT,
)
from custos_connector.providers import Providers
from custos_connector.settings import Settings
from tests._fakes import (
    FakeCatalogAdapter,
    FakeConnectorInstanceAdapter,
    FakeLeaseAdapter,
    FakeMetadataAdapter,
    StubPluginBinder,
    build_bind_for_step_service,
    build_lease_manager,
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

_WORKSPACE = "ws-1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ctx_header(*, permissions: list[str] | None = None) -> dict[str, str]:
    """Build a dev-shim call-context header for the sidecar identity."""
    perms = permissions if permissions is not None else [CONNECTOR_LEASE_MINT]
    return {
        CALLCTX_HEADER: json.dumps(
            {
                "workspace_id": _WORKSPACE,
                "principal_id": "svc:connector-sidecar",
                "permissions": perms,
            }
        )
    }


def _build_providers(
    *,
    lease_store: FakeLeaseAdapter | None = None,
    metadata: FakeMetadataAdapter | None = None,
    max_concurrent: int = 16,
    clock: Any | None = None,
) -> Providers:
    """Wire :class:`Providers` with in-memory fakes for the router tests."""
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    metadata_store = metadata or FakeMetadataAdapter(applied_revisions={1, 2, 3, 4})
    leases = lease_store or FakeLeaseAdapter(applied_revisions={1})
    registry = IdentityResolverRegistry(resolvers=[])
    binder = StubPluginBinder()
    bind_service = build_bind_for_step_service(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata_store,
        identity_registry=registry,
        plugin_binder=binder,
    )
    return Providers(
        catalog_store=catalog,  # type: ignore[arg-type]
        instance_store=instances,  # type: ignore[arg-type]
        lease_store=leases,  # type: ignore[arg-type]
        metadata_store=metadata_store,  # type: ignore[arg-type]
        identity_registry=registry,
        bind_for_step_service=bind_service,
        lease_manager=build_lease_manager(
            lease_store=leases,
            metadata_store=metadata_store,
            max_concurrent=max_concurrent,
            clock=clock,
        ),
    )


def _issue_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "runId": "run-1",
        "stepId": "step-1",
        "attempt": 1,
        "slot": "source",
        "capability": "objects.read",
        "connectorInstanceId": str(uuid4()),
        "tokenType": "Bearer",
    }
    body.update(overrides)
    return body


@contextmanager
def _make_client(
    *,
    providers: Providers | None = None,
    settings: Settings = _BASE_SETTINGS,
) -> Iterator[TestClient]:
    """Wrap ``TestClient`` so the FastAPI lifespan (which wires
    ``app.state.providers``) fires for every test."""
    app = create_app(settings=settings, providers=providers or _build_providers())
    with TestClient(app) as client:
        yield client


# ---------------------------------------------------------------------------
# Issue
# ---------------------------------------------------------------------------


def test_issue_happy_path_returns_full_lease_envelope() -> None:
    with _make_client(providers=_build_providers()) as client:
        body = _issue_body()
        resp = client.post("/internal/v1/leases:issue", json=body, headers=_ctx_header())
        assert resp.status_code == 200, resp.text
        envelope = resp.json()
        assert set(envelope.keys()) == {"lease"}
        lease = envelope["lease"]
        assert lease["workspaceId"] == _WORKSPACE
        assert lease["runId"] == body["runId"]
        assert lease["stepId"] == body["stepId"]
        assert lease["attempt"] == body["attempt"]
        assert lease["slot"] == body["slot"]
        assert lease["capability"] == body["capability"]
        assert lease["connectorInstanceId"] == body["connectorInstanceId"]
        assert lease["tokenType"] == body["tokenType"]
        assert lease["leaseId"].startswith("lease_")
        # Datetimes round-trip via fromisoformat.
        assert datetime.fromisoformat(lease["issuedAt"]).tzinfo is not None
        issued = datetime.fromisoformat(lease["issuedAt"])
        expires = datetime.fromisoformat(lease["expiresAt"])
        assert expires > issued
        assert lease["releasedAt"] is None
        assert lease["revokedAt"] is None
        assert lease["revokeReason"] is None


def test_issue_capacity_exceeded_returns_429_with_retry_after() -> None:
    # cap=1: one issue succeeds, the second hits CAPACITY_EXCEEDED.
    with _make_client(providers=_build_providers(max_concurrent=1)) as client:
        body = _issue_body()
        first = client.post("/internal/v1/leases:issue", json=body, headers=_ctx_header())
        assert first.status_code == 200, first.text
        second = client.post("/internal/v1/leases:issue", json=body, headers=_ctx_header())
        assert second.status_code == 429, second.text
        assert second.json() == {
            "error": {
                "code": "CAPACITY_EXCEEDED",
                "detail": second.json()["error"]["detail"],
            }
        }
        assert "concurrent-lease cap reached" in second.json()["error"]["detail"]
        assert second.headers["Retry-After"] == "5"


def test_issue_invalid_request_returns_400() -> None:
    # step_deadline already in the past triggers INVALID_REQUEST from
    # the TTL precedence ladder; the LeaseManager raises *before* mint.
    with _make_client(providers=_build_providers()) as client:
        body = _issue_body(stepDeadline="2000-01-01T00:00:00+00:00")
        resp = client.post("/internal/v1/leases:issue", json=body, headers=_ctx_header())
        assert resp.status_code == 400, resp.text
        payload = resp.json()
        assert payload["error"]["code"] == "INVALID_REQUEST"
        assert "step_deadline" in payload["error"]["detail"]


def test_issue_extra_fields_are_rejected_422() -> None:
    # ``workspaceId`` in the body is forbidden — the workspace must come
    # from the call-context so the sidecar cannot forge a lease across
    # workspaces.
    with _make_client(providers=_build_providers()) as client:
        body = _issue_body()
        body["workspaceId"] = "other-ws"
        resp = client.post("/internal/v1/leases:issue", json=body, headers=_ctx_header())
        assert resp.status_code == 422, resp.text
        assert resp.json()["error"]["code"] == "invalid-request"


def test_issue_attempt_must_be_positive_422() -> None:
    with _make_client(providers=_build_providers()) as client:
        body = _issue_body(attempt=0)
        resp = client.post("/internal/v1/leases:issue", json=body, headers=_ctx_header())
        assert resp.status_code == 422, resp.text


def test_issue_without_lease_mint_permission_returns_403() -> None:
    with _make_client(providers=_build_providers()) as client:
        body = _issue_body()
        headers = _ctx_header(permissions=[ADMIN_CONNECTOR])
        resp = client.post("/internal/v1/leases:issue", json=body, headers=headers)
        assert resp.status_code == 403, resp.text


def test_issue_without_call_context_returns_401() -> None:
    with _make_client(providers=_build_providers()) as client:
        resp = client.post("/internal/v1/leases:issue", json=_issue_body())
        assert resp.status_code == 401, resp.text


def test_issue_uses_workspace_from_call_context_not_body() -> None:
    # Verify the issued lease is in the *caller's* workspace, regardless
    # of what the body might claim (since the body has no workspaceId).
    with _make_client(providers=_build_providers()) as client:
        resp = client.post("/internal/v1/leases:issue", json=_issue_body(), headers=_ctx_header())
        assert resp.status_code == 200
        assert resp.json()["lease"]["workspaceId"] == _WORKSPACE


# ---------------------------------------------------------------------------
# Refresh
# ---------------------------------------------------------------------------


def test_refresh_happy_path_extends_expiry_same_lease_id() -> None:
    with _make_client(providers=_build_providers()) as client:
        issued = client.post(
            "/internal/v1/leases:issue", json=_issue_body(), headers=_ctx_header()
        ).json()["lease"]
        resp = client.post(
            "/internal/v1/leases:refresh",
            json={"leaseId": issued["leaseId"], "requestedTtlSec": 1200},
            headers=_ctx_header(),
        )
        assert resp.status_code == 200, resp.text
        refreshed = resp.json()["lease"]
        assert refreshed["leaseId"] == issued["leaseId"]
        assert datetime.fromisoformat(refreshed["expiresAt"]) >= datetime.fromisoformat(
            issued["expiresAt"]
        )


def test_refresh_unknown_lease_returns_404() -> None:
    with _make_client(providers=_build_providers()) as client:
        resp = client.post(
            "/internal/v1/leases:refresh",
            json={"leaseId": "lease_DOES_NOT_EXIST"},
            headers=_ctx_header(),
        )
        assert resp.status_code == 404, resp.text
        assert resp.json()["error"]["code"] == "NOT_FOUND"


def test_refresh_released_lease_returns_410() -> None:
    with _make_client(providers=_build_providers()) as client:
        issued = client.post(
            "/internal/v1/leases:issue", json=_issue_body(), headers=_ctx_header()
        ).json()["lease"]
        rel = client.post(
            "/internal/v1/leases:release",
            json={"leaseId": issued["leaseId"]},
            headers=_ctx_header(),
        )
        assert rel.status_code == 204
        resp = client.post(
            "/internal/v1/leases:refresh",
            json={"leaseId": issued["leaseId"]},
            headers=_ctx_header(),
        )
        assert resp.status_code == 410, resp.text
        assert resp.json()["error"]["code"] == "ALREADY_RELEASED"


def test_refresh_without_lease_mint_permission_returns_403() -> None:
    with _make_client(providers=_build_providers()) as client:
        resp = client.post(
            "/internal/v1/leases:refresh",
            json={"leaseId": "lease_anything"},
            headers=_ctx_header(permissions=[ADMIN_CONNECTOR]),
        )
        assert resp.status_code == 403, resp.text


# ---------------------------------------------------------------------------
# Release
# ---------------------------------------------------------------------------


def test_release_returns_204_on_success() -> None:
    with _make_client(providers=_build_providers()) as client:
        issued = client.post(
            "/internal/v1/leases:issue", json=_issue_body(), headers=_ctx_header()
        ).json()["lease"]
        resp = client.post(
            "/internal/v1/leases:release",
            json={"leaseId": issued["leaseId"]},
            headers=_ctx_header(),
        )
        assert resp.status_code == 204, resp.text
        assert resp.content == b""


def test_release_unknown_lease_returns_204_idempotent() -> None:
    # The design says release is best-effort: callers should not
    # distinguish "found" from "unknown".
    with _make_client(providers=_build_providers()) as client:
        resp = client.post(
            "/internal/v1/leases:release",
            json={"leaseId": "lease_NEVER_ISSUED"},
            headers=_ctx_header(),
        )
        assert resp.status_code == 204, resp.text


def test_release_double_call_is_idempotent() -> None:
    with _make_client(providers=_build_providers()) as client:
        issued = client.post(
            "/internal/v1/leases:issue", json=_issue_body(), headers=_ctx_header()
        ).json()["lease"]
        for _ in range(2):
            resp = client.post(
                "/internal/v1/leases:release",
                json={"leaseId": issued["leaseId"]},
                headers=_ctx_header(),
            )
            assert resp.status_code == 204, resp.text


def test_release_capacity_slot_is_freed_after_release() -> None:
    # Issue → release → issue again succeeds even at cap=1: this is the
    # contract the sidecar relies on so an activity that returns its
    # lease can immediately ask for another.
    with _make_client(providers=_build_providers(max_concurrent=1)) as client:
        body = _issue_body()
        first = client.post("/internal/v1/leases:issue", json=body, headers=_ctx_header())
        assert first.status_code == 200, first.text
        rel = client.post(
            "/internal/v1/leases:release",
            json={"leaseId": first.json()["lease"]["leaseId"]},
            headers=_ctx_header(),
        )
        assert rel.status_code == 204
        second = client.post("/internal/v1/leases:issue", json=body, headers=_ctx_header())
        assert second.status_code == 200, second.text


def test_release_without_lease_mint_permission_returns_403() -> None:
    with _make_client(providers=_build_providers()) as client:
        resp = client.post(
            "/internal/v1/leases:release",
            json={"leaseId": "lease_anything"},
            headers=_ctx_header(permissions=[ADMIN_CONNECTOR]),
        )
        assert resp.status_code == 403, resp.text


# ---------------------------------------------------------------------------
# Cross-workspace isolation
# ---------------------------------------------------------------------------


def test_refresh_from_other_workspace_returns_404() -> None:
    # Lease issued in ws-1. Refresh from ws-2 must see NOT_FOUND, not
    # leak the lease across workspace boundaries.
    with _make_client(providers=_build_providers()) as client:
        issued = client.post(
            "/internal/v1/leases:issue", json=_issue_body(), headers=_ctx_header()
        ).json()["lease"]
        other_ctx = {
            CALLCTX_HEADER: json.dumps(
                {
                    "workspace_id": "ws-other",
                    "principal_id": "svc:connector-sidecar",
                    "permissions": [CONNECTOR_LEASE_MINT],
                }
            )
        }
        resp = client.post(
            "/internal/v1/leases:refresh",
            json={"leaseId": issued["leaseId"]},
            headers=other_ctx,
        )
        assert resp.status_code == 404, resp.text
        assert resp.json()["error"]["code"] == "NOT_FOUND"


# ---------------------------------------------------------------------------
# Wire-shape smoke: server-generated IDs round-trip the SPL Id newtypes.
# ---------------------------------------------------------------------------


def test_issue_payload_round_trips_through_spl_id_newtypes() -> None:
    # Sanity check that the router converts strings to ConnectorInstanceId
    # / RunId / StepId without breaking the LeaseManager's expectations.
    with _make_client(providers=_build_providers()) as client:
        body = _issue_body(
            runId=str(RunId("run-spl")),
            stepId=str(StepId("step-spl")),
            connectorInstanceId=str(ConnectorInstanceId(str(uuid4()))),
        )
        resp = client.post("/internal/v1/leases:issue", json=body, headers=_ctx_header())
        assert resp.status_code == 200, resp.text
        lease = resp.json()["lease"]
        assert lease["runId"] == body["runId"]
        assert lease["stepId"] == body["stepId"]


# ---------------------------------------------------------------------------
# Revoke (CONN-IMPL-020)
# ---------------------------------------------------------------------------


def test_revoke_single_lease_returns_revoked_status() -> None:
    """Happy path: issue + revoke → 200 with one ``revoked`` ack."""
    with _make_client(providers=_build_providers()) as client:
        issued = client.post(
            "/internal/v1/leases:issue", json=_issue_body(), headers=_ctx_header()
        ).json()["lease"]
        resp = client.post(
            "/internal/v1/leases:revoke",
            json={"leaseIds": [issued["leaseId"]], "reason": "rotation"},
            headers=_ctx_header(),
        )
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"results": [{"leaseId": issued["leaseId"], "status": "revoked"}]}


def test_revoke_unknown_lease_returns_not_found_status() -> None:
    with _make_client(providers=_build_providers()) as client:
        resp = client.post(
            "/internal/v1/leases:revoke",
            json={"leaseIds": ["lease_NOPE"], "reason": "x"},
            headers=_ctx_header(),
        )
        assert resp.status_code == 200
        assert resp.json() == {"results": [{"leaseId": "lease_NOPE", "status": "not-found"}]}


def test_revoke_already_revoked_returns_already_revoked_status() -> None:
    with _make_client(providers=_build_providers()) as client:
        issued = client.post(
            "/internal/v1/leases:issue", json=_issue_body(), headers=_ctx_header()
        ).json()["lease"]
        first = client.post(
            "/internal/v1/leases:revoke",
            json={"leaseIds": [issued["leaseId"]], "reason": "first"},
            headers=_ctx_header(),
        )
        assert first.json()["results"][0]["status"] == "revoked"
        second = client.post(
            "/internal/v1/leases:revoke",
            json={"leaseIds": [issued["leaseId"]], "reason": "second"},
            headers=_ctx_header(),
        )
        assert second.status_code == 200
        assert second.json()["results"][0]["status"] == "already-revoked"


def test_revoke_already_released_returns_already_expired_status() -> None:
    with _make_client(providers=_build_providers()) as client:
        issued = client.post(
            "/internal/v1/leases:issue", json=_issue_body(), headers=_ctx_header()
        ).json()["lease"]
        client.post(
            "/internal/v1/leases:release",
            json={"leaseId": issued["leaseId"]},
            headers=_ctx_header(),
        )
        resp = client.post(
            "/internal/v1/leases:revoke",
            json={"leaseIds": [issued["leaseId"]], "reason": "too-late"},
            headers=_ctx_header(),
        )
        assert resp.status_code == 200
        assert resp.json()["results"][0]["status"] == "already-expired"


def test_revoke_batch_preserves_input_order_and_mixed_statuses() -> None:
    with _make_client(providers=_build_providers()) as client:
        a = client.post(
            "/internal/v1/leases:issue", json=_issue_body(), headers=_ctx_header()
        ).json()["lease"]["leaseId"]
        b = client.post(
            "/internal/v1/leases:issue",
            json=_issue_body(stepId="step-2"),
            headers=_ctx_header(),
        ).json()["lease"]["leaseId"]
        # Pre-release `b` so it surfaces as `already-expired`.
        client.post(
            "/internal/v1/leases:release",
            json={"leaseId": b},
            headers=_ctx_header(),
        )
        resp = client.post(
            "/internal/v1/leases:revoke",
            json={"leaseIds": [a, b, "lease_missing"], "reason": "batch"},
            headers=_ctx_header(),
        )
        assert resp.status_code == 200
        results = resp.json()["results"]
        assert [r["leaseId"] for r in results] == [a, b, "lease_missing"]
        assert [r["status"] for r in results] == [
            "revoked",
            "already-expired",
            "not-found",
        ]


def test_revoke_empty_lease_ids_returns_422() -> None:
    with _make_client(providers=_build_providers()) as client:
        resp = client.post(
            "/internal/v1/leases:revoke",
            json={"leaseIds": [], "reason": "x"},
            headers=_ctx_header(),
        )
        assert resp.status_code == 422


def test_revoke_without_lease_mint_permission_returns_403() -> None:
    with _make_client(providers=_build_providers()) as client:
        resp = client.post(
            "/internal/v1/leases:revoke",
            json={"leaseIds": ["lease_x"], "reason": "x"},
            headers=_ctx_header(permissions=[ADMIN_CONNECTOR]),
        )
        assert resp.status_code == 403


def test_revoke_cross_workspace_isolation() -> None:
    """A revoke from another workspace must see ``not-found``, not leak existence."""
    with _make_client(providers=_build_providers()) as client:
        issued = client.post(
            "/internal/v1/leases:issue", json=_issue_body(), headers=_ctx_header()
        ).json()["lease"]
        other_ctx = {
            CALLCTX_HEADER: json.dumps(
                {
                    "workspace_id": "ws-other",
                    "principal_id": "svc:connector-sidecar",
                    "permissions": [CONNECTOR_LEASE_MINT],
                }
            )
        }
        resp = client.post(
            "/internal/v1/leases:revoke",
            json={"leaseIds": [issued["leaseId"]], "reason": "x"},
            headers=other_ctx,
        )
        assert resp.status_code == 200
        assert resp.json()["results"][0]["status"] == "not-found"

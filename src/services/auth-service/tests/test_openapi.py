"""Spec-validator gate for the public OpenAPI schema (AS-IMPL-024).

The design treats ``/openapi.json`` as the documented public surface for
the auth-service. We don't want a route definition (or a Pydantic model
quirk) to silently break the spec — a downstream client codegen pipeline
or an OpenAPI-Spec-aware gateway would only notice at deploy time.

This module asserts that the served schema is structurally valid OpenAPI
3.x using the ``openapi-spec-validator`` library, which is the reference
validator used by the OpenAPI Tools project.
"""

from __future__ import annotations

from fastapi.testclient import TestClient
from openapi_spec_validator import validate


def test_openapi_spec_is_valid(client: TestClient) -> None:
    resp = client.get("/openapi.json")
    assert resp.status_code == 200
    spec = resp.json()
    # ``validate`` raises on any structural issue, so a clean call is
    # an implicit assertion that the spec parses against the OpenAPI
    # 3.x meta-schema.
    validate(spec)


def test_openapi_spec_advertises_documented_routes(client: TestClient) -> None:
    """Sanity-check that the spec lists every M1 public-surface route.

    Mirrors the M1 slice of the design's "Public Interface" table —
    if a route is listed there but missing here, either the design or
    the impl has drifted. We pin only the M1 surface; ``/healthz`` and
    ``/readyz`` are excluded from the schema by ``include_in_schema``
    so they intentionally do not appear. Endpoints reserved for later
    milestones (login / logout / refresh, authz.check, etc.) are
    deliberately not asserted here so the gate doesn't fail before
    those handlers land.
    """
    resp = client.get("/openapi.json")
    spec = resp.json()
    paths = spec.get("paths", {})
    expected = {
        "/v1/auth/verify",
        "/v1/auth/login/oidc/callback",
        "/v1/authz/verify-and-authorize",
        "/.well-known/jwks.json",
        # AS-IMPL-025 internal RPC surface — the five method names
        # from the design's "Internal RPC" table, exposed under the
        # ``/rpc/`` prefix.
        "/rpc/authn.verifyToken",
        "/rpc/authz.authorize",
        "/rpc/authz.verifyAndAuthorize",
        "/rpc/callctx.sign",
        "/rpc/callctx.verify",
    }
    missing = expected - paths.keys()
    assert not missing, f"missing routes in OpenAPI spec: {sorted(missing)}"

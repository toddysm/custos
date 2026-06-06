"""Tests for the Request Validator (AGW-IMPL-011)."""

from __future__ import annotations

import pytest

from custos_gateway.errors import GatewayError, GatewayErrorCode
from custos_gateway.middleware.validate import (
    PUBLISH_ROUTE_SUFFIXES,
    RouteClass,
    classify_route,
    enforce_body_size,
    enforce_content_type,
    is_json_media_type,
    is_publish_route,
    resolve_body_limit,
)
from custos_gateway.settings import Settings

# --- classify_route ----------------------------------------------------------


@pytest.mark.parametrize(
    "path,expected",
    [
        ("/v1/webhooks/github", RouteClass.WEBHOOK),
        ("/v1/webhooks/", RouteClass.WEBHOOK),
        ("/v1/auth/login", RouteClass.AUTH_BOOTSTRAP),
        ("/v1/auth/login/device", RouteClass.AUTH_BOOTSTRAP),
        ("/v1/auth/login2", RouteClass.STANDARD),
        ("/v1/workspaces/ws_1/runs", RouteClass.STANDARD),
    ],
)
def test_classify_route(path: str, expected: RouteClass) -> None:
    assert classify_route(path) is expected


# --- is_json_media_type ------------------------------------------------------


@pytest.mark.parametrize(
    "content_type",
    [
        "application/json",
        "application/json; charset=utf-8",
        "APPLICATION/JSON",
        "application/vnd.custos.thing+json",
        "application/problem+json",
    ],
)
def test_json_media_types_accepted(content_type: str) -> None:
    assert is_json_media_type(content_type) is True


@pytest.mark.parametrize(
    "content_type",
    ["", "text/plain", "application/xml", "multipart/form-data", "application/jsonish"],
)
def test_non_json_media_types_rejected(content_type: str) -> None:
    assert is_json_media_type(content_type) is False


def test_missing_content_type_is_not_json() -> None:
    assert is_json_media_type(None) is False


# --- is_publish_route / resolve_body_limit -----------------------------------


@pytest.mark.parametrize(
    "route_template",
    [
        "/v1/workspaces/{workspaceId}/workflows",
        "/v1/workspaces/{workspaceId}/workflows/",
        "/v1/workspaces/{workspaceId}/templates",
    ],
)
def test_publish_routes_detected(route_template: str) -> None:
    assert is_publish_route(route_template) is True


@pytest.mark.parametrize(
    "route_template",
    [
        "/v1/workspaces/{workspaceId}/workflows/{id}",
        "/v1/workspaces/{workspaceId}/runs",
        "/v1/workspaces/{workspaceId}/activity-types",
    ],
)
def test_non_publish_routes_not_detected(route_template: str) -> None:
    assert is_publish_route(route_template) is False


def test_publish_suffixes_cover_workflows_and_templates() -> None:
    assert set(PUBLISH_ROUTE_SUFFIXES) == {"/workflows", "/templates"}


def test_resolve_body_limit_uses_publish_cap(settings: Settings) -> None:
    limit = resolve_body_limit("/v1/workspaces/{workspaceId}/workflows", settings)
    assert limit == settings.body_max_bytes_publish == 5_242_880


def test_resolve_body_limit_uses_default_cap(settings: Settings) -> None:
    limit = resolve_body_limit("/v1/workspaces/{workspaceId}/runs", settings)
    assert limit == settings.body_max_bytes_default == 1_048_576


# --- enforce_body_size -------------------------------------------------------


def test_enforce_body_size_admits_at_limit() -> None:
    enforce_body_size(1_000, 1_000)  # exactly at the cap is allowed


def test_enforce_body_size_rejects_over_limit() -> None:
    with pytest.raises(GatewayError) as excinfo:
        enforce_body_size(1_001, 1_000)
    error = excinfo.value
    assert error.code is GatewayErrorCode.BODY_TOO_LARGE
    assert error.status == 413
    assert error.extras == {"limitBytes": 1_000, "bodyBytes": 1_001}


# --- enforce_content_type ----------------------------------------------------


@pytest.mark.parametrize("method", ["POST", "put", "Patch"])
def test_enforce_content_type_requires_json_on_write(method: str) -> None:
    with pytest.raises(GatewayError) as excinfo:
        enforce_content_type(method=method, content_type="text/plain")
    error = excinfo.value
    assert error.code is GatewayErrorCode.UNSUPPORTED_MEDIA_TYPE
    assert error.status == 415


def test_enforce_content_type_rejects_missing_content_type_on_write() -> None:
    with pytest.raises(GatewayError) as excinfo:
        enforce_content_type(method="POST", content_type=None)
    assert excinfo.value.code is GatewayErrorCode.UNSUPPORTED_MEDIA_TYPE


@pytest.mark.parametrize("content_type", ["application/json", "application/vnd.x+json"])
def test_enforce_content_type_admits_json_write(content_type: str) -> None:
    enforce_content_type(method="POST", content_type=content_type)


@pytest.mark.parametrize("method", ["GET", "HEAD", "DELETE", "OPTIONS"])
def test_enforce_content_type_skips_non_write_methods(method: str) -> None:
    # These methods don't carry a JSON request body the gateway validates, so a
    # non-JSON (or absent) content type is fine (DELETE included).
    enforce_content_type(method=method, content_type="text/plain")


@pytest.mark.parametrize("route_class", [RouteClass.WEBHOOK, RouteClass.AUTH_BOOTSTRAP])
def test_enforce_content_type_bypasses_raw_body_families(route_class: RouteClass) -> None:
    # Webhook (opaque) and auth-bootstrap (form-encoded) write bodies are exempt.
    enforce_content_type(
        method="POST",
        content_type="application/x-www-form-urlencoded",
        route_class=route_class,
    )

"""Tests for the OpenAPI 3.1 document emission (AGW-IMPL-017)."""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from custos_gateway.app import create_app
from custos_gateway.clients.auth import FakeAuthServiceClient
from custos_gateway.errors import PROBLEM_MEDIA_TYPE
from custos_gateway.openapi import (
    BEARER_SCHEME_NAME,
    OPENAPI_VERSION,
    PROBLEM_SCHEMA_NAME,
    WEBHOOK_SCHEME_NAME,
    X_IDEMPOTENT_EXTENSION,
    X_REQUIRED_PERMISSION_EXTENSION,
    annotate_paths,
    build_openapi,
    openapi_path,
)
from custos_gateway.routes.devicecode import DEVICE_CODE_START_PATH
from custos_gateway.routes.registry import M1_ROUTE_REGISTRY
from custos_gateway.routes.webhook import WEBHOOK_PATH
from custos_gateway.settings import Settings

_OPERATION_METHODS = {"get", "put", "post", "delete", "patch", "options", "head", "trace"}


def _spec(settings: Settings, auth_client: FakeAuthServiceClient) -> dict[str, Any]:
    return build_openapi(create_app(settings=settings, auth_client=auth_client))


def _operations(spec: dict[str, Any]) -> list[tuple[str, str, dict[str, Any]]]:
    out: list[tuple[str, str, dict[str, Any]]] = []
    for path, item in spec["paths"].items():
        for method, operation in item.items():
            if method.lower() in _OPERATION_METHODS:
                out.append((method.upper(), path, operation))
    return out


def test_openapi_version_is_3_1(settings: Settings, auth_client: FakeAuthServiceClient) -> None:
    assert _spec(settings, auth_client)["openapi"] == OPENAPI_VERSION
    assert OPENAPI_VERSION.startswith("3.1")


def test_security_schemes_present(settings: Settings, auth_client: FakeAuthServiceClient) -> None:
    schemes = _spec(settings, auth_client)["components"]["securitySchemes"]
    assert schemes[BEARER_SCHEME_NAME]["type"] == "http"
    assert schemes[BEARER_SCHEME_NAME]["scheme"] == "bearer"
    assert WEBHOOK_SCHEME_NAME in schemes


def test_problem_details_schema_registered(
    settings: Settings, auth_client: FakeAuthServiceClient
) -> None:
    schema = _spec(settings, auth_client)["components"]["schemas"][PROBLEM_SCHEMA_NAME]
    # The shared RFC 7807 envelope carries the locked taxonomy selector + type.
    assert schema["properties"]["code"]
    assert schema["properties"]["correlationId"]


def test_every_operation_carries_both_extensions(
    settings: Settings, auth_client: FakeAuthServiceClient
) -> None:
    for _method, _path, operation in _operations(_spec(settings, auth_client)):
        assert X_REQUIRED_PERMISSION_EXTENSION in operation
        assert X_IDEMPOTENT_EXTENSION in operation
        assert isinstance(operation[X_IDEMPOTENT_EXTENSION], bool)


def test_registry_operations_reflect_registry_attributes(
    settings: Settings, auth_client: FakeAuthServiceClient
) -> None:
    spec = _spec(settings, auth_client)
    by_key = {(m, p): op for m, p, op in _operations(spec)}
    for route in M1_ROUTE_REGISTRY:
        operation = by_key[(route.method.upper(), openapi_path(route.path))]
        assert operation[X_REQUIRED_PERMISSION_EXTENSION] == route.required_permission
        assert operation[X_IDEMPOTENT_EXTENSION] == route.requires_idempotency_key
        assert operation["security"] == [{BEARER_SCHEME_NAME: []}]


def test_webhook_operation_uses_webhook_scheme(
    settings: Settings, auth_client: FakeAuthServiceClient
) -> None:
    by_key = {(m, p): op for m, p, op in _operations(_spec(settings, auth_client))}
    operation = by_key[("POST", WEBHOOK_PATH)]
    assert operation["security"] == [{WEBHOOK_SCHEME_NAME: []}]
    assert operation[X_REQUIRED_PERMISSION_EXTENSION] is None
    assert operation[X_IDEMPOTENT_EXTENSION] is False


def test_device_code_route_is_anonymous(
    settings: Settings, auth_client: FakeAuthServiceClient
) -> None:
    by_key = {(m, p): op for m, p, op in _operations(_spec(settings, auth_client))}
    operation = by_key[("POST", DEVICE_CODE_START_PATH)]
    assert operation["security"] == []
    assert operation[X_REQUIRED_PERMISSION_EXTENSION] is None


def test_default_error_response_references_problem_schema(
    settings: Settings, auth_client: FakeAuthServiceClient
) -> None:
    for _method, _path, operation in _operations(_spec(settings, auth_client)):
        default = operation["responses"]["default"]
        ref = default["content"][PROBLEM_MEDIA_TYPE]["schema"]["$ref"]
        assert ref == f"#/components/schemas/{PROBLEM_SCHEMA_NAME}"


def test_health_probes_excluded_from_schema(
    settings: Settings, auth_client: FakeAuthServiceClient
) -> None:
    paths = _spec(settings, auth_client)["paths"]
    assert "/healthz" not in paths
    assert "/readyz" not in paths


def test_openapi_served_and_cached(settings: Settings, auth_client: FakeAuthServiceClient) -> None:
    app = create_app(settings=settings, auth_client=auth_client)
    with TestClient(app) as client:
        response = client.get("/openapi.json")
    assert response.status_code == 200
    body = response.json()
    assert body["openapi"] == OPENAPI_VERSION
    # A second build returns the cached document.
    assert build_openapi(app) is app.openapi_schema


def test_annotate_paths_skips_non_operation_keys() -> None:
    operation: dict[str, Any] = {}
    paths = {"/v1/anon": {"parameters": [], "get": operation}}
    annotate_paths(paths, {})
    # The non-operation "parameters" key is untouched; the operation is stamped.
    assert paths["/v1/anon"]["parameters"] == []
    assert X_REQUIRED_PERMISSION_EXTENSION in operation
    assert operation["security"] == []

"""OpenAPI 3.1 document emission for the Custos API Gateway (AGW-IMPL-017).

The gateway publishes a single OpenAPI 3.1 document at ``/openapi.json`` so the
SDK and CLI can be generated from one source of truth (see
``design/components/api-gateway/design.md`` § OpenAPI). FastAPI already
introspects the mounted routes into a base document; this module post-processes
that document to add the three gateway-specific contract elements the design
pins:

* **Security schemes.** :data:`BEARER_SCHEME_NAME` (``BearerAuth``) — the OIDC
  ID token or Custos service-account bearer every authenticated route carries —
  and :data:`WEBHOOK_SCHEME_NAME` (``WebhookNoAuth``) — the anonymous webhook
  ingress, which the gateway forwards verbatim without authentication (signature
  verification belongs to Trigger Service / the connector plugin). Each operation
  references the scheme that actually applies; the device-code bootstrap routes
  are explicitly unsecured (``security: []``).
* **Operation extensions.** Every operation carries
  :data:`X_REQUIRED_PERMISSION_EXTENSION` (``x-custos-required-permission``) and
  :data:`X_IDEMPOTENT_EXTENSION` (``x-custos-idempotent``), sourced from the
  declarative route registry (:data:`~custos_gateway.routes.registry.M1_ROUTE_REGISTRY`).
  Anonymous gateway-owned routes carry an explicit ``null`` permission and
  ``false`` idempotency so the two fields are present on *every* operation.
* **Shared error schema.** The RFC 7807 ``ProblemDetails`` envelope
  (:class:`~custos_gateway.errors.ProblemDetail`) is registered once under
  ``components.schemas`` and referenced as the ``default`` response of every
  operation, so the SDK sees one uniform error shape across the surface.

:func:`install_openapi` wires :func:`build_openapi` onto ``app.openapi`` so the
post-processed document is cached and served at ``/openapi.json``.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Final

from fastapi.openapi.utils import get_openapi

from custos_gateway.errors import PROBLEM_MEDIA_TYPE, ProblemDetail
from custos_gateway.routes.registry import M1_ROUTE_REGISTRY
from custos_gateway.routes.webhook import WEBHOOK_PATH

if TYPE_CHECKING:
    from fastapi import FastAPI

__all__ = [
    "BEARER_SCHEME_NAME",
    "OPENAPI_VERSION",
    "PROBLEM_SCHEMA_NAME",
    "WEBHOOK_SCHEME_NAME",
    "X_IDEMPOTENT_EXTENSION",
    "X_REQUIRED_PERMISSION_EXTENSION",
    "annotate_paths",
    "build_openapi",
    "install_openapi",
    "openapi_path",
]

#: The OpenAPI version the gateway pins (the document the SDK/CLI generate from).
OPENAPI_VERSION: Final[str] = "3.1.0"

#: Security-scheme names referenced by operations.
BEARER_SCHEME_NAME: Final[str] = "BearerAuth"
WEBHOOK_SCHEME_NAME: Final[str] = "WebhookNoAuth"

#: ``components.schemas`` key the shared RFC 7807 envelope is registered under.
PROBLEM_SCHEMA_NAME: Final[str] = "ProblemDetails"

#: Per-operation extension field names (design § OpenAPI).
X_REQUIRED_PERMISSION_EXTENSION: Final[str] = "x-custos-required-permission"
X_IDEMPOTENT_EXTENSION: Final[str] = "x-custos-idempotent"

#: Lower-case HTTP methods that name an Operation Object in a Path Item.
_OPERATION_METHODS: Final[frozenset[str]] = frozenset(
    {"get", "put", "post", "delete", "options", "head", "patch", "trace"}
)

#: Matches a Starlette path converter suffix inside a path parameter, e.g. the
#: ``:path`` in ``{workflowVersionId:path}`` — FastAPI strips it from the emitted
#: OpenAPI path, so the registry index must strip it too to match.
_CONVERTER_SUFFIX: Final[re.Pattern[str]] = re.compile(r"{([^}:]+):[^}]+}")


def openapi_path(path: str) -> str:
    """Normalize a route template to the path string FastAPI emits in OpenAPI.

    Strips Starlette path-converter suffixes inside path parameters
    (``{name:path}`` → ``{name}``) so a registry spec path matches the
    corresponding key under ``schema["paths"]``. Literal action suffixes such as
    ``{ref}:deprecate`` (the ``:deprecate`` is outside the braces) are untouched.
    """
    return _CONVERTER_SUFFIX.sub(r"{\1}", path)


#: The security schemes the gateway publishes.
_SECURITY_SCHEMES: Final[dict[str, dict[str, str]]] = {
    BEARER_SCHEME_NAME: {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT",
        "description": (
            "OIDC ID token or Custos service-account token. The gateway delegates "
            "verification and authorization to the Auth Service on every request."
        ),
    },
    WEBHOOK_SCHEME_NAME: {
        "type": "apiKey",
        "in": "header",
        "name": "Authorization",
        "description": (
            "Sentinel scheme for the anonymous webhook ingress. The gateway does "
            "not authenticate webhook callers and forwards the request verbatim; "
            "signature/HMAC verification is performed downstream by Trigger "
            "Service and the owning connector plugin. No gateway bearer is "
            "required on this route."
        ),
    },
}


def _registry_index() -> dict[tuple[str, str], tuple[str, bool]]:
    """Map ``(METHOD, path)`` to ``(required_permission, requires_idempotency_key)``."""
    return {
        (spec.method.upper(), openapi_path(spec.path)): (
            spec.required_permission,
            spec.requires_idempotency_key,
        )
        for spec in M1_ROUTE_REGISTRY
    }


def _problem_schema() -> dict[str, Any]:
    """The JSON schema for the shared RFC 7807 ``ProblemDetails`` envelope."""
    return ProblemDetail.model_json_schema(
        by_alias=True, ref_template="#/components/schemas/{model}"
    )


def _annotate_operation(
    operation: dict[str, Any],
    method: str,
    path: str,
    index: dict[tuple[str, str], tuple[str, bool]],
) -> None:
    """Stamp the security, ``x-custos-*`` extensions, and default error response."""
    registry_entry = index.get((method.upper(), path))
    if registry_entry is not None:
        permission, idempotent = registry_entry
        operation["security"] = [{BEARER_SCHEME_NAME: []}]
        operation[X_REQUIRED_PERMISSION_EXTENSION] = permission
        operation[X_IDEMPOTENT_EXTENSION] = idempotent
    elif path == WEBHOOK_PATH:
        operation["security"] = [{WEBHOOK_SCHEME_NAME: []}]
        operation[X_REQUIRED_PERMISSION_EXTENSION] = None
        operation[X_IDEMPOTENT_EXTENSION] = False
    else:
        # Device-code bootstrap + any other gateway-owned route: anonymous.
        operation["security"] = []
        operation[X_REQUIRED_PERMISSION_EXTENSION] = None
        operation[X_IDEMPOTENT_EXTENSION] = False

    responses = operation.setdefault("responses", {})
    responses.setdefault(
        "default",
        {
            "description": "Gateway error envelope (RFC 7807).",
            "content": {
                PROBLEM_MEDIA_TYPE: {
                    "schema": {"$ref": f"#/components/schemas/{PROBLEM_SCHEMA_NAME}"}
                }
            },
        },
    )


def annotate_paths(paths: dict[str, Any], index: dict[tuple[str, str], tuple[str, bool]]) -> None:
    """Stamp every operation under ``paths`` in place.

    Path Item Objects may also carry non-operation keys (``parameters``,
    ``summary``, ``$ref``); those are skipped.
    """
    for path, path_item in paths.items():
        for method, operation in path_item.items():
            if method.lower() not in _OPERATION_METHODS:
                continue
            _annotate_operation(operation, method, path, index)


def build_openapi(app: FastAPI) -> dict[str, Any]:
    """Build (and cache on ``app.openapi_schema``) the gateway's OpenAPI document.

    Generates the FastAPI base document, pins the OpenAPI version, injects the
    security schemes and the shared ``ProblemDetails`` schema, and stamps every
    operation with its security requirement and ``x-custos-*`` extensions sourced
    from the route registry.
    """
    if app.openapi_schema:
        return app.openapi_schema

    schema = get_openapi(
        title=app.title,
        version=app.version,
        openapi_version=OPENAPI_VERSION,
        description=app.description,
        routes=app.routes,
    )
    schema["openapi"] = OPENAPI_VERSION

    components = schema.setdefault("components", {})
    security_schemes = components.setdefault("securitySchemes", {})
    security_schemes.update(_SECURITY_SCHEMES)
    schemas = components.setdefault("schemas", {})
    schemas[PROBLEM_SCHEMA_NAME] = _problem_schema()

    annotate_paths(schema.get("paths", {}), _registry_index())

    app.openapi_schema = schema
    return schema


def install_openapi(app: FastAPI) -> None:
    """Wire :func:`build_openapi` onto ``app.openapi`` so it serves at ``/openapi.json``."""

    def _openapi() -> dict[str, Any]:
        return build_openapi(app)

    app.openapi = _openapi  # type: ignore[method-assign]

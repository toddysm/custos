"""Request Validator for the Custos API Gateway ingress (AGW-IMPL-011).

The gateway terminates the external HTTP surface, so it is the single place that
enforces request-shape limits before any body reaches a downstream component:

* **Body-size caps** — every request body is bounded. The default cap is 1 MB;
  the workflow/template *publish* routes are widened to 5 MB (both
  configurable via ``CUSTOS_GATEWAY_BODY_MAX_BYTES_*``). An oversized body is
  rejected ``413 body-too-large`` (:func:`enforce_body_size`).
* **Content-type enforcement** — a standard write route (`POST`/`PUT`/`PATCH`)
  must carry a JSON body; anything else is rejected ``415
  unsupported-media-type`` (:func:`enforce_content_type`).
* **Bypass-family detection** — webhook ingress and the auth-bootstrap login
  routes are anonymous, raw-body families: the gateway never inspects their
  payload, so they are dispatched around content-type enforcement
  (:func:`classify_route`).

The validator owns no request wiring; the request router (AGW-IMPL-016) calls
these primitives on the ingress path. They are deliberately small and pure so
they are trivial to unit-test and to reuse from the route registry (AGW-IMPL-012).
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Final

from custos_gateway.errors import GatewayError, GatewayErrorCode
from custos_gateway.middleware.auth import (
    AUTH_BOOTSTRAP_BYPASS_PREFIX,
    WEBHOOK_BYPASS_PREFIX,
)

if TYPE_CHECKING:
    from custos_gateway.settings import Settings

__all__ = [
    "PUBLISH_ROUTE_SUFFIXES",
    "WRITE_METHODS",
    "RouteClass",
    "classify_route",
    "enforce_body_size",
    "enforce_content_type",
    "is_json_media_type",
    "is_publish_route",
    "resolve_body_limit",
]

#: Methods that carry a request body the gateway validates for shape.
WRITE_METHODS: Final[frozenset[str]] = frozenset({"POST", "PUT", "PATCH"})

#: Route-template suffixes whose *collection* writes (workflow/template publish)
#: are granted the larger publish body cap. Matched against the route template
#: with any trailing slash stripped, so ``/v1/workspaces/{workspaceId}/workflows``
#: and ``.../templates`` qualify while their sub-resources fall back to the
#: default cap.
PUBLISH_ROUTE_SUFFIXES: Final[tuple[str, ...]] = ("/workflows", "/templates")


class RouteClass(StrEnum):
    """The ingress-handling family a request path belongs to.

    The router dispatches ``WEBHOOK`` and ``AUTH_BOOTSTRAP`` requests around the
    JSON content-type check — both are anonymous, raw-body families (webhook
    payloads are opaque to the gateway; the auth-bootstrap token/device routes
    are form-encoded). ``STANDARD`` requests run the full validation path.
    """

    WEBHOOK = "webhook"
    AUTH_BOOTSTRAP = "auth-bootstrap"
    STANDARD = "standard"


def classify_route(path: str) -> RouteClass:
    """Classify ``path`` into its ingress-handling family.

    Webhook ingress (``/v1/webhooks/...``) and the auth-bootstrap login routes
    (``/v1/auth/login`` and ``/v1/auth/login/...``) are the two bypass families;
    everything else is :attr:`RouteClass.STANDARD`. The login check matches the
    exact segment or a deeper sub-path so unrelated siblings (e.g.
    ``/v1/auth/login2``) are *not* treated as auth-bootstrap.
    """
    if path.startswith(WEBHOOK_BYPASS_PREFIX):
        return RouteClass.WEBHOOK
    if path == AUTH_BOOTSTRAP_BYPASS_PREFIX or path.startswith(AUTH_BOOTSTRAP_BYPASS_PREFIX + "/"):
        return RouteClass.AUTH_BOOTSTRAP
    return RouteClass.STANDARD


def is_json_media_type(content_type: str | None) -> bool:
    """Return ``True`` when ``content_type`` denotes a JSON payload.

    Matches ``application/json`` and any structured-syntax-suffix JSON media
    type (RFC 6839 ``+json``), e.g. ``application/vnd.custos.thing+json``,
    ignoring parameters such as ``; charset=utf-8``. A missing header (``None``)
    is treated as non-JSON.
    """
    if content_type is None:
        return False
    media_type = content_type.split(";", 1)[0].strip().lower()
    return media_type == "application/json" or media_type.endswith("+json")


def is_publish_route(route_template: str) -> bool:
    """Return whether ``route_template`` is a workflow/template publish route."""
    normalized = route_template.rstrip("/")
    return any(normalized.endswith(suffix) for suffix in PUBLISH_ROUTE_SUFFIXES)


def resolve_body_limit(route_template: str, settings: Settings) -> int:
    """Return the body-size cap (bytes) that applies to ``route_template``.

    Publish routes get :attr:`Settings.body_max_bytes_publish`; every other
    route gets :attr:`Settings.body_max_bytes_default`.
    """
    if is_publish_route(route_template):
        return settings.body_max_bytes_publish
    return settings.body_max_bytes_default


def enforce_body_size(body_size: int, limit: int) -> None:
    """Reject a body whose size exceeds ``limit`` with ``413 body-too-large``.

    Raises:
        GatewayError: ``body-too-large`` (413) when ``body_size`` exceeds
            ``limit``.
    """
    if body_size > limit:
        raise GatewayError(
            GatewayErrorCode.BODY_TOO_LARGE,
            detail=f"Request body of {body_size} bytes exceeds the {limit}-byte limit.",
            extras={"limitBytes": limit, "bodyBytes": body_size},
        )


def enforce_content_type(
    *,
    method: str,
    content_type: str | None,
    route_class: RouteClass = RouteClass.STANDARD,
) -> None:
    """Require a JSON body on standard write routes; bypass raw-body families.

    Webhook and auth-bootstrap requests carry opaque or form-encoded bodies and
    are exempt. A read method (no body) is exempt. A standard write request must
    declare a JSON content type; a missing (``None``) content type is rejected.

    Raises:
        GatewayError: ``unsupported-media-type`` (415) when a standard write
            request does not declare a JSON content type.
    """
    if route_class is not RouteClass.STANDARD:
        return
    if method.upper() not in WRITE_METHODS:
        return
    if not is_json_media_type(content_type):
        raise GatewayError(
            GatewayErrorCode.UNSUPPORTED_MEDIA_TYPE,
            detail="Write requests must use a JSON content type (application/json or +json).",
        )

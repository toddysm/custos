"""FastAPI router for the connector-type registration RPC (CONN-REG / #898).

Route ``POST /internal/v1/connectors:register`` is a service-to-service
surface that registers a connector *type* version from its OCI image. It
drives the :class:`~custos_connector.loader.Loader`, which discovers the
connector-manifest payload from the image, validates it, derives the
identity category, and persists the ``ConnectorTypeVersion`` row.

The image reference is **host-relative** — ``<repository>@sha256:<hex>`` —
because the registry host is fixed by the startup-configured client bound
to ``CONN_CONNECTOR_REGISTRY_URL`` (see
:func:`custos_connector.providers.load_registration_registry_client`). Tag
references are rejected by the Loader in v1.

The route is gated by
:data:`custos_connector.permissions.CONNECTOR_REGISTER`. Connector types
are platform-global, so no workspace is read from the body; the call
context is still required for authz.

Successful response: ``201`` with the registered
``{type, version, digest, imageRef, deprecated}`` row. Loader rejections
map onto stable 4xx/5xx envelopes via :data:`_STATUS_BY_CODE`.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Body, Depends, Request
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import JSONResponse

from custos_connector.api._common import error_response
from custos_connector.api.connector_types import _ConnectorTypeVersionWire, _row_to_wire
from custos_connector.loader import Loader, LoaderError, LoaderErrorCode
from custos_connector.middleware import CallContext, require_permission
from custos_connector.permissions import CONNECTOR_REGISTER

router = APIRouter(prefix="/internal/v1", tags=["connector-types"])


#: Maps each Loader rejection code onto an HTTP status. Client / manifest
#: problems are 4xx; upstream-registry problems are 502 (the connector
#: image registry, not this service, is at fault); (type, version) and
#: capability conflicts are 409. Unmapped codes fall back to 400.
_STATUS_BY_CODE: dict[LoaderErrorCode, int] = {
    LoaderErrorCode.INVALID_IMAGE_REF: 400,
    LoaderErrorCode.DISCOVERY_FAILED: 502,
    LoaderErrorCode.INVALID_ARTIFACT_MANIFEST: 422,
    LoaderErrorCode.PAYLOAD_FETCH_FAILED: 502,
    LoaderErrorCode.PAYLOAD_DIGEST_MISMATCH: 502,
    LoaderErrorCode.PAYLOAD_NOT_JSON: 422,
    LoaderErrorCode.MANIFEST_INVALID: 422,
    LoaderErrorCode.UNKNOWN_AUTHENTICATION_TYPE: 422,
    LoaderErrorCode.UNKNOWN_VENDOR_AUTH_TYPE: 422,
    LoaderErrorCode.CONFLICT_DIGEST: 409,
    LoaderErrorCode.CAPABILITY_REGRESSION: 409,
}


class _RegisterConnectorWire(BaseModel):
    """Wire shape for the registration request body."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    image_ref: str = Field(..., min_length=1, alias="imageRef")


def _resolve_loader(request: Request) -> Loader:
    """Pull the registration :class:`Loader` off ``app.state.providers``.

    ``None`` means the registration surface is not enabled
    (``CONN_CONNECTOR_REGISTRY_URL`` unset). That is a deployment
    configuration error for a route that is being called, so it surfaces
    as a startup-wiring :class:`RuntimeError` (HTTP 500), matching the
    sibling internal RPC routers.
    """
    providers = request.app.state.providers
    loader = getattr(providers, "loader", None)
    if loader is None:
        raise RuntimeError(
            "loader is not wired on app.state.providers; set "
            "CONN_CONNECTOR_REGISTRY_URL to enable the connector-type "
            "registration surface (CONN-REG / #898)"
        )
    assert isinstance(loader, Loader)
    return loader


@router.post("/connectors:register", response_model=_ConnectorTypeVersionWire, status_code=201)
async def register_connector(
    body: Annotated[_RegisterConnectorWire, Body()],
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(CONNECTOR_REGISTER))],
) -> JSONResponse:
    """Register a connector-type version from its OCI image reference."""
    loader = _resolve_loader(request)
    try:
        loaded = await loader.register(body.image_ref)
    except LoaderError as exc:
        return error_response(
            status_code=_STATUS_BY_CODE.get(exc.code, 400),
            code=str(exc.code),
            detail=exc.detail,
        )
    wire = _row_to_wire(loaded.row)
    return JSONResponse(status_code=201, content=wire.model_dump(by_alias=True))


__all__ = ["router"]

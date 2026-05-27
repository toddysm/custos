"""FastAPI router for the ``ValidateConnector`` internal RPC (CONN-IMPL-027).

Route ``POST /internal/v1/connectors:validate`` is a service-to-service
preflight surface. Two modes share one envelope:

* ``mode: "instance"`` — Workflow / Catalog service hands in an
  existing ``connectorInstanceId`` and (optionally) a
  ``requiredCapabilities`` array. The service re-runs
  :func:`validate_instance_config` against the *current* catalog
  manifest for the instance's pinned ``(type, version)`` so manifest
  drift surfaces as a pre-bind 400.

* ``mode: "manifest"`` — Catalog Service runs this from the
  operator-facing "test before save" UI, passing
  ``type`` + ``version`` + ``targetConfig`` +
  ``credentialsAuthentication`` + ``usedCapabilities`` directly.
  No persistence write.

The route is gated by
:data:`custos_connector.permissions.CONNECTOR_VALIDATE`. The
workspace identifier comes from the call context, never the body
(call-context is the single source of authority on workspace in the
internal RPC surface; see :class:`BindForStepService` for the same
pattern).

Successful response: ``200 {"ok": true, "type": "...", "version": "..."}``.
Lookup misses: ``404`` with code ``connector.instance_not_found`` or
``connector.instance_type_not_registered``. Validation failures:
``400`` with code ``connector.instance_config_invalid`` and a sibling
``issues`` array carrying every collected issue (matching the public
:func:`custos_connector.api.instances._validation_error_response`
shape so cross-surface tooling parses one schema).
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from fastapi import APIRouter, Body, Depends, Request
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import JSONResponse

from custos_connector.api._common import error_response
from custos_connector.instances.service import (
    ConnectorInstanceNotFound,
    ConnectorTypeNotRegistered,
)
from custos_connector.instances.validator import InstanceConfigValidationError
from custos_connector.middleware import CallContext, require_permission
from custos_connector.permissions import CONNECTOR_VALIDATE
from custos_connector.validate.service import (
    ValidateConnectorService,
    ValidateInstanceRequest,
    ValidateManifestRequest,
)

router = APIRouter(prefix="/internal/v1", tags=["validate"])


# ---------------------------------------------------------------------------
# Wire models
# ---------------------------------------------------------------------------


class _ValidateInstanceWire(BaseModel):
    """Wire shape for the instance-mode preflight body."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    mode: Literal["instance"]
    connector_instance_id: str = Field(..., min_length=1, alias="connectorInstanceId")
    required_capabilities: list[str] | None = Field(default=None, alias="requiredCapabilities")


class _ValidateManifestWire(BaseModel):
    """Wire shape for the manifest-mode preflight body."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    mode: Literal["manifest"]
    type: str = Field(..., min_length=1)
    version: str = Field(..., min_length=1)
    target_config: dict[str, Any] = Field(default_factory=dict, alias="targetConfig")
    credentials_authentication: dict[str, Any] = Field(
        default_factory=dict, alias="credentialsAuthentication"
    )
    used_capabilities: list[str] | None = Field(default=None, alias="usedCapabilities")


#: Discriminated-union request body. FastAPI / pydantic v2 routes the
#: JSON to the right shape via the ``mode`` field literal so the
#: handler does not branch on string equality.
_ValidateBody = Annotated[
    _ValidateInstanceWire | _ValidateManifestWire,
    Field(discriminator="mode"),
]


class _ValidateResponseWire(BaseModel):
    """Wire shape of the success response body."""

    model_config = ConfigDict(populate_by_name=True)

    ok: Literal[True] = True
    type: str
    version: str


# ---------------------------------------------------------------------------
# Service resolver + error mapping
# ---------------------------------------------------------------------------


def _resolve_service(request: Request) -> ValidateConnectorService:
    """Pull the :class:`ValidateConnectorService` off ``app.state.providers``."""
    providers = request.app.state.providers
    service = getattr(providers, "validate_service", None)
    if service is None:
        raise RuntimeError(
            "validate_service is not wired on app.state.providers; "
            "this is a startup wiring bug (CONN-IMPL-027)"
        )
    assert isinstance(service, ValidateConnectorService)
    return service


def _validation_error_response(exc: InstanceConfigValidationError) -> JSONResponse:
    """Render the full per-issue list in the canonical envelope.

    Identical wire shape to
    :func:`custos_connector.api.instances._validation_error_response`
    so any caller that already parses the public-instance-create
    400 body reuses one schema for the internal preflight 400.
    """
    issues_payload = [
        {"code": str(issue.code), "path": issue.path, "detail": issue.detail}
        for issue in exc.issues
    ]
    rendered = "; ".join(
        f"[{i.code}] {i.path}: {i.detail}" if i.path else f"[{i.code}] {i.detail}"
        for i in exc.issues
    )
    return JSONResponse(
        status_code=400,
        content={
            "error": {
                "code": "connector.instance_config_invalid",
                "detail": (
                    f"instance config validation failed: {rendered}"
                    if rendered
                    else "instance config validation failed"
                ),
                "issues": issues_payload,
            }
        },
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.post("/connectors:validate", response_model=_ValidateResponseWire)
async def validate_connector(
    body: Annotated[_ValidateBody, Body()],
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(CONNECTOR_VALIDATE))],
) -> JSONResponse:
    """Preflight a connector instance or manifest payload.

    Workspace comes from the call context — instance-mode lookup
    scopes the row to ``ctx.workspace_id`` and manifest mode runs
    against the platform-wide catalog (workspace still carried for
    audit attribution in a follow-up).
    """
    service = _resolve_service(request)
    if isinstance(body, _ValidateInstanceWire):
        service_request: ValidateInstanceRequest | ValidateManifestRequest = (
            ValidateInstanceRequest(
                mode="instance",
                instance_id=body.connector_instance_id,
                required_capabilities=(
                    tuple(body.required_capabilities)
                    if body.required_capabilities is not None
                    else None
                ),
            )
        )
    else:
        service_request = ValidateManifestRequest(
            mode="manifest",
            type=body.type,
            version=body.version,
            target_config=dict(body.target_config),
            credentials_authentication=dict(body.credentials_authentication),
            used_capabilities=(
                tuple(body.used_capabilities) if body.used_capabilities is not None else None
            ),
        )
    try:
        result = await service.validate(
            workspace_id=ctx.workspace_id,
            request=service_request,
        )
    except ConnectorInstanceNotFound as exc:
        return error_response(
            status_code=404,
            code=exc.code,
            detail=str(exc),
        )
    except ConnectorTypeNotRegistered as exc:
        return error_response(
            status_code=404,
            code=exc.code,
            detail=str(exc),
        )
    except InstanceConfigValidationError as exc:
        return _validation_error_response(exc)
    wire = _ValidateResponseWire(ok=True, type=result.type, version=result.version)
    return JSONResponse(status_code=200, content=wire.model_dump(by_alias=True))


__all__ = ["router"]

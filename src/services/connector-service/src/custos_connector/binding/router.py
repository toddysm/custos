"""FastAPI router for the ``BindForStep`` RPC.

The route lives under ``/internal/v1/bind-for-step`` to mark it as a
service-to-service surface (Workflow Service is the only caller per
the design § "Operation: Bind Multi-Connector Step"). The
call-context middleware still applies — every request must carry the
``X-Call-Context`` header — but the route is gated by a dedicated
:data:`~custos_connector.permissions.CONNECTOR_BIND` permission that
the Workflow Service's service identity holds.

Request bodies are validated by FastAPI/pydantic; the application
registers a :class:`fastapi.exceptions.RequestValidationError`
handler in :mod:`custos_connector` that renders the same
``{"error": {"code", "detail"}}`` envelope used by the rest of the
service, so wire-shape errors are indistinguishable from
middleware-level rejections on the wire. Domain errors raised by
:class:`BindForStepService` come through as :class:`BindError` and
are mapped to HTTP status codes via
:func:`custos_connector.binding.errors.http_status_for`.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import JSONResponse

from custos_connector.binding.errors import BindError, http_status_for
from custos_connector.binding.models import (
    BindForStepRequest,
    BindForStepResponse,
    BindSlotRequest,
)
from custos_connector.binding.service import BindForStepService
from custos_connector.middleware import CallContext, require_permission
from custos_connector.permissions import CONNECTOR_BIND

router = APIRouter(prefix="/internal/v1", tags=["binding"])


# ---------------------------------------------------------------------------
# Wire models — these mirror BindForStepRequest but live in router-land so the
# service layer can stay dataclass-only (decoupled from pydantic).
# ---------------------------------------------------------------------------


class _BindSlotWire(BaseModel):
    """Wire shape of a single slot in the ``BindForStep`` request."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1)
    instance_id: str = Field(..., min_length=1, alias="instanceId")
    required_capabilities: list[str] = Field(..., alias="requiredCapabilities")


class _BindForStepWire(BaseModel):
    """Wire shape of the ``BindForStep`` request body."""

    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(..., min_length=1, alias="runId")
    step_id: str = Field(..., min_length=1, alias="stepId")
    attempt: int = Field(..., ge=1)
    step_key: str = Field(..., min_length=1, alias="stepKey")
    slots: list[_BindSlotWire]


class _ConnectorContextWire(BaseModel):
    """Wire shape of a single resolved slot context."""

    endpoint: str
    token_type_hint: str | None = Field(default=None, alias="tokenTypeHint")
    handle: dict[str, object]
    extras: dict[str, object]

    model_config = ConfigDict(populate_by_name=True)


class _BindForStepResponseWire(BaseModel):
    """Wire shape of the ``BindForStep`` response body."""

    contexts: dict[str, _ConnectorContextWire]


# ---------------------------------------------------------------------------
# Adapters between the wire model and the dataclass model
# ---------------------------------------------------------------------------


def _to_service_request(
    wire: _BindForStepWire,
    *,
    actor: str,
) -> BindForStepRequest:
    return BindForStepRequest(
        run_id=wire.run_id,
        step_id=wire.step_id,
        attempt=wire.attempt,
        step_key=wire.step_key,
        slots=tuple(
            BindSlotRequest(
                name=s.name,
                instance_id=s.instance_id,
                required_capabilities=tuple(s.required_capabilities),
            )
            for s in wire.slots
        ),
        actor=actor,
    )


def _to_response_wire(response: BindForStepResponse) -> _BindForStepResponseWire:
    return _BindForStepResponseWire(
        contexts={
            slot: _ConnectorContextWire(
                endpoint=ctx.endpoint,
                tokenTypeHint=ctx.token_type_hint,
                handle=dict(ctx.handle),
                extras=dict(ctx.extras),
            )
            for slot, ctx in response.contexts.items()
        }
    )


# ---------------------------------------------------------------------------
# Service dependency resolver
# ---------------------------------------------------------------------------


def _resolve_service(request: Request) -> BindForStepService:
    """Pull the :class:`BindForStepService` off ``app.state.providers``."""
    providers = request.app.state.providers
    service = getattr(providers, "bind_for_step_service", None)
    if service is None:
        raise RuntimeError(
            "bind_for_step_service is not wired on app.state.providers; "
            "this is a startup wiring bug"
        )
    assert isinstance(service, BindForStepService)
    return service


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.post("/bind-for-step", response_model=_BindForStepResponseWire)
async def bind_for_step(
    body: _BindForStepWire,
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(CONNECTOR_BIND))],
) -> JSONResponse:
    """Resolve every connector slot a step references.

    Idempotent on ``(workspace_id, run_id, step_id, attempt)`` — see
    :class:`BindForStepService` for the cache semantics and the M1
    durability caveat.
    """
    service = _resolve_service(request)
    service_request = _to_service_request(body, actor=ctx.principal_id)
    try:
        response = await service.bind_for_step(
            workspace_id=ctx.workspace_id,
            request=service_request,
        )
    except BindError as exc:
        return JSONResponse(
            status_code=http_status_for(exc.code),
            content={"error": {"code": str(exc.code), "detail": exc.detail}},
        )
    wire = _to_response_wire(response)
    return JSONResponse(
        status_code=200,
        content=wire.model_dump(by_alias=True),
    )


__all__ = ["router"]

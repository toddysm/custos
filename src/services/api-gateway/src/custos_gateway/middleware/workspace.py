"""Workspace resolution for the Custos API Gateway (AGW-IMPL-006).

Workspace-scoped endpoints address their workspace in the URL path
(``/v1/workspaces/{workspaceId}/...``); the URL is *always authoritative*
(see ``design/components/api-gateway/design.md`` § "URL Shape and Workspace
Addressing"). The resolver extracts ``{workspaceId}`` from the path so the
downstream stages — ``authz.verifyAndAuthorize`` (AGW-IMPL-005) and the
call-context minter (AGW-IMPL-007) — operate on a single trusted value, and it
rejects requests whose body references a *different* workspace with
``400 workspace-mismatch`` (a programming error, not retryable).

The mechanism is a FastAPI dependency, :func:`resolve_workspace`: a scoped route
declares ``Depends(resolve_workspace)`` (ahead of the authz dependency) and the
dependency binds the resolved workspace to ``request.state.workspace_id`` — the
exact attribute the authz dependency already reads — and returns a
:class:`ResolvedWorkspace`. Unscoped routes (workspace discovery, ``/me``,
auth-bootstrap) carry no ``{workspaceId}`` path parameter and resolve to *no*
workspace cleanly; the body check is skipped because the URL implies no
workspace for the body to diverge from.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Final

from fastapi import Request

from custos_gateway.errors import GatewayError, GatewayErrorCode
from custos_gateway.middleware.validate import is_json_media_type

__all__ = [
    "WORKSPACE_ID_BODY_FIELD",
    "WORKSPACE_ID_PATH_PARAM",
    "WORKSPACE_STATE_ATTR",
    "ResolvedWorkspace",
    "resolve_workspace",
]

#: Path parameter that carries the workspace id on scoped routes.
WORKSPACE_ID_PATH_PARAM: Final[str] = "workspaceId"

#: Top-level JSON body field reconciled against the URL workspace. The URL wins;
#: a body that *names* a different workspace is a client bug, not a silent
#: override.
WORKSPACE_ID_BODY_FIELD: Final[str] = "workspaceId"

#: ``request.state`` attribute the resolved workspace id is bound to. The authz
#: dependency (:func:`custos_gateway.middleware.auth.require_permission`) reads
#: this attribute, so the resolver must run ahead of it on scoped routes.
WORKSPACE_STATE_ATTR: Final[str] = "workspace_id"


@dataclass(frozen=True, slots=True)
class ResolvedWorkspace:
    """The workspace a request resolved to.

    ``workspace_id`` is the URL-authoritative workspace for a scoped route, or
    ``None`` for an unscoped route (workspace discovery, ``/me``, auth-bootstrap)
    that carries no ``{workspaceId}`` segment.
    """

    workspace_id: str | None

    @property
    def is_scoped(self) -> bool:
        """Return ``True`` when the request addressed a concrete workspace."""
        return self.workspace_id is not None


def _is_json_content_type(content_type: str) -> bool:
    """Return ``True`` when ``content_type`` denotes a JSON payload.

    Thin alias over :func:`custos_gateway.middleware.validate.is_json_media_type`
    so the gateway has a single JSON media-type rule (the Request Validator,
    AGW-IMPL-011, owns it).
    """
    return is_json_media_type(content_type)


async def _body_workspace_id(request: Request) -> str | None:
    """Return a workspace id named in the JSON body, or ``None``.

    Only a JSON object body with a non-empty string ``workspaceId`` field yields
    a value. A non-JSON content type, an empty/unparsable body, a non-object
    payload, or a missing/blank field all resolve to ``None`` — there is nothing
    to reconcile against the URL. Reading the body caches it on the request, so
    the downstream router still sees it.
    """
    if not _is_json_content_type(request.headers.get("content-type", "")):
        return None
    body = await request.body()
    if not body:
        return None
    try:
        payload = json.loads(body)
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    value = payload.get(WORKSPACE_ID_BODY_FIELD)
    if isinstance(value, str) and value:
        return value
    return None


async def resolve_workspace(request: Request) -> ResolvedWorkspace:
    """Resolve the URL-authoritative workspace and reject body divergence.

    On a scoped route (a non-empty ``{workspaceId}`` path parameter) the
    dependency reconciles the body against the URL — a body naming a *different*
    workspace raises ``workspace-mismatch`` (400, not retryable) — then binds the
    URL workspace to ``request.state.workspace_id`` and returns it. On an
    unscoped route it binds nothing and returns a :class:`ResolvedWorkspace` with
    ``workspace_id=None``.
    """
    path_workspace = request.path_params.get(WORKSPACE_ID_PATH_PARAM)
    if not (isinstance(path_workspace, str) and path_workspace):
        return ResolvedWorkspace(workspace_id=None)

    body_workspace = await _body_workspace_id(request)
    if body_workspace is not None and body_workspace != path_workspace:
        raise GatewayError(
            GatewayErrorCode.WORKSPACE_MISMATCH,
            detail=(
                "Request body references a different workspace than the URL; "
                "the URL workspace is authoritative."
            ),
        )

    setattr(request.state, WORKSPACE_STATE_ATTR, path_workspace)
    return ResolvedWorkspace(workspace_id=path_workspace)

"""``CatalogClient`` production adapter — ``GetWorkflowVersion`` over Dapr (WF-IMPL-113).

The Start-Run validator (REQ-040) and the Run Controller resolve a
workflow version from the Catalog Service before compiling a run.
The narrow surface they depend on is the
:class:`~custos_workflow.runs.controller.CatalogClient` Protocol
(``async get_workflow_version(workspace_id, workflow_version_id)``);
until now the only implementations were in-memory test doubles and
the ``_NotConfiguredCatalogClient`` stub the lifespan wires by
default.

This module lands the production adapter,
:class:`DaprCatalogClient`, which calls the Catalog Service's
read-only RPC over Dapr Service Invocation:

    ``GET /rpc/v1/workflow-versions/{workflowVersionId}``

where ``workflowVersionId`` is the Catalog's triple-encoded handle
``<workspaceId>/<workflowName>@<version>`` (the opaque value the
Start-Run request already carries; the adapter forwards it
verbatim as a single ``:path`` segment).

The Catalog response envelope (``WorkflowVersionBody``) is mapped
onto the Workflow Service's
:class:`~custos_workflow.runs.controller.WorkflowVersion`:

==================  ====================================
WorkflowVersion     source
==================  ====================================
``id``              the requested ``workflow_version_id``
``workflow_id``     ``f"{workspaceId}/{workflowName}"``
``name``            ``workflowName``
``version_label``   ``str(version)``
``document``        ``WorkflowDocument.model_validate(document)``
==================  ====================================

Failure modes are normalised through the WF-IMPL-075
:class:`~custos_workflow.clients._errors.OutboundRpcError`
taxonomy, with one Catalog-specific addition:

* **HTTP 404** (the version does not exist, or is not visible to
  this workspace) → :class:`CatalogWorkflowVersionNotFound`, a
  :class:`LookupError` subclass. The Start-Run validator catches
  :class:`LookupError` and re-raises it as a
  :class:`~custos_workflow.validator.errors.WorkflowVersionNotFoundError`,
  so a missing version surfaces to the caller as a clean 404 rather
  than a transport error.
* **HTTP 499** (upstream cancelled) →
  :class:`~custos_workflow.clients._errors.OutboundRpcCancelledError`.
* **Any other non-2xx** →
  :class:`~custos_workflow.clients._errors.OutboundRpcStatusError`.
* **Transport failure** (no response observed) →
  :class:`~custos_workflow.clients._errors.OutboundRpcTransportError`.
* **Non-JSON / contract-violating body** →
  :class:`~custos_workflow.clients._errors.OutboundRpcDecodeError`.

As a defence-in-depth guard against cross-workspace leakage, a
response whose ``workspaceId`` does not match the requested
``workspace_id`` is treated as *not found* for the calling
workspace (:class:`CatalogWorkflowVersionNotFound`) — the Catalog
already enforces this server-side via the caller's token claims,
but the adapter never trusts a mismatched body.

Two test doubles ship alongside the adapter, mirroring the rest of
``custos_workflow.clients``:

* :class:`NoopCatalogClient` — the safe default the lifespan wires
  before the adapter is installed; every call
  :class:`NotImplementedError`-s.
* :class:`FakeCatalogClient` — an in-memory map of
  ``workflow_version_id → WorkflowVersion`` that raises
  :class:`CatalogWorkflowVersionNotFound` for an unknown id, used
  by unit tests to drive deterministic scenarios without Dapr.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final

import httpx

from custos_workflow.clients._dapr_invoke import (
    DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS,
    DaprInvokeEndpoint,
    build_invoke_url,
)

if TYPE_CHECKING:
    from custos_workflow.runs.controller import WorkflowVersion

__all__ = [
    "GET_WORKFLOW_VERSION_DAPR_METHOD_PREFIX",
    "CatalogWorkflowVersionNotFound",
    "DaprCatalogClient",
    "FakeCatalogClient",
    "NoopCatalogClient",
]

#: Dapr Service-Invocation ``method`` prefix for the Catalog
#: Service's read-only ``GetWorkflowVersion`` RPC. The triple-
#: encoded ``workflowVersionId`` handle is appended as a single
#: trailing ``:path`` segment:
#: ``rpc/v1/workflow-versions/<workspaceId>/<workflowName>@<version>``.
GET_WORKFLOW_VERSION_DAPR_METHOD_PREFIX: Final[str] = "rpc/v1/workflow-versions/"

#: HTTP status the Catalog returns (or the sidecar surfaces) when
#: the requested workflow version does not exist or is not visible
#: to the calling workspace. Mapped to
#: :class:`CatalogWorkflowVersionNotFound`.
_NOT_FOUND_STATUS: Final[int] = 404

#: HTTP status the Dapr sidecar surfaces when an upstream cancelled
#: the request (nginx-style ``client-closed-request``). Mapped to
#: :class:`~custos_workflow.clients._errors.OutboundRpcCancelledError`.
#: Mirrors :data:`custos_workflow.clients.trigger._CLIENT_CLOSED_REQUEST_STATUS`.
_CLIENT_CLOSED_REQUEST_STATUS: Final[int] = 499


# ---------------------------------------------------------------------------
# Catalog-specific not-found error
# ---------------------------------------------------------------------------


class CatalogWorkflowVersionNotFound(LookupError):
    """The Catalog has no workflow version for the requested handle.

    Subclasses :class:`LookupError` so the Start-Run validator's
    ``except LookupError`` arm maps it onto
    :class:`~custos_workflow.validator.errors.WorkflowVersionNotFoundError`
    without the adapter having to import the validator error
    taxonomy (which would couple the client layer to the validator
    and risk an import cycle).

    Carries the requested :attr:`workspace_id` /
    :attr:`workflow_version_id` so the validator's wrapped error
    and any log line can name exactly what was missing.
    """

    def __init__(self, workspace_id: str, workflow_version_id: str) -> None:
        self.workspace_id = workspace_id
        self.workflow_version_id = workflow_version_id
        super().__init__(
            f"Catalog has no workflow version {workflow_version_id!r} "
            f"visible to workspace {workspace_id!r}"
        )


# ---------------------------------------------------------------------------
# Wire → WorkflowVersion mapping
# ---------------------------------------------------------------------------


def _require_str(body: Mapping[str, Any], key: str) -> str:
    """Return ``body[key]`` as a non-empty string or raise a decode error."""
    from custos_workflow.clients._errors import OutboundRpcDecodeError

    value = body.get(key)
    if value is None:
        raise OutboundRpcDecodeError(
            f"Catalog GetWorkflowVersion response is missing the required {key!r} field"
        )
    if not isinstance(value, str) or not value:
        raise OutboundRpcDecodeError(
            f"Catalog GetWorkflowVersion response {key!r} must be a non-empty string, "
            f"got {type(value).__name__}"
        )
    return value


def _parse_workflow_version_response(
    body: Any,
    *,
    workspace_id: str,
    workflow_version_id: str,
) -> WorkflowVersion:
    """Map a Catalog ``WorkflowVersionBody`` onto a :class:`WorkflowVersion`.

    Any contract violation — non-object body, missing / wrong-typed
    field, or a ``document`` that fails
    :class:`~custos_workflow.document.models.WorkflowDocument`
    validation — surfaces as
    :class:`~custos_workflow.clients._errors.OutboundRpcDecodeError`
    so the failure is classified as a permanent contract violation
    rather than a transient transport error.

    A response whose ``workspaceId`` does not match the requested
    ``workspace_id`` raises :class:`CatalogWorkflowVersionNotFound`
    (defence-in-depth against cross-workspace leakage).
    """
    from custos_workflow.clients._errors import OutboundRpcDecodeError
    from custos_workflow.document.loader import DocumentParseError
    from custos_workflow.document.models import WorkflowDocument
    from custos_workflow.runs.controller import WorkflowVersion as _WorkflowVersion

    if not isinstance(body, Mapping):
        raise OutboundRpcDecodeError(
            "Catalog GetWorkflowVersion response body must be a JSON object, "
            f"got {type(body).__name__}"
        )

    body_workspace_id = _require_str(body, "workspaceId")
    workflow_name = _require_str(body, "workflowName")

    if body_workspace_id != workspace_id:
        # The Catalog enforces this server-side; never trust a body
        # that crosses workspaces.
        raise CatalogWorkflowVersionNotFound(workspace_id, workflow_version_id)

    version = body.get("version")
    if isinstance(version, bool) or not isinstance(version, int):
        raise OutboundRpcDecodeError(
            "Catalog GetWorkflowVersion response 'version' must be an integer, "
            f"got {type(version).__name__}"
        )

    document_raw = body.get("document")
    if not isinstance(document_raw, Mapping):
        raise OutboundRpcDecodeError(
            "Catalog GetWorkflowVersion response 'document' must be a JSON object, "
            f"got {type(document_raw).__name__}"
        )

    try:
        document = WorkflowDocument.model_validate(dict(document_raw))
    except (DocumentParseError, ValueError) as exc:
        raise OutboundRpcDecodeError(
            f"Catalog GetWorkflowVersion 'document' failed WorkflowDocument validation: {exc}"
        ) from exc

    return _WorkflowVersion(
        id=workflow_version_id,
        workflow_id=f"{body_workspace_id}/{workflow_name}",
        name=workflow_name,
        version_label=str(version),
        document=document,
    )


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class NoopCatalogClient:
    """Safe default that explicitly :class:`NotImplementedError`-s every call.

    Wired by the FastAPI lifespan at startup so the process does
    *not* silently resolve workflow versions before the real
    adapter (:class:`DaprCatalogClient`) is installed.
    """

    async def get_workflow_version(
        self, workspace_id: str, workflow_version_id: str
    ) -> WorkflowVersion:
        raise NotImplementedError(
            "NoopCatalogClient.get_workflow_version: no production CatalogClient "
            "adapter is wired yet (DaprCatalogClient, WF-IMPL-114)."
        )


@dataclass(slots=True)
class FakeCatalogClient:
    """In-memory :class:`CatalogClient` test double.

    Resolves ``workflow_version_id`` against an in-memory
    :attr:`versions` map and raises
    :class:`CatalogWorkflowVersionNotFound` for an unknown id —
    exactly the not-found contract :class:`DaprCatalogClient`
    presents — so the validator / controller tests can exercise
    both the happy path and the missing-version path without
    standing up Dapr.

    Every call is recorded on :attr:`calls` so tests can assert
    call patterns without monkey-patching.
    """

    versions: dict[str, WorkflowVersion] = field(default_factory=dict)
    calls: list[tuple[str, str]] = field(default_factory=list)

    async def get_workflow_version(
        self, workspace_id: str, workflow_version_id: str
    ) -> WorkflowVersion:
        self.calls.append((workspace_id, workflow_version_id))
        try:
            return self.versions[workflow_version_id]
        except KeyError:
            raise CatalogWorkflowVersionNotFound(workspace_id, workflow_version_id) from None


# ---------------------------------------------------------------------------
# Production adapter: Dapr Service-Invocation HTTP transport
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class DaprCatalogClient:
    """Production :class:`CatalogClient` adapter over Dapr Service Invocation.

    Issues a ``GET`` against
    ``…/v1.0/invoke/<catalog-app-id>/method/rpc/v1/workflow-versions/<id>``
    on the local Dapr sidecar, where ``<id>`` is the Catalog's
    triple-encoded ``workflow_version_id`` handle, forwarded
    verbatim as a single ``:path`` segment.

    The adapter does **not** own the :class:`httpx.AsyncClient` —
    the FastAPI lifespan hook builds and ``aclose``-es it, mirroring
    the other Dapr-backed adapters in this package.

    :param http_client: Lifespan-owned async HTTP client.
    :param endpoint: Resolved Dapr Service-Invocation endpoint for
        the Catalog Service app-id (built by
        :func:`~custos_workflow.clients._dapr_invoke.read_dapr_env`).
    :param timeout: Per-request timeout in seconds. Defaults to
        :data:`~custos_workflow.clients._dapr_invoke.DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS`.
    """

    http_client: httpx.AsyncClient
    endpoint: DaprInvokeEndpoint
    timeout: float = DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS

    async def get_workflow_version(
        self, workspace_id: str, workflow_version_id: str
    ) -> WorkflowVersion:
        """Fetch one workflow version through the Catalog RPC.

        Returns the mapped :class:`WorkflowVersion` on success.
        Failure modes are raised as documented on the module:
        :class:`CatalogWorkflowVersionNotFound` (HTTP 404),
        :class:`~custos_workflow.clients._errors.OutboundRpcCancelledError`
        (HTTP 499),
        :class:`~custos_workflow.clients._errors.OutboundRpcStatusError`
        (any other non-2xx),
        :class:`~custos_workflow.clients._errors.OutboundRpcTransportError`
        (no response observed), and
        :class:`~custos_workflow.clients._errors.OutboundRpcDecodeError`
        (non-JSON or contract-violating body).
        """
        from custos_workflow.clients._errors import (
            OutboundRpcCancelledError,
            OutboundRpcDecodeError,
            OutboundRpcStatusError,
            OutboundRpcTransportError,
        )

        method = f"{GET_WORKFLOW_VERSION_DAPR_METHOD_PREFIX}{workflow_version_id}"
        url = build_invoke_url(self.endpoint, method)

        try:
            response = await self.http_client.get(
                url,
                timeout=self.timeout,
                headers={"Accept": "application/json"},
            )
        except httpx.HTTPError as exc:
            raise OutboundRpcTransportError(
                f"Dapr GetWorkflowVersion transport failure: {exc!r}"
            ) from exc

        status_code = response.status_code
        if status_code == _NOT_FOUND_STATUS:
            raise CatalogWorkflowVersionNotFound(workspace_id, workflow_version_id)
        if status_code == _CLIENT_CLOSED_REQUEST_STATUS:
            raise OutboundRpcCancelledError(
                f"Dapr GetWorkflowVersion cancelled upstream (HTTP {status_code})"
            )
        if status_code // 100 != 2:
            body_preview = response.text[:200] if response.text else ""
            raise OutboundRpcStatusError(
                f"Dapr GetWorkflowVersion returned HTTP {status_code}: {body_preview!r}",
                status_code=status_code,
            )

        try:
            body = response.json()
        except ValueError as exc:
            raise OutboundRpcDecodeError(
                f"Dapr GetWorkflowVersion response is not valid JSON: {exc!r}"
            ) from exc

        return _parse_workflow_version_response(
            body,
            workspace_id=workspace_id,
            workflow_version_id=workflow_version_id,
        )

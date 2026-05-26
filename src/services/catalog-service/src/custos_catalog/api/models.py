"""Pydantic request/response models for the Catalog REST surface.

These models are wire types only. They never carry behaviour and never
hold internal state — manager-layer dataclasses (e.g.
:class:`custos_catalog.managers.definition.WorkflowVersionRef`) remain
the in-process truth, and the route handlers translate manager objects
into the models defined here.

Field names use camelCase to match the design's REST surface (see
``design/components/catalog-service/design.md`` § Public Interface and
the table of request/response shapes). Where a Catalog manager surface
uses snake_case, the conversion lives in the route function.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

# ---------------------------------------------------------------------------
# Shared envelopes
# ---------------------------------------------------------------------------


class _Wire(BaseModel):
    """Base for every wire model.

    ``populate_by_name`` lets handlers pass snake_case keyword arguments
    that map to the camelCase wire field via ``alias=`` (each model
    overrides individual fields as needed).
    """

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        frozen=True,
    )


class ErrorBody(_Wire):
    """The ``error`` payload of an :class:`ErrorResponse`."""

    code: str = Field(..., description="Stable machine-readable error code.")
    detail: str = Field(..., description="Human-readable explanation.")
    issues: list[dict[str, Any]] | None = Field(
        default=None,
        description=(
            "Structured per-field issues. Present on publish-time validation "
            "failures (schema / placeholders / CEL / resolver) and on "
            "manifest envelope failures."
        ),
    )


class ErrorResponse(_Wire):
    """Single error envelope shared by every 4xx / 5xx response."""

    error: ErrorBody

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "error": {
                        "code": "catalog.workflow_not_found",
                        "detail": "workflow 'orders' version 7 not found",
                    },
                },
            ],
        },
    )


class DeprecateRequest(_Wire):
    """POST body for every ``:deprecate`` action."""

    reason: str | None = Field(
        default=None,
        max_length=1024,
        description="Optional free-form deprecation reason (audit only).",
    )

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        json_schema_extra={"examples": [{"reason": "superseded by v8"}]},
    )


class StatusResponse(_Wire):
    """Plain ``{"status": "ok"}`` body returned by deprecate routes."""

    status: Literal["ok"] = "ok"


# ---------------------------------------------------------------------------
# Workflow / Template payloads
# ---------------------------------------------------------------------------


class DefinitionPublishRequest(_Wire):
    """POST body for workflow + template publish.

    The wire shape is intentionally permissive: ``definition`` is either
    the raw YAML/JSON string (preferred — preserves byte-identity for
    digesting) or a pre-parsed mapping. The route forwards the value
    untouched to the manager's parse stage; the schema validator is the
    single source of truth for shape errors.
    """

    definition: str | dict[str, Any] = Field(
        ...,
        description="Raw workflow/template body (YAML/JSON string or pre-parsed object).",
    )

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "definition": (
                        "apiVersion: custos.dev/v1\nkind: Workflow\n"
                        "metadata:\n  name: orders\nspec:\n  steps: []\n"
                    ),
                },
            ],
        },
    )


class WorkflowVersionRefBody(_Wire):
    """Wire form of :class:`WorkflowVersionRef`."""

    workspace_id: str = Field(..., alias="workspaceId")
    workflow_name: str = Field(..., alias="workflowName")
    version: int

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        frozen=True,
        json_schema_extra={
            "examples": [{"workspaceId": "ws-1", "workflowName": "orders", "version": 7}],
        },
    )


class WorkflowTemplateVersionRefBody(_Wire):
    """Wire form of :class:`WorkflowTemplateVersionRef`."""

    workspace_id: str = Field(..., alias="workspaceId")
    template_name: str = Field(..., alias="templateName")
    version: int

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        frozen=True,
        json_schema_extra={
            "examples": [{"workspaceId": "ws-1", "templateName": "etl", "version": 3}],
        },
    )


class WorkflowVersionBody(_Wire):
    """Wire form of an SPL ``WorkflowVersion`` row."""

    workspace_id: str = Field(..., alias="workspaceId")
    workflow_name: str = Field(..., alias="workflowName")
    version: int
    document: dict[str, Any]
    derived_from_template_version_id: str | None = Field(
        default=None, alias="derivedFromTemplateVersionId"
    )
    parent_deprecated: bool = Field(False, alias="parentDeprecated")
    published_at: datetime = Field(..., alias="publishedAt")


class WorkflowTemplateVersionBody(_Wire):
    """Wire form of an SPL ``WorkflowTemplateVersion`` row."""

    workspace_id: str = Field(..., alias="workspaceId")
    template_name: str = Field(..., alias="templateName")
    version: int
    document: dict[str, Any]
    derived_from_workflow_version_id: str | None = Field(
        default=None, alias="derivedFromWorkflowVersionId"
    )
    parent_deprecated: bool = Field(False, alias="parentDeprecated")
    published_at: datetime = Field(..., alias="publishedAt")


class WorkflowListResponse(_Wire):
    """List response for ``GET /v1/workspaces/{ws}/workflows/{name}``.

    Per design § Public Interface the response is ``[WorkflowVersionRef]``
    (refs only); callers fetch the full normalized ``document`` via the
    workspaced get-by-ref endpoint. Keeping the list payload small
    matters because workflow documents are kilobytes-scale normalized
    JSON, and authoring UIs commonly list-across many workflows.
    """

    items: list[WorkflowVersionRefBody]
    next_cursor: str | None = Field(default=None, alias="nextCursor")


class TemplateListResponse(_Wire):
    """List response for templates (kept as a separate type for OpenAPI)."""

    items: list[WorkflowTemplateVersionBody]
    next_cursor: str | None = Field(default=None, alias="nextCursor")


# ---------------------------------------------------------------------------
# Template extract / materialize
# ---------------------------------------------------------------------------


class SelectorBody(_Wire):
    """A single extraction selector (mirrors :class:`custos_catalog.extract.Selector`).

    Field names use camelCase on the wire to match the public REST
    surface; manager-side conversion happens in the route handler.
    """

    path: str = Field(
        ...,
        description="Dotted path into the workflow document (e.g. spec.steps[0].timeoutSeconds).",
    )
    placeholder_name: str = Field(
        ...,
        alias="placeholderName",
        min_length=1,
        description="Name of the resulting placeholder; must be unique across the selector set.",
    )
    placeholder_type: Literal[
        "connectorRef",
        "activityRef",
        "string",
        "integer",
        "number",
        "boolean",
        "json",
    ] = Field(..., alias="placeholderType")
    required: bool = True
    default: Any | None = None
    connector_type: str | None = Field(default=None, alias="connectorType")
    activity_type: str | None = Field(default=None, alias="activityType")
    description: str | None = None

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "path": "spec.steps[0].timeoutSeconds",
                    "placeholderName": "timeout",
                    "placeholderType": "integer",
                    "required": True,
                },
            ],
        },
    )


class ExtractTemplateRequest(_Wire):
    """POST body for ``:extractTemplate``."""

    selectors: list[SelectorBody]
    template_name: str = Field(..., alias="templateName", min_length=1)

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "selectors": [
                        {
                            "path": "spec.steps[0].timeoutSeconds",
                            "placeholderName": "timeout",
                            "placeholderType": "integer",
                        },
                    ],
                    "templateName": "orders-template",
                },
            ],
        },
    )


class MaterializeRequest(_Wire):
    """POST body for ``:materialize``."""

    bindings: dict[str, Any] = Field(default_factory=dict)
    target_name: str = Field(..., alias="targetName", min_length=1)

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        json_schema_extra={
            "examples": [
                {"bindings": {"timeout": 30}, "targetName": "orders-prod"},
            ],
        },
    )


# ---------------------------------------------------------------------------
# Activity types
# ---------------------------------------------------------------------------


class ActivityTypeRegisterRequest(_Wire):
    """POST body for ``/v1/workspaces/{ws}/activity-types``."""

    manifest: dict[str, Any]
    referrer_ref: str | None = Field(default=None, alias="referrerRef")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "manifest": {
                        "apiVersion": "custos.dev/v1",
                        "kind": "ActivityManifest",
                        "metadata": {
                            "namespace": "ws-1",
                            "type": "fetch-orders",
                            "version": "1.2.0",
                        },
                        "spec": {},
                    },
                    "referrerRef": "ghcr.io/example/activity@sha256:...",
                },
            ],
        },
    )


class ActivityTypeRefBody(_Wire):
    """Wire form of :class:`ActivityTypeRef`."""

    namespace: str
    type: str
    version: str
    digest: str


class ActivityTypeVersionBody(_Wire):
    """Wire form of an SPL ``ActivityTypeVersion`` row."""

    namespace: str
    type: str
    version: str
    digest: str
    normalized_manifest: dict[str, Any] = Field(..., alias="normalizedManifest")
    parent_deprecated: bool = Field(False, alias="parentDeprecated")
    published_at: datetime = Field(..., alias="publishedAt")


class ActivityTypeListResponse(_Wire):
    """List response for activity types.

    Per design § Public Interface, the list endpoint returns refs only;
    callers fetch the full ``normalizedManifest`` via the get-by-ref
    endpoint. Keeping the list payload small matters because authoring
    UIs commonly call list across many ``(namespace, type)`` pairs.
    """

    items: list[ActivityTypeRefBody]
    next_cursor: str | None = Field(default=None, alias="nextCursor")


# ---------------------------------------------------------------------------
# Connector types
# ---------------------------------------------------------------------------


class ConnectorTypeRegisterRequest(_Wire):
    """POST body for ``/v1/catalog/connector-types``."""

    image_ref: str = Field(..., alias="imageRef", min_length=1)
    manifest: dict[str, Any]

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "imageRef": (
                        "ghcr.io/custos/connector-oci-registry@sha256:"
                        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    ),
                    "manifest": {
                        "apiVersion": "custos.dev/connector-manifest/v1",
                        "kind": "ConnectorManifest",
                        "metadata": {"type": "oci-registry", "version": "2.3.1"},
                        "spec": {
                            "capabilities": ["oci.pull", "oci.push"],
                            "target": {"kind": "oci-registry", "endpoint": "https://ghcr.io"},
                            "credentials": {"authenticationType": "oidc"},
                        },
                    },
                },
            ],
        },
    )


class ConnectorTypeRefBody(_Wire):
    """Wire form of :class:`ConnectorTypeRef`."""

    type: str
    version: str
    digest: str


class ConnectorTypeVersionBody(_Wire):
    """Wire form of an SPL ``ConnectorTypeVersion`` row."""

    type: str
    version: str
    digest: str
    normalized_manifest: dict[str, Any] = Field(..., alias="normalizedManifest")
    parent_deprecated: bool = Field(False, alias="parentDeprecated")
    published_at: datetime = Field(..., alias="publishedAt")


class ConnectorTypeListResponse(_Wire):
    """List response for connector types.

    Per design § Public Interface, the list endpoint returns refs only;
    callers fetch the full ``normalizedManifest`` via the get-by-ref
    endpoint.
    """

    items: list[ConnectorTypeRefBody]
    next_cursor: str | None = Field(default=None, alias="nextCursor")


__all__ = [
    "ActivityTypeListResponse",
    "ActivityTypeRefBody",
    "ActivityTypeRegisterRequest",
    "ActivityTypeVersionBody",
    "ConnectorTypeListResponse",
    "ConnectorTypeRefBody",
    "ConnectorTypeRegisterRequest",
    "ConnectorTypeVersionBody",
    "DefinitionPublishRequest",
    "DeprecateRequest",
    "ErrorBody",
    "ErrorResponse",
    "ExtractTemplateRequest",
    "MaterializeRequest",
    "SelectorBody",
    "StatusResponse",
    "TemplateListResponse",
    "WorkflowListResponse",
    "WorkflowTemplateVersionBody",
    "WorkflowTemplateVersionRefBody",
    "WorkflowVersionBody",
    "WorkflowVersionRefBody",
]

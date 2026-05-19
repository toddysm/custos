"""Strongly-typed identifier aliases for SPL.

Each identifier is a `typing.NewType` wrapper around `str`. They cost
nothing at runtime (every value is just a string) but let mypy reject
calls that mix up two opaque IDs (e.g. passing a `RunId` where a
`StepId` is expected).

Identifier values are opaque strings. SPL does not constrain their
format — that is the responsibility of the issuing service (Workflow
Service for `RunId`, Auth Service for `PrincipalId`, etc.).
"""

from __future__ import annotations

from typing import NewType

WorkspaceId = NewType("WorkspaceId", str)
TenantId = NewType("TenantId", str)
PrincipalId = NewType("PrincipalId", str)

WorkflowId = NewType("WorkflowId", str)
WorkflowTemplateId = NewType("WorkflowTemplateId", str)

RunId = NewType("RunId", str)
StepId = NewType("StepId", str)

ArtifactId = NewType("ArtifactId", str)

SubscriptionId = NewType("SubscriptionId", str)
ConnectorInstanceId = NewType("ConnectorInstanceId", str)
ConnectorTypeId = NewType("ConnectorTypeId", str)
ActivityTypeId = NewType("ActivityTypeId", str)


__all__ = [
    "ActivityTypeId",
    "ArtifactId",
    "ConnectorInstanceId",
    "ConnectorTypeId",
    "PrincipalId",
    "RunId",
    "StepId",
    "SubscriptionId",
    "TenantId",
    "WorkflowId",
    "WorkflowTemplateId",
    "WorkspaceId",
]

"""Custos Storage Provider Layer.

See `design/components/storage-provider-layer/design.md` for the full contract.
"""

from importlib.metadata import PackageNotFoundError, version

from custos_spl.errors import (
    BackendUnavailable,
    ConflictDigest,
    ImmutableViolation,
    InvalidTransactionHandle,
    LeaseBusy,
    LeaseExpired,
    MigrationRequired,
    NotReserved,
    QueryUnsupported,
    SPLError,
    WorkspaceMismatch,
)
from custos_spl.ids import (
    ActivityTypeId,
    ArtifactId,
    ConnectorInstanceId,
    ConnectorTypeId,
    PrincipalId,
    RunId,
    StepId,
    SubscriptionId,
    TenantId,
    WorkflowId,
    WorkflowTemplateId,
    WorkspaceId,
)
from custos_spl.pagination import Cursor, Page

try:
    __version__ = version("custos_spl")
except PackageNotFoundError:
    __version__ = "0+unknown"

__all__ = [
    "ActivityTypeId",
    "ArtifactId",
    "BackendUnavailable",
    "ConflictDigest",
    "ConnectorInstanceId",
    "ConnectorTypeId",
    "Cursor",
    "ImmutableViolation",
    "InvalidTransactionHandle",
    "LeaseBusy",
    "LeaseExpired",
    "MigrationRequired",
    "NotReserved",
    "Page",
    "PrincipalId",
    "QueryUnsupported",
    "RunId",
    "SPLError",
    "StepId",
    "SubscriptionId",
    "TenantId",
    "WorkflowId",
    "WorkflowTemplateId",
    "WorkspaceId",
    "WorkspaceMismatch",
    "__version__",
]

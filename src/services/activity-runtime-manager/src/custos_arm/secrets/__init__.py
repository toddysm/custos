"""Secret Injector (ARM-IMPL-010).

Materializes connector credentials at ``/custos/in/secrets/<connector-slot>/
<key>`` (tmpfs, ``0400``) from the pre-resolved ``ConnectorContexts``, mints
the ``(runId, stepId, attempt)``-scoped ``/custos/in/sidecar-token``, and
proactively refreshes connector leases for long-running steps via the real
Dapr ``RefreshLease`` adapter against the Connector Service.
"""

from __future__ import annotations

from custos_arm.secrets.errors import (
    MissingConnectorError,
    MissingSecretError,
    SecretInjectorError,
)
from custos_arm.secrets.injector import SecretInjector
from custos_arm.secrets.lease import (
    ConnectorLeaseClient,
    ConnectorLeaseError,
    ConnectorUnavailableError,
    DaprConnectorLeaseClient,
    Lease,
    LeaseRefreshRejectedError,
)
from custos_arm.secrets.models import (
    SECRET_FILE_MODE,
    SECRETS_SUBDIR,
    SIDECAR_TOKEN_FILENAME,
    ConnectorContext,
    InjectionResult,
    SecretSink,
    SidecarToken,
)
from custos_arm.secrets.token import SidecarTokenMinter

__all__ = [
    "SECRETS_SUBDIR",
    "SECRET_FILE_MODE",
    "SIDECAR_TOKEN_FILENAME",
    "ConnectorContext",
    "ConnectorLeaseClient",
    "ConnectorLeaseError",
    "ConnectorUnavailableError",
    "DaprConnectorLeaseClient",
    "InjectionResult",
    "Lease",
    "LeaseRefreshRejectedError",
    "MissingConnectorError",
    "MissingSecretError",
    "SecretInjector",
    "SecretInjectorError",
    "SecretSink",
    "SidecarToken",
    "SidecarTokenMinter",
]

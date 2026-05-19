"""Provider interface contracts.

Populated by:
- SPL-003 — DefinitionStoreProvider + CatalogStoreProvider
- SPL-004 — MetadataStoreProvider (full surface)
- SPL-005 — ArtifactStoreProvider
- SPL-006 — AuthStoreProvider
- SPL-007 — LogQueryProvider + MetricsQueryProvider
"""

from custos_spl.interfaces.catalog_store import (
    ActivityTypeVersion,
    CatalogStoreProvider,
    ConnectorTypeVersion,
)
from custos_spl.interfaces.definition_store import (
    DefinitionListFilter,
    DefinitionStoreProvider,
    WorkflowTemplateVersion,
    WorkflowVersion,
)

__all__ = [
    "ActivityTypeVersion",
    "CatalogStoreProvider",
    "ConnectorTypeVersion",
    "DefinitionListFilter",
    "DefinitionStoreProvider",
    "WorkflowTemplateVersion",
    "WorkflowVersion",
]

"""Activity resolution — ``activityRef`` to pinned ``ActivityTypeVersion``.

The resolver is the first stage of an attempt: it turns the workflow's
fully-qualified, workspace-scoped activity reference into the immutable,
content-addressed type version (pinned image digest, input/output schemas,
connectors, resources, isolation floor) the Scheduler executes against, by
reading the Catalog Service over Dapr Service-Invocation.
"""

from __future__ import annotations

from custos_arm.resolve.errors import (
    ActivityUnresolvedError,
    CatalogUnavailableError,
    ResolveError,
)
from custos_arm.resolve.models import ActivityRef, ActivityTypeVersion
from custos_arm.resolve.resolver import (
    DEFAULT_RESOLVE_TIMEOUT_SECONDS,
    ActivityResolver,
    CatalogActivityResolver,
)

__all__ = [
    "DEFAULT_RESOLVE_TIMEOUT_SECONDS",
    "ActivityRef",
    "ActivityResolver",
    "ActivityTypeVersion",
    "ActivityUnresolvedError",
    "CatalogActivityResolver",
    "CatalogUnavailableError",
    "ResolveError",
]

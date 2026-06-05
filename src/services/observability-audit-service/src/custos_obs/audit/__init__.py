"""Audit subsystem for the Observability and Audit Service.

Houses the audit-outbox drain pipeline: the :mod:`drainer` reads batches off the
SPL audit outbox and the pipeline (later phases) dispatches them to the durable
audit store and the alerting matcher, each committing its own cursor.
"""

from __future__ import annotations

from custos_obs.audit.drainer import (
    DEFAULT_AUDIT_OUTBOX_BATCH_SIZE,
    AuditOutboxBatchHandler,
    AuditOutboxDrainer,
)

__all__ = [
    "DEFAULT_AUDIT_OUTBOX_BATCH_SIZE",
    "AuditOutboxBatchHandler",
    "AuditOutboxDrainer",
]

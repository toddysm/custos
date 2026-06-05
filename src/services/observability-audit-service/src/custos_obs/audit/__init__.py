"""Audit subsystem for the Observability and Audit Service.

Houses the audit-outbox drain pipeline: the :mod:`drainer` reads batches off the
SPL audit outbox and the :mod:`pipeline` dispatches them to the durable audit
store and the alerting matcher, each committing its own cursor.
"""

from __future__ import annotations

from custos_obs.audit.drainer import (
    DEFAULT_AUDIT_OUTBOX_BATCH_SIZE,
    AuditOutboxBatchHandler,
    AuditOutboxDrainer,
)
from custos_obs.audit.pipeline import (
    AUDIT_ALERT_PIPELINE_ID,
    AUDIT_STORE_PIPELINE_ID,
    AuditConsumer,
    AuditOutboxRowWriter,
    AuditPipeline,
    AuditStoreConsumer,
)
from custos_obs.audit.retention import (
    DEFAULT_RETENTION_PIPELINE_IDS,
    AuditRetentionStore,
    AuditRetentionWorker,
    RetentionResult,
)

__all__ = [
    "AUDIT_ALERT_PIPELINE_ID",
    "AUDIT_STORE_PIPELINE_ID",
    "DEFAULT_AUDIT_OUTBOX_BATCH_SIZE",
    "DEFAULT_RETENTION_PIPELINE_IDS",
    "AuditConsumer",
    "AuditOutboxBatchHandler",
    "AuditOutboxDrainer",
    "AuditOutboxRowWriter",
    "AuditPipeline",
    "AuditRetentionStore",
    "AuditRetentionWorker",
    "AuditStoreConsumer",
    "RetentionResult",
]

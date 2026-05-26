"""Revision-1 DDL for `ConnectorInstanceStoreProvider`.

Workspace-scoped (every row carries `workspace_id`). The composite PK
`(workspace_id, instance_id)` matches the workspace-scoping contract:
queries are constant-prefix filtered by `workspace_id` so leakage
across workspaces is structurally impossible at the SQL layer.

The `(type, version)` columns are NOT foreign-keyed to
`catalog.connector_type_version`. Catalog rows are platform-wide and
the connector instance store is a separate provider; cross-provider
foreign keys are explicitly excluded by SPL § Atomicity. The
connector-service domain layer validates the type/version exists in
the catalog at instance-create time.

Soft-state columns (`status`, `health_status`) are present in v1 even
though state-transition logic lands in CONN-IMPL-013 — keeping the
schema stable lets CONN-IMPL-013 ship without a follow-up migration.
"""

from __future__ import annotations

from custos_pg.migrations import Revision

CONNECTOR_INSTANCE_REV1 = Revision(
    number=1,
    statements=(
        "CREATE SCHEMA IF NOT EXISTS connector_instance",
        """
        CREATE TABLE IF NOT EXISTS connector_instance.connector_instance (
            workspace_id       TEXT        NOT NULL,
            instance_id        TEXT        NOT NULL,
            type               TEXT        NOT NULL,
            version            TEXT        NOT NULL,
            name               TEXT,
            lease_ttl_seconds  INTEGER,
            enabled            BOOLEAN     NOT NULL DEFAULT TRUE,
            status             TEXT        NOT NULL DEFAULT 'active',
            health_status      TEXT,
            created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (workspace_id, instance_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS connector_instance_by_created_at
            ON connector_instance.connector_instance
            (workspace_id, created_at DESC, instance_id ASC)
        """,
        """
        CREATE INDEX IF NOT EXISTS connector_instance_by_type
            ON connector_instance.connector_instance
            (workspace_id, type)
        """,
    ),
)


__all__ = ["CONNECTOR_INSTANCE_REV1"]

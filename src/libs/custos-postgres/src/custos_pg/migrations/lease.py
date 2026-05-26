"""Revision-1 DDL for `LeaseStoreProvider`.

Workspace-scoped (every row carries `workspace_id`). The composite PK
``(workspace_id, lease_id)`` mirrors the workspace-scoping contract:
queries are constant-prefix filtered by ``workspace_id`` so leakage
across workspaces is structurally impossible at the SQL layer.

The store carries the full set of columns required by Phase G/2 (issue
+ refresh + release + cap check) and also reserves
``revoked_at`` / ``revoke_reason`` for the Phase L operator revoke
flow (CONN-IMPL-028) so the schema stays stable when that lands
without a follow-up migration.

The secondary index ``lease_by_step_attempt`` powers the per-step-attempt
concurrent-lease cap (16 by default) — the Lease Manager runs a
constant-time ``COUNT(*) ... WHERE workspace_id = $1 AND run_id = $2
AND step_id = $3 AND attempt = $4 AND released_at IS NULL AND
expires_at > $5`` against this index.

``connector_instance_id`` is NOT foreign-keyed to
``connector_instance.connector_instance``. Cross-provider foreign keys
are explicitly excluded by SPL § Atomicity; the connector-service
domain layer holds the join.
"""

from __future__ import annotations

from custos_pg.migrations import Revision

LEASE_REV1 = Revision(
    number=1,
    statements=(
        "CREATE SCHEMA IF NOT EXISTS lease",
        """
        CREATE TABLE IF NOT EXISTS lease.lease (
            workspace_id            TEXT        NOT NULL,
            lease_id                TEXT        NOT NULL,
            run_id                  TEXT        NOT NULL,
            step_id                 TEXT        NOT NULL,
            attempt                 INTEGER     NOT NULL,
            slot                    TEXT        NOT NULL,
            capability              TEXT        NOT NULL,
            connector_instance_id   TEXT        NOT NULL,
            token_type              TEXT        NOT NULL,
            issued_at               TIMESTAMPTZ NOT NULL,
            expires_at              TIMESTAMPTZ NOT NULL,
            released_at             TIMESTAMPTZ,
            revoked_at              TIMESTAMPTZ,
            revoke_reason           TEXT,
            created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (workspace_id, lease_id)
        )
        """,
        # Concurrent-lease cap check: count non-released, non-expired
        # leases for a given (workspace, run, step, attempt). The
        # partial WHERE keeps the index narrow because the vast
        # majority of historical rows are released or expired.
        """
        CREATE INDEX IF NOT EXISTS lease_by_step_attempt
            ON lease.lease
            (workspace_id, run_id, step_id, attempt, expires_at)
            WHERE released_at IS NULL
        """,
        # Operator revoke flows (CONN-IMPL-028) scan by
        # connector_instance_id to find every lease backed by a
        # specific instance.
        """
        CREATE INDEX IF NOT EXISTS lease_by_connector_instance
            ON lease.lease
            (workspace_id, connector_instance_id)
            WHERE released_at IS NULL AND revoked_at IS NULL
        """,
        # Startup rehydration of in-memory cap counters: newest-first
        # paginated listing of currently-active leases.
        """
        CREATE INDEX IF NOT EXISTS lease_active_by_issued_at
            ON lease.lease
            (workspace_id, issued_at DESC, lease_id ASC)
            WHERE released_at IS NULL
        """,
    ),
)


__all__ = ["LEASE_REV1"]

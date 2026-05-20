"""Revision-1 DDL for `DefinitionStoreProvider`.

Schema:
  - `definition.workflow` and `definition.workflow_template` carry the
    parent rows whose `deprecated` flag is denormalized onto
    `WorkflowVersion.parent_deprecated` at fetch time.
  - `definition.workflow_version` and `definition.workflow_template_version`
    are the write-once version rows. Their PK enforces the
    `ImmutableViolation` contract — re-inserts surface as 23505 which
    the adapter classifies into `ImmutableViolation`.
  - Indices on `(workspace_id, workflow_id, published_at DESC)` keep
    `list_workflow_versions` (newest-first) cheap.

All version-row tables have a foreign key onto their parent so a row
cannot exist without its `Workflow` / `WorkflowTemplate`. The parent
table is autocreated on first `put_*_version` (see adapter).
"""

from __future__ import annotations

from custos_pg.migrations import Revision

DEFINITION_REV1 = Revision(
    number=1,
    statements=(
        "CREATE SCHEMA IF NOT EXISTS definition",
        """
        CREATE TABLE IF NOT EXISTS definition.workflow (
            workspace_id  TEXT        NOT NULL,
            workflow_id   TEXT        NOT NULL,
            deprecated    BOOLEAN     NOT NULL DEFAULT FALSE,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (workspace_id, workflow_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS definition.workflow_version (
            workspace_id                     TEXT        NOT NULL,
            workflow_id                      TEXT        NOT NULL,
            version                          TEXT        NOT NULL,
            normalized_doc                   JSONB       NOT NULL,
            derived_from_template_version_id TEXT,
            published_at                     TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (workspace_id, workflow_id, version),
            FOREIGN KEY (workspace_id, workflow_id)
                REFERENCES definition.workflow (workspace_id, workflow_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS workflow_version_by_published_at
            ON definition.workflow_version (workspace_id, workflow_id, published_at DESC)
        """,
        """
        CREATE TABLE IF NOT EXISTS definition.workflow_template (
            workspace_id  TEXT        NOT NULL,
            template_id   TEXT        NOT NULL,
            deprecated    BOOLEAN     NOT NULL DEFAULT FALSE,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (workspace_id, template_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS definition.workflow_template_version (
            workspace_id                     TEXT        NOT NULL,
            template_id                      TEXT        NOT NULL,
            version                          TEXT        NOT NULL,
            normalized_doc                   JSONB       NOT NULL,
            derived_from_workflow_version_id TEXT,
            published_at                     TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (workspace_id, template_id, version),
            FOREIGN KEY (workspace_id, template_id)
                REFERENCES definition.workflow_template (workspace_id, template_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS workflow_template_version_by_published_at
            ON definition.workflow_template_version (workspace_id, template_id, published_at DESC)
        """,
    ),
)


__all__ = ["DEFINITION_REV1"]

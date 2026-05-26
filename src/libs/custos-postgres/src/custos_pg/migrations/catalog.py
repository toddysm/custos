"""Revision-1 DDL for `CatalogStoreProvider`.

The catalog is platform-wide (no `workspace_id`). PKs:
  - `catalog.activity_type_version`: `(namespace, type, version)`
  - `catalog.connector_type_version`: `(type, version)`

The digest column on version rows is what the adapter inspects to
distinguish `ConflictDigest` (different digest, same key) from an
idempotent re-put (identical digest).
"""

from __future__ import annotations

from custos_pg.migrations import Revision

CATALOG_REV1 = Revision(
    number=1,
    statements=(
        "CREATE SCHEMA IF NOT EXISTS catalog",
        """
        CREATE TABLE IF NOT EXISTS catalog.activity_type (
            namespace   TEXT        NOT NULL,
            type        TEXT        NOT NULL,
            deprecated  BOOLEAN     NOT NULL DEFAULT FALSE,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (namespace, type)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS catalog.activity_type_version (
            namespace            TEXT        NOT NULL,
            type                 TEXT        NOT NULL,
            version              TEXT        NOT NULL,
            digest               TEXT        NOT NULL,
            normalized_manifest  JSONB       NOT NULL,
            published_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (namespace, type, version),
            FOREIGN KEY (namespace, type)
                REFERENCES catalog.activity_type (namespace, type)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS activity_type_version_by_published_at
            ON catalog.activity_type_version (namespace, type, published_at DESC)
        """,
        """
        CREATE TABLE IF NOT EXISTS catalog.connector_type (
            type        TEXT        NOT NULL,
            deprecated  BOOLEAN     NOT NULL DEFAULT FALSE,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (type)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS catalog.connector_type_version (
            type                 TEXT        NOT NULL,
            version              TEXT        NOT NULL,
            digest               TEXT        NOT NULL,
            normalized_manifest  JSONB       NOT NULL,
            published_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (type, version),
            FOREIGN KEY (type) REFERENCES catalog.connector_type (type)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS connector_type_version_by_published_at
            ON catalog.connector_type_version (type, published_at DESC)
        """,
    ),
)


CATALOG_REV2 = Revision(
    number=2,
    statements=(
        """
        ALTER TABLE catalog.connector_type_version
        ADD COLUMN IF NOT EXISTS image_ref TEXT
        """,
        """
        UPDATE catalog.connector_type_version
        SET image_ref = CONCAT('unresolved://', type, '@', digest)
        WHERE image_ref IS NULL OR image_ref = ''
        """,
        """
        ALTER TABLE catalog.connector_type_version
        ALTER COLUMN image_ref SET NOT NULL
        """,
    ),
)


__all__ = ["CATALOG_REV1", "CATALOG_REV2"]

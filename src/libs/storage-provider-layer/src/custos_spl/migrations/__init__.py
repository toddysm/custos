"""Migration runner and per-interface SQL revisions.

Populated by:
- SPL-009 — Audit Partition Enforcer DDL (this module: `audit`)
- SPL-011 — Migration runner and `custos migrate up` CLI (this module:
  `runner`, `cli`, `__main__`)
- SPL-017 — Postgres migrations rev 1-4 (Metadata 1-4, Definition 1, Catalog 1, Auth 1)
"""

from custos_spl.migrations.audit import (
    DEFAULT_AUDIT_RETENTION_ROLE,
    DEFAULT_AUDIT_SCHEMA,
    DEFAULT_PLATFORM_ROLE,
    audit_partition_ddl,
)
from custos_spl.migrations.runner import (
    MigrationCapable,
    check_revisions,
    required_revisions,
)

__all__ = [
    "DEFAULT_AUDIT_RETENTION_ROLE",
    "DEFAULT_AUDIT_SCHEMA",
    "DEFAULT_PLATFORM_ROLE",
    "MigrationCapable",
    "audit_partition_ddl",
    "check_revisions",
    "required_revisions",
]

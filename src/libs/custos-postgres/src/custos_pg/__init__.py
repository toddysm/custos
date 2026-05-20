"""Custos Postgres adapters for the Storage Provider Layer.

This package implements `DefinitionStoreProvider` and
`CatalogStoreProvider` against Postgres 14+ via `asyncpg`. See the
package README and `design/components/storage-provider-layer/design.md`
for the contract surface.

Adapters are discovered by SPL through the `custos_spl.adapters`
entry-point group; importers usually do not need to touch this package
directly.
"""

from __future__ import annotations

from custos_pg.adapters.catalog import PgCatalogAdapter
from custos_pg.adapters.catalog import make_adapter as make_catalog_adapter
from custos_pg.adapters.definition import (
    PgDefinitionAdapter,
)
from custos_pg.adapters.definition import (
    make_adapter as make_definition_adapter,
)

__all__ = [
    "PgCatalogAdapter",
    "PgDefinitionAdapter",
    "make_catalog_adapter",
    "make_definition_adapter",
]

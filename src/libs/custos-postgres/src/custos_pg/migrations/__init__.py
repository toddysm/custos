"""Shared types and helpers for `custos_pg.migrations`.

Each interface ships its DDL as a tuple of statements per revision.
`Revision` carries the per-revision integer label plus the SQL bundle
so adapters can iterate and call `record_revision` after each.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Revision:
    """A single forward-only revision step for one interface.

    Attributes:
        number: 1-based monotonically increasing label.
        statements: SQL strings executed in order inside the same
            transaction. Idempotent constructs (`CREATE TABLE IF NOT
            EXISTS`) are preferred so re-runs after partial failure
            do not error.
    """

    number: int
    statements: tuple[str, ...]


__all__ = ["Revision"]

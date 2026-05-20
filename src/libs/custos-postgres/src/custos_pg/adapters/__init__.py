"""Adapter package — `custos_pg.adapters`.

Each module exposes:
  - `PgXxxAdapter`: the asyncpg-backed implementation.
  - `make_adapter`: zero-arg factory used by the
    `custos_spl.adapters` entry-point group. The factory reads
    `CUSTOS_PG_DSN` and constructs a fresh pool; the SPL CLI invokes
    it once per process.
"""

from __future__ import annotations

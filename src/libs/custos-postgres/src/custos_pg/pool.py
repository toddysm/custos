"""Connection-pool helper for the Custos Postgres adapters.

Adapters take an injected `asyncpg.Pool` so unit tests can pass a stub
or a testcontainers-managed pool. In production, the entry-point
factory captures the DSN from `CUSTOS_PG_DSN` synchronously, and the
adapter creates the pool lazily on first async use — this is required
because the SPL CLI's adapter discovery is synchronous (factories are
plain callables) but pool construction must happen inside an event
loop.

The pool is owned by whoever constructs it; adapters do not close
pools they did not create.
"""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING

import asyncpg

if TYPE_CHECKING:
    from asyncpg.pool import Pool

DSN_ENV_VAR = "CUSTOS_PG_DSN"


def read_dsn_from_env() -> str:
    """Return the DSN from `CUSTOS_PG_DSN` or raise `RuntimeError`.

    The CLI catches `RuntimeError` and prints an operator-actionable
    message rather than leaking a traceback.
    """
    dsn = os.environ.get(DSN_ENV_VAR)
    if not dsn:
        raise RuntimeError(
            f"{DSN_ENV_VAR} is not set; cannot construct Postgres pool. "
            "Set it to a libpq DSN such as "
            "'postgresql://user:pw@host:5432/custos'."
        )
    return dsn


class LazyPool:
    """An `asyncpg.Pool` constructed on first await.

    Adapters call `get()` from every async method. The first call
    inside the running event loop builds the pool; subsequent calls
    return the same instance. Construction is guarded by an
    `asyncio.Lock` so concurrent first-use cannot race.
    """

    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 10) -> None:
        self._dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self._pool: Pool | None = None
        self._lock = asyncio.Lock()

    async def get(self) -> Pool:
        if self._pool is not None:
            return self._pool
        async with self._lock:
            if self._pool is None:
                self._pool = await asyncpg.create_pool(
                    dsn=self._dsn,
                    min_size=self._min_size,
                    max_size=self._max_size,
                )
                assert self._pool is not None
        return self._pool


__all__ = ["DSN_ENV_VAR", "LazyPool", "read_dsn_from_env"]

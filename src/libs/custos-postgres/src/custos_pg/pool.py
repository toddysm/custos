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
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

import asyncpg

if TYPE_CHECKING:
    from asyncpg.connection import Connection
    from asyncpg.pool import Pool

DSN_ENV_VAR = "CUSTOS_PG_DSN"

#: Per-connection initialiser. `asyncpg` invokes this once for every
#: connection the pool checks out from the underlying socket pool —
#: the standard place to register type codecs (e.g. JSONB <-> dict).
ConnectionInit = Callable[["Connection"], Awaitable[None]]


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

    Args:
        dsn: libpq-style connection string.
        min_size / max_size: passed through to `asyncpg.create_pool`.
        init: optional per-connection initialiser. Forwarded to
            `asyncpg.create_pool(init=...)`. Adapters use this to
            register type codecs (most importantly the JSONB <-> dict
            codec used by `PgAuthAdapter.put_role_binding` and the
            metadata adapter's run-error column).
    """

    def __init__(
        self,
        dsn: str,
        *,
        min_size: int = 1,
        max_size: int = 10,
        init: ConnectionInit | None = None,
    ) -> None:
        self._dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self._init = init
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
                    init=self._init,
                )
                assert self._pool is not None
        return self._pool


__all__ = ["DSN_ENV_VAR", "ConnectionInit", "LazyPool", "read_dsn_from_env"]

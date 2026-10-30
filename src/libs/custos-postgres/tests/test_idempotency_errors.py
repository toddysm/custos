"""Idempotency backend failures surface through SPL without hiding driver or pool errors."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest
from custos_spl.errors import BackendUnavailable

from custos_pg import PgMetadataAdapter


@pytest.mark.parametrize("operation", ["reserve", "complete", "cleanup"])
@pytest.mark.parametrize("stage", ["pool", "acquire", "statement"])
@pytest.mark.parametrize("error_type", [OSError, asyncpg.InterfaceError, asyncpg.PostgresError])
async def test_idempotency_backend_errors_are_classified(
    monkeypatch: pytest.MonkeyPatch, operation: str, stage: str, error_type: type[Exception]
) -> None:
    """Pool creation, acquisition and SQL faults preserve their cause in a stable SPL error."""
    error = error_type("private backend details")
    connection = MagicMock(spec=asyncpg.Connection)
    connection.fetchrow = AsyncMock(side_effect=error)
    connection.execute = AsyncMock(side_effect=error)
    pool = MagicMock(spec=asyncpg.Pool)
    pool.acquire.return_value.__aenter__ = AsyncMock(
        side_effect=error if stage == "acquire" else None, return_value=connection
    )
    adapter = PgMetadataAdapter(pool=pool)
    monkeypatch.setattr(
        adapter,
        "_pool_ref",
        AsyncMock(side_effect=error if stage == "pool" else None, return_value=pool),
    )
    with pytest.raises(BackendUnavailable) as failed:
        if operation == "reserve":
            await adapter.reserve_idempotency_record(
                "workspace", "operator", "route", "key", "hash", 60
            )
        elif operation == "complete":
            await adapter.complete_idempotency_record("workspace", "operator", "route", "key", {})
        else:
            await adapter.delete_expired_idempotency_records(datetime.now(UTC))
    assert failed.value.__cause__ is error

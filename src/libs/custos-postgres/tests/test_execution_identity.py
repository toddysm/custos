"""Live revision-14 constraints and old-writer safety for retained execution references."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from asyncpg import CheckViolationError
from asyncpg.pool import Pool
from custos_spl.ids import RunId, StepId, WorkspaceId
from custos_spl.interfaces.metadata_store import ResumeSubscription

from custos_pg.adapters.metadata import PgMetadataAdapter

pytestmark = pytest.mark.integration
IDENTITY = {"workspaceId": "ws", "principalId": "account", "bindingId": "original"}


@pytest.mark.parametrize(
    "value,valid",
    [
        (IDENTITY, True),
        ({**IDENTITY, "principalId": "a" * 120}, True),
        ({**IDENTITY, "principalId": "a" * 121}, False),
        ({**IDENTITY, "principalId": "\taccount"}, False),
        ({**IDENTITY, "principalId": "account\u00a0"}, False),
        ({**IDENTITY, "principalId": "\u202faccount"}, False),
        ({**IDENTITY, "principalId": "\x1caccount"}, False),
        ({**IDENTITY, "principalId": ""}, False),
        ({**IDENTITY, "workspaceId": "other"}, False),
        ({**IDENTITY, "token": "not-allowed"}, False),
        (None, False),
        ([], False),
    ],
)
async def test_sql_reference_matches_closed_python_contract(
    pg_pool: Pool, value: Any, valid: bool
) -> None:
    """Test actual SQL results, including exact length and non-ASCII whitespace boundaries."""
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    await adapter.check_execution_identity_ready()
    assert (
        await pg_pool.fetchval(
            "SELECT custos_state.valid_execution_identity($1, $2::jsonb)", "ws", json.dumps(value)
        )
        is valid
    )


@pytest.mark.parametrize("protocol", ["missing", None, "2", True, 1, 2.0, 3])
async def test_direct_sql_cannot_use_identity_without_integer_protocol_two(
    pg_pool: Pool, protocol: object
) -> None:
    """Missing/NULL and string protocol values cannot exploit PostgreSQL CHECK semantics."""
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    payload: dict[str, object] = {"executionIdentity": IDENTITY}
    if protocol != "missing":
        payload["deliveryProtocol"] = protocol
    with pytest.raises(CheckViolationError):
        await pg_pool.execute(
            "INSERT INTO custos_state.resume_subscription "
            "(workspace_id,resume_id,run_id,step_id,expires_at,payload) "
            "VALUES ('ws','resume','run','step',clock_timestamp(),$1::jsonb)",
            json.dumps(payload),
        )


async def test_old_writers_cannot_remove_or_rebind_execution_identity(pg_pool: Pool) -> None:
    """Even expired waits retain the original account; legacy payload replacement is rejected."""
    adapter = PgMetadataAdapter(pool=pg_pool)
    await adapter.apply_pending()
    now = datetime.now(UTC)
    original = ResumeSubscription(
        workspace_id=WorkspaceId("ws"),
        resume_id="resume",
        run_id=RunId("run"),
        step_id=StepId("step"),
        expires_at=now - timedelta(seconds=1),
        payload={"deliveryProtocol": 2, "executionIdentity": IDENTITY},
    )
    await adapter.put_resume_subscription("ws", original)
    changed = {**IDENTITY, "principalId": "replacement"}
    for payload in (
        {"deliveryProtocol": 2},
        {"deliveryProtocol": 2, "executionIdentity": changed},
    ):
        with pytest.raises(CheckViolationError):
            await pg_pool.execute(
                "UPDATE custos_state.resume_subscription SET payload=$1::jsonb "
                "WHERE workspace_id='ws' AND resume_id='resume'",
                json.dumps(payload),
            )
    replacements = await asyncio.gather(
        *(
            adapter.register_resume_subscription(
                "ws",
                replace(
                    original,
                    expires_at=now + timedelta(minutes=5),
                    payload={"deliveryProtocol": 2, "executionIdentity": changed},
                ),
                as_of=now,
            )
            for _ in range(2)
        )
    )
    assert all(row.payload["executionIdentity"] == IDENTITY for row in replacements)
    assert all(row.expires_at == original.expires_at for row in replacements)

"""Connector reference/catalog reads and explicit backend-error translation."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from asyncpg import InterfaceError, PostgresError
from asyncpg.pool import Pool
from custos_spl.errors import BackendUnavailable
from custos_spl.ids import ConnectorInstanceId, WorkspaceId

from custos_pg import PgCatalogAdapter, PgConnectorInstanceAdapter


def _adapter(result: object) -> tuple[PgConnectorInstanceAdapter, AsyncMock]:
    """Provide one controlled SQL result without mocking adapter validation."""
    connection = AsyncMock()
    connection.fetchval.return_value = result
    pool = MagicMock(spec=Pool)
    pool.acquire.return_value.__aenter__.return_value = connection
    return PgConnectorInstanceAdapter(pool=pool), connection


@pytest.mark.parametrize("exists", [False, True])
async def test_reference_lookup_selects_only_scoped_existence(exists: bool) -> None:
    """No credential/target columns are requested, including absent references."""
    adapter, connection = _adapter(exists)
    assert (
        await adapter.has_connector_instance(WorkspaceId("ws"), ConnectorInstanceId("id")) is exists
    )
    query, workspace, instance = connection.fetchval.call_args.args
    assert "SELECT EXISTS(SELECT 1" in query
    assert "workspace_id = $1 AND instance_id = $2" in query
    assert "credentials" not in query and "target_config" not in query
    assert (workspace, instance) == ("ws", "id")


@pytest.mark.parametrize("result", [None, 1, "true"])
async def test_reference_lookup_rejects_non_boolean_results(result: object) -> None:
    """Malformed backend output must not become a missing/success fallback."""
    adapter, _ = _adapter(result)
    with pytest.raises(BackendUnavailable, match="invalid result"):
        await adapter.has_connector_instance(WorkspaceId("ws"), ConnectorInstanceId("id"))


@pytest.mark.parametrize(
    "failure", [OSError("offline"), InterfaceError("closed"), PostgresError("unavailable")]
)
async def test_reference_backend_errors_are_explicit(failure: Exception) -> None:
    """Connection, pool and database failures retain a causal unavailable error."""
    adapter, connection = _adapter(False)
    connection.fetchval.side_effect = failure
    with pytest.raises(BackendUnavailable) as raised:
        await adapter.has_connector_instance(WorkspaceId("ws"), ConnectorInstanceId("id"))
    assert raised.value.__cause__ is failure


@pytest.mark.parametrize("stage", ["pool", "acquire", "query"])
@pytest.mark.parametrize(
    "failure",
    [
        TimeoutError("slow"),
        OSError("offline"),
        InterfaceError("closed"),
        PostgresError("unavailable"),
    ],
)
async def test_catalog_read_outages_are_translated(
    stage: str, failure: Exception, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pool and query failures retain a safe provider error with causal context."""
    connection = AsyncMock()
    pool = MagicMock(spec=Pool)
    pool.acquire.return_value.__aenter__.return_value = connection
    adapter = PgCatalogAdapter(pool=pool)
    if stage == "pool":
        monkeypatch.setattr(adapter, "_pool_ref", AsyncMock(side_effect=failure))
    elif stage == "acquire":
        pool.acquire.return_value.__aenter__.side_effect = failure
    else:
        connection.fetchrow.side_effect = failure
    with pytest.raises(BackendUnavailable, match="connector catalog lookup failed") as raised:
        await adapter.get_connector_type_version("registry", "1")
    assert raised.value.__cause__ is failure

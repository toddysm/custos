"""Optional storage readiness for immutable background execution identity references."""

from typing import Protocol, runtime_checkable

EXECUTION_IDENTITY_SCHEMA_REVISION = 14


@runtime_checkable
class ExecutionIdentityStoreProvider(Protocol):
    """Prevent activation before persistent registration identity guards are installed."""

    async def check_execution_identity_ready(self) -> None:
        """Raise MigrationRequired or an explicit backend error if the capability is unavailable."""
        ...

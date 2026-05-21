"""Shared fixtures for adapter conformance tests.

Provides containerized Postgres instance, database URLs, and common
test data factories.
"""

from __future__ import annotations

from typing import Generator

import pytest
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Provide containerized Postgres for conformance tests.

    Scope: session — single container shared across all tests for efficiency.
    Yields container; testcontainers automatically cleans up on exit.
    """
    with PostgresContainer(
        image="postgres:15-alpine",
        dbname="custos_test",
        username="custos",
        password="test_password",
    ) as container:
        # Wait for container to be ready
        container.get_connection_client().close()
        yield container


@pytest.fixture(scope="session")
def testdb_url(postgres_container: PostgresContainer) -> str:
    """Provide test database connection URL.

    Format: postgres://user:password@host:port/dbname
    """
    return postgres_container.get_connection_url()


@pytest.fixture
async def test_workspace_ids() -> dict[str, str]:
    """Provide test workspace IDs for multi-tenant scenarios."""
    return {
        "primary": "ws-test-primary",
        "secondary": "ws-test-secondary",
        "tertiary": "ws-test-tertiary",
    }

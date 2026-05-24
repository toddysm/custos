"""Catalog Service integration tests (CS-IMPL-021).

Exercises the catalog managers + FastAPI surface end-to-end against a
live Postgres backend. Skipped unless either ``CUSTOS_PG_DSN`` is set
(CI service-container pattern) or ``testcontainers[postgres]`` can
spin up a container locally.

Each test file under this package carries
``pytestmark = pytest.mark.integration`` so the unit-test ``pytest -q``
run filters them out via ``-m "not integration"`` in pyproject.toml.
"""

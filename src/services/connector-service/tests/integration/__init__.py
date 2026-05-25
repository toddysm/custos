"""Connector-service integration tests (CONN-IMPL-003).

Markers
-------

Every module in this package is decorated with the ``integration`` pytest
marker. The default `pytest` invocation (``pytest -m "not integration"``)
skips them; CI's ``connector-service-integration`` job runs them against a
live Postgres backend. Skipped unless either ``CUSTOS_PG_DSN`` is set
(CI service-container pattern) or ``testcontainers[postgres]`` can
boot a local container (developer ``make`` flow).
"""

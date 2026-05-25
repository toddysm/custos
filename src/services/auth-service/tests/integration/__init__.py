"""Integration tests for auth-service (AS-IMPL-028).

These tests run against a live Postgres — testcontainers locally,
GitHub Actions service container in CI. They are gated behind the
``integration`` pytest marker so the default ``pytest -q`` run (which
powers the per-PR coverage gate) stays a fast, network-free suite.

The matching CI job is ``auth-service-integration`` in
``.github/workflows/python-services.yml``.
"""

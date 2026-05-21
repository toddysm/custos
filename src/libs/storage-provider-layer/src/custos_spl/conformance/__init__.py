"""Conformance test suite framework for SPL adapters.

Provides base test classes and fixtures that all adapter implementations
must pass, covering workspace scoping, immutability, leases, audit outbox,
idempotency records, and transaction semantics.
"""

from .artifact_store import ArtifactStoreConformanceTests
from .base import AdapterConformanceBase
from .fixtures import postgres_container, testdb_url
from .log_query import LogQueryConformanceTests
from .metrics_query import MetricsQueryConformanceTests

__all__ = [
    "AdapterConformanceBase",
    "ArtifactStoreConformanceTests",
    "LogQueryConformanceTests",
    "MetricsQueryConformanceTests",
    "postgres_container",
    "testdb_url",
]

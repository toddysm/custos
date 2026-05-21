"""Conformance test suite framework for SPL adapters.

Provides base test classes and fixtures for adapter conformance testing,
including currently implemented suites for artifact storage, log queries,
and metrics queries.
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

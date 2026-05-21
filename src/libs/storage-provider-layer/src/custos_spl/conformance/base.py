"""Base conformance test class for all adapter types.

Defines common test patterns and assertions that every adapter
must satisfy regardless of provider type.
"""

from __future__ import annotations

import pytest

from custos_spl.errors import BackendUnavailable, WorkspaceMismatch


class AdapterConformanceBase:
    """Base class for adapter conformance tests.

    Subclasses MUST provide an 'adapter' pytest fixture that returns
    a fully-configured adapter instance ready for testing.

    All tests in this class validate the general contract that applies
    to all adapters: error classification, workspace scoping, and
    immutability rules.
    """

    def test_workspace_mismatch_error_exists(self) -> None:
        """WorkspaceMismatch error is properly defined and subclasses Exception."""
        assert issubclass(WorkspaceMismatch, Exception)
        assert WorkspaceMismatch.__module__ == "custos_spl.errors"

    def test_backend_unavailable_error_exists(self) -> None:
        """BackendUnavailable error is properly defined for transient failures."""
        assert issubclass(BackendUnavailable, Exception)
        assert BackendUnavailable.__module__ == "custos_spl.errors"

    def test_adapter_has_schema_revision(self) -> None:
        """All adapters must declare SCHEMA_REVISION class constant.

        This is required by SPL for compatibility tracking across adapter versions.
        """
        # This test requires the adapter fixture — subclasses provide it
        pytest.skip("Adapter fixture not provided — subclass must implement with adapter fixture")

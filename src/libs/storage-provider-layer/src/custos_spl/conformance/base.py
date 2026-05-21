"""Base conformance test class for all adapter types.

Defines common test patterns and assertions that every adapter
must satisfy regardless of provider type.
"""

from __future__ import annotations

from custos_spl.errors import WorkspaceMismatch


class AdapterConformanceBase:
    """Base class for adapter conformance tests.

    Subclasses should inherit from this and implement specific provider tests.
    Provides common fixtures and assertions for workspace scoping, error handling,
    and immutability contracts.
    """

    def test_workspace_mismatch_error_exists(self) -> None:
        """WorkspaceMismatch error is properly defined."""
        assert issubclass(WorkspaceMismatch, Exception)

    def test_workspace_scoping_contract(self) -> None:
        """Document the workspace scoping contract for this adapter.

        Every adapter MUST enforce workspace scoping per the SPL middleware.
        Cross-workspace access MUST raise WorkspaceMismatch (mapped to 404).
        """
        # This is a documentation test — subclasses implement actual scoping tests
        pass

    def test_immutability_contract(self) -> None:
        """Document immutability rules for this adapter.

        Immutable fields MUST raise ImmutableViolation on update attempts.
        This contract varies by provider — subclasses implement specifics.
        """
        pass

    def test_error_classification_contract(self) -> None:
        """Document error classification for this adapter.

        Errors MUST be classified as transient (BackendUnavailable) or
        permanent (domain error). Subclasses implement driver error mapping.
        """
        pass

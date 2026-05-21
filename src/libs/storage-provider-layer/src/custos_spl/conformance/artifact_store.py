"""Conformance tests for ArtifactStoreProvider adapters.

Tests that any ArtifactStore implementation must pass:
- Workspace scoping enforcement
- Content-addressability (digest-based keying)
- Idempotency (identical content → same digest)
- Sweeper-only deletion
- Streaming semantics (put/get)
- Error classification
"""

from __future__ import annotations

import pytest

from custos_spl.errors import BackendUnavailable, WorkspaceMismatch

from .base import AdapterConformanceBase


class ArtifactStoreConformanceTests(AdapterConformanceBase):
    """Base conformance tests for ArtifactStoreProvider adapters.

    Subclasses MUST provide:
    - `adapter` fixture: ArtifactStoreProvider instance
    - `workspace_id` fixture: test workspace ID
    - `sample_content` fixture: bytes for testing

    Example:
        @pytest.fixture
        def adapter(self) -> ArtifactStoreProvider:
            return MyS3Adapter(bucket="test")
    """

    def test_content_addressability_idempotency(self) -> None:
        """Identical content produces identical digest and artifact ID.

        Writing the same content twice must produce the same artifact_id
        and digest (idempotency guarantee).
        """
        pytest.skip(
            "Adapter must implement: test identical content → same artifact_id"
        )

    def test_streaming_put_memory_efficiency(self) -> None:
        """put() streams without buffering entire content in memory.

        O(1) memory contract: memory usage independent of content size.
        Content streamed to backend or temp file during upload.
        """
        pytest.skip(
            "Adapter must implement: test put() streaming O(1) memory contract"
        )

    def test_streaming_get_memory_efficiency(self) -> None:
        """get() returns async generator yielding chunks.

        Caller processes chunks without loading entire artifact
        in memory (O(1) memory contract for caller).
        """
        pytest.skip(
            "Adapter must implement: test get() returns streaming chunks"
        )

    def test_workspace_scoping_put_associates_workspace(self) -> None:
        """put() associates artifact with workspace.

        Artifact digest/key includes workspace_id, preventing
        cross-workspace collisions.
        """
        pytest.skip(
            "Adapter must implement: test put() associates artifact with workspace"
        )

    def test_workspace_scoping_get_blocks_cross_workspace(self) -> None:
        """get() rejects cross-workspace access.

        Attempting to retrieve artifact from different workspace
        raises WorkspaceMismatch (caller maps to 404).
        """
        pytest.skip(
            "Adapter must implement: test get() blocks cross-workspace access"
        )

    def test_workspace_scoping_head_returns_none_for_cross_workspace(self) -> None:
        """head() returns None for cross-workspace artifacts.

        Doesn't disclose cross-workspace existence (returns None, not error).
        """
        pytest.skip(
            "Adapter must implement: test head() returns None for cross-workspace"
        )

    def test_sweeper_only_deletion_requires_flag(self) -> None:
        """delete() requires is_sweeper=True flag.

        Prevents accidental deletion; only sweeper process can garbage-collect.
        """
        pytest.skip(
            "Adapter must implement: test delete() requires is_sweeper=True"
        )

    def test_deletion_idempotency_no_error_if_missing(self) -> None:
        """delete() succeeds even if artifact already absent.

        No error raised for missing artifact (safe for retry-able sweeper).
        """
        pytest.skip(
            "Adapter must implement: test delete() idempotency (no error if missing)"
        )

    def test_media_type_consistency_stored_vs_returned(self) -> None:
        """put() stores and returns consistent media_type.

        Returned ArtifactDescriptor.media_type must match what was stored
        (not just the input argument, which may be None).
        """
        pytest.skip(
            "Adapter must implement: test media_type consistency"
        )

    def test_error_classification_transient_failures(self) -> None:
        """Network/transient errors raise BackendUnavailable.

        Connection refused, timeout, HTTP 503 → BackendUnavailable.
        Caller retries with backoff.
        """
        pytest.skip(
            "Adapter must implement: test transient errors raise BackendUnavailable"
        )

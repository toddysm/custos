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
from custos_spl.ids import ArtifactId, WorkspaceId

from .base import AdapterConformanceBase


class ArtifactStoreConformanceTests(AdapterConformanceBase):
    """Base conformance tests for ArtifactStoreProvider adapters.

    Subclasses MUST provide an 'adapter' fixture that returns a configured
    ArtifactStoreProvider implementation ready for testing.

    Example:
        @pytest.fixture
        def adapter(self) -> ArtifactStoreProvider:
            return MyS3Adapter(bucket="test")
    """

    def test_content_addressability(self) -> None:
        """Identical content produces identical digest and artifact ID.

        This ensures idempotency: writing the same content twice
        produces the same artifact_id and digest.

        Subclasses MUST implement:
        1. Put same content twice
        2. Assert both calls return same artifact_id and digest
        """
        pytest.skip("Adapter must implement content-addressability test")

    def test_streaming_put_contract(self) -> None:
        """put() accepts async iterator of bytes without buffering.

        The adapter MUST stream to temp file or backend without loading
        entire content in memory (O(1) memory guarantee).

        Subclasses MUST implement:
        1. Provide large content via async iterator
        2. Monitor memory during put()
        3. Assert memory stays O(1) relative to content size
        """
        pytest.skip("Adapter must implement streaming put contract test")

    def test_streaming_get_contract(self) -> None:
        """get() returns async generator yielding chunks.

        Caller can stream and process chunks without loading
        entire artifact in memory.

        Subclasses MUST implement:
        1. Store artifact
        2. Call get() and iterate chunks
        3. Assert can process chunks without loading all in memory
        """
        pytest.skip("Adapter must implement streaming get contract test")

    def test_workspace_scoping_put(self) -> None:
        """put() associates artifact with workspace.

        Artifact digest includes workspace_id in key space,
        preventing cross-workspace collisions.

        Subclasses MUST implement:
        1. Put artifact in workspace A
        2. Verify artifact belongs to workspace A
        """
        pytest.skip("Adapter must implement workspace scoping for put test")

    def test_workspace_scoping_get(self) -> None:
        """get() rejects cross-workspace access.

        Attempting to retrieve artifact from different workspace
        raises WorkspaceMismatch (caller maps to 404).

        Subclasses MUST implement:
        1. Put artifact in workspace A
        2. Attempt get() from workspace B
        3. Assert raises WorkspaceMismatch
        """
        pytest.skip("Adapter must implement workspace scoping for get test")

    def test_workspace_scoping_head(self) -> None:
        """head() returns None for cross-workspace artifacts.

        Lightweight check that doesn't disclose cross-workspace existence.

        Subclasses MUST implement:
        1. Put artifact in workspace A
        2. Call head() from workspace B
        3. Assert returns None (not error)
        """
        pytest.skip("Adapter must implement workspace scoping for head test")

    def test_sweeper_only_deletion(self) -> None:
        """delete() requires is_sweeper=True flag.

        Prevents accidental deletion; only sweeper process can garbage-collect.

        Subclasses MUST implement:
        1. Put artifact
        2. Call delete(is_sweeper=False)
        3. Assert raises ValueError
        4. Call delete(is_sweeper=True)
        5. Assert succeeds
        """
        pytest.skip("Adapter must implement sweeper-only deletion test")

    def test_deletion_idempotency(self) -> None:
        """delete() succeeds even if artifact already absent.

        No error raised for missing artifact; safe for retry-able sweeper.

        Subclasses MUST implement:
        1. Call delete(is_sweeper=True) on missing artifact
        2. Assert succeeds (no error raised)
        """
        pytest.skip("Adapter must implement deletion idempotency test")

    def test_media_type_consistency(self) -> None:
        """put() stores effective media_type consistently.

        Returned ArtifactDescriptor.media_type must match what
        was stored (not the argument, which may be None).

        Subclasses MUST implement:
        1. Put with media_type=None
        2. Assert returned descriptor has effective type (e.g., application/octet-stream)
        3. Call head() and verify media_type matches
        """
        pytest.skip("Adapter must implement media type consistency test")

    def test_error_classification(self) -> None:
        """Network errors classified as BackendUnavailable.

        Transient failures (connection, timeout, HTTP 503) raise
        BackendUnavailable; caller retries with backoff.

        Subclasses MUST implement:
        1. Simulate backend unavailable (mock connection error)
        2. Call adapter method
        3. Assert raises BackendUnavailable (not other exception type)
        """
        pytest.skip("Adapter must implement error classification test")

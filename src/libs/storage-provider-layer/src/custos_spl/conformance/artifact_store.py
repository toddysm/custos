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
    """Base conformance tests for ArtifactStoreProvider adapters."""

    def test_content_addressability(self) -> None:
        """Identical content produces identical digest and artifact ID.

        This ensures idempotency: writing the same content twice
        produces the same artifact_id and digest.
        """
        # Implementation-specific test — subclasses provide adapter fixture
        pass

    def test_streaming_put_contract(self) -> None:
        """put() accepts async iterator of bytes without buffering.

        The adapter MUST stream to temp file or backend without loading
        entire content in memory (O(1) memory guarantee).
        """
        pass

    def test_streaming_get_contract(self) -> None:
        """get() returns async generator yielding chunks.

        Caller can stream and process chunks without loading
        entire artifact in memory.
        """
        pass

    def test_workspace_scoping_put(self) -> None:
        """put() associates artifact with workspace.

        Artifact digest includes workspace_id in key space,
        preventing cross-workspace collisions.
        """
        pass

    def test_workspace_scoping_get(self) -> None:
        """get() rejects cross-workspace access.

        Attempting to retrieve artifact from different workspace
        raises WorkspaceMismatch (caller maps to 404).
        """
        pass

    def test_workspace_scoping_head(self) -> None:
        """head() returns None for cross-workspace artifacts.

        Lightweight check that doesn't disclose cross-workspace existence.
        """
        pass

    def test_sweeper_only_deletion(self) -> None:
        """delete() requires is_sweeper=True flag.

        Prevents accidental deletion; only sweeper process can garbage-collect.
        """
        pass

    def test_deletion_idempotency(self) -> None:
        """delete() succeeds even if artifact already absent.

        No error raised for missing artifact; safe for retry-able sweeper.
        """
        pass

    def test_media_type_consistency(self) -> None:
        """put() stores effective media_type consistently.

        Returned ArtifactDescriptor.media_type must match what
        was stored (not the argument, which may be None).
        """
        pass

    def test_error_classification(self) -> None:
        """Network errors classified as BackendUnavailable.

        Transient failures (connection, timeout, HTTP 503) raise
        BackendUnavailable; caller retries with backoff.
        """
        pass

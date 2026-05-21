"""Conformance tests for S3 ArtifactStore adapter.

These tests verify that the S3 adapter satisfies the ArtifactStoreProvider
conformance contract. They use moto to mock S3 — no real AWS account needed.

Run with: pytest tests/test_conformance.py -v -m integration
"""

from __future__ import annotations

import boto3
import pytest
from moto import mock_s3

from custos_spl.conformance import ArtifactStoreConformanceTests
from custos_spl.ids import WorkspaceId

pytest.importorskip("custos_s3")

from custos_s3.adapters import S3ArtifactAdapter


@mock_s3
@pytest.mark.integration
class TestS3ArtifactConformance(ArtifactStoreConformanceTests):
    """S3 adapter conformance tests.

    Uses moto to mock S3; no external service needed.
    """

    @pytest.fixture(scope="class", autouse=True)
    def _setup_s3_bucket(self) -> None:
        """Create test bucket before running conformance tests.

        Required because S3ArtifactAdapter calls put_object() which fails
        if the bucket doesn't exist (NoSuchBucket error).
        """
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="conformance-test")

    @pytest.fixture
    def adapter(self) -> S3ArtifactAdapter:
        """Provide configured S3 artifact adapter with mocked S3."""
        return S3ArtifactAdapter(
            bucket="conformance-test",
            region="us-east-1",
        )

    @pytest.fixture
    def workspace_id(self) -> WorkspaceId:
        """Primary test workspace."""
        return WorkspaceId("ws-conformance-test-primary")

    @pytest.fixture
    def other_workspace_id(self) -> WorkspaceId:
        """Secondary workspace for cross-workspace tests."""
        return WorkspaceId("ws-conformance-test-secondary")

    @pytest.fixture
    async def sample_content(self) -> bytes:
        """Sample content for artifact store tests."""
        return b"sample s3 artifact content for conformance testing"


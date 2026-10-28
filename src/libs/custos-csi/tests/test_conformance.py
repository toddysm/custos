"""Conformance tests for CSI ArtifactStore adapter.

These tests verify the filesystem adapter against an isolated temporary mount.
They do not certify Kubernetes provisioning or a particular CSI driver.

Run with: pytest tests/test_conformance.py -v -m integration
"""

from __future__ import annotations

from pathlib import Path

import pytest
from custos_spl.conformance import ArtifactStoreConformanceTests
from custos_spl.ids import WorkspaceId

pytest.importorskip("custos_csi")

from custos_csi.adapters import CsiArtifactAdapter


@pytest.mark.integration
class TestCsiArtifactConformance(ArtifactStoreConformanceTests):
    """CSI adapter conformance tests.

    No cluster or shared volume is required for the adapter's filesystem contract.
    """

    @pytest.fixture
    def adapter(self, tmp_path: Path) -> CsiArtifactAdapter:
        """Use a fresh mount directory so tests never share artifact state."""
        return CsiArtifactAdapter(pvc_mount=tmp_path)

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
        return b"sample csi artifact content for conformance testing"

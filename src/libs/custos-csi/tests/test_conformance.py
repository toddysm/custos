"""Conformance tests for CSI ArtifactStore adapter.

These tests verify that the CSI adapter satisfies the ArtifactStoreProvider
conformance contract. They require:
- custos-spl[conformance] package installed
- Kubernetes cluster or CSI volume accessible

Run with: pytest tests/test_conformance.py -v -m integration
Skip without CSI: pytest tests/test_conformance.py -v -m "not integration"
"""

from __future__ import annotations

import os

import pytest

from custos_spl.conformance import ArtifactStoreConformanceTests
from custos_spl.ids import WorkspaceId

pytest.importorskip("custos_csi")

from custos_csi.adapters import CsiArtifactAdapter


@pytest.mark.integration
class TestCsiArtifactConformance(ArtifactStoreConformanceTests):
    """CSI adapter conformance tests.

    Skipped if CSI volume is unavailable or custos-csi not installed.
    """

    @pytest.fixture(scope="class", autouse=True)
    def _check_csi_available(self) -> None:
        """Skip entire test class if CSI volume is not accessible."""
        csi_path = os.environ.get("CUSTOS_CSI_VOLUME_PATH", "/mnt/csi-test")
        if not os.path.exists(csi_path) or not os.path.isdir(csi_path):
            pytest.skip(
                f"CSI volume not available at {csi_path} — skipping integration tests"
            )

    @pytest.fixture
    def adapter(self) -> CsiArtifactAdapter:
        """Provide configured CSI artifact adapter."""
        csi_path = os.environ.get("CUSTOS_CSI_VOLUME_PATH", "/mnt/csi-test")
        return CsiArtifactAdapter(volume_path=csi_path)

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

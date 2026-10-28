"""S3 conformance against a loopback Moto server compatible with async clients."""

from __future__ import annotations

from collections.abc import Iterator

import boto3
import pytest
from custos_spl.conformance import ArtifactStoreConformanceTests
from custos_spl.ids import WorkspaceId
from moto.server import ThreadedMotoServer

from custos_s3.adapters import S3ArtifactAdapter


@pytest.fixture
def s3_endpoint(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Run an isolated loopback S3 endpoint with inert, test-only credentials."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    try:
        host, port = server.get_host_and_port()
        endpoint = f"http://{host}:{port}"
        client = boto3.client("s3", region_name="us-east-1", endpoint_url=endpoint)
        try:
            client.create_bucket(Bucket="conformance-test")
        finally:
            client.close()
        yield endpoint
    finally:
        server.stop()


@pytest.mark.integration
class TestS3ArtifactConformance(ArtifactStoreConformanceTests):
    """Exercise the real async adapter over HTTP, without synchronous Moto interception."""

    @pytest.fixture
    def adapter(self, s3_endpoint: str) -> S3ArtifactAdapter:
        """Bind the adapter exclusively to the fixture's loopback endpoint."""
        return S3ArtifactAdapter(
            bucket="conformance-test", region="us-east-1", endpoint_url=s3_endpoint
        )

    @pytest.fixture
    def workspace_id(self) -> WorkspaceId:
        """Return the primary artifact workspace."""
        return WorkspaceId("ws-conformance-test-primary")

    @pytest.fixture
    def other_workspace_id(self) -> WorkspaceId:
        """Return a separate workspace for isolation checks."""
        return WorkspaceId("ws-conformance-test-secondary")

    @pytest.fixture
    async def sample_content(self) -> bytes:
        """Return stable bytes for streaming and content-addressing checks."""
        return b"sample s3 artifact content for conformance testing"

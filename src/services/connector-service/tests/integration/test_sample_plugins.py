"""End-to-end integration test for the sample plugin manifests.

This test takes the two sample plugin manifests that ship under
``src/libs/connector-plugins/`` — exactly the bytes a packager would
publish to an OCI registry as the connector-manifest artifact — and
verifies:

1. They satisfy the v1 manifest JSON schema (``validate_manifest``).
2. The Connector Service's OCI discovery path can fetch them from a
   real registry via both supported resolution mechanisms:

   * **Referrers API**  — used for the ``oci-registry`` plugin.
   * **Fallback tag**   — used for the ``slack-notifier`` plugin
     against a registry (``distribution/distribution``) that does not
     implement the Referrers API in its default configuration, which
     is exactly the production deployment shape we promise to support.

The push helpers (``_push_blob``, ``_push_subject_image``,
``_push_connector_manifest``) are intentionally re-implemented here as
copies of the helpers in
:mod:`tests.integration.test_oci_discovery` to keep this test
self-contained: that module's helpers are tied to its own
parametrisation and lifting them into a shared module would expand the
diff for marginal value at this stage. If a third consumer appears we
should promote them to ``tests.integration._oci_push_helpers``.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from custos_connector.manifest import (
    CONNECTOR_MANIFEST_MEDIA_TYPE,
    discover_manifest,
    fallback_tag_for_digest,
)
from custos_connector.manifest.validator import validate_manifest

pytestmark = [pytest.mark.integration, pytest.mark.oci_integration]


# ---------------------------------------------------------------------------
# Sample plugin manifests
# ---------------------------------------------------------------------------

#: Repository root, derived from this test file's path. Layout:
#:    <repo>/src/services/connector-service/tests/integration/<this>
_REPO_ROOT = Path(__file__).resolve().parents[5]

_SAMPLE_PLUGINS_DIR = _REPO_ROOT / "src" / "libs" / "connector-plugins"

_OCI_REGISTRY_MANIFEST_PATH = _SAMPLE_PLUGINS_DIR / "oci-registry" / "connector-manifest.json"
_SLACK_NOTIFIER_MANIFEST_PATH = _SAMPLE_PLUGINS_DIR / "slack-notifier" / "connector-manifest.json"


def _read_manifest_bytes(path: Path) -> bytes:
    """Read manifest bytes verbatim from disk.

    We re-serialize via :func:`json.dumps` so the artifact body has a
    canonical byte form regardless of trailing whitespace in the
    source file. The on-disk JSON is the source of truth for *content*;
    serialization is just a representation choice for the registry.
    """
    return json.dumps(json.loads(path.read_text(encoding="utf-8")), separators=(",", ":")).encode(
        "utf-8"
    )


# ---------------------------------------------------------------------------
# Registry fixture (single-impl: zot for Referrers, distribution for fallback)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def zot_url() -> Iterator[str]:
    """Zot registry — supports the Referrers API."""
    env_url = os.environ.get("CUSTOS_OCI_REGISTRY_ZOT_URL")
    if env_url:
        yield env_url.rstrip("/")
        return

    try:
        from testcontainers.core.container import DockerContainer
        from testcontainers.core.waiting_utils import wait_for_logs
    except ImportError:  # pragma: no cover
        pytest.skip("CUSTOS_OCI_REGISTRY_ZOT_URL not set and testcontainers not installed")

    try:
        container = DockerContainer(
            "ghcr.io/project-zot/zot-linux-amd64:v2.1.2"
        ).with_exposed_ports(5000)
        container.start()
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"could not start zot testcontainer: {exc}")

    try:
        with contextlib.suppress(Exception):
            wait_for_logs(container, "listening", timeout=30)
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5000)
        yield f"http://{host}:{port}"
    finally:
        container.stop()


@pytest.fixture(scope="session")
def distribution_url() -> Iterator[str]:
    """distribution/distribution — does NOT implement Referrers in default
    config; exercised against the fallback-tag path."""
    env_url = os.environ.get("CUSTOS_OCI_REGISTRY_DISTRIBUTION_URL")
    if env_url:
        yield env_url.rstrip("/")
        return

    try:
        from testcontainers.core.container import DockerContainer
        from testcontainers.core.waiting_utils import wait_for_logs
    except ImportError:  # pragma: no cover
        pytest.skip("CUSTOS_OCI_REGISTRY_DISTRIBUTION_URL not set and testcontainers not installed")

    try:
        container = DockerContainer("registry:2.8.3").with_exposed_ports(5000)
        container.start()
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"could not start distribution testcontainer: {exc}")

    try:
        with contextlib.suppress(Exception):
            wait_for_logs(container, "listening", timeout=30)
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5000)
        yield f"http://{host}:{port}"
    finally:
        container.stop()


# ---------------------------------------------------------------------------
# OCI push helpers (mirror of test_oci_discovery; see module docstring)
# ---------------------------------------------------------------------------

_CONFIG_BODY: bytes = (
    b'{"architecture":"amd64","os":"linux","rootfs":{"type":"layers","diff_ids":[]}}'
)
_LAYER_BODY: bytes = b"custos-sample-plugin-layer"


def _sha256(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


async def _push_blob(client: httpx.AsyncClient, repo: str, data: bytes) -> str:
    digest = _sha256(data)
    start = await client.post(f"/v2/{repo}/blobs/uploads/")
    assert start.status_code in (202, 201), (start.status_code, start.text)
    upload_loc = start.headers["location"]
    if upload_loc.startswith("http"):
        upload_url = httpx.URL(upload_loc)
        upload_path = upload_url.path
        if upload_url.query:
            upload_path = f"{upload_path}?{upload_url.query.decode('ascii')}"
    else:
        upload_path = upload_loc
    sep = "&" if "?" in upload_path else "?"
    put = await client.put(
        f"{upload_path}{sep}digest={digest}",
        content=data,
        headers={"content-type": "application/octet-stream"},
    )
    assert put.status_code in (201, 202, 200), (put.status_code, put.text)
    return digest


async def _push_subject_image(client: httpx.AsyncClient, repo: str) -> tuple[str, int]:
    config_digest = await _push_blob(client, repo, _CONFIG_BODY)
    layer_digest = await _push_blob(client, repo, _LAYER_BODY)
    manifest: dict[str, Any] = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config_digest,
            "size": len(_CONFIG_BODY),
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar",
                "digest": layer_digest,
                "size": len(_LAYER_BODY),
            }
        ],
    }
    body = json.dumps(manifest).encode("utf-8")
    local_digest = _sha256(body)
    put = await client.put(
        f"/v2/{repo}/manifests/{local_digest}",
        content=body,
        headers={"content-type": "application/vnd.oci.image.manifest.v1+json"},
    )
    assert put.status_code in (200, 201), (put.status_code, put.text)
    server_digest = put.headers.get("docker-content-digest", local_digest)
    return server_digest, len(body)


async def _fetch_connector_body(
    client: httpx.AsyncClient, repo: str, manifest_digest: str
) -> dict[str, Any]:
    """Pull the layer that carries the connector-manifest payload from
    an artifact OCI manifest pinned by ``manifest_digest``.
    """
    get_manifest = await client.get(
        f"/v2/{repo}/manifests/{manifest_digest}",
        headers={"accept": "application/vnd.oci.image.manifest.v1+json"},
    )
    get_manifest.raise_for_status()
    artifact = get_manifest.json()
    layers = [
        layer
        for layer in artifact.get("layers", [])
        if layer.get("mediaType") == CONNECTOR_MANIFEST_MEDIA_TYPE
    ]
    assert len(layers) == 1, f"expected exactly one connector-manifest layer; got {layers!r}"
    body_digest = layers[0]["digest"]
    body_resp = await client.get(f"/v2/{repo}/blobs/{body_digest}")
    body_resp.raise_for_status()
    parsed = json.loads(body_resp.content)
    assert isinstance(parsed, dict)
    return parsed


async def _push_connector_manifest(
    client: httpx.AsyncClient,
    repo: str,
    subject_digest: str,
    subject_size: int,
    manifest_body: bytes,
    *,
    use_referrers: bool,
    use_fallback_tag: bool,
) -> str:
    """Push a v1 connector manifest artifact whose payload is the
    supplied ``manifest_body`` and return the *artifact* digest.
    """
    body_digest = await _push_blob(client, repo, manifest_body)

    manifest: dict[str, Any] = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "artifactType": CONNECTOR_MANIFEST_MEDIA_TYPE,
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": _sha256(b"{}"),
            "size": 2,
        },
        "layers": [
            {
                "mediaType": CONNECTOR_MANIFEST_MEDIA_TYPE,
                "digest": body_digest,
                "size": len(manifest_body),
            }
        ],
    }
    if use_referrers:
        manifest["subject"] = {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": subject_digest,
            "size": subject_size,
        }
    await _push_blob(client, repo, b"{}")

    body = json.dumps(manifest).encode("utf-8")
    digest = _sha256(body)
    refs: Iterable[str] = (
        (fallback_tag_for_digest(subject_digest), digest) if use_fallback_tag else (digest,)
    )
    for ref in refs:
        put = await client.put(
            f"/v2/{repo}/manifests/{ref}",
            content=body,
            headers={"content-type": "application/vnd.oci.image.manifest.v1+json"},
        )
        assert put.status_code in (200, 201), (put.status_code, put.text)
    return digest


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_sample_manifests_satisfy_v1_schema() -> None:
    """The on-disk bytes must validate against the v1 schema.

    This test is unit-fast (no network) but lives in the integration
    suite alongside its end-to-end cousins so a single test run can
    confirm the whole pipeline — schema, OCI publication, discovery —
    is wired up correctly for both sample plugins.
    """
    for path in (_OCI_REGISTRY_MANIFEST_PATH, _SLACK_NOTIFIER_MANIFEST_PATH):
        manifest = json.loads(path.read_text(encoding="utf-8"))
        validate_manifest(manifest)


@pytest.mark.asyncio
async def test_oci_registry_plugin_manifest_round_trips_via_referrers(
    zot_url: str,
) -> None:
    """Push the oci-registry sample manifest as an OCI artifact with a
    ``subject`` reference and confirm the Connector Service's discovery
    path retrieves and re-validates it.
    """
    repo = "custos-integration/sample-oci-registry"
    manifest_bytes = _read_manifest_bytes(_OCI_REGISTRY_MANIFEST_PATH)

    async with httpx.AsyncClient(base_url=zot_url, timeout=30.0) as client:
        subject_digest, subject_size = await _push_subject_image(client, repo)
        await _push_connector_manifest(
            client,
            repo,
            subject_digest,
            subject_size,
            manifest_bytes,
            use_referrers=True,
            use_fallback_tag=False,
        )

        descriptor = await discover_manifest(
            client,
            repository=repo,
            subject_digest=subject_digest,
        )

        retrieved = await _fetch_connector_body(client, repo, descriptor.digest)
        validate_manifest(retrieved)
        assert retrieved["metadata"]["type"] == "custos-oci-registry"


@pytest.mark.asyncio
async def test_slack_notifier_plugin_manifest_round_trips_via_fallback_tag(
    distribution_url: str,
) -> None:
    """Push the slack-notifier sample manifest as an OCI artifact with
    only the fallback tag (no Referrers) and confirm the Connector
    Service's discovery path retrieves and re-validates it.
    """
    repo = "custos-integration/sample-slack-notifier"
    manifest_bytes = _read_manifest_bytes(_SLACK_NOTIFIER_MANIFEST_PATH)

    async with httpx.AsyncClient(base_url=distribution_url, timeout=30.0) as client:
        subject_digest, subject_size = await _push_subject_image(client, repo)
        await _push_connector_manifest(
            client,
            repo,
            subject_digest,
            subject_size,
            manifest_bytes,
            use_referrers=False,
            use_fallback_tag=True,
        )

        descriptor = await discover_manifest(
            client,
            repository=repo,
            subject_digest=subject_digest,
        )
        retrieved = await _fetch_connector_body(client, repo, descriptor.digest)
        validate_manifest(retrieved)
        assert retrieved["metadata"]["type"] == "custos-slack-notifier"

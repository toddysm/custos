"""Integration tests for the OCI Referrers discovery client (CONN-IMPL-007).

These tests exercise :func:`custos_connector.manifest.discovery.discover_manifest`
against a real OCI registry container. The fixture is parameterised over
two registry implementations:

* ``distribution`` — the CNCF distribution reference (``registry:2``).
  Supports the Referrers API since v2.8.2; widely deployed.
* ``zot`` — Project Zot's lightweight registry. Native Referrers support;
  smaller image footprint.

Resolution order for each registry:

1. ``CUSTOS_OCI_REGISTRY_<NAME>_URL`` env var — set in CI from a service
   container (or any externally-managed registry).
2. ``testcontainers`` fallback — boots a fresh container per session. Skipped
   if Docker isn't reachable.

Each registry parameter is independently optional: if neither env var is
set and Docker isn't available, the corresponding test instances are
skipped without failing the suite.

Marked with both :pytest.mark:`integration` (so the default test run
skips them) and :pytest.mark:`oci_integration` (so the CI matrix can
opt-in to the OCI subset specifically).
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any

import httpx
import pytest

from custos_connector.manifest import (
    AUDIT_EVENT_FALLBACK_IGNORED,
    AUDIT_EVENT_FALLBACK_USED,
    CONNECTOR_MANIFEST_MEDIA_TYPE,
    DiscoveryErrorCode,
    ManifestDiscoveryError,
    discover_manifest,
    fallback_tag_for_digest,
)

pytestmark = [pytest.mark.integration, pytest.mark.oci_integration]


# ---------------------------------------------------------------------------
# Registry fixture
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _RegistrySpec:
    """One parameterised registry implementation under test."""

    name: str
    image: str
    container_port: int
    env_var: str
    #: ``True`` if the implementation exposes the OCI Referrers API at
    #: ``GET /v2/<name>/referrers/<digest>``. ``zot`` does;
    #: ``distribution/distribution`` does not in its released default
    #: configuration (verified against v2.8.x, v3.0.0 and v3.1.1) and is
    #: gated out of the Referrers-only test.
    supports_referrers: bool = True


REGISTRY_SPECS: tuple[_RegistrySpec, ...] = (
    _RegistrySpec(
        name="distribution",
        image="registry:2.8.3",
        container_port=5000,
        env_var="CUSTOS_OCI_REGISTRY_DISTRIBUTION_URL",
        # distribution/distribution does NOT expose the OCI Referrers
        # API at /v2/<name>/referrers/<digest> in any released default
        # configuration we have validated against (v2.8.x, v3.0.0,
        # v3.1.1). The endpoint simply isn't routed: requests return
        # the Go default "404 page not found" text/plain response with
        # no v2 JSON error envelope, and no OCI fallback index tag
        # ("<algo>-<digest>") is created either. distribution is
        # therefore exercised only against the connector-specific
        # fallback tag path.
        supports_referrers=False,
    ),
    _RegistrySpec(
        name="zot",
        image="ghcr.io/project-zot/zot-linux-amd64:v2.1.2",
        container_port=5000,
        env_var="CUSTOS_OCI_REGISTRY_ZOT_URL",
    ),
)


@pytest.fixture(scope="session", params=REGISTRY_SPECS, ids=lambda s: s.name)
def registry_spec(request: pytest.FixtureRequest) -> _RegistrySpec:
    """Expose the active ``_RegistrySpec`` so tests can gate on
    capability flags such as ``supports_referrers`` without having to
    re-parameterise themselves.
    """
    return request.param  # type: ignore[no-any-return]


@pytest.fixture(scope="session")
def registry_url(registry_spec: _RegistrySpec) -> Iterator[str]:
    """Per-implementation registry base URL.

    Yields the base URL of a usable OCI registry. Skips when neither
    the env var nor testcontainers can produce one.
    """
    spec = registry_spec

    # CI / externally-managed registry: pick it up via env var.
    env_url = os.environ.get(spec.env_var)
    if env_url:
        yield env_url.rstrip("/")
        return

    # Local fallback: spin a fresh testcontainer.
    try:
        from testcontainers.core.container import DockerContainer
        from testcontainers.core.waiting_utils import wait_for_logs
    except ImportError:  # pragma: no cover - dev-dep gate
        pytest.skip(f"{spec.env_var} not set and testcontainers not installed")

    try:
        container = DockerContainer(spec.image).with_exposed_ports(spec.container_port)
        container.start()
    except Exception as exc:  # pragma: no cover - docker not available
        pytest.skip(f"could not start {spec.name} testcontainer: {exc}")

    try:
        # Wait for the registry to be ready. ``distribution`` logs
        # "listening on" once the HTTP server binds; zot logs
        # "HTTP port" similarly. We log-match on a substring common to
        # both so the same wait works for either image.
        with contextlib.suppress(Exception):  # pragma: no cover - log drift
            wait_for_logs(container, "listening", timeout=30)
        host = container.get_container_host_ip()
        port = container.get_exposed_port(spec.container_port)
        yield f"http://{host}:{port}"
    finally:
        container.stop()


# ---------------------------------------------------------------------------
# Helpers — push a subject image + connector manifest artifact
# ---------------------------------------------------------------------------


@pytest.fixture
def subject_repo(request: pytest.FixtureRequest) -> str:
    """Per-test repository name so tests don't cross-pollinate.

    The session-scoped ``registry_url`` fixture keeps the same
    container alive across all tests, so two tests that pushed to
    the same repo + subject digest would see each other's manifests
    — e.g. ``test_discover_via_fallback_tag`` would resolve via the
    leftover Referrers entry that ``test_discover_via_referrers``
    just pushed against the same constant subject body. Giving each
    test its own repo isolates registry-side state without paying for
    a fresh container.
    """
    # request.node.name looks like 'test_discover_via_referrers[zot]'.
    # Strip the ``[param]`` suffix and lowercase to keep the repo name
    # within the OCI distribution-spec regex.
    base = request.node.name.split("[")[0].lower().replace("_", "-")
    return f"custos-integration/{base}"


#: A trivial OCI image config object so the registry accepts the
#: manifest. Content is arbitrary; only its sha256 matters.
_CONFIG_BODY: bytes = (
    b'{"architecture":"amd64","os":"linux","rootfs":{"type":"layers","diff_ids":[]}}'
)

#: A trivial layer blob so manifests have non-empty ``layers``.
_LAYER_BODY: bytes = b"custos-integration-layer"


def _sha256(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


async def _push_blob(client: httpx.AsyncClient, repo: str, data: bytes) -> str:
    """Upload a blob via two-step monolithic POST + PUT and return its digest."""
    digest = _sha256(data)

    # Step 1: start upload session, get the upload URL via the Location header.
    start = await client.post(f"/v2/{repo}/blobs/uploads/")
    assert start.status_code in (202, 201), (start.status_code, start.text)
    upload_loc = start.headers["location"]
    # Some registries return absolute URLs, others relative.
    if upload_loc.startswith("http"):
        # Strip the scheme+host so httpx joins it onto the base URL cleanly,
        # while preserving any upload-session query parameters (for example
        # registry-managed state tokens).
        upload_url = httpx.URL(upload_loc)
        upload_path = upload_url.path
        if upload_url.query:
            upload_path = f"{upload_path}?{upload_url.query.decode('ascii')}"
    else:
        upload_path = upload_loc

    # Step 2: PUT the blob with ?digest=<sha256> to commit.
    sep = "&" if "?" in upload_path else "?"
    put = await client.put(
        f"{upload_path}{sep}digest={digest}",
        content=data,
        headers={"content-type": "application/octet-stream"},
    )
    assert put.status_code in (201, 202, 200), (put.status_code, put.text)
    return digest


async def _push_subject_image(client: httpx.AsyncClient, repo: str) -> tuple[str, int]:
    """Push a minimal OCI image manifest; return ``(digest, size)``.

    The returned ``digest`` is the one the registry stored under
    (``Docker-Content-Digest`` from the PUT response when available,
    falling back to the locally-computed sha for compliant registries
    that return no such header). The size is the byte length of the
    JSON body we PUT, so callers building a Referrers ``subject``
    descriptor don't need to round-trip the registry to learn it.
    """
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


async def _push_connector_manifest(
    client: httpx.AsyncClient,
    repo: str,
    subject_digest: str,
    subject_size: int,
    *,
    use_referrers: bool,
    use_fallback_tag: bool,
) -> str:
    """Push a v1 connector manifest artifact and return its digest.

    If ``use_referrers`` is True, the manifest carries a ``subject``
    reference so the registry indexes it under the Referrers API.
    If ``use_fallback_tag`` is True, the manifest is *also* tagged
    with the deterministic fallback tag derived from ``subject_digest``.
    """
    body_bytes = b'{"connector":"sample"}'
    body_digest = await _push_blob(client, repo, body_bytes)

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
                "size": len(body_bytes),
            }
        ],
    }
    if use_referrers:
        manifest["subject"] = {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": subject_digest,
            "size": subject_size,
        }
    # Push the connector manifest config blob (the empty "{}" we
    # referenced above) before pushing the manifest itself.
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


@pytest.mark.asyncio
async def test_discover_via_referrers(
    registry_url: str,
    registry_spec: _RegistrySpec,
    subject_repo: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Push a manifest with a ``subject`` reference; discover_manifest
    MUST find it via the Referrers API and emit ``fallback-ignored``.
    """
    if not registry_spec.supports_referrers:
        pytest.skip(f"{registry_spec.name} does not implement the OCI Referrers API")
    import logging

    async with httpx.AsyncClient(base_url=registry_url, timeout=30.0) as client:
        subject_digest, subject_size = await _push_subject_image(client, subject_repo)
        manifest_digest = await _push_connector_manifest(
            client,
            subject_repo,
            subject_digest,
            subject_size,
            use_referrers=True,
            use_fallback_tag=False,
        )

        with caplog.at_level(logging.INFO, logger="custos_connector.audit"):
            descriptor = await discover_manifest(
                client,
                repository=subject_repo,
                subject_digest=subject_digest,
            )
    assert descriptor.digest == manifest_digest
    assert any(AUDIT_EVENT_FALLBACK_IGNORED in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_discover_via_fallback_tag(
    registry_url: str,
    subject_repo: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Push a manifest tagged with the fallback tag but WITHOUT a
    ``subject`` reference; discover_manifest MUST fall back to the
    deterministic tag and emit ``fallback-used``.
    """
    import logging

    async with httpx.AsyncClient(base_url=registry_url, timeout=30.0) as client:
        subject_digest, subject_size = await _push_subject_image(client, subject_repo)
        await _push_connector_manifest(
            client,
            subject_repo,
            subject_digest,
            subject_size,
            use_referrers=False,
            use_fallback_tag=True,
        )

        with caplog.at_level(logging.INFO, logger="custos_connector.audit"):
            descriptor = await discover_manifest(
                client,
                repository=subject_repo,
                subject_digest=subject_digest,
            )
    # Tagged-only path -> the descriptor digest comes from the HEAD
    # response header on the fallback tag; just assert it's a sha256
    # value (the exact value depends on registry response headers).
    assert descriptor.digest.startswith("sha256:")
    assert any(AUDIT_EVENT_FALLBACK_USED in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_discover_no_manifest_for_unknown_subject(
    registry_url: str,
    subject_repo: str,
) -> None:
    """A subject with no associated connector manifest in either path
    MUST raise NO_MANIFEST_FOUND."""
    unknown_digest = "sha256:" + "9" * 64
    async with httpx.AsyncClient(base_url=registry_url, timeout=30.0) as client:
        with pytest.raises(ManifestDiscoveryError) as exc_info:
            await discover_manifest(
                client,
                repository=subject_repo,
                subject_digest=unknown_digest,
            )
    assert exc_info.value.code == DiscoveryErrorCode.NO_MANIFEST_FOUND

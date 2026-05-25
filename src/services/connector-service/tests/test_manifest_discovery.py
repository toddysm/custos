"""Unit tests for :mod:`custos_connector.manifest.discovery` (CONN-IMPL-007).

Coverage:

* :func:`fallback_tag_for_digest` rejection codes
  (``unsupported-digest-algorithm``, ``invalid-digest-format``) and the
  derivation rule itself (lowercase hex, ``:`` -> ``-``, length cap).
* :func:`discover_manifest` orchestration: Referrers wins / fallback
  used / both empty -> ``no-manifest-found`` / Referrers ambiguous.
* Audit events fire with the documented names + payload keys.

Network is mocked via :class:`httpx.MockTransport`; no live registry
required.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable

import httpx
import pytest

from custos_connector.manifest import (
    AUDIT_EVENT_FALLBACK_IGNORED,
    AUDIT_EVENT_FALLBACK_REJECTED,
    AUDIT_EVENT_FALLBACK_USED,
    CONNECTOR_MANIFEST_MEDIA_TYPE,
    DiscoveryErrorCode,
    ManifestDescriptor,
    ManifestDiscoveryError,
    discover_manifest,
    fallback_tag_for_digest,
    resolve_fallback_tag,
    resolve_referrers,
)
from custos_connector.manifest.discovery import (
    MAX_OCI_TAG_LENGTH,
)

# Use the real audit logger name (mirror of audit module) so caplog
# picks it up without touching propagation flags.
AUDIT_LOGGER = "custos_connector.audit"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


SUBJECT_DIGEST = "sha256:" + "a" * 64
SUBJECT_REPO = "team-a/oci-registry-conn"


def _build_client(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.AsyncClient:
    """Build an ``httpx.AsyncClient`` with a mock transport."""
    return httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://registry.test",
    )


def _make_referrers_body(
    *,
    descriptors: list[dict[str, object]],
) -> bytes:
    body = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": descriptors,
    }
    return json.dumps(body).encode("utf-8")


def _valid_referrer(
    *,
    digest: str = "sha256:" + "b" * 64,
    size: int = 512,
    artifact_type: str = CONNECTOR_MANIFEST_MEDIA_TYPE,
) -> dict[str, object]:
    return {
        "digest": digest,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "artifactType": artifact_type,
        "size": size,
    }


# ---------------------------------------------------------------------------
# fallback_tag_for_digest
# ---------------------------------------------------------------------------


def test_fallback_tag_for_digest_happy_path() -> None:
    digest = "sha256:" + "f" * 64
    tag = fallback_tag_for_digest(digest)
    assert tag == "custos-connector-manifest-v1_sha256-" + "f" * 64
    assert len(tag) <= MAX_OCI_TAG_LENGTH


def test_fallback_tag_rejects_unsupported_algorithm() -> None:
    digest = "sha512:" + "f" * 128
    with pytest.raises(ManifestDiscoveryError) as exc_info:
        fallback_tag_for_digest(digest)
    assert exc_info.value.code == DiscoveryErrorCode.UNSUPPORTED_DIGEST_ALGORITHM
    assert exc_info.value.digest == digest


def test_fallback_tag_rejects_missing_colon() -> None:
    with pytest.raises(ManifestDiscoveryError) as exc_info:
        fallback_tag_for_digest("sha256deadbeef")
    assert exc_info.value.code == DiscoveryErrorCode.INVALID_DIGEST_FORMAT


def test_fallback_tag_rejects_short_hex() -> None:
    with pytest.raises(ManifestDiscoveryError) as exc_info:
        fallback_tag_for_digest("sha256:abc")
    assert exc_info.value.code == DiscoveryErrorCode.INVALID_DIGEST_FORMAT


def test_fallback_tag_normalizes_uppercase_hex() -> None:
    """Uppercase hex MUST be accepted and lowered into the canonical tag form."""
    upper_digest = "sha256:" + "A" * 64
    tag = fallback_tag_for_digest(upper_digest)
    assert tag == "custos-connector-manifest-v1_sha256-" + "a" * 64
    # Mixed case behaves the same as the all-lower input.
    mixed = "sha256:" + ("Ab" * 32)
    assert fallback_tag_for_digest(mixed) == fallback_tag_for_digest(
        "sha256:" + ("ab" * 32)
    )


def test_fallback_tag_rejects_non_hex_chars() -> None:
    with pytest.raises(ManifestDiscoveryError) as exc_info:
        fallback_tag_for_digest("sha256:" + "z" * 64)
    assert exc_info.value.code == DiscoveryErrorCode.INVALID_DIGEST_FORMAT


def test_fallback_tag_rejects_non_string_digest() -> None:
    with pytest.raises(ManifestDiscoveryError) as exc_info:
        fallback_tag_for_digest(None)  # type: ignore[arg-type]
    assert exc_info.value.code == DiscoveryErrorCode.INVALID_DIGEST_FORMAT


# ---------------------------------------------------------------------------
# resolve_referrers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_referrers_returns_filtered_descriptors() -> None:
    other_artifact = _valid_referrer(artifact_type="application/vnd.some.other+json")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/v2/{SUBJECT_REPO}/referrers/{SUBJECT_DIGEST}"
        assert request.url.params.get("artifactType") == CONNECTOR_MANIFEST_MEDIA_TYPE
        return httpx.Response(
            200,
            content=_make_referrers_body(
                descriptors=[
                    _valid_referrer(digest="sha256:" + "1" * 64),
                    other_artifact,
                ]
            ),
            headers={"content-type": "application/vnd.oci.image.index.v1+json"},
        )

    async with _build_client(handler) as client:
        out = await resolve_referrers(
            client, repository=SUBJECT_REPO, subject_digest=SUBJECT_DIGEST
        )
    assert len(out) == 1
    assert out[0].digest == "sha256:" + "1" * 64
    assert out[0].media_type == "application/vnd.oci.image.manifest.v1+json"
    assert out[0].size == 512


@pytest.mark.asyncio
async def test_resolve_referrers_404_returns_empty() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    async with _build_client(handler) as client:
        assert (
            await resolve_referrers(client, repository=SUBJECT_REPO, subject_digest=SUBJECT_DIGEST)
            == []
        )


@pytest.mark.asyncio
async def test_resolve_referrers_500_returns_empty() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    async with _build_client(handler) as client:
        assert (
            await resolve_referrers(client, repository=SUBJECT_REPO, subject_digest=SUBJECT_DIGEST)
            == []
        )


@pytest.mark.asyncio
async def test_resolve_referrers_invalid_json_returns_empty() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not-json")

    async with _build_client(handler) as client:
        assert (
            await resolve_referrers(client, repository=SUBJECT_REPO, subject_digest=SUBJECT_DIGEST)
            == []
        )


@pytest.mark.asyncio
async def test_resolve_referrers_network_error_returns_empty() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("registry unreachable")

    async with _build_client(handler) as client:
        assert (
            await resolve_referrers(client, repository=SUBJECT_REPO, subject_digest=SUBJECT_DIGEST)
            == []
        )


@pytest.mark.asyncio
async def test_resolve_referrers_non_object_body_returns_empty() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"[]")

    async with _build_client(handler) as client:
        assert (
            await resolve_referrers(client, repository=SUBJECT_REPO, subject_digest=SUBJECT_DIGEST)
            == []
        )


@pytest.mark.asyncio
async def test_resolve_referrers_skips_entries_missing_fields() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=_make_referrers_body(
                descriptors=[
                    # Missing size
                    {
                        "digest": "sha256:" + "c" * 64,
                        "mediaType": "application/vnd.oci.image.manifest.v1+json",
                        "artifactType": CONNECTOR_MANIFEST_MEDIA_TYPE,
                    },
                    # Not an object at all
                    "garbage",  # type: ignore[list-item]
                    # Valid
                    _valid_referrer(digest="sha256:" + "d" * 64),
                ]
            ),
        )

    async with _build_client(handler) as client:
        out = await resolve_referrers(
            client, repository=SUBJECT_REPO, subject_digest=SUBJECT_DIGEST
        )
    assert len(out) == 1
    assert out[0].digest == "sha256:" + "d" * 64


# ---------------------------------------------------------------------------
# resolve_fallback_tag
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_fallback_tag_returns_descriptor_when_head_succeeds() -> None:
    expected_tag = "custos-connector-manifest-v1_sha256-" + "a" * 64

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "HEAD"
        assert request.url.path == f"/v2/{SUBJECT_REPO}/manifests/{expected_tag}"
        return httpx.Response(
            200,
            headers={
                "docker-content-digest": "sha256:" + "e" * 64,
                "content-type": CONNECTOR_MANIFEST_MEDIA_TYPE,
                "content-length": "1234",
            },
        )

    async with _build_client(handler) as client:
        descriptor = await resolve_fallback_tag(
            client, repository=SUBJECT_REPO, subject_digest=SUBJECT_DIGEST
        )
    assert descriptor is not None
    assert descriptor.digest == "sha256:" + "e" * 64
    assert descriptor.media_type == CONNECTOR_MANIFEST_MEDIA_TYPE
    assert descriptor.size == 1234


@pytest.mark.asyncio
async def test_resolve_fallback_tag_404_returns_none() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    async with _build_client(handler) as client:
        assert (
            await resolve_fallback_tag(
                client, repository=SUBJECT_REPO, subject_digest=SUBJECT_DIGEST
            )
            is None
        )


@pytest.mark.asyncio
async def test_resolve_fallback_tag_network_error_returns_none() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("dropped")

    async with _build_client(handler) as client:
        assert (
            await resolve_fallback_tag(
                client, repository=SUBJECT_REPO, subject_digest=SUBJECT_DIGEST
            )
            is None
        )


@pytest.mark.asyncio
async def test_resolve_fallback_tag_handles_missing_headers() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200)  # no docker-content-digest, no content-length

    async with _build_client(handler) as client:
        descriptor = await resolve_fallback_tag(
            client, repository=SUBJECT_REPO, subject_digest=SUBJECT_DIGEST
        )
    assert descriptor is not None
    assert descriptor.digest == ""  # no header -> empty
    assert descriptor.size == 0
    assert descriptor.media_type == CONNECTOR_MANIFEST_MEDIA_TYPE  # fallback


@pytest.mark.asyncio
async def test_resolve_fallback_tag_handles_non_integer_content_length() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={
                "docker-content-digest": "sha256:" + "f" * 64,
                "content-length": "not-a-number",
            },
        )

    async with _build_client(handler) as client:
        descriptor = await resolve_fallback_tag(
            client, repository=SUBJECT_REPO, subject_digest=SUBJECT_DIGEST
        )
    assert descriptor is not None
    assert descriptor.size == 0


# ---------------------------------------------------------------------------
# discover_manifest orchestration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_discover_referrers_wins_emits_fallback_ignored(
    caplog: pytest.LogCaptureFixture,
) -> None:
    expected = "sha256:" + "1" * 64

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and "/referrers/" in request.url.path:
            return httpx.Response(
                200,
                content=_make_referrers_body(descriptors=[_valid_referrer(digest=expected)]),
            )
        raise AssertionError("fallback path must not be consulted when Referrers wins")

    with caplog.at_level(logging.INFO, logger=AUDIT_LOGGER):
        async with _build_client(handler) as client:
            descriptor = await discover_manifest(
                client, repository=SUBJECT_REPO, subject_digest=SUBJECT_DIGEST
            )
    assert descriptor.digest == expected
    assert any(AUDIT_EVENT_FALLBACK_IGNORED in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_discover_referrers_empty_falls_back_emits_fallback_used(
    caplog: pytest.LogCaptureFixture,
) -> None:
    expected_digest = "sha256:" + "2" * 64

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and "/referrers/" in request.url.path:
            return httpx.Response(200, content=_make_referrers_body(descriptors=[]))
        if request.method == "HEAD" and "/manifests/" in request.url.path:
            return httpx.Response(
                200,
                headers={
                    "docker-content-digest": expected_digest,
                    "content-length": "42",
                },
            )
        raise AssertionError(f"unexpected request {request.method} {request.url}")

    with caplog.at_level(logging.INFO, logger=AUDIT_LOGGER):
        async with _build_client(handler) as client:
            descriptor = await discover_manifest(
                client, repository=SUBJECT_REPO, subject_digest=SUBJECT_DIGEST
            )
    assert descriptor.digest == expected_digest
    assert descriptor.size == 42
    assert any(AUDIT_EVENT_FALLBACK_USED in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_discover_no_manifest_found_emits_fallback_rejected(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and "/referrers/" in request.url.path:
            return httpx.Response(200, content=_make_referrers_body(descriptors=[]))
        if request.method == "HEAD" and "/manifests/" in request.url.path:
            return httpx.Response(404)
        raise AssertionError(f"unexpected {request.method} {request.url}")

    with caplog.at_level(logging.INFO, logger=AUDIT_LOGGER):
        async with _build_client(handler) as client:
            with pytest.raises(ManifestDiscoveryError) as exc_info:
                await discover_manifest(
                    client, repository=SUBJECT_REPO, subject_digest=SUBJECT_DIGEST
                )
    assert exc_info.value.code == DiscoveryErrorCode.NO_MANIFEST_FOUND
    assert any(AUDIT_EVENT_FALLBACK_REJECTED in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_discover_referrers_multiple_descriptors_ambiguous(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and "/referrers/" in request.url.path:
            return httpx.Response(
                200,
                content=_make_referrers_body(
                    descriptors=[
                        _valid_referrer(digest="sha256:" + "3" * 64),
                        _valid_referrer(digest="sha256:" + "4" * 64),
                    ]
                ),
            )
        raise AssertionError("fallback must not be consulted on ambiguous referrers")

    with caplog.at_level(logging.INFO, logger=AUDIT_LOGGER):
        async with _build_client(handler) as client:
            with pytest.raises(ManifestDiscoveryError) as exc_info:
                await discover_manifest(
                    client, repository=SUBJECT_REPO, subject_digest=SUBJECT_DIGEST
                )
    assert exc_info.value.code == DiscoveryErrorCode.AMBIGUOUS_MANIFEST
    assert any(AUDIT_EVENT_FALLBACK_REJECTED in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_discover_rejects_malformed_digest_before_network(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("network must not be touched for malformed digest")

    with caplog.at_level(logging.INFO, logger=AUDIT_LOGGER):
        async with _build_client(handler) as client:
            with pytest.raises(ManifestDiscoveryError) as exc_info:
                await discover_manifest(
                    client,
                    repository=SUBJECT_REPO,
                    subject_digest="sha512:" + "a" * 128,
                )
    assert exc_info.value.code == DiscoveryErrorCode.UNSUPPORTED_DIGEST_ALGORITHM
    assert any(AUDIT_EVENT_FALLBACK_REJECTED in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# ManifestDescriptor + error ergonomics
# ---------------------------------------------------------------------------


def test_manifest_descriptor_construction() -> None:
    descriptor = ManifestDescriptor(
        digest="sha256:" + "f" * 64,
        media_type="application/vnd.oci.image.manifest.v1+json",
        size=128,
        raw={"annotations": {"x": "y"}},
    )
    assert descriptor.size == 128
    assert descriptor.raw["annotations"]["x"] == "y"

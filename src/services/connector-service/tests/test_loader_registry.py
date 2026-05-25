"""Unit tests for :mod:`custos_connector.loader.registry` (CONN-IMPL-008).

Coverage:

* :meth:`Loader.register` happy path: discover -> fetch wrapper ->
  fetch payload -> validate -> normalize -> persist -> audit. The row
  is stored, the audit event carries the derived identity category,
  and the same call is idempotent on identical payload bytes.
* Each :class:`LoaderErrorCode` reachable via a single registration is
  exercised with a tailored MockTransport response.
* :meth:`Loader.get` / :meth:`Loader.list_versions` /
  :meth:`Loader.deprecate` pass-through behaviour.

Network is mocked via :class:`httpx.MockTransport`; no live registry is
required. SPL persistence is mocked via the in-memory
:class:`FakeCatalogAdapter` (which enforces the
:class:`custos_spl.ConflictDigest` contract on digest divergence).
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
from custos_spl.interfaces.catalog_store import CatalogStoreProvider

from custos_connector.audit import logger as audit_logger
from custos_connector.loader import (
    AUDIT_EVENT_DEPRECATION_TOGGLED,
    AUDIT_EVENT_REGISTRATION_ACCEPTED,
    AUDIT_EVENT_REGISTRATION_REJECTED,
    IdentityCategory,
    Loader,
    LoaderError,
    LoaderErrorCode,
)
from custos_connector.manifest import (
    CONNECTOR_MANIFEST_MEDIA_TYPE,
    compute_digest,
    fallback_tag_for_digest,
)
from tests._fakes import FakeCatalogAdapter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


SUBJECT_REPO = "team-a/oci-registry-conn"
SUBJECT_DIGEST = "sha256:" + "a" * 64
IMAGE_REF = f"{SUBJECT_REPO}@{SUBJECT_DIGEST}"


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "design").is_dir() and (parent / "src").is_dir():
            return parent
    raise RuntimeError("could not locate repository root from tests/")


def _load_example(name: str) -> dict[str, Any]:
    path = _repo_root() / "design" / "components" / "connector-service" / "examples" / name
    parsed: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return parsed


def _build_client(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://registry.test",
    )


def _make_loader(
    *,
    handler: Callable[[httpx.Request], httpx.Response],
    catalog: FakeCatalogAdapter | None = None,
) -> tuple[Loader, FakeCatalogAdapter, httpx.AsyncClient]:
    """Construct a Loader wired against a MockTransport + fake catalog."""
    cat = catalog if catalog is not None else FakeCatalogAdapter()
    client = _build_client(handler)
    return (
        Loader(
            catalog_store=cast(CatalogStoreProvider, cat),
            registry_client=client,
        ),
        cat,
        client,
    )


def _registry_state(
    *,
    payload: dict[str, Any],
    subject_digest: str = SUBJECT_DIGEST,
    artifact_type: str = CONNECTOR_MANIFEST_MEDIA_TYPE,
    layer_media_type: str = CONNECTOR_MANIFEST_MEDIA_TYPE,
    referrers_supported: bool = True,
    use_fallback_tag: bool = False,
    tamper_payload_bytes: bytes | None = None,
) -> tuple[bytes, str, bytes, str, int]:
    """Build the registry-side artifact wrapper for ``payload``.

    Returns ``(payload_bytes, payload_digest, wrapper_bytes,
    wrapper_digest, payload_size)``. ``payload_bytes`` is what the
    blob endpoint should return; ``wrapper_bytes`` is what the manifest
    endpoint should return.

    ``tamper_payload_bytes`` overrides the bytes the blob endpoint
    serves while leaving the wrapper digest pointing at the real
    payload — used by the ``payload-digest-mismatch`` test.
    """
    payload_bytes = json.dumps(payload).encode("utf-8")
    payload_digest = "sha256:" + hashlib.sha256(payload_bytes).hexdigest()
    wrapper = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "artifactType": artifact_type,
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:" + hashlib.sha256(b"{}").hexdigest(),
            "size": 2,
        },
        "layers": [
            {
                "mediaType": layer_media_type,
                "digest": payload_digest,
                "size": len(payload_bytes),
            }
        ],
        "subject": {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": subject_digest,
            "size": 100,
        },
    }
    wrapper_bytes = json.dumps(wrapper).encode("utf-8")
    wrapper_digest = "sha256:" + hashlib.sha256(wrapper_bytes).hexdigest()

    served_payload = tamper_payload_bytes if tamper_payload_bytes is not None else payload_bytes
    # Unused vars only used by the closure
    del referrers_supported, use_fallback_tag  # silence ruff
    return served_payload, payload_digest, wrapper_bytes, wrapper_digest, len(payload_bytes)


def _make_handler(
    *,
    payload: dict[str, Any],
    referrers_supported: bool = True,
    use_fallback_tag: bool = False,
    artifact_type: str = CONNECTOR_MANIFEST_MEDIA_TYPE,
    layer_media_type: str = CONNECTOR_MANIFEST_MEDIA_TYPE,
    tamper_payload_bytes: bytes | None = None,
    extra_referrers: Iterable[dict[str, Any]] = (),
) -> Callable[[httpx.Request], httpx.Response]:
    """Build a MockTransport handler simulating a happy-path registry."""
    served_payload, payload_digest, wrapper_bytes, wrapper_digest, payload_size = _registry_state(
        payload=payload,
        artifact_type=artifact_type,
        layer_media_type=layer_media_type,
        tamper_payload_bytes=tamper_payload_bytes,
    )
    fallback_tag = fallback_tag_for_digest(SUBJECT_DIGEST)
    referrers_path = f"/v2/{SUBJECT_REPO}/referrers/{SUBJECT_DIGEST}"
    fallback_path = f"/v2/{SUBJECT_REPO}/manifests/{fallback_tag}"
    wrapper_path = f"/v2/{SUBJECT_REPO}/manifests/{wrapper_digest}"
    blob_path = f"/v2/{SUBJECT_REPO}/blobs/{payload_digest}"

    referrer_descriptor = {
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "artifactType": CONNECTOR_MANIFEST_MEDIA_TYPE,
        "digest": wrapper_digest,
        "size": len(wrapper_bytes),
    }
    referrers_body = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [referrer_descriptor, *extra_referrers],
        }
    ).encode("utf-8")

    def handle(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == referrers_path:
            if referrers_supported:
                return httpx.Response(
                    200,
                    content=referrers_body,
                    headers={"content-type": "application/vnd.oci.image.index.v1+json"},
                )
            return httpx.Response(404)
        if path == fallback_path:
            if use_fallback_tag:
                return httpx.Response(
                    200,
                    headers={
                        "Docker-Content-Digest": wrapper_digest,
                        "Content-Type": "application/vnd.oci.image.manifest.v1+json",
                        "Content-Length": str(len(wrapper_bytes)),
                    },
                )
            return httpx.Response(404)
        if path == wrapper_path:
            return httpx.Response(
                200,
                content=wrapper_bytes,
                headers={"content-type": "application/vnd.oci.image.manifest.v1+json"},
            )
        if path == blob_path:
            return httpx.Response(
                200,
                content=served_payload,
                headers={"content-type": layer_media_type},
            )
        return httpx.Response(404, content=f"unmapped path: {path}".encode("ascii"))

    # Expose digests for tests that need to assert them.
    handle.payload_digest = payload_digest  # type: ignore[attr-defined]
    handle.wrapper_digest = wrapper_digest  # type: ignore[attr-defined]
    handle.payload_size = payload_size  # type: ignore[attr-defined]
    return handle


def _baseline_payload() -> dict[str, Any]:
    return _load_example("oci-registry-azure-key-vault-secrets.manifest.json")


def _audit_records(caplog: pytest.LogCaptureFixture, event_name: str) -> list[str]:
    """Return audit-log messages whose payload mentions ``event_name``."""
    return [
        r.message
        for r in caplog.records
        if r.name == audit_logger.name and f"name={event_name}" in r.message
    ]


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_happy_path_persists_row_and_emits_accepted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    payload = _baseline_payload()
    handler = _make_handler(payload=payload)
    loader, catalog, client = _make_loader(handler=handler)

    caplog.set_level("INFO", logger=audit_logger.name)
    async with client:
        loaded = await loader.register(IMAGE_REF)

    # Persistence: the row is in the fake.
    assert loaded.row.type == payload["metadata"]["type"]
    assert loaded.row.version == payload["metadata"]["version"]
    _, expected_digest = compute_digest(payload)
    assert loaded.row.digest == expected_digest
    assert loaded.row.parent_deprecated is False
    # The normalized manifest round-trips through the registry pipeline
    # and remains structurally equal to the input.
    assert loaded.row.normalized_manifest["spec"]["capabilities"] == payload["spec"]["capabilities"]

    # Identity category derived from azure-key-vault → kms.
    assert loaded.identity_category is IdentityCategory.KMS

    # The accepted audit event fired with the expected fields.
    accepted = _audit_records(caplog, AUDIT_EVENT_REGISTRATION_ACCEPTED)
    assert len(accepted) == 1
    assert payload["metadata"]["type"] in accepted[0]
    assert payload["metadata"]["version"] in accepted[0]
    assert "kms" in accepted[0]

    # And no rejection fired.
    assert _audit_records(caplog, AUDIT_EVENT_REGISTRATION_REJECTED) == []

    # The same row is now retrievable via Loader.get.
    fetched = await catalog.get_connector_type_version(
        payload["metadata"]["type"], payload["metadata"]["version"]
    )
    assert fetched is not None and fetched.digest == expected_digest


@pytest.mark.asyncio
async def test_register_is_idempotent_on_identical_payload() -> None:
    payload = _baseline_payload()
    handler = _make_handler(payload=payload)
    loader, _catalog, client = _make_loader(handler=handler)

    async with client:
        first = await loader.register(IMAGE_REF)
        second = await loader.register(IMAGE_REF)

    assert first.row.digest == second.row.digest
    assert first.row.published_at == second.row.published_at  # idempotent re-put


@pytest.mark.asyncio
async def test_register_uses_fallback_tag_when_referrers_empty() -> None:
    payload = _baseline_payload()
    handler = _make_handler(payload=payload, referrers_supported=False, use_fallback_tag=True)
    loader, _catalog, client = _make_loader(handler=handler)

    async with client:
        loaded = await loader.register(IMAGE_REF)

    assert loaded.row.type == payload["metadata"]["type"]


# ---------------------------------------------------------------------------
# Identity-category derivation through the loader (smoke)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("example", "expected"),
    [
        ("oci-registry-azure-key-vault-secrets.manifest.json", IdentityCategory.KMS),
        ("oci-registry-amazon-kms-secrets.manifest.json", IdentityCategory.KMS),
        ("oci-registry-azure-managed-identity.manifest.json", IdentityCategory.WORKLOAD),
        ("oci-registry-oidc-federated.manifest.json", IdentityCategory.FEDERATED),
    ],
)
@pytest.mark.asyncio
async def test_register_derives_identity_category_from_each_built_in_auth_type(
    example: str,
    expected: IdentityCategory,
) -> None:
    payload = _load_example(example)
    handler = _make_handler(payload=payload)
    loader, _catalog, client = _make_loader(handler=handler)
    async with client:
        loaded = await loader.register(IMAGE_REF)
    assert loaded.identity_category is expected


@pytest.mark.asyncio
async def test_register_resolves_x_vendor_auth_type_via_override() -> None:
    payload = _baseline_payload()
    payload["spec"]["credentials"]["authenticationType"] = "x-acme-vault"
    handler = _make_handler(payload=payload)
    cat = FakeCatalogAdapter()
    client = _build_client(handler)
    loader = Loader(
        catalog_store=cast(CatalogStoreProvider, cat),
        registry_client=client,
        vendor_identity_categories={"x-acme-vault": IdentityCategory.KMS},
    )
    async with client:
        loaded = await loader.register(IMAGE_REF)
    assert loaded.identity_category is IdentityCategory.KMS


@pytest.mark.asyncio
async def test_register_rejects_x_vendor_auth_type_without_override(
    caplog: pytest.LogCaptureFixture,
) -> None:
    payload = _baseline_payload()
    payload["spec"]["credentials"]["authenticationType"] = "x-acme-vault"
    handler = _make_handler(payload=payload)
    loader, _catalog, client = _make_loader(handler=handler)

    caplog.set_level("INFO", logger=audit_logger.name)
    async with client:
        with pytest.raises(LoaderError) as exc_info:
            await loader.register(IMAGE_REF)

    assert exc_info.value.code is LoaderErrorCode.UNKNOWN_VENDOR_AUTH_TYPE
    assert _audit_records(caplog, AUDIT_EVENT_REGISTRATION_REJECTED)


# ---------------------------------------------------------------------------
# Conflict / idempotency
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_same_type_version_different_digest_raises_conflict_digest(
    caplog: pytest.LogCaptureFixture,
) -> None:
    payload_a = _baseline_payload()
    payload_b = copy.deepcopy(payload_a)
    # Tweak a non-key field so digest diverges while (type, version) stay equal.
    payload_b["spec"]["description"] = "tweaked description so digest diverges"

    handler_a = _make_handler(payload=payload_a)
    handler_b = _make_handler(payload=payload_b)

    cat = FakeCatalogAdapter()

    async with _build_client(handler_a) as client_a:
        loader_a = Loader(
            catalog_store=cast(CatalogStoreProvider, cat),
            registry_client=client_a,
        )
        await loader_a.register(IMAGE_REF)

    caplog.set_level("INFO", logger=audit_logger.name)
    async with _build_client(handler_b) as client_b:
        loader_b = Loader(
            catalog_store=cast(CatalogStoreProvider, cat),
            registry_client=client_b,
        )
        with pytest.raises(LoaderError) as exc_info:
            await loader_b.register(IMAGE_REF)

    assert exc_info.value.code is LoaderErrorCode.CONFLICT_DIGEST
    rejected = _audit_records(caplog, AUDIT_EVENT_REGISTRATION_REJECTED)
    assert len(rejected) == 1 and "conflict-digest" in rejected[0]


# ---------------------------------------------------------------------------
# Negative paths — one per reachable LoaderErrorCode
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_rejects_invalid_image_ref() -> None:
    loader, _catalog, client = _make_loader(handler=lambda _r: httpx.Response(404))
    async with client:
        with pytest.raises(LoaderError) as exc_info:
            await loader.register("not-a-real-ref")
    assert exc_info.value.code is LoaderErrorCode.INVALID_IMAGE_REF


@pytest.mark.asyncio
async def test_register_rejects_when_discovery_returns_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Both Referrers and fallback return 404 → discovery raises
    # NO_MANIFEST_FOUND, which the loader wraps.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    loader, _catalog, client = _make_loader(handler=handler)
    caplog.set_level("INFO", logger=audit_logger.name)
    async with client:
        with pytest.raises(LoaderError) as exc_info:
            await loader.register(IMAGE_REF)

    assert exc_info.value.code is LoaderErrorCode.DISCOVERY_FAILED
    assert "no-manifest-found" in exc_info.value.detail
    assert _audit_records(caplog, AUDIT_EVENT_REGISTRATION_REJECTED)


@pytest.mark.asyncio
async def test_register_rejects_wrapper_with_wrong_artifact_type() -> None:
    payload = _baseline_payload()
    handler = _make_handler(payload=payload, artifact_type="application/vnd.example.other")
    loader, _catalog, client = _make_loader(handler=handler)
    async with client:
        with pytest.raises(LoaderError) as exc_info:
            await loader.register(IMAGE_REF)
    assert exc_info.value.code is LoaderErrorCode.INVALID_ARTIFACT_MANIFEST


@pytest.mark.asyncio
async def test_register_rejects_wrapper_with_wrong_layer_media_type() -> None:
    payload = _baseline_payload()
    handler = _make_handler(payload=payload, layer_media_type="application/octet-stream")
    loader, _catalog, client = _make_loader(handler=handler)
    async with client:
        with pytest.raises(LoaderError) as exc_info:
            await loader.register(IMAGE_REF)
    assert exc_info.value.code is LoaderErrorCode.INVALID_ARTIFACT_MANIFEST


@pytest.mark.asyncio
async def test_register_rejects_when_blob_does_not_match_layer_digest() -> None:
    payload = _baseline_payload()
    handler = _make_handler(payload=payload, tamper_payload_bytes=b'{"tampered":true}')
    loader, _catalog, client = _make_loader(handler=handler)
    async with client:
        with pytest.raises(LoaderError) as exc_info:
            await loader.register(IMAGE_REF)
    assert exc_info.value.code is LoaderErrorCode.PAYLOAD_DIGEST_MISMATCH


@pytest.mark.asyncio
async def test_register_rejects_non_json_payload() -> None:
    # We have to bypass _make_handler so the layer descriptor's digest
    # matches the served bytes (non-JSON but digest-consistent).
    body = b"not json at all"
    body_digest = "sha256:" + hashlib.sha256(body).hexdigest()
    wrapper = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "artifactType": CONNECTOR_MANIFEST_MEDIA_TYPE,
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:" + hashlib.sha256(b"{}").hexdigest(),
            "size": 2,
        },
        "layers": [
            {
                "mediaType": CONNECTOR_MANIFEST_MEDIA_TYPE,
                "digest": body_digest,
                "size": len(body),
            }
        ],
    }
    wrapper_bytes = json.dumps(wrapper).encode("utf-8")
    wrapper_digest = "sha256:" + hashlib.sha256(wrapper_bytes).hexdigest()
    referrers_body = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "artifactType": CONNECTOR_MANIFEST_MEDIA_TYPE,
                    "digest": wrapper_digest,
                    "size": len(wrapper_bytes),
                }
            ],
        }
    ).encode("utf-8")

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == f"/v2/{SUBJECT_REPO}/referrers/{SUBJECT_DIGEST}":
            return httpx.Response(200, content=referrers_body)
        if path == f"/v2/{SUBJECT_REPO}/manifests/{wrapper_digest}":
            return httpx.Response(200, content=wrapper_bytes)
        if path == f"/v2/{SUBJECT_REPO}/blobs/{body_digest}":
            return httpx.Response(200, content=body)
        return httpx.Response(404)

    loader, _catalog, client = _make_loader(handler=handler)
    async with client:
        with pytest.raises(LoaderError) as exc_info:
            await loader.register(IMAGE_REF)
    assert exc_info.value.code is LoaderErrorCode.PAYLOAD_NOT_JSON


@pytest.mark.asyncio
async def test_register_rejects_when_manifest_validation_fails() -> None:
    payload = _baseline_payload()
    # Schema rejection: nuke a required root field.
    del payload["spec"]
    handler = _make_handler(payload=payload)
    loader, _catalog, client = _make_loader(handler=handler)
    async with client:
        with pytest.raises(LoaderError) as exc_info:
            await loader.register(IMAGE_REF)
    assert exc_info.value.code is LoaderErrorCode.MANIFEST_INVALID


@pytest.mark.asyncio
async def test_register_rejects_when_wrapper_fetch_returns_500() -> None:
    payload = _baseline_payload()
    handler = _make_handler(payload=payload)
    # Mutate to return 500 on the wrapper GET.
    wrapper_digest = handler.wrapper_digest  # type: ignore[attr-defined]
    payload_digest = handler.payload_digest  # type: ignore[attr-defined]
    referrers_body = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "artifactType": CONNECTOR_MANIFEST_MEDIA_TYPE,
                    "digest": wrapper_digest,
                    "size": 1,
                }
            ],
        }
    ).encode("utf-8")

    def wrapper_500(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == f"/v2/{SUBJECT_REPO}/referrers/{SUBJECT_DIGEST}":
            return httpx.Response(200, content=referrers_body)
        if path == f"/v2/{SUBJECT_REPO}/manifests/{wrapper_digest}":
            return httpx.Response(500, content=b"oops")
        if path == f"/v2/{SUBJECT_REPO}/blobs/{payload_digest}":
            return httpx.Response(200, content=b"unused")
        return httpx.Response(404)

    loader, _catalog, client = _make_loader(handler=wrapper_500)
    async with client:
        with pytest.raises(LoaderError) as exc_info:
            await loader.register(IMAGE_REF)
    assert exc_info.value.code is LoaderErrorCode.PAYLOAD_FETCH_FAILED


# ---------------------------------------------------------------------------
# Pass-through surface: get, list_versions, deprecate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_returns_none_for_unknown_version() -> None:
    loader, _catalog, client = _make_loader(handler=lambda _r: httpx.Response(404))
    async with client:
        assert await loader.get("oci-registry", "0.0.0-unknown") is None


@pytest.mark.asyncio
async def test_list_versions_walks_pagination_to_completion() -> None:
    cat = FakeCatalogAdapter()
    # Seed three versions directly via the fake's put surface.
    base = _baseline_payload()
    for v in ("2.3.1-akv.1", "2.3.2-akv.1", "2.4.0-akv.1"):
        payload = copy.deepcopy(base)
        payload["metadata"]["version"] = v
        _, digest = compute_digest(payload)
        await cat.put_connector_type_version(
            type=payload["metadata"]["type"],
            version=v,
            digest=digest,
            normalized_manifest=payload,
        )
    loader = Loader(
        catalog_store=cast(CatalogStoreProvider, cat),
        registry_client=_build_client(lambda _r: httpx.Response(404)),
    )
    rows = await loader.list_versions("oci-registry", limit=2)
    assert [r.version for r in rows] == ["2.3.1-akv.1", "2.3.2-akv.1"]
    rows_all = await loader.list_versions("oci-registry")
    assert [r.version for r in rows_all] == ["2.3.1-akv.1", "2.3.2-akv.1", "2.4.0-akv.1"]


@pytest.mark.asyncio
async def test_deprecate_toggles_flag_and_emits_audit(
    caplog: pytest.LogCaptureFixture,
) -> None:
    payload = _baseline_payload()
    handler = _make_handler(payload=payload)
    loader, cat, client = _make_loader(handler=handler)
    async with client:
        loaded = await loader.register(IMAGE_REF)

    caplog.set_level("INFO", logger=audit_logger.name)
    await loader.deprecate(payload["metadata"]["type"])

    fetched = await cat.get_connector_type_version(loaded.row.type, loaded.row.version)
    assert fetched is not None and fetched.parent_deprecated is True

    toggled = _audit_records(caplog, AUDIT_EVENT_DEPRECATION_TOGGLED)
    assert len(toggled) == 1
    assert "deprecated" in toggled[0]

    # Un-deprecate round-trips back to False.
    await loader.deprecate(payload["metadata"]["type"], deprecated=False)
    fetched2 = await cat.get_connector_type_version(loaded.row.type, loaded.row.version)
    assert fetched2 is not None and fetched2.parent_deprecated is False

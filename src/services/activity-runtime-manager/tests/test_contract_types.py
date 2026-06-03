"""Tests for the Activity Contract platform types (ARM-IMPL-003)."""

from __future__ import annotations

import pytest
from pydantic import TypeAdapter, ValidationError

from custos_arm.contract import ArtifactRef, ConnectorRef, Duration, ImageRef, OciDescriptor


def test_image_ref_round_trips() -> None:
    payload = {"ref": "ghcr.io/acme/app:v1", "digest": "sha256:abc"}
    ref = ImageRef.model_validate(payload)
    assert ref.model_dump(by_alias=True, exclude_none=True) == payload


def test_image_ref_digest_optional() -> None:
    ref = ImageRef(ref="ghcr.io/acme/app:v1")
    assert ref.digest is None
    assert ref.model_dump(by_alias=True, exclude_none=True) == {"ref": "ghcr.io/acme/app:v1"}


def test_image_ref_requires_non_empty_ref() -> None:
    with pytest.raises(ValidationError):
        ImageRef(ref="")


def test_oci_descriptor_round_trips() -> None:
    payload = {
        "ref": "ghcr.io/acme/app@sha256:abc",
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "digest": "sha256:abc",
        "size": 1234,
        "artifactType": "application/vnd.cyclonedx+json",
        "annotations": {"org.opencontainers.image.title": "app"},
    }
    desc = OciDescriptor.model_validate(payload)
    assert desc.media_type == payload["mediaType"]
    assert desc.model_dump(by_alias=True, exclude_none=True) == payload


def test_oci_descriptor_optional_fields_omitted() -> None:
    desc = OciDescriptor(
        ref="ghcr.io/acme/app@sha256:abc",
        mediaType="application/vnd.oci.image.manifest.v1+json",
        digest="sha256:abc",
        size=10,
    )
    dumped = desc.model_dump(by_alias=True, exclude_none=True)
    assert "artifactType" not in dumped
    assert "annotations" not in dumped


def test_oci_descriptor_rejects_negative_size() -> None:
    with pytest.raises(ValidationError):
        OciDescriptor(ref="r", mediaType="m", digest="d", size=-1)


def test_connector_ref_round_trips_with_default_labels() -> None:
    payload = {"host": "ghcr.io", "endpoint": "https://ghcr.io", "type": "oci-registry"}
    ref = ConnectorRef.model_validate(payload)
    assert ref.labels == {}
    assert ref.model_dump(by_alias=True, exclude_none=True) == {**payload, "labels": {}}


def test_artifact_ref_author_shape() -> None:
    payload = {"kind": "ArtifactRef", "name": "report"}
    ref = ArtifactRef.model_validate(payload)
    assert ref.kind == "ArtifactRef"
    assert ref.model_dump(by_alias=True, exclude_none=True) == payload


def test_artifact_ref_finalized_shape_round_trips() -> None:
    payload = {
        "kind": "ArtifactRef",
        "name": "report",
        "id": "art-9f3a",
        "mediaType": "application/vnd.cyclonedx+json",
        "digest": "sha256:def",
        "size": 84231,
    }
    ref = ArtifactRef.model_validate(payload)
    assert ref.model_dump(by_alias=True, exclude_none=True) == payload


def test_artifact_ref_kind_is_fixed() -> None:
    with pytest.raises(ValidationError):
        ArtifactRef.model_validate({"kind": "ImageRef", "name": "x"})


def test_artifact_ref_requires_name() -> None:
    with pytest.raises(ValidationError):
        ArtifactRef.model_validate({"kind": "ArtifactRef", "name": ""})


@pytest.mark.parametrize("value", ["PT30S", "PT1H30M", "P1D", "P1W"])
def test_duration_accepts_valid_iso8601(value: str) -> None:
    adapter = TypeAdapter(Duration)
    assert adapter.validate_python(value) == value


@pytest.mark.parametrize("value", ["", "30s", "PT", "P", "P1Y", "nonsense"])
def test_duration_rejects_invalid(value: str) -> None:
    adapter = TypeAdapter(Duration)
    with pytest.raises(ValidationError):
        adapter.validate_python(value)


def test_duration_serializes_as_bare_string() -> None:
    adapter = TypeAdapter(Duration)
    assert adapter.dump_python("PT5M") == "PT5M"

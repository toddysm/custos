"""Tests for the shared digest-pinned image-ref resolver (DEVCLI-IMPL-006)."""

from __future__ import annotations

import pytest

from custosctl import imageref, shell
from custosctl.config import Settings, Target

_DIGEST = "sha256:" + "c" * 64


def _settings(**overrides: object) -> Settings:
    base: dict[str, object] = {"target": Target.REMOTE, "image_prefix": "ghcr.io/acme/custos"}
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


def test_derives_and_resolves_digest(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shell, "resolve_image_digest", lambda ref: _DIGEST)
    image, ref = imageref.resolve_image_ref(
        _settings(), name="dockerhub", version="1.2.0", image_ref=None
    )
    assert image == "ghcr.io/acme/custos/dockerhub:v1.2.0"
    assert ref == f"ghcr.io/acme/custos/dockerhub:v1.2.0@{_DIGEST}"


def test_explicit_pinned_ref_passes() -> None:
    pinned = f"ghcr.io/acme/custos/x:v1@{_DIGEST}"
    image, ref = imageref.resolve_image_ref(_settings(), name="x", version="1", image_ref=pinned)
    assert image == "ghcr.io/acme/custos/x:v1"
    assert ref == pinned


def test_rejects_non_digest_ref() -> None:
    with pytest.raises(RuntimeError, match="digest-pinned"):
        imageref.resolve_image_ref(
            _settings(), name="x", version="1", image_ref="ghcr.io/acme/x:v1"
        )


def test_rejects_non_ghcr_explicit_ref() -> None:
    with pytest.raises(RuntimeError, match="GHCR"):
        imageref.resolve_image_ref(
            _settings(), name="x", version="1", image_ref=f"docker.io/acme/x:v1@{_DIGEST}"
        )


def test_rejects_non_ghcr_prefix_on_derive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shell, "resolve_image_digest", lambda ref: _DIGEST)
    with pytest.raises(RuntimeError, match="GHCR"):
        imageref.resolve_image_ref(
            _settings(image_prefix="docker.io/acme"), name="x", version="1", image_ref=None
        )

"""Shared digest-pinned image-reference resolution for the catalog commands.

Both `connector register` and `activity register` need a **digest-pinned**
GHCR reference: either an explicit ``--image-ref`` (validated here) or one
derived from ``CUSTOS_IMAGE_PREFIX`` and resolved to a digest via the registry
tooling in :mod:`custosctl.shell`.
"""

from __future__ import annotations

import re

from custosctl import shell
from custosctl.config import Settings

#: A fully digest-pinned reference: ``<repo>@sha256:<64 lowercase hex>``.
DIGEST_PINNED_RE = re.compile(r".+@sha256:[0-9a-f]{64}$")


def resolve_image_ref(
    settings: Settings,
    *,
    name: str,
    version: str,
    image_ref: str | None,
) -> tuple[str, str]:
    """Return ``(image, ref)`` for the extension named ``name`` at ``version``.

    ``image`` is the tag-form reference (``<repo>:v<version>``) and ``ref`` is
    the digest-pinned form (``<repo>:v<version>@sha256:…``). When ``image_ref``
    is given it must already be digest-pinned; otherwise the image is derived
    from ``CUSTOS_IMAGE_PREFIX`` and its digest resolved via docker buildx /
    skopeo / crane.

    ``name`` is the image repository basename, which by the OOTB publish
    convention is the extension *folder* name (e.g. ``dockerhub``, ``copy-image``)
    — where ``publish-<kind>-<name>.yml`` and ``seed-ootb.sh`` push. It is
    distinct from the registered type (manifest ``metadata.type``, e.g.
    ``custos-dockerhub``); pass ``--image-ref`` when the image lives elsewhere.
    """
    if image_ref:
        if not DIGEST_PINNED_RE.match(image_ref):
            raise RuntimeError(
                "--image-ref must be a digest-pinned reference "
                f"(<repo>@sha256:<64 hex>); got {image_ref!r}"
            )
        return image_ref.split("@", 1)[0], image_ref
    image = f"{settings.image_prefix}/{name}:v{version}"
    digest = shell.resolve_image_digest(image)
    return image, f"{image}@{digest}"


__all__ = ["DIGEST_PINNED_RE", "resolve_image_ref"]

"""Custos Catalog Service (COMP-007).

This package hosts the Catalog Service runtime: workflow + template
definition lifecycle, the activity-type and connector-type read-side index,
and the publish-time validation gate (schema, CEL syntactic + name-binding,
reference resolution, digest pinning).

See the design at:
https://github.com/toddysm/custos/blob/main/design/components/catalog-service/design.md

The scaffold ships only the package skeleton and a placeholder
:func:`create_app` factory. Real wiring lands incrementally across
CS-IMPL-003 through CS-IMPL-022 (see issue #226).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - import-time only, never executed
    from fastapi import FastAPI

__all__ = ["__version__", "create_app"]

__version__ = "0.1.0"


def create_app() -> FastAPI:
    """Build and return the Catalog Service FastAPI application.

    This is the canonical entry point used by ``custos_catalog.__main__``
    and by ASGI servers. The real implementation lands in CS-IMPL-017
    (API Adapter). Until then, the factory raises :class:`NotImplementedError`
    so any accidental wiring fails loudly rather than silently serving
    a half-built surface.
    """
    raise NotImplementedError(
        "custos_catalog.create_app() is a scaffold placeholder. "
        "The FastAPI application is wired in CS-IMPL-017 "
        "(see https://github.com/toddysm/custos/issues/218)."
    )

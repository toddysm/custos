"""FastAPI HTTP surface for Catalog Service (CS-IMPL-017).

Per :mod:`design/components/catalog-service/design.md` § Public Interface,
Catalog exposes 16 REST routes (plus 2 internal RPC reads landing in
CS-IMPL-018). The routers live in :mod:`custos_catalog.api.routes`;
:mod:`custos_catalog.api.errors` maps every manager-level exception to a
stable HTTP envelope; :mod:`custos_catalog.api.dependencies` builds the
per-request manager instances out of :class:`Providers` so the factory
stays import-safe and the lifespan stays the only place that touches the
network.
"""

from __future__ import annotations

from custos_catalog.api.errors import register_exception_handlers
from custos_catalog.api.routes import all_routers

__all__ = [
    "all_routers",
    "register_exception_handlers",
]

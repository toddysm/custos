"""Catalog service Manager classes.

Manager classes orchestrate the publish / read / lifecycle pipelines
on top of the storage-provider-layer surface. Each manager is a thin,
testable wrapper that takes a :class:`DefinitionStoreProvider` plus
any collaborators it needs (activity registry, connector client,
versioning manager) and exposes one logical operation per method.

The split keeps each manager small enough to unit test with hand-rolled
fakes that satisfy the SPL Protocol surface, and lets the FastAPI
routers (CS-IMPL-017) compose them without dragging the full
application state into request handlers.
"""

from __future__ import annotations

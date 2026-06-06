"""Protocols the I/O Broker reads produced artifacts through.

The broker never touches the sandbox filesystem directly. The RuntimeDriver
(ARM-IMPL-013) exposes the materialized ``/custos/out/artifacts/`` tree through
an :class:`OutputArtifactReader`, keeping the broker's two-phase finalization
independent of any concrete runtime (OCI container today; HTTP / WASM later).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol, runtime_checkable


@runtime_checkable
class OutputArtifactReader(Protocol):
    """Read-only view over the activity's produced ``/custos/out/artifacts/`` tree.

    Artifacts are keyed by their manifest-declared ``spec.outputs.artifacts[].name``.
    """

    def has(self, name: str) -> bool:
        """Return ``True`` when the activity produced an artifact named ``name``."""
        ...

    def open(self, name: str) -> AsyncIterator[bytes]:
        """Stream the bytes of the produced artifact ``name``.

        Callers must only invoke this for names where :meth:`has` is ``True``.
        """
        ...


@runtime_checkable
class InputArtifactWriter(Protocol):
    """Write-only sink for upstream artifacts materialized under ``/custos/in/artifacts/``.

    The broker fetches each consumed ``ArtifactRef`` input from the
    ``ArtifactStoreProvider`` and hands the bytes to this writer, keeping the
    broker independent of the sandbox filesystem (the Scheduler owns that
    boundary, just as it does for produced outputs).
    """

    async def write(self, name: str, data: bytes) -> None:
        """Materialize ``data`` as the input artifact named ``name``."""
        ...


__all__ = ["InputArtifactWriter", "OutputArtifactReader"]

"""Filesystem adapters bridging the pure I/O Broker to a sandbox handle.

The :class:`~custos_arm.io.IOBroker` is a pure transform: it builds the
``inputs.json`` / ``ctx.json`` envelopes and finalizes ``outputs.json`` without
ever touching the sandbox filesystem. The Scheduler owns that boundary — it
writes the input envelopes the driver staged under
:attr:`~custos_arm.runtime.SandboxHandle.input_root`, reads the raw
``outputs.json`` back from :attr:`~custos_arm.runtime.OutputBundle.root`, and
exposes the produced-artifact tree and the secret sink the Secret Injector and
I/O Broker consume.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Final

from custos_arm.contract import CtxEnvelope, InputsEnvelope

#: Contract filenames under ``/custos/in`` and ``/custos/out``.
INPUTS_FILENAME: Final[str] = "inputs.json"
CTX_FILENAME: Final[str] = "ctx.json"
OUTPUTS_FILENAME: Final[str] = "outputs.json"

#: Subdirectory of ``/custos/out`` holding produced file artifacts.
ARTIFACTS_SUBDIR: Final[str] = "artifacts"

#: Streaming chunk size for produced artifacts.
_CHUNK_SIZE: Final[int] = 64 * 1024


def write_inputs(input_root: Path, envelope: InputsEnvelope) -> None:
    """Write the ``inputs.json`` envelope into the sandbox input tree."""
    _write_json(input_root / INPUTS_FILENAME, envelope.model_dump_json(by_alias=True))


def write_ctx(input_root: Path, envelope: CtxEnvelope) -> None:
    """Write the ``ctx.json`` execution context into the sandbox input tree."""
    _write_json(input_root / CTX_FILENAME, envelope.model_dump_json(by_alias=True))


def read_outputs(output_root: Path) -> bytes | None:
    """Return the raw ``outputs.json`` bytes, or ``None`` when none was written."""
    path = output_root / OUTPUTS_FILENAME
    if not path.is_file():
        return None
    return path.read_bytes()


def _write_json(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8")


class FilesystemSecretSink:
    """A :class:`~custos_arm.secrets.SecretSink` writing under ``input_root``.

    Secret files and the sidecar bootstrap token are written relative to the
    sandbox input tree the driver staged, with the mode the Secret Injector
    requests (``0o400`` for credential material).
    """

    def __init__(self, input_root: Path) -> None:
        self._input_root = input_root

    async def write_secret(self, *, relative_path: str, content: bytes, mode: int) -> None:
        """Write ``content`` to ``input_root/relative_path`` with ``mode``."""
        path = self._input_root.joinpath(*relative_path.split("/"))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        path.chmod(mode)


class FilesystemArtifactReader:
    """An :class:`~custos_arm.io.OutputArtifactReader` over ``/custos/out/artifacts``."""

    def __init__(self, output_root: Path) -> None:
        self._artifacts_root = output_root / ARTIFACTS_SUBDIR

    def has(self, name: str) -> bool:
        """Return ``True`` when the activity produced an artifact named ``name``."""
        return (self._artifacts_root / name).is_file()

    async def open(self, name: str) -> AsyncIterator[bytes]:
        """Stream the bytes of the produced artifact ``name`` in chunks."""
        path = self._artifacts_root / name
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(_CHUNK_SIZE)
                if not chunk:
                    break
                yield chunk


__all__ = [
    "ARTIFACTS_SUBDIR",
    "CTX_FILENAME",
    "INPUTS_FILENAME",
    "OUTPUTS_FILENAME",
    "FilesystemArtifactReader",
    "FilesystemSecretSink",
    "read_outputs",
    "write_ctx",
    "write_inputs",
]

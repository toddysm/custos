"""Runtime-driver dispatcher errors (ARM-IMPL-013)."""

from __future__ import annotations

from collections.abc import Iterable

__all__ = [
    "DuplicateRuntimeKindError",
    "RuntimeDriverError",
    "UnknownRuntimeKindError",
]


class RuntimeDriverError(RuntimeError):
    """Base class for runtime-driver dispatch failures."""


class UnknownRuntimeKindError(RuntimeDriverError):
    """Raised when no driver is registered for a requested ``runtime.kind``."""

    def __init__(self, kind: str, registered: Iterable[str]) -> None:
        self.kind = kind
        self.registered = tuple(registered)
        registered_repr = ", ".join(repr(k) for k in self.registered) or "<none>"
        super().__init__(
            f"no runtime driver registered for kind {kind!r}; registered kinds: {registered_repr}"
        )


class DuplicateRuntimeKindError(RuntimeDriverError):
    """Raised when two drivers claim the same ``runtime.kind``."""

    def __init__(self, kind: str) -> None:
        self.kind = kind
        super().__init__(f"a runtime driver is already registered for kind {kind!r}")

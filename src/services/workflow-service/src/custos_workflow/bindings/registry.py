"""Activity output schema registry for the Definition Compiler (WF-IMPL-017).

At publish time the Catalog Service holds the canonical
:class:`ActivityType` registry that maps a fully-qualified activity
reference (``<ns>/<type>@<version>``) to the activity's structural
input + output JSON Schemas. The Definition Compiler needs only the
**output** side: at type-check time it has to know what
``steps.<id>.outputs.*`` looks like for every prior activity step.

This module exposes a typed :class:`ActivityTypeRegistry` Protocol so
the Compiler does not depend on a concrete client today. The real
Catalog wiring is a follow-up cross-component task (parallel to
CS-IMPL-023, #224); for tests and local development the
:class:`InMemoryActivityTypeRegistry` seeded from a dict is enough.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol


class ActivityTypeNotFoundError(LookupError):
    """Raised when an activity reference is not known to the registry.

    The missing reference is always preserved as ``args[0]`` *and* as
    the :attr:`activity_ref` attribute so downstream taxonomy code
    (WF-IMPL-024) can extract it programmatically. An optional
    diagnostic ``message`` augments ``str(exc)`` for human readers
    without disturbing the machine-readable form. Re-raise sites that
    want a richer message should use ``raise ActivityTypeNotFoundError(
    activity_ref, message=...) from exc`` rather than re-formatting
    ``args[0]``.
    """

    def __init__(self, activity_ref: str, *, message: str | None = None) -> None:
        # ``args[0]`` MUST remain the raw reference so callers can
        # rely on it as a stable, machine-readable handle.
        super().__init__(activity_ref)
        self.activity_ref = activity_ref
        self._message = message

    def __str__(self) -> str:
        return self._message if self._message is not None else self.activity_ref


class ActivityTypeRegistry(Protocol):
    """Read-only view of the activity-type catalog.

    The Compiler only needs ``outputs`` schemas; ``inputs`` validation
    of the step's ``with:`` block is the Catalog Service's job at
    publish time (CS-IMPL-006).
    """

    def get_outputs_schema(self, activity_ref: str) -> Mapping[str, Any]:
        """Return the JSON Schema describing the activity's outputs.

        Args:
            activity_ref: Fully-qualified ``<namespace>/<type>@<version>``
                reference. Must match the form enforced by
                ``ActivityStep`` (WF-IMPL-016) — callers do not need to
                re-validate.

        Returns:
            A JSON Schema mapping (typically an ``object`` schema with
            ``properties``). The returned mapping should be treated as
            read-only by callers.

        Raises:
            ActivityTypeNotFoundError: ``activity_ref`` is unknown.
        """
        ...


class InMemoryActivityTypeRegistry:
    """Dict-backed :class:`ActivityTypeRegistry` for tests / local dev.

    Seed once with a mapping of activity ref → outputs schema; lookups
    are O(1). Tests reach for this directly; real deployments wire a
    Catalog-backed implementation later.
    """

    def __init__(self, outputs: Mapping[str, Mapping[str, Any]]) -> None:
        # Defensive copy so the registry is immutable from the caller's
        # perspective after construction. Inner schemas are not copied
        # (cheap, callers should not mutate them).
        self._outputs: dict[str, Mapping[str, Any]] = dict(outputs)

    def get_outputs_schema(self, activity_ref: str) -> Mapping[str, Any]:
        try:
            return self._outputs[activity_ref]
        except KeyError as exc:
            raise ActivityTypeNotFoundError(activity_ref) from exc

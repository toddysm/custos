"""Activity schemas and authoritative connector requirements for compilation.

At publish time the Catalog Service holds the canonical
:class:`ActivityType` registry that maps a fully-qualified activity
reference (``<ns>/<type>@<version>``) to the activity's structural
input + output JSON Schemas. At type-check time the compiler must know what
``steps.<id>.outputs.*`` looks like for every prior activity step.

The base protocol keeps legacy injected output registries compatible.
Execution-aware Catalog reads produce a :class:`ResolvedActivityTypeRegistry`
whose connector-slot facts are also retained in the compiled graph. Replay
never substitutes current manifest metadata for an accepted run's snapshot.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictStr


class ConnectorRequirement(BaseModel):
    """Immutable connector-slot authority read from a pinned activity manifest."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: StrictStr = Field(min_length=1)
    type: StrictStr = Field(min_length=1)
    required: StrictBool
    capabilities: tuple[StrictStr, ...] = ()


@runtime_checkable
class ActivityConnectorRegistry(Protocol):
    """Optional registry capability supplying replayable activity slot requirements."""

    def get_connector_requirements(self, activity_ref: str) -> tuple[ConnectorRequirement, ...]:
        """Return the manifest's exact slot set, including an authoritative empty set."""
        ...


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


class ResolvedActivityTypeRegistry(InMemoryActivityTypeRegistry):
    """Per-lookup Catalog facts; never a mutable cross-workspace or per-process cache.

    Every response must name the requested version and contain matching manifest
    metadata. Only output schemas and connector requirements are retained.
    """

    def __init__(self, responses: Mapping[str, Mapping[str, Any]]) -> None:
        """Validate actual Catalog activity responses before any workflow is accepted."""
        outputs: dict[str, Mapping[str, Any]] = {}
        self._requirements: dict[str, tuple[ConnectorRequirement, ...]] = {}
        for ref, body in responses.items():
            path, separator, version = ref.partition("@")
            namespace, slash, name = path.partition("/")
            if not separator or not slash or not version:
                raise ValueError("invalid pinned activity reference")
            expected = {"namespace": namespace, "type": name, "version": version}
            if any(body.get(key) != value for key, value in expected.items()):
                raise ValueError("Catalog activity response changed its requested reference")
            manifest = body.get("normalizedManifest")
            if not isinstance(manifest, Mapping):
                raise ValueError("Catalog activity manifest is required")
            metadata = manifest.get("metadata")
            if not isinstance(metadata, Mapping) or any(
                metadata.get(key) != value for key, value in expected.items()
            ):
                raise ValueError("activity manifest metadata does not match its reference")
            spec = manifest.get("spec")
            if not isinstance(spec, Mapping):
                raise ValueError("activity manifest spec is required")
            output = spec.get("outputs")
            if not isinstance(output, Mapping) or not isinstance(output.get("schema"), Mapping):
                raise ValueError("activity output schema is required")
            slots = spec.get("connectors", [])
            if not isinstance(slots, list):
                raise ValueError("activity connectors must be an array")
            requirements = tuple(ConnectorRequirement.model_validate(slot) for slot in slots)
            if len({item.name for item in requirements}) != len(requirements):
                raise ValueError("activity connector slots must be unique")
            outputs[ref] = dict(output["schema"])
            self._requirements[ref] = requirements
        super().__init__(outputs)

    def get_connector_requirements(self, activity_ref: str) -> tuple[ConnectorRequirement, ...]:
        """Return immutable slot facts for exactly the requested activity version."""
        try:
            return self._requirements[activity_ref]
        except KeyError as exc:
            raise ActivityTypeNotFoundError(activity_ref) from exc

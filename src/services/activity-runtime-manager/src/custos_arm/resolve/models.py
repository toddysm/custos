"""Activity reference grammar and the resolved ``ActivityTypeVersion`` record.

An ``activityRef`` is the fully-qualified, workspace-scoped name a workflow
step pins to. Two forms are accepted (design § Versioning, catalog reference
grammar):

* an **exact pin** — ``<namespace>/<type>@<MAJOR.MINOR.PATCH>`` (immutable); and
* a **major ref** — ``<namespace>/<type>@<MAJOR>`` (a moving pointer the
  Catalog resolves to the latest non-deprecated minor/patch).

The resolver turns either form into an :class:`ActivityTypeVersion`: the
single, immutable, content-addressed record the rest of ARM executes against.
Only exact-pin resolutions are cacheable — a major ref is a moving target and
must be re-read every time.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict

from custos_arm.manifest import (
    ActivityManifest,
    ConnectorSpec,
    IsolationTier,
    OutputsSpec,
    Resources,
    Runtime,
)

__all__ = [
    "ActivityRef",
    "ActivityTypeVersion",
]

#: ``<namespace>/<type>@<version>`` — namespace and type are single
#: ``/``-free segments; the version is either ``MAJOR`` or ``MAJOR.MINOR.PATCH``
#: (validated separately so the error message can distinguish the two faults).
_REF_PATTERN: re.Pattern[str] = re.compile(
    r"^(?P<namespace>[^/@]+)/(?P<type>[^/@]+)@(?P<version>[^/@]+)$"
)

#: An exact semver pin — three numeric components, no leading zeros.
_EXACT_VERSION_PATTERN: re.Pattern[str] = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")

#: A bare major ref — a single numeric component, no leading zeros.
_MAJOR_VERSION_PATTERN: re.Pattern[str] = re.compile(r"^(0|[1-9]\d*)$")


@dataclass(frozen=True, slots=True)
class ActivityRef:
    """A parsed, validated ``<namespace>/<type>@<version>`` activity reference.

    Instances are frozen + hashable so they can key the resolver's immutable
    cache directly.
    """

    namespace: str
    type: str
    version: str

    @classmethod
    def parse(cls, ref: str) -> ActivityRef:
        """Parse a fully-qualified ``activityRef`` string.

        :raises ValueError: when ``ref`` is not of the form
            ``<namespace>/<type>@<MAJOR>`` or
            ``<namespace>/<type>@<MAJOR.MINOR.PATCH>``.
        """
        match = _REF_PATTERN.match(ref)
        if match is None:
            raise ValueError(
                f"activity ref {ref!r} must be of the form <namespace>/<type>@<version>"
            )
        version = match["version"]
        if not (_EXACT_VERSION_PATTERN.match(version) or _MAJOR_VERSION_PATTERN.match(version)):
            raise ValueError(
                f"activity ref version {version!r} must be a bare major "
                "(e.g. '2') or an exact semver pin (e.g. '2.1.0')"
            )
        return cls(namespace=match["namespace"], type=match["type"], version=version)

    @property
    def is_exact_pin(self) -> bool:
        """Whether this ref names one immutable version (cacheable)."""
        return _EXACT_VERSION_PATTERN.match(self.version) is not None

    def __str__(self) -> str:
        return f"{self.namespace}/{self.type}@{self.version}"


class ActivityTypeVersion(BaseModel):
    """A pinned, immutable activity type version resolved from the Catalog.

    Carries the content-addressed image ``digest`` and the full parsed
    :class:`~custos_arm.manifest.ActivityManifest` — the schemas, connectors,
    resources, and isolation floor the Scheduler wires the sandbox from. The
    ``namespace``/``type``/``version`` here are the *resolved* exact triple,
    which differs from the requested ref when a major ref was supplied.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    namespace: str
    type: str
    version: str
    digest: str
    manifest: ActivityManifest
    parent_deprecated: bool = False
    published_at: datetime | None = None

    @property
    def ref(self) -> ActivityRef:
        """The resolved exact-pin reference."""
        return ActivityRef(namespace=self.namespace, type=self.type, version=self.version)

    @property
    def runtime(self) -> Runtime:
        """The activity's packaging (pinned image + isolation hint)."""
        return self.manifest.spec.runtime

    @property
    def input_schema(self) -> dict[str, Any]:
        """The JSON Schema the Scheduler validates ``inputs.json`` against."""
        return self.manifest.spec.inputs.json_schema

    @property
    def outputs(self) -> OutputsSpec:
        """The output schema plus declared file artifacts."""
        return self.manifest.spec.outputs

    @property
    def connectors(self) -> list[ConnectorSpec]:
        """The connector slots the workflow must bind."""
        return self.manifest.spec.connectors

    @property
    def resources(self) -> Resources:
        """The activity's requested resource envelope and timeout."""
        return self.manifest.spec.resources

    @property
    def isolation_floor(self) -> IsolationTier:
        """The minimum isolation tier, defaulting to ``process`` when silent."""
        isolation = self.runtime.isolation
        if isolation is None or isolation.min_tier is None:
            return IsolationTier.PROCESS
        return isolation.min_tier

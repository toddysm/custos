"""The file-based ARM activity contract (COPY-IMPL-002).

ARM mounts ``/custos/in`` (read-only, populated before start) and
``/custos/out`` (collected after exit) into the activity Pod. This module
is the typed, stdlib-only adapter over that contract:

* read :class:`InputsEnvelope` (``inputs.json``) and :class:`Context`
  (``ctx.json``);
* read per-slot injected secrets and the sidecar bootstrap token;
* write the success / failure ``outputs.json`` envelope, file artifacts,
  and optional ``audit.jsonl`` lines.

See ``docs/developers/activity-author.md`` for the wire contract. The base
directory defaults to ``/custos`` but is overridable (``CUSTOS_IO_ROOT``)
so the contract can be exercised in a temp tree under test.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

#: Authoritative retry signal carried in the failure envelope (ADR-008).
ErrorClass = Literal["permanent", "retryable", "cancelled"]

_SCHEMA_VERSION = "1"
_CONTRACT_VERSION = "1"
_DEFAULT_ROOT = "/custos"
_IO_ROOT_ENV = "CUSTOS_IO_ROOT"


class ActivityError(Exception):
    """A categorized activity failure that maps to the failure envelope.

    ``code`` is the manifest-declared error code (or a generic
    ``activity.*`` code); ``error_class`` is the authoritative retry
    signal written to ``outputs.json`` and reflected in the exit code.
    """

    def __init__(self, code: str, error_class: ErrorClass, message: str) -> None:
        self.code = code
        self.error_class: ErrorClass = error_class
        self.message = message
        super().__init__(f"{code}: {message}")


def exit_code_for(error_class: ErrorClass) -> int:
    """Map a failure class to the ADR-008 fallback exit code.

    ``outputs.json`` is the authoritative signal; the exit code is the
    fallback (``2`` permanent, ``1`` retryable; ``cancelled`` also exits
    ``1`` since the envelope carries the real class).
    """
    return 2 if error_class == "permanent" else 1


# ---------------------------------------------------------------------------
# Envelope models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ActivityIdentity:
    """The activity ``type``/``version`` echoed in ``inputs.json``."""

    type: str
    version: str


@dataclass(frozen=True)
class StepRef:
    """The originating run/step/attempt."""

    run_id: str
    step_id: str
    attempt: int


@dataclass(frozen=True)
class InputsEnvelope:
    """The parsed ``/custos/in/inputs.json`` document."""

    schema_version: str
    contract_version: str
    activity: ActivityIdentity
    step: StepRef
    inputs: dict[str, Any]


@dataclass(frozen=True)
class Context:
    """The parsed ``/custos/in/ctx.json`` execution context.

    Carries connector *handles* (never credentials), the deadline, and the
    raw document for fields not modeled here.
    """

    run_id: str
    step_id: str
    attempt: int
    workspace_id: str | None
    connectors: dict[str, Any]
    deadline: str | None
    raw: dict[str, Any] = field(default_factory=dict)


def _require_mapping(value: Any, *, where: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ActivityError(
            "activity.contract_violation",
            "permanent",
            f"{where} must be a JSON object",
        )
    return value


def _require_str(value: Any, *, where: str) -> str:
    if not isinstance(value, str) or not value:
        raise ActivityError(
            "activity.contract_violation",
            "permanent",
            f"{where} must be a non-empty string",
        )
    return value


# ---------------------------------------------------------------------------
# Sandbox
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Sandbox:
    """Typed access to the ``/custos`` activity sandbox."""

    base: Path

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Sandbox:
        source = os.environ if env is None else env
        return cls(base=Path(source.get(_IO_ROOT_ENV, _DEFAULT_ROOT)))

    @property
    def in_dir(self) -> Path:
        return self.base / "in"

    @property
    def out_dir(self) -> Path:
        return self.base / "out"

    @property
    def artifacts_dir(self) -> Path:
        return self.out_dir / "artifacts"

    # -- inputs ------------------------------------------------------------

    def _read_json(self, path: Path) -> dict[str, Any]:
        try:
            raw = path.read_text(encoding="utf-8")
        except FileNotFoundError as exc:
            raise ActivityError(
                "activity.contract_violation",
                "permanent",
                f"required sandbox file {path.name} is missing",
            ) from exc
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ActivityError(
                "activity.contract_violation",
                "permanent",
                f"sandbox file {path.name} is not valid JSON: {exc}",
            ) from exc
        return _require_mapping(parsed, where=path.name)

    def read_inputs(self) -> InputsEnvelope:
        doc = self._read_json(self.in_dir / "inputs.json")
        activity = _require_mapping(doc.get("activity"), where="inputs.json activity")
        step = _require_mapping(doc.get("step"), where="inputs.json step")
        inputs = _require_mapping(doc.get("inputs", {}), where="inputs.json inputs")
        attempt = step.get("attempt", 1)
        if not isinstance(attempt, int):
            raise ActivityError(
                "activity.contract_violation",
                "permanent",
                "inputs.json step.attempt must be an int",
            )
        return InputsEnvelope(
            schema_version=str(doc.get("schemaVersion", _SCHEMA_VERSION)),
            contract_version=str(doc.get("contractVersion", _CONTRACT_VERSION)),
            activity=ActivityIdentity(
                type=_require_str(activity.get("type"), where="inputs.json activity.type"),
                version=_require_str(activity.get("version"), where="inputs.json activity.version"),
            ),
            step=StepRef(
                run_id=_require_str(step.get("runId"), where="inputs.json step.runId"),
                step_id=_require_str(step.get("stepId"), where="inputs.json step.stepId"),
                attempt=attempt,
            ),
            inputs=inputs,
        )

    def read_context(self) -> Context:
        doc = self._read_json(self.in_dir / "ctx.json")
        connectors = doc.get("connectors", {})
        if not isinstance(connectors, dict):
            connectors = {}
        attempt = doc.get("attempt", 1)
        if not isinstance(attempt, int):
            attempt = 1
        deadline = doc.get("deadline")
        return Context(
            run_id=str(doc.get("runId", "")),
            step_id=str(doc.get("stepId", "")),
            attempt=attempt,
            workspace_id=doc.get("workspaceId"),
            connectors=connectors,
            deadline=deadline if isinstance(deadline, str) else None,
            raw=doc,
        )

    # -- secrets -----------------------------------------------------------

    def secret_path(self, slot: str, key: str) -> Path:
        return self.in_dir / "secrets" / slot / key

    def has_secret(self, slot: str, key: str) -> bool:
        return self.secret_path(slot, key).is_file()

    def read_secret(self, slot: str, key: str) -> str:
        path = self.secret_path(slot, key)
        try:
            return path.read_text(encoding="utf-8").rstrip("\n")
        except FileNotFoundError as exc:
            raise ActivityError(
                f"{slot}.unauthorized",
                "permanent",
                f"missing injected secret for slot {slot!r} key {key!r}",
            ) from exc

    def sidecar_token(self) -> str | None:
        path = self.in_dir / "sidecar-token"
        if not path.is_file():
            return None
        return path.read_text(encoding="utf-8").strip() or None

    # -- outputs -----------------------------------------------------------

    def _write_outputs(self, envelope: dict[str, Any]) -> None:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        (self.out_dir / "outputs.json").write_text(
            json.dumps(envelope, separators=(",", ":")), encoding="utf-8"
        )

    def write_success(self, outputs: Mapping[str, Any]) -> None:
        self._write_outputs(
            {
                "schemaVersion": _SCHEMA_VERSION,
                "contractVersion": _CONTRACT_VERSION,
                "status": "success",
                "outputs": dict(outputs),
            }
        )

    def write_failure(self, code: str, error_class: ErrorClass, message: str) -> None:
        self._write_outputs(
            {
                "schemaVersion": _SCHEMA_VERSION,
                "contractVersion": _CONTRACT_VERSION,
                "status": "failure",
                "error": {"code": code, "class": error_class, "message": message},
                "outputs": {},
            }
        )

    def write_artifact(self, name: str, content: str | bytes) -> None:
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        target = self.artifacts_dir / name
        if isinstance(content, str):
            target.write_text(content, encoding="utf-8")
        else:
            target.write_bytes(content)

    @staticmethod
    def artifact_ref(name: str) -> dict[str, str]:
        """A name-only ``ArtifactRef`` for ``outputs.json`` (ARM fills the id)."""
        return {"kind": "ArtifactRef", "name": name}

    def append_audit(self, record: Mapping[str, Any]) -> None:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        with (self.out_dir / "audit.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, separators=(",", ":")) + "\n")

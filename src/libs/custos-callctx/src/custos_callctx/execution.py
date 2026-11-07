"""Non-secret execution references and operator-owned workspace credential bindings.

Only stable references cross durable workflow boundaries. Secret locations are
deployment configuration, not fields accepted from an event or workflow input.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

ExecutionOperation = Literal[
    "workflow-start",
    "workflow-resume",
    "catalog-read",
    "catalog-activity-read",
    "connector-bind",
    "activity-execute",
    "connector-credentials",
]
EXECUTION_PERMISSIONS: Mapping[str, str] = MappingProxyType(
    {
        "workflow-start": "workflow:execute",
        "workflow-resume": "workflow:execute",
        "catalog-read": "catalog:workflows:read",
        "catalog-activity-read": "catalog:activity-types:read",
        "connector-bind": "connector:bind",
        "activity-execute": "workflow:execute",
        "connector-credentials": "connector:bind",
    }
)
EXECUTION_INLINE_BYTES = 65_536
EXECUTION_FILE_BYTES = 1_000_000
EXECUTION_BINDING_COUNT = 1024


def _identifier(value: object) -> str:
    """Reject ambiguous, oversized or non-string references before credential lookup."""
    if (
        not isinstance(value, str)
        or not value.strip()
        or value != value.strip()
        or "\0" in value
        or len(value) > 120
    ):
        raise ValueError(
            "execution identity references must be nonblank and at most 120 characters"
        )
    return value


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Reject duplicate JSON configuration keys instead of silently selecting the last identity."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate execution configuration key")
        result[key] = value
    return result


@dataclass(frozen=True, slots=True)
class ExecutionIdentity:
    """A pinned workspace/account/binding reference containing no bearer or signed context."""

    workspace_id: str
    principal_id: str
    binding_id: str

    def __post_init__(self) -> None:
        """Validate all scope coordinates without coercing untrusted scalar values."""
        for value in (self.workspace_id, self.principal_id, self.binding_id):
            _identifier(value)

    def to_dict(self) -> dict[str, str]:
        """Serialize only the stable identity fields allowed in retained run/target state."""
        return {
            "workspaceId": self.workspace_id,
            "principalId": self.principal_id,
            "bindingId": self.binding_id,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ExecutionIdentity:
        """Reject unknown fields so credentials cannot be smuggled into durable identity state."""
        if set(value) != {"workspaceId", "principalId", "bindingId"}:
            raise ValueError("execution identity has unsupported or missing fields")
        return cls(
            *(_identifier(value[key]) for key in ("workspaceId", "principalId", "bindingId"))
        )


@dataclass(frozen=True, slots=True)
class ExecutionBinding:
    """Operator-provisioned secret reference; credential material is never part of this value."""

    identity: ExecutionIdentity
    secret_ref: str
    secret_key: str

    def __post_init__(self) -> None:
        """Keep secret addressing explicit and separate from user-selected execution data."""
        _identifier(self.secret_ref)
        _identifier(self.secret_key)


class ExecutionBindings:
    """Immutable deployment registry with explicit per-workspace defaults and pinned lookup."""

    def __init__(self, bindings: tuple[ExecutionBinding, ...], defaults: Mapping[str, str]) -> None:
        """Reject duplicate/rebound identities and defaults that point outside their workspace."""
        rows: dict[tuple[str, str], ExecutionBinding] = {}
        for binding in bindings:
            key = (binding.identity.workspace_id, binding.identity.binding_id)
            if key in rows:
                raise ValueError("duplicate execution binding")
            rows[key] = binding
        selected: dict[str, str] = {}
        for workspace, identifier in defaults.items():
            _identifier(workspace)
            _identifier(identifier)
            if (workspace, identifier) not in rows:
                raise ValueError("workspace default has no matching execution binding")
            selected[workspace] = identifier
        self._bindings = MappingProxyType(rows)
        self._defaults = MappingProxyType(selected)

    @property
    def identities(self) -> tuple[ExecutionIdentity, ...]:
        """Return probe references, not a fallback identity-selection policy."""
        return tuple(binding.identity for binding in self._bindings.values())

    @property
    def bindings(self) -> tuple[ExecutionBinding, ...]:
        """Return immutable reference-only rows for operator authorization provisioning."""
        return tuple(self._bindings.values())

    def select(self, workspace: str) -> ExecutionIdentity:
        """Select identity for new work only; absent configuration is never a creator fallback."""
        _identifier(workspace)
        identifier = self._defaults.get(workspace)
        if identifier is None:
            raise ValueError("workspace execution identity is not configured")
        return self._bindings[(workspace, identifier)].identity

    def resolve(self, identity: ExecutionIdentity) -> ExecutionBinding:
        """Resolve the pinned reference; changed defaults or principals cannot retarget old work."""
        binding = self._bindings.get((identity.workspace_id, identity.binding_id))
        if binding is None or binding.identity != identity:
            raise ValueError("original execution identity is unavailable or has changed")
        return binding

    @classmethod
    def from_json(cls, raw: str) -> ExecutionBindings:
        """Parse the legacy inline envelope without increasing its 65,536-byte ceiling."""
        if len(raw.encode()) > EXECUTION_INLINE_BYTES:
            raise ValueError("execution binding configuration exceeds 64 KiB")
        return cls._parse_json(raw)

    @classmethod
    def from_file(cls, path: str, expected_sha256: str) -> ExecutionBindings:
        """Load one regular-file snapshot, bounded to 1,000,000 bytes and verified before parse.

        Absolute projected-file symlinks are supported. Special files are opened
        nonblocking and rejected by descriptor type; failures expose no input/path.
        """
        return cls.from_file_bytes(read_execution_file(path, expected_sha256))

    @classmethod
    def from_file_bytes(cls, raw: bytes) -> ExecutionBindings:
        """Validate a bounded file snapshot for service or offline bundle provisioning."""
        if len(raw) > EXECUTION_FILE_BYTES:
            raise ValueError("execution binding file exceeds 1000000 bytes")
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            raise ValueError("execution binding file requires strict UTF-8") from None
        return cls._parse_json(text)

    @classmethod
    def _parse_json(cls, raw: str) -> ExecutionBindings:
        """Apply the same closed semantic validation after source-specific byte checks."""
        try:
            value = json.loads(raw, object_pairs_hook=_unique_object)
        except (json.JSONDecodeError, RecursionError):
            raise ValueError("execution binding configuration is not valid JSON") from None
        if not isinstance(value, dict) or set(value) != {"bindings", "defaults"}:
            raise ValueError("execution binding configuration requires bindings and defaults")
        entries, defaults = value["bindings"], value["defaults"]
        if (
            not isinstance(entries, list)
            or not isinstance(defaults, dict)
            or len(entries) > EXECUTION_BINDING_COUNT
        ):
            raise ValueError("invalid execution binding configuration")
        bindings = []
        for entry in entries:
            if not isinstance(entry, dict) or set(entry) != {
                "workspaceId",
                "principalId",
                "bindingId",
                "secretRef",
                "secretKey",
            }:
                raise ValueError("execution binding has unsupported or missing fields")
            identity = ExecutionIdentity.from_dict(
                {key: entry[key] for key in ("workspaceId", "principalId", "bindingId")}
            )
            bindings.append(
                ExecutionBinding(
                    identity, _identifier(entry["secretRef"]), _identifier(entry["secretKey"])
                )
            )
        return cls(tuple(bindings), defaults)


def read_execution_snapshot(path: str) -> bytes:
    """Read at most cap+1 bytes from one regular descriptor, including projected symlinks."""
    if not os.path.isabs(path) or "\0" in path:
        raise ValueError("execution binding file requires an absolute path")
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_CLOEXEC)
        with os.fdopen(fd, "rb", buffering=0) as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise ValueError("execution binding source must be a regular file")
            remaining = EXECUTION_FILE_BYTES + 1
            chunks = []
            while remaining:
                chunk = stream.read(remaining)
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            raw = b"".join(chunks)
    except OSError:
        raise ValueError("execution binding file is missing or unreadable") from None
    if len(raw) > EXECUTION_FILE_BYTES:
        raise ValueError("execution binding file exceeds 1000000 bytes")
    return raw


def read_execution_file(path: str, expected_sha256: str) -> bytes:
    """Verify a mandatory expected digest against the same bounded file snapshot."""
    if re.fullmatch(r"[a-f0-9]{64}", expected_sha256) is None:
        raise ValueError("execution binding file requires a lowercase SHA-256 digest")
    raw = read_execution_snapshot(path)
    if not hmac.compare_digest(hashlib.sha256(raw).hexdigest(), expected_sha256):
        raise ValueError("execution binding file SHA-256 mismatch")
    return raw


def load_execution_bindings(env: Mapping[str, str]) -> ExecutionBindings | None:
    """Load one enabled inline/file source, rejecting ambiguity and digest misuse.

    Disabled mode ignores unused source settings. File mode requires the exact
    expected digest; neither mode falls back on missing/invalid configuration.
    """
    enabled = env.get("CUSTOS_EXECUTION_IDENTITY_ENABLED", "false").strip().lower()
    if enabled not in {"true", "false"}:
        raise ValueError("CUSTOS_EXECUTION_IDENTITY_ENABLED must be true or false")
    if enabled == "false":
        return None
    inline = "CUSTOS_EXECUTION_BINDINGS"
    file = "CUSTOS_EXECUTION_BINDINGS_FILE"
    digest = "CUSTOS_EXECUTION_BINDINGS_SHA256"
    if inline in env and file in env:
        raise ValueError("execution bindings have ambiguous inline and file sources")
    if file in env:
        return ExecutionBindings.from_file(env[file], env.get(digest, ""))
    if digest in env:
        raise ValueError("execution binding digest requires the file source")
    raw = env.get(inline, "")
    if not raw.strip():
        raise ValueError("enabled execution identity requires CUSTOS_EXECUTION_BINDINGS")
    return ExecutionBindings.from_json(raw)

"""Verify exact transport budgets, closed parsing and immutable pinned file snapshots."""

import hashlib
import json
import os
from pathlib import Path

import pytest

from custos_callctx.execution import ExecutionBindings, load_execution_bindings


def registry(count: int = 1) -> bytes:
    """Produce distinct reference-only workspaces with explicit local defaults."""
    return json.dumps(
        {
            "bindings": [
                {
                    "workspaceId": f"ws-{i}",
                    "principalId": f"account-{i}",
                    "bindingId": "v1",
                    "secretRef": f"execution-{i}",
                    "secretKey": "token",
                }
                for i in range(count)
            ],
            "defaults": {f"ws-{i}": "v1" for i in range(count)},
        },
        separators=(",", ":"),
    ).encode()


def file_env(path: Path, raw: bytes) -> dict[str, str]:
    """Persist a snapshot and return the actual enabled file/digest environment."""
    path.write_bytes(raw)
    return {
        "CUSTOS_EXECUTION_IDENTITY_ENABLED": "true",
        "CUSTOS_EXECUTION_BINDINGS_FILE": str(path),
        "CUSTOS_EXECUTION_BINDINGS_SHA256": hashlib.sha256(raw).hexdigest(),
    }


@pytest.mark.parametrize("size", [1_000_000, 1_000_001])
def test_exact_file_byte_boundary(tmp_path: Path, size: int) -> None:
    """Padding is counted as UTF-8 bytes before parsing, including one over the cap."""
    raw = registry()
    env = file_env(tmp_path / "bindings.json", raw + b" " * (size - len(raw)))
    if size == 1_000_000:
        loaded = load_execution_bindings(env)
        assert loaded is not None and loaded.select("ws-0").principal_id == "account-0"
    else:
        with pytest.raises(ValueError, match="exceeds 1000000"):
            load_execution_bindings(env)


@pytest.mark.parametrize("count", [51, 200, 1000, 1024, 1025])
def test_actual_file_cardinality(tmp_path: Path, count: int) -> None:
    """The count cap is independent of the old inline transport limit."""
    env = file_env(tmp_path / "bindings.json", registry(count))
    if count > 1024:
        with pytest.raises(ValueError, match="invalid execution binding"):
            load_execution_bindings(env)
    else:
        loaded = load_execution_bindings(env)
        assert loaded is not None and len(loaded.identities) == count
        assert loaded.select(f"ws-{count - 1}").principal_id == f"account-{count - 1}"


@pytest.mark.parametrize("size", [65_536, 65_537])
def test_inline_boundary_is_unchanged(size: int) -> None:
    """File support does not enlarge legacy inline admission."""
    raw = registry().decode()
    raw += " " * (size - len(raw))
    if size == 65_536:
        assert ExecutionBindings.from_json(raw).select("ws-0").binding_id == "v1"
    else:
        with pytest.raises(ValueError, match="64 KiB"):
            ExecutionBindings.from_json(raw)


@pytest.mark.parametrize(
    "raw",
    [
        b"\xff",
        b'{"bindings":[],"bindings":[],"defaults":{}}',
        b'{"bindings":[],"defaults":{},"credential":"synthetic"}',
        b'{"bindings":[',
        b'{"bindings":[],"defaults":{"other":"v1"}}',
    ],
)
def test_invalid_file_is_not_a_fallback(tmp_path: Path, raw: bytes) -> None:
    """Malformed encoding, schema, duplicates and scope fail through the real loader."""
    env = file_env(tmp_path / "bindings.json", raw)
    with pytest.raises(ValueError):
        load_execution_bindings(env)


@pytest.mark.parametrize("digest", ["", "A" * 64, "0" * 64, "bad"])
def test_digest_is_required_and_verified(tmp_path: Path, digest: str) -> None:
    """Missing/invalid/mismatched expected bytes never admit a file."""
    env = file_env(tmp_path / "bindings.json", registry())
    env["CUSTOS_EXECUTION_BINDINGS_SHA256"] = digest
    with pytest.raises(ValueError, match="SHA-256"):
        load_execution_bindings(env)


def test_ambiguous_disabled_and_orphan_digest(tmp_path: Path) -> None:
    """Presence, even with empty values, is ambiguous; off mode ignores all source keys."""
    env = file_env(tmp_path / "bindings.json", registry())
    env["CUSTOS_EXECUTION_BINDINGS"] = ""
    with pytest.raises(ValueError, match="ambiguous"):
        load_execution_bindings(env)
    env["CUSTOS_EXECUTION_IDENTITY_ENABLED"] = "false"
    assert load_execution_bindings(env) is None
    env.pop("CUSTOS_EXECUTION_BINDINGS_FILE")
    env["CUSTOS_EXECUTION_IDENTITY_ENABLED"] = "true"
    with pytest.raises(ValueError, match="digest requires"):
        load_execution_bindings(env)


def test_projected_symlink_and_startup_snapshot(tmp_path: Path) -> None:
    """A loaded registry does not follow a later projection/default update."""
    target = tmp_path / "version"
    env = file_env(target, registry())
    link = tmp_path / "bindings.json"
    link.symlink_to(target)
    env["CUSTOS_EXECUTION_BINDINGS_FILE"] = str(link)
    loaded = load_execution_bindings(env)
    assert loaded is not None
    target.write_bytes(b"changed")
    assert loaded.select("ws-0").principal_id == "account-0"
    with pytest.raises(ValueError, match="mismatch"):
        load_execution_bindings(env)


@pytest.mark.parametrize("kind", ["missing", "directory", "fifo", "relative"])
def test_special_sources_fail_without_waiting(tmp_path: Path, kind: str) -> None:
    """Nonblocking descriptor validation rejects pipes/directories and missing/relative paths."""
    path = tmp_path / "source"
    if kind == "fifo":
        os.mkfifo(path)
    elif kind == "directory":
        path.mkdir()
    env = {
        "CUSTOS_EXECUTION_IDENTITY_ENABLED": "true",
        "CUSTOS_EXECUTION_BINDINGS_FILE": "relative" if kind == "relative" else str(path),
        "CUSTOS_EXECUTION_BINDINGS_SHA256": "0" * 64,
    }
    with pytest.raises(ValueError):
        load_execution_bindings(env)


def test_utf8_bytes_not_character_count(tmp_path: Path) -> None:
    """Multibyte workspace references remain valid, but bytes determine both source budgets."""
    data = json.loads(registry())
    data["bindings"][0]["workspaceId"] = "界" * 120
    data["defaults"] = {"界" * 120: "v1"}
    raw = json.dumps(data, ensure_ascii=False).encode()
    env = file_env(tmp_path / "bindings.json", raw)
    loaded = load_execution_bindings(env)
    assert loaded is not None and loaded.select("界" * 120).principal_id == "account-0"
    text = raw.decode() + " " * (1_000_000 - len(raw.decode()))
    assert len(text) == 1_000_000 and len(text.encode()) > 1_000_000
    env = file_env(tmp_path / "oversize.json", text.encode())
    with pytest.raises(ValueError, match="exceeds"):
        load_execution_bindings(env)


def test_unreadable_file_is_sanitized(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Operating-system errors reveal neither paths nor their potentially sensitive messages."""
    env = file_env(tmp_path / "bindings.json", registry())

    def denied(*args: object, **kwargs: object) -> int:
        """Model unreadability independently of root/administrator filesystem privileges."""
        raise PermissionError("sensitive path and contents")

    monkeypatch.setattr("custos_callctx.execution.os.open", denied)
    with pytest.raises(ValueError, match="missing or unreadable") as error:
        load_execution_bindings(env)
    assert "sensitive" not in str(error.value)

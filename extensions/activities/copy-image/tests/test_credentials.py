"""Tests for per-slot credential materialization (COPY-IMPL-003)."""

from __future__ import annotations

import base64
import json
import stat
from pathlib import Path

import pytest
from copy_image.contract import ActivityError, Sandbox
from copy_image.credentials import (
    SlotCredentials,
    build_auths,
    read_slot_credentials,
    redact,
    write_authfile,
)


def _seed_secret(base: Path, slot: str, *, username: str, token: str) -> None:
    d = base / "in" / "secrets" / slot
    d.mkdir(parents=True, exist_ok=True)
    (d / "username").write_text(username + "\n", encoding="utf-8")
    (d / "token").write_text(token + "\n", encoding="utf-8")


def test_read_slot_credentials(tmp_path: Path) -> None:
    _seed_secret(tmp_path, "source", username="alice", token="pat-123")
    creds = read_slot_credentials(Sandbox(base=tmp_path), "source")
    assert creds.username == "alice"
    assert creds.secret == "pat-123"


def test_missing_secret_is_slot_unauthorized(tmp_path: Path) -> None:
    with pytest.raises(ActivityError) as excinfo:
        read_slot_credentials(Sandbox(base=tmp_path), "dest")
    assert excinfo.value.code == "dest.unauthorized"
    assert excinfo.value.error_class == "permanent"


def test_repr_redacts_secret() -> None:
    creds = SlotCredentials(username="alice", secret="super-secret-pat")
    assert "super-secret-pat" not in repr(creds)
    assert "super-secret-pat" not in str(creds)
    assert "<redacted>" in repr(creds)


def test_docker_auth_is_base64_user_colon_secret() -> None:
    creds = SlotCredentials(username="alice", secret="pat-123")
    decoded = base64.b64decode(creds.docker_auth()).decode("ascii")
    assert decoded == "alice:pat-123"


def test_build_auths_keys_by_host() -> None:
    entries = {
        "registry-1.docker.io": SlotCredentials("alice", "p1"),
        "ghcr.io": SlotCredentials("bob", "p2"),
    }
    doc = build_auths(entries)
    assert doc["auths"].keys() == entries.keys()
    for host, creds in entries.items():
        decoded = base64.b64decode(doc["auths"][host]["auth"]).decode()
        assert decoded == f"{creds.username}:{creds.secret}"


def test_write_authfile_is_private_and_valid(tmp_path: Path) -> None:
    entries = {"ghcr.io": SlotCredentials("bob", "p2")}
    path = write_authfile(tmp_path / "run", entries)
    assert path.name == "auth.json"
    mode = stat.S_IMODE(path.stat().st_mode)
    assert mode == 0o600, oct(mode)
    doc = json.loads(path.read_text())
    assert doc["auths"].keys() == entries.keys()


def test_redact_scrubs_secrets() -> None:
    text = "skopeo: authentication required for token=pat-123 user=alice"
    scrubbed = redact(text, ["pat-123", ""])
    assert "pat-123" not in scrubbed
    assert "***" in scrubbed

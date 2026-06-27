"""Tests for skopeo error classification (COPY-IMPL-005)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from copy_image.errors import classify_skopeo_error


def test_source_unauthorized() -> None:
    err = classify_skopeo_error(
        "Error reading manifest latest: unauthorized: authentication required"
    )
    assert err.code == "source.unauthorized"
    assert err.error_class == "permanent"


def test_dest_unauthorized() -> None:
    err = classify_skopeo_error(
        "Error writing manifest to destination: requested access to the resource is denied"
    )
    assert err.code == "dest.unauthorized"
    assert err.error_class == "permanent"


def test_source_not_found() -> None:
    err = classify_skopeo_error("Error reading manifest v9: manifest unknown")
    assert err.code == "source.not_found"
    assert err.error_class == "permanent"


def test_copy_manifest_mismatch() -> None:
    err = classify_skopeo_error("Error: Digest did not match, expected sha256:aaa, got sha256:bbb")
    assert err.code == "copy.manifest_mismatch"
    assert err.error_class == "permanent"


def test_unclassified_is_dest_push_failed_retryable() -> None:
    err = classify_skopeo_error(
        "Error copying blob: received unexpected HTTP status: 500 Internal Server Error"
    )
    assert err.code == "dest.push_failed"
    assert err.error_class == "retryable"


def test_empty_stderr_has_a_detail() -> None:
    err = classify_skopeo_error("")
    assert err.code == "dest.push_failed"
    assert err.message == "skopeo copy failed"


def test_detail_is_redacted() -> None:
    err = classify_skopeo_error(
        "auth failed presenting token=pat-XYZ to registry", redactions=["pat-XYZ"]
    )
    assert "pat-XYZ" not in err.message
    assert "***" in err.message


def test_end_to_end_mapping_through_main(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import subprocess

    from copy_image.__main__ import main

    inputs = {
        "schemaVersion": "1",
        "contractVersion": "1",
        "activity": {"type": "copy-image", "version": "0.1.0"},
        "step": {"runId": "r", "stepId": "copy", "attempt": 1},
        "inputs": {
            "source": {"ref": "registry-1.docker.io/library/x:latest"},
            "destination": {"repository": "octo/x"},
        },
    }
    ctx = {
        "runId": "r",
        "stepId": "copy",
        "attempt": 1,
        "workspaceId": "ws",
        "connectors": {
            "source": {"endpoint": "https://registry-1.docker.io"},
            "dest": {"endpoint": "https://ghcr.io"},
        },
    }
    in_dir = tmp_path / "in"
    for slot in ("source", "dest"):
        (in_dir / "secrets" / slot).mkdir(parents=True, exist_ok=True)
        (in_dir / "secrets" / slot / "username").write_text("u", encoding="utf-8")
        (in_dir / "secrets" / slot / "token").write_text("pat", encoding="utf-8")
    (in_dir / "inputs.json").write_text(json.dumps(inputs), encoding="utf-8")
    (in_dir / "ctx.json").write_text(json.dumps(ctx), encoding="utf-8")
    monkeypatch.setenv("CUSTOS_IO_ROOT", str(tmp_path))

    def runner(argv: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(argv, 1, "", "Error reading manifest: manifest unknown")

    monkeypatch.setattr("copy_image.copy.subprocess.run", runner)
    assert main([]) == 2  # permanent
    out = json.loads((tmp_path / "out" / "outputs.json").read_text())
    assert out["error"]["code"] == "source.not_found"
    assert out["error"]["class"] == "permanent"

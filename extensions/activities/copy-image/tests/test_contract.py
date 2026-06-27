"""Tests for the file-based activity contract (COPY-IMPL-002)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from copy_image.__main__ import main
from copy_image.contract import ActivityError, Sandbox, exit_code_for

_INPUTS: dict[str, Any] = {
    "schemaVersion": "1",
    "contractVersion": "1",
    "activity": {"type": "copy-image", "version": "0.1.0"},
    "step": {"runId": "run-1", "stepId": "copy", "attempt": 2},
    "inputs": {
        "source": {"ref": "docker.io/library/hello-world:latest"},
        "destination": {"repository": "octo-org/hello-world", "tag": "latest"},
        "copyReferrers": False,
    },
}

_CTX: dict[str, Any] = {
    "runId": "run-1",
    "stepId": "copy",
    "attempt": 2,
    "workspaceId": "ws-1",
    "connectors": {
        "source": {"endpoint": "https://registry-1.docker.io"},
        "dest": {"endpoint": "https://ghcr.io"},
    },
    "deadline": "2026-06-27T01:00:00Z",
}


def _seed(base: Path, *, inputs: Any = _INPUTS, ctx: Any = _CTX) -> Sandbox:
    in_dir = base / "in"
    in_dir.mkdir(parents=True, exist_ok=True)
    if inputs is not None:
        (in_dir / "inputs.json").write_text(json.dumps(inputs), encoding="utf-8")
    if ctx is not None:
        (in_dir / "ctx.json").write_text(json.dumps(ctx), encoding="utf-8")
    return Sandbox(base=base)


# ---------------------------------------------------------------------------
# inputs / context
# ---------------------------------------------------------------------------


def test_read_inputs(tmp_path: Path) -> None:
    env = _seed(tmp_path).read_inputs()
    assert env.activity.type == "copy-image"
    assert env.activity.version == "0.1.0"
    assert env.step.run_id == "run-1"
    assert env.step.attempt == 2
    assert env.inputs["destination"]["repository"] == "octo-org/hello-world"


def test_read_context(tmp_path: Path) -> None:
    ctx = _seed(tmp_path).read_context()
    assert ctx.workspace_id == "ws-1"
    assert ctx.connectors["source"]["endpoint"] == "https://registry-1.docker.io"
    assert ctx.deadline == "2026-06-27T01:00:00Z"


def test_missing_inputs_is_permanent_contract_violation(tmp_path: Path) -> None:
    sandbox = _seed(tmp_path, inputs=None)
    with pytest.raises(ActivityError) as excinfo:
        sandbox.read_inputs()
    assert excinfo.value.code == "activity.contract_violation"
    assert excinfo.value.error_class == "permanent"


def test_malformed_inputs_json_is_contract_violation(tmp_path: Path) -> None:
    (tmp_path / "in").mkdir(parents=True)
    (tmp_path / "in" / "inputs.json").write_text("{not json", encoding="utf-8")
    with pytest.raises(ActivityError) as excinfo:
        Sandbox(base=tmp_path).read_inputs()
    assert excinfo.value.error_class == "permanent"


def test_inputs_missing_required_field_is_contract_violation(tmp_path: Path) -> None:
    bad = {**_INPUTS, "activity": {"type": "copy-image"}}  # missing version
    sandbox = _seed(tmp_path, inputs=bad)
    with pytest.raises(ActivityError):
        sandbox.read_inputs()


# ---------------------------------------------------------------------------
# secrets
# ---------------------------------------------------------------------------


def test_read_secret(tmp_path: Path) -> None:
    sandbox = _seed(tmp_path)
    secret_dir = tmp_path / "in" / "secrets" / "source"
    secret_dir.mkdir(parents=True)
    (secret_dir / "token").write_text("pat-value\n", encoding="utf-8")
    assert sandbox.has_secret("source", "token") is True
    assert sandbox.read_secret("source", "token") == "pat-value"


def test_missing_secret_maps_to_slot_unauthorized(tmp_path: Path) -> None:
    sandbox = _seed(tmp_path)
    assert sandbox.has_secret("dest", "token") is False
    with pytest.raises(ActivityError) as excinfo:
        sandbox.read_secret("dest", "token")
    assert excinfo.value.code == "dest.unauthorized"
    assert excinfo.value.error_class == "permanent"


def test_sidecar_token(tmp_path: Path) -> None:
    sandbox = _seed(tmp_path)
    assert sandbox.sidecar_token() is None
    (tmp_path / "in" / "sidecar-token").write_text("tok\n", encoding="utf-8")
    assert sandbox.sidecar_token() == "tok"


# ---------------------------------------------------------------------------
# outputs / artifacts / audit
# ---------------------------------------------------------------------------


def test_write_success_envelope(tmp_path: Path) -> None:
    sandbox = Sandbox(base=tmp_path)
    sandbox.write_success(
        {
            "destinationRef": "ghcr.io/octo-org/hello-world:latest",
            "digest": "sha256:" + "a" * 64,
            "reportRef": Sandbox.artifact_ref("copy-report"),
        }
    )
    doc = json.loads((tmp_path / "out" / "outputs.json").read_text())
    assert doc["status"] == "success"
    assert doc["schemaVersion"] == "1"
    assert doc["outputs"]["digest"].startswith("sha256:")
    assert doc["outputs"]["reportRef"] == {"kind": "ArtifactRef", "name": "copy-report"}


def test_write_failure_envelope(tmp_path: Path) -> None:
    sandbox = Sandbox(base=tmp_path)
    sandbox.write_failure("dest.push_failed", "retryable", "push rejected")
    doc = json.loads((tmp_path / "out" / "outputs.json").read_text())
    assert doc["status"] == "failure"
    assert doc["error"] == {
        "code": "dest.push_failed",
        "class": "retryable",
        "message": "push rejected",
    }
    assert doc["outputs"] == {}


def test_write_artifact_and_audit(tmp_path: Path) -> None:
    sandbox = Sandbox(base=tmp_path)
    sandbox.write_artifact("copy-report", json.dumps({"copied": 1}))
    sandbox.append_audit({"event": "copy.started"})
    sandbox.append_audit({"event": "copy.finished"})
    assert json.loads((tmp_path / "out" / "artifacts" / "copy-report").read_text()) == {"copied": 1}
    lines = (tmp_path / "out" / "audit.jsonl").read_text().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["event"] == "copy.started"


# ---------------------------------------------------------------------------
# exit codes + entry point
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("error_class", "expected"),
    [("permanent", 2), ("retryable", 1), ("cancelled", 1)],
)
def test_exit_code_for(error_class: Any, expected: int) -> None:
    assert exit_code_for(error_class) == expected


def test_from_env_uses_io_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CUSTOS_IO_ROOT", str(tmp_path))
    assert Sandbox.from_env().base == tmp_path


def test_entrypoint_reads_inputs_and_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _seed(tmp_path)
    monkeypatch.setenv("CUSTOS_IO_ROOT", str(tmp_path))
    assert main([]) == 2
    doc = json.loads((tmp_path / "out" / "outputs.json").read_text())
    assert doc["status"] == "failure"
    assert doc["error"]["code"] == "activity.not_implemented"
    assert doc["error"]["class"] == "permanent"

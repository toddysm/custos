"""Tests for the skopeo copy engine + entry-point wiring (COPY-IMPL-004)."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest
from copy_image.contract import Context, InputsEnvelope
from copy_image.copy import (
    SkopeoError,
    build_argv,
    host_of,
    resolve_copy_plan,
    run_skopeo_copy,
)

_INPUTS = {
    "schemaVersion": "1",
    "contractVersion": "1",
    "activity": {"type": "copy-image", "version": "0.1.0"},
    "step": {"runId": "run-1", "stepId": "copy", "attempt": 1},
    "inputs": {
        "source": {"ref": "registry-1.docker.io/library/hello-world:latest"},
        "destination": {"repository": "octo-org/hello-world", "tag": "v1"},
    },
}
_CTX = {
    "runId": "run-1",
    "stepId": "copy",
    "attempt": 1,
    "workspaceId": "ws-1",
    "connectors": {
        "source": {"endpoint": "https://registry-1.docker.io/v2/library"},
        "dest": {"endpoint": "https://ghcr.io/v2/octo-org"},
    },
    "deadline": None,
}


def _plan(inputs: dict[str, Any] = _INPUTS, ctx: dict[str, Any] = _CTX):  # type: ignore[no-untyped-def]
    return resolve_copy_plan(_inputs(inputs), _context(ctx))


def _inputs(doc: dict[str, Any]) -> InputsEnvelope:
    from copy_image.contract import ActivityIdentity, StepRef

    return InputsEnvelope(
        schema_version="1",
        contract_version="1",
        activity=ActivityIdentity("copy-image", "0.1.0"),
        step=StepRef("run-1", "copy", 1),
        inputs=doc["inputs"],
    )


def _context(doc: dict[str, Any]) -> Context:
    return Context(
        run_id="run-1",
        step_id="copy",
        attempt=1,
        workspace_id="ws-1",
        connectors=doc["connectors"],
        deadline=None,
        raw=doc,
    )


# ---------------------------------------------------------------------------
# host parsing + plan
# ---------------------------------------------------------------------------


def test_host_of() -> None:
    assert host_of("https://ghcr.io/v2/ns") == "ghcr.io"
    assert host_of("registry-1.docker.io") == "registry-1.docker.io"
    assert host_of("docker.io/library/x:1") == "docker.io"


def test_resolve_copy_plan_basic() -> None:
    plan = _plan()
    assert plan.source_transport == "docker://registry-1.docker.io/library/hello-world:latest"
    assert plan.dest_transport == "docker://ghcr.io/octo-org/hello-world:v1"
    assert plan.destination_ref == "ghcr.io/octo-org/hello-world:v1"
    assert plan.source_host == "registry-1.docker.io"
    assert plan.dest_host == "ghcr.io"
    assert plan.all_platforms is False


def test_plan_defaults_tag_to_latest() -> None:
    doc = json.loads(json.dumps(_INPUTS))
    del doc["inputs"]["destination"]["tag"]
    plan = _plan(doc)
    assert plan.destination_ref.endswith(":latest")


def test_plan_pins_source_to_digest_when_provided() -> None:
    doc = json.loads(json.dumps(_INPUTS))
    doc["inputs"]["source"]["digest"] = "sha256:" + "a" * 64
    plan = _plan(doc)
    assert plan.source_transport == (
        "docker://registry-1.docker.io/library/hello-world@sha256:" + "a" * 64
    )


def test_plan_requires_dest_endpoint() -> None:
    doc = json.loads(json.dumps(_CTX))
    doc["connectors"]["dest"] = {}
    from copy_image.contract import ActivityError

    with pytest.raises(ActivityError) as excinfo:
        _plan(ctx=doc)
    assert excinfo.value.error_class == "permanent"


def test_plan_requires_source_host() -> None:
    from copy_image.contract import ActivityError

    ctx = json.loads(json.dumps(_CTX))
    ctx["connectors"]["source"] = {}  # no endpoint
    inputs = json.loads(json.dumps(_INPUTS))
    inputs["inputs"]["source"]["ref"] = "/"  # host_of("/") == ""
    with pytest.raises(ActivityError) as excinfo:
        _plan(inputs, ctx)
    assert excinfo.value.error_class == "permanent"


def test_plan_rejects_non_bool_all_platforms() -> None:
    from copy_image.contract import ActivityError

    doc = json.loads(json.dumps(_INPUTS))
    doc["inputs"]["allPlatforms"] = "false"
    with pytest.raises(ActivityError) as excinfo:
        _plan(doc)
    assert excinfo.value.error_class == "permanent"


# ---------------------------------------------------------------------------
# argv + run
# ---------------------------------------------------------------------------


def test_build_argv() -> None:
    plan = _plan()
    argv = build_argv(plan, Path("/run/auth.json"), Path("/run/digest"))
    assert argv[0:2] == ["skopeo", "copy"]
    assert "--authfile" in argv and "/run/auth.json" in argv
    assert "--digestfile" in argv and "/run/digest" in argv
    assert argv[-2:] == [plan.source_transport, plan.dest_transport]
    assert "--all" not in argv


def test_build_argv_all_platforms() -> None:
    doc = json.loads(json.dumps(_INPUTS))
    doc["inputs"]["allPlatforms"] = True
    argv = build_argv(_plan(doc), Path("/a"), Path("/d"))
    assert "--all" in argv


def test_build_argv_platform_override() -> None:
    doc = json.loads(json.dumps(_INPUTS))
    doc["inputs"]["platform"] = "linux/arm64"
    argv = build_argv(_plan(doc), Path("/a"), Path("/d"))
    # global --override-* come before the `copy` subcommand.
    assert argv.index("--override-os") < argv.index("copy")
    assert argv[argv.index("--override-os") + 1] == "linux"
    assert argv[argv.index("--override-arch") + 1] == "arm64"


def test_resolve_plan_parses_copy_referrers() -> None:
    doc = json.loads(json.dumps(_INPUTS))
    doc["inputs"]["copyReferrers"] = True
    plan = _plan(doc)
    assert plan.copy_referrers is True
    assert plan.source_ref == "registry-1.docker.io/library/hello-world:latest"


def test_plan_rejects_non_bool_copy_referrers() -> None:
    from copy_image.contract import ActivityError

    doc = json.loads(json.dumps(_INPUTS))
    doc["inputs"]["copyReferrers"] = "yes"
    with pytest.raises(ActivityError) as excinfo:
        _plan(doc)
    assert excinfo.value.error_class == "permanent"


def test_build_oras_argv() -> None:
    from copy_image.copy import build_oras_argv

    plan = _plan()
    argv = build_oras_argv(plan, Path("/run/auth.json"))
    assert argv[0:3] == ["oras", "cp", "--recursive"]
    assert "--registry-config" in argv and "/run/auth.json" in argv
    assert argv[-2:] == [plan.source_ref, plan.destination_ref]


def _oras_runner(returncode: int, *, stdout: str = "", stderr: str = ""):  # type: ignore[no-untyped-def]
    def runner(argv: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(argv, returncode, stdout, stderr)

    return runner


def test_run_oras_copy_success(tmp_path: Path) -> None:
    from copy_image.copy import run_oras_copy

    image = "sha256:" + "e" * 64
    referrer = "sha256:" + "f" * 64
    stdout = f"Copied src => dst {image}\nReferrer {referrer}\nDigest: {image}\n"
    outcome = run_oras_copy(_plan(), tmp_path / "auth", runner=_oras_runner(0, stdout=stdout))
    assert outcome.digest == image
    assert outcome.manifests_copied == 2  # image + 1 referrer


def test_run_oras_copy_failure(tmp_path: Path) -> None:
    from copy_image.copy import OrasError, run_oras_copy

    with pytest.raises(OrasError) as excinfo:
        run_oras_copy(_plan(), tmp_path / "a", runner=_oras_runner(1, stderr="denied"))
    assert excinfo.value.returncode == 1


def test_run_oras_copy_no_digest_is_retryable(tmp_path: Path) -> None:
    from copy_image.contract import ActivityError
    from copy_image.copy import run_oras_copy

    with pytest.raises(ActivityError) as excinfo:
        run_oras_copy(_plan(), tmp_path / "a", runner=_oras_runner(0, stdout="done"))
    assert excinfo.value.error_class == "retryable"


def _fake_runner(returncode: int, *, digest: str = "", stderr: str = ""):  # type: ignore[no-untyped-def]
    def runner(argv: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        if returncode == 0 and digest:
            df = Path(argv[argv.index("--digestfile") + 1])
            df.write_text(digest, encoding="utf-8")
        return subprocess.CompletedProcess(argv, returncode, "", stderr)

    return runner


def test_run_skopeo_copy_success(tmp_path: Path) -> None:
    digest = "sha256:" + "b" * 64
    outcome = run_skopeo_copy(
        _plan(),
        tmp_path / "auth.json",
        runner=_fake_runner(0, digest=digest),
        work_dir=tmp_path / "work",
    )
    assert outcome.digest == digest
    assert outcome.destination_ref == "ghcr.io/octo-org/hello-world:v1"
    assert outcome.manifests_copied == 1


def test_run_skopeo_copy_idempotent_repeat(tmp_path: Path) -> None:
    digest = "sha256:" + "c" * 64
    runner = _fake_runner(0, digest=digest)
    first = run_skopeo_copy(_plan(), tmp_path / "a", runner=runner, work_dir=tmp_path / "w1")
    second = run_skopeo_copy(_plan(), tmp_path / "a", runner=runner, work_dir=tmp_path / "w2")
    assert first.digest == second.digest == digest


def test_run_skopeo_copy_raises_on_failure(tmp_path: Path) -> None:
    with pytest.raises(SkopeoError) as excinfo:
        run_skopeo_copy(
            _plan(),
            tmp_path / "a",
            runner=_fake_runner(1, stderr="boom"),
            work_dir=tmp_path / "w",
        )
    assert excinfo.value.returncode == 1
    assert excinfo.value.stderr == "boom"


def test_run_skopeo_copy_empty_digest_is_retryable(tmp_path: Path) -> None:
    from copy_image.contract import ActivityError

    with pytest.raises(ActivityError) as excinfo:
        run_skopeo_copy(
            _plan(),
            tmp_path / "a",
            runner=_fake_runner(0, digest=""),  # success but no digestfile written
            work_dir=tmp_path / "w",
        )
    assert excinfo.value.error_class == "retryable"


# ---------------------------------------------------------------------------
# entry point end-to-end
# ---------------------------------------------------------------------------


def _seed_sandbox(base: Path) -> None:
    in_dir = base / "in"
    (in_dir / "secrets" / "source").mkdir(parents=True, exist_ok=True)
    (in_dir / "secrets" / "dest").mkdir(parents=True, exist_ok=True)
    (in_dir / "inputs.json").write_text(json.dumps(_INPUTS), encoding="utf-8")
    (in_dir / "ctx.json").write_text(json.dumps(_CTX), encoding="utf-8")
    for slot in ("source", "dest"):
        (in_dir / "secrets" / slot / "username").write_text("user", encoding="utf-8")
        (in_dir / "secrets" / slot / "token").write_text(f"pat-{slot}", encoding="utf-8")


def test_main_success(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from copy_image.__main__ import main

    _seed_sandbox(tmp_path)
    monkeypatch.setenv("CUSTOS_IO_ROOT", str(tmp_path))
    digest = "sha256:" + "d" * 64
    monkeypatch.setattr("copy_image.copy.subprocess.run", _fake_runner(0, digest=digest))
    assert main([]) == 0
    out = json.loads((tmp_path / "out" / "outputs.json").read_text())
    assert out["status"] == "success"
    assert out["outputs"]["digest"] == digest
    assert out["outputs"]["destinationRef"] == "ghcr.io/octo-org/hello-world:v1"
    assert out["outputs"]["reportRef"] == {"kind": "ArtifactRef", "name": "copy-report"}
    report = json.loads((tmp_path / "out" / "artifacts" / "copy-report").read_text())
    assert report["digest"] == digest


def test_main_missing_secret_is_permanent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _seed_sandbox(tmp_path)
    (tmp_path / "in" / "secrets" / "source" / "token").unlink()
    from copy_image.__main__ import main

    monkeypatch.setenv("CUSTOS_IO_ROOT", str(tmp_path))
    assert main([]) == 2
    out = json.loads((tmp_path / "out" / "outputs.json").read_text())
    assert out["error"]["code"] == "source.unauthorized"


def test_main_copy_failure_redacts_secret(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _seed_sandbox(tmp_path)
    from copy_image.__main__ import main

    monkeypatch.setenv("CUSTOS_IO_ROOT", str(tmp_path))
    monkeypatch.setattr(
        "copy_image.copy.subprocess.run",
        _fake_runner(1, stderr="auth failed for token pat-source"),
    )
    assert main([]) == 1
    out = json.loads((tmp_path / "out" / "outputs.json").read_text())
    assert out["status"] == "failure"
    assert "pat-source" not in out["error"]["message"]
    assert "***" in out["error"]["message"]


def test_main_copy_referrers_uses_oras(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from copy_image.__main__ import main

    doc = json.loads(json.dumps(_INPUTS))
    doc["inputs"]["copyReferrers"] = True
    _seed_sandbox(tmp_path)
    (tmp_path / "in" / "inputs.json").write_text(json.dumps(doc), encoding="utf-8")
    monkeypatch.setenv("CUSTOS_IO_ROOT", str(tmp_path))

    image = "sha256:" + "a" * 64
    referrer = "sha256:" + "b" * 64
    seen: dict[str, list[str]] = {}

    def runner(argv: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        seen["argv"] = argv
        stdout = f"Copied {image}\nReferrer {referrer}\nDigest: {image}\n"
        return subprocess.CompletedProcess(argv, 0, stdout, "")

    monkeypatch.setattr("copy_image.copy.subprocess.run", runner)
    assert main([]) == 0
    assert seen["argv"][0:2] == ["oras", "cp"]
    out = json.loads((tmp_path / "out" / "outputs.json").read_text())
    assert out["status"] == "success"
    assert out["outputs"]["digest"] == image
    assert out["outputs"]["manifestsCopied"] == 2

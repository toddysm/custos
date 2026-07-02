"""Tests for the e2e one-shot orchestration (DEVCLI-IMPL-009)."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from custosctl import activities, connectors, e2e, lifecycle, seed, workflows
from custosctl.config import Settings, Target


def _settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "target": Target.REMOTE,
        "gateway": "https://gw.example",
        "token": "cst_x",
        "workspace": "ws-prod",
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


class _Stubs:
    """Patch every building block e2e delegates to and record the call order."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, *, run_status: str = "succeeded") -> None:
        self.calls: list[str] = []
        self.run_status = run_status
        self.acts: list[dict[str, str]] = [{"version": "0.1.0"}]
        self.conns: list[dict[str, str]] = [{"version": "0.1.0"}]

        def rec(name: str, ret: object = None) -> Callable[..., object]:
            def _fn(*_a: object, **_k: object) -> object:
                self.calls.append(name)
                return ret

            return _fn

        monkeypatch.setattr(lifecycle, "up", rec("up"))
        monkeypatch.setattr(lifecycle, "down", rec("down"))
        monkeypatch.setattr(seed, "seed_ootb", rec("seed"))
        monkeypatch.setattr(activities, "list_versions", lambda *a, **k: self.acts)
        monkeypatch.setattr(connectors, "list_versions", lambda *a, **k: self.conns)

        def _apply(*_a: object, **_k: object) -> dict[str, object]:
            self.calls.append("apply")
            return {"workspaceId": "ws-prod", "workflowName": "demo", "version": 2}

        def _run(*_a: object, **_k: object) -> dict[str, object]:
            self.calls.append("run")
            return {"runId": "run-1", "status": "running"}

        def _wait(*_a: object, **_k: object) -> dict[str, object]:
            self.calls.append("wait")
            return {"runId": "run-1", "status": self.run_status, "reason": "because"}

        monkeypatch.setattr(workflows, "apply", _apply)
        monkeypatch.setattr(workflows, "run", _run)
        monkeypatch.setattr(workflows, "wait_for", _wait)
        # is_success is the real implementation.


def test_e2e_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    stubs = _Stubs(monkeypatch, run_status="succeeded")
    echoes: list[str] = []
    ok = e2e.run_e2e(_settings(), echo=echoes.append)
    assert ok is True
    assert stubs.calls == ["up", "seed", "apply", "run", "wait"]
    # version id is built from the apply ref.
    assert any("ws-prod/demo@2" in m for m in echoes)
    assert any("PASS" in m for m in echoes)


def test_e2e_failed_run_returns_false(monkeypatch: pytest.MonkeyPatch) -> None:
    _Stubs(monkeypatch, run_status="failed")
    echoes: list[str] = []
    ok = e2e.run_e2e(_settings(), echo=echoes.append)
    assert ok is False
    assert any("FAIL" in m for m in echoes)
    assert any("reason: because" in m for m in echoes)


def test_e2e_skip_up(monkeypatch: pytest.MonkeyPatch) -> None:
    stubs = _Stubs(monkeypatch)
    e2e.run_e2e(_settings(), echo=lambda _m: None, skip_up=True)
    assert "up" not in stubs.calls
    assert stubs.calls[0] == "seed"


def test_e2e_teardown_runs_down(monkeypatch: pytest.MonkeyPatch) -> None:
    stubs = _Stubs(monkeypatch)
    e2e.run_e2e(_settings(), echo=lambda _m: None, teardown=True)
    assert stubs.calls[-1] == "down"


def test_e2e_teardown_runs_even_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    stubs = _Stubs(monkeypatch)

    def _boom(*_a: object, **_k: object) -> None:
        raise RuntimeError("seed exploded")

    monkeypatch.setattr(seed, "seed_ootb", _boom)
    with pytest.raises(RuntimeError, match="seed exploded"):
        e2e.run_e2e(_settings(), echo=lambda _m: None, teardown=True)
    # down still ran via the finally block.
    assert "down" in stubs.calls


def test_e2e_verify_fails_when_activity_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    stubs = _Stubs(monkeypatch)
    stubs.acts = []  # copy-image not registered
    with pytest.raises(RuntimeError, match="OOTB verify failed"):
        e2e.run_e2e(_settings(), echo=lambda _m: None)


def test_e2e_verify_fails_when_connector_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    stubs = _Stubs(monkeypatch)
    stubs.conns = []  # dockerhub connector not registered
    with pytest.raises(RuntimeError, match="OOTB verify failed"):
        e2e.run_e2e(_settings(), echo=lambda _m: None)

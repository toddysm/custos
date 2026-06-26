"""End-to-end stdin/stdout test for the plugin entry point.

These tests drive ``oci_registry_plugin.__main__.main`` with a fake
``sys.stdin`` and pytest's :class:`capsys` so they exercise the JSON
wire contract the runtime relies on. They complement the in-process
tests in ``test_plugin.py`` which call :func:`oci_registry_plugin.handle`
directly.

We rely on :class:`capsys` rather than monkey-patching ``sys.stdout``
because pytest's default capture intercepts the write before the
monkey-patched ``StringIO`` can see it, so the patched stream would
always be empty.
"""

from __future__ import annotations

import io
import json
import sys
from collections.abc import Iterator
from typing import Any

import pytest

from oci_registry_plugin.__main__ import main


@pytest.fixture
def fake_stdin(monkeypatch: pytest.MonkeyPatch) -> Iterator[io.StringIO]:
    stdin = io.StringIO()
    monkeypatch.setattr(sys, "stdin", stdin)
    yield stdin


def _request_envelope() -> dict[str, Any]:
    return {
        "apiVersion": 1,
        "hook": "health",
        "connector": {
            "type": "custos-oci-registry",
            "version": "1.0.0",
            "imageRef": "ghcr.io/example/custos-oci-registry:1.0.0",
            "digest": "sha256:" + "b" * 64,
            "manifest": {"spec": {"target": {"endpoint": "https://registry.example.com"}}},
        },
        "instance": {
            "workspaceId": "ws-1",
            "instanceId": "inst-1",
            "type": "custos-oci-registry",
            "version": "1.0.0",
            "name": "prod",
            "enabled": True,
            "status": "active",
            "healthStatus": "unknown",
            "leaseTtlSeconds": 600,
            "targetConfig": {"repositoryNamespace": "team-a"},
            "credentialsAuthentication": {},
            "usedCapabilities": [],
        },
        "input": {},
    }


def test_main_success_path(fake_stdin: io.StringIO, capsys: pytest.CaptureFixture[str]) -> None:
    fake_stdin.write(json.dumps(_request_envelope()))
    fake_stdin.seek(0)
    exit_code = main(["health"])
    assert exit_code == 0
    response = json.loads(capsys.readouterr().out)
    assert response["ok"] is True
    assert response["result"]["healthy"] is True


def test_main_missing_hook_argument(
    fake_stdin: io.StringIO, capsys: pytest.CaptureFixture[str]
) -> None:
    fake_stdin.write(json.dumps(_request_envelope()))
    fake_stdin.seek(0)
    exit_code = main([])
    assert exit_code == 0
    response = json.loads(capsys.readouterr().out)
    assert response == {
        "ok": False,
        "error": {"code": "invalid-response", "detail": "missing hook argument"},
    }


def test_main_empty_stdin(fake_stdin: io.StringIO, capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["health"])
    assert exit_code == 0
    response = json.loads(capsys.readouterr().out)
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid-response"
    assert "empty request body" in response["error"]["detail"]


def test_main_malformed_json_stdin(
    fake_stdin: io.StringIO, capsys: pytest.CaptureFixture[str]
) -> None:
    fake_stdin.write("{not json")
    fake_stdin.seek(0)
    exit_code = main(["health"])
    assert exit_code == 0
    response = json.loads(capsys.readouterr().out)
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid-response"
    assert "not valid JSON" in response["error"]["detail"]


def test_main_stdin_not_object(fake_stdin: io.StringIO, capsys: pytest.CaptureFixture[str]) -> None:
    fake_stdin.write("[]")
    fake_stdin.seek(0)
    exit_code = main(["health"])
    assert exit_code == 0
    response = json.loads(capsys.readouterr().out)
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid-response"


def test_main_plugin_error_passthrough(
    fake_stdin: io.StringIO, capsys: pytest.CaptureFixture[str]
) -> None:
    req = _request_envelope()
    req["hook"] = "listen"
    req["input"] = {"mode": "pull", "cursor": {"encoding": "x", "value": {}}}
    fake_stdin.write(json.dumps(req))
    fake_stdin.seek(0)
    exit_code = main(["listen"])
    assert exit_code == 0
    response = json.loads(capsys.readouterr().out)
    assert response == {
        "ok": False,
        "error": {
            "code": "cursor-encoding-mismatch",
            "detail": "persisted encoding 'x' != plugin encoding 'oci-list-tags-v1'",
            "data": {"persistedEncoding": "x", "pluginEncoding": "oci-list-tags-v1"},
        },
    }


def test_main_unhandled_exception_becomes_envelope(
    fake_stdin: io.StringIO,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Force ``handle`` to raise a non-PluginError so we exercise the
    catch-all envelope path in ``__main__``."""
    import oci_registry_plugin.__main__ as entry

    def _boom(_hook: str, _request: dict[str, Any]) -> dict[str, Any]:
        raise RuntimeError("simulated upstream library bug")

    monkeypatch.setattr(entry, "handle", _boom)
    fake_stdin.write(json.dumps(_request_envelope()))
    fake_stdin.seek(0)
    exit_code = main(["health"])
    assert exit_code == 0
    response = json.loads(capsys.readouterr().out)
    assert response["ok"] is False
    assert response["error"]["code"] == "unknown-plugin-error"
    assert "simulated upstream library bug" in response["error"]["detail"]

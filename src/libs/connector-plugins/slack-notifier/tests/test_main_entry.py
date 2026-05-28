"""End-to-end stdin/stdout test for the slack-notifier plugin entry point.

These tests drive ``slack_notifier_plugin.__main__.main`` with a fake
``sys.stdin`` and pytest's :class:`capsys` so they exercise the JSON
wire contract the runtime relies on. They complement the in-process
tests in ``test_plugin.py`` which call
:func:`slack_notifier_plugin.handle` directly.

We rely on :class:`capsys` rather than monkey-patching ``sys.stdout``
because pytest's default capture intercepts the write before the
monkey-patched ``StringIO`` can see it.
"""

from __future__ import annotations

import io
import json
import sys
from collections.abc import Iterator
from typing import Any

import pytest

from slack_notifier_plugin.__main__ import main


@pytest.fixture
def fake_stdin(monkeypatch: pytest.MonkeyPatch) -> Iterator[io.StringIO]:
    stdin = io.StringIO()
    monkeypatch.setattr(sys, "stdin", stdin)
    yield stdin


def _request_envelope(hook: str = "health") -> dict[str, Any]:
    return {
        "apiVersion": 1,
        "hook": hook,
        "connector": {
            "type": "custos-slack-notifier",
            "version": "1.0.0",
            "imageRef": "ghcr.io/example/custos-slack-notifier:1.0.0",
            "digest": "sha256:" + "c" * 64,
            "manifest": {"spec": {"target": {"endpoint": "https://hooks.slack.com"}}},
        },
        "instance": {
            "workspaceId": "ws-1",
            "instanceId": "inst-1",
            "type": "custos-slack-notifier",
            "version": "1.0.0",
            "name": "alerts",
            "enabled": True,
            "status": "active",
            "healthStatus": "unknown",
            "leaseTtlSeconds": 600,
            "targetConfig": {"channel": "#deploys"},
            "credentialsAuthentication": {},
            "usedCapabilities": ["slack.post"],
        },
        "input": {"slot": "notification", "capability": "slack.post"} if hook == "bind" else {},
    }


def test_main_health_success(fake_stdin: io.StringIO, capsys: pytest.CaptureFixture[str]) -> None:
    fake_stdin.write(json.dumps(_request_envelope("health")))
    fake_stdin.seek(0)
    exit_code = main(["health"])
    assert exit_code == 0
    response = json.loads(capsys.readouterr().out)
    assert response["ok"] is True
    assert response["result"]["healthy"] is True


def test_main_listen_returns_typed_error(
    fake_stdin: io.StringIO, capsys: pytest.CaptureFixture[str]
) -> None:
    fake_stdin.write(json.dumps(_request_envelope("listen")))
    fake_stdin.seek(0)
    exit_code = main(["listen"])
    assert exit_code == 0
    response = json.loads(capsys.readouterr().out)
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid-response"
    assert "sink connector" in response["error"]["detail"]


def test_main_missing_hook_argument(
    fake_stdin: io.StringIO, capsys: pytest.CaptureFixture[str]
) -> None:
    fake_stdin.write(json.dumps(_request_envelope("health")))
    fake_stdin.seek(0)
    exit_code = main([])
    assert exit_code == 0
    response = json.loads(capsys.readouterr().out)
    assert response == {
        "ok": False,
        "error": {"code": "invalid-response", "detail": "missing hook argument"},
    }


def test_main_malformed_stdin(fake_stdin: io.StringIO, capsys: pytest.CaptureFixture[str]) -> None:
    fake_stdin.write("not json at all")
    fake_stdin.seek(0)
    exit_code = main(["health"])
    assert exit_code == 0
    response = json.loads(capsys.readouterr().out)
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid-response"
    assert "not valid JSON" in response["error"]["detail"]

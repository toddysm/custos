"""End-to-end stdin/stdout test for the GHCR plugin entry point.

Drives ``ghcr_plugin.__main__.main`` with a fake ``sys.stdin`` and
pytest's :class:`capsys` so the JSON wire contract the runtime relies on
is exercised. The success path uses ``bind`` (deterministic, no network)
to keep these tests offline; ``health``'s live probe is covered in
:mod:`test_probe`.
"""

from __future__ import annotations

import io
import json
import sys
from collections.abc import Iterator
from typing import Any

import pytest

from ghcr_plugin.__main__ import main


@pytest.fixture
def fake_stdin(monkeypatch: pytest.MonkeyPatch) -> Iterator[io.StringIO]:
    stdin = io.StringIO()
    monkeypatch.setattr(sys, "stdin", stdin)
    yield stdin


def _request_envelope() -> dict[str, Any]:
    return {
        "apiVersion": 1,
        "hook": "bind",
        "connector": {
            "type": "custos-ghcr",
            "version": "0.1.0",
            "imageRef": "ghcr.io/example/custos-ghcr:0.1.0",
            "digest": "sha256:" + "b" * 64,
            "manifest": {"spec": {"target": {"endpoint": "https://ghcr.io"}}},
        },
        "instance": {
            "workspaceId": "ws-1",
            "instanceId": "inst-1",
            "type": "custos-ghcr",
            "version": "0.1.0",
            "name": "prod",
            "enabled": True,
            "status": "active",
            "healthStatus": "unknown",
            "leaseTtlSeconds": 3600,
            "targetConfig": {"repositoryNamespace": "acme"},
            "credentialsAuthentication": {},
            "usedCapabilities": [],
        },
        "input": {"slot": "source", "capability": "oci.pull"},
    }


def test_main_success_path(fake_stdin: io.StringIO, capsys: pytest.CaptureFixture[str]) -> None:
    fake_stdin.write(json.dumps(_request_envelope()))
    fake_stdin.seek(0)
    exit_code = main(["bind"])
    assert exit_code == 0
    response = json.loads(capsys.readouterr().out)
    assert response["ok"] is True
    assert response["result"]["endpoint"] == "https://ghcr.io/v2/acme"
    assert response["result"]["tokenTypeHint"] == "basic"


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
    exit_code = main(["bind"])
    assert exit_code == 0
    response = json.loads(capsys.readouterr().out)
    assert response == {
        "ok": False,
        "error": {"code": "invalid-response", "detail": "empty request body"},
    }


def test_main_invalid_json(fake_stdin: io.StringIO, capsys: pytest.CaptureFixture[str]) -> None:
    fake_stdin.write("{not json")
    fake_stdin.seek(0)
    exit_code = main(["bind"])
    assert exit_code == 0
    response = json.loads(capsys.readouterr().out)
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid-response"


def test_main_plugin_error_is_enveloped(
    fake_stdin: io.StringIO, capsys: pytest.CaptureFixture[str]
) -> None:
    envelope = _request_envelope()
    envelope["input"] = {"slot": "source", "capability": "s3.read"}
    fake_stdin.write(json.dumps(envelope))
    fake_stdin.seek(0)
    exit_code = main(["bind"])
    assert exit_code == 0
    response = json.loads(capsys.readouterr().out)
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid-response"
    assert "s3.read" in response["error"]["detail"]

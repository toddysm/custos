"""Tests for the workflow catalog + run commands (DEVCLI-IMPL-007)."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

from custosctl import workflows
from custosctl.api import ApiClient
from custosctl.config import Settings, Target

Handler = Callable[[httpx.Request], httpx.Response]


def _client(handler: Handler) -> ApiClient:
    return ApiClient(
        base_url="https://gw.example",
        token="cst_x",
        transport=httpx.MockTransport(handler),
    )


def _settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "target": Target.REMOTE,
        "gateway": "https://gw.example",
        "token": "cst_x",
        "workspace": "ws-prod",
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


# --- workspace resolution -------------------------------------------------


def test_resolve_workspace_flag_wins() -> None:
    assert workflows.resolve_workspace(_settings(), "ws-flag") == "ws-flag"


def test_resolve_workspace_falls_back_to_setting() -> None:
    assert workflows.resolve_workspace(_settings(), None) == "ws-prod"


def test_resolve_workspace_required() -> None:
    with pytest.raises(RuntimeError, match="workspace is required"):
        workflows.resolve_workspace(_settings(workspace=None), None)


# --- apply ----------------------------------------------------------------


def test_apply_posts_definition_text(tmp_path: Path) -> None:
    wf = tmp_path / "wf.yaml"
    wf.write_text("apiVersion: custos.dev/v1\nkind: Workflow\nmetadata:\n  name: demo\n")
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content)
        return httpx.Response(
            201, json={"workspaceId": "ws-prod", "workflowName": "demo", "version": 3}
        )

    with _client(handler) as client:
        ref = workflows.apply(_settings(), path=str(wf), client=client)

    assert ref == {"workspaceId": "ws-prod", "workflowName": "demo", "version": 3}
    assert seen["path"] == "/v1/workspaces/ws-prod/workflows"
    assert "kind: Workflow" in seen["body"]["definition"]  # type: ignore[index]


def test_apply_file_not_found() -> None:
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - not reached
        return httpx.Response(201, json={})

    with _client(handler) as client, pytest.raises(RuntimeError, match="workflow file not found"):
        workflows.apply(_settings(), path="/no/such/file.yaml", client=client)


# --- run ------------------------------------------------------------------


def test_run_posts_version_inputs_and_idempotency_key() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["idem"] = request.headers.get("idempotency-key")
        seen["body"] = json.loads(request.content)
        return httpx.Response(202, json={"runId": "run-1", "status": "running"})

    with _client(handler) as client:
        ref = workflows.run(
            _settings(),
            workflow_version_id="wfv-1",
            inputs={"image": "ghcr.io/x:1"},
            idempotency_key="k1",
            client=client,
        )

    assert ref == {"runId": "run-1", "status": "running"}
    assert seen["path"] == "/v1/workspaces/ws-prod/runs"
    assert seen["idem"] == "k1"
    body = seen["body"]
    assert isinstance(body, dict)
    assert body["workflowVersionId"] == "wfv-1"
    assert body["inputs"] == {"image": "ghcr.io/x:1"}


def test_run_defaults_inputs_to_empty() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        assert body["inputs"] == {}
        return httpx.Response(202, json={"runId": "run-2", "status": "queued"})

    with _client(handler) as client:
        workflows.run(_settings(), workflow_version_id="wfv-2", client=client)


# --- status / wait --------------------------------------------------------


def test_get_status_reads_run() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/workspaces/ws-prod/runs/run-9"
        return httpx.Response(200, json={"runId": "run-9", "status": "succeeded"})

    with _client(handler) as client:
        record = workflows.get_status(_settings(), run_id="run-9", client=client)
    assert record["status"] == "succeeded"


def test_wait_for_polls_until_terminal() -> None:
    statuses = iter(["running", "running", "succeeded"])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"runId": "run-9", "status": next(statuses)})

    slept: list[float] = []
    with _client(handler) as client:
        record = workflows.wait_for(
            _settings(),
            run_id="run-9",
            client=client,
            interval=0.0,
            sleep=slept.append,
        )
    assert record["status"] == "succeeded"
    assert len(slept) == 2  # slept twice before the terminal read


def test_wait_for_times_out() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"runId": "run-9", "status": "running"})

    with _client(handler) as client, pytest.raises(TimeoutError, match="terminal status"):
        workflows.wait_for(
            _settings(),
            run_id="run-9",
            client=client,
            timeout=0.0,  # deadline is immediately in the past after the first read
            interval=0.0,
            sleep=lambda _s: None,
        )


def test_wait_for_rejects_negative_interval() -> None:
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - not reached
        return httpx.Response(200, json={})

    with _client(handler) as client, pytest.raises(RuntimeError, match="non-negative"):
        workflows.wait_for(_settings(), run_id="run-9", client=client, interval=-1.0)


def test_is_success() -> None:
    assert workflows.is_success({"status": "succeeded"}) is True
    assert workflows.is_success({"status": "failed"}) is False

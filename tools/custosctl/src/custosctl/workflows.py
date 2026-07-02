"""Workflow catalog + run commands (DEVCLI-IMPL-007).

Three thin wrappers over the gateway:

* ``apply`` — publish a workflow definition
  (`POST /v1/workspaces/<ws>/workflows`, body ``{definition: <text>}``).
* ``run`` — start a run
  (`POST /v1/workspaces/<ws>/runs`, body ``{workflowVersionId, inputs?, idempotencyKey?}``).
* ``get_status`` / ``wait_for`` — read a run
  (`GET /v1/workspaces/<ws>/runs/<run_id>`) and optionally poll to a terminal state.

Each accepts an injected :class:`~custosctl.api.ApiClient` so the tests drive
them over an ``httpx.MockTransport`` without a live gateway. The workspace comes
from ``--workspace`` / ``CUSTOS_WORKSPACE``.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from custosctl.api import ApiClient, build_client
from custosctl.config import Settings

#: Run statuses that will not change further.
TERMINAL_STATUSES = frozenset({"succeeded", "failed", "cancelled"})
_SUCCESS_STATUS = "succeeded"


def resolve_workspace(settings: Settings, workspace: str | None) -> str:
    """Return the workspace to operate in (flag > ``CUSTOS_WORKSPACE``)."""
    ws = workspace or settings.workspace
    if not ws or not ws.strip():
        raise RuntimeError("a workspace is required — pass --workspace or set CUSTOS_WORKSPACE")
    return ws.strip()


def apply(
    settings: Settings,
    *,
    path: str,
    workspace: str | None = None,
    client: ApiClient | None = None,
) -> dict[str, Any]:
    """Publish the workflow document at ``path``. Returns the version ref."""
    ws = resolve_workspace(settings, workspace)
    definition = _read_text(Path(path))
    owns = client is None
    client = client or build_client(settings)
    try:
        body = client.post(f"/v1/workspaces/{ws}/workflows", json={"definition": definition})
    finally:
        if owns:
            client.close()
    return _as_object(body, "publish")


def run(
    settings: Settings,
    *,
    workflow_version_id: str,
    workspace: str | None = None,
    inputs: dict[str, Any] | None = None,
    idempotency_key: str | None = None,
    client: ApiClient | None = None,
) -> dict[str, Any]:
    """Start a run of ``workflow_version_id``. Returns the run ref."""
    ws = resolve_workspace(settings, workspace)
    payload: dict[str, Any] = {"workflowVersionId": workflow_version_id, "inputs": inputs or {}}
    owns = client is None
    client = client or build_client(settings)
    try:
        body = client.post(
            f"/v1/workspaces/{ws}/runs",
            json=payload,
            idempotency_key=idempotency_key,
        )
    finally:
        if owns:
            client.close()
    return _as_object(body, "run")


def get_status(
    settings: Settings,
    *,
    run_id: str,
    workspace: str | None = None,
    client: ApiClient | None = None,
) -> dict[str, Any]:
    """Fetch a single run record."""
    ws = resolve_workspace(settings, workspace)
    owns = client is None
    client = client or build_client(settings)
    try:
        body = client.get(f"/v1/workspaces/{ws}/runs/{run_id}")
    finally:
        if owns:
            client.close()
    return _as_object(body, "status")


def wait_for(
    settings: Settings,
    *,
    run_id: str,
    workspace: str | None = None,
    timeout: float = 600.0,
    interval: float = 3.0,
    client: ApiClient | None = None,
    sleep: Any = time.sleep,
) -> dict[str, Any]:
    """Poll a run until it reaches a terminal status or ``timeout`` elapses.

    Returns the last run record read. Raises :class:`TimeoutError` if the run
    is still non-terminal when ``timeout`` is exceeded.
    """
    ws = resolve_workspace(settings, workspace)
    owns = client is None
    client = client or build_client(settings)
    deadline = time.monotonic() + timeout
    try:
        while True:
            body = _as_object(client.get(f"/v1/workspaces/{ws}/runs/{run_id}"), "status")
            status = body.get("status")
            if isinstance(status, str) and status in TERMINAL_STATUSES:
                return body
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"run {run_id} did not reach a terminal status within {timeout:.0f}s "
                    f"(last status: {status})"
                )
            sleep(interval)
    finally:
        if owns:
            client.close()


def is_success(run_record: dict[str, Any]) -> bool:
    return run_record.get("status") == _SUCCESS_STATUS


def _read_text(path: Path) -> str:
    if not path.is_file():
        raise RuntimeError(f"workflow file not found: {path}")
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"could not read workflow file {path}: {exc}") from exc


def _as_object(body: Any, what: str) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise RuntimeError(f"unexpected {what} response (expected a JSON object)")
    return body


__all__ = [
    "TERMINAL_STATUSES",
    "apply",
    "get_status",
    "is_success",
    "resolve_workspace",
    "run",
    "wait_for",
]

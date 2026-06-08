#!/usr/bin/env python3
"""Custos helm-test smoke scenario (DEPLOY-IMPL-018).

Drives the public API through the in-cluster api-gateway Service. Uses only the
Python standard library so it runs inside any Custos service image with no extra
dependency to mirror for air-gapped installs.

Phases:
  * readiness  -- always: wait for /readyz, then /healthz + /openapi.json.
  * scenario   -- opt-in (requires CUSTOS_TEST_TOKEN): verify token ->
                  create workspace -> register connector ->
                  (if CUSTOS_WORKFLOW_VERSION_ID) start run -> poll to terminal.

Exit code 0 = pass, 1 = fail. All steps log an actionable one-line result.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE_URL = os.environ["CUSTOS_GATEWAY_URL"].rstrip("/")
PREFIX = os.environ.get("CUSTOS_API_PREFIX", "/v1")
TOKEN = os.environ.get("CUSTOS_TEST_TOKEN", "").strip()
TENANT_ID = os.environ.get("CUSTOS_TENANT_ID", "tenant-default")
WORKSPACE_ID = os.environ.get("CUSTOS_WORKSPACE_ID", "ws-smoke-test")
WORKFLOW_VERSION_ID = os.environ.get("CUSTOS_WORKFLOW_VERSION_ID", "").strip()
READY_TIMEOUT = int(os.environ.get("CUSTOS_READY_TIMEOUT", "120"))
RUN_POLL_TIMEOUT = int(os.environ.get("CUSTOS_RUN_POLL_TIMEOUT", "120"))

TERMINAL_OK = {"succeeded"}
TERMINAL_BAD = {"failed", "cancelled"}


def _log(msg):
    print(msg, flush=True)


def _request(method, path, body=None, token=None, timeout=10):
    url = path if path.startswith("http") else f"{BASE_URL}{path}"
    data = None
    headers = {"accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["content-type"] = "application/json"
    if token:
        headers["authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
            return resp.status, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")
    except urllib.error.URLError as exc:
        return 0, str(exc.reason)


def _json(raw):
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return {}


def fail(step, detail):
    _log(f"FAIL  [{step}] {detail}")
    sys.exit(1)


def wait_ready():
    _log(f"==> waiting up to {READY_TIMEOUT}s for gateway readiness at {BASE_URL}")
    deadline = time.monotonic() + READY_TIMEOUT
    last = ""
    while time.monotonic() < deadline:
        status, raw = _request("GET", "/readyz", timeout=5)
        if status == 200:
            _log("ok    [readyz] gateway is ready")
            return
        last = f"status={status} body={raw[:200]}"
        time.sleep(3)
    fail("readyz", f"gateway not ready within {READY_TIMEOUT}s ({last})")


def check_reachability():
    status, raw = _request("GET", "/healthz", timeout=5)
    if status != 200:
        fail("healthz", f"status={status} body={raw[:200]}")
    _log("ok    [healthz] liveness reports healthy")
    status, raw = _request("GET", "/openapi.json", timeout=10)
    if status != 200:
        fail("openapi", f"status={status} body={raw[:200]}")
    _log("ok    [openapi] API surface is serving /openapi.json")


def authenticated_scenario():
    if not TOKEN:
        _log("skip  [scenario] no CUSTOS_TEST_TOKEN provided; ran readiness/reachability only")
        return

    # 1. login -- verify the supplied service token.
    status, raw = _request("POST", f"{PREFIX}/auth/verify", {"token": TOKEN})
    if status != 200:
        fail("auth.verify", f"status={status} body={raw[:300]}")
    _log(f"ok    [auth.verify] principal={_json(raw).get('principal_id')}")

    # 2. create workspace (tolerate already-exists on re-run).
    body = {"workspace_id": WORKSPACE_ID, "display_name": "Smoke Test Workspace"}
    status, raw = _request(
        "POST", f"{PREFIX}/tenants/{TENANT_ID}/workspaces", body, token=TOKEN
    )
    if status not in (200, 201, 409):
        fail("workspace.create", f"status={status} body={raw[:300]}")
    _log(f"ok    [workspace.create] workspace={WORKSPACE_ID} (status={status})")

    # 3. register connector (tolerate already-exists on re-run).
    body = {
        "type": "oci-registry",
        "version": "1.0.0",
        "name": "smoke-test-registry",
        "enabled": True,
        # oci-registry instances require repositoryNamespace (connector-service
        # validator); endpoint alone 400s.
        "targetConfig": {"repositoryNamespace": "smoke/test"},
    }
    status, raw = _request(
        "POST", f"{PREFIX}/workspaces/{WORKSPACE_ID}/connectors", body, token=TOKEN
    )
    if status not in (200, 201, 409):
        fail("connector.register", f"status={status} body={raw[:300]}")
    _log(f"ok    [connector.register] type=oci-registry (status={status})")

    # 4 + 5. start workflow run + inspect it (requires a published version).
    if not WORKFLOW_VERSION_ID:
        _log("skip  [workflow] no CUSTOS_WORKFLOW_VERSION_ID; skipping run start/inspect")
        return
    body = {
        "workflowVersionId": WORKFLOW_VERSION_ID,
        "inputs": {},
        # Nanosecond resolution so back-to-back reruns don't collide on the
        # idempotency key and unintentionally replay the same run.
        "idempotencyKey": f"smoke-{time.time_ns()}",
    }
    status, raw = _request(
        "POST", f"{PREFIX}/workspaces/{WORKSPACE_ID}/runs", body, token=TOKEN
    )
    if status not in (200, 202):
        fail("run.start", f"status={status} body={raw[:300]}")
    run_id = _json(raw).get("runId")
    if not run_id:
        fail("run.start", f"no runId in response body={raw[:300]}")
    _log(f"ok    [run.start] run={run_id}")
    poll_run(run_id)


def poll_run(run_id):
    deadline = time.monotonic() + RUN_POLL_TIMEOUT
    last = ""
    while time.monotonic() < deadline:
        status, raw = _request(
            "GET", f"{PREFIX}/workspaces/{WORKSPACE_ID}/runs/{run_id}", token=TOKEN
        )
        if status != 200:
            fail("run.inspect", f"status={status} body={raw[:300]}")
        state = (_json(raw).get("status") or "").lower()
        last = state
        if state in TERMINAL_OK:
            _log(f"ok    [run.inspect] run {run_id} reached '{state}'")
            return
        if state in TERMINAL_BAD:
            fail("run.inspect", f"run {run_id} reached terminal '{state}': {raw[:300]}")
        time.sleep(3)
    fail("run.inspect", f"run {run_id} did not finish within {RUN_POLL_TIMEOUT}s (last='{last}')")


def main():
    _log("=== Custos helm-test smoke scenario ===")
    wait_ready()
    check_reachability()
    authenticated_scenario()
    _log("PASS  smoke scenario completed successfully")


if __name__ == "__main__":
    main()

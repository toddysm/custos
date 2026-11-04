"""Verify the bundled activity against the actual production Sidecar over a Unix socket."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
import uvicorn
from custos_callctx.bootstrap import BoundTriple, mint_bootstrap_token
from custos_sidecar.auth import BootstrapTokenVerifier
from custos_sidecar.broker import RuntimeBundle, create_broker_app


@pytest.mark.parametrize("valid", [True, False])
async def test_bundled_client_uses_actual_sidecar_authentication(valid: bool) -> None:
    """Run the real activity transport and verifier, simulating only the remote issuer response."""
    triple = BoundTriple("run", "step", 1)
    key = b"k" * 32
    bundle = RuntimeBundle.model_validate(
        {
            "workspaceId": "ws",
            "runId": "run",
            "stepId": "step",
            "attempt": 1,
            "endpoint": "https://runtime.internal",
            "proof": "private-attempt-proof-" + "x" * 32,
        }
    )
    calls: list[dict[str, object]] = []

    def upstream(request: httpx.Request) -> httpx.Response:
        """Check the distinct private-hop authority and return genuine protocol-two framing."""
        assert request.headers["authorization"] == "Bearer " + bundle.proof.get_secret_value()
        assert "Custos-Sidecar-Token" not in request.headers
        body = json.loads(request.content)
        calls.append(body)
        assert (body["workspaceId"], body["runId"], body["stepId"], body["attempt"]) == (
            "ws",
            "run",
            "step",
            1,
        )
        if body["operation"] == "release":
            assert body["leaseId"] == "upstream-lease"
            return httpx.Response(200, json={"released": True})
        assert (body["slot"], body["capability"]) == ("source", "oci.pull")
        now = datetime.now(UTC)
        return httpx.Response(
            200,
            json={
                "schemaVersion": "2",
                "lease": {
                    "leaseId": "upstream-lease",
                    "workspaceId": "ws",
                    "runId": "run",
                    "stepId": "step",
                    "attempt": 1,
                    "slot": "source",
                    "capability": "oci.pull",
                    "connectorInstanceId": "instance",
                    "expiresAt": (now + timedelta(minutes=5)).isoformat(),
                },
                "credential": {
                    "authenticationType": "x-dapr-secret",
                    "material": {"username": "registry-user", "token": "synthetic-test-pat"},
                    "issuedAt": now.isoformat(),
                    "expiresAt": None,
                },
            },
        )

    with tempfile.TemporaryDirectory(prefix="copy-sidecar-", dir="/tmp") as directory:
        root = Path(directory)
        (root / "in").mkdir()
        (root / "run").mkdir()
        token = mint_bootstrap_token(key=key, triple=triple, ttl_sec=300, now=time.time())
        (root / "in/sidecar-token").write_text(token if valid else "invalid-test-token")
        (root / "in/ctx.json").write_text(
            json.dumps(
                {
                    "workspaceId": "ws",
                    "runId": "run",
                    "stepId": "step",
                    "attempt": 1,
                }
            )
        )
        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as http:
            app = create_broker_app(
                bundle,
                BootstrapTokenVerifier(key=key, triple=triple),
                http_client=http,
            )
            server = uvicorn.Server(
                uvicorn.Config(
                    app,
                    uds=str(root / "run/connector.sock"),
                    log_config=None,
                    access_log=False,
                )
            )
            serving = asyncio.create_task(server.serve())
            process: asyncio.subprocess.Process | None = None
            try:
                async with asyncio.timeout(5):
                    while not server.started:
                        if serving.done():
                            await serving
                            pytest.fail("Sidecar stopped before opening its socket")
                        await asyncio.sleep(0.01)
                activity = (
                    Path(__file__).resolve().parents[4] / "extensions/activities/copy-image/app"
                )
                script = (
                    "from copy_image.contract import Sandbox, ActivityError, exit_code_for\n"
                    "from copy_image import credentials\n"
                    "s = Sandbox.from_env()\n"
                    "try:\n"
                    "    c = credentials.read_slot_credentials(s, 'source')\n"
                    "    assert c.username == 'registry-user'\n"
                    "    assert c.secret == 'synthetic-test-pat'\n"
                    "    assert c.provider_expires_at is None\n"
                    "    credentials.release_slot_credentials(s, 'source', c)\n"
                    "except ActivityError as error:\n"
                    "    raise SystemExit(exit_code_for(error.error_class))\n"
                    "print('released')\n"
                )
                process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-c",
                    script,
                    env={**os.environ, "PYTHONPATH": str(activity), "CUSTOS_IO_ROOT": str(root)},
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await asyncio.wait_for(process.communicate(), 5)
                assert process.returncode == (0 if valid else 2), stderr.decode()
                assert stdout == (b"released\n" if valid else b"")
            finally:
                if process is not None and process.returncode is None:
                    process.kill()
                    await process.wait()
                server.should_exit = True
                await asyncio.wait_for(serving, 5)
    assert [call["operation"] for call in calls] == (["issue", "release"] if valid else [])

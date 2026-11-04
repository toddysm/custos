"""Real production Sidecar, binary pod-exec delivery and private-mount isolation on kind."""

import asyncio
import json
import os
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

from custos_arm.contract import ImageRef
from custos_arm.runtime import SidecarSpec, TmpfsMount
from custos_arm.runtime.oci import OciContainerDriver
from custos_arm.scheduler.fsio import FilesystemSecretSink
from custos_arm.secrets import SidecarTokenMinter
from tests.integration.test_oci_lifecycle_integration import _plan

pytestmark = pytest.mark.integration


@pytest.fixture
def production_apis() -> Iterator[tuple[object, object]]:
    """Require the configured cluster; missing infrastructure fails explicitly."""
    config.load_kube_config()
    with client.ApiClient() as api:
        core, batch = client.CoreV1Api(api), client.BatchV1Api(api)
        try:
            core.create_namespace({"metadata": {"name": "custos-arm-e2e"}}, _request_timeout=10)
        except ApiException as exc:
            if exc.status != 409:
                raise
        yield batch, core


async def test_real_private_bootstrap_and_unix_socket(
    production_apis: tuple[object, object],
    tmp_path: Path,
) -> None:
    """Verify an ARM-minted token at the real Sidecar after binary Kubernetes delivery."""
    image = os.environ["CUSTOS_ARM_BOOTSTRAP_IMAGE"]
    sidecar_image = os.environ["CUSTOS_ARM_SIDECAR_IMAGE"]
    base = _plan(image=image, run_id="bootstrap-" + uuid4().hex)
    plan = replace(
        base,
        workspace_id="bootstrap-workspace",
        image=ImageRef(ref=image),
        sidecar=SidecarSpec(sidecar_image, "https://127.0.0.1:1", credential_broker=True),
        tmpfs_mounts=(
            *base.tmpfs_mounts,
            TmpfsMount("/custos/sidecar", read_only=True, size_limit="16Ki"),
            TmpfsMount("/custos/run", size_limit="1Mi"),
        ),
    )
    driver = OciContainerDriver(
        batch_api=production_apis[0],
        core_api=production_apis[1],
        staging_root=tmp_path,
        api_timeout=10,
    )
    handle = await asyncio.to_thread(driver.prepare, plan)
    token = SidecarTokenMinter().mint(
        step=plan.step,
        workspace_id=plan.workspace_id,
        deadline=plan.deadline,
    )
    sink = FilesystemSecretSink(handle.input_root, private_root=handle.sidecar_root)
    try:
        await sink.write_secret(
            relative_path="sidecar-token", content=token.value.encode(), mode=0o400
        )
        await sink.write_private_secret(
            relative_path="sidecar-key", content=token.verification_key, mode=0o400
        )
        await sink.write_private_secret(
            relative_path="runtime.json",
            mode=0o400,
            content=json.dumps(
                {
                    "workspaceId": plan.workspace_id,
                    "runId": plan.step.run_id,
                    "stepId": plan.step.step_id,
                    "attempt": plan.step.attempt,
                    "endpoint": "https://127.0.0.1:1",
                    "proof": "private-probe-" + "x" * 40,
                }
            ).encode(),
        )
        await asyncio.to_thread(driver.start, handle)
        outcome = await asyncio.to_thread(driver.await_terminal, handle, plan.deadline)
        assert outcome.exit_code == 0
        output = await asyncio.to_thread(driver.collect, handle)
        result = json.loads((output.root / "outputs.json").read_bytes())
        assert result == {"privateVisible": False, "valid": 503, "invalid": 401}
        assert token.value not in json.dumps(result)
    finally:
        await asyncio.to_thread(driver.cleanup, handle)

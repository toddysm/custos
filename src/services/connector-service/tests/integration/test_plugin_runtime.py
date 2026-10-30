from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.catalog_store import ConnectorTypeVersion
from custos_spl.interfaces.connector_instance_store import ConnectorInstance

from custos_connector.runtime import (
    CursorEncodingMismatch,
    CursorEnvelope,
    CursorExpired,
    DockerCliHookRunner,
    ListenMode,
    PluginInvoker,
    UpstreamUnauthorized,
    UpstreamUnreachable,
)


def _connector(image_ref: str) -> ConnectorTypeVersion:
    return ConnectorTypeVersion(
        type="stub",
        version="1.0.0",
        digest="sha256:manifest",
        image_ref=image_ref,
        normalized_manifest={
            "metadata": {"type": "stub", "version": "1.0.0"},
            "spec": {
                "events": {
                    "pull": {
                        "cursorEncoding": "stub-cursor-v1",
                    }
                }
            },
        },
        parent_deprecated=False,
        published_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


def _instance(*, host: str = "registry.example.com") -> ConnectorInstance:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    return ConnectorInstance(
        workspace_id=WorkspaceId("ws_123"),
        instance_id=ConnectorInstanceId("conn_123"),
        type="stub",
        version="1.0.0",
        name="stub connection",
        lease_ttl_seconds=600,
        enabled=True,
        status="enabled",
        health_status="healthy",
        target_config={"host": host},
        credentials_authentication={"issuerUri": "https://issuer.example.com"},
        used_capabilities=("oci.pull",),
        created_at=now,
        updated_at=now,
    )


@pytest.fixture(scope="module")
def stub_plugin_image() -> Iterator[str]:
    if shutil.which("docker") is None:
        if os.environ.get("CI") == "true":
            pytest.fail("Docker is required for hosted hook-lifecycle validation")
        pytest.skip("docker CLI not available")

    fixture_dir = Path(__file__).resolve().parents[1] / "fixtures" / "plugin_runtime_stub"
    tag = f"custos-plugin-runtime-stub:{uuid4().hex[:12]}"
    build = subprocess.run(
        ["docker", "build", "-t", tag, str(fixture_dir)],
        capture_output=True,
        text=True,
        check=False,
    )
    if build.returncode != 0:
        if os.environ.get("CI") == "true":
            pytest.fail(f"runtime stub build failed: {build.stderr.strip()}")
        pytest.skip(f"could not build runtime stub image: {build.stderr.strip()}")

    try:
        yield tag
    finally:
        subprocess.run(
            ["docker", "image", "rm", "-f", tag],
            capture_output=True,
            text=True,
            check=False,
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_real_container_stops_before_runner_returns(
    stub_plugin_image: str,
    cancel: bool,
) -> None:
    """Interrupt a running hook, then verify its actual owned container is gone."""
    label = "custos.hook-test=" + uuid4().hex

    async def containers(*, running: bool = False) -> list[str]:
        """Inspect only this test's uniquely labelled resources."""
        args = ["docker", "ps", "-aq", "--filter", "label=" + label]
        if running:
            args += ["--filter", "status=running"]
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        assert proc.returncode == 0, stderr.decode("utf-8", "replace")
        return stdout.decode().split()

    runner = DockerCliHookRunner(extra_run_args=("--label", label, "--entrypoint", "sleep"))
    task = asyncio.create_task(
        runner.run(
            image_ref=stub_plugin_image,
            hook="30",
            payload={},
            timeout_seconds=30 if cancel else 5,
        )
    )
    try:
        async with asyncio.timeout(10):
            while not await containers(running=True):
                assert not task.done(), "hook ended before a running container was observed"
                await asyncio.sleep(0.05)
        if cancel:
            task.cancel()
        with pytest.raises(asyncio.CancelledError if cancel else TimeoutError):
            await task
        assert not await containers()
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        for identifier in await containers():
            proc = await asyncio.create_subprocess_exec(
                "docker",
                "rm",
                "--force",
                identifier,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await proc.communicate()
            assert proc.returncode == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_docker_cli_runner_health_success(stub_plugin_image: str) -> None:
    invoker = PluginInvoker(DockerCliHookRunner())
    result = await invoker.health(
        connector=_connector(stub_plugin_image),
        instance=_instance(),
    )
    assert result.healthy is True
    assert result.detail == "ok"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_docker_cli_runner_bind_success(stub_plugin_image: str) -> None:
    invoker = PluginInvoker(DockerCliHookRunner())
    result = await invoker.bind(
        connector=_connector(stub_plugin_image),
        instance=_instance(),
        slot="source",
        capability="oci.pull",
        identity_material={"kind": "oidc"},
    )
    assert result.endpoint == "https://registry.example.com"
    assert result.token_type_hint == "bearer"


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("host", "expected"),
    [
        ("down.example", UpstreamUnreachable),
        ("bad-auth.example", UpstreamUnauthorized),
    ],
)
async def test_docker_cli_runner_health_maps_upstream_errors(
    stub_plugin_image: str,
    host: str,
    expected: type[Exception],
) -> None:
    invoker = PluginInvoker(DockerCliHookRunner())
    with pytest.raises(expected):
        await invoker.health(
            connector=_connector(stub_plugin_image),
            instance=_instance(host=host),
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_docker_cli_runner_maps_cursor_expired(stub_plugin_image: str) -> None:
    invoker = PluginInvoker(DockerCliHookRunner())
    with pytest.raises(CursorExpired):
        await invoker.listen(
            connector=_connector(stub_plugin_image),
            instance=_instance(),
            mode=ListenMode.PULL,
            cursor=CursorEnvelope(encoding="stub-cursor-v1", value="expired"),
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_docker_cli_runner_maps_cursor_encoding_mismatch(stub_plugin_image: str) -> None:
    invoker = PluginInvoker(DockerCliHookRunner())
    with pytest.raises(CursorEncodingMismatch):
        await invoker.listen(
            connector=_connector(stub_plugin_image),
            instance=_instance(),
            mode=ListenMode.PULL,
            cursor=CursorEnvelope(encoding="wrong-cursor-v9", value="cursor-1"),
        )

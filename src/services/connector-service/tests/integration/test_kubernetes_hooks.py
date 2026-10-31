"""Real namespace-scoped hook execution; enabled only by the dedicated hosted Kubernetes job."""

from __future__ import annotations

import asyncio
import copy
import json
import os
import socket
import subprocess
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from ipaddress import IPv4Address, ip_address
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
import yaml
from aiohttp import web
from kubernetes_asyncio import client, config

from custos_connector.hook_images import RegistryHookImageResolver
from custos_connector.hook_kubernetes_transport import KubernetesHookTransport
from custos_connector.hook_runtime import InClusterHookRunner
from custos_connector.hook_sandbox import HookPod, HookSandboxSettings
from custos_connector.runtime import PluginCleanupFailed, PluginInvocationFailed

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("CUSTOS_HOOK_KUBERNETES_TEST") != "1",
        reason="dedicated Kubernetes hook job is required",
    ),
]


@dataclass
class Runtime:
    """Owned namespace, controller identity, observed evidence and upstream effect counter."""

    runner: InClusterHookRunner
    api: Any
    namespace: str
    plugin: str
    documents: list[object]
    logs: list[str]
    effect_url: str
    effects: list[bytes]
    effect_seen: asyncio.Event


@pytest_asyncio.fixture
async def runtime(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Runtime]:
    """Install Helm permissions and execute with the controller service-account token."""
    namespace = "hook-tests-" + uuid4().hex[:8]
    controller_namespace = namespace + "-controller"
    helper = os.environ["CUSTOS_HOOK_HELPER_IMAGE"]
    plugin = os.environ["CUSTOS_HOOK_PLUGIN_IMAGE"].removeprefix("localhost:5001/")
    network = subprocess.run(
        ["docker", "network", "inspect", "kind"],
        check=True,
        capture_output=True,
        text=True,
    )
    effect_host = _effect_host(json.loads(network.stdout)[0])
    repository = Path(__file__).resolve().parents[5]
    chart = repository / "deploy/helm/charts/connector-service"
    render = subprocess.run(
        [
            "helm",
            "template",
            "hook-test",
            str(chart),
            "--namespace",
            controller_namespace,
            "--set",
            "hookRuntime.enabled=true",
            "--set",
            f"hookRuntime.namespace={namespace}",
            "--set",
            f"hookRuntime.helperImage={helper}",
            "--set",
            "hookRuntime.registryUrl=http://localhost:5001",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    kinds = {"Namespace", "ServiceAccount", "Role", "RoleBinding", "NetworkPolicy"}
    documents = [row for row in yaml.safe_load_all(render.stdout) if row and row["kind"] in kinds]
    for row in documents:
        if row["kind"] != "Namespace":
            row["metadata"].setdefault("namespace", controller_namespace)
    documents.insert(
        0,
        {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": controller_namespace},
        },
    )
    subprocess.run(
        ["kubectl", "--context", "kind-custos-hooks", "apply", "-f", "-"],
        input=json.dumps({"apiVersion": "v1", "kind": "List", "items": documents}),
        check=True,
        capture_output=True,
        text=True,
    )
    admin = client.Configuration()
    await config.load_kube_config(context="kind-custos-hooks", client_configuration=admin)
    credentials = copy.deepcopy(admin)
    token = subprocess.run(
        [
            "kubectl",
            "--context",
            "kind-custos-hooks",
            "create",
            "token",
            "hook-test-connector-service",
            "--namespace",
            controller_namespace,
            "--duration=10m",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    credentials.cert_file = None
    credentials.key_file = None
    credentials.api_key = {"BearerToken": token}
    credentials.api_key_prefix = {"BearerToken": "Bearer"}
    credentials.refresh_api_key_hook = None
    if "BearerToken" not in credentials.auth_settings():
        raise RuntimeError("SDK did not bind the scoped controller credential")
    observed: list[object] = []
    logs: list[str] = []
    effects: list[bytes] = []
    effect_seen = asyncio.Event()
    original_create = KubernetesHookTransport.create
    original_release = KubernetesHookTransport.release

    async def create(transport: KubernetesHookTransport, manifest: Mapping[str, Any]) -> HookPod:
        """Retain the actual admitted object, without modifying production admission logic."""
        pod = await original_create(transport, manifest)
        raw = await transport.api.read_namespaced_pod(pod.name, namespace)
        observed.append(transport.api.api_client.sanitize_for_serialization(raw))
        return pod

    async def release(transport: KubernetesHookTransport, pod: HookPod) -> None:
        """Read application logs while the positively terminated Pod is still retained."""
        logs.append(await inspector.read_namespaced_pod_log(pod.name, namespace, container="hook"))
        await original_release(transport, pod)

    async def effect(request: web.Request) -> web.Response:
        """Count real plugin upstream effects independently of the exec reply."""
        effects.append(await request.read())
        effect_seen.set()
        return web.Response(text="recorded")

    monkeypatch.setattr(KubernetesHookTransport, "create", create)
    monkeypatch.setattr(KubernetesHookTransport, "release", release)
    application = web.Application()
    application.router.add_post("/", effect)
    server = web.AppRunner(application)
    await server.setup()
    listener = socket.socket()
    listener.bind((effect_host, 0))
    listener.listen()
    listener.setblocking(False)
    port = listener.getsockname()[1]
    site = web.SockSite(server, listener)
    await site.start()
    try:
        async with (
            client.ApiClient(credentials) as api,
            client.ApiClient(admin) as observation_api,
            httpx.AsyncClient(base_url="http://localhost:5001") as registry,
        ):
            inspector = client.CoreV1Api(observation_api)
            runner = InClusterHookRunner(
                credentials,
                RegistryHookImageResolver(registry, architecture="amd64"),
                HookSandboxSettings(namespace, helper, "connector-hook"),
            )
            await runner.check_ready()
            yield Runtime(
                runner,
                client.CoreV1Api(api),
                namespace,
                plugin,
                observed,
                logs,
                f"http://{effect_host}:{port}/",
                effects,
                effect_seen,
            )
    finally:
        await server.cleanup()
        listener.close()
        async with client.ApiClient(admin) as api:
            core = client.CoreV1Api(api)
            await core.delete_namespace(namespace)
            await core.delete_namespace(controller_namespace)


def _effect_host(network: Mapping[str, Any]) -> str:
    """Select the IPv4 gateway regardless of Docker's dual-stack IPAM ordering."""
    gateways = [ip_address(row["Gateway"]) for row in network["IPAM"]["Config"]]
    candidates = [address for address in gateways if isinstance(address, IPv4Address)]
    if len(candidates) != 1 or candidates[0].is_unspecified or candidates[0].is_multicast:
        raise ValueError("the isolated test network must have exactly one concrete IPv4 gateway")
    return str(candidates[0])


async def assert_clean(runtime: Runtime, canary: str) -> None:
    """Verify removal and absence of private data in admitted objects and Pod logs."""
    pods = await runtime.api.list_namespaced_pod(runtime.namespace)
    assert not pods.items
    assert runtime.documents and runtime.logs
    assert canary not in json.dumps(runtime.documents)
    assert all(canary not in text for text in runtime.logs)


@pytest.mark.parametrize("mode,exit_code", [("echo", 0), ("exit", 7)])
async def test_real_private_reply_and_exit_status(
    runtime: Runtime, mode: str, exit_code: int
) -> None:
    """Preserve the original entrypoint, private payload and plugin exit status."""
    canary = "synthetic-canary-" + uuid4().hex
    reply = await runtime.runner.run(
        image_ref=runtime.plugin,
        hook="health",
        payload={"mode": mode, "canary": canary},
        timeout_seconds=60,
    )
    assert reply.exit_code == exit_code
    assert json.loads(reply.stdout) == {"hook": "health", "echo": canary}
    assert reply.stderr.strip() == canary.encode()
    await assert_clean(runtime, canary)


async def test_real_output_overflow_is_bounded_and_stopped(runtime: Runtime) -> None:
    """Overflow must fail after the actual owned sandbox is stopped and removed."""
    canary = "synthetic-canary-" + uuid4().hex
    with pytest.raises(PluginInvocationFailed):
        await runtime.runner.run(
            image_ref=runtime.plugin,
            hook="listen",
            payload={"mode": "overflow", "canary": canary},
            timeout_seconds=60,
        )
    await assert_clean(runtime, canary)


async def test_collision_has_one_effect_and_cancellation_owns_cleanup(runtime: Runtime) -> None:
    """An API creation collision cannot execute a second poll or kill the first invocation."""
    canary = "synthetic-canary-" + uuid4().hex
    invocation = uuid4().hex
    payload = {"mode": "wait", "canary": canary, "effectUrl": runtime.effect_url}
    first = asyncio.create_task(
        runtime.runner.run(
            image_ref=runtime.plugin,
            hook="listen",
            payload=payload,
            timeout_seconds=120,
            invocation_id=invocation,
        )
    )
    try:
        async with asyncio.timeout(60):
            await runtime.effect_seen.wait()
        with pytest.raises(PluginCleanupFailed):
            await runtime.runner.run(
                image_ref=runtime.plugin,
                hook="listen",
                payload=payload,
                timeout_seconds=30,
                invocation_id=invocation,
            )
        assert len(runtime.effects) == 1 and not first.done()
        first.cancel()
        await asyncio.sleep(0)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
    finally:
        if not first.done():
            first.cancel()
        await asyncio.gather(first, return_exceptions=True)
    assert len(runtime.effects) == 1
    await assert_clean(runtime, canary)


async def test_actual_execution_timeout_retains_cleanup_ownership(runtime: Runtime) -> None:
    """A timed-out hook is not reported stopped until the real Pod terminal state is observed."""
    canary = "synthetic-canary-" + uuid4().hex
    with pytest.raises(TimeoutError):
        await runtime.runner.run(
            image_ref=runtime.plugin,
            hook="listen",
            payload={"mode": "wait", "canary": canary, "effectUrl": runtime.effect_url},
            timeout_seconds=15,
        )
    assert len(runtime.effects) == 1
    await assert_clean(runtime, canary)

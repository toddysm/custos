"""Exercise the actual SDK adapter's admission and UID preconditions with controlled API replies."""

from __future__ import annotations

import struct
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest
from kubernetes_asyncio import client
from kubernetes_asyncio.client.exceptions import ApiException

from custos_connector.hook_kubernetes_transport import KubernetesHookTransport, _admission_matches
from custos_connector.hook_sandbox import (
    HOOK_FINALIZER,
    MAX_HOOK_FRAME_BYTES,
    HookImage,
    HookSandboxSettings,
    build_hook_pod,
)
from custos_connector.runtime import PluginCleanupFailed, PluginInvocationFailed


def manifest() -> dict[str, Any]:
    """Build the real inert hardened pod specification."""
    return build_hook_pod(
        HookSandboxSettings("custos-hooks", "registry/helper@sha256:" + "2" * 64, "hook"),
        HookImage("registry/plugin@sha256:" + "1" * 64, ("/plugin",)),
        "owned",
        "a" * 32,
        30,
    )


def observed(plan: dict[str, Any]) -> dict[str, Any]:
    """Add the fields a real API server and kubelet provide."""
    pod = deepcopy(plan)
    pod["metadata"].update({"uid": "uid-one", "resourceVersion": "7"})
    pod["status"] = {
        "containerStatuses": [
            {
                "name": "hook",
                "containerID": "containerd://one",
                "ready": True,
                "restartCount": 0,
                "state": {"running": {"startedAt": "2026-10-30T00:00:00Z"}},
            }
        ]
    }
    return pod


def transport_for(pod: dict[str, Any]) -> tuple[KubernetesHookTransport, SimpleNamespace]:
    """Expose the real adapter to a narrow, observable SDK seam."""
    api = SimpleNamespace(
        api_client=SimpleNamespace(sanitize_for_serialization=lambda value: value),
        create_namespaced_pod=AsyncMock(return_value=pod),
        read_namespaced_pod=AsyncMock(return_value=pod),
        delete_namespaced_pod=AsyncMock(),
        patch_namespaced_pod=AsyncMock(),
    )
    return KubernetesHookTransport(api, client.Configuration(), "custos-hooks"), api


@pytest.mark.parametrize(
    "mutation",
    [
        "privileged",
        "image",
        "host",
        "extra-container",
        "env",
        "volume",
        "working-dir",
        "probe",
        "sidecar",
        "ports",
        "dns",
        "termination-path",
        "unknown",
    ],
)
def test_changed_admission_is_not_executable(mutation: str) -> None:
    """Admission cannot add privileges, payload channels or an autonomous replacement command."""
    plan = manifest()
    pod = observed(plan)
    assert _admission_matches(pod, plan)
    if mutation == "privileged":
        pod["spec"]["containers"][0]["securityContext"]["privileged"] = True
    elif mutation == "image":
        pod["spec"]["containers"][0]["image"] = "other:mutable"
    elif mutation == "host":
        pod["spec"]["hostPID"] = True
    elif mutation == "extra-container":
        pod["spec"]["containers"].append({"name": "unexpected"})
    elif mutation == "env":
        pod["spec"]["containers"][0]["env"] = [{"name": "PRIVATE", "value": "forbidden"}]
    elif mutation == "volume":
        pod["spec"]["volumes"].append({"name": "host", "hostPath": {"path": "/"}})
    elif mutation == "working-dir":
        pod["spec"]["containers"][0]["workingDir"] = "/injected"
    elif mutation == "probe":
        pod["spec"]["containers"][0]["livenessProbe"] = {"exec": {"command": ["/injected"]}}
    elif mutation == "sidecar":
        pod["spec"]["initContainers"][0]["restartPolicy"] = "Always"
    elif mutation == "ports":
        pod["spec"]["containers"][0]["ports"] = [{"containerPort": 8080, "hostPort": 8080}]
    elif mutation == "dns":
        pod["spec"]["hostAliases"] = [{"ip": "192.0.2.1", "hostnames": ["upstream.example"]}]
    elif mutation == "termination-path":
        pod["spec"]["containers"][0]["terminationMessagePath"] = "/custos/io/private"
    else:
        pod["spec"]["containers"][0]["futureExecutionField"] = "unexpected"
    assert not _admission_matches(pod, plan)


def test_defaulted_container_fields_preserve_admission() -> None:
    """Normal API defaults remain valid without allowing injected execution fields."""
    plan = manifest()
    pod = observed(plan)
    for container in pod["spec"]["containers"] + pod["spec"]["initContainers"]:
        container.update(
            {
                "imagePullPolicy": "IfNotPresent",
                "terminationMessagePath": "/dev/termination-log",
                "terminationMessagePolicy": "File",
                "stdin": False,
                "tty": False,
            }
        )
    assert _admission_matches(pod, plan)


async def test_uid_preconditions_and_owned_finalizer_removal() -> None:
    """Deletion and finalizer mutation preserve the exact UID and unrelated finalizers."""
    plan = manifest()
    pod = observed(plan)
    pod["metadata"]["finalizers"].append("another.example/finalizer")
    transport, api = transport_for(pod)
    owned = await transport.create(plan)
    assert owned.running and owned.spec_verified and not owned.terminated
    await transport.stop(owned)
    assert api.delete_namespaced_pod.await_args.kwargs["body"]["preconditions"] == {
        "uid": "uid-one"
    }
    await transport.release(owned)
    patch = api.patch_namespaced_pod.await_args.kwargs["body"]["metadata"]
    assert (
        api.patch_namespaced_pod.await_args.kwargs["_content_type"]
        == "application/merge-patch+json"
    )
    assert patch == {
        "uid": "uid-one",
        "resourceVersion": "7",
        "finalizers": ["another.example/finalizer"],
    }


async def test_collision_never_adopts_or_overwrites_active_ownership() -> None:
    """A second call cannot replace the original admission record or touch its Pod."""
    plan = manifest()
    transport, api = transport_for(observed(plan))
    owned = await transport.create(plan)
    with pytest.raises(PluginCleanupFailed):
        await transport.create(plan)
    assert api.create_namespaced_pod.await_count == 1
    assert await transport.observe(owned.name) == owned
    api.delete_namespaced_pod.assert_not_awaited()


@pytest.mark.parametrize("status", [403, 409, 500])
async def test_create_error_classification(status: int) -> None:
    """Only definite rejection can be treated as a non-created invocation."""
    transport, api = transport_for(observed(manifest()))
    api.create_namespaced_pod.side_effect = ApiException(status=status)
    expected = PluginInvocationFailed if status == 403 else PluginCleanupFailed
    with pytest.raises(expected):
        await transport.create(manifest())
    assert not transport._expected
    api.delete_namespaced_pod.assert_not_awaited()


@pytest.mark.parametrize("error_type", [ValueError, TypeError, RuntimeError, Exception])
@pytest.mark.parametrize("normalization", [False, True])
async def test_decode_failures_after_possible_creation_remain_uncertain(
    error_type: type[Exception],
    normalization: bool,
) -> None:
    """SDK decoding and local normalization errors cannot certify that creation was rejected."""
    transport, api = transport_for(observed(manifest()))
    failure = error_type("synthetic decoding failure")
    if normalization:
        api.api_client.sanitize_for_serialization = Mock(side_effect=failure)
    else:
        api.create_namespaced_pod.side_effect = failure
    with pytest.raises(PluginCleanupFailed):
        await transport.create(manifest())
    assert not transport._expected
    api.delete_namespaced_pod.assert_not_awaited()


async def test_absence_is_distinct_from_backend_failure() -> None:
    """Only an actual404 produces absence; transport failures are not successful empty reads."""
    transport, api = transport_for(observed(manifest()))
    api.read_namespaced_pod.side_effect = ApiException(status=404)
    assert await transport.observe("owned") is None
    api.read_namespaced_pod.side_effect = ApiException(status=500)
    with pytest.raises(PluginInvocationFailed):
        await transport.observe("owned")


async def test_finalizer_loss_cannot_confirm_termination() -> None:
    """An object whose proof-retention finalizer disappeared is no longer a trusted lifecycle."""
    plan = manifest()
    pod = observed(plan)
    transport, _ = transport_for(pod)
    await transport.create(plan)
    pod["metadata"]["finalizers"].remove(HOOK_FINALIZER)
    state = await transport.observe("owned")
    assert state is not None and not state.spec_verified


@pytest.mark.parametrize(
    "reason,finished,expected",
    [
        ("Completed", "2026-10-30T00:00:00Z", True),
        ("ContainerStatusUnknown", "2026-10-30T00:00:00Z", False),
        ("NodeLost", "2026-10-30T00:00:00Z", False),
        ("Error", None, False),
        ("Error", "0001-01-01T00:00:00Z", False),
    ],
)
async def test_only_concrete_container_termination_is_proof(
    reason: str,
    finished: str | None,
    expected: bool,
) -> None:
    """Synthetic lost-container status must not clear an invocation fence."""
    plan = manifest()
    pod = observed(plan)
    pod["status"]["containerStatuses"][0]["state"] = {
        "terminated": {"exitCode": 0, "reason": reason, "finishedAt": finished},
    }
    transport, _ = transport_for(pod)
    assert (await transport.create(plan)).terminated is expected


@pytest.mark.parametrize("failure", [None, "missing-status", "overflow", "channel"])
async def test_private_exec_channels_are_bounded_and_closed(
    monkeypatch: pytest.MonkeyPatch,
    failure: str | None,
) -> None:
    """Use the SDK's real context/iteration shape, including fragmented status messages."""
    from custos_connector import hook_kubernetes_transport as adapter

    plan = manifest()
    transport, _ = transport_for(observed(plan))
    owned = await transport.create(plan)
    reply = struct.pack(">I", 2) + b"{}"
    channels = [b"\1" + reply[:3], b"\1" + reply[3:]]
    if failure == "overflow":
        channels = [b"\1" + struct.pack(">I", MAX_HOOK_FRAME_BYTES + 1)]
    elif failure == "channel":
        channels = [b"\7private"]
    if failure != "missing-status":
        channels.extend([b'\3{"status":', b'\3"Success"}'])
    writes: list[bytes] = []
    closed: list[str] = []

    async def messages() -> AsyncIterator[SimpleNamespace]:
        """Yield binary channel fragments rather than a nonexistent read_stdout API."""
        for data in channels:
            yield SimpleNamespace(type=aiohttp.WSMsgType.BINARY, data=data)

    class Socket:
        """Minimal websocket protocol with observable private input."""

        async def send_bytes(self, data: bytes) -> None:
            """Record bytes without forwarding them to a diagnostic channel."""
            writes.append(data)

        def __aiter__(self) -> AsyncIterator[SimpleNamespace]:
            """Expose fragmented channel messages."""
            return messages()

    @asynccontextmanager
    async def socket_context() -> AsyncIterator[Socket]:
        """Record stream closure on both success and failure."""
        try:
            yield Socket()
        finally:
            closed.append("stream")

    @asynccontextmanager
    async def api_context(**kwargs: object) -> AsyncIterator[object]:
        """Record the per-exec client lifetime."""
        try:
            yield object()
        finally:
            closed.append("api")

    endpoint = SimpleNamespace(
        connect_get_namespaced_pod_exec=AsyncMock(return_value=socket_context()),
    )
    monkeypatch.setattr(adapter, "WsApiClient", api_context)
    monkeypatch.setattr(client, "CoreV1Api", lambda **kwargs: endpoint)
    if failure is None:
        assert await transport.execute(owned, b"private-input") == reply
    else:
        with pytest.raises(PluginInvocationFailed):
            await transport.execute(owned, b"private-input")
    assert writes == [b"\0private-input"] and closed == ["stream", "api"]
    assert endpoint.connect_get_namespaced_pod_exec.await_args.kwargs["command"][-1] == "a" * 32

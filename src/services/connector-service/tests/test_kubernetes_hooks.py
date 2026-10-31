"""Invocation ownership and uncertain termination over a controlled Kubernetes seam."""

from __future__ import annotations

import asyncio
import json
import struct
from collections.abc import Mapping
from dataclasses import replace
from typing import Any, NoReturn
from unittest.mock import AsyncMock

import pytest

from custos_connector.hook_sandbox import HookImage, HookPod, HookSandboxSettings
from custos_connector.kubernetes_hooks import KubernetesHookRunner
from custos_connector.runtime import PluginCleanupFailed

IMAGE = "registry.example/plugin@sha256:" + "1" * 64
HELPER = "registry.example/connector@sha256:" + "2" * 64


class Resolver:
    """Return one immutable registered image without interpreting hook inputs."""

    async def resolve(self, image_ref: str) -> HookImage:
        """Preserve the expected digest and original entrypoint."""
        assert image_ref == IMAGE
        return HookImage(image_ref, ("/plugin",))


class Transport:
    """Model exact UID ownership and explicit termination, not a success-shaped absence fallback."""

    def __init__(self) -> None:
        """Initialize independently controllable creation, execution and cleanup observations."""
        self.pod: HookPod | None = None
        self.started = asyncio.Event()
        self.exec_cancelled = False
        self.stall = False
        self.disappear = False
        self.create_unknown = False
        self.foreign_uid = False
        self.request: bytes | None = None
        self.stopped: list[str] = []
        self.released: list[str] = []

    async def create(self, manifest: Mapping[str, Any]) -> HookPod:
        """Assign a concrete UID, optionally losing the acceptance response."""
        metadata = manifest["metadata"]
        self.pod = HookPod(
            metadata["name"],
            "owned-uid",
            metadata["annotations"]["custos.dev/creation-nonce"],
            True,
            False,
            True,
        )
        if self.create_unknown:
            raise OSError("private transport diagnostic")
        return self.pod

    async def observe(self, name: str) -> HookPod | None:
        """Return the current generation or deliberate evidence loss."""
        if self.pod is not None:
            assert self.pod.name == name
            if self.foreign_uid:
                self.pod = replace(self.pod, uid="replacement")
        return self.pod

    async def execute(self, pod: HookPod, request: bytes) -> bytes:
        """Record private input and await cancellation or a positively terminated invocation."""
        assert self.pod == pod
        self.request = request
        self.started.set()
        if self.stall:
            try:
                await asyncio.Event().wait()
            finally:
                self.exec_cancelled = True
        self.pod = None if self.disappear else replace(pod, running=False, terminated=True)
        body = json.dumps({"version": 1, "status": "completed", "exitCode": 0}).encode()
        return struct.pack(">I", len(body)) + body

    async def stop(self, pod: HookPod) -> None:
        """Reject a stale UID before requesting deletion."""
        if self.pod is None or self.pod.uid != pod.uid:
            raise PluginCleanupFailed(pod.name)
        self.stopped.append(pod.uid)
        self.pod = None if self.disappear else replace(self.pod, running=False, terminated=True)

    async def release(self, pod: HookPod) -> None:
        """Remove only the verified original finalizer and resource."""
        assert self.pod is not None and self.pod.uid == pod.uid and self.pod.terminated
        self.released.append(pod.uid)
        self.pod = None


def runner(transport: Transport) -> KubernetesHookRunner:
    """Use a short cleanup budget without changing the execution/ownership semantics."""
    return KubernetesHookRunner(
        transport,
        Resolver(),
        HookSandboxSettings("custos-hooks", HELPER, "hook", cleanup_seconds=1),
    )


async def test_completion_requires_owned_terminal_state_and_cleanup() -> None:
    """An exec reply is returned only after observed termination and finalizer release."""
    transport = Transport()
    result = await runner(transport).run(
        image_ref=IMAGE,
        hook="health",
        payload={"credential": "private"},
        timeout_seconds=30,
        invocation_id="invocation",
    )
    assert result.exit_code == 0 and result.stdout == b""
    assert transport.stopped == ["owned-uid"] and transport.released == ["owned-uid"]
    assert transport.pod is None and transport.request is not None


async def test_exec_completion_stops_the_still_idle_pid_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A drained exec reply must trigger termination, not wait for PID 1 to exit independently."""
    transport = Transport()

    async def execute(pod: HookPod, request: bytes) -> bytes:
        """Complete the exec stream while retaining the real bridge's inert main process."""
        assert transport.pod == pod
        body = json.dumps({"version": 1, "status": "completed", "exitCode": 0}).encode()
        return struct.pack(">I", len(body)) + body

    monkeypatch.setattr(transport, "execute", execute)
    result = await runner(transport).run(
        image_ref=IMAGE,
        hook="health",
        payload={},
        timeout_seconds=30,
    )
    assert result.exit_code == 0
    assert transport.pod is None and transport.stopped == ["owned-uid"]


async def test_exec_reply_followed_by_absence_is_not_termination_proof() -> None:
    """API disappearance cannot certify that an executor on an unreachable node has stopped."""
    transport = Transport()
    transport.disappear = True
    with pytest.raises(PluginCleanupFailed):
        await runner(transport).run(
            image_ref=IMAGE,
            hook="listen",
            payload={},
            timeout_seconds=30,
            invocation_id="fence",
        )
    assert not transport.released


async def test_creation_response_loss_never_dispatches_or_adopts() -> None:
    """An uncertain create leaves the fence intact even though only the inert bridge may exist."""
    transport = Transport()
    transport.create_unknown = True
    with pytest.raises(PluginCleanupFailed) as uncertain:
        await runner(transport).run(
            image_ref=IMAGE,
            hook="listen",
            payload={},
            timeout_seconds=30,
            invocation_id="fence",
        )
    assert not transport.started.is_set() and not transport.released
    assert "private transport diagnostic" not in str(uncertain.value)


@pytest.mark.parametrize("error_type", [ValueError, TypeError, RuntimeError, Exception])
async def test_unknown_create_failure_cannot_release_uncertainty(
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[Exception],
) -> None:
    """An unclassified transport failure cannot become an ordinary retryable invocation error."""
    transport = Transport()

    async def create(manifest: Mapping[str, Any]) -> NoReturn:
        """Model a decoder failure after the server may have accepted a resource."""
        raise error_type("synthetic create failure")

    monkeypatch.setattr(transport, "create", create)
    with pytest.raises(PluginCleanupFailed):
        await runner(transport).run(
            image_ref=IMAGE,
            hook="listen",
            payload={},
            timeout_seconds=30,
        )
    assert not transport.started.is_set()


@pytest.mark.parametrize("response", [None, {}])
async def test_malformed_create_result_is_not_a_confirmed_rejection(
    monkeypatch: pytest.MonkeyPatch,
    response: object,
) -> None:
    """A malformed response cannot prove that the API did not accept the sandbox."""
    transport = Transport()
    monkeypatch.setattr(transport, "create", AsyncMock(return_value=response))
    with pytest.raises(PluginCleanupFailed):
        await runner(transport).run(
            image_ref=IMAGE,
            hook="listen",
            payload={},
            timeout_seconds=30,
        )
    assert not transport.started.is_set()


@pytest.mark.parametrize("operation", ["stop", "observe", "release"])
@pytest.mark.parametrize("error_type", [ValueError, TypeError, RuntimeError, Exception])
async def test_all_observed_cleanup_failures_preserve_the_fence(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    error_type: type[Exception],
) -> None:
    """No ordinary cleanup exception may escape as evidence permitting another poll."""
    transport = Transport()
    observe = transport.observe

    async def fail(value: HookPod | str) -> HookPod | None:
        """Fail only during cleanup, not during the initial inert-container observation."""
        if operation == "observe" and not transport.started.is_set():
            assert isinstance(value, str)
            return await observe(value)
        raise error_type("synthetic cleanup failure")

    monkeypatch.setattr(transport, operation, fail)
    with pytest.raises(PluginCleanupFailed):
        await runner(transport).run(
            image_ref=IMAGE,
            hook="listen",
            payload={},
            timeout_seconds=30,
        )
    assert not transport.released


async def test_uid_replacement_is_never_mutated() -> None:
    """Name equality does not authorize deleting or invoking another generation."""
    transport = Transport()
    transport.foreign_uid = True
    with pytest.raises(PluginCleanupFailed):
        await runner(transport).run(
            image_ref=IMAGE,
            hook="listen",
            payload={},
            timeout_seconds=30,
            invocation_id="fence",
        )
    assert not transport.started.is_set() and not transport.stopped and not transport.released
    assert transport.pod is not None and transport.pod.uid == "replacement"


async def test_timeout_preserves_failure_until_cleanup_is_confirmed() -> None:
    """The operation deadline cancels exec, then waits for owned termination before surfacing."""
    transport = Transport()
    transport.stall = True
    with pytest.raises(TimeoutError):
        await runner(transport).run(
            image_ref=IMAGE,
            hook="listen",
            payload={},
            timeout_seconds=0.01,
            invocation_id="fence",
        )
    assert transport.exec_cancelled and transport.pod is None
    assert transport.released == ["owned-uid"]


@pytest.mark.parametrize("lose_proof", [False, True])
async def test_cancellation_waits_for_cleanup_or_reports_uncertainty(lose_proof: bool) -> None:
    """Cancellation never leaves a hidden live task or clears uncertain execution authority."""
    transport = Transport()
    transport.stall = True
    transport.disappear = lose_proof
    task = asyncio.create_task(
        runner(transport).run(
            image_ref=IMAGE,
            hook="listen",
            payload={},
            timeout_seconds=30,
            invocation_id="fence",
        )
    )
    await transport.started.wait()
    task.cancel()
    if lose_proof:
        with pytest.raises(PluginCleanupFailed) as failure:
            await task
        assert failure.value.cancellation_requested
        assert not transport.released
    else:
        with pytest.raises(asyncio.CancelledError):
            await task
        assert transport.released == ["owned-uid"]
    assert transport.exec_cancelled and task.done()

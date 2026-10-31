"""Kubernetes hook sandbox contracts, confidential framing and ownership-safe manifests.

The controller transports sensitive bytes only through an exec stream to the
static bridge. Pod specifications contain image identities and resource locators,
not plugin inputs, credentials or results.
"""

from __future__ import annotations

import base64
import binascii
import json
import math
import re
import struct
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from custos_connector.runtime import HookRunResult, PluginInvocationFailed, PluginProtocolError

MAX_HOOK_PAYLOAD_BYTES = 4 * 1024 * 1024
MAX_HOOK_OUTPUT_BYTES = 4 * 1024 * 1024
MAX_HOOK_FRAME_BYTES = 6 * 1024 * 1024
HOOK_FINALIZER = "custos.dev/hook-termination"
HOOK_CONTAINER = "hook"
BRIDGE_PATH = "/custos/bridge/runner"
IO_PATH = "/custos/io"
_DNS_NAME = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
_DIGEST_IMAGE = re.compile(r"[^@\s]+@sha256:[a-f0-9]{64}")


@dataclass(frozen=True)
class HookImage:
    """Verified immutable image and the original effective entrypoint."""

    reference: str
    entrypoint: tuple[str, ...]
    architecture: str = "amd64"

    def __post_init__(self) -> None:
        """Reject mutable images and commands that cannot cross the bounded bridge."""
        if _DIGEST_IMAGE.fullmatch(self.reference) is None:
            raise ValueError("hook image must be digest-pinned")
        if self.architecture not in {"amd64", "arm64"}:
            raise ValueError("unsupported hook image architecture")
        if not isinstance(self.entrypoint, tuple) or len(self.entrypoint) > 63:
            raise ValueError("hook entrypoint must be an immutable bounded sequence")
        if any(
            not isinstance(arg, str) or "\0" in arg or len(arg.encode()) > 4096
            for arg in self.entrypoint
        ):
            raise ValueError("invalid hook entrypoint argument")
        if self.entrypoint and not self.entrypoint[0]:
            raise ValueError("hook entrypoint executable is empty")


@dataclass(frozen=True)
class HookSandboxSettings:
    """Explicit deployment-owned namespace, helper identity and pod admission bounds."""

    namespace: str
    helper_image: str
    service_account: str
    runtime_class: str | None = None
    image_pull_secrets: tuple[str, ...] = ()
    cleanup_seconds: float = 30

    def __post_init__(self) -> None:
        """Fail before resource creation when identity or cleanup settings are incomplete."""
        if not isinstance(self.image_pull_secrets, tuple):
            raise ValueError("hook pull-secret references must be immutable")
        for value in (self.namespace, self.service_account, *self.image_pull_secrets):
            if _DNS_NAME.fullmatch(value) is None:
                raise ValueError("invalid hook sandbox namespace or identity")
        if _DIGEST_IMAGE.fullmatch(self.helper_image) is None:
            raise ValueError("hook helper image must be digest-pinned")
        if self.runtime_class is not None and _DNS_NAME.fullmatch(self.runtime_class) is None:
            raise ValueError("invalid hook runtime class")
        if not math.isfinite(self.cleanup_seconds) or not 1 <= self.cleanup_seconds <= 60:
            raise ValueError("hook cleanup budget must be 1..60 seconds")


@dataclass(frozen=True)
class HookPod:
    """Observed immutable ownership and execution state, never inferred from API absence."""

    name: str
    uid: str
    creation_nonce: str
    running: bool
    terminated: bool
    spec_verified: bool


class HookImageResolver(Protocol):
    """Resolve the registered immutable OCI artifact without leaking registry credentials."""

    async def resolve(self, image_ref: str) -> HookImage:
        """Return verified image launch metadata or raise a typed invocation failure."""
        ...


class HookSandboxTransport(Protocol):
    """Namespace-bound operations with UID/admission checks before exec or mutation."""

    async def create(self, manifest: Mapping[str, Any]) -> HookPod:
        """Create once; ambiguous acceptance/collision must raise PluginCleanupFailed."""
        ...

    async def observe(self, name: str) -> HookPod | None:
        """Read current state; absence alone is not proof of terminated execution."""
        ...

    async def execute(self, pod: HookPod, request: bytes) -> bytes:
        """Send private framed input and collect bounded output without using application logs."""
        ...

    async def stop(self, pod: HookPod) -> None:
        """Request graceful, UID-preconditioned deletion while retaining the proof finalizer."""
        ...

    async def release(self, pod: HookPod) -> None:
        """Remove only the owned finalizer after safe termination has been established."""
        ...


def build_hook_pod(
    settings: HookSandboxSettings, image: HookImage, name: str, nonce: str, timeout_seconds: float
) -> dict[str, Any]:
    """Build a non-restarting inert sandbox; plugin execution requires a later private exec."""
    if _DNS_NAME.fullmatch(name) is None or re.fullmatch(r"[a-f0-9]{32}", nonce) is None:
        raise ValueError("invalid hook sandbox ownership")
    if not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 300:
        raise ValueError("hook execution budget must be positive and at most 300 seconds")
    security = {
        "runAsNonRoot": True,
        "runAsUser": 1000,
        "runAsGroup": 1000,
        "allowPrivilegeEscalation": False,
        "privileged": False,
        "readOnlyRootFilesystem": True,
        "capabilities": {"drop": ["ALL"]},
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    spec: dict[str, Any] = {
        "restartPolicy": "Never",
        "automountServiceAccountToken": False,
        "serviceAccountName": settings.service_account,
        "hostNetwork": False,
        "hostPID": False,
        "hostIPC": False,
        "shareProcessNamespace": False,
        "activeDeadlineSeconds": math.ceil(timeout_seconds + settings.cleanup_seconds),
        "terminationGracePeriodSeconds": 10,
        "securityContext": {"runAsNonRoot": True, "fsGroup": 1000},
        "nodeSelector": {"kubernetes.io/arch": image.architecture, "kubernetes.io/os": "linux"},
        "volumes": [
            {"name": "bridge", "emptyDir": {"medium": "Memory", "sizeLimit": "16Mi"}},
            {"name": "io", "emptyDir": {"medium": "Memory", "sizeLimit": "16Mi"}},
        ],
        "initContainers": [
            {
                "name": "bridge-init",
                "image": settings.helper_image,
                "command": ["/opt/custos/bin/connector-hook-bridge"],
                "args": ["install", BRIDGE_PATH, nonce],
                "securityContext": security,
                "volumeMounts": [{"name": "bridge", "mountPath": "/custos/bridge"}],
                "resources": {
                    "requests": {"cpu": "50m", "memory": "32Mi"},
                    "limits": {"cpu": "250m", "memory": "64Mi"},
                },
            }
        ],
        "containers": [
            {
                "name": HOOK_CONTAINER,
                "image": image.reference,
                "command": [BRIDGE_PATH],
                "args": ["wait", IO_PATH, nonce],
                "readinessProbe": {
                    "exec": {"command": [BRIDGE_PATH, "ready", IO_PATH, nonce]},
                    "periodSeconds": 1,
                    "timeoutSeconds": 1,
                },
                "securityContext": security,
                "volumeMounts": [
                    {"name": "bridge", "mountPath": "/custos/bridge", "readOnly": True},
                    {"name": "io", "mountPath": IO_PATH},
                ],
                "resources": {
                    "requests": {"cpu": "100m", "memory": "64Mi"},
                    "limits": {"cpu": "1", "memory": "256Mi"},
                },
            }
        ],
    }
    if settings.runtime_class is not None:
        spec["runtimeClassName"] = settings.runtime_class
    if settings.image_pull_secrets:
        spec["imagePullSecrets"] = [{"name": value} for value in settings.image_pull_secrets]
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": settings.namespace,
            "labels": {"app.kubernetes.io/managed-by": "custos-connector", "custos.dev/hook": name},
            "annotations": {"custos.dev/creation-nonce": nonce, "dapr.io/enabled": "false"},
            "finalizers": [HOOK_FINALIZER],
        },
        "spec": spec,
    }


def encode_hook_request(
    name: str, image: HookImage, hook: str, payload: Mapping[str, Any], timeout_seconds: float
) -> bytes:
    """Frame private input without placing payloads in Kubernetes API command arguments."""
    if _DNS_NAME.fullmatch(name) is None or hook not in {
        "bind",
        "listen",
        "health",
        "verifyWebhook",
    }:
        raise ValueError("invalid hook invocation")
    if not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 300:
        raise ValueError("invalid hook deadline")
    body = json.dumps(
        dict(payload), ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode()
    if len(body) > MAX_HOOK_PAYLOAD_BYTES:
        raise ValueError("hook payload exceeds its byte limit")
    envelope = json.dumps(
        {
            "version": 1,
            "scope": name,
            "argv": [*image.entrypoint, hook],
            "payload": base64.b64encode(body).decode("ascii"),
            "timeoutMillis": max(1, math.ceil(timeout_seconds * 1000)),
        },
        separators=(",", ":"),
    ).encode()
    if len(envelope) > MAX_HOOK_FRAME_BYTES:
        raise ValueError("hook frame exceeds its byte limit")
    return struct.pack(">I", len(envelope)) + envelope


def _unique_reply(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject ambiguous control fields rather than choosing a repeated value."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate hook response field")
        result[key] = value
    return result


def decode_hook_reply(frame: bytes) -> HookRunResult:
    """Validate the bounded private response without exposing arbitrary bridge diagnostics."""
    if len(frame) < 4:
        raise PluginProtocolError("incomplete hook response")
    length = struct.unpack(">I", frame[:4])[0]
    if length != len(frame) - 4 or length > MAX_HOOK_FRAME_BYTES:
        raise PluginProtocolError("invalid hook response length")
    try:
        result = json.loads(frame[4:], object_pairs_hook=_unique_reply)
        if not isinstance(result, dict) or set(result) - {
            "version",
            "status",
            "exitCode",
            "stdout",
            "stderr",
        }:
            raise ValueError("invalid result shape")
        if type(result.get("version")) is not int or result["version"] != 1:
            raise ValueError("invalid result version")
        if result.get("status") == "timeout":
            raise TimeoutError("hook execution deadline exceeded")
        if result.get("status") != "completed":
            raise PluginInvocationFailed("hook bridge did not complete the invocation")
        code = result.get("exitCode")
        if type(code) is not int or not -1 <= code <= 255:
            raise ValueError("invalid exit code")
        stdout = base64.b64decode(result.get("stdout", ""), validate=True)
        stderr = base64.b64decode(result.get("stderr", ""), validate=True)
        if len(stdout) + len(stderr) > MAX_HOOK_OUTPUT_BYTES:
            raise ValueError("hook output exceeds limit")
        return HookRunResult(code, stdout, stderr)
    except (ValueError, TypeError, binascii.Error) as exc:
        raise PluginProtocolError("invalid hook response") from exc

"""Async Kubernetes API adapter for private hook execution and UID-bound finalizers."""

from __future__ import annotations

import json
import struct
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import aiohttp
from kubernetes_asyncio import client
from kubernetes_asyncio.client.exceptions import ApiException
from kubernetes_asyncio.stream import WsApiClient

from custos_connector.hook_sandbox import (
    BRIDGE_PATH,
    HOOK_CONTAINER,
    HOOK_FINALIZER,
    IO_PATH,
    MAX_HOOK_FRAME_BYTES,
    HookPod,
)
from custos_connector.runtime import PluginCleanupFailed, PluginInvocationFailed


def _admission_matches(actual: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    """Reject changed execution, privileges, mounts or injected containers before admitting exec."""
    spec, wanted = actual.get("spec"), expected.get("spec")
    if not isinstance(spec, dict) or not isinstance(wanted, dict):
        return False
    for key in (
        "restartPolicy",
        "automountServiceAccountToken",
        "serviceAccountName",
        "hostNetwork",
        "hostPID",
        "hostIPC",
        "shareProcessNamespace",
        "activeDeadlineSeconds",
        "terminationGracePeriodSeconds",
        "runtimeClassName",
        "nodeSelector",
    ):
        if spec.get(
            key, False if key.startswith("host") or key == "shareProcessNamespace" else None
        ) != wanted.get(
            key, False if key.startswith("host") or key == "shareProcessNamespace" else None
        ):
            return False
    if spec.get("ephemeralContainers") or spec.get("volumes") != wanted["volumes"]:
        return False
    if any(spec.get(key) for key in ("hostAliases", "dnsConfig", "hostname", "subdomain")):
        return False
    if spec.get("dnsPolicy", "ClusterFirst") != "ClusterFirst":
        return False
    if spec.get("securityContext") != wanted["securityContext"]:
        return False
    for group in ("containers", "initContainers"):
        observed, desired = spec.get(group), wanted[group]
        if not isinstance(observed, list) or len(observed) != len(desired):
            return False
        for current, target in zip(observed, desired, strict=True):
            if not isinstance(current, dict):
                return False
            defaults: dict[str, Any] = {
                "env": [],
                "envFrom": [],
                "ports": [],
                "volumeDevices": [],
                "workingDir": "",
                "stdin": False,
                "stdinOnce": False,
                "tty": False,
                "terminationMessagePath": "/dev/termination-log",
                "terminationMessagePolicy": "File",
            }
            if set(current) - (set(target) | set(defaults) | {"imagePullPolicy"}):
                return False
            if any(current.get(key, value) != value for key, value in defaults.items()):
                return False
            if current.get("imagePullPolicy", "IfNotPresent") not in {
                "Always",
                "IfNotPresent",
                "Never",
            }:
                return False
            for key in ("name", "image", "command", "args", "securityContext", "resources"):
                if current.get(key) != target[key]:
                    return False
            if target["name"] == HOOK_CONTAINER:
                probe = current.get("readinessProbe")
                if (
                    not isinstance(probe, dict)
                    or probe.get("exec") != target["readinessProbe"]["exec"]
                ):
                    return False
            mounts = current.get("volumeMounts")
            if not isinstance(mounts, list) or len(mounts) != len(target["volumeMounts"]):
                return False
            for mount, expected_mount in zip(mounts, target["volumeMounts"], strict=True):
                if not isinstance(mount, dict):
                    return False
                if (
                    mount.get("name") != expected_mount["name"]
                    or mount.get("mountPath") != expected_mount["mountPath"]
                    or bool(mount.get("readOnly")) != bool(expected_mount.get("readOnly"))
                    or mount.get("subPath")
                    or mount.get("subPathExpr")
                ):
                    return False
    return True


class KubernetesHookTransport:
    """Caller-owned API clients; no ambient namespace, kubeconfig fallback or stream logging."""

    def __init__(
        self, api: client.CoreV1Api, configuration: client.Configuration, namespace: str
    ) -> None:
        """Bind the already authenticated controller and one explicit sandbox namespace."""
        self.api, self.configuration, self.namespace = api, configuration, namespace
        self._expected: dict[str, Mapping[str, Any]] = {}

    def _pod(self, raw: Any, expected: Mapping[str, Any] | None) -> HookPod:
        """Normalize concrete ownership and terminal evidence without trusting Pod phase alone."""
        value = self.api.api_client.sanitize_for_serialization(raw)
        if not isinstance(value, dict) or not isinstance(value.get("metadata"), dict):
            raise PluginInvocationFailed("invalid hook sandbox metadata")
        metadata = value["metadata"]
        name, uid = metadata.get("name"), metadata.get("uid")
        annotations = metadata.get("annotations")
        if not isinstance(annotations, dict):
            raise PluginInvocationFailed("invalid hook sandbox annotations")
        nonce = annotations.get("custos.dev/creation-nonce")
        if (
            not isinstance(name, str)
            or not isinstance(uid, str)
            or not uid
            or not isinstance(nonce, str)
            or metadata.get("namespace") != self.namespace
        ):
            raise PluginInvocationFailed("invalid hook sandbox identity")
        valid = expected is not None and _admission_matches(value, expected)
        if HOOK_FINALIZER not in metadata.get("finalizers", []):
            valid = False
        status_body = value.get("status")
        if status_body is None:
            status_body = {}
        if not isinstance(status_body, dict):
            raise PluginInvocationFailed("invalid hook sandbox status")
        statuses = status_body.get("containerStatuses")
        if statuses is None:
            statuses = []
        if not isinstance(statuses, list) or not all(isinstance(row, dict) for row in statuses):
            raise PluginInvocationFailed("invalid hook container status")
        status: dict[str, Any] = next(
            (row for row in statuses if row.get("name") == HOOK_CONTAINER), {}
        )
        state = status.get("state")
        if state is None:
            state = {}
        if not isinstance(state, dict):
            raise PluginInvocationFailed("invalid hook container state")
        if status.get("restartCount", 0) != 0:
            valid = False
        container_id = status.get("containerID")
        terminal = state.get("terminated")
        terminated = False
        if isinstance(terminal, dict) and terminal.get("reason") not in {
            "ContainerStatusUnknown",
            "NodeLost",
        }:
            finished = terminal.get("finishedAt")
            if isinstance(finished, str) and type(terminal.get("exitCode")) is int:
                try:
                    timestamp = datetime.fromisoformat(finished.replace("Z", "+00:00"))
                except ValueError as exc:
                    raise PluginInvocationFailed("invalid hook termination timestamp") from exc
                terminated = timestamp.tzinfo is not None and timestamp > datetime(
                    1970, 1, 1, tzinfo=UTC
                )
        has_container = isinstance(container_id, str) and bool(container_id)
        return HookPod(
            name,
            uid,
            nonce,
            bool(state.get("running")) and status.get("ready") is True and has_container,
            terminated and has_container,
            valid,
        )

    async def create(self, manifest: Mapping[str, Any]) -> HookPod:
        """Create once; ambiguous API acceptance and collisions never authorize adoption."""
        name = manifest["metadata"]["name"]
        if name in self._expected:
            raise PluginCleanupFailed(name)
        self._expected[name] = manifest
        accepted = False
        try:
            raw = await self.api.create_namespaced_pod(
                self.namespace,
                dict(manifest),
                _request_timeout=10,
            )
            try:
                pod = self._pod(raw, manifest)
                accepted = True
                return pod
            except PluginInvocationFailed as exc:
                raise PluginCleanupFailed(name) from exc
        except ApiException as exc:
            if exc.status in {400, 401, 403, 404, 422}:
                raise PluginInvocationFailed("Kubernetes rejected hook sandbox creation") from exc
            raise PluginCleanupFailed(name) from exc
        except (aiohttp.ClientError, OSError, TimeoutError) as exc:
            raise PluginCleanupFailed(name) from exc
        finally:
            if not accepted:
                self._expected.pop(name, None)

    async def observe(self, name: str) -> HookPod | None:
        """Only 404 means absence; every transport or backend failure remains explicit."""
        try:
            raw = await self.api.read_namespaced_pod(name, self.namespace, _request_timeout=10)
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise PluginInvocationFailed("hook sandbox observation unavailable") from exc
        except (aiohttp.ClientError, OSError, TimeoutError) as exc:
            raise PluginInvocationFailed("hook sandbox observation unavailable") from exc
        return self._pod(raw, self._expected.get(name))

    async def execute(self, pod: HookPod, request: bytes) -> bytes:
        """Stream framed stdin/output over exec channels, never Pod application logs."""
        observed = await self.observe(pod.name)
        if observed != pod or not pod.running or not pod.spec_verified:
            raise PluginCleanupFailed(pod.name)
        output = bytearray()
        status_data = bytearray()
        diagnostic_size = 0
        try:
            async with WsApiClient(configuration=self.configuration) as api:
                endpoint = client.CoreV1Api(api_client=api)
                socket = await endpoint.connect_get_namespaced_pod_exec(
                    pod.name,
                    self.namespace,
                    container=HOOK_CONTAINER,
                    command=[BRIDGE_PATH, "invoke", IO_PATH, pod.creation_nonce],
                    stdin=True,
                    stdout=True,
                    stderr=True,
                    tty=False,
                    _preload_content=False,
                )
                async with socket as stream:
                    for offset in range(0, len(request), 64 * 1024):
                        await stream.send_bytes(b"\0" + request[offset : offset + 64 * 1024])
                    async for message in stream:
                        if message.type != aiohttp.WSMsgType.BINARY or not message.data:
                            raise PluginInvocationFailed("invalid hook exec stream")
                        channel, data = message.data[0], message.data[1:]
                        if channel == 1:
                            if len(output) + len(data) > MAX_HOOK_FRAME_BYTES + 4:
                                raise PluginInvocationFailed("hook exec response exceeds limit")
                            output.extend(data)
                            if (
                                len(output) >= 4
                                and struct.unpack(">I", output[:4])[0] > MAX_HOOK_FRAME_BYTES
                            ):
                                raise PluginInvocationFailed("hook exec frame exceeds limit")
                        elif channel in (2, 3):
                            diagnostic_size += len(data)
                            if diagnostic_size > 16 * 1024:
                                raise PluginInvocationFailed("hook exec status exceeds limit")
                            if channel == 3:
                                status_data.extend(data)
                        else:
                            raise PluginInvocationFailed("unsupported hook exec channel")
            if not status_data:
                raise PluginInvocationFailed("hook exec completion status is missing")
            status = json.loads(status_data)
            if not isinstance(status, dict) or status.get("status") != "Success":
                raise PluginInvocationFailed("hook bridge process failed")
        except (ApiException, aiohttp.ClientError, OSError, ValueError) as exc:
            raise PluginInvocationFailed("hook exec transport failed") from exc
        return bytes(output)

    async def stop(self, pod: HookPod) -> None:
        """Request graceful UID-preconditioned deletion, retaining the termination finalizer."""
        try:
            await self.api.delete_namespaced_pod(
                pod.name,
                self.namespace,
                body={"preconditions": {"uid": pod.uid}, "gracePeriodSeconds": 10},
                _request_timeout=10,
            )
        except (ApiException, aiohttp.ClientError, OSError) as exc:
            raise PluginCleanupFailed(pod.name) from exc

    async def release(self, pod: HookPod) -> None:
        """Remove only this finalizer using the latest resource version and exact owned UID."""
        try:
            raw = await self.api.read_namespaced_pod(pod.name, self.namespace, _request_timeout=10)
            current = self._pod(raw, self._expected.get(pod.name))
            if (
                current.uid != pod.uid
                or current.creation_nonce != pod.creation_nonce
                or not current.spec_verified
                or (pod.terminated and not current.terminated)
            ):
                raise PluginCleanupFailed(pod.name)
            value = self.api.api_client.sanitize_for_serialization(raw)
            metadata = value["metadata"]
            if (
                metadata["uid"] != pod.uid
                or metadata["annotations"].get("custos.dev/creation-nonce") != pod.creation_nonce
            ):
                raise PluginCleanupFailed(pod.name)
            finalizers = [item for item in metadata.get("finalizers", []) if item != HOOK_FINALIZER]
            await self.api.patch_namespaced_pod(
                pod.name,
                self.namespace,
                body={
                    "metadata": {
                        "uid": pod.uid,
                        "resourceVersion": metadata["resourceVersion"],
                        "finalizers": finalizers,
                    }
                },
                _request_timeout=10,
                _content_type="application/merge-patch+json",
            )
        except (ApiException, aiohttp.ClientError, OSError, PluginInvocationFailed) as exc:
            raise PluginCleanupFailed(pod.name) from exc
        self._expected.pop(pod.name, None)

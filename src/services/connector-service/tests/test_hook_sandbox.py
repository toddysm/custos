"""Kubernetes sandbox hardening and private bridge framing without a cluster dependency."""

from __future__ import annotations

import base64
import json
import struct
from dataclasses import replace

import pytest

from custos_connector.hook_sandbox import (
    BRIDGE_PATH,
    HOOK_FINALIZER,
    MAX_HOOK_FRAME_BYTES,
    MAX_HOOK_OUTPUT_BYTES,
    MAX_HOOK_PAYLOAD_BYTES,
    HookImage,
    HookSandboxSettings,
    build_hook_pod,
    decode_hook_reply,
    encode_hook_request,
)
from custos_connector.runtime import PluginInvocationFailed, PluginProtocolError

IMAGE = "registry.example/plugin@sha256:" + "1" * 64
HELPER = "registry.example/connector@sha256:" + "2" * 64


def settings() -> HookSandboxSettings:
    """Provide explicit namespace/identity without an implicit cluster default."""
    return HookSandboxSettings("custos-hooks", HELPER, "hook-sandbox")


def frame(body: bytes) -> bytes:
    """Represent the bridge's exact bounded wire frame."""
    return struct.pack(">I", len(body)) + body


def test_manifest_is_inert_hardened_and_confidential() -> None:
    """No plugin input or entrypoint runs during Pod creation or appears in its specification."""
    image = HookImage(IMAGE, ("/plugin",))
    pod = build_hook_pod(settings(), image, "custos-hook-one", "a" * 32, 30)
    spec = pod["spec"]
    assert pod["metadata"]["finalizers"] == [HOOK_FINALIZER]
    assert pod["metadata"]["annotations"]["dapr.io/enabled"] == "false"
    assert spec["restartPolicy"] == "Never" and not spec["automountServiceAccountToken"]
    assert not any(
        spec[key] for key in ("hostNetwork", "hostPID", "hostIPC", "shareProcessNamespace")
    )
    assert spec["activeDeadlineSeconds"] == 60
    assert all(value["emptyDir"]["medium"] == "Memory" for value in spec["volumes"])
    assert all("hostPath" not in value for value in spec["volumes"])
    for container in spec["containers"] + spec["initContainers"]:
        security = container["securityContext"]
        assert security["runAsNonRoot"] and security["readOnlyRootFilesystem"]
        assert not security["privileged"] and not security["allowPrivilegeEscalation"]
        assert security["capabilities"] == {"drop": ["ALL"]}
        assert security["seccompProfile"] == {"type": "RuntimeDefault"}
        assert "env" not in container and "envFrom" not in container
        assert container["resources"]["limits"]
    assert spec["containers"][0]["command"] == [BRIDGE_PATH]
    assert spec["containers"][0]["args"] == [
        "wait",
        "/custos/io",
        "a" * 32,
    ]
    assert "/plugin" not in spec["containers"][0]["command"]
    configured = build_hook_pod(
        replace(settings(), runtime_class="sandboxed", image_pull_secrets=("private-registry",)),
        image,
        "custos-hook-one",
        "a" * 32,
        30,
    )
    assert configured["spec"]["runtimeClassName"] == "sandboxed"
    assert configured["spec"]["imagePullSecrets"] == [{"name": "private-registry"}]


def test_wire_preserves_entrypoint_and_private_payload() -> None:
    """Docker's supplied hook argument overrides image CMD without requiring a plugin shell."""
    image = HookImage(IMAGE, ("/plugin", "--safe"))
    payload = {"credential": "fixture-only-confidential"}
    request = encode_hook_request("custos-hook-one", image, "listen", payload, 0.001)
    size = struct.unpack(">I", request[:4])[0]
    assert size == len(request) - 4
    decoded = json.loads(request[4:])
    assert decoded["argv"] == ["/plugin", "--safe", "listen"]
    assert decoded["timeoutMillis"] == 1
    assert json.loads(base64.b64decode(decoded["payload"])) == payload
    assert decoded["scope"] == "custos-hook-one"
    empty = json.loads(
        encode_hook_request(
            "custos-hook-one",
            HookImage(IMAGE, ()),
            "health",
            {},
            30,
        )[4:]
    )
    assert empty["argv"] == ["health"]


@pytest.mark.parametrize("timeout", [0, -1, 301, float("inf"), float("nan")])
def test_deadline_validation_precedes_resource_creation(timeout: float) -> None:
    """Invalid or unbounded deadlines are never sent to Kubernetes."""
    with pytest.raises(ValueError):
        build_hook_pod(settings(), HookImage(IMAGE, ()), "owned", "a" * 32, timeout)
    with pytest.raises(ValueError):
        encode_hook_request("owned", HookImage(IMAGE, ()), "health", {}, timeout)


def test_identity_and_payload_limits_fail_closed() -> None:
    """Mutable image references and oversized/private-control inputs cannot become runnable."""
    for reference in ("plugin:latest", "plugin", "plugin@sha256:bad"):
        with pytest.raises(ValueError):
            HookImage(reference, ())
    with pytest.raises(ValueError):
        HookImage(IMAGE, ("bad\0command",))
    with pytest.raises(ValueError):
        replace(settings(), namespace="../other")
    with pytest.raises(ValueError):
        replace(settings(), helper_image="helper:latest")
    with pytest.raises(ValueError):
        replace(settings(), cleanup_seconds=0)
    with pytest.raises(ValueError):
        build_hook_pod(settings(), HookImage(IMAGE, ()), "owned", "bad", 30)
    with pytest.raises(ValueError):
        encode_hook_request("owned", HookImage(IMAGE, ()), "unknown", {}, 30)
    with pytest.raises(ValueError):
        encode_hook_request(
            "owned", HookImage(IMAGE, ()), "bind", {"p": "x" * MAX_HOOK_PAYLOAD_BYTES}, 30
        )


def test_completed_reply_preserves_plugin_bytes_and_exit() -> None:
    """Only a complete versioned reply can supply the original plugin result."""
    reply = decode_hook_reply(
        frame(
            json.dumps(
                {
                    "version": 1,
                    "status": "completed",
                    "exitCode": 7,
                    "stdout": base64.b64encode(b'{"ok":false}').decode(),
                    "stderr": base64.b64encode(b"private diagnostic").decode(),
                }
            ).encode()
        )
    )
    assert reply.exit_code == 7 and reply.stdout == b'{"ok":false}'
    assert reply.stderr == b"private diagnostic"
    assert (
        decode_hook_reply(frame(b'{"version":1,"status":"completed","exitCode":0}')).stdout == b""
    )


@pytest.mark.parametrize(
    "body",
    [
        b"[]",
        b"invalid",
        b'{"version":true,"status":"completed","exitCode":0}',
        b'{"version":1,"version":2,"status":"completed","exitCode":0}',
        b'{"version":1,"status":"completed","exitCode":true}',
        b'{"version":1,"status":"completed","exitCode":0,"stdout":"!"}',
        b'{"version":1,"status":"completed","exitCode":0,"unknown":"secret"}',
    ],
)
def test_malformed_reply_never_exposes_arbitrary_details(body: bytes) -> None:
    """Malformed control data produces a closed generic protocol failure."""
    with pytest.raises(PluginProtocolError, match="invalid hook response"):
        decode_hook_reply(frame(body))


def test_failure_and_wire_byte_bounds_are_explicit() -> None:
    """Transport failures and over-limit output never become successful empty replies."""
    with pytest.raises(TimeoutError, match="deadline exceeded"):
        decode_hook_reply(frame(b'{"version":1,"status":"timeout","exitCode":-1}'))
    with pytest.raises(PluginInvocationFailed, match="did not complete"):
        decode_hook_reply(frame(b'{"version":1,"status":"output-limit","exitCode":-1}'))
    for raw in (b"", struct.pack(">I", 10), struct.pack(">I", MAX_HOOK_FRAME_BYTES + 1)):
        with pytest.raises(PluginProtocolError):
            decode_hook_reply(raw)
    huge = json.dumps(
        {
            "version": 1,
            "status": "completed",
            "exitCode": 0,
            "stdout": base64.b64encode(b"x" * (MAX_HOOK_OUTPUT_BYTES + 1)).decode(),
        }
    ).encode()
    with pytest.raises(PluginProtocolError):
        decode_hook_reply(frame(huge))

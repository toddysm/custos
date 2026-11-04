"""Exercise collection through the real binary exec transport, replacing external Kubernetes I/O."""

import base64
import gzip
import io
import tarfile
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest
from kubernetes import client as kube_client

from custos_arm.runtime.oci import OciContainerDriver, SandboxFailureError
from tests.test_oci_lifecycle import _driver, _handle, _init_running, _plan, _pod_with_init


def archive() -> tuple[bytes, bytes]:
    """Build the reproduced 10,240-byte tar with data that cannot survive a text round trip."""
    content = bytes(range(256)) * 28
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as stream:
        member = tarfile.TarInfo("binary.dat")
        member.size = len(content)
        stream.addfile(member, io.BytesIO(content))
    result = buffer.getvalue()
    assert len(result) == 10240
    return result, content


class WireStream:
    """The external WebSocket seam; actual exec framing, byte limits and closure stay production."""

    def __init__(
        self,
        stdout: bytes | str,
        *,
        stderr: bytes | str = b"",
        status: int | None = 0,
    ) -> None:
        """Split actual bytes into several frames and preserve malformed-frame variants."""
        self.chunks = [stdout[offset : offset + 4096] for offset in range(0, len(stdout), 4096)]
        self.diagnostic = stderr
        self.returncode = status
        self.sock = SimpleNamespace(settimeout=Mock())
        self.open = True
        self.closed = False
        self.written: list[bytes] = []

    def is_open(self) -> bool:
        """Remain connected until all supplied output/diagnostic frames have been consumed."""
        return self.open

    def update(self, timeout: int) -> None:
        """Expose EOF on the next iteration without fabricating a terminal status."""
        if not self.chunks and not self.diagnostic:
            self.open = False

    def peek_stdout(self) -> bool:
        """Report buffered data without consuming it."""
        return bool(self.chunks)

    def read_stdout(self) -> bytes | str:
        """Return one frame, optionally invalid text to test fail-closed binary framing."""
        return self.chunks.pop(0)

    def peek_stderr(self) -> bool:
        """Report the single diagnostic frame."""
        return bool(self.diagnostic)

    def read_stderr(self) -> bytes | str:
        """Consume the exact diagnostic frame without decoding it in the external seam."""
        value, self.diagnostic = self.diagnostic, b""
        return value

    def write_stdin(self, value: bytes) -> None:
        """Retain the private input bytes to prove that delivery is not encoded or altered."""
        self.written.append(value)

    def close(self) -> None:
        """Record the production transport's mandatory cleanup."""
        self.open = False
        self.closed = True


def transport(
    monkeypatch: pytest.MonkeyPatch,
    root: Path,
    payload: bytes | str,
    *,
    limit: int,
    stderr: bytes | str = b"",
    status: int | None = 0,
) -> tuple[OciContainerDriver, list[WireStream], list[list[str]], list[kube_client.ApiClient]]:
    """Use real API clients, with only Pod discovery and the WebSocket exchange substituted."""
    streams: list[WireStream] = []
    commands: list[list[str]] = []
    closed: list[kube_client.ApiClient] = []
    original_close = kube_client.ApiClient.close

    def close(api: kube_client.ApiClient) -> None:
        """Track isolated-client closure while still running its actual cleanup."""
        closed.append(api)
        original_close(api)

    def connect(method: Callable[..., Any], pod: str, namespace: str, **kwargs: Any) -> WireStream:
        """Emulate what the selected shell command emits, not a pre-completed exec result."""
        assert kwargs["binary"] is True and kwargs["_preload_content"] is False
        command = kwargs["command"]
        commands.append(command)
        output = payload
        if command[0] == "touch":
            output = b""
        elif "base64" in " ".join(command):
            assert isinstance(payload, bytes)
            output = base64.b64encode(payload)
        stream = WireStream(output, stderr=stderr, status=status)
        streams.append(stream)
        return stream

    monkeypatch.setattr(kube_client.ApiClient, "close", close)
    monkeypatch.setattr("kubernetes.stream.stream", connect)
    core = Mock()
    core.api_client.configuration = kube_client.Configuration()
    core.list_namespaced_pod.return_value = _pod_with_init([_init_running()])
    return _driver(core=core, staging_root=root, max_output_bytes=limit), streams, commands, closed


@pytest.mark.parametrize("limit", [12288, 10241, 10240, 10239])
def test_archive_limit_is_not_reduced_by_wire_encoding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    limit: int,
) -> None:
    """The exact archive ceiling accepts near/at-limit binary tar and rejects one byte over."""
    raw, content = archive()
    driver, streams, commands, closed = transport(monkeypatch, tmp_path, raw, limit=limit)
    handle = _handle(_plan(), tmp_path)
    if limit < len(raw):
        with pytest.raises(SandboxFailureError, match="byte budget"):
            driver.collect(handle)
        assert len(commands) == 1
    else:
        bundle = driver.collect(handle)
        assert (bundle.root / "binary.dat").read_bytes() == content
        assert commands[0] == ["tar", "-c", "-C", "/custos/out", "."]
        assert len(commands) == 2 and commands[1][0] == "touch"
    assert all(stream.closed for stream in streams)
    assert len(closed) == len(streams)


@pytest.mark.parametrize(
    "failure",
    [
        "empty",
        "corrupt",
        "compressed",
        "truncated-file",
        "truncated-trailer",
        "missing-status",
        "failed-status",
        "text-output",
        "text-diagnostics",
        "diagnostic-limit",
        "deadline",
    ],
)
def test_collection_rejects_incomplete_or_unbounded_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    """Invalid or unfinished streams must close resources without signalling collected output."""
    raw, _ = archive()
    payload: bytes | str = raw
    stderr: bytes | str = b""
    status: int | None = 0
    if failure == "empty":
        payload = b""
    elif failure == "corrupt":
        payload = b"x" * 2048
    elif failure == "compressed":
        compressed = gzip.compress(raw)
        payload = compressed + b"\0" * ((-len(compressed) % 512) + 1024)
    elif failure == "truncated-file":
        payload = raw[:1000]
    elif failure == "truncated-trailer":
        payload = raw[:7680]
    elif failure == "missing-status":
        status = None
    elif failure == "failed-status":
        status = 2
    elif failure == "text-output":
        payload = "not binary"
    elif failure == "text-diagnostics":
        stderr = "not binary"
    elif failure == "diagnostic-limit":
        stderr = b"x" * (64 * 1024 + 1)
    driver, streams, commands, closed = transport(
        monkeypatch,
        tmp_path,
        payload,
        limit=16384,
        stderr=stderr,
        status=status,
    )
    if failure == "deadline":
        ticks = iter((0.0, 31.0))
        monkeypatch.setattr("custos_arm.runtime.oci.lifecycle.time.monotonic", lambda: next(ticks))
    with pytest.raises(SandboxFailureError):
        driver.collect(_handle(_plan(), tmp_path))
    assert len(commands) == 1
    assert len(closed) == 1 and streams[0].closed


def test_private_stdin_and_diagnostic_boundary_stay_binary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raw collection does not alter private input delivery or the separate 64KiB stderr budget."""
    raw, _ = archive()
    driver, streams, _, closed = transport(
        monkeypatch,
        tmp_path,
        b"",
        limit=len(raw),
        stderr=b"x" * (64 * 1024),
    )
    result = driver._default_pod_exec(
        namespace="ns",
        pod="pod",
        container="private-injector",
        command=["tar", "-x", "-C", "/custos/sidecar"],
        stdin=raw,
    )
    assert streams[0].written == [raw]
    assert result.exit_code == 0 and result.stdout == b"" and len(result.stderr) == 64 * 1024
    assert streams[0].closed and len(closed) == 1


def test_stream_open_failure_closes_isolated_api_client(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failure before a WebSocket exists must still release its isolated API client."""
    raw, _ = archive()
    driver, streams, _, closed = transport(monkeypatch, tmp_path, raw, limit=len(raw))

    def unavailable(*args: object, **kwargs: object) -> None:
        """Simulate external exec connection failure before stream ownership transfers."""
        raise OSError("external transport unavailable")

    monkeypatch.setattr("kubernetes.stream.stream", unavailable)
    with pytest.raises(OSError):
        driver.collect(_handle(_plan(), tmp_path))
    assert not streams and len(closed) == 1

"""Exercise the actual dependency-free activity client over a Unix socket."""

from __future__ import annotations

import json
import tempfile
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from socketserver import UnixStreamServer
from typing import Any

import pytest
from copy_image.contract import ActivityError, Sandbox
from copy_image.credentials import read_slot_credentials, release_slot_credentials


@pytest.fixture
def sandbox() -> Iterator[Sandbox]:
    """Use a short owned path within macOS's Unix socket pathname limit."""
    with tempfile.TemporaryDirectory(prefix="copy-broker-", dir="/tmp") as directory:
        base = Path(directory)
        (base / "in").mkdir()
        (base / "run").mkdir()
        (base / "in/sidecar-token").write_text("bootstrap")
        (base / "in/ctx.json").write_text(
            json.dumps(
                {
                    "workspaceId": "ws",
                    "runId": "run",
                    "stepId": "step",
                    "attempt": 1,
                }
            )
        )
        yield Sandbox(base)


def envelope() -> dict[str, Any]:
    """Return scoped opaque registry material with explicitly unknown provider expiry."""
    return {
        "schemaVersion": "2",
        "lease": {
            "leaseId": "lease-source",
            "workspaceId": "ws",
            "runId": "run",
            "stepId": "step",
            "attempt": 1,
            "slot": "source",
            "capability": "oci.pull",
            "connectorInstanceId": "instance",
            "expiresAt": "2099-01-01T00:00:00Z",
        },
        "credential": {
            "authenticationType": "x-dapr-secret",
            "material": {"username": "alice", "token": "private-pat"},
            "issuedAt": "2026-01-01T00:00:00Z",
            "expiresAt": None,
        },
    }


@contextmanager
def broker(
    sandbox: Sandbox,
    payload: dict[str, Any],
    status: int = 200,
) -> Iterator[list[tuple[str, str]]]:
    """Serve real HTTP framing; capture requests without logging credential content."""
    calls: list[tuple[str, str]] = []

    class Handler(BaseHTTPRequestHandler):
        """Return the chosen envelope through an authenticated local test socket."""

        def do_GET(self) -> None:
            """Record issuance requests and write the controlled response."""
            assert self.headers["Custos-Sidecar-Token"] == "bootstrap"
            assert "Authorization" not in self.headers
            calls.append(("GET", self.path))
            self.reply(payload)

        def do_POST(self) -> None:
            """Record the exact release body independently of the issuance envelope."""
            assert self.headers["Custos-Sidecar-Token"] == "bootstrap"
            assert "Authorization" not in self.headers
            calls.append(("POST", self.rfile.read(int(self.headers["Content-Length"])).decode()))
            self.reply({"released": True})

        def reply(self, body: dict[str, Any]) -> None:
            """Emit bounded JSON with standard HTTP headers."""
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, format: str, *args: Any) -> None:
            """Avoid credential-bearing request logging in this test fixture."""

    with UnixStreamServer(str(sandbox.base / "run/connector.sock"), Handler) as server:
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            yield calls
        finally:
            server.shutdown()
            thread.join()


def test_real_broker_material_without_legacy_secret_files(sandbox: Sandbox) -> None:
    """Production injection has no username/token files; consume the actual broker envelope."""
    with broker(sandbox, envelope()) as calls:
        credentials = read_slot_credentials(sandbox, "source")
        release_slot_credentials(sandbox, "source", credentials)
    assert credentials.username == "alice"
    assert credentials.secret == "private-pat"
    assert credentials.provider_expires_at is None
    assert calls == [
        ("GET", "/v1/token?slot=source&purpose=oci.pull"),
        ("POST", '{"leaseId": "lease-source"}'),
    ]


@pytest.mark.parametrize("status", [401, 403, 429, 503])
def test_broker_errors_do_not_expose_private_diagnostics(sandbox: Sandbox, status: int) -> None:
    """Only safe status classification may escape the private response boundary."""
    with (
        broker(sandbox, {"error": "private-provider-diagnostic"}, status),
        pytest.raises(ActivityError) as caught,
    ):
        read_slot_credentials(sandbox, "source")
    assert "private-provider-diagnostic" not in str(caught.value)
    assert caught.value.error_class == ("permanent" if status in {401, 403} else "retryable")


@pytest.mark.parametrize(
    "change",
    [
        "workspace",
        "attempt",
        "slot",
        "capability",
        "material",
        "expired",
        "naive",
        "missing-expiry",
        "version",
    ],
)
def test_invalid_broker_material_is_rejected(sandbox: Sandbox, change: str) -> None:
    """Reject wrong authority and malformed material rather than stringify or invent expiry."""
    payload = envelope()
    if change == "workspace":
        payload["lease"]["workspaceId"] = "other"
    elif change == "attempt":
        payload["lease"]["attempt"] = True
    elif change in {"slot", "capability"}:
        payload["lease"][change] = "other"
    elif change == "material":
        payload["credential"]["material"]["token"] = {"private": "value"}
    elif change in {"expired", "naive"}:
        payload["credential"]["expiresAt"] = (
            "2000-01-01T00:00:00Z" if change == "expired" else "2099-01-01T00:00:00"
        )
    elif change == "missing-expiry":
        del payload["credential"]["expiresAt"]
    else:
        payload["schemaVersion"] = "1"
    with broker(sandbox, payload) as calls, pytest.raises(ActivityError):
        read_slot_credentials(sandbox, "source")
    if change in {"material", "expired", "naive", "missing-expiry"}:
        assert calls[-1] == ("POST", '{"leaseId": "lease-source"}')
    else:
        assert len(calls) == 1


def test_partial_legacy_files_never_mix_with_broker(sandbox: Sandbox) -> None:
    """A missing half of an explicit credential pair cannot be filled from another authority."""
    path = sandbox.base / "in/secrets/source"
    path.mkdir(parents=True)
    (path / "username").write_text("legacy-user")
    with broker(sandbox, envelope()) as calls, pytest.raises(ActivityError):
        read_slot_credentials(sandbox, "source")
    assert calls == []


@pytest.mark.parametrize("phase", ["headers", "body"])
def test_total_exchange_deadline_bounds_trickled_http(
    sandbox: Sandbox,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    """Progress on individual socket reads must not reset the overall exchange budget."""
    from copy_image import broker as transport

    monkeypatch.setattr(transport, "_TIMEOUT_SECONDS", 0.08)
    sent = threading.Event()

    class Trickle(BaseHTTPRequestHandler):
        """Send a valid response slowly enough to exceed the complete request budget."""

        def do_GET(self) -> None:
            """Keep both header and body cases active until the client stops reading."""
            data = json.dumps(envelope()).encode()
            headers = f"HTTP/1.1 200 OK\r\nContent-Length: {len(data)}\r\n\r\n".encode()
            raw = headers + data if phase == "headers" else data
            if phase == "body":
                self.wfile.write(headers)
            for byte in raw:
                try:
                    self.wfile.write(bytes([byte]))
                except (BrokenPipeError, ConnectionResetError):
                    sent.set()
                    return
                time.sleep(0.01)

    with UnixStreamServer(str(sandbox.base / "run/connector.sock"), Trickle) as server:
        thread = threading.Thread(target=server.handle_request)
        thread.start()
        try:
            started = time.monotonic()
            with pytest.raises(ActivityError) as caught:
                read_slot_credentials(sandbox, "source")
            assert caught.value.error_class == "retryable"
            assert time.monotonic() - started < 0.5
        finally:
            thread.join(timeout=2)
        assert not thread.is_alive() and sent.is_set()


def test_response_budget_is_enforced(sandbox: Sandbox, monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject oversized envelopes even when the HTTP response is otherwise successful."""
    from copy_image import broker as transport

    monkeypatch.setattr(transport, "_MAX_RESPONSE_BYTES", 32)
    with broker(sandbox, envelope()), pytest.raises(ActivityError) as caught:
        read_slot_credentials(sandbox, "source")
    assert caught.value.error_class == "permanent"


def seed_copy(sandbox: Sandbox) -> None:
    """Supply ordinary copy inputs/context without plaintext credential injection."""
    (sandbox.base / "in/inputs.json").write_text(
        json.dumps(
            {
                "schemaVersion": "1",
                "contractVersion": "1",
                "activity": {"type": "copy-image", "version": "0.1.0"},
                "step": {"runId": "run", "stepId": "step", "attempt": 1},
                "inputs": {
                    "source": {"ref": "registry.test/source:latest"},
                    "destination": {"repository": "org/image"},
                },
            }
        )
    )
    context = json.loads((sandbox.base / "in/ctx.json").read_text())
    context["connectors"] = {"dest": {"endpoint": "https://ghcr.io"}}
    (sandbox.base / "in/ctx.json").write_text(json.dumps(context))


def test_partial_acquisition_releases_source(
    sandbox: Sandbox,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A destination rejection cannot abandon an already acquired source lease."""
    from copy_image.__main__ import main

    seed_copy(sandbox)
    monkeypatch.setenv("CUSTOS_IO_ROOT", str(sandbox.base))
    with broker(sandbox, envelope()) as calls:
        assert main([]) == 2
    assert calls[-1] == ("POST", '{"leaseId": "lease-source"}')
    assert len(calls) == 3


@pytest.mark.parametrize("cleanup_failure", ["lease", "auth-file"])
def test_completed_copy_stays_successful_when_cleanup_fails(
    sandbox: Sandbox,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    cleanup_failure: str,
) -> None:
    """Clean private auth data and report cleanup failure without retrying an external effect."""
    from copy_image.__main__ import main
    from copy_image.copy import CopyOutcome, CopyPlan
    from copy_image.credentials import SlotCredentials

    seed_copy(sandbox)
    monkeypatch.setenv("CUSTOS_IO_ROOT", str(sandbox.base))
    cleanup_original = tempfile.TemporaryDirectory.cleanup
    retained: list[tempfile.TemporaryDirectory[str]] = []

    def cleanup(directory: tempfile.TemporaryDirectory[str]) -> None:
        """Fail actual auth directory cleanup, retaining the object for safe fixture teardown."""
        if cleanup_failure == "auth-file" and Path(directory.name).name.startswith(
            "copy-image-auth-"
        ):
            retained.append(directory)
            raise PermissionError("private-cleanup-body")
        cleanup_original(directory)

    def credentials(sandbox: Sandbox, slot: str) -> SlotCredentials:
        """Return leased material for the lifecycle test, without changing transport tests."""
        return SlotCredentials("user", "private-pat", "lease-" + slot)

    def copy(plan: CopyPlan, authfile: Path) -> CopyOutcome:
        """Assert the actual private file location before completing the external effect."""
        assert authfile.parent.parent == sandbox.base / "run"
        assert authfile.is_file()
        return CopyOutcome(plan.destination_ref, "sha256:" + "a" * 64, 1)

    def release(sandbox: Sandbox, slot: str, credentials: SlotCredentials) -> None:
        """Simulate a cleanup failure whose private message must not enter logs."""
        if cleanup_failure == "lease":
            raise ActivityError("activity.unexpected_error", "retryable", "private-release-body")

    monkeypatch.setattr("copy_image.__main__.read_slot_credentials", credentials)
    monkeypatch.setattr("copy_image.__main__.run_skopeo_copy", copy)
    monkeypatch.setattr("copy_image.__main__.release_slot_credentials", release)
    monkeypatch.setattr(tempfile.TemporaryDirectory, "cleanup", cleanup)
    try:
        assert main([]) == 0
        if cleanup_failure == "auth-file":
            assert retained and (Path(retained[0].name) / "auth.json").is_file()
            assert "Private auth cleanup failed" in caplog.text
        else:
            assert list((sandbox.base / "run").iterdir()) == []
            assert "Credential lease cleanup failed" in caplog.text
        assert all(
            secret not in caplog.text
            for secret in (
                "private-release-body",
                "private-pat",
                "private-cleanup-body",
            )
        )
    finally:
        for directory in retained:
            cleanup_original(directory)

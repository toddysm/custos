"""Verify the configuration fixture's real TLS wrapping without Docker or a cluster."""

import importlib.util
import ssl
import subprocess
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "binding_probe", Path(__file__).parent / "binding_probe/probe.py"
)
assert SPEC is not None and SPEC.loader is not None
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


@pytest.mark.parametrize("tls", [False, True])
def test_probe_preserves_managed_application_protocol(
    tls: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Managed Runtime TLS uses an actual matching keypair; other consumers keep plaintext."""
    monkeypatch.setenv("PORT", "0")
    monkeypatch.setenv("CUSTOS_EXECUTION_IDENTITY_ENABLED", "false")
    for key in ("ARM_TLS_CERT_FILE", "ARM_TLS_KEY_FILE"):
        monkeypatch.delenv(key, raising=False)
    if tls:
        certificate, key_path = tmp_path / "tls.crt", tmp_path / "tls.key"
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "2",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost,IP:127.0.0.1",
                "-out",
                str(certificate),
                "-keyout",
                str(key_path),
            ],
            capture_output=True,
            check=True,
        )
        monkeypatch.setenv("ARM_TLS_CERT_FILE", str(certificate))
        monkeypatch.setenv("ARM_TLS_KEY_FILE", str(key_path))
    servers = []

    def server(
        address: tuple[str, int], handler: type[BaseHTTPRequestHandler]
    ) -> HTTPServer:
        """Bind an isolated loopback socket but return immediately instead of running a daemon."""
        assert address == ("0.0.0.0", 0)
        result = HTTPServer(("127.0.0.1", 0), handler)
        servers.append(result)
        monkeypatch.setattr(result, "serve_forever", lambda: None)
        return result

    monkeypatch.setattr(probe, "HTTPServer", server)
    try:
        probe.main()
        assert len(servers) == 1
        assert isinstance(servers[0].socket, ssl.SSLSocket) is tls
    finally:
        for value in servers:
            value.server_close()


def test_migration_fixture_does_not_open_a_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A no-PORT hook exits without pretending to perform application or migration work."""
    monkeypatch.delenv("PORT", raising=False)

    def forbidden(*args: object) -> None:
        """Reject a hook fixture that accidentally starts serving."""
        raise AssertionError("hook must not bind an application port")

    monkeypatch.setattr(probe, "HTTPServer", forbidden)
    probe.main()

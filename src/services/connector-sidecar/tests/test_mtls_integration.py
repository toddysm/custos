"""mTLS integration test for the sidecar control channel (CONN-IMPL-020).

Spins up the real :func:`create_control_app` under uvicorn with a
self-signed CA + server cert + client cert and verifies:

* A peer presenting a valid client cert signed by the configured CA
  reaches the handler and gets a normal ``revoked`` ack.
* A peer with no client cert (or one not signed by the configured CA)
  is rejected at the TLS handshake — the connection fails before any
  handler runs.

Marked ``@pytest.mark.integration`` so it stays out of the default
unit-test gate; run explicitly with ``pytest -m integration``. Skipped
when :mod:`cryptography` is not installed (CI installs it via the
sidecar's ``[dev]`` extra).
"""

from __future__ import annotations

import asyncio
import socket
import ssl
import threading
import time
from collections.abc import Iterator
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import pytest
import uvicorn

from custos_sidecar.control_app import create_control_app
from custos_sidecar.revocation import RevocationRegistry

cryptography = pytest.importorskip("cryptography")

if TYPE_CHECKING:
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
    from cryptography.x509 import Certificate

pytestmark = [pytest.mark.integration]


# --------------------------------------------------------------------------- #
# Self-signed PKI helpers
# --------------------------------------------------------------------------- #


def _free_port() -> int:
    """Allocate an unused TCP port on localhost for the test server."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _make_ca() -> tuple[Certificate, RSAPrivateKey]:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "custos-sidecar-test-ca")]
    )
    ski = x509.SubjectKeyIdentifier.from_public_key(key.public_key())
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(UTC) - timedelta(minutes=1))
        .not_valid_after(datetime.now(UTC) + timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(ski, critical=False)
        .sign(private_key=key, algorithm=hashes.SHA256())
    )
    return cert, key


def _sign_leaf(
    *,
    common_name: str,
    is_server: bool,
    ca_cert: Certificate,
    ca_key: RSAPrivateKey,
) -> tuple[Certificate, RSAPrivateKey]:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    eku = (
        x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH])
        if is_server
        else x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH])
    )
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(UTC) - timedelta(minutes=1))
        .not_valid_after(datetime.now(UTC) + timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(eku, critical=False)
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
            critical=False,
        )
    )
    if is_server:
        builder = builder.add_extension(
            x509.SubjectAlternativeName([x509.DNSName("localhost")]),
            critical=False,
        )
    cert = builder.sign(private_key=ca_key, algorithm=hashes.SHA256())
    return cert, key


def _write_pem(
    tmp: Path,
    name: str,
    cert: Certificate,
    key: RSAPrivateKey | None,
) -> tuple[Path, Path]:
    from cryptography.hazmat.primitives import serialization

    cert_path = tmp / f"{name}.crt"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    if key is None:
        return cert_path, cert_path
    key_path = tmp / f"{name}.key"
    key_path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    return cert_path, key_path


@pytest.fixture
def pki(tmp_path: Path) -> dict[str, Path]:
    """Mint a self-signed CA + server cert + good client cert + rogue cert.

    Returns a dict of PEM paths the test parametrises over.
    """
    ca_cert, ca_key = _make_ca()
    server_cert, server_key = _sign_leaf(
        common_name="localhost", is_server=True, ca_cert=ca_cert, ca_key=ca_key
    )
    client_cert, client_key = _sign_leaf(
        common_name="test-client", is_server=False, ca_cert=ca_cert, ca_key=ca_key
    )
    # Rogue CA + client signed by it (server's trust store does NOT include this CA).
    rogue_ca_cert, rogue_ca_key = _make_ca()
    rogue_client_cert, rogue_client_key = _sign_leaf(
        common_name="rogue-client",
        is_server=False,
        ca_cert=rogue_ca_cert,
        ca_key=rogue_ca_key,
    )

    paths: dict[str, Path] = {}
    paths["ca_cert"], _ = _write_pem(tmp_path, "ca", ca_cert, None)
    paths["server_cert"], paths["server_key"] = _write_pem(
        tmp_path, "server", server_cert, server_key
    )
    paths["client_cert"], paths["client_key"] = _write_pem(
        tmp_path, "client", client_cert, client_key
    )
    paths["rogue_cert"], paths["rogue_key"] = _write_pem(
        tmp_path, "rogue", rogue_client_cert, rogue_client_key
    )
    return paths


# --------------------------------------------------------------------------- #
# Server harness
# --------------------------------------------------------------------------- #


class _FakeGateway:
    """Minimal gateway stub for the integration server.

    Always returns ``revoked`` for every requested lease. The
    integration test only needs the handler to reach Connector
    Service; the wire shape of the ack is fixed.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[list[str], str]] = []

    async def revoke_many(self, lease_ids: list[str], reason: str) -> list[dict[str, str]]:
        self.calls.append((list(lease_ids), reason))
        return [{"leaseId": lid, "status": "revoked"} for lid in lease_ids]


@pytest.fixture
def control_server(pki: dict[str, Path]) -> Iterator[tuple[int, _FakeGateway, RevocationRegistry]]:
    """Spin up the real control app under uvicorn with mTLS on a free port.

    Yields ``(port, fake_gateway, registry)`` and tears down the
    server on exit.
    """
    port = _free_port()
    gateway = _FakeGateway()
    registry = RevocationRegistry()
    app = create_control_app(
        revocation_registry=registry,
        lease_gateway=gateway,  # type: ignore[arg-type]
    )
    config = uvicorn.Config(
        app=app,
        host="127.0.0.1",
        port=port,
        log_level="warning",
        access_log=False,
        lifespan="on",
        ssl_keyfile=str(pki["server_key"]),
        ssl_certfile=str(pki["server_cert"]),
        ssl_ca_certs=str(pki["ca_cert"]),
        ssl_cert_reqs=ssl.CERT_REQUIRED,
    )
    server = uvicorn.Server(config)

    def _run() -> None:
        asyncio.run(server.serve())

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()

    # Wait for the listener to come up.
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        if server.started:
            break
        time.sleep(0.05)
    else:
        server.should_exit = True
        thread.join(timeout=5)
        raise RuntimeError("control server failed to start within timeout")

    try:
        yield port, gateway, registry
    finally:
        server.should_exit = True
        thread.join(timeout=5)


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #


def test_mtls_valid_client_cert_reaches_handler(
    control_server: tuple[int, _FakeGateway, RevocationRegistry],
    pki: dict[str, Path],
) -> None:
    port, gateway, registry = control_server
    ctx = ssl.create_default_context(cafile=str(pki["ca_cert"]))
    ctx.load_cert_chain(certfile=str(pki["client_cert"]), keyfile=str(pki["client_key"]))
    with httpx.Client(verify=ctx, timeout=10.0) as client:
        resp = client.post(
            f"https://localhost:{port}/sidecar-admin/v1/revoke",
            json={"leaseIds": ["lease_001"], "reason": "rotation"},
        )
    assert resp.status_code == 200
    assert resp.json() == {"results": [{"leaseId": "lease_001", "status": "revoked"}]}
    assert gateway.calls == [(["lease_001"], "rotation")]
    assert registry.is_revoked("lease_001")


def test_mtls_no_client_cert_rejected_at_handshake(
    control_server: tuple[int, _FakeGateway, RevocationRegistry],
    pki: dict[str, Path],
) -> None:
    """Without a client cert the TLS handshake fails — no handler runs."""
    port, gateway, _ = control_server
    ctx = ssl.create_default_context(cafile=str(pki["ca_cert"]))
    # No load_cert_chain — client presents no cert.
    with (
        httpx.Client(verify=ctx, timeout=10.0) as client,
        pytest.raises((httpx.TransportError, ssl.SSLError)),
    ):
        client.post(
            f"https://localhost:{port}/sidecar-admin/v1/revoke",
            json={"leaseIds": ["lease_x"], "reason": "x"},
        )
    assert gateway.calls == []


def test_mtls_rogue_client_cert_rejected_at_handshake(
    control_server: tuple[int, _FakeGateway, RevocationRegistry],
    pki: dict[str, Path],
) -> None:
    """A client cert signed by a CA the server does not trust is rejected."""
    port, gateway, _ = control_server
    ctx = ssl.create_default_context(cafile=str(pki["ca_cert"]))
    ctx.load_cert_chain(certfile=str(pki["rogue_cert"]), keyfile=str(pki["rogue_key"]))
    with (
        httpx.Client(verify=ctx, timeout=10.0) as client,
        pytest.raises((httpx.TransportError, ssl.SSLError)),
    ):
        client.post(
            f"https://localhost:{port}/sidecar-admin/v1/revoke",
            json={"leaseIds": ["lease_x"], "reason": "x"},
        )
    assert gateway.calls == []

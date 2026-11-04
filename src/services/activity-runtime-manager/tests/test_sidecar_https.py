"""Use the production Sidecar client against a real HTTPS endpoint and explicit private CA."""

import ipaddress
import json
import ssl
import threading
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from custos_callctx.bootstrap import BoundTriple, mint_bootstrap_token
from custos_sidecar.auth import BootstrapTokenVerifier
from custos_sidecar.broker import RuntimeBundle, create_broker_app

from tests.test_sidecar_trust import public_ca


def certificates(root: Path) -> tuple[Path, Path, Path]:
    """Create strict-validation-compatible test certificates without persisting an issuer key."""
    ca_key, server_key = (
        ec.generate_private_key(ec.SECP256R1()),
        ec.generate_private_key(ec.SECP256R1()),
    )
    issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test-issuer")])
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.now(UTC)

    def build(name: x509.Name, key: ec.EllipticCurvePrivateKey, *, ca: bool) -> x509.Certificate:
        """Sign a bounded root or server certificate with explicit usage and key identifiers."""
        builder = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
            .add_extension(
                x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False
            )
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
                critical=False,
            )
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=False,
                    data_encipherment=False,
                    key_agreement=False,
                    key_cert_sign=ca,
                    crl_sign=ca,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
        )
        if not ca:
            builder = builder.add_extension(
                x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False
            ).add_extension(
                x509.SubjectAlternativeName(
                    [
                        x509.DNSName("localhost"),
                        x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                    ]
                ),
                critical=False,
            )
        return builder.sign(ca_key, hashes.SHA256())

    ca_file, cert_file, key_file = (root / name for name in ("ca.crt", "tls.crt", "tls.key"))
    ca_file.write_bytes(build(issuer, ca_key, ca=True).public_bytes(serialization.Encoding.PEM))
    cert_file.write_bytes(
        build(subject, server_key, ca=False).public_bytes(serialization.Encoding.PEM)
    )
    key_file.touch(mode=0o600)
    key_file.write_bytes(
        server_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return ca_file, cert_file, key_file


@pytest.mark.parametrize("trusted", [True, False])
async def test_real_https_proxy_requires_configured_ca(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    trusted: bool,
) -> None:
    """Only matching trust permits the real broker to send its proof; no verify=False fallback."""
    ca, cert, key = certificates(tmp_path)
    calls: list[dict[str, Any]] = []
    proof = "private-unit-proof-" + "x" * 40

    class Proxy(BaseHTTPRequestHandler):
        """Substitute only the remote issuer response, retaining real TLS and Sidecar behavior."""

        def do_POST(self) -> None:
            """Record a proof only after a fully verified TLS handshake."""
            assert self.headers["Authorization"] == "Bearer " + proof
            calls.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            raw = json.dumps(
                {
                    "schemaVersion": "2",
                    "credential": {"material": {"token": "unit-material"}},
                    "lease": {
                        "workspaceId": "ws",
                        "runId": "run",
                        "stepId": "step",
                        "attempt": 1,
                        "slot": "source",
                        "capability": "oci.pull",
                        "leaseId": "lease",
                    },
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, format: str, *args: object) -> None:
            """Never print request headers or credential material from the local test server."""

    with HTTPServer(("127.0.0.1", 0), Proxy) as server:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(cert, key)
        server.socket = context.wrap_socket(server.socket, server_side=True)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            if not trusted:
                ca.write_bytes(public_ca())
            monkeypatch.setenv("CUSTOS_SIDECAR_CA_BUNDLE", str(ca))
            triple = BoundTriple("run", "step", 1)
            verifier = BootstrapTokenVerifier(key=b"k" * 32, triple=triple, clock=lambda: 1009)
            bundle = RuntimeBundle.model_validate(
                {
                    "workspaceId": "ws",
                    "runId": "run",
                    "stepId": "step",
                    "attempt": 1,
                    "proof": proof,
                    "endpoint": f"https://127.0.0.1:{server.server_port}",
                }
            )
            app = create_broker_app(bundle, verifier)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://uds"
                ) as client,
            ):
                response = await client.get(
                    "/v1/token?slot=source&purpose=oci.pull",
                    headers={
                        "Custos-Sidecar-Token": mint_bootstrap_token(
                            key=b"k" * 32,
                            triple=triple,
                            now=1009,
                            ttl_sec=300,
                        )
                    },
                )
            assert response.status_code == (200 if trusted else 503)
            assert len(calls) == (1 if trusted else 0)
            assert proof not in response.text
        finally:
            server.shutdown()
            thread.join()

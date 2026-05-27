"""End-to-end harness for the sidecar (CONN-IMPL-021).

Drives the whole sidecar process surface in one test:

* a fake Connector Service stub (real FastAPI app on a free TCP port)
  serving ``/internal/v1/leases:{issue,refresh,release,revoke}``;
* the production sidecar entrypoint wiring — both the UDS server and
  the mTLS-gated control HTTPS server, sharing one
  :class:`RevocationRegistry` and one :class:`LeaseGateway` instance,
  spun up in-process via ``_run_servers``;
* a fake activity client that mints a real bootstrap token and walks
  the lease lifecycle over the UDS;
* a fake operator client that drives ``/sidecar-admin/v1/revoke`` via
  mTLS and observes the revoke being enforced on subsequent UDS hits.

Marked ``@pytest.mark.integration`` so it stays out of the default
unit-test gate; run explicitly with ``pytest -m integration``. Skipped
when :mod:`cryptography` is not installed (CI installs it via the
sidecar's ``[dev]`` extra).

Acceptance criteria from CONN-IMPL-021:

* the activity-facing UDS API and the control-channel mTLS API are
  asserted in one test run;
* the harness uses no fixtures from the unit suite — all
  collaborators (CS stub, PKI, sidecar settings) are stood up here.
"""

from __future__ import annotations

import asyncio
import socket
import ssl
import tempfile
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
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from custos_sidecar import create_app
from custos_sidecar.__main__ import (
    _build_control_server,
    _build_uds_server,
    _run_servers,
)
from custos_sidecar.auth import BootstrapTokenVerifier, BoundTriple, mint_bootstrap_token
from custos_sidecar.context_registry import ContextRegistry, SlotContext
from custos_sidecar.control_app import create_control_app
from custos_sidecar.credential_minter import StubCredentialMinter
from custos_sidecar.lease_gateway import LeaseGateway, LeaseGatewaySettings
from custos_sidecar.revocation import RevocationRegistry

cryptography = pytest.importorskip("cryptography")

if TYPE_CHECKING:
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
    from cryptography.x509 import Certificate

pytestmark = [pytest.mark.integration]


# --------------------------------------------------------------------------- #
# Common helpers
# --------------------------------------------------------------------------- #


HMAC_KEY = b"e2e-key-0123456789abcdef0123456789abcdef"
RUN_ID = "run_01HZE2E0000000000000000000"
STEP_ID = "build"
ATTEMPT = 1
WORKSPACE_ID = "ws_e2e"
SLOT = "primary"
PURPOSE = "read"
CONNECTOR_INSTANCE_ID = "ci_01HZE2E1111111111111111111111"


def _free_port() -> int:
    """Allocate an unused TCP port on localhost."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


# --------------------------------------------------------------------------- #
# Self-signed PKI for the mTLS control channel
# --------------------------------------------------------------------------- #


def _make_ca() -> tuple[Certificate, RSAPrivateKey]:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "custos-sidecar-e2e-ca")])
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
    """Mint a self-signed CA + server cert + client cert in ``tmp_path``."""
    ca_cert, ca_key = _make_ca()
    server_cert, server_key = _sign_leaf(
        common_name="localhost", is_server=True, ca_cert=ca_cert, ca_key=ca_key
    )
    client_cert, client_key = _sign_leaf(
        common_name="e2e-client", is_server=False, ca_cert=ca_cert, ca_key=ca_key
    )
    paths: dict[str, Path] = {}
    paths["ca_cert"], _ = _write_pem(tmp_path, "ca", ca_cert, None)
    paths["server_cert"], paths["server_key"] = _write_pem(
        tmp_path, "server", server_cert, server_key
    )
    paths["client_cert"], paths["client_key"] = _write_pem(
        tmp_path, "client", client_cert, client_key
    )
    return paths


# --------------------------------------------------------------------------- #
# Fake Connector Service stub
# --------------------------------------------------------------------------- #


class _FakeConnectorService:
    """In-memory state for the fake CS HTTP server.

    Tracks issued leases (so refresh can hand back a stable id) and
    revoked lease ids (so a refresh after revoke can return
    ``NOT_FOUND`` if the real CS would). The harness drives a happy
    path where the sidecar's revocation registry — not CS — is the
    enforcement point on the UDS surface; CS is just asked to ack the
    revoke.
    """

    def __init__(self) -> None:
        self.calls: dict[str, int] = {}
        self.next_lease_serial = 0
        self.revoked: set[str] = set()

    def _bump(self, name: str) -> None:
        self.calls[name] = self.calls.get(name, 0) + 1

    def _new_lease_id(self) -> str:
        self.next_lease_serial += 1
        return f"lease_e2e_{self.next_lease_serial:03d}"


def _lease_envelope(
    *,
    lease_id: str,
    body: dict[str, object],
    expires_in_sec: int = 3600,
) -> dict[str, object]:
    """Build a lease envelope matching :meth:`LeaseRecord.from_wire`."""
    now = datetime.now(UTC).replace(microsecond=0)
    return {
        "workspaceId": WORKSPACE_ID,
        "leaseId": lease_id,
        "runId": body.get("runId", RUN_ID),
        "stepId": body.get("stepId", STEP_ID),
        "attempt": int(str(body.get("attempt", ATTEMPT))),
        "slot": body.get("slot", SLOT),
        "capability": body.get("capability", PURPOSE),
        "connectorInstanceId": body.get("connectorInstanceId", CONNECTOR_INSTANCE_ID),
        "tokenType": body.get("tokenType", "Bearer"),
        "issuedAt": now.isoformat(),
        "expiresAt": (now + timedelta(seconds=expires_in_sec)).isoformat(),
        "releasedAt": None,
        "revokedAt": None,
        "revokeReason": None,
    }


def _build_fake_cs_app(state: _FakeConnectorService) -> FastAPI:
    """Build a FastAPI app implementing the CS internal lease RPC.

    Exposes only the four endpoints the sidecar calls. Everything
    else returns 404; the gateway's wire contract is well-tested
    elsewhere so we do not need full CS parity here.
    """
    app = FastAPI()

    @app.post("/internal/v1/leases:issue")
    async def issue(request: Request) -> Response:
        body = await request.json()
        state._bump("issue")
        lease_id = state._new_lease_id()
        envelope = _lease_envelope(lease_id=lease_id, body=body)
        return JSONResponse(status_code=200, content={"lease": envelope})

    @app.post("/internal/v1/leases:refresh")
    async def refresh(request: Request) -> Response:
        body = await request.json()
        state._bump("refresh")
        lease_id = str(body["leaseId"])
        envelope = _lease_envelope(
            lease_id=lease_id,
            body={
                "runId": RUN_ID,
                "stepId": STEP_ID,
                "attempt": ATTEMPT,
                "slot": SLOT,
                "capability": PURPOSE,
            },
            expires_in_sec=7200,
        )
        return JSONResponse(status_code=200, content={"lease": envelope})

    @app.post("/internal/v1/leases:release")
    async def release(_request: Request) -> Response:
        state._bump("release")
        return Response(status_code=204)

    @app.post("/internal/v1/leases:revoke")
    async def revoke(request: Request) -> Response:
        body = await request.json()
        state._bump("revoke")
        ids = [str(x) for x in body["leaseIds"]]
        for lease_id in ids:
            state.revoked.add(lease_id)
        return JSONResponse(
            status_code=200,
            content={"results": [{"leaseId": lid, "status": "revoked"} for lid in ids]},
        )

    return app


@pytest.fixture
def fake_cs(tmp_path: Path) -> Iterator[tuple[int, _FakeConnectorService]]:
    """Spin up the fake CS app under uvicorn on a free TCP port."""
    port = _free_port()
    state = _FakeConnectorService()
    config = uvicorn.Config(
        app=_build_fake_cs_app(state),
        host="127.0.0.1",
        port=port,
        log_level="warning",
        access_log=False,
        lifespan="on",
    )
    server = uvicorn.Server(config)

    def _run() -> None:
        asyncio.run(server.serve())

    thread = threading.Thread(target=_run, daemon=True, name="fake-cs")
    thread.start()
    _wait_for_started(server, what="fake CS")
    try:
        yield port, state
    finally:
        server.should_exit = True
        thread.join(timeout=5)


def _wait_for_started(server: uvicorn.Server, *, what: str, timeout_sec: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if server.started:
            return
        time.sleep(0.05)
    server.should_exit = True
    raise RuntimeError(f"{what} failed to start within {timeout_sec}s")


# --------------------------------------------------------------------------- #
# Sidecar harness
# --------------------------------------------------------------------------- #


@pytest.fixture
def short_uds_dir() -> Iterator[Path]:
    """Return a short directory for the UDS file.

    macOS' ``AF_UNIX`` path is capped at 104 chars; ``tmp_path`` lives
    under ``/private/var/folders/...`` which routinely blows that
    budget. Allocate a short ``/tmp/custos-sc-e2e-<pid>`` instead so
    the socket path stays well under the limit.
    """
    parent = tempfile.mkdtemp(prefix="custos-sc-e2e-", dir="/tmp")
    try:
        yield Path(parent)
    finally:
        import shutil

        shutil.rmtree(parent, ignore_errors=True)


@pytest.fixture
def sidecar(
    fake_cs: tuple[int, _FakeConnectorService],
    pki: dict[str, Path],
    short_uds_dir: Path,
) -> Iterator[tuple[Path, int, str, _FakeConnectorService]]:
    """Spin up the full sidecar: UDS server + mTLS control server.

    Yields ``(socket_path, control_port, bootstrap_token, fake_cs_state)``.

    Wires the same collaborators ``custos_sidecar.__main__.main``
    builds in production (``LeaseGateway.from_settings`` against the
    fake CS URL, a real :class:`BootstrapTokenVerifier`, the
    :class:`StubCredentialMinter`, a fresh
    :class:`RevocationRegistry` shared by both surfaces), but skips
    settings parsing — the harness constructs collaborators directly
    so the test can hold a handle on the registry and the gateway.
    """
    cs_port, cs_state = fake_cs

    socket_path = short_uds_dir / "connector.sock"
    bound_triple = BoundTriple(run_id=RUN_ID, step_id=STEP_ID, attempt=ATTEMPT)
    verifier = BootstrapTokenVerifier(key=HMAC_KEY, triple=bound_triple)
    context_registry = ContextRegistry(
        [
            SlotContext(
                slot=SLOT,
                connector_instance_id=CONNECTOR_INSTANCE_ID,
                capabilities=("read", "write"),
                endpoint="https://upstream.example/primary",
                token_type="Bearer",
                extras={"region": "us-west-2"},
            ),
        ]
    )
    gateway = LeaseGateway.from_settings(
        LeaseGatewaySettings(
            connector_service_url=f"http://127.0.0.1:{cs_port}",
            call_context='{"workspaceId":"ws_e2e","principal":"svc:connector-sidecar"}',
        )
    )
    minter = StubCredentialMinter()
    revocation_registry = RevocationRegistry()

    uds_app = create_app(
        bootstrap_verifier=verifier,
        context_registry=context_registry,
        lease_gateway=gateway,
        credential_minter=minter,
        bound_triple=(RUN_ID, STEP_ID, ATTEMPT),
        revocation_registry=revocation_registry,
    )
    control_app = create_control_app(
        revocation_registry=revocation_registry,
        lease_gateway=gateway,
    )

    control_port = _free_port()
    # Build a Settings struct just to feed _build_control_server; the
    # field names match the production wiring and validate __post_init__.
    from custos_sidecar.settings import Settings

    settings = Settings(
        socket_path=str(socket_path),
        bootstrap_token_path=str(short_uds_dir / "bootstrap-token"),
        bootstrap_key_path=str(short_uds_dir / "bootstrap-key"),
        run_id=RUN_ID,
        step_id=STEP_ID,
        attempt=ATTEMPT,
        workspace_id=WORKSPACE_ID,
        connector_service_url=f"http://127.0.0.1:{cs_port}",
        call_context='{"workspaceId":"ws_e2e","principal":"svc:connector-sidecar"}',
        contexts_wire=[],
        activity_gid=None,
        control_enabled=True,
        control_host="127.0.0.1",
        control_port=control_port,
        control_tls_cert_path=str(pki["server_cert"]),
        control_tls_key_path=str(pki["server_key"]),
        control_tls_ca_path=str(pki["ca_cert"]),
    )

    socket_path.parent.mkdir(parents=True, exist_ok=True)
    if socket_path.exists():  # pragma: no cover - fresh tmp dir
        socket_path.unlink()

    uds_server = _build_uds_server(
        app=uds_app,
        socket_path=socket_path,
        activity_gid=None,
    )
    control_server = _build_control_server(app=control_app, settings=settings)

    def _run() -> None:
        try:
            asyncio.run(_run_servers(uds_server=uds_server, control_server=control_server))
        finally:
            with __import__("contextlib").suppress(RuntimeError):
                asyncio.run(gateway.aclose())

    thread = threading.Thread(target=_run, daemon=True, name="sidecar-e2e")
    thread.start()

    # Wait for both servers to come up.
    _wait_for_started(uds_server, what="sidecar UDS")
    _wait_for_started(control_server, what="sidecar control")

    bootstrap_token = mint_bootstrap_token(key=HMAC_KEY, triple=bound_triple, ttl_sec=600)

    try:
        yield socket_path, control_port, bootstrap_token, cs_state
    finally:
        uds_server.should_exit = True
        control_server.should_exit = True
        thread.join(timeout=10)


# --------------------------------------------------------------------------- #
# The single end-to-end test
# --------------------------------------------------------------------------- #


def test_lease_lifecycle_uds_and_revoke_e2e(
    sidecar: tuple[Path, int, str, _FakeConnectorService],
    pki: dict[str, Path],
) -> None:
    """Exercise the full sidecar surface in one run.

    Walks the activity contract over the UDS (issue, refresh,
    release) and the operator/ARM contract over the mTLS control
    channel (revoke), then verifies the cross-surface invariant: a
    revoked lease's refresh and release both fall back to 410
    ``lease-revoked`` even though the UDS bootstrap token is still
    valid.
    """
    socket_path, control_port, bootstrap_token, cs_state = sidecar

    # --------------------------------------------------------------- UDS: issue
    transport = httpx.AsyncHTTPTransport(uds=str(socket_path))
    asyncio.run(
        _drive_uds_lifecycle(
            transport=transport,
            control_port=control_port,
            bootstrap_token=bootstrap_token,
            pki=pki,
            cs_state=cs_state,
        )
    )


async def _drive_uds_lifecycle(
    *,
    transport: httpx.AsyncHTTPTransport,
    control_port: int,
    bootstrap_token: str,
    pki: dict[str, Path],
    cs_state: _FakeConnectorService,
) -> None:
    headers = {"Custos-Sidecar-Token": bootstrap_token}
    async with httpx.AsyncClient(transport=transport, base_url="http://sidecar.local") as activity:
        # 1) Issue a lease.
        resp = await activity.get(
            "/v1/token",
            params={"slot": SLOT, "purpose": PURPOSE},
            headers=headers,
        )
        assert resp.status_code == 200, resp.text
        token_envelope_1 = resp.json()
        # Per the design's locked envelope schema the lease id, token,
        # tokenType and scope live at the top level.
        first_lease_id = token_envelope_1["leaseId"]
        first_token = token_envelope_1["token"]
        assert token_envelope_1["tokenType"] == "Bearer"
        assert token_envelope_1["scope"]["connectorSlot"] == SLOT
        assert first_lease_id.startswith("lease_e2e_")
        assert first_token  # stub minter returns a non-empty string
        assert cs_state.calls["issue"] == 1

        # 2) Refresh the lease — same id, fresh token.
        resp = await activity.post(
            "/v1/token/refresh",
            json={"leaseId": first_lease_id},
            headers=headers,
        )
        assert resp.status_code == 200, resp.text
        refreshed = resp.json()
        assert refreshed["leaseId"] == first_lease_id
        # The stub minter is deterministic per (leaseId, slot, purpose);
        # the refreshed token may equal the issued token. The wire
        # invariant we care about is that the lease id is stable.
        assert refreshed["token"]
        assert cs_state.calls["refresh"] == 1

        # 3) Issue a second lease so we can release the first without revoking it.
        resp = await activity.get(
            "/v1/token",
            params={"slot": SLOT, "purpose": PURPOSE},
            headers=headers,
        )
        assert resp.status_code == 200, resp.text
        second_lease_id = resp.json()["leaseId"]
        assert second_lease_id != first_lease_id
        assert cs_state.calls["issue"] == 2

        # 4) Revoke the first lease via the mTLS control channel.
        ctx = ssl.create_default_context(cafile=str(pki["ca_cert"]))
        ctx.load_cert_chain(certfile=str(pki["client_cert"]), keyfile=str(pki["client_key"]))
        async with httpx.AsyncClient(verify=ctx, timeout=10.0) as operator:
            ctl = await operator.post(
                f"https://localhost:{control_port}/sidecar-admin/v1/revoke",
                json={"leaseIds": [first_lease_id], "reason": "e2e-test"},
            )
        assert ctl.status_code == 200, ctl.text
        assert ctl.json() == {"results": [{"leaseId": first_lease_id, "status": "revoked"}]}
        assert cs_state.calls["revoke"] == 1

        # 5) Refresh of the revoked lease must hit 410 lease-revoked.
        resp = await activity.post(
            "/v1/token/refresh",
            json={"leaseId": first_lease_id},
            headers=headers,
        )
        assert resp.status_code == 410, resp.text
        # FastAPI emits problem+json; the revocation reason is carried verbatim.
        problem = resp.json()
        assert problem["type"].endswith("lease-revoked")
        # ``cs_state.refresh`` count must not have ticked: revocation is
        # enforced locally before the gateway is dialled.
        assert cs_state.calls["refresh"] == 1

        # 6) Release of the revoked lease also short-circuits to 410.
        resp = await activity.post(
            "/v1/token/release",
            json={"leaseId": first_lease_id},
            headers=headers,
        )
        assert resp.status_code == 410, resp.text
        assert "release" not in cs_state.calls

        # 7) Release of the non-revoked second lease succeeds (204).
        resp = await activity.post(
            "/v1/token/release",
            json={"leaseId": second_lease_id},
            headers=headers,
        )
        assert resp.status_code == 204
        assert cs_state.calls["release"] == 1

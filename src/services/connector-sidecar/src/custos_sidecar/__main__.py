"""Sidecar entry point (CONN-IMPL-019 + CONN-IMPL-020).

Reads :class:`~custos_sidecar.settings.Settings` from environment
variables, wires the production collaborators, and launches two
``uvicorn`` servers concurrently:

* The UDS HTTP server bound to ``${CUSTOS_SIDECAR_SOCKET_PATH}`` for
  the activity-facing token API (CONN-IMPL-019).
* When ``CUSTOS_SIDECAR_CONTROL_ENABLED=true``, the mTLS-gated HTTPS
  control server bound to ``${CUSTOS_SIDECAR_CONTROL_PORT}`` for the
  operator/ARM-driven revoke API (CONN-IMPL-020).

This module is intentionally thin so the bulk of the logic stays
under unit tests; the actual ``Server.serve()`` invocations are only
exercised by the integration UDS / mTLS harnesses (and in production
by ARM).
"""

from __future__ import annotations

import asyncio
import os
import ssl
import sys
from contextlib import suppress
from pathlib import Path

import uvicorn
from fastapi import FastAPI

from custos_sidecar import create_app
from custos_sidecar.auth import BootstrapTokenVerifier, BoundTriple
from custos_sidecar.context_registry import ContextRegistry
from custos_sidecar.control_app import create_control_app
from custos_sidecar.credential_minter import StubCredentialMinter
from custos_sidecar.lease_gateway import LeaseGateway, LeaseGatewaySettings
from custos_sidecar.revocation import RevocationRegistry
from custos_sidecar.settings import Settings, load_settings

#: Mode the sidecar applies to the UDS file after uvicorn binds it.
#: ``0o660`` = ``rw`` for the sidecar UID and (when ``activity_gid`` is set)
#: ``rw`` for the activity GID; world-deny. The activity container connects
#: via group membership, not UID equality, so a tighter ``0o600`` would lock
#: the activity uid out of the socket entirely.
UDS_SOCKET_MODE = 0o660


def _build_verifier(settings: Settings) -> BootstrapTokenVerifier:
    """Read the HMAC key from disk and build the verifier."""
    key_path = Path(settings.bootstrap_key_path)
    try:
        key = key_path.read_bytes()
    except OSError as exc:
        raise RuntimeError(f"failed to read bootstrap key from {key_path}: {exc!s}") from exc
    triple = BoundTriple(
        run_id=settings.run_id,
        step_id=settings.step_id,
        attempt=settings.attempt,
    )
    return BootstrapTokenVerifier(key=key, triple=triple)


def _build_gateway(settings: Settings) -> LeaseGateway:
    """Build the production Lease Gateway from settings."""
    gateway_settings = LeaseGatewaySettings(
        connector_service_url=settings.connector_service_url,
        call_context=settings.call_context,
    )
    return LeaseGateway.from_settings(gateway_settings)


def _apply_socket_perms(socket_path: Path, activity_gid: int | None) -> None:
    """Chmod (and optionally chown) the UDS file after uvicorn binds it.

    Called from :class:`_PermFixingServer.startup` so the perms are
    applied **between** uvicorn's ``loop.create_unix_server`` (which
    creates the file with whatever default mode the loop chooses) and
    the server actually accepting connections. The activity container
    can therefore connect to a correctly-permissioned socket on the
    very first request.
    """
    try:
        os.chmod(socket_path, UDS_SOCKET_MODE)
    except OSError as exc:  # pragma: no cover - production-only path
        print(
            f"[sidecar] failed to chmod UDS {socket_path} to {UDS_SOCKET_MODE:o}: {exc!s}",
            file=sys.stderr,
        )
    if activity_gid is not None:
        try:
            os.chown(socket_path, -1, activity_gid)
        except OSError as exc:  # pragma: no cover - production-only path
            print(
                f"[sidecar] failed to chown UDS {socket_path} to gid={activity_gid}: {exc!s}",
                file=sys.stderr,
            )


class _PermFixingServer(uvicorn.Server):
    """:class:`uvicorn.Server` that tightens the UDS file's perms.

    Uvicorn binds the unix socket inside ``startup()`` and then resets
    the file to ``0o666``. We override ``startup`` to run our own
    chmod/chown immediately after the super call so the socket is
    correctly permissioned **before** uvicorn enters its accept loop.
    """

    def __init__(
        self,
        config: uvicorn.Config,
        *,
        socket_path: Path,
        activity_gid: int | None,
    ) -> None:
        super().__init__(config)
        self._socket_path = socket_path
        self._activity_gid = activity_gid

    async def startup(self, sockets: list[object] | None = None) -> None:  # type: ignore[override]
        await super().startup(sockets=sockets)  # type: ignore[arg-type]
        _apply_socket_perms(self._socket_path, self._activity_gid)


def _build_uds_server(
    *,
    app: FastAPI,
    socket_path: Path,
    activity_gid: int | None,
) -> _PermFixingServer:
    """Build the UDS uvicorn server with the perm-fixing subclass."""
    config = uvicorn.Config(
        app=app,
        uds=str(socket_path),
        log_level="info",
        access_log=False,
        lifespan="on",
    )
    return _PermFixingServer(
        config,
        socket_path=socket_path,
        activity_gid=activity_gid,
    )


def _build_control_server(
    *,
    app: FastAPI,
    settings: Settings,
) -> uvicorn.Server:
    """Build the control-channel HTTPS uvicorn server with mTLS.

    Uvicorn is configured with ``ssl_cert_reqs=ssl.CERT_REQUIRED`` and
    ``ssl_ca_certs=<ca path>`` so any client presenting an unsigned or
    unsigned-by-the-configured-CA certificate is rejected at the TLS
    handshake (no handler runs). The handler layer therefore does not
    re-check the client identity.

    Requires :attr:`Settings.control_tls_cert_path`,
    :attr:`Settings.control_tls_key_path`, and
    :attr:`Settings.control_tls_ca_path` to be non-None; the settings
    loader validates this invariant at parse time so a bad config
    crashes the pod at start.
    """
    assert settings.control_tls_cert_path is not None
    assert settings.control_tls_key_path is not None
    assert settings.control_tls_ca_path is not None
    config = uvicorn.Config(
        app=app,
        host=settings.control_host,
        port=settings.control_port,
        log_level="info",
        access_log=False,
        lifespan="on",
        ssl_keyfile=settings.control_tls_key_path,
        ssl_certfile=settings.control_tls_cert_path,
        ssl_ca_certs=settings.control_tls_ca_path,
        ssl_cert_reqs=ssl.CERT_REQUIRED,
    )
    return uvicorn.Server(config)


async def _run_servers(
    *,
    uds_server: _PermFixingServer,
    control_server: uvicorn.Server | None,
) -> None:
    """Serve both surfaces concurrently; cancel the other on either exit.

    When the control server is ``None`` (control disabled by settings),
    only the UDS server runs and the function returns when it exits.
    Otherwise we ``gather`` both ``serve()`` coroutines; if either
    returns or raises we set ``should_exit`` on the survivor so the
    process shuts down cleanly instead of hanging on one half.
    """
    if control_server is None:
        await uds_server.serve()
        return

    async def _wrap(server: uvicorn.Server, peer: uvicorn.Server) -> None:
        try:
            await server.serve()
        finally:
            # Make sure the other server unblocks even on cancel/crash.
            peer.should_exit = True

    await asyncio.gather(
        _wrap(uds_server, control_server),
        _wrap(control_server, uds_server),
    )


def main() -> int:
    """Process entry point used by the ``custos-connector-sidecar`` script.

    Returns the exit code; the server loop typically blocks until a
    signal triggers ``should_exit``. A non-zero return only happens on
    a setup error (bad settings, unreadable HMAC key, stale socket
    that cannot be removed).
    """
    try:
        settings = load_settings()
    except ValueError as exc:
        print(f"[sidecar] settings error: {exc!s}", file=sys.stderr)
        return 2

    verifier = _build_verifier(settings)
    registry = ContextRegistry.from_wire(settings.contexts_wire)
    gateway = _build_gateway(settings)
    minter = StubCredentialMinter()  # CONN-IMPL-019 stub; real minter ships later.
    revocation_registry = RevocationRegistry()

    app = create_app(
        bootstrap_verifier=verifier,
        context_registry=registry,
        lease_gateway=gateway,
        credential_minter=minter,
        bound_triple=(settings.run_id, settings.step_id, settings.attempt),
        revocation_registry=revocation_registry,
    )

    # Best-effort: remove any stale socket file so uvicorn can bind. The
    # tmpfs mount is per-pod so cross-pod collisions cannot happen.
    socket_path = Path(settings.socket_path)
    try:
        if socket_path.exists():
            socket_path.unlink()
    except OSError as exc:
        print(
            f"[sidecar] failed to remove stale socket {socket_path}: {exc!s}",
            file=sys.stderr,
        )
        return 2

    socket_path.parent.mkdir(parents=True, exist_ok=True)

    uds_server = _build_uds_server(
        app=app,
        socket_path=socket_path,
        activity_gid=settings.activity_gid,
    )

    control_server: uvicorn.Server | None = None
    if settings.control_enabled:
        control_app = create_control_app(
            revocation_registry=revocation_registry,
            lease_gateway=gateway,
        )
        control_server = _build_control_server(app=control_app, settings=settings)

    try:
        asyncio.run(_run_servers(uds_server=uds_server, control_server=control_server))
    finally:
        # Release the lease gateway's httpx connection pool so we do
        # not leak sockets when the sidecar is signalled (or when a
        # test imports ``main`` and then tears the process down).
        with suppress(RuntimeError):  # pragma: no cover - loop already closed
            asyncio.run(gateway.aclose())
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised in container
    raise SystemExit(main())

"""Sidecar entry point (CONN-IMPL-019).

Reads :class:`~custos_sidecar.settings.Settings` from environment
variables, wires the production collaborators, and launches
``uvicorn`` bound to a Unix Domain Socket.

This module is intentionally thin so the bulk of the logic stays
under unit tests; the only thing here that is *not* unit-tested is
the actual ``uvicorn.run`` invocation, which is exercised by the
integration UDS harness (and in production by ARM).
"""

from __future__ import annotations

import asyncio
import os
import sys
from contextlib import suppress
from pathlib import Path

import uvicorn

from custos_sidecar import create_app
from custos_sidecar.auth import BootstrapTokenVerifier, BoundTriple
from custos_sidecar.context_registry import ContextRegistry
from custos_sidecar.credential_minter import StubCredentialMinter
from custos_sidecar.lease_gateway import LeaseGateway, LeaseGatewaySettings
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


def main() -> int:
    """Process entry point used by the ``custos-connector-sidecar`` script.

    Returns the exit code; ``uvicorn.run`` typically blocks forever so
    a non-zero return only happens on a setup error.
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

    app = create_app(
        bootstrap_verifier=verifier,
        context_registry=registry,
        lease_gateway=gateway,
        credential_minter=minter,
        bound_triple=(settings.run_id, settings.step_id, settings.attempt),
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

    # uvicorn binds the UDS itself inside ``Server.startup``; our
    # :class:`_PermFixingServer` subclass chmods/chowns the socket
    # immediately after the bind so the activity container can connect.
    config = uvicorn.Config(
        app=app,
        uds=str(socket_path),
        log_level="info",
        access_log=False,
        lifespan="on",
    )
    server = _PermFixingServer(
        config,
        socket_path=socket_path,
        activity_gid=settings.activity_gid,
    )
    try:
        server.run()
    finally:
        # Release the lease gateway's httpx connection pool so we do
        # not leak sockets when the sidecar is signalled (or when a
        # test imports ``main`` and then tears the process down).
        with suppress(RuntimeError):  # pragma: no cover - loop already closed
            asyncio.run(gateway.aclose())
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised in container
    raise SystemExit(main())

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

import os
import stat
import sys
from pathlib import Path

import uvicorn

from custos_sidecar import create_app
from custos_sidecar.auth import BootstrapTokenVerifier, BoundTriple
from custos_sidecar.context_registry import ContextRegistry
from custos_sidecar.credential_minter import StubCredentialMinter
from custos_sidecar.lease_gateway import LeaseGateway, LeaseGatewaySettings
from custos_sidecar.settings import Settings, load_settings


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

    # uvicorn binds the UDS itself; we set the socket mode immediately
    # after so the activity uid can connect but nothing else can.
    config = uvicorn.Config(
        app=app,
        uds=str(socket_path),
        log_level="info",
        access_log=False,
        lifespan="on",
    )
    server = uvicorn.Server(config)
    try:
        os.umask(0o077)
        server.run()
    finally:
        try:
            if socket_path.exists():
                # Tighten perms in case uvicorn created the socket world-readable.
                socket_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
        except OSError:
            pass
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised in container
    raise SystemExit(main())

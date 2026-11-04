"""Trigger Service ASGI entry point.

Thin wrapper that launches :func:`custos_trigger.create_app` under uvicorn.
This module exists so the package is launchable as ``python -m custos_trigger``
or via the ``custos-trigger-service`` console script declared in
``pyproject.toml``.
"""

from __future__ import annotations

import os


def main() -> None:
    """Launch the Trigger Service under uvicorn.

    Honours ``HOST`` / ``PORT`` env vars (defaults ``0.0.0.0`` / ``8080``)
    so the deployment manifests can override without code changes. The
    actual FastAPI application is constructed by
    :func:`custos_trigger.create_app`. Activated source receivers own bounded
    OTLP exporters for the server lifetime, including paused worker deployments.
    ``CUSTOS_HTTP_DRAIN_SECONDS`` bounds HTTP shutdown before owned workers drain;
    an unset value preserves uvicorn's legacy default.
    """
    # Imported lazily so ``python -m custos_trigger --help``-style probes
    # do not require uvicorn to be installed for non-runtime tooling.
    import uvicorn

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    from custos_trigger._runtime import receiver_telemetry
    from custos_trigger.settings import read_source_receivers_enabled

    with receiver_telemetry(enabled=read_source_receivers_enabled(os.environ)):
        uvicorn.run(
            "custos_trigger:create_app",
            host=host,
            port=port,
            factory=True,
            timeout_graceful_shutdown=(
                int(os.environ["CUSTOS_HTTP_DRAIN_SECONDS"])
                if "CUSTOS_HTTP_DRAIN_SECONDS" in os.environ
                else None
            ),
        )


if __name__ == "__main__":  # pragma: no cover - exercised via ``python -m``
    main()

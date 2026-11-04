"""Launch the Connector factory, bounding managed HTTP shutdown before owned worker cleanup."""

from __future__ import annotations

import os


def main() -> None:
    """Launch the Connector Service under uvicorn.

    Honours ``HOST`` / ``PORT`` env vars (defaults ``0.0.0.0`` / ``8080``)
    so the deployment manifests can override without code changes. The
    actual FastAPI application is constructed by :func:`custos_connector.create_app`.
    ``CUSTOS_HTTP_DRAIN_SECONDS`` optionally bounds the HTTP wait before lifespan
    shutdown; unset preserves the legacy uvicorn default.
    """
    # Imported lazily so ``python -m custos_connector --help``-style probes
    # do not require uvicorn to be installed for non-runtime tooling.
    import uvicorn

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run(
        "custos_connector:create_app",
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

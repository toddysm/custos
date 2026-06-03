"""Activity Runtime Manager ASGI entry point.

Thin wrapper that launches :func:`custos_arm.create_app` under uvicorn.
ARM-IMPL-001 wires the real factory; later ARM-IMPL-* tasks extend the
lifespan with configuration load, the Dapr worker, and collaborator
warm-up without touching this entry point.
"""

from __future__ import annotations

import os


def main() -> None:
    """Launch the Activity Runtime Manager under uvicorn.

    Honours ``HOST`` / ``PORT`` env vars (defaults ``0.0.0.0`` / ``8080``)
    so the deployment manifests can override without code changes. The
    FastAPI application is constructed by :func:`custos_arm.create_app`.
    """
    # Imported lazily so ``python -m custos_arm --help``-style probes do
    # not require uvicorn to be installed for non-runtime tooling.
    import uvicorn

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run(
        "custos_arm:create_app",
        host=host,
        port=port,
        factory=True,
    )


if __name__ == "__main__":  # pragma: no cover - exercised via ``python -m``
    main()

"""Connector Service ASGI entry point.

Thin wrapper that launches :func:`custos_connector.create_app` under
uvicorn. The application factory exposes only the ``/healthz`` and
``/readyz`` probes at Phase A; this module exists so the package is
launchable as ``python -m custos_connector`` or via the
``custos-connector-service`` console script declared in ``pyproject.toml``.
"""

from __future__ import annotations

import os


def main() -> None:
    """Launch the Connector Service under uvicorn.

    Honours ``HOST`` / ``PORT`` env vars (defaults ``0.0.0.0`` / ``8080``)
    so the deployment manifests can override without code changes. The
    actual FastAPI application is constructed by
    :func:`custos_connector.create_app`, which at Phase A only serves
    ``/healthz`` and ``/readyz``.
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
    )


if __name__ == "__main__":  # pragma: no cover - exercised via ``python -m``
    main()

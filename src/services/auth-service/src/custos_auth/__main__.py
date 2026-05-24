"""Auth Service ASGI entry point.

Thin wrapper that launches :func:`custos_auth.create_app` under uvicorn.
The application factory is a scaffold stub at AS-IMPL-001; this module
exists so the package is launchable as ``python -m custos_auth`` or via
the ``custos-auth-service`` console script declared in ``pyproject.toml``.
"""

from __future__ import annotations

import os


def main() -> None:
    """Launch the Auth Service under uvicorn.

    Honours ``HOST`` / ``PORT`` env vars (defaults ``0.0.0.0`` / ``8080``)
    so the deployment manifests can override without code changes.
    """
    # Imported lazily so ``python -m custos_auth --help``-style probes
    # do not require uvicorn to be installed for non-runtime tooling.
    import uvicorn

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run(
        "custos_auth:create_app",
        host=host,
        port=port,
        factory=True,
    )


if __name__ == "__main__":  # pragma: no cover - exercised via ``python -m``
    main()

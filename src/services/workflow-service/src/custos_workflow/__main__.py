"""Workflow Service ASGI entry point.

Thin wrapper that launches :func:`custos_workflow.create_app` under
uvicorn. WF-IMPL-015 wires the real factory; later WF-IMPL-* tasks
extend the lifespan with the Definition Compiler bootstrap and the
Catalog client warm-up without touching this entry point.
"""

from __future__ import annotations

import os


def main() -> None:
    """Launch the Workflow Service under uvicorn.

    Honours ``HOST`` / ``PORT`` env vars (defaults ``0.0.0.0`` / ``8080``)
    so the deployment manifests can override without code changes. The
    FastAPI application is constructed by
    :func:`custos_workflow.create_app`.
    """
    # Imported lazily so ``python -m custos_workflow --help``-style probes
    # do not require uvicorn to be installed for non-runtime tooling.
    import uvicorn

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    uvicorn.run(
        "custos_workflow:create_app",
        host=host,
        port=port,
        factory=True,
    )


if __name__ == "__main__":  # pragma: no cover - exercised via ``python -m``
    main()

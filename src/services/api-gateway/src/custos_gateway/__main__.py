"""``python -m custos_gateway`` entry point.

Thin CLI wrapper that boots the FastAPI application under uvicorn. The full
``create_app`` factory + lifespan wiring lands across AGW-IMPL-002 and
AGW-IMPL-016; this entry point keeps the module runnable from the first task so
the container image and the Helm probes have a stable command to invoke.

Honours the ``HOST`` / ``PORT`` environment variables (defaults ``0.0.0.0`` /
``8080``) so deployment manifests can override without code changes; explicit
``--host`` / ``--port`` flags take precedence over the environment.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="custos-api-gateway",
        description="Custos API Gateway (COMP-001) service host.",
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("HOST", "0.0.0.0"),
        help="Bind host for the HTTP listener (env HOST, default: 0.0.0.0).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("PORT", "8080")),
        help="Bind port for the HTTP listener (env PORT, default: 8080).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments and start the service host.

    Returns the process exit code. uvicorn is imported lazily and launched with
    the ``custos_gateway:create_app`` import string + ``factory=True`` so the
    app is constructed after uvicorn config is applied (matching the other
    services and keeping multi-worker / reload setups available).
    """
    args = _build_parser().parse_args(argv)

    import uvicorn

    uvicorn.run("custos_gateway:create_app", host=args.host, port=args.port, factory=True)
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised via the console script
    sys.exit(main())

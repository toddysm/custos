"""``python -m custos_gateway`` entry point.

Thin CLI wrapper that boots the FastAPI application under uvicorn. The full
``create_app`` factory + lifespan wiring lands across AGW-IMPL-002 and
AGW-IMPL-016; this entry point keeps the module runnable from the first task so
the container image and the Helm probes have a stable command to invoke.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="custos-api-gateway",
        description="Custos API Gateway (COMP-001) service host.",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Bind host for the HTTP listener (default: 0.0.0.0).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Bind port for the HTTP listener (default: 8080).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments and start the service host.

    Returns the process exit code. The uvicorn boot is imported lazily so
    ``--help`` works without the optional server extras installed.
    """
    args = _build_parser().parse_args(argv)

    import uvicorn

    from custos_gateway.app import create_app

    uvicorn.run(create_app(), host=args.host, port=args.port)
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised via the console script
    sys.exit(main())

"""``custos-migrate-job`` — Helm pre-install/pre-upgrade migration hook.

Thin wrapper around the SPL migration CLI (:mod:`custos_spl.migrations.cli`).
The job runs the forward-only ``migrate up`` against the Custos Postgres
cluster (the ``custos_state`` and ``custos_audit`` schemas live on the same
physical database, reached through a single libpq DSN). The SPL strict
migration policy is honoured end to end: if required schema revisions are still
missing after applying, the underlying CLI returns a non-zero exit code, the
job propagates it, and the Helm install/upgrade aborts before any component
starts against an unmigrated database.

Database connection
-------------------
The SPL Postgres adapters read the libpq DSN from ``CUSTOS_PG_DSN``. When the
job runs as the Helm hook it inherits the CloudNativePG-generated application
secret via ``envFrom`` (keys ``host`` / ``port`` / ``dbname`` / ``username`` /
``password`` / ``uri``). This module resolves ``CUSTOS_PG_DSN`` from the first
available of:

1. ``CUSTOS_PG_DSN`` — explicit override, used verbatim.
2. ``DATABASE_URL`` / ``uri`` — a ready-made libpq/postgres connection URL
   (the CloudNativePG secret exposes the connection string under ``uri``).

The resolved value is exported as ``CUSTOS_PG_DSN`` before delegating to the
SPL CLI. If none is present the job exits non-zero with an operator-actionable
message.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Mapping, Sequence

from custos_spl.migrations import cli as spl_cli

#: Env var the SPL Postgres adapters read for their libpq DSN.
DSN_ENV_VAR = "CUSTOS_PG_DSN"

#: Fallback env vars, in priority order, that may carry a ready-made
#: libpq/postgres connection URL (e.g. the CloudNativePG ``uri`` secret key).
_DSN_FALLBACK_VARS = ("DATABASE_URL", "uri")


def resolve_dsn(env: Mapping[str, str]) -> str | None:
    """Return the libpq DSN to use, or ``None`` if it cannot be resolved.

    An explicit ``CUSTOS_PG_DSN`` always wins; otherwise the first non-empty
    fallback connection URL (``DATABASE_URL`` then ``uri``) is used.
    """
    explicit = env.get(DSN_ENV_VAR)
    if explicit:
        return explicit
    for name in _DSN_FALLBACK_VARS:
        value = env.get(name)
        if value:
            return value
    return None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="custos-migrate-job",
        description=(
            "Custos migration Job — applies pending schema revisions "
            "(forward-only `migrate up`) as the Helm pre-install/pre-upgrade hook."
        ),
    )
    parser.add_argument(
        "--adapter",
        default=None,
        help="restrict the migration to a single SPL adapter entry-point name.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="report whether migration is needed without applying it.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Resolve the DSN and run ``custos migrate up``; return the exit code.

    Returns the SPL CLI exit code (``0`` success, ``1`` error or remaining
    revision gap, ``2`` gaps reported under ``--check``), or ``1`` if no DSN
    could be resolved.
    """
    args = _build_parser().parse_args(argv)

    dsn = resolve_dsn(os.environ)
    if dsn is None:
        print(
            f"error: no Postgres DSN available; set {DSN_ENV_VAR} (or provide a "
            "DATABASE_URL/uri) to a libpq connection string such as "
            "'postgresql://user:pw@host:5432/custos'.",
            file=sys.stderr,
        )
        return 1
    os.environ[DSN_ENV_VAR] = dsn

    up_argv: list[str] = ["up"]
    if args.adapter is not None:
        up_argv += ["--adapter", args.adapter]
    if args.check:
        up_argv.append("--check")

    return spl_cli.main(up_argv)


if __name__ == "__main__":  # pragma: no cover - exercised via the console script
    sys.exit(main())

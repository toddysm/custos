"""`custos-migrate up` CLI — operator command that gates platform startup.

The platform never auto-migrates (see `design/components/storage-provider-layer/design.md`
§ Migration Runner). An operator runs:

    custos-migrate up [--adapter NAME] [--check]

The CLI discovers candidate adapters through the `custos_spl.adapters`
entry-point group. Each entry point name is the human-friendly adapter
name (e.g. `postgres-metadata`) and the loaded object must be a
zero-arg factory returning an instance that satisfies
`MigrationCapable`. Adapter packages declare the entry point in their
`pyproject.toml`:

    [project.entry-points."custos_spl.adapters"]
    postgres-metadata = "custos_pg.adapters:make_metadata_adapter"

Subcommands:

  - `up`     — call `apply_pending()` on every selected adapter,
               forward-only. Exit code 0 on success.
  - `status` — print required vs declared revisions; exit code 0 if
               the platform would start, exit code 2 on gaps.

The CLI deliberately does not import any backend code itself; it only
walks the entry-point registry. Building a fresh CLI process avoids the
"platform auto-migrate" footgun and keeps the operator step explicit.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Iterable, Sequence
from importlib.metadata import EntryPoint, entry_points
from typing import TextIO

from custos_spl.errors import MigrationRequired
from custos_spl.migrations.runner import (
    MigrationCapable,
    check_revisions,
    required_revisions,
)

ENTRY_POINT_GROUP = "custos_spl.adapters"


def _discover_entry_points() -> list[EntryPoint]:
    """Return all entry points registered under our group."""
    return list(entry_points(group=ENTRY_POINT_GROUP))


def _load_adapter(ep: EntryPoint) -> MigrationCapable:
    """Load and instantiate one adapter from its entry point.

    Raises `RuntimeError` with operator-actionable context on failure
    so the CLI can print it and exit non-zero rather than leak a
    Python traceback.
    """
    try:
        factory = ep.load()
    except Exception as exc:
        raise RuntimeError(
            f"failed to import adapter entry point {ep.name!r} "
            f"({ep.value}): {exc}"
        ) from exc
    try:
        instance = factory()
    except Exception as exc:
        raise RuntimeError(
            f"adapter factory {ep.name!r} ({ep.value}) raised on "
            f"instantiation: {exc}"
        ) from exc
    if not isinstance(instance, MigrationCapable):
        raise RuntimeError(
            f"adapter {ep.name!r} returned object of type "
            f"{type(instance).__name__} which does not implement "
            f"the MigrationCapable protocol"
        )
    return instance


def _select(
    entries: Sequence[EntryPoint], requested: str | None
) -> list[EntryPoint]:
    """Filter entry points to those the operator selected.

    `None` means "every registered adapter". A literal name must match
    exactly one registered adapter or it is an error.
    """
    if requested is None:
        return list(entries)
    matching = [ep for ep in entries if ep.name == requested]
    if not matching:
        known = ", ".join(sorted(ep.name for ep in entries)) or "(none)"
        raise RuntimeError(
            f"no adapter named {requested!r} registered under "
            f"{ENTRY_POINT_GROUP!r}; known: {known}"
        )
    return matching


def _cmd_status(args: argparse.Namespace, stream: TextIO) -> int:
    """Print required + declared revisions; return 0 if clean, 2 on gaps."""
    entries = _select(_discover_entry_points(), args.adapter)
    adapters: list[MigrationCapable] = [_load_adapter(ep) for ep in entries]

    print("Platform required revisions:", file=stream)
    for name, rev in sorted(required_revisions().items()):
        print(f"  {name}: rev{rev}", file=stream)

    if not adapters:
        print("\nNo adapters discovered.", file=stream)
    else:
        print("\nAdapter declared revisions:", file=stream)
        for ep, adapter in zip(entries, adapters, strict=True):
            print(f"  {ep.name}:", file=stream)
            for iface, revs in sorted(adapter.declared_revisions.items()):
                pretty = ", ".join(f"rev{r}" for r in sorted(revs)) or "(none)"
                print(f"    {iface}: {pretty}", file=stream)

    try:
        check_revisions(adapters)
    except MigrationRequired as exc:
        print(f"\n{exc}", file=stream)
        print("Run `custos migrate up` to apply pending revisions.", file=stream)
        return 2
    print("\nAll required revisions are present.", file=stream)
    return 0


def _cmd_up(args: argparse.Namespace, stream: TextIO) -> int:
    """Apply pending migrations on each selected adapter."""
    entries = _select(_discover_entry_points(), args.adapter)
    if not entries:
        print(
            f"No adapters registered under {ENTRY_POINT_GROUP!r}; "
            "nothing to migrate.",
            file=stream,
        )
        return 1
    adapters: list[tuple[str, MigrationCapable]] = [
        (ep.name, _load_adapter(ep)) for ep in entries
    ]

    if args.check:
        try:
            check_revisions(a for _, a in adapters)
        except MigrationRequired as exc:
            print(str(exc), file=stream)
            return 2
        print("Already up to date.", file=stream)
        return 0

    async def run_all() -> int:
        for name, adapter in adapters:
            print(f"[{name}] applying pending migrations...", file=stream)
            applied = await adapter.apply_pending()
            if applied:
                for entry in applied:
                    print(f"[{name}]   {entry}", file=stream)
            else:
                print(f"[{name}]   (nothing to apply)", file=stream)
        # Post-condition: required revisions must now be satisfied.
        try:
            check_revisions(a for _, a in adapters)
        except MigrationRequired as exc:
            print(
                f"Migrations ran but gaps remain: {exc}",
                file=stream,
            )
            return 1
        print("Migration complete.", file=stream)
        return 0

    return asyncio.run(run_all())


def build_parser() -> argparse.ArgumentParser:
    """Construct the CLI parser. Exposed for tests."""
    parser = argparse.ArgumentParser(
        prog="custos migrate",
        description="Custos schema-migration admin CLI",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    up = sub.add_parser(
        "up",
        help="apply pending forward migrations",
        description=(
            "Apply pending forward migrations on each registered "
            "adapter. Forward-only; v1 has no down-migration path."
        ),
    )
    up.add_argument(
        "--adapter",
        help="restrict to a single adapter entry-point name",
        default=None,
    )
    up.add_argument(
        "--check",
        action="store_true",
        help="report whether migration is needed; do not apply",
    )
    up.set_defaults(func=_cmd_up)

    status = sub.add_parser(
        "status",
        help="show required and declared revisions",
        description=(
            "Print the platform's required revisions and each "
            "adapter's declared revisions. Exit 0 if the platform "
            "would start, exit 2 if `custos migrate up` is needed."
        ),
    )
    status.add_argument(
        "--adapter",
        help="restrict to a single adapter entry-point name",
        default=None,
    )
    status.set_defaults(func=_cmd_status)
    return parser


def main(argv: Iterable[str] | None = None, *, stream: TextIO | None = None) -> int:
    """Entry point for `python -m custos_spl.migrations` and the
    `custos` console script. Returns a Unix exit code.
    """
    out = stream if stream is not None else sys.stdout
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        return int(args.func(args, out))
    except RuntimeError as exc:
        print(f"error: {exc}", file=out)
        return 1


__all__ = ["ENTRY_POINT_GROUP", "build_parser", "main"]

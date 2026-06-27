"""Entry point for the copy-image activity (``python -m copy_image`` /
the ``custos-copy-image`` console script).

ARM runs the image's entry point with the sandbox already populated:
inputs at ``/custos/in`` and an empty ``/custos/out`` to write into. The
real contract handling lands in COPY-IMPL-002 (I/O envelope) and the copy
engine in COPY-IMPL-004. This scaffold entry point fails closed so an
accidentally-run image is never mistaken for a successful copy.
"""

from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    """Return a non-zero (permanent-failure) exit code until implemented."""
    sys.stderr.write("copy-image activity is not yet implemented (scaffold)\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

"""`python -m custos_spl.migrations` — admin CLI dispatcher.

Forwards to `custos_spl.migrations.cli.main`. Operators can also use
the `custos` console script (registered in `pyproject.toml`).
"""

from __future__ import annotations

import sys

from custos_spl.migrations.cli import main

if __name__ == "__main__":
    sys.exit(main())

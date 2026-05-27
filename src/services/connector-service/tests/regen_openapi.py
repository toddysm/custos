"""Regenerate ``openapi.json`` snapshot for CONN-IMPL-026.

Usage::

    cd src/services/connector-service
    python -m tests.regen_openapi > openapi.json

The script reuses the in-memory fakes the snapshot test wires up so
the resulting spec is identical (byte-for-byte) to what the test
generates.
"""

from __future__ import annotations

import json
import sys

from tests.test_openapi_snapshot import _generate_spec


def main() -> int:
    spec = _generate_spec()
    json.dump(spec, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

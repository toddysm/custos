"""Entry point invoked by ``docker run --rm -i <image> <hook>``.

The runtime calls the plugin with one positional argument naming the
hook to invoke (``bind``, ``listen`` or ``health``) and a single JSON
request envelope on stdin. The plugin writes the response envelope on
stdout. Any unexpected failure is converted into the
``unknown-plugin-error`` envelope so the runtime always sees a
well-formed response and never has to interpret a free-form stderr or
non-zero exit code.
"""

from __future__ import annotations

import json
import sys
from typing import Any

from .plugin import PluginError, handle


def _envelope_error(
    code: str, detail: str, *, data: dict[str, Any] | None = None
) -> dict[str, Any]:
    err: dict[str, Any] = {"code": code, "detail": detail}
    if data is not None:
        err["data"] = data
    return {"ok": False, "error": err}


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        sys.stdout.write(json.dumps(_envelope_error("invalid-response", "missing hook argument")))
        return 0
    hook = args[0]
    raw = sys.stdin.read()
    if not raw.strip():
        sys.stdout.write(json.dumps(_envelope_error("invalid-response", "empty request body")))
        return 0
    try:
        request = json.loads(raw)
    except json.JSONDecodeError as exc:
        sys.stdout.write(
            json.dumps(
                _envelope_error("invalid-response", f"request body is not valid JSON: {exc}")
            )
        )
        return 0
    if not isinstance(request, dict):
        sys.stdout.write(
            json.dumps(
                _envelope_error("invalid-response", "request envelope must be a JSON object")
            )
        )
        return 0
    try:
        response = handle(hook, request)
    except PluginError as exc:
        sys.stdout.write(json.dumps(_envelope_error(exc.code, exc.detail, data=exc.data)))
        return 0
    except Exception as exc:
        sys.stdout.write(
            json.dumps(
                _envelope_error("unknown-plugin-error", f"unhandled plugin exception: {exc}")
            )
        )
        return 0
    sys.stdout.write(json.dumps(response))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

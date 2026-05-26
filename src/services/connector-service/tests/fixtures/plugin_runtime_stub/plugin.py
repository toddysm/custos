from __future__ import annotations

import json
import sys


def _write(body: dict[str, object]) -> None:
    sys.stdout.write(json.dumps(body))
    sys.stdout.flush()


def _error(code: str, detail: str, data: dict[str, object] | None = None) -> None:
    error: dict[str, object] = {
        "code": code,
        "detail": detail,
    }
    if data is not None:
        error["data"] = data
    payload: dict[str, object] = {
        "ok": False,
        "error": error,
    }
    _write(payload)


def main() -> int:
    hook = sys.argv[1] if len(sys.argv) > 1 else ""
    payload = json.load(sys.stdin)
    manifest = payload.get("connector", {}).get("manifest", {})
    spec = manifest.get("spec", {}) if isinstance(manifest, dict) else {}
    expected_cursor = (
        spec.get("events", {}).get("pull", {}).get("cursorEncoding", "stub-cursor-v1")
        if isinstance(spec, dict)
        else "stub-cursor-v1"
    )
    target_config = payload.get("instance", {}).get("targetConfig", {})
    if not isinstance(target_config, dict):
        target_config = {}

    if hook == "bind":
        host = str(target_config.get("host", "registry.example.com"))
        _write(
            {
                "ok": True,
                "result": {
                    "endpoint": f"https://{host}",
                    "tokenTypeHint": "bearer",
                    "handle": {"binding": "stub"},
                    "extras": {"source": "stub-plugin"},
                },
            }
        )
        return 0

    if hook == "health":
        host = str(target_config.get("host", "registry.example.com"))
        if host == "down.example":
            _error("upstream-unreachable", "dial tcp timeout")
            return 0
        if host == "bad-auth.example":
            _error("upstream-unauthorized", "401 from upstream")
            return 0
        _write(
            {
                "ok": True,
                "result": {
                    "healthy": True,
                    "detail": "ok",
                    "checkedAt": "2026-01-01T00:00:00Z",
                    "extras": {"source": "stub-plugin"},
                },
            }
        )
        return 0

    if hook == "listen":
        hook_input = payload.get("input", {})
        cursor = hook_input.get("cursor") if isinstance(hook_input, dict) else None
        if isinstance(cursor, dict) and cursor.get("value") == "expired":
            _error("cursor-expired", "upstream cursor expired")
            return 0
        actual_encoding = cursor.get("encoding") if isinstance(cursor, dict) else None
        if actual_encoding != expected_cursor:
            _error(
                "cursor-encoding-mismatch",
                "cursor encoding mismatch",
                {
                    "persistedEncoding": actual_encoding,
                    "pluginEncoding": expected_cursor,
                },
            )
            return 0
        mode = hook_input.get("mode") if isinstance(hook_input, dict) else "pull"
        if mode == "push":
            _write(
                {
                    "ok": True,
                    "result": {
                        "events": [],
                        "nextCursor": None,
                        "receiverEndpoint": "https://receiver.example.com/hook",
                    },
                }
            )
            return 0
        _write(
            {
                "ok": True,
                "result": {
                    "events": [{"eventId": "evt-1", "type": "stub.event"}],
                    "nextCursor": {
                        "encoding": expected_cursor,
                        "value": "cursor-2",
                        "advancedAt": "2026-01-01T00:00:00Z",
                    },
                    "receiverEndpoint": None,
                },
            }
        )
        return 0

    _error("unknown-plugin-error", f"unsupported hook {hook!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

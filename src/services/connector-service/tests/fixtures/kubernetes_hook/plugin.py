"""One-shot Kubernetes fixture: an observable upstream effect and private bounded output."""

import json
import sys
import time
import urllib.request


def main() -> None:
    """Record one effect, then complete, fail, overflow or wait for controller cancellation."""
    body = json.load(sys.stdin)
    if "effectUrl" in body:
        with urllib.request.urlopen(
            urllib.request.Request(
                body["effectUrl"],
                data=b"one-effect",
                method="POST",
            ),
            timeout=5,
        ) as response:
            response.read()
    mode = body.get("mode", "echo")
    if mode == "wait":
        time.sleep(300)
    if mode == "overflow":
        sys.stdout.write("x" * (4 * 1024 * 1024 + 1))
        return
    print(body["secret"], file=sys.stderr)
    print(json.dumps({"hook": sys.argv[1], "echo": body["secret"]}), flush=True)
    sys.exit(7 if mode == "exit" else 0)


if __name__ == "__main__":
    main()

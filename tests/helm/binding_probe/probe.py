"""Configuration-only image: production loader and real Dapr Secret boundaries.

All service Deployments keep their real chart environments, ServiceAccounts,
mounts, sidecars and authorization. This image substitutes application business
logic, not the loader or Dapr, and explicitly does not certify receiver effects.
Hook Jobs without PORT are no-op fixtures; no database migration is claimed.
"""

from __future__ import annotations

import hashlib
import json
import os
import ssl
import time
import tracemalloc
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

from custos_callctx.execution import ExecutionIdentity, load_execution_bindings


def main() -> None:
    """Load once before HTTP readiness; hook fixtures exit without claiming migration work."""
    if "PORT" not in os.environ:
        return
    tracemalloc.start()
    started = time.perf_counter()
    registry = load_execution_bindings(os.environ)
    elapsed = time.perf_counter() - started
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    digest = os.environ.get("CUSTOS_EXECUTION_BINDINGS_SHA256")
    if registry is not None and digest is None:
        digest = hashlib.sha256(
            os.environ["CUSTOS_EXECUTION_BINDINGS"].encode()
        ).hexdigest()

    class Handler(BaseHTTPRequestHandler):
        """Expose only synthetic snapshot references, Boolean authorization and health results."""

        def log_message(self, format: str, *args: Any) -> None:
            """Keep request references and any error response bodies out of access logs."""

        def respond(self, value: Any, status: int = 200) -> None:
            """Send a bounded diagnostic response without echoing credentials or request bodies."""
            data = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:
            """Report the immutable startup snapshot or exercise real sidecar Secret authorization."""
            if self.path == "/dapr/subscribe":
                self.respond([])
            elif self.path == "/snapshot" and registry is not None:
                first = registry.identities[0]
                self.respond(
                    {
                        "sha256": digest,
                        "count": len(registry.bindings),
                        "selected": registry.select(first.workspace_id).to_dict(),
                        "parseSeconds": elapsed,
                        "parsePeakBytes": peak,
                    }
                )
            elif self.path == "/authority" and registry is not None:
                refs = [
                    registry.bindings[0].secret_ref,
                    registry.bindings[-1].secret_ref,
                    "custos-app",
                    "custos-redis-auth",
                    "binding-denied",
                ]
                outcomes = []
                for ref in refs:
                    request = Request(
                        f"http://127.0.0.1:3500/v1.0/secrets/kubernetes/{quote(ref, safe='')}",
                        headers={"dapr-api-token": os.environ["DAPR_API_TOKEN"]},
                    )
                    try:
                        with urlopen(request, timeout=5) as response:
                            outcomes.append(response.status)
                    except HTTPError as error:
                        outcomes.append(error.code)
                self.respond(outcomes)
            else:
                self.respond({"ready": True})

        def do_POST(self) -> None:
            """Resolve a synthetic pinned identity without exposing any Secret value."""
            if self.path != "/resolve" or registry is None:
                self.respond({}, 404)
                return
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 2048:
                self.respond({}, 400)
                return
            identity = ExecutionIdentity.from_dict(json.loads(self.rfile.read(length)))
            registry.resolve(identity)
            self.respond({"resolved": True})

    server = HTTPServer(("0.0.0.0", int(os.environ["PORT"])), Handler)
    if "ARM_TLS_CERT_FILE" in os.environ or "ARM_TLS_KEY_FILE" in os.environ:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(
            os.environ["ARM_TLS_CERT_FILE"], os.environ["ARM_TLS_KEY_FILE"]
        )
        server.socket = context.wrap_socket(server.socket, server_side=True)
    server.serve_forever()


if __name__ == "__main__":
    main()

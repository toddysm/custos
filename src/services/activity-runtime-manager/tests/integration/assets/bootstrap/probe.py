"""Verify deployed bootstrap/private-volume isolation without returning any private material."""

import http.client
import json
import socket
from pathlib import Path


def request(token: str) -> int:
    """Call the actual Sidecar over its shared Unix socket and return only the status."""
    connection = http.client.HTTPConnection("localhost", timeout=20)
    channel = socket.socket(socket.AF_UNIX)
    channel.settimeout(20)
    channel.connect("/custos/run/connector.sock")
    connection.sock = channel
    try:
        connection.request(
            "GET",
            "/v1/token?slot=source&purpose=oci.pull",
            headers={"Custos-Sidecar-Token": token},
        )
        return connection.getresponse().status
    finally:
        connection.close()


def main() -> None:
    """A valid proof reaches the unavailable broker; an invalid proof fails locally."""
    private_visible = any(
        Path("/custos/sidecar", name).exists() for name in ("sidecar-key", "runtime.json")
    )
    token = Path("/custos/in/sidecar-token").read_text()
    result = {
        "privateVisible": private_visible,
        "valid": request(token),
        "invalid": request("invalid"),
    }
    assert result == {"privateVisible": False, "valid": 503, "invalid": 401}, result
    Path("/custos/out/outputs.json").write_text(json.dumps(result))


if __name__ == "__main__":
    main()

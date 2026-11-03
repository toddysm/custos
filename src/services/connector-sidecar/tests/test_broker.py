"""Production broker Sidecar retains private file boundaries and actual provider response shapes."""

import json
import os
from pathlib import Path

import httpx
import pytest
from custos_callctx.bootstrap import BoundTriple, mint_bootstrap_token
from pydantic import ValidationError

from custos_sidecar.__main__ import main
from custos_sidecar.broker import RuntimeBundle, create_broker_app, load_runtime_bundle


def private_bundle(root: Path) -> Path:
    """Create the exact private files delivered by the runtime, with owner-only permissions."""
    bundle = root / "runtime.json"
    bundle.write_text(
        json.dumps(
            {
                "workspaceId": "ws",
                "runId": "run",
                "stepId": "step",
                "attempt": 1,
                "endpoint": "https://runtime.internal",
                "proof": "private-attempt-proof-" + "x" * 32,
            }
        )
    )
    os.chmod(bundle, 0o400)
    key = root / "sidecar-key"
    key.write_bytes(b"k" * 32)
    os.chmod(key, 0o400)
    return bundle


@pytest.mark.parametrize(
    "failure", [None, "tampered", "wrong-attempt", "missing", "duplicate", "upstream"]
)
async def test_production_broker_authentication_and_lossless_response(
    tmp_path: Path,
    failure: str | None,
) -> None:
    """Run real bootstrap verification and broker transport; no synthetic minter is present."""
    bundle, verifier = load_runtime_bundle(private_bundle(tmp_path))
    calls: list[dict[str, object]] = []

    def upstream(request: httpx.Request) -> httpx.Response:
        """Validate the actual private-hop request and return opaque provider material."""
        assert request.url == "https://runtime.internal/internal/v1/activity-credentials"
        assert request.headers["authorization"] == "Bearer " + bundle.proof.get_secret_value()
        calls.append(json.loads(request.content))
        if failure == "upstream":
            return httpx.Response(503, text=bundle.proof.get_secret_value())
        return httpx.Response(
            200,
            json={
                "schemaVersion": "2",
                "credential": {
                    "material": {"secret": {"nested": [1, "opaque"]}},
                    "expiresAt": None,
                },
                "lease": {
                    "leaseId": "lease",
                    "expiresAt": "2030-01-01T00:00:00Z",
                    "workspaceId": "ws",
                    "runId": "run",
                    "stepId": "step",
                    "attempt": 1,
                    "slot": "source",
                    "capability": "oci.pull",
                },
            },
        )

    token = mint_bootstrap_token(
        key=b"k" * 32,
        triple=BoundTriple("run", "step", 2 if failure == "wrong-attempt" else 1),
        ttl_sec=300,
    )
    if failure == "tampered":
        token = token[:-1] + ("a" if token[-1] != "a" else "b")
    headers = [] if failure == "missing" else [("Custos-Sidecar-Token", token)]
    if failure == "duplicate":
        headers *= 2
    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as http:
        app = create_broker_app(bundle, verifier, http_client=http)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://uds",
            ) as client,
        ):
            response = await client.get("/v1/token?slot=source&purpose=oci.pull", headers=headers)
    if failure in {"missing", "duplicate", "tampered", "wrong-attempt"}:
        assert response.status_code == 401
        assert not calls
    elif failure == "upstream":
        assert response.status_code == 503
        assert bundle.proof.get_secret_value() not in response.text
    else:
        assert response.status_code == 200
        assert response.json()["credential"] == {
            "material": {"secret": {"nested": [1, "opaque"]}},
            "expiresAt": None,
        }
        assert calls == [
            {
                "workspaceId": "ws",
                "runId": "run",
                "stepId": "step",
                "attempt": 1,
                "operation": "issue",
                "slot": "source",
                "capability": "oci.pull",
            }
        ]


@pytest.mark.parametrize("unsafe", ["readable", "symlink", "short-key", "oversized"])
def test_private_bundle_rejects_unsafe_delivery(tmp_path: Path, unsafe: str) -> None:
    """A missing/private-boundary failure cannot trigger a fallback to legacy synthetic issuance."""
    path = private_bundle(tmp_path)
    if unsafe == "readable":
        os.chmod(path, 0o444)
    elif unsafe == "symlink":
        target = tmp_path / "symlink"
        target.symlink_to(path)
        path = target
    elif unsafe == "short-key":
        key = tmp_path / "sidecar-key"
        os.chmod(key, 0o600)
        key.write_bytes(b"short")
        os.chmod(key, 0o400)
    else:
        os.chmod(path, 0o600)
        path.write_bytes(b"x" * (16 * 1024 + 1))
        os.chmod(path, 0o400)
    with pytest.raises((OSError, ValueError)):
        load_runtime_bundle(path)


def test_entry_point_fails_without_private_broker_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The actual production main never chooses StubCredentialMinter."""
    monkeypatch.setenv("CUSTOS_SIDECAR_RUNTIME_CONFIG", str(tmp_path / "missing"))
    assert main() == 2
    assert "private broker configuration" in capsys.readouterr().err


@pytest.mark.parametrize(
    "endpoint", ["http://runtime", "https://user:pass@runtime", "https://runtime/path"]
)
def test_runtime_bundle_requires_private_tls_origin(endpoint: str) -> None:
    """The sandbox proof cannot follow a plain, credential-bearing or path-selected destination."""
    with pytest.raises(ValidationError):
        RuntimeBundle.model_validate(
            {
                "workspaceId": "ws",
                "runId": "run",
                "stepId": "step",
                "attempt": 1,
                "endpoint": endpoint,
                "proof": "x" * 43,
            }
        )

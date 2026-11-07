"""Verify fixture TLS, isolated namespaces and fixed diagnostics without Docker or a cluster."""

import importlib.util
import json
import ssl
import subprocess
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest
import test_execution_bundle_cluster as cluster
import yaml
from test_execution_bundle_cluster import (
    DIAGNOSTIC_MARKERS,
    diagnostic_markers,
    settings,
)
from test_receiver_runtime import CHART

SPEC = importlib.util.spec_from_file_location(
    "binding_probe", Path(__file__).parent / "binding_probe/probe.py"
)
assert SPEC is not None and SPEC.loader is not None
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


@pytest.mark.parametrize("tls", [False, True])
def test_probe_preserves_managed_application_protocol(
    tls: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Managed Runtime TLS uses an actual matching keypair; other consumers keep plaintext."""
    monkeypatch.setenv("PORT", "0")
    monkeypatch.setenv("CUSTOS_EXECUTION_IDENTITY_ENABLED", "false")
    for key in ("ARM_TLS_CERT_FILE", "ARM_TLS_KEY_FILE"):
        monkeypatch.delenv(key, raising=False)
    if tls:
        certificate, key_path = tmp_path / "tls.crt", tmp_path / "tls.key"
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "2",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost,IP:127.0.0.1",
                "-out",
                str(certificate),
                "-keyout",
                str(key_path),
            ],
            capture_output=True,
            check=True,
        )
        monkeypatch.setenv("ARM_TLS_CERT_FILE", str(certificate))
        monkeypatch.setenv("ARM_TLS_KEY_FILE", str(key_path))
    servers = []

    def server(
        address: tuple[str, int], handler: type[BaseHTTPRequestHandler]
    ) -> HTTPServer:
        """Bind an isolated loopback socket but return immediately instead of running a daemon."""
        assert address == ("0.0.0.0", 0)
        result = HTTPServer(("127.0.0.1", 0), handler)
        servers.append(result)
        monkeypatch.setattr(result, "serve_forever", lambda: None)
        return result

    monkeypatch.setattr(probe, "HTTPServer", server)
    try:
        probe.main()
        assert len(servers) == 1
        assert isinstance(servers[0].socket, ssl.SSLSocket) is tls
        if tls:
            assert isinstance(servers[0].socket, ssl.SSLSocket)
            assert servers[0].socket.context.minimum_version == ssl.TLSVersion.TLSv1_2
    finally:
        for value in servers:
            value.server_close()


def test_migration_fixture_does_not_open_a_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A no-PORT hook exits without pretending to perform application or migration work."""
    monkeypatch.delenv("PORT", raising=False)

    def forbidden(*args: object) -> None:
        """Reject a hook fixture that accidentally starts serving."""
        raise AssertionError("hook must not bind an application port")

    monkeypatch.setattr(probe, "HTTPServer", forbidden)
    probe.main()


def test_lifecycle_fixtures_isolate_all_namespaces_and_preserve_database_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep disjoint Namespaces, actual DB authority and backend labels selected by managed policies."""
    namespaces = []
    backends = []

    def kube(namespace: str, *arguments: str, **options: object) -> str:
        """Capture backend Pod labels without contacting a cluster."""
        assert arguments[0] in {"create", "expose", "rollout"}
        if arguments[0] == "create":
            payload = options["payload"]
            assert isinstance(payload, dict)
            backends.append(payload)
        return ""

    monkeypatch.setattr(cluster, "kube", kube)
    for namespace in ("binding-files", "binding-bridge"):
        config = settings(namespace, chart_owned=True)
        values = tmp_path / f"{namespace}.yaml"
        values.write_text(yaml.safe_dump(config))
        result = subprocess.run(
            [
                "helm",
                "template",
                "custos",
                str(CHART),
                "--namespace",
                namespace,
                "--kube-version",
                "1.31.0",
                "-f",
                str(values),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        documents = [
            document for document in yaml.safe_load_all(result.stdout) if document
        ]
        owned = {
            document["metadata"]["name"]
            for document in documents
            if document["kind"] == "Namespace"
        }
        assert owned == {
            namespace,
            namespace + "-activities",
            namespace + "-hooks",
            namespace + "-credentials",
        }
        namespaces.append(owned)
        state_store = next(
            document
            for document in documents
            if document["kind"] == "Component"
            and document["spec"]["type"] == "state.postgresql"
        )
        assert (
            state_store["spec"]["metadata"][0]["secretKeyRef"]["name"] == "custos-app"
        )
        reader = next(
            document
            for document in documents
            if document["kind"] == "Role"
            and document["metadata"]["name"] == "custos-dapr-secret-reader"
        )
        assert "custos-app" in reader["rules"][0]["resourceNames"]
        for resource, port, labels in (
            ("redis", 6379, {"app.kubernetes.io/name": "redis"}),
            (
                "postgres",
                5432,
                config["global"]["networkPolicies"]["postgresPodSelector"],
            ),
        ):
            cluster.backend(
                namespace,
                f"binding-{resource}",
                port,
                {"name": resource},
                labels=labels,
            )
            policy = next(
                document
                for document in documents
                if document["kind"] == "NetworkPolicy"
                and document["metadata"]["name"] == f"custos-receivers-{resource}"
            )
            pod_labels = backends[-1]["spec"]["template"]["metadata"]["labels"]
            assert all(
                pod_labels.get(key) == value
                for key, value in policy["spec"]["podSelector"]["matchLabels"].items()
            )
    assert namespaces[0].isdisjoint(namespaces[1])


def test_workload_diagnostics_emit_only_fixed_categories() -> None:
    """Credential-looking or arbitrary log text never appears in the bounded diagnostic output."""
    log = "canary-do-not-print: password authentication failed for state.postgresql"
    markers = diagnostic_markers(log)
    assert markers == [
        "state.postgresql",
        "authentication failed",
        "password authentication failed",
    ]
    assert all(marker in DIAGNOSTIC_MARKERS for marker in markers)
    assert "canary-do-not-print" not in repr(markers)
    assert diagnostic_markers("an unknown message or credential") == []


@pytest.mark.parametrize("retained", [False, True])
def test_release_retirement_waits_for_actual_workloads(
    monkeypatch: pytest.MonkeyPatch, retained: bool
) -> None:
    """Keep cleanup behind real Pod/controller retirement, with a bounded ten-minute wait."""
    calls: list[tuple[str, ...]] = []
    delays: list[int] = []

    def kube(namespace: str, *arguments: str) -> str:
        """Retain a terminating release Pod until its actual inventory is empty."""
        assert namespace == "binding-files"
        calls.append(arguments)
        items = [{"metadata": {"deletionTimestamp": "2026-01-01T00:00:00Z"}}]
        if not retained and len(calls) > 1:
            items = []
        return json.dumps({"items": items})

    monkeypatch.setattr(cluster, "kube", kube)
    monkeypatch.setattr(time, "sleep", delays.append)
    if retained:
        with pytest.raises(pytest.fail.Exception, match="did not retire"):
            cluster.await_release_retirement("binding-files")
        assert len(calls) == 120
        assert sum(delays) == 600
    else:
        cluster.await_release_retirement("binding-files")
        assert len(calls) == 2
        assert delays == [5]
    assert all(
        call
        == (
            "get",
            "pods,deployments,replicasets,statefulsets,daemonsets,jobs,cronjobs",
            "-l",
            "app.kubernetes.io/instance=custos",
            "-o",
            "json",
        )
        for call in calls
    )

"""All-profile receiver activation is explicit, reference-only and fail-closed."""

import copy
import json
import re
import runpy
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT / "deploy/helm/custos"
PROFILES = ("connected-eval", "connected-ha", "airgapped-eval", "airgapped-ha")
SERVICES = (
    "api-gateway",
    "auth-service",
    "catalog-service",
    "connector-service",
    "workflow-service",
    "trigger-service",
    "activity-runtime-manager",
    "observability-audit-service",
)


def assert_unique_keys(node: yaml.Node) -> None:
    """Reject duplicate or non-string Kubernetes mapping keys before safe_load can hide them."""
    if isinstance(node, yaml.MappingNode):
        names = [key.value for key, _ in node.value if isinstance(key, yaml.ScalarNode)]
        assert len(names) == len(node.value) and len(names) == len(set(names))
        assert all(key.tag == "tag:yaml.org,2002:str" for key, _ in node.value)
        for _, value in node.value:
            assert_unique_keys(value)
    elif isinstance(node, yaml.SequenceNode):
        for value in node.value:
            assert_unique_keys(value)


def enabled_values() -> dict[str, Any]:
    """Provide only synthetic deployment references; no token, signing or TLS material."""
    digest = "@sha256:" + "a" * 64
    return {
        "global": {
            "imageTag": "tested-receiver-release",
            "networkPolicies": {"enabled": True},
            "receiverRuntime": {
                "enabled": True,
                "admissionEnabled": True,
                "deploymentId": "unit-deployment",
                "apiTokenSecret": "dapr-api",
                "appTokenSecrets": {name: name + "-app-token" for name in SERVICES},
                "authSigningKeyRef": "auth-signing",
                "sandboxNamespace": "activity-sandboxes",
                "tlsSecret": "runtime-tls",
                "artifactClaim": "shared-artifacts",
                "telemetryEndpoint": "http://otel.monitoring.svc:4318",
                "sidecarImage": "registry.test/sidecar" + digest,
                "ioBridgeImage": "registry.test/bridge" + digest,
                "kubernetesApiCIDRs": ["10.96.0.1/32"],
                "executionBindings": {
                    "bindings": [
                        {
                            "workspaceId": "ws",
                            "principalId": "account",
                            "bindingId": "v1",
                            "secretRef": "execution-ws",
                            "secretKey": "token",
                        }
                    ],
                    "defaults": {"ws": "v1"},
                },
            },
        },
        "connector-service": {
            "hookRuntime": {
                "enabled": True,
                "namespace": "hook-sandboxes",
                "helperImage": "registry.test/helper" + digest,
                "registryUrl": "https://registry.test",
            },
        },
    }


def render(
    profile: str,
    values: dict[str, Any],
    *,
    kube_version: str | None = None,
    namespace: str = "controllers",
) -> subprocess.CompletedProcess[str]:
    """Render the real chart with a namespace distinct from sandbox and hook execution."""
    command = [
        "helm",
        "template",
        "custos",
        str(CHART),
        "--namespace",
        namespace,
        "-f",
        str(CHART / f"values-{profile}.yaml"),
        "-f",
        "-",
    ]
    if kube_version is not None:
        command.extend(["--kube-version", kube_version])
    return subprocess.run(
        command,
        check=False,
        input=yaml.safe_dump(values),
        text=True,
        capture_output=True,
    )


@pytest.mark.parametrize("profile", PROFILES)
def test_enabled_profiles_wire_actual_receiver_prerequisites(profile: str) -> None:
    """The same opt-in projection carries exact gates, private trust, refs and shared state."""
    result = render(profile, enabled_values())
    assert result.returncode == 0, result.stderr
    for node in yaml.compose_all(result.stdout):
        assert_unique_keys(node)
    documents = [value for value in yaml.safe_load_all(result.stdout) if value]
    deployments = {
        value["metadata"]["name"]: value
        for value in documents
        if value["kind"] == "Deployment"
    }
    configmaps = {
        value["metadata"]["name"]: value["data"]
        for value in documents
        if value["kind"] == "ConfigMap"
    }
    for service in SERVICES:
        pod = deployments["custos-" + service]["spec"]["template"]
        env = pod["spec"]["containers"][0]["env"]
        names = [entry["name"] for entry in env]
        assert len(names) == len(set(names))
        values = {entry["name"]: entry for entry in env}
        assert values["ENVIRONMENT"]["value"] == "production"
        assert (
            values["APP_API_TOKEN"]["valueFrom"]["secretKeyRef"]["name"]
            == service + "-app-token"
        )
        assert (
            pod["metadata"]["annotations"]["dapr.io/app-token-secret"]
            == service + "-app-token"
        )
        assert pod["spec"]["terminationGracePeriodSeconds"] == 420
        annotations = pod["metadata"]["annotations"]
        assert annotations["dapr.io/graceful-shutdown-seconds"] == "5"
        if service in {
            "trigger-service",
            "connector-service",
            "workflow-service",
            "activity-runtime-manager",
        }:
            assert annotations["dapr.io/block-shutdown-duration"] == "390s"
            assert annotations["dapr.io/enable-app-health-check"] == "false"
            assert pod["spec"]["terminationGracePeriodSeconds"] > 390 + 5
        if service in {
            "trigger-service",
            "connector-service",
            "activity-runtime-manager",
        }:
            environment = dict(configmaps["custos-" + service])
            for entry in env:
                environment[entry["name"]] = (
                    entry["value"]
                    if "value" in entry
                    else "postgresql://unit:unit@localhost/unit"
                    if entry["valueFrom"]["secretKeyRef"]["key"] == "uri"
                    else "unit-api-token"
                )
            package = {
                "trigger-service": "custos_trigger/settings.py",
                "connector-service": "custos_connector/settings.py",
                "activity-runtime-manager": "custos_arm/config.py",
            }[service]
            module = runpy.run_path(
                str(ROOT / "src/services" / service / "src" / package)
            )
            parsed = module["load_settings"](environment)
            assert parsed.environment == "production"
            if service == "connector-service":
                assert parsed.pull_worker_enabled and parsed.durable_pull_enabled
            elif service == "activity-runtime-manager":
                assert parsed.runtime_enabled and parsed.artifact_store == "csi"
                assert parsed.sidecar_ca_file == "/custos/tls/ca.crt"
            else:
                assert module["read_receiver_worker_settings"](environment).enabled
    trigger = deployments["custos-trigger-service"]["spec"]["template"]["spec"][
        "containers"
    ][0]
    env = {entry["name"]: entry.get("value") for entry in trigger["env"]}
    assert env["TRIGGER_RECEIVER_WORKERS_ENABLED"] == "true"
    assert env["TRIGGER_SOURCE_RECEIVERS_ENABLED"] == "true"
    assert env["TRIGGER_WORKFLOW_NAMESPACE"] == "controllers"
    assert json.loads(env["CUSTOS_EXECUTION_BINDINGS"])["defaults"] == {"ws": "v1"}
    runtime = deployments["custos-activity-runtime-manager"]["spec"]["template"]
    container = runtime["spec"]["containers"][0]
    assert container["command"] == ["python", "-m", "custos_arm"]
    runtime_env = {entry["name"]: entry.get("value") for entry in container["env"]}
    assert runtime_env["ARM_TLS_CERT_FILE"] == "/custos/tls/tls.crt"
    assert runtime_env["ARM_TLS_KEY_FILE"] == "/custos/tls/tls.key"
    assert runtime_env["ARM_METRICS_PORT"] == "8090"
    assert container["readinessProbe"]["httpGet"]["scheme"] == "HTTPS"
    assert runtime["metadata"]["annotations"]["dapr.io/app-protocol"] == "https"
    assert any(
        v.get("persistentVolumeClaim", {}).get("claimName") == "shared-artifacts"
        for v in runtime["spec"]["volumes"]
    )
    configs = [value for value in documents if value["kind"] == "Configuration"]
    assert len(configs) == 8
    assert all(
        value["spec"]["accessControl"]["defaultAction"] == "deny" for value in configs
    )
    assert all(
        policy["namespace"] == "controllers"
        for value in configs
        for policy in value["spec"]["accessControl"]["policies"]
    )
    connector_config = next(
        value
        for value in configs
        if value["metadata"]["name"] == "custos-receivers-connector-service"
    )
    assert "catalog-service" in {
        policy["appId"]
        for policy in connector_config["spec"]["accessControl"]["policies"]
    }
    catalog_env = {
        entry["name"]: entry.get("value")
        for entry in deployments["custos-catalog-service"]["spec"]["template"]["spec"][
            "containers"
        ][0]["env"]
    }
    assert catalog_env["CAT_CONNECTOR_ENDPOINT"] == (
        "http://127.0.0.1:3500/v1.0/invoke/connector-service/method"
    )
    subscriptions = {
        value["spec"]["topic"]: value
        for value in documents
        if value["kind"] == "Subscription"
        and value.get("scopes") == ["trigger-service"]
    }
    assert {
        name: value["spec"]["routes"]["default"]
        for name, value in subscriptions.items()
    } == {
        "custos.workflow.events": "/internal/events/workflow",
        "custos.connector.events": "/internal/events/connectors",
        "custos.triggers.normalized": "/internal/events/normalized",
    }
    pubsub = next(
        value
        for value in documents
        if value["kind"] == "Component" and value["metadata"]["name"] == "custos-pubsub"
    )
    metadata = {
        entry["name"]: entry.get("value") for entry in pubsub["spec"]["metadata"]
    }
    assert "custos.triggers.normalized" in metadata["protectedTopics"]
    assert "trigger-service=custos.triggers.normalized" in metadata["publishingScopes"]
    assert env["CUSTOS_EXECUTION_AUTH_APP_ID"] == "custos-auth"
    policies = [
        value
        for value in documents
        if value["kind"] == "NetworkPolicy"
        and value["metadata"].get("namespace") == "controllers"
        and value["metadata"]["name"].startswith("custos-receivers-")
    ]
    assert len(policies) == 13
    assert all(
        port.get("port") not in {3500, 50001}
        for policy in policies
        for rule in policy["spec"].get("ingress", [])
        for port in rule.get("ports", [])
    )


@pytest.mark.parametrize("profile", PROFILES)
def test_backend_and_preupgrade_job_policy_paths(profile: str) -> None:
    """Actual Jobs retain database/API paths before an upgrade reaches regular resources."""
    result = render(profile, enabled_values())
    assert result.returncode == 0, result.stderr
    documents = [value for value in yaml.safe_load_all(result.stdout) if value]
    policies = {
        value["metadata"]["name"]: value
        for value in documents
        if value["kind"] == "NetworkPolicy"
    }
    jobs = {
        value["metadata"]["name"]: value
        for value in documents
        if value["kind"] == "Job"
    }
    for suffix in ("database-jobs", "injector-gate", "database-ingress"):
        annotations = policies[f"custos-receivers-{suffix}"]["metadata"]["annotations"]
        assert annotations["helm.sh/hook"] == "pre-install,pre-upgrade"
        assert int(annotations["helm.sh/hook-weight"]) < min(
            int(jobs[name]["metadata"]["annotations"]["helm.sh/hook-weight"])
            for name in ("custos-migrate", "custos-dapr-injector-wait")
        )
        assert annotations["helm.sh/hook-delete-policy"] == "before-hook-creation"
    database_jobs = policies["custos-receivers-database-jobs"]["spec"]
    selected = database_jobs["podSelector"]["matchExpressions"][0]["values"]
    for name in ("custos-migrate", "custos-bootstrap"):
        labels = jobs[name]["spec"]["template"]["metadata"]["labels"]
        assert labels["app.kubernetes.io/component"] in selected
        assert labels["app.kubernetes.io/instance"] == "custos"
    pg_selector = {"cnpg.io/cluster": "custos"}
    assert database_jobs["egress"][1]["to"] == [
        {"podSelector": {"matchLabels": pg_selector}}
    ]
    assert database_jobs["egress"][1]["ports"] == [{"protocol": "TCP", "port": 5432}]
    ingress = policies["custos-receivers-database-ingress"]["spec"]
    assert ingress["podSelector"]["matchLabels"] == pg_selector
    assert (
        ingress["ingress"][0]["from"][0]["podSelector"] == database_jobs["podSelector"]
    )
    gate = policies["custos-receivers-injector-gate"]["spec"]
    assert gate["podSelector"]["matchExpressions"][0]["values"] == [
        "dapr-injector-wait"
    ]
    assert gate["egress"][1]["to"] == [{"ipBlock": {"cidr": "10.96.0.1/32"}}]
    for suffix, port in (("postgres", 5432), ("redis", 6379)):
        backend = policies[f"custos-receivers-{suffix}"]["spec"]
        assert backend["ingress"][0]["from"][0]["podSelector"]["matchExpressions"][0][
            "values"
        ] == list(SERVICES)
        assert backend["ingress"][0]["ports"] == [{"protocol": "TCP", "port": port}]


@pytest.mark.parametrize(
    ("drain", "grace", "accepted"),
    [
        (330, 360, False),
        (330, 395, False),
        (330, 396, True),
        (700, 765, False),
        (700, 766, True),
    ],
)
def test_shutdown_reserves_owned_cleanup_and_dapr_time(
    drain: int,
    grace: int,
    accepted: bool,
) -> None:
    """Reject the exact insufficient boundary, not just one previously unsafe default."""
    values = enabled_values()
    values["global"]["receiverRuntime"].update(
        drainSeconds=drain,
        terminationGraceSeconds=grace,
    )
    result = render("connected-ha", values)
    assert (result.returncode == 0) is accepted, result.stderr
    if not accepted:
        assert "drainSeconds+65" in result.stderr
        return
    documents = [value for value in yaml.safe_load_all(result.stdout) if value]
    connector = next(
        value
        for value in documents
        if value["kind"] == "Deployment"
        and value["metadata"]["name"] == "custos-connector-service"
    )
    pod = connector["spec"]["template"]
    assert (
        pod["metadata"]["annotations"]["dapr.io/block-shutdown-duration"]
        == f"{drain + 60}s"
    )
    assert pod["spec"]["terminationGracePeriodSeconds"] > drain + 15 + 30 + 15 + 5


@pytest.mark.parametrize(
    "missing",
    [
        "apiTokenSecret",
        "tlsSecret",
        "artifactClaim",
        "authSigningKeyRef",
        "executionBindings",
        "kubernetesApiCIDRs",
    ],
)
def test_partial_receiver_configuration_fails_render(missing: str) -> None:
    """Missing authority/storage/transport dependencies cannot render an apparently enabled rollout."""
    values = enabled_values()
    values["global"]["receiverRuntime"][missing] = (
        {} if missing == "executionBindings" else ""
    )
    result = render("connected-eval", values)
    assert result.returncode != 0


def test_disabled_rollout_retains_legacy_profile(
    rendered: dict[str, list[dict[str, Any]]],
) -> None:
    """No new authority, worker flags or sandbox resources are activated by ordinary profile renders."""
    for documents in rendered.values():
        for value in documents:
            if value["kind"] != "Deployment":
                continue
            pod = value["spec"]["template"]
            assert "dapr.io/app-token-secret" not in pod["metadata"]["annotations"]
            for container in pod["spec"]["containers"]:
                assert not any(
                    entry["name"] == "TRIGGER_RECEIVER_WORKERS_ENABLED"
                    for entry in container.get("env", [])
                )


def test_shared_app_secret_is_rejected() -> None:
    """Knowing one service's app token must not grant direct app-port impersonation of another."""
    values = copy.deepcopy(enabled_values())
    secrets = values["global"]["receiverRuntime"]["appTokenSecrets"]
    secrets["trigger-service"] = secrets["auth-service"]
    result = render("connected-eval", values)
    assert result.returncode != 0 and "distinct app tokens" in result.stderr


def test_pause_preserves_authority_and_accepted_delivery() -> None:
    """Pausing receivers stops new upstream admission without downgrading accepted-work identity."""
    values = enabled_values()
    values["global"]["receiverRuntime"]["admissionEnabled"] = False
    result = render("connected-ha", values)
    assert result.returncode == 0, result.stderr
    documents = [value for value in yaml.safe_load_all(result.stdout) if value]
    deployments = {
        value["metadata"]["name"]: value
        for value in documents
        if value["kind"] == "Deployment"
    }
    for service, gate in (
        ("trigger-service", "TRIGGER_RECEIVER_WORKERS_ENABLED"),
        ("connector-service", "CONN_PULL_WORKER_ENABLED"),
    ):
        pod = deployments["custos-" + service]["spec"]["template"]
        env = {entry["name"]: entry for entry in pod["spec"]["containers"][0]["env"]}
        assert env[gate]["value"] == "false"
        assert "APP_API_TOKEN" in env
        if service == "trigger-service":
            assert env["TRIGGER_SOURCE_RECEIVERS_ENABLED"]["value"] == "true"
            assert env["CUSTOS_EXECUTION_IDENTITY_ENABLED"]["value"] == "true"
            assert env["TRIGGER_WEBHOOK_RECEIVER_ENABLED"]["value"] == "false"


@pytest.mark.parametrize("tag", ["dev", "latest"])
def test_activation_rejects_unqualified_controller_images(tag: str) -> None:
    """Production activation cannot accidentally keep cached development controller binaries."""
    values = enabled_values()
    values["global"]["imageTag"] = tag
    result = render("connected-eval", values)
    assert result.returncode != 0 and "tested controller image tags" in result.stderr


def test_native_sidecar_version_is_an_activation_prerequisite() -> None:
    """Keep legacy chart compatibility while refusing unsupported native-sidecar activation."""
    result = render("connected-eval", enabled_values(), kube_version="1.28.10")
    assert result.returncode != 0 and "Kubernetes >=1.29" in result.stderr


def test_admission_pause_does_not_restart_the_execution_control_plane() -> None:
    """A receiver pause must not roll runtime/Auth/Workflow Pods and interrupt accepted activities."""
    snapshots = []
    for active in (True, False):
        values = enabled_values()
        values["global"]["receiverRuntime"]["admissionEnabled"] = active
        result = render("connected-ha", values)
        assert result.returncode == 0, result.stderr
        snapshots.append(
            {
                value["metadata"]["name"]: value["spec"]["template"]
                for value in yaml.safe_load_all(result.stdout)
                if value and value["kind"] == "Deployment"
            }
        )
    changed = {
        name for name in snapshots[0] if snapshots[0][name] != snapshots[1][name]
    }
    assert changed == {"custos-trigger-service", "custos-connector-service"}


def test_static_and_operator_scrapes_select_only_the_metrics_listener() -> None:
    """Both scrape surfaces target plaintext operational metrics, never the private TLS app port."""
    values = enabled_values()
    values["activity-runtime-manager"] = {"serviceMonitor": {"enabled": True}}
    result = render("connected-ha", values)
    assert result.returncode == 0, result.stderr
    documents = [value for value in yaml.safe_load_all(result.stdout) if value]
    monitor = next(
        value
        for value in documents
        if value["kind"] == "ServiceMonitor"
        and value["metadata"]["name"] == "custos-activity-runtime-manager"
    )
    endpoint = monitor["spec"]["endpoints"][0]
    assert endpoint["port"] == "metrics" and endpoint.get("scheme", "http") == "http"
    service = next(
        value
        for value in documents
        if value["kind"] == "Service"
        and value["metadata"]["name"] == "custos-activity-runtime-manager"
    )
    assert {(port["name"], port["port"]) for port in service["spec"]["ports"]} == {
        ("https", 8080),
        ("metrics", 8090),
    }
    chart = yaml.safe_load((CHART / "values.yaml").read_text())
    installer = (ROOT / "scripts/install-prereqs.sh").read_text()
    source = installer.split("<<'PROMETHEUS_SCRAPE_VALUES'\n", 1)[1].split(
        "\nPROMETHEUS_SCRAPE_VALUES",
        1,
    )[0]
    snippets = [
        chart["prometheus"]["extraScrapeConfigs"],
        yaml.safe_load(source)["extraScrapeConfigs"],
    ]
    for snippet in snippets:
        jobs = yaml.safe_load(
            snippet.replace("{{ .Release.Namespace }}", "controllers")
        )
        pattern = jobs[0]["relabel_configs"][0]["regex"]
        assert re.fullmatch(pattern, "metrics") and re.fullmatch(pattern, "http")
        assert not re.fullmatch(pattern, "https")


@pytest.mark.parametrize("monitoring_namespace", [None, "separate-monitoring"])
def test_installed_prometheus_namespace_and_discovery_egress(
    monitoring_namespace: str | None,
) -> None:
    """The supplied installation works under deny-all; explicit external monitoring stays supported."""
    values = enabled_values()
    if monitoring_namespace is not None:
        values["global"]["networkPolicies"]["monitoringNamespace"] = (
            monitoring_namespace
        )
    result = render("connected-ha", values, namespace="custos-system")
    assert result.returncode == 0, result.stderr
    documents = [value for value in yaml.safe_load_all(result.stdout) if value]
    policies = {
        value["metadata"]["name"]: value
        for value in documents
        if value["kind"] == "NetworkPolicy"
    }
    source = monitoring_namespace or "custos-system"
    ingress = policies["custos-receivers-activity-runtime-manager"]["spec"]["ingress"]
    scrape = next(
        rule for rule in ingress if rule["ports"] == [{"protocol": "TCP", "port": 8090}]
    )
    assert scrape["from"] == [
        {
            "namespaceSelector": {
                "matchLabels": {
                    "kubernetes.io/metadata.name": source,
                }
            }
        }
    ]
    monitor = policies.get("custos-receivers-prometheus")
    if monitoring_namespace is not None:
        assert monitor is None
        return
    assert monitor is not None
    spec = monitor["spec"]
    assert spec["podSelector"]["matchLabels"] == {
        "app.kubernetes.io/name": "prometheus",
        "app.kubernetes.io/component": "server",
    }
    assert spec["policyTypes"] == ["Egress"]
    assert spec["egress"][1]["to"] == [{"ipBlock": {"cidr": "10.96.0.1/32"}}]
    assert spec["egress"][2]["ports"] == [
        {"protocol": "TCP", "port": 8080},
        {"protocol": "TCP", "port": 8090},
    ]

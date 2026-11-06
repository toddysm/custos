"""Verify generated bulk resources, compact release references and legacy bridge retention."""

import copy
import hashlib
import importlib.util
import json
import random
import string
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml
from custos_callctx.execution import load_execution_bindings
from test_receiver_runtime import CHART, PROFILES, enabled_values, render

SPEC = importlib.util.spec_from_file_location(
    "receiver_bindings", CHART.parents[2] / "scripts/receiver_bindings.py"
)
assert SPEC is not None and SPEC.loader is not None
operator = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = operator
SPEC.loader.exec_module(operator)


def fixture(count: int, *, maximum: bool = False) -> bytes:
    """Generate deterministic, nonrepetitive references valid as Kubernetes names/keys."""
    rng = random.Random(1048)
    entries = []
    for i in range(count):
        entry = {
            key: "".join(rng.choices(string.ascii_lowercase + string.digits, k=120))
            if maximum
            else f"{key.lower()}-{i}"
            for key in (
                "workspaceId",
                "principalId",
                "bindingId",
                "secretRef",
                "secretKey",
            )
        }
        entries.append(entry)
    return json.dumps(
        {
            "bindings": entries,
            "defaults": {r["workspaceId"]: r["bindingId"] for r in entries},
        },
        separators=(",", ":"),
    ).encode()


def configuration() -> dict[str, Any]:
    """Merge actual chart defaults and reference-only managed prerequisites."""
    return operator.merge(
        yaml.safe_load((CHART / "values.yaml").read_text()), enabled_values()
    )


@pytest.mark.parametrize("existing", ["missing", "same", "conflict"])
def test_create_only_bundle(
    existing: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing objects are explicitly created, exact objects reused, conflicts never overwritten."""
    bundle = operator.prepare(fixture(1), configuration(), "custos", "controllers")
    objects = (
        {
            (row["kind"], row["metadata"]["name"]): copy.deepcopy(row)
            for row in bundle["objects"]
        }
        if existing != "missing"
        else {}
    )
    if existing == "conflict":
        objects[("ConfigMap", bundle["descriptor"]["configMapName"])]["immutable"] = (
            False
        )
    writes: list[list[str]] = []

    def run(command: list[str], payload: Any = None) -> str:
        """Emulate the actual named-resource API boundary; reject unexpected command classes."""
        if command[:3] == ["kubectl", "get", "namespace"]:
            return json.dumps({"metadata": {"name": "controllers"}})
        if command[:2] == ["helm", "list"]:
            return "[]"
        if command[:2] == ["kubectl", "get"]:
            value = objects.get((command[2], command[3]))
            return json.dumps(value) if value else ""
        assert command[:2] == ["kubectl", "create"]
        writes.append(command)
        if "--dry-run=server" not in command:
            objects[(payload["kind"], payload["metadata"]["name"])] = payload
        return json.dumps(payload)

    monkeypatch.setattr(operator, "run", run)
    if existing == "conflict":
        with pytest.raises(ValueError, match="conflicts"):
            operator.provision(bundle, create=True)
        assert writes == []
    else:
        operator.provision(bundle, create=True)
        assert len(writes) == (26 if existing == "missing" else 0)
        operator.provision(bundle, create=False)


def test_verify_missing_bundle_is_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verification never creates a missing authorization object or reports success."""
    bundle = operator.prepare(fixture(1), configuration(), "custos", "controllers")
    monkeypatch.setattr(operator, "transition_preflight", lambda *args, **kwargs: None)
    monkeypatch.setattr(operator, "get_resource", lambda resource: None)
    with pytest.raises(ValueError, match="missing"):
        operator.provision(bundle, create=False)


def test_recovery_patch_is_guarded_and_preserves_unrelated_fields() -> None:
    """Timeout recovery removes every file-only field without replacing the Pod template."""
    values = enabled_values()
    bridge_docs = list(yaml.safe_load_all(render("connected-eval", values).stdout))
    target = next(
        row
        for row in bridge_docs
        if row
        and row["kind"] == "Deployment"
        and row["metadata"]["name"] == "custos-api-gateway"
    )
    live = copy.deepcopy(target)
    live["metadata"]["resourceVersion"] = "42"
    pod = live["spec"]["template"]
    container = pod["spec"]["containers"][0]
    container["env"] = [
        row for row in container["env"] if row["name"] not in operator.SOURCE_ENVS
    ] + [
        {"name": "CUSTOS_EXECUTION_IDENTITY_ENABLED", "value": "true"},
        {"name": "CUSTOS_EXECUTION_BINDINGS_FILE", "value": "/file"},
        {"name": "CUSTOS_EXECUTION_BINDINGS_SHA256", "value": "0" * 64},
        {"name": "UNRELATED", "value": "preserved"},
    ]
    container["volumeMounts"] = [
        {"name": "execution-bindings", "mountPath": "/custos/execution"},
        {"name": "unrelated", "mountPath": "/other"},
    ]
    pod["spec"]["volumes"] = [
        {"name": "execution-bindings", "configMap": {"name": "old"}},
        {"name": "unrelated", "emptyDir": {}},
    ]
    pod["metadata"]["annotations"].update(
        {
            "checksum/execution-bindings": "0" * 64,
            "checksum/execution-bundle": "0" * 64,
            "unrelated": "preserved",
        }
    )
    patch = operator.reconcile_source(live, target)
    assert patch[0] == {
        "op": "test",
        "path": "/metadata/resourceVersion",
        "value": "42",
    }
    for operation in patch[1:]:
        path = operation["path"].split("/")[1:]
        parent = live
        for key in path[:-1]:
            parent = parent[int(key)] if isinstance(parent, list) else parent[key]
        parent[path[-1]] = operation["value"]
    assert operator.source_fields(live) == operator.source_fields(target)
    assert container["env"][-1] != {"name": "UNRELATED", "value": "preserved"}
    assert {"name": "UNRELATED", "value": "preserved"} in container["env"]
    assert container["volumeMounts"] == [{"name": "unrelated", "mountPath": "/other"}]
    assert pod["metadata"]["annotations"]["unrelated"] == "preserved"
    assert all(row["path"] != "/spec/template" for row in patch)


@pytest.mark.parametrize("reference", ["pod", "controller", "history", "work"])
def test_cleanup_refuses_retained_references(
    reference: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cleanup refuses each retention class before making any deletion request."""
    bundle = operator.prepare(fixture(1), configuration(), "custos", "controllers")
    config_name = bundle["descriptor"]["configMapName"]
    writes: list[list[str]] = []

    def run(command: list[str], payload: Any = None) -> str:
        """Return one selected retention reference and record any forbidden deletion."""
        if command[:2] == ["kubectl", "delete"]:
            writes.append(command)
            return "{}"
        if command[:3] == ["kubectl", "get", "pods"]:
            return json.dumps({"reference": config_name} if reference == "pod" else {})
        if command[:2] == ["kubectl", "get"]:
            return json.dumps(
                {"reference": config_name} if reference == "controller" else {}
            )
        if command[:2] == ["helm", "list"]:
            return '[{"name":"custos"}]'
        if command[:2] == ["helm", "history"]:
            return '[{"revision":1}]'
        if command[:3] == ["helm", "get", "manifest"]:
            return config_name if reference == "history" else ""
        raise AssertionError("unexpected cleanup command")

    monkeypatch.setattr(operator, "run", run)
    with pytest.raises(ValueError):
        operator.cleanup_resources(
            bundle, confirmed_no_retained_work=reference != "work"
        )
    assert writes == []


def retained_fixture() -> tuple[dict[str, Any], dict[tuple[str, str], dict[str, Any]]]:
    """Model the actual bridge render and API-managed metadata without inventing policy fixtures."""
    config = enabled_values()
    config["global"]["receiverRuntime"]["retainLegacyExecutionResources"] = True
    config["namespace"] = {"name": "controllers", "create": True}
    docs = [
        row
        for row in yaml.safe_load_all(render("connected-eval", config).stdout)
        if row
    ]
    resources = operator.bridge_resources(docs, "custos", "controllers")
    live = {}
    records = []
    for index, resource in enumerate(resources):
        value = copy.deepcopy(resource)
        metadata = value["metadata"]
        metadata["uid"] = f"original-{index}"
        metadata.setdefault("labels", {})["app.kubernetes.io/managed-by"] = "Helm"
        metadata.setdefault("annotations", {}).update(
            {
                "meta.helm.sh/release-name": "custos",
                "meta.helm.sh/release-namespace": "controllers",
            }
        )
        if value["kind"] == "Namespace":
            value["spec"] = {"finalizers": ["kubernetes"]}
        live[(resource["kind"], metadata["name"])] = value
        records.append(
            {
                "resource": resource,
                "uid": metadata["uid"],
                "labels": copy.deepcopy(metadata["labels"]),
                "sha256": hashlib.sha256(
                    operator.canonical(operator.effective(resource))
                ).hexdigest(),
            }
        )
    return {"release": "custos", "namespace": "controllers", "resources": records}, live


@pytest.mark.parametrize(
    "change", ["none", "missing", "uid", "policy", "owner", "labels", "digest"]
)
def test_retained_identity_policy_and_namespace_guards(
    change: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retained verification normalizes API defaults but rejects every protected lifecycle change."""
    inventory, live = retained_fixture()
    role_key = next(key for key in live if key[0] == "Role")
    if change == "missing":
        live.pop(role_key)
    elif change == "uid":
        live[role_key]["metadata"]["uid"] = "recreated"
    elif change == "policy":
        live[role_key]["rules"][0]["verbs"] = ["list"]
    elif change == "owner":
        live[role_key]["metadata"]["annotations"]["meta.helm.sh/release-name"] = (
            "another"
        )
    elif change == "labels":
        live[("Namespace", "controllers")]["metadata"]["labels"][
            "pod-security.kubernetes.io/enforce"
        ] = "privileged"
    elif change == "digest":
        inventory["resources"][0]["sha256"] = "0" * 64
    monkeypatch.setattr(
        operator,
        "get_resource",
        lambda resource: live.get((resource["kind"], resource["metadata"]["name"])),
    )
    if change == "none":
        operator.verify_retained(inventory)
    else:
        with pytest.raises(ValueError):
            operator.verify_retained(inventory)


@pytest.mark.parametrize("operation", ["atomic", "native-inline-rollback"])
def test_preparation_rejects_unsupported_helm_transition(
    operation: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Planned unsafe Helm operations fail before provisioning or changing any existing object."""
    inventory, _ = retained_fixture()
    bundle = operator.prepare(fixture(1), configuration(), "custos", "controllers")

    def run(command: list[str], payload: Any = None) -> str:
        """Expose a retained inline predecessor; all mutation commands are forbidden."""
        if command[:3] == ["kubectl", "get", "namespace"]:
            return '{"metadata":{"name":"controllers"}}'
        if command[:2] == ["helm", "list"]:
            return '[{"name":"custos"}]'
        if command[:3] == ["helm", "get", "values"]:
            return json.dumps(configuration())
        if command[:3] == ["helm", "get", "manifest"]:
            return yaml.safe_dump_all(
                [row["resource"] for row in inventory["resources"]]
            )
        raise AssertionError("unexpected mutation or preflight command")

    monkeypatch.setattr(operator, "run", run)
    with pytest.raises(ValueError, match="prohibited"):
        operator.transition_preflight(
            bundle,
            atomic=operation == "atomic",
            rollback_revision=1 if operation == "native-inline-rollback" else None,
        )


@pytest.mark.parametrize("size", [1_000_000, 1_000_001])
def test_preparation_byte_budget(size: int) -> None:
    """The operator preserves exact padding bytes and uses the independent production file cap."""
    raw = fixture(1)
    padded = raw + b" " * (size - len(raw))
    if size > 1_000_000:
        with pytest.raises(ValueError, match="exceeds"):
            operator.prepare(padded, configuration(), "custos", "controllers")
    else:
        bundle = operator.prepare(padded, configuration(), "custos", "controllers")
        assert bundle["objects"][0]["data"]["bindings.json"].encode() == padded
        assert len(padded) + len(b"bindings.json") <= 1_000_013


@pytest.mark.parametrize("profile", PROFILES)
@pytest.mark.parametrize("count", [51, 200, 1000, 1024])
def test_file_bundle_profiles(profile: str, count: int, tmp_path: Path) -> None:
    """Every profile projects exact external bytes without copying bulk data into Helm."""
    raw = fixture(count, maximum=True)
    bundle = operator.prepare(raw, configuration(), "custos", "controllers")
    values = enabled_values()
    runtime = values["global"]["receiverRuntime"]
    runtime["executionBindings"] = {"bindings": [], "defaults": {}}
    runtime["executionBindingsBundle"] = bundle["descriptor"]
    for service in operator.CONSUMERS:
        values.setdefault(service, {})["env"] = [
            {"name": "CUSTOS_EXECUTION_BINDINGS", "value": "legacy"},
            {"name": "CUSTOS_EXECUTION_BINDINGS_FILE", "value": "/wrong"},
            {"name": "CUSTOS_EXECUTION_BINDINGS_SHA256", "value": "bad"},
            {"name": "UNRELATED", "value": "preserved"},
        ]
    result = render(profile, values)
    assert result.returncode == 0, result.stderr
    docs = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
    deployments = {
        doc["metadata"]["name"]: doc for doc in docs if doc["kind"] == "Deployment"
    }
    for service in operator.CONSUMERS:
        pod = deployments["custos-" + service]["spec"]["template"]
        env = {row["name"]: row for row in pod["spec"]["containers"][0]["env"]}
        assert "CUSTOS_EXECUTION_BINDINGS" not in env
        assert env["UNRELATED"]["value"] == "preserved"
        assert (
            env["CUSTOS_EXECUTION_BINDINGS_FILE"]["value"]
            == "/custos/execution/bindings.json"
        )
        assert (
            env["CUSTOS_EXECUTION_BINDINGS_SHA256"]["value"]
            == hashlib.sha256(raw).hexdigest()
        )
        assert (
            pod["metadata"]["annotations"]["dapr.io/config"]
            == (bundle["descriptor"]["daprConfigurations"][service])
        )
        volumes = {row["name"]: row for row in pod["spec"]["volumes"]}
        assert (
            volumes["execution-bindings"]["configMap"]["name"]
            == bundle["descriptor"]["configMapName"]
        )
        mounts = pod["spec"]["containers"][0]["volumeMounts"]
        mount = next(row for row in mounts if row["name"] == "execution-bindings")
        assert mount["readOnly"] and "subPath" not in mount
        assert not any(
            doc["kind"] == "Configuration"
            and doc["metadata"]["name"] == f"custos-receivers-{service}"
            for doc in docs
        )
    assert raw.decode() not in result.stdout
    assert bundle["objects"][0]["immutable"] is True
    path = tmp_path / "bindings.json"
    path.write_text(bundle["objects"][0]["data"]["bindings.json"])
    loaded = load_execution_bindings(
        {
            "CUSTOS_EXECUTION_IDENTITY_ENABLED": "true",
            "CUSTOS_EXECUTION_BINDINGS_FILE": str(path),
            "CUSTOS_EXECUTION_BINDINGS_SHA256": bundle["descriptor"]["sha256"],
        }
    )
    assert loaded is not None and len(loaded.bindings) == count
    assert len(raw) + len("bindings.json") <= 1_000_013
    assert operator.verify_bundle(bundle, configuration()) == bundle


def test_policy_equivalence_and_create_only_digest() -> None:
    """External and legacy resources have identical effective authority, excluding names."""
    config = configuration()
    raw = json.dumps(config["global"]["receiverRuntime"]["executionBindings"]).encode()
    bundle = operator.prepare(raw, config, "custos", "controllers")
    legacy = [
        doc
        for doc in yaml.safe_load_all(render("connected-eval", enabled_values()).stdout)
        if doc
    ]
    for service in operator.CONSUMERS:
        resource_name = bundle["descriptor"]["daprConfigurations"][service]
        for kind, middle, content in (
            ("Configuration", "receivers", "spec"),
            ("Role", "execution-secrets", "rules"),
            ("RoleBinding", "execution-secrets", "subjects"),
        ):
            expected = next(
                doc[content]
                for doc in legacy
                if doc["kind"] == kind
                and doc["metadata"]["name"] == f"custos-{middle}-{service}"
            )
            generated = next(
                doc[content]
                for doc in bundle["objects"]
                if doc["kind"] == kind and doc["metadata"]["name"] == resource_name
            )
            assert generated == expected
    changed = copy.deepcopy(bundle)
    changed["objects"][1]["spec"]["accessControl"]["defaultAction"] = "allow"
    with pytest.raises(ValueError, match="changed"):
        operator.verify_bundle(changed, config)


def test_retention_bridge_has_exact_legacy_resources_and_namespace() -> None:
    """Opt-in retention appears in the stored render, not merely mutable live annotations."""
    values = enabled_values()
    values["global"]["receiverRuntime"]["retainLegacyExecutionResources"] = True
    values["namespace"] = {"create": True, "name": "controllers"}
    result = render("connected-eval", values)
    assert result.returncode == 0, result.stderr
    docs = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
    kept = [
        doc
        for doc in docs
        if doc["kind"] in {"Configuration", "Role", "RoleBinding"}
        and doc["metadata"].get("annotations", {}).get("helm.sh/resource-policy")
        == "keep"
    ]
    # Other sandbox/runtime policies also have keep; select only the twelve controller resources.
    legacy = [
        doc
        for doc in kept
        if doc["metadata"].get("namespace") == "controllers"
        and any(
            doc["metadata"]["name"].endswith(service) for service in operator.CONSUMERS
        )
    ]
    assert len(legacy) == 12
    ns = next(
        doc
        for doc in docs
        if doc["kind"] == "Namespace" and doc["metadata"]["name"] == "controllers"
    )
    assert ns["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"


@pytest.mark.parametrize(
    "fault", ["mixed", "namespace", "retention", "digest", "unknown", "consumer"]
)
def test_file_mode_closed_guards(fault: str) -> None:
    """Invalid or mixed deployment descriptors never render an admission-ready workload."""
    values = enabled_values()
    bundle = operator.prepare(fixture(1), configuration(), "custos", "controllers")
    runtime = values["global"]["receiverRuntime"]
    runtime["executionBindings"] = {"bindings": [], "defaults": {}}
    runtime["executionBindingsBundle"] = bundle["descriptor"]
    if fault == "mixed":
        runtime["executionBindings"] = enabled_values()["global"]["receiverRuntime"][
            "executionBindings"
        ]
    elif fault == "namespace":
        values["namespace"] = {"create": True}
    elif fault == "retention":
        runtime["retainLegacyExecutionResources"] = True
    elif fault == "digest":
        runtime["executionBindingsBundle"]["sha256"] = "bad"
    elif fault == "unknown":
        runtime["executionBindingsBundle"]["unrecognized"] = True
    else:
        runtime["executionBindingsBundle"]["daprConfigurations"].pop("api-gateway")
    assert render("connected-eval", values).returncode != 0

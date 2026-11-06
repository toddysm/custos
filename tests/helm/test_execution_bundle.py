"""Verify generated bulk resources, compact release references and legacy bridge retention."""

import copy
import hashlib
import importlib.util
import json
import random
import string
import subprocess
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
    result = operator.merge(
        yaml.safe_load((CHART / "values.yaml").read_text()), enabled_values()
    )
    assert isinstance(result, dict)
    return result


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


@pytest.mark.parametrize("release", ["a" * 19 + ".b", "a" * 19 + "-b", "a." * 10 + "b"])
def test_truncated_release_prefix_remains_a_valid_resource_name(release: str) -> None:
    """Valid Helm release names remain valid after truncation at a dot/hyphen boundary."""
    bundle = operator.prepare(fixture(1), configuration(), release, "controllers")
    for resource in bundle["objects"]:
        assert (
            operator.name(resource["metadata"]["name"]) == resource["metadata"]["name"]
        )


def test_cli_preserves_actionable_safe_domain_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A real bundle-integrity rejection identifies remediation without dumping registry contents."""
    config = configuration()
    bundle = operator.prepare(fixture(1), config, "custos", "controllers")
    bundle["descriptor"]["sha256"] = "0" * 64
    artifact = tmp_path / "bundle.json"
    artifact.write_text(json.dumps(bundle))
    monkeypatch.setattr(operator, "values", lambda paths: config)
    monkeypatch.setattr(
        sys, "argv", ["receiver_bindings.py", "verify", "--bundle", str(artifact)]
    )
    with pytest.raises(SystemExit) as failure:
        operator.main()
    assert failure.value.code == 1
    error = capsys.readouterr().err
    assert "reason=bundle content or deployment/security inputs changed" in error
    assert "workspaceid-0" not in error


@pytest.mark.parametrize(
    "error",
    [
        ValueError("private-canary"),
        OSError("private-canary"),
        KeyError("private-canary"),
    ],
)
def test_cli_suppresses_untrusted_exception_details(
    error: Exception,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Only helper-authored reasons are public; parser/OS/library failures remain sanitized."""

    def values(paths: list[Path]) -> dict[str, Any]:
        """Raise an untrusted failure that could otherwise disclose file contents."""
        raise error

    monkeypatch.setattr(operator, "values", values)
    monkeypatch.setattr(
        sys, "argv", ["receiver_bindings.py", "verify", "--bundle", "unused"]
    )
    with pytest.raises(SystemExit) as failure:
        operator.main()
    assert failure.value.code == 1
    output = capsys.readouterr().err
    assert f"reason={type(error).__name__}" in output
    assert "private-canary" not in output


def test_dapr_metric_admission_defaults() -> None:
    """Known enabled defaults compare equally; changed metrics/policies remain conflicts."""
    bundle = operator.prepare(fixture(1), configuration(), "custos", "controllers")
    desired = bundle["objects"][1]
    original = copy.deepcopy(desired)
    live = copy.deepcopy(desired)
    live["spec"].update(metric={"enabled": True}, metrics={"enabled": True})
    assert operator.effective(live) == operator.effective(desired)
    assert desired == original
    live["spec"]["metrics"]["enabled"] = False
    assert operator.effective(live) != operator.effective(desired)
    live["spec"]["metrics"]["enabled"] = True
    live["spec"]["accessControl"]["defaultAction"] = "allow"
    assert operator.effective(live) != operator.effective(desired)


def test_prepare_fragment_deletes_inherited_inline_defaults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The actual CLI fragment clears earlier inline/retention values under Helm's merge rules."""
    config = configuration()
    config["global"]["receiverRuntime"]["retainLegacyExecutionResources"] = True
    config["namespace"]["create"] = True
    baseline = tmp_path / "inline.yaml"
    baseline.write_text(yaml.safe_dump(config))
    registry = tmp_path / "bindings.json"
    registry.write_bytes(fixture(1))
    artifact = tmp_path / "bundle.json"
    monkeypatch.setattr(operator, "transition_preflight", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "receiver_bindings.py",
            "prepare",
            "--registry",
            str(registry),
            "--bundle",
            str(artifact),
            "--values",
            str(baseline),
        ],
    )
    operator.main()
    fragment = json.loads(capsys.readouterr().out.split("\n", 1)[1])
    assert (
        fragment["global"]["receiverRuntime"]["executionBindings"]["defaults"] is None
    )
    override = tmp_path / "file.yaml"
    override.write_text(yaml.safe_dump(fragment))
    result = subprocess.run(
        [
            "helm",
            "template",
            "custos",
            str(CHART),
            "--namespace",
            "controllers",
            "--kube-version",
            "1.31.0",
            "-f",
            str(baseline),
            "-f",
            str(override),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    docs = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
    for service in operator.CONSUMERS:
        deployment = next(
            doc
            for doc in docs
            if doc["kind"] == "Deployment"
            and doc["metadata"]["name"] == f"custos-{service}"
        )
        env = {
            entry["name"]: entry
            for entry in deployment["spec"]["template"]["spec"]["containers"][0]["env"]
        }
        assert "CUSTOS_EXECUTION_BINDINGS" not in env
        assert "CUSTOS_EXECUTION_BINDINGS_FILE" in env


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
    pod = {"spec": {"volumes": [{"configMap": {"name": config_name}}]}}
    writes: list[list[str]] = []

    def run(command: list[str], payload: Any = None) -> str:
        """Return one selected retention reference and record any forbidden deletion."""
        if command[:2] == ["kubectl", "delete"]:
            writes.append(command)
            return "{}"
        if command[:3] == ["kubectl", "get", "pods"]:
            return json.dumps({"items": [pod] if reference == "pod" else []})
        if command[:2] == ["kubectl", "get"]:
            return json.dumps({"items": [pod] if reference == "controller" else []})
        if command[:2] == ["helm", "list"]:
            return '[{"name":"custos"}]'
        if command[:2] == ["helm", "history"]:
            return '[{"revision":1}]'
        if command[:3] == ["helm", "get", "manifest"]:
            return (
                yaml.safe_dump(bundle["objects"][0]) if reference == "history" else ""
            )
        raise AssertionError("unexpected cleanup command")

    monkeypatch.setattr(operator, "run", run)
    with pytest.raises(ValueError):
        operator.cleanup_resources(
            bundle, confirmed_no_retained_work=reference != "work"
        )
    assert writes == []


def test_cleanup_inspects_later_helm_release_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retained rollback reference beyond Helm's default page prevents every deletion."""
    bundle = operator.prepare(fixture(1), configuration(), "custos", "controllers")
    pages = []
    writes = []

    def run(command: list[str], payload: Any = None) -> str:
        """Return 257 releases with the sole dependency in the last release's older revision."""
        if command[:2] == ["kubectl", "get"]:
            return '{"items":[]}'
        if command[:2] == ["helm", "list"]:
            assert command[command.index("--max") + 1] == "256"
            offset = int(command[command.index("--offset") + 1])
            pages.append(offset)
            return json.dumps(
                [{"name": f"release-{index}"} for index in range(256)]
                if offset == 0
                else [{"name": "last-release"}]
            )
        if command[:2] == ["helm", "history"]:
            return '[{"revision":1}]' if command[2] == "last-release" else "[]"
        if command[:3] == ["helm", "get", "manifest"]:
            return yaml.safe_dump(bundle["objects"][0])
        writes.append(command)
        raise AssertionError("deletion must not precede the complete history scan")

    monkeypatch.setattr(operator, "run", run)
    with pytest.raises(ValueError, match="retained Helm"):
        operator.cleanup_resources(bundle, confirmed_no_retained_work=True)
    assert pages == [0, 256]
    assert writes == []


@pytest.mark.parametrize(
    "stage",
    [
        "pod",
        "pod-items",
        "controller",
        "controller-items",
        "release",
        "history",
        "manifest",
    ],
)
def test_cleanup_rejects_malformed_retention_inventories(
    stage: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Malformed structured inventory must not become an empty successful retention scan."""
    bundle = operator.prepare(fixture(1), configuration(), "custos", "controllers")

    def run(command: list[str], payload: Any = None) -> str:
        """Corrupt one required inventory; every mutation is forbidden."""
        if command[:3] == ["kubectl", "get", "pods"]:
            if stage == "pod-items":
                return '{"items":["invalid"]}'
            return "{}" if stage == "pod" else '{"items":[]}'
        if command[:2] == ["kubectl", "get"]:
            if stage == "controller-items":
                return '{"items":["invalid"]}'
            return "{}" if stage == "controller" else '{"items":[]}'
        if command[:2] == ["helm", "list"]:
            return "{}" if stage == "release" else '[{"name":"custos"}]'
        if command[:2] == ["helm", "history"]:
            return "{}" if stage == "history" else '[{"revision":1}]'
        if command[:3] == ["helm", "get", "manifest"]:
            return "not-a-resource"
        raise AssertionError("invalid inventory must prevent every mutation")

    monkeypatch.setattr(operator, "run", run)
    with pytest.raises(TypeError, match="invalid"):
        operator.cleanup_resources(bundle, confirmed_no_retained_work=True)


@pytest.mark.parametrize("dependency", ["owned", "unrelated", "external"])
def test_cleanup_checks_live_rolebindings_before_any_delete(
    monkeypatch: pytest.MonkeyPatch, dependency: str
) -> None:
    """Protect externally referenced Roles while allowing deletion of verified own bindings."""
    bundle = operator.prepare(fixture(1), configuration(), "custos", "controllers")
    live = {
        (row["kind"], row["metadata"]["name"]): copy.deepcopy(row)
        for row in bundle["objects"]
    }
    for index, row in enumerate(live.values()):
        row["metadata"].update(uid=f"uid-{index}", resourceVersion="1")
    bindings = [
        copy.deepcopy(row) for row in live.values() if row["kind"] == "RoleBinding"
    ]
    if dependency != "owned":
        extra = copy.deepcopy(bindings[0])
        extra["metadata"]["name"] = "standalone-reader"
        if dependency == "unrelated":
            extra["roleRef"]["name"] = "unrelated-role"
        bindings.append(extra)
    writes: list[list[str]] = []

    def run(command: list[str], payload: Any = None) -> str:
        """Return live authorization dependencies and record every attempted deletion."""
        if command[:3] == ["kubectl", "get", "pods"]:
            return '{"items":[]}'
        if command[:2] == ["kubectl", "get"]:
            assert command[2].endswith(",rolebindings")
            return json.dumps({"items": bindings})
        if command[:2] == ["helm", "list"]:
            return "[]"
        if command[:2] == ["kubectl", "delete"]:
            writes.append(command)
            return "{}"
        raise AssertionError(command)

    monkeypatch.setattr(operator, "run", run)
    monkeypatch.setattr(
        operator,
        "get_resource",
        lambda resource: live[(resource["kind"], resource["metadata"]["name"])],
    )
    if dependency == "external":
        with pytest.raises(ValueError, match="RoleBinding"):
            operator.cleanup_resources(bundle, confirmed_no_retained_work=True)
        assert writes == []
    else:
        operator.cleanup_resources(bundle, confirmed_no_retained_work=True)
        assert len(writes) == len(bundle["objects"])
        paths = [command[3] for command in writes]
        for binding in bindings[:4]:
            binding_path = f"/rolebindings/{binding['metadata']['name']}"
            role_path = f"/roles/{binding['roleRef']['name']}"
            binding_index = next(
                i for i, path in enumerate(paths) if path.endswith(binding_path)
            )
            role_index = next(
                i for i, path in enumerate(paths) if path.endswith(role_path)
            )
            assert binding_index < role_index


def bridge_configuration() -> dict[str, Any]:
    """Use actual chart prerequisites with retained inline policies and an owned Namespace."""
    config = enabled_values()
    config["global"]["receiverRuntime"]["retainLegacyExecutionResources"] = True
    config["namespace"] = {"name": "controllers", "create": True}
    return config


def retained_fixture() -> tuple[dict[str, Any], dict[tuple[str, str], dict[str, Any]]]:
    """Model the actual bridge render and API-managed metadata without inventing policy fixtures."""
    docs = [
        row
        for row in yaml.safe_load_all(
            render("connected-eval", bridge_configuration()).stdout
        )
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
    return {
        "release": "custos",
        "namespace": "controllers",
        "resources": records,
        "renderingSha256": operator.rendering_digest(docs),
    }, live


@pytest.mark.parametrize(
    "ownership",
    [
        "none",
        "same",
        "foreign-name",
        "foreign-namespace",
        "missing-namespace",
        "namespace-only",
        "managed-only",
    ],
)
def test_namespace_preflight_checks_the_complete_helm_owner(
    ownership: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same-named releases elsewhere and partial Helm ownership fail before release lookup."""
    bundle = operator.prepare(fixture(1), configuration(), "custos", "controllers")
    annotations = {"helm.sh/resource-policy": "keep"}
    metadata: dict[str, Any] = {"name": "controllers", "annotations": annotations}
    if ownership != "none":
        metadata["labels"] = {"app.kubernetes.io/managed-by": "Helm"}
    if ownership not in {"none", "managed-only"}:
        annotations["meta.helm.sh/release-name"] = "custos"
        annotations["meta.helm.sh/release-namespace"] = "controllers"
    if ownership == "foreign-name":
        annotations["meta.helm.sh/release-name"] = "another"
    elif ownership == "foreign-namespace":
        annotations["meta.helm.sh/release-namespace"] = "another-namespace"
    elif ownership == "missing-namespace":
        annotations.pop("meta.helm.sh/release-namespace")
    elif ownership == "namespace-only":
        annotations.pop("meta.helm.sh/release-name")

    def run(command: list[str], payload: Any = None) -> str:
        """Expose an existing file release only after the Namespace ownership check passes."""
        if command[:3] == ["kubectl", "get", "namespace"]:
            return json.dumps({"metadata": metadata})
        assert ownership in {"none", "same"}, (
            "foreign owner must prevent release lookup"
        )
        if command[:2] == ["helm", "list"]:
            return '[{"name":"custos"}]'
        if command[:3] == ["helm", "get", "manifest"]:
            return ""
        raise AssertionError("preflight must not mutate resources")

    monkeypatch.setattr(operator, "run", run)
    if ownership in {"none", "same"}:
        operator.transition_preflight(bundle)
    else:
        with pytest.raises(operator.OperatorValidationError, match="release namespace"):
            operator.transition_preflight(bundle)


@pytest.mark.parametrize("pin", [None, [], "A" * 64, "0" * 63, "valid"])
def test_cutover_requires_a_complete_rendering_pin(
    pin: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject old/malformed recovery evidence without weakening retained-resource cleanup checks."""
    inventory, live = retained_fixture()
    if pin is None:
        inventory.pop("renderingSha256")
    elif pin != "valid":
        inventory["renderingSha256"] = pin
    bundle = operator.prepare(fixture(1), configuration(), "custos", "controllers")

    def run(command: list[str], payload: Any = None) -> str:
        """Expose a file release without permitting any write during pin validation."""
        if command[:3] == ["kubectl", "get", "namespace"]:
            return '{"metadata":{"name":"controllers"}}'
        if command[:2] == ["helm", "list"]:
            return '[{"name":"custos"}]'
        if command[:3] == ["helm", "get", "manifest"]:
            return ""
        raise AssertionError("pin validation must not mutate resources")

    monkeypatch.setattr(operator, "run", run)
    monkeypatch.setattr(
        operator,
        "get_resource",
        lambda resource: live[(resource["kind"], resource["metadata"]["name"])],
    )
    operator.verify_retained(inventory)
    if pin == "valid":
        operator.transition_preflight(bundle, inventory)
    else:
        with pytest.raises(
            operator.OperatorValidationError, match="complete rendering"
        ):
            operator.transition_preflight(bundle, inventory)


def test_full_render_digest_preserves_all_fields_and_document_multiplicity() -> None:
    """Full-render comparison includes metadata, Secret fields, hooks and extra/duplicate objects."""
    docs: list[dict[str, Any]] = [
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "synthetic", "labels": {"scope": "bridge"}},
            "type": "Opaque",
            "stringData": {"synthetic-key": "synthetic-value"},
        },
        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": "hook",
                "annotations": {"helm.sh/hook": "pre-upgrade"},
            },
            "spec": {
                "template": {
                    "spec": {"containers": [{"name": "hook", "image": "verified:1"}]}
                }
            },
        },
    ]
    original = copy.deepcopy(docs)
    digest = operator.rendering_digest(docs)
    assert digest == operator.rendering_digest(list(reversed(docs)))
    assert digest == operator.rendering_digest(
        json.loads(json.dumps(docs, sort_keys=True))
    )
    assert docs == original
    changed = []
    for field, value in (
        ("type", "kubernetes.io/basic-auth"),
        ("stringData", {"synthetic-key": "private-canary"}),
        ("data", {"synthetic-key": "dW52ZXJpZmllZA=="}),
        ("metadata", {"name": "synthetic", "labels": {"scope": "changed"}}),
    ):
        variant = copy.deepcopy(docs)
        variant[0][field] = value
        changed.append(variant)
    variant = copy.deepcopy(docs)
    variant[1]["spec"]["template"]["spec"]["containers"][0]["image"] = "unverified:2"
    changed.extend([variant, docs[:1], [*docs, docs[0]]])
    assert all(operator.rendering_digest(variant) != digest for variant in changed)


@pytest.mark.parametrize(
    "change",
    [
        "none",
        "auth-image",
        "hook-image",
        "metadata",
        "extra",
        "missing",
        "history-race",
    ],
)
def test_record_bridge_verifies_the_complete_stored_render(
    change: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject changes outside the four consumers and bind manifests/hooks to one deployed revision."""
    config = bridge_configuration()
    stored = [
        row
        for row in yaml.safe_load_all(render("connected-eval", config).stdout)
        if row
    ]
    desired_config = copy.deepcopy(config)
    if change == "auth-image":
        desired_config.setdefault("auth-service", {}).setdefault("image", {})["tag"] = (
            "unverified"
        )
    desired = [
        row
        for row in yaml.safe_load_all(render("connected-eval", desired_config).stdout)
        if row
    ]
    if change == "hook-image":
        hook = next(
            row
            for row in desired
            if row["kind"] == "Job"
            and row["metadata"].get("annotations", {}).get("helm.sh/hook")
        )
        hook["spec"]["template"]["spec"]["containers"][0]["image"] = "unverified:2"
    elif change == "metadata":
        auth = next(
            row
            for row in desired
            if row["kind"] == "Deployment"
            and row["metadata"]["name"] == "custos-auth-service"
        )
        auth["metadata"].setdefault("labels", {})["scope"] = "unverified"
    elif change == "extra":
        desired.append(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "unverified"},
                "data": {"value": "synthetic"},
            }
        )
    elif change == "missing":
        desired = [
            row for row in desired if row["metadata"]["name"] != "custos-auth-service"
        ]
    chart = tmp_path / "saved.tgz"
    chart.write_bytes(b"synthetic saved chart archive")
    path = tmp_path / "values.yaml"
    path.write_text(yaml.safe_dump(desired_config))
    _, live = retained_fixture()
    history_calls = 0

    def run(command: list[str], payload: Any = None) -> str:
        """Return actual chart renders at a pinned revision; every mutation is forbidden."""
        nonlocal history_calls
        if command[:2] == ["helm", "history"]:
            history_calls += 1
            revision = 3 if change == "history-race" and history_calls > 1 else 2
            return json.dumps([{"revision": revision, "status": "deployed"}])
        if command[:3] in (["helm", "get", "manifest"], ["helm", "get", "hooks"]):
            assert command[command.index("--revision") + 1] == "2"
            hooks = command[2] == "hooks"
            return yaml.safe_dump_all(
                [
                    row
                    for row in stored
                    if bool(row["metadata"].get("annotations", {}).get("helm.sh/hook"))
                    == hooks
                ]
            )
        if command[:2] == ["helm", "template"]:
            assert "--is-upgrade" in command
            return yaml.safe_dump_all(desired)
        raise AssertionError("recording must never mutate resources")

    monkeypatch.setattr(operator, "run", run)
    monkeypatch.setattr(
        operator,
        "get_resource",
        lambda resource: live[(resource["kind"], resource["metadata"]["name"])],
    )
    if change == "none":
        inventory = operator.record_bridge(chart, [path], "custos", "controllers")
        assert inventory["renderingSha256"] == operator.rendering_digest(stored)
        assert inventory["revision"] == 2
        assert len(inventory["resources"]) == 13
    else:
        with pytest.raises(
            operator.OperatorValidationError, match="full rendering|history changed"
        ):
            operator.record_bridge(chart, [path], "custos", "controllers")


@pytest.mark.parametrize(
    "change", ["auth-image", "hook-image", "missing-pin", "wrong-pin"]
)
def test_recovery_checks_the_complete_render_before_live_access(
    change: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Even supplied artifact hashes cannot bypass the recorded full rendering before reconciliation."""
    inventory, _ = retained_fixture()
    config = bridge_configuration()
    if change == "auth-image":
        config.setdefault("auth-service", {}).setdefault("image", {})["tag"] = (
            "unverified"
        )
    desired = [
        row
        for row in yaml.safe_load_all(render("connected-eval", config).stdout)
        if row
    ]
    if change == "hook-image":
        hook = next(
            row
            for row in desired
            if row["kind"] == "Job"
            and row["metadata"].get("annotations", {}).get("helm.sh/hook")
        )
        hook["spec"]["template"]["spec"]["containers"][0]["image"] = "unverified:2"
    elif change == "missing-pin":
        inventory.pop("renderingSha256")
    elif change == "wrong-pin":
        inventory["renderingSha256"] = "0" * 64
    chart = tmp_path / "saved.tgz"
    chart.write_bytes(b"synthetic saved chart archive")
    path = tmp_path / "values.yaml"
    path.write_text(yaml.safe_dump(config))
    inventory["artifacts"] = operator.artifact_digests(chart, [path])

    def run(command: list[str], payload: Any = None) -> str:
        """Permit rendering only; no live query, patch or upgrade may precede the full check."""
        assert command[:2] == ["helm", "template"]
        return yaml.safe_dump_all(desired)

    monkeypatch.setattr(operator, "run", run)
    with pytest.raises(operator.OperatorValidationError, match="full rendering"):
        operator.recover_bridge(inventory, chart, [path])


@pytest.mark.parametrize(
    "reference", ["collision", "resource", "configuration", "role"]
)
def test_bridge_cleanup_uses_typed_references(
    reference: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A same-named NetworkPolicy does not retain Dapr policy; actual identities/references do."""
    inventory, live = retained_fixture()
    configuration_resource = next(
        row["resource"]
        for row in inventory["resources"]
        if row["resource"]["kind"] == "Configuration"
    )
    role_resource = next(
        row["resource"]
        for row in inventory["resources"]
        if row["resource"]["kind"] == "Role"
    )
    config_name = configuration_resource["metadata"]["name"]
    document: dict[str, Any] = {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {"name": config_name, "namespace": "controllers"},
    }
    if reference == "resource":
        document = configuration_resource
    elif reference == "configuration":
        document = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "consumer"},
            "spec": {
                "template": {
                    "metadata": {"annotations": {"dapr.io/config": config_name}}
                }
            },
        }
    elif reference == "role":
        document = {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": {"name": "retained-binding"},
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "Role",
                "name": role_resource["metadata"]["name"],
            },
        }
    writes = []
    for resource in live.values():
        resource["metadata"]["resourceVersion"] = "1"

    def run(command: list[str], payload: Any = None) -> str:
        """Emulate an otherwise unused bridge with one manifest and guarded individual deletes."""
        if command[:2] == ["kubectl", "get"]:
            return '{"items":[]}'
        if command[:2] == ["helm", "list"]:
            return '[{"name":"custos"}]'
        if command[:2] == ["helm", "history"]:
            return '[{"revision":2}]'
        if command[:3] == ["helm", "get", "manifest"]:
            return yaml.safe_dump(document)
        assert command[:2] == ["kubectl", "delete"]
        writes.append((command, payload))
        return "{}"

    monkeypatch.setattr(operator, "run", run)
    monkeypatch.setattr(
        operator,
        "get_resource",
        lambda resource: live[(resource["kind"], resource["metadata"]["name"])],
    )
    if reference != "collision":
        with pytest.raises(ValueError, match="retained Helm"):
            operator.cleanup_resources(inventory, confirmed_no_retained_work=True)
        assert writes == []
    else:
        operator.cleanup_resources(inventory, confirmed_no_retained_work=True)
        assert len(writes) == 12
        assert all("/namespaces/controllers/" in command[3] for command, _ in writes)
        assert all(
            payload["preconditions"]["resourceVersion"] == "1" for _, payload in writes
        )


@pytest.mark.parametrize(
    "change",
    [
        "none",
        "missing",
        "uid",
        "policy",
        "owner",
        "labels",
        "digest",
        "lifecycle-owner",
        "terminating",
        "namespace-owner",
    ],
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
    elif change in {"lifecycle-owner", "namespace-owner"}:
        key = ("Namespace", "controllers") if change == "namespace-owner" else role_key
        live[key]["metadata"]["ownerReferences"] = [{"uid": "garbage-collection-owner"}]
    elif change == "terminating":
        live[role_key]["metadata"]["deletionTimestamp"] = "2026-01-01T00:00:00Z"
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


@pytest.mark.parametrize("section", [None, [], "private-canary"])
def test_stored_descriptor_rejects_non_mapping_sections(section: object) -> None:
    """Malformed retained values fail with a fixed domain reason, never an attribute traceback."""
    for config in (
        {"global": section},
        {"global": {"receiverRuntime": section}},
        {"global": {"receiverRuntime": {"executionBindingsBundle": section}}},
    ):
        with pytest.raises(operator.OperatorValidationError, match="must be mappings"):
            operator.execution_descriptor(config)
    assert operator.execution_descriptor({}) == {}


@pytest.mark.parametrize(
    "target", ["deployed-bridge", "superseded-bridge", "file", "no-success"]
)
def test_atomic_retry_checks_actual_successful_rollback_target(
    target: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed file revision cannot hide the inline bridge Helm would choose for atomic rollback."""
    previous = operator.prepare(fixture(1), configuration(), "custos", "controllers")
    bundle = operator.prepare(fixture(2), configuration(), "custos", "controllers")
    requested = []

    def run(command: list[str], payload: Any = None) -> str:
        """Expose a failed latest revision and forbid every mutation during transition preflight."""
        if command[:3] == ["kubectl", "get", "namespace"]:
            return '{"metadata":{"name":"controllers"}}'
        if command[:2] == ["helm", "list"]:
            return '[{"name":"custos"}]'
        if command[:3] == ["helm", "get", "manifest"]:
            return ""
        if command[:2] == ["helm", "history"]:
            assert command[command.index("--max") + 1] == "2147483647"
            rows = [{"revision": 3, "status": "failed"}]
            if target == "file":
                rows.extend(
                    [
                        {"revision": 2, "status": "deployed"},
                        {"revision": 1, "status": "superseded"},
                    ]
                )
            elif target != "no-success":
                rows.append({"revision": 1, "status": target.split("-")[0]})
            return json.dumps(rows)
        if command[:3] == ["helm", "get", "values"]:
            requested.append(int(command[command.index("--revision") + 1]))
            config = configuration()
            if target == "file":
                config["global"]["receiverRuntime"]["executionBindingsBundle"] = (
                    previous["descriptor"]
                )
            return json.dumps(config)
        raise AssertionError("atomic preflight must not mutate resources")

    monkeypatch.setattr(operator, "run", run)
    if target == "file":
        operator.transition_preflight(bundle, atomic=True)
        assert requested == [2]
    else:
        with pytest.raises(ValueError, match="prohibited|successful"):
            operator.transition_preflight(bundle, atomic=True)
        assert requested == ([] if target == "no-success" else [1])


@pytest.mark.parametrize("field", ["ownerReferences", "deletionTimestamp"])
def test_bridge_cleanup_rejects_lifecycle_ownership(
    field: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Kept Helm objects cannot be treated as retained while another lifecycle can delete them."""
    inventory, live = retained_fixture()
    role = next(row for row in live.values() if row["kind"] == "Role")
    role["metadata"][field] = (
        [{"uid": "garbage-collection-owner"}]
        if field == "ownerReferences"
        else "2026-01-01T00:00:00Z"
    )
    writes = []

    def run(command: list[str], payload: Any = None) -> str:
        """Return no work references but prohibit deletion of any policy."""
        if command[:2] == ["kubectl", "get"]:
            return '{"items":[]}'
        if command[:2] == ["helm", "list"]:
            return "[]"
        writes.append(command)
        raise AssertionError("lifecycle ownership must prevent every deletion")

    monkeypatch.setattr(operator, "run", run)
    monkeypatch.setattr(
        operator,
        "get_resource",
        lambda resource: live[(resource["kind"], resource["metadata"]["name"])],
    )
    with pytest.raises(ValueError, match="ownership"):
        operator.cleanup_resources(inventory, confirmed_no_retained_work=True)
    assert writes == []


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

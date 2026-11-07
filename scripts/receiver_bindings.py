"""Prepare and create-or-verify reference-only receiver bundles outside Helm storage.

Run with the repository's Helm-test dependencies and installed custos-callctx.
No Kubernetes Secret values are requested directly. Bundle resources are create-only and
must remain available for retained work and rollback; ordinary uninstall never
deletes them. Output files contain references and must still be operator-owned.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml
from custos_callctx.execution import ExecutionBindings, read_execution_snapshot

CONSUMERS = (
    "api-gateway",
    "trigger-service",
    "workflow-service",
    "activity-runtime-manager",
)
DIGEST_ANNOTATION = "custos.io/execution-bundle-sha256"
CHART = Path(__file__).resolve().parents[1] / "deploy/helm/custos"
FILE_ENVS = {"CUSTOS_EXECUTION_BINDINGS_FILE", "CUSTOS_EXECUTION_BINDINGS_SHA256"}
SOURCE_ENVS = FILE_ENVS | {
    "CUSTOS_EXECUTION_BINDINGS",
    "CUSTOS_EXECUTION_IDENTITY_ENABLED",
}


class OperatorCommandError(ValueError):
    """A failed operator subprocess carrying only a safe tool/operation/exit summary."""


class OperatorValidationError(ValueError):
    """A helper-authored rejection with a safe fixed message, never raw input or parser details."""


def canonical(value: Any) -> bytes:
    """Encode deterministic JSON for digests and Kubernetes requests."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()


def name(value: str) -> str:
    """Validate Kubernetes DNS-subdomain references without coercion/truncation."""
    if (
        not isinstance(value, str)
        or len(value) > 253
        or re.fullmatch(
            r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]*[a-z0-9])?)*",
            value,
        )
        is None
    ):
        raise OperatorValidationError("invalid deployment resource name")
    return value


def require_namespace(
    resource: dict[str, Any], namespace: str, *, allow_implicit: bool = False
) -> None:
    """Reject foreign namespaces; rendered manifests may inherit Helm's release namespace."""
    if (
        resource["metadata"].get("namespace", namespace if allow_implicit else None)
        != namespace
    ):
        raise OperatorValidationError("resource belongs to a different namespace")


def merge(base: dict[str, Any], update: dict[str, Any]) -> dict[str, Any]:
    """Merge Helm mapping overrides; lists/scalars replace rather than concatenate."""
    result = copy.deepcopy(base)
    for key, value in update.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def values(paths: list[Path], chart: Path = CHART) -> dict[str, Any]:
    """Resolve the same ordered baseline/profile/override files passed to Helm."""
    result: dict[str, Any] = {}
    for path in [chart / "values.yaml", *paths]:
        data = yaml.safe_load(path.read_text())
        if not isinstance(data, dict):
            raise TypeError("values file must contain a mapping")
        result = merge(result, data)
    return result


def prepare(
    raw: bytes, config: dict[str, Any], release: str, namespace: str
) -> dict[str, Any]:
    """Build deterministic immutable registry and equivalent named-Secret/Dapr policies.

    The shared chart caller matrix is the only caller-policy source. Exact
    identity/default semantics come from the production file parser.
    """
    name(release)
    name(namespace)
    if len(release) > 53 or len(namespace) > 63 or "." in namespace:
        raise OperatorValidationError("invalid Helm release or namespace length")
    if len(raw) > 1_000_000:
        raise OperatorValidationError("execution binding file exceeds 1000000 bytes")
    digest = hashlib.sha256(raw).hexdigest()
    # Apply the production semantic parser after this source's independent cap.
    bindings = ExecutionBindings.from_file_bytes(raw)
    if not bindings.bindings:
        raise OperatorValidationError(
            "managed bundle requires explicit execution bindings"
        )
    runtime = config["global"]["receiverRuntime"]
    database = name(runtime["databaseSecret"])
    broker = name(config["dapr"]["components"]["pubsub"]["redis"]["passwordSecret"])
    secrets = sorted({name(row.secret_ref) for row in bindings.bindings})
    for row in bindings.bindings:
        if re.fullmatch(r"[-._a-zA-Z0-9]+", row.secret_key) is None:
            raise OperatorValidationError(
                "execution Secret key is not Kubernetes-compatible"
            )
    callers = json.loads((CHART / "files/receiver-callers.json").read_text())
    inputs: dict[str, Any] = {
        "sha256": digest,
        "release": release,
        "namespace": namespace,
        "database": database,
        "broker": broker,
        "callers": {service: callers[service] for service in CONSUMERS},
        "serviceAccounts": {
            service: f"{release}-{service}"[:63].rstrip("-") for service in CONSUMERS
        },
    }
    bundle_digest = hashlib.sha256(canonical(inputs)).hexdigest()
    prefix = f"{release[:20].rstrip('.-')}-exec-{bundle_digest[:20]}"
    objects: list[dict[str, Any]] = [
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": prefix,
                "namespace": namespace,
                "annotations": {DIGEST_ANNOTATION: bundle_digest},
            },
            "immutable": True,
            "data": {"bindings.json": raw.decode("utf-8")},
        }
    ]
    configurations: dict[str, str] = {}
    for index, service in enumerate(CONSUMERS):
        resource_name = f"{prefix}-{index}"
        configurations[service] = resource_name
        metadata = {
            "name": resource_name,
            "namespace": namespace,
            "annotations": {DIGEST_ANNOTATION: bundle_digest},
        }
        objects.extend(
            [
                {
                    "apiVersion": "dapr.io/v1alpha1",
                    "kind": "Configuration",
                    "metadata": copy.deepcopy(metadata),
                    "spec": {
                        "accessControl": {
                            "defaultAction": "deny",
                            "trustDomain": "public",
                            "policies": [
                                {
                                    "appId": "custos-auth"
                                    if caller == "auth-service"
                                    else caller,
                                    "namespace": namespace,
                                    "trustDomain": "public",
                                    "defaultAction": "allow",
                                }
                                for caller in callers[service]
                            ],
                        },
                        "secrets": {
                            "scopes": [
                                {
                                    "storeName": "kubernetes",
                                    "defaultAccess": "deny",
                                    "allowedSecrets": list(
                                        dict.fromkeys([*secrets, database, broker])
                                    ),
                                }
                            ]
                        },
                    },
                },
                {
                    "apiVersion": "rbac.authorization.k8s.io/v1",
                    "kind": "Role",
                    "metadata": copy.deepcopy(metadata),
                    "rules": [
                        {
                            "apiGroups": [""],
                            "resources": ["secrets"],
                            "resourceNames": secrets,
                            "verbs": ["get"],
                        }
                    ],
                },
                {
                    "apiVersion": "rbac.authorization.k8s.io/v1",
                    "kind": "RoleBinding",
                    "metadata": copy.deepcopy(metadata),
                    "roleRef": {
                        "apiGroup": "rbac.authorization.k8s.io",
                        "kind": "Role",
                        "name": resource_name,
                    },
                    "subjects": [
                        {
                            "kind": "ServiceAccount",
                            "name": inputs["serviceAccounts"][service],
                            "namespace": namespace,
                        }
                    ],
                },
            ]
        )
    return {
        "release": release,
        "namespace": namespace,
        "inputs": inputs,
        "objects": objects,
        "descriptor": {
            "configMapName": prefix,
            "sha256": digest,
            "bundleSha256": bundle_digest,
            "daprConfigurations": configurations,
        },
    }


def run(command: list[str], payload: Any = None) -> str:
    """Run an explicit operator command; suppress potentially sensitive tool stderr on failure."""
    result = subprocess.run(
        command,
        input=None if payload is None else canonical(payload).decode(),
        capture_output=True,
        text=True,
        timeout=1500,
        check=False,
    )
    if result.returncode:
        raise OperatorCommandError(
            f"{command[0]} {command[1]} failed (exit {result.returncode})"
        )
    return result.stdout


def effective(resource: dict[str, Any]) -> dict[str, Any]:
    """Compare managed content, normalizing metadata and Dapr's enabled metric defaults."""
    metadata = resource["metadata"]
    result = {
        "apiVersion": resource["apiVersion"],
        "kind": resource["kind"],
        "name": metadata["name"],
        "namespace": metadata.get("namespace"),
        "digest": metadata.get("annotations", {}).get(DIGEST_ANNOTATION),
        **{
            key: resource[key]
            for key in (
                "data",
                "binaryData",
                "immutable",
                "spec",
                "rules",
                "subjects",
                "roleRef",
            )
            if key in resource
            and not (resource["kind"] == "Namespace" and key == "spec")
        },
    }
    if resource["kind"] == "Configuration":
        spec = copy.deepcopy(resource.get("spec", {}))
        for field in ("metric", "metrics"):
            spec.setdefault(field, {"enabled": True})
        result["spec"] = spec
    return result


def get_resource(resource: dict[str, Any]) -> dict[str, Any] | None:
    """Fetch one named object; absence is distinct from API/permission failure."""
    metadata = resource["metadata"]
    command = ["kubectl", "get", resource["kind"], metadata["name"]]
    if resource["kind"] != "Namespace":
        command.extend(["-n", metadata["namespace"]])
    output = run([*command, "--ignore-not-found", "-o", "json"])
    return json.loads(output) if output.strip() else None


def verify_bundle(bundle: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    """Regenerate and compare all bundle content before trusting local artifact names/policies."""
    cm = bundle["objects"][0]
    raw = cm["data"]["bindings.json"].encode()
    expected = prepare(raw, config, bundle["release"], bundle["namespace"])
    if expected != bundle:
        raise OperatorValidationError(
            "bundle content or deployment/security inputs changed"
        )
    return expected


def execution_descriptor(config: Any) -> dict[str, Any]:
    """Read stored binding values, preserving omitted inline paths and rejecting non-mapping sections."""
    for key in ("global", "receiverRuntime", "executionBindingsBundle"):
        if not isinstance(config, dict):
            raise OperatorValidationError(
                "retained Helm binding values must be mappings"
            )
        config = config.get(key, {})
    if not isinstance(config, dict):
        raise OperatorValidationError("retained Helm binding values must be mappings")
    return config


def transition_preflight(
    bundle: dict[str, Any],
    inventory: dict[str, Any] | None = None,
    *,
    atomic: bool = False,
    rollback_revision: int | None = None,
) -> None:
    """Reject missing namespaces, unprotected namespace ownership and unbridged inline releases."""
    namespace = bundle["namespace"]
    ns = json.loads(run(["kubectl", "get", "namespace", namespace, "-o", "json"]))
    annotations = ns["metadata"].get("annotations", {})
    owner = annotations.get("meta.helm.sh/release-name")
    owner_namespace = annotations.get("meta.helm.sh/release-namespace")
    helm_managed = (
        ns["metadata"].get("labels", {}).get("app.kubernetes.io/managed-by") == "Helm"
    )
    if (owner or owner_namespace or helm_managed) and (
        owner != bundle["release"] or owner_namespace != namespace
    ):
        raise OperatorValidationError(
            "namespace is owned by a different Helm release or release namespace"
        )
    if ns["metadata"].get("deletionTimestamp") or ns["metadata"].get("ownerReferences"):
        raise OperatorValidationError("namespace has an unprotected lifecycle owner")
    releases = json.loads(
        run(
            [
                "helm",
                "list",
                "--all",
                "-n",
                namespace,
                "--filter",
                "^" + re.escape(bundle["release"]) + "$",
                "-o",
                "json",
            ]
        )
    )
    if not releases:
        if owner or rollback_revision is not None:
            raise OperatorValidationError(
                "namespace retention requires verified owning release"
            )
        return
    if rollback_revision is not None:
        if rollback_revision < 1:
            raise OperatorValidationError("rollback revision must be positive")
        target = json.loads(
            run(
                [
                    "helm",
                    "get",
                    "values",
                    bundle["release"],
                    "-n",
                    namespace,
                    "--revision",
                    str(rollback_revision),
                    "-o",
                    "json",
                ]
            )
        )
        descriptor = execution_descriptor(target)
        if not descriptor or descriptor != bundle["descriptor"]:
            raise OperatorValidationError(
                "native inline bridge rollback is prohibited; verify a retained file target"
            )
    manifest = run(["helm", "get", "manifest", bundle["release"], "-n", namespace])
    docs = [doc for doc in yaml.safe_load_all(manifest) if doc]
    legacy = [
        doc
        for doc in docs
        if doc["kind"] in {"Configuration", "Role", "RoleBinding"}
        and any(
            doc["metadata"]["name"] == f"{bundle['release']}-{middle}-{service}"
            for service in CONSUMERS
            for middle in ("receivers", "execution-secrets")
        )
    ]
    namespace_docs = [
        doc
        for doc in docs
        if doc["kind"] == "Namespace" and doc["metadata"]["name"] == namespace
    ]
    if (
        owner
        and not namespace_docs
        and annotations.get("helm.sh/resource-policy") != "keep"
    ):
        raise OperatorValidationError("namespace owner retention is not established")
    if legacy and len(legacy) != 12:
        raise OperatorValidationError("incomplete legacy authorization bridge")
    if legacy and atomic:
        raise OperatorValidationError("atomic first file migration is prohibited")
    if atomic:
        history = json.loads(
            run(
                [
                    "helm",
                    "history",
                    bundle["release"],
                    "-n",
                    namespace,
                    "--max",
                    "2147483647",
                    "-o",
                    "json",
                ]
            )
        )
        if not isinstance(history, list) or not all(
            isinstance(row, dict)
            and type(row.get("revision")) is int
            and row["revision"] > 0
            for row in history
        ):
            raise TypeError("invalid Helm revision inventory")
        successful = [
            row for row in history if row.get("status") in {"deployed", "superseded"}
        ]
        if not successful:
            raise OperatorValidationError(
                "atomic upgrade requires a successful retained file predecessor"
            )
        revision = max(successful, key=lambda row: row["revision"])["revision"]
        target = json.loads(
            run(
                [
                    "helm",
                    "get",
                    "values",
                    bundle["release"],
                    "-n",
                    namespace,
                    "--revision",
                    str(revision),
                    "-o",
                    "json",
                ]
            )
        )
        descriptor = execution_descriptor(target)
        if not isinstance(descriptor, dict) or set(descriptor) != set(
            bundle["descriptor"]
        ):
            raise OperatorValidationError(
                "atomic rollback to an inline bridge is prohibited"
            )
    if inventory is not None:
        if (inventory.get("release"), inventory.get("namespace")) != (
            bundle["release"],
            namespace,
        ):
            raise OperatorValidationError(
                "bridge inventory belongs to a different deployment"
            )
        rendering_pin = inventory.get("renderingSha256")
        if (
            not isinstance(rendering_pin, str)
            or re.fullmatch(r"[0-9a-f]{64}", rendering_pin) is None
        ):
            raise OperatorValidationError(
                "bridge inventory lacks a complete rendering digest"
            )
        verify_retained(inventory)
    if legacy:
        if inventory is None or (
            inventory.get("release"),
            inventory.get("namespace"),
        ) != (bundle["release"], namespace):
            raise OperatorValidationError(
                "first migration requires recorded bridge inventory"
            )
        recorded = {
            (row["resource"]["kind"], row["resource"]["metadata"]["name"]): row
            for row in inventory["resources"]
        }
    for doc in [*legacy, *namespace_docs]:
        if (
            doc["metadata"].get("annotations", {}).get("helm.sh/resource-policy")
            != "keep"
        ):
            raise OperatorValidationError(
                "stored Helm manifest lacks required retention bridge"
            )
        if doc["kind"] == "Namespace":
            live = ns
        else:
            live = get_resource(doc)
        if live is None or effective(live) != effective(doc):
            raise OperatorValidationError(
                "legacy bridge live content differs from stored release"
            )
        if doc["kind"] == "Namespace" and any(
            live["metadata"].get("labels", {}).get(key) != value
            for key, value in doc["metadata"].get("labels", {}).items()
        ):
            raise OperatorValidationError("namespace security labels changed")
        if not live["metadata"].get("uid"):
            raise OperatorValidationError("retained resource identity is missing")
        live_metadata = live["metadata"]
        live_annotations = live_metadata.get("annotations", {})
        if (
            live_metadata.get("ownerReferences")
            or live_metadata.get("deletionTimestamp")
            or live_annotations.get("meta.helm.sh/release-name") != bundle["release"]
            or live_annotations.get("meta.helm.sh/release-namespace") != namespace
            or live_metadata.get("labels", {}).get("app.kubernetes.io/managed-by")
            != "Helm"
        ):
            raise OperatorValidationError(
                "legacy resource has conflicting Helm ownership"
            )
        if legacy:
            row = recorded.get((doc["kind"], doc["metadata"]["name"]))
            if (
                row is None
                or row["uid"] != live["metadata"]["uid"]
                or effective(row["resource"]) != effective(doc)
            ):
                raise OperatorValidationError(
                    "retained resource differs from recorded bridge inventory"
                )


def provision(
    bundle: dict[str, Any],
    *,
    create: bool,
    inventory: dict[str, Any] | None = None,
    atomic: bool = False,
    rollback_revision: int | None = None,
) -> None:
    """Preflight then create-or-verify all resources, never patch/replace existing objects."""
    transition_preflight(
        bundle, inventory, atomic=atomic, rollback_revision=rollback_revision
    )
    missing = []
    for resource in bundle["objects"]:
        live = get_resource(resource)
        if live is not None:
            if (
                effective(live) != effective(resource)
                or live["metadata"].get("ownerReferences")
                or live["metadata"].get("deletionTimestamp")
                or live["metadata"]
                .get("annotations", {})
                .get("meta.helm.sh/release-name")
            ):
                raise OperatorValidationError(
                    "existing bundle resource conflicts; no overwrite allowed"
                )
        elif create:
            missing.append(resource)
        else:
            raise OperatorValidationError("required bundle resource is missing")
    for resource in missing:
        run(
            ["kubectl", "create", "--dry-run=server", "-f", "-", "-o", "json"], resource
        )
    for resource in missing:
        run(["kubectl", "create", "-f", "-", "-o", "json"], resource)
        live = get_resource(resource)
        if live is None or effective(live) != effective(resource):
            raise OperatorValidationError(
                f"created bundle {resource['kind']} could not be verified"
            )


def reconcile_source(
    live: dict[str, Any], target: dict[str, Any]
) -> list[dict[str, Any]]:
    """Create a resource-version-tested patch for managed source fields only.

    Normal template rollouts replace Pods. Other environment, mounts, volumes
    and annotations survive, including concurrent operator values.
    """
    old = live["spec"]["template"]
    bridge = target["spec"]["template"]
    desired = copy.deepcopy(old)
    old_container = desired["spec"]["containers"][0]
    target_container = bridge["spec"]["containers"][0]
    old_container["env"] = [
        entry
        for entry in old_container.get("env", [])
        if entry["name"] not in SOURCE_ENVS
    ] + [entry for entry in target_container["env"] if entry["name"] in SOURCE_ENVS]
    if any(entry["name"] in FILE_ENVS for entry in old_container["env"]):
        raise OperatorValidationError("bridge template still selects file mode")
    inline = [
        entry
        for entry in old_container["env"]
        if entry["name"] == "CUSTOS_EXECUTION_BINDINGS"
    ]
    if len(inline) != 1:
        raise OperatorValidationError("bridge requires exactly one inline source")
    ExecutionBindings.from_json(inline[0]["value"])
    for owner, key in ((old_container, "volumeMounts"), (desired["spec"], "volumes")):
        if key in owner:
            owner[key] = [
                row for row in owner[key] if row["name"] != "execution-bindings"
            ]
    annotations = desired["metadata"].setdefault("annotations", {})
    for key in ("checksum/execution-bindings", "checksum/execution-bundle"):
        annotations.pop(key, None)
    annotations["dapr.io/config"] = bridge["metadata"]["annotations"]["dapr.io/config"]
    patches = [
        {
            "op": "test",
            "path": "/metadata/resourceVersion",
            "value": live["metadata"]["resourceVersion"],
        },
    ]
    for path, value in (
        ("/spec/template/spec/containers/0/env", old_container["env"]),
        ("/spec/template/metadata/annotations", annotations),
    ):
        patches.append({"op": "add", "path": path, "value": value})
    for container, key, path in (
        (
            old_container,
            "volumeMounts",
            "/spec/template/spec/containers/0/volumeMounts",
        ),
        (desired["spec"], "volumes", "/spec/template/spec/volumes"),
    ):
        if key in container:
            patches.append({"op": "add", "path": path, "value": container[key]})
    return patches


def source_fields(deployment: dict[str, Any]) -> dict[str, Any]:
    """Extract the exact managed source/annotation/mount contract for post-patch verification."""
    pod = deployment["spec"]["template"]
    container = pod["spec"]["containers"][0]
    return {
        "env": [row for row in container.get("env", []) if row["name"] in SOURCE_ENVS],
        "mounts": [
            row
            for row in container.get("volumeMounts", [])
            if row["name"] == "execution-bindings"
        ],
        "volumes": [
            row
            for row in pod["spec"].get("volumes", [])
            if row["name"] == "execution-bindings"
        ],
        "annotations": {
            key: value
            for key, value in pod["metadata"].get("annotations", {}).items()
            if key
            in {
                "checksum/execution-bindings",
                "checksum/execution-bundle",
                "dapr.io/config",
            }
        },
    }


def bridge_resources(
    docs: list[dict[str, Any]], release: str, namespace: str
) -> list[dict[str, Any]]:
    """Select twelve controller-namespace legacy policies and any chart-owned Namespace."""
    resources = [
        doc
        for doc in docs
        if (
            doc["kind"] in {"Configuration", "Role", "RoleBinding"}
            and any(
                doc["metadata"]["name"] == f"{release}-{middle}-{service}"
                for service in CONSUMERS
                for middle in ("receivers", "execution-secrets")
            )
        )
        or (doc["kind"] == "Namespace" and doc["metadata"]["name"] == namespace)
    ]
    if len([doc for doc in resources if doc["kind"] != "Namespace"]) != 12:
        raise OperatorValidationError(
            "bridge must contain all twelve legacy authorization resources"
        )
    for doc in resources:
        if doc["kind"] != "Namespace":
            require_namespace(doc, namespace)
        if (
            doc["metadata"].get("annotations", {}).get("helm.sh/resource-policy")
            != "keep"
        ):
            raise OperatorValidationError(
                "bridge stored manifest lacks retention annotations"
            )
    return resources


def rendered_bridge(
    chart: Path, paths: list[Path], release: str, namespace: str
) -> list[dict[str, Any]]:
    """Render the saved bridge in upgrade context with complete values, never live values."""
    command = [
        "helm",
        "template",
        release,
        str(chart),
        "-n",
        namespace,
        "--kube-version",
        "1.29.0",
        "--is-upgrade",
    ]
    for path in paths:
        command.extend(["-f", str(path)])
    return [doc for doc in yaml.safe_load_all(run(command)) if doc]


def rendering_digest(docs: list[dict[str, Any]]) -> str:
    """Fingerprint every manifest/hook field, independent of document and mapping order.

    Duplicate document multiplicity is preserved. Only the digest is retained;
    complete Helm manifests can contain sensitive operator configuration.
    """
    return hashlib.sha256(
        b"\n".join(sorted(canonical(doc) for doc in docs))
    ).hexdigest()


def artifact_digests(chart: Path, paths: list[Path]) -> dict[str, Any]:
    """Bind recovery to a packaged chart and ordered complete values without storing their data."""
    if not chart.is_file() or not paths:
        raise OperatorValidationError(
            "bridge requires a saved chart archive and complete values files"
        )
    return {
        "chart": hashlib.sha256(chart.read_bytes()).hexdigest(),
        "values": [hashlib.sha256(path.read_bytes()).hexdigest() for path in paths],
    }


def record_bridge(
    chart: Path, paths: list[Path], release: str, namespace: str
) -> dict[str, Any]:
    """Pin the full stored bridge and hooks to the recovery upgrade render and artifacts.

    All reads use one deployed revision; concurrent history changes abort
    recording. Retained policy identities/ownership are also checked live.
    """
    history_command = ["helm", "history", release, "-n", namespace, "-o", "json"]
    history = json.loads(run(history_command))
    if (
        not isinstance(history, list)
        or not history
        or not isinstance(history[-1], dict)
        or history[-1].get("status") != "deployed"
        or type(history[-1].get("revision")) is not int
        or history[-1]["revision"] < 1
    ):
        raise OperatorValidationError("bridge revision must be successfully deployed")
    revision = history[-1]["revision"]
    docs = [
        doc
        for doc in yaml.safe_load_all(
            run(
                [
                    "helm",
                    "get",
                    "manifest",
                    release,
                    "-n",
                    namespace,
                    "--revision",
                    str(revision),
                ]
            )
        )
        if doc
    ]
    hooks = [
        doc
        for doc in yaml.safe_load_all(
            run(
                [
                    "helm",
                    "get",
                    "hooks",
                    release,
                    "-n",
                    namespace,
                    "--revision",
                    str(revision),
                ]
            )
        )
        if doc
    ]
    stored = bridge_resources(docs, release, namespace)
    rendered = rendered_bridge(chart, paths, release, namespace)
    bridge_resources(rendered, release, namespace)
    rendering_pin = rendering_digest([*docs, *hooks])
    if rendering_pin != rendering_digest(rendered):
        raise OperatorValidationError(
            "saved bridge full rendering differs from stored Helm release"
        )
    records = []
    for doc in stored:
        live = get_resource(doc)
        if live is None or effective(live) != effective(doc):
            raise OperatorValidationError(
                "bridge live resources differ from stored manifest"
            )
        if doc["kind"] == "Namespace" and any(
            live["metadata"].get("labels", {}).get(key) != value
            for key, value in doc["metadata"].get("labels", {}).items()
        ):
            raise OperatorValidationError(
                "bridge namespace security labels differ from stored manifest"
            )
        metadata = live["metadata"]
        annotations = metadata.get("annotations", {})
        if (
            metadata.get("ownerReferences")
            or metadata.get("deletionTimestamp")
            or annotations.get("meta.helm.sh/release-name") != release
            or annotations.get("meta.helm.sh/release-namespace") != namespace
            or metadata.get("labels", {}).get("app.kubernetes.io/managed-by") != "Helm"
        ):
            raise OperatorValidationError(
                "bridge resource has conflicting Helm ownership"
            )
        records.append(
            {
                "resource": doc,
                "uid": metadata["uid"],
                "labels": metadata.get("labels", {}),
                "sha256": hashlib.sha256(canonical(effective(doc))).hexdigest(),
            }
        )
    if json.loads(run(history_command)) != history:
        raise OperatorValidationError("bridge release history changed while recording")
    return {
        "release": release,
        "namespace": namespace,
        "artifacts": artifact_digests(chart, paths),
        "revision": revision,
        "renderingSha256": rendering_pin,
        "resources": records,
    }


def verify_retained(inventory: dict[str, Any]) -> None:
    """Verify recorded legacy content, unique identifiers, ownership and Namespace labels."""
    release, namespace = inventory["release"], inventory["namespace"]
    bridge_resources(
        [row["resource"] for row in inventory["resources"]], release, namespace
    )
    for record in inventory["resources"]:
        if (
            hashlib.sha256(canonical(effective(record["resource"]))).hexdigest()
            != record["sha256"]
        ):
            raise OperatorValidationError("retained bridge content digest changed")
        live = get_resource(record["resource"])
        if (
            live is None
            or live["metadata"]["uid"] != record["uid"]
            or effective(live) != effective(record["resource"])
        ):
            raise OperatorValidationError("retained bridge identity/policy changed")
        metadata = live["metadata"]
        annotations = metadata.get("annotations", {})
        if (
            metadata.get("ownerReferences")
            or metadata.get("deletionTimestamp")
            or annotations.get("meta.helm.sh/release-name") != release
            or annotations.get("meta.helm.sh/release-namespace") != namespace
            or metadata.get("labels", {}).get("app.kubernetes.io/managed-by") != "Helm"
        ):
            raise OperatorValidationError("retained bridge ownership changed")
        if (
            live["kind"] == "Namespace"
            and metadata.get("labels", {}) != record["labels"]
        ):
            raise OperatorValidationError("retained namespace labels changed")


def verify_recovery_deployment(
    deployment: dict[str, Any], deployment_name: str, release: str, namespace: str
) -> None:
    """Reject foreign, terminating or controller-owned live Deployments before recovery."""
    require_namespace(deployment, namespace)
    metadata = deployment["metadata"]
    annotations = metadata.get("annotations", {})
    if (
        deployment.get("apiVersion") != "apps/v1"
        or deployment.get("kind") != "Deployment"
        or metadata.get("name") != deployment_name
        or metadata.get("ownerReferences")
        or metadata.get("deletionTimestamp")
        or annotations.get("meta.helm.sh/release-name") != release
        or annotations.get("meta.helm.sh/release-namespace") != namespace
        or metadata.get("labels", {}).get("app.kubernetes.io/managed-by") != "Helm"
    ):
        raise OperatorValidationError(
            "recovery Deployment identity or ownership changed"
        )


def recover_bridge(inventory: dict[str, Any], chart: Path, paths: list[Path]) -> None:
    """Reconcile managed live fields then recover via an ownership-checked normal upgrade.

    The operator must pause admission/drain first. Current live identities must
    all resolve through the inline target; otherwise file-aware recovery is
    required. The complete recovery render, including non-consumers and hooks,
    must match the recorded bridge digest before any live reconciliation.
    All four live Deployment owners and identity references are checked before
    the first patch; resource-version tests reject changes after that preflight.
    No force, ownership bypass, atomic rollback or recreation is used.
    """
    if artifact_digests(chart, paths) != inventory["artifacts"]:
        raise OperatorValidationError("bridge recovery artifacts changed")
    release, namespace = inventory["release"], inventory["namespace"]
    docs = rendered_bridge(chart, paths, release, namespace)
    if rendering_digest(docs) != inventory.get("renderingSha256"):
        raise OperatorValidationError(
            "saved bridge full rendering differs from recorded release"
        )
    expected = bridge_resources(docs, release, namespace)
    if [effective(row["resource"]) for row in inventory["resources"]] != [
        effective(doc) for doc in expected
    ]:
        raise OperatorValidationError("bridge resource inventory changed")
    verify_retained(inventory)
    planned: list[tuple[str, dict[str, Any], list[dict[str, Any]]]] = []
    for service in CONSUMERS:
        deployment_name = f"{release}-{service}"[:63].rstrip("-")
        target = next(
            doc
            for doc in docs
            if doc["kind"] == "Deployment"
            and doc["metadata"]["name"] == deployment_name
        )
        require_namespace(target, namespace, allow_implicit=True)
        live = json.loads(
            run(
                [
                    "kubectl",
                    "get",
                    "deployment",
                    deployment_name,
                    "-n",
                    namespace,
                    "-o",
                    "json",
                ]
            )
        )
        verify_recovery_deployment(live, deployment_name, release, namespace)
        env = {
            row["name"]: row
            for row in live["spec"]["template"]["spec"]["containers"][0]["env"]
        }
        desired_env = {
            row["name"]: row
            for row in target["spec"]["template"]["spec"]["containers"][0]["env"]
        }
        registry = ExecutionBindings.from_json(
            desired_env["CUSTOS_EXECUTION_BINDINGS"]["value"]
        )
        if "CUSTOS_EXECUTION_BINDINGS_FILE" in env:
            volumes = live["spec"]["template"]["spec"]["volumes"]
            cm_name = next(
                row["configMap"]["name"]
                for row in volumes
                if row["name"] == "execution-bindings"
            )
            cm = json.loads(
                run(
                    [
                        "kubectl",
                        "get",
                        "configmap",
                        cm_name,
                        "-n",
                        namespace,
                        "-o",
                        "json",
                    ]
                )
            )
            raw = cm["data"]["bindings.json"].encode()
            if (
                hashlib.sha256(raw).hexdigest()
                != env["CUSTOS_EXECUTION_BINDINGS_SHA256"]["value"]
            ):
                raise OperatorValidationError("live file snapshot digest mismatch")
            current = ExecutionBindings.from_file_bytes(raw)
        else:
            current = ExecutionBindings.from_json(
                env["CUSTOS_EXECUTION_BINDINGS"]["value"]
            )
        for binding in current.bindings:
            if registry.resolve(binding.identity) != binding:
                raise OperatorValidationError(
                    "inline recovery cannot preserve current identity references"
                )
        planned.append((service, target, reconcile_source(live, target)))
    for service, target, patch in planned:
        print(
            f"Execution bridge: reconciling {service}; admission remains paused",
            file=sys.stderr,
        )
        deployment_name = target["metadata"]["name"]
        run(
            [
                "kubectl",
                "patch",
                "deployment",
                deployment_name,
                "-n",
                namespace,
                "--type=json",
                "-p",
                json.dumps(patch),
            ]
        )
        updated = json.loads(
            run(
                [
                    "kubectl",
                    "get",
                    "deployment",
                    deployment_name,
                    "-n",
                    namespace,
                    "-o",
                    "json",
                ]
            )
        )
        verify_recovery_deployment(updated, deployment_name, release, namespace)
        if source_fields(updated) != source_fields(target):
            raise OperatorValidationError(
                f"recovery source verification failed for {service}"
            )
    command = [
        "helm",
        "upgrade",
        release,
        str(chart),
        "-n",
        namespace,
        "--reset-values",
        "--wait",
        "--timeout",
        "20m",
    ]
    for path in paths:
        command.extend(["-f", str(path)])
    run(command)
    for service in CONSUMERS:
        deployment_name = f"{release}-{service}"[:63].rstrip("-")
        run(
            [
                "kubectl",
                "rollout",
                "status",
                f"deployment/{deployment_name}",
                "-n",
                namespace,
                "--timeout=20m",
            ]
        )
        current = json.loads(
            run(
                [
                    "kubectl",
                    "get",
                    "deployment",
                    deployment_name,
                    "-n",
                    namespace,
                    "-o",
                    "json",
                ]
            )
        )
        verify_recovery_deployment(current, deployment_name, release, namespace)
        target = next(
            row
            for row in docs
            if row["kind"] == "Deployment"
            and row["metadata"]["name"] == deployment_name
        )
        if source_fields(current) != source_fields(target):
            raise OperatorValidationError(
                f"recovery source changed after upgrade for {service}"
            )
    verify_retained(inventory)


def references_resources(
    document: Any,
    resources: list[dict[str, Any]],
    namespace: str,
    *,
    source_namespace: str | None = None,
) -> bool:
    """Find managed identities and actual Kubernetes/Dapr references, not name substrings.

    Walk workload templates and List items while preserving their namespace.
    Unqualified documents inherit their release's source namespace, or the
    target namespace for live inventories; explicit namespaces override it.
    A same-named resource of a different kind/group is not a dependency.
    """
    identities = {
        (row["apiVersion"].split("/")[0], row["kind"], row["metadata"]["name"])
        for row in resources
    }
    names = {
        kind: {row["metadata"]["name"] for row in resources if row["kind"] == kind}
        for kind in ("ConfigMap", "Configuration", "Role")
    }

    def referenced(node: Any, current_namespace: str) -> bool:
        """Recognize resource identities, projected maps, Dapr configurations and Role references."""
        if isinstance(node, list):
            return any(referenced(value, current_namespace) for value in node)
        if not isinstance(node, dict):
            return False
        metadata = node.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        declared_namespace = metadata.get("namespace")
        if isinstance(declared_namespace, str) and declared_namespace:
            current_namespace = declared_namespace
        if current_namespace == namespace:
            version, kind, resource_name = (
                node.get("apiVersion"),
                node.get("kind"),
                metadata.get("name"),
            )
            if (
                isinstance(version, str)
                and isinstance(kind, str)
                and isinstance(resource_name, str)
                and (version.split("/")[0], kind, resource_name) in identities
            ):
                return True
            annotations = metadata.get("annotations", {})
            if (
                isinstance(annotations, dict)
                and isinstance(annotations.get("dapr.io/config"), str)
                and annotations.get("dapr.io/config") in names["Configuration"]
            ):
                return True
            for field in ("configMap", "configMapRef", "configMapKeyRef"):
                reference = node.get(field)
                if (
                    isinstance(reference, dict)
                    and isinstance(reference.get("name"), str)
                    and reference.get("name") in names["ConfigMap"]
                ):
                    return True
            role = node.get("roleRef")
            if (
                isinstance(role, dict)
                and role.get("apiGroup") == "rbac.authorization.k8s.io"
                and role.get("kind") == "Role"
                and isinstance(role.get("name"), str)
                and role.get("name") in names["Role"]
            ):
                return True
        return any(referenced(value, current_namespace) for value in node.values())

    return referenced(
        document, namespace if source_namespace is None else source_namespace
    )


def cleanup_resources(
    bundle: dict[str, Any], *, confirmed_no_retained_work: bool
) -> None:
    """Delete only an unused verified bundle after explicit retained-work attestation.

    All Pods, controllers, other live RoleBindings and the manifests and hooks
    of every revision of every paginated release across all namespaces must be
    free of typed references.
    This does not query durable work stores: the operator must first establish
    and explicitly attest that no retained work needs these identities. The
    Namespace is never eligible. UID/resource-version delete preconditions
    refuse concurrent replacement instead of deleting a different object.
    """
    if not confirmed_no_retained_work:
        raise OperatorValidationError(
            "cleanup requires explicit no-retained-work confirmation"
        )
    namespace = bundle["namespace"]
    records = bundle.get("resources")
    resources = bundle.get("objects")
    if resources is None:
        if not isinstance(records, list):
            raise OperatorValidationError(
                "cleanup requires a verified bundle or bridge inventory"
            )
        bridge_resources(
            [row["resource"] for row in records], bundle["release"], namespace
        )
        resources = [
            row["resource"] for row in records if row["resource"]["kind"] != "Namespace"
        ]
    if any(row["kind"] == "Namespace" for row in resources):
        raise OperatorValidationError("namespace cleanup is prohibited")
    for resource in resources:
        require_namespace(resource, namespace)
    pods = json.loads(run(["kubectl", "get", "pods", "-n", namespace, "-o", "json"]))
    if (
        not isinstance(pods, dict)
        or not isinstance(pods.get("items"), list)
        or not all(isinstance(row, dict) for row in pods["items"])
    ):
        raise TypeError("invalid Pod inventory")
    if references_resources(pods, resources, namespace):
        raise OperatorValidationError("bundle is referenced by a live or retained Pod")
    controllers = json.loads(
        run(
            [
                "kubectl",
                "get",
                "deployments,replicasets,statefulsets,daemonsets,jobs,cronjobs,rolebindings",
                "-n",
                namespace,
                "-o",
                "json",
            ]
        )
    )
    if (
        not isinstance(controllers, dict)
        or not isinstance(controllers.get("items"), list)
        or not all(isinstance(row, dict) for row in controllers["items"])
    ):
        raise TypeError("invalid workload controller inventory")
    owned_bindings = {
        row["metadata"]["name"] for row in resources if row["kind"] == "RoleBinding"
    }
    remaining = []
    for row in controllers["items"]:
        if row.get("kind") == "RoleBinding":
            metadata = row.get("metadata")
            if not isinstance(metadata, dict):
                raise TypeError("invalid RoleBinding inventory")
            if (
                metadata.get("namespace", namespace) == namespace
                and metadata.get("name") in owned_bindings
            ):
                continue
        remaining.append(row)
    controllers["items"] = remaining
    if references_resources(controllers, resources, namespace):
        raise OperatorValidationError(
            "bundle is referenced by a retained workload controller or RoleBinding"
        )
    offset = 0
    while True:
        releases = json.loads(
            run(
                [
                    "helm",
                    "list",
                    "--all",
                    "--all-namespaces",
                    "--max",
                    "256",
                    "--offset",
                    str(offset),
                    "-o",
                    "json",
                ]
            )
        )
        if not isinstance(releases, list):
            raise TypeError("invalid Helm release inventory")
        for current in releases:
            if (
                not isinstance(current, dict)
                or not isinstance(current.get("name"), str)
                or not isinstance(current.get("namespace"), str)
            ):
                raise TypeError("invalid Helm release inventory")
            release_name = name(current["name"])
            release_namespace = name(current["namespace"])
            history = json.loads(
                run(
                    [
                        "helm",
                        "history",
                        release_name,
                        "-n",
                        release_namespace,
                        "--max",
                        "2147483647",
                        "-o",
                        "json",
                    ]
                )
            )
            if not isinstance(history, list):
                raise TypeError("invalid Helm revision inventory")
            for revision in history:
                for source in ("manifest", "hooks"):
                    content = run(
                        [
                            "helm",
                            "get",
                            source,
                            release_name,
                            "-n",
                            release_namespace,
                            "--revision",
                            str(revision["revision"]),
                        ]
                    )
                    for document in yaml.safe_load_all(content):
                        if document is None:
                            continue
                        if not isinstance(document, dict):
                            raise TypeError(f"invalid retained Helm {source}")
                        if references_resources(
                            document,
                            resources,
                            namespace,
                            source_namespace=release_namespace,
                        ):
                            raise OperatorValidationError(
                                "bundle is referenced by a retained Helm revision"
                            )
        if len(releases) < 256:
            break
        offset += len(releases)
    live_resources = []
    for resource in resources:
        live = get_resource(resource)
        if live is None or effective(live) != effective(resource):
            raise OperatorValidationError(
                "cleanup resource missing or changed; no deletion performed"
            )
        if records is not None:
            record = next(
                row
                for row in records
                if row["resource"]["kind"] == resource["kind"]
                and row["resource"]["metadata"]["name"] == resource["metadata"]["name"]
            )
            if live["metadata"]["uid"] != record["uid"]:
                raise OperatorValidationError(
                    "retained bridge resource identity changed"
                )
            annotations = live["metadata"].get("annotations", {})
            if (
                live["metadata"].get("ownerReferences")
                or live["metadata"].get("deletionTimestamp")
                or annotations.get("meta.helm.sh/release-name") != bundle["release"]
                or annotations.get("meta.helm.sh/release-namespace") != namespace
                or live["metadata"]
                .get("labels", {})
                .get("app.kubernetes.io/managed-by")
                != "Helm"
            ):
                raise OperatorValidationError(
                    "retained bridge cleanup ownership changed"
                )
        elif (
            live["metadata"].get("deletionTimestamp")
            or live["metadata"].get("ownerReferences")
            or live["metadata"].get("annotations", {}).get("meta.helm.sh/release-name")
        ):
            raise OperatorValidationError("bundle cleanup ownership changed")
        live_resources.append(live)
    endpoints = {
        "ConfigMap": "/api/v1",
        "Configuration": "/apis/dapr.io/v1alpha1",
        "Role": "/apis/rbac.authorization.k8s.io/v1",
        "RoleBinding": "/apis/rbac.authorization.k8s.io/v1",
    }
    plurals = {
        "ConfigMap": "configmaps",
        "Configuration": "configurations",
        "Role": "roles",
        "RoleBinding": "rolebindings",
    }
    for live in reversed(live_resources):
        kind, metadata = live["kind"], live["metadata"]
        endpoint = f"{endpoints[kind]}/namespaces/{name(namespace)}/{plurals[kind]}/{name(metadata['name'])}"
        run(
            ["kubectl", "delete", "--raw", endpoint, "-f", "-"],
            {
                "apiVersion": "v1",
                "kind": "DeleteOptions",
                "preconditions": {
                    "uid": metadata["uid"],
                    "resourceVersion": metadata["resourceVersion"],
                },
            },
        )


def main() -> None:
    """Expose offline preparation and explicit create/verify operations with safe diagnostics."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "operation",
        choices=(
            "prepare",
            "provision",
            "verify",
            "record-bridge",
            "recover",
            "cleanup",
            "cleanup-bridge",
        ),
    )
    parser.add_argument("--registry", type=Path)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--values", type=Path, action="append", default=[])
    parser.add_argument("--release", default="custos")
    parser.add_argument("--namespace", default="controllers")
    parser.add_argument("--bridge-chart", type=Path)
    parser.add_argument("--bridge-inventory", type=Path)
    parser.add_argument("--acknowledge-paused-drained", action="store_true")
    parser.add_argument("--confirm-no-retained-work", action="store_true")
    parser.add_argument(
        "--atomic",
        action="store_true",
        help="preflight a planned atomic file upgrade; first cutover is rejected",
    )
    parser.add_argument(
        "--rollback-revision",
        type=int,
        help="preflight a planned native rollback; retained inline targets are rejected",
    )
    args = parser.parse_args()
    try:
        config = values(args.values)
        if args.operation == "prepare":
            if args.registry is None:
                raise OperatorValidationError("prepare requires --registry")
            path = args.registry.resolve()
            raw = read_execution_snapshot(str(path))
            result = prepare(raw, config, args.release, args.namespace)
            inventory = (
                json.loads(args.bridge_inventory.read_text())
                if args.bridge_inventory
                else None
            )
            transition_preflight(
                result,
                inventory,
                atomic=args.atomic,
                rollback_revision=args.rollback_revision,
            )
            with args.bundle.open("x") as stream:
                stream.write(json.dumps(result, indent=2, ensure_ascii=False))
            print("Execution bundle: prepared; references only; provisioning required")
            print(
                json.dumps(
                    {
                        "global": {
                            "receiverRuntime": {
                                "executionBindings": {"bindings": [], "defaults": None},
                                "executionBindingsBundle": result["descriptor"],
                                "retainLegacyExecutionResources": False,
                            }
                        },
                        "namespace": {"create": False},
                    },
                    indent=2,
                )
            )
        elif args.operation == "cleanup-bridge":
            if not args.acknowledge_paused_drained:
                raise OperatorValidationError(
                    "cleanup requires paused/drained acknowledgment"
                )
            cleanup_resources(
                json.loads(args.bundle.read_text()),
                confirmed_no_retained_work=args.confirm_no_retained_work,
            )
            print("Execution bridge: obsolete policies removed; namespace retained")
        elif args.operation in {"record-bridge", "recover"}:
            if args.bridge_chart is None or not args.acknowledge_paused_drained:
                raise OperatorValidationError(
                    "bridge operation requires saved chart and paused/drained acknowledgment"
                )
            if args.operation == "record-bridge":
                record = record_bridge(
                    args.bridge_chart, args.values, args.release, args.namespace
                )
                with args.bundle.open("x") as stream:
                    stream.write(json.dumps(record, indent=2))
                print(
                    "Execution bridge: verified and recorded; preserve artifacts and inventory"
                )
            else:
                recover_bridge(
                    json.loads(args.bundle.read_text()), args.bridge_chart, args.values
                )
                print(
                    "Execution bridge: recovered; verify readiness before resuming admission"
                )
        else:
            bundle = verify_bundle(json.loads(args.bundle.read_text()), config)
            inventory = (
                json.loads(args.bridge_inventory.read_text())
                if args.bridge_inventory
                else None
            )
            if args.operation == "cleanup":
                if not args.acknowledge_paused_drained:
                    raise OperatorValidationError(
                        "cleanup requires paused/drained acknowledgment"
                    )
                cleanup_resources(
                    bundle, confirmed_no_retained_work=args.confirm_no_retained_work
                )
            else:
                provision(
                    bundle,
                    create=args.operation == "provision",
                    inventory=inventory,
                    atomic=args.atomic,
                    rollback_revision=args.rollback_revision,
                )
            print(
                "Execution bundle: obsolete resources removed; namespace retained"
                if args.operation == "cleanup"
                else "Execution bundle: verified; references and policies retained"
            )
    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        yaml.YAMLError,
        subprocess.TimeoutExpired,
    ) as exc:
        # Parser/OS/tool exceptions can embed file contents/paths; never print them.
        reason = (
            str(exc)
            if isinstance(exc, (OperatorCommandError, OperatorValidationError))
            else type(exc).__name__
        )
        print(
            f"Execution bundle: rejected (operation={args.operation}; reason={reason}); admission remains paused; no fallback",
            file=sys.stderr,
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()

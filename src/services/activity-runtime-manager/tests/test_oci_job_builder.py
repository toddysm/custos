"""Tests for the OCI Container Driver Job builder (ARM-IMPL-015)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

import pytest
from kubernetes.client import ApiClient

from custos_arm.contract import ImageRef, StepRef
from custos_arm.limit import EffectiveResources
from custos_arm.manifest import IsolationTier
from custos_arm.runtime.isolation import (
    HARDENED_SECURITY_CONTEXT,
    Capabilities,
    SecurityContext,
)
from custos_arm.runtime.models import SandboxPlan, SidecarSpec, TmpfsMount
from custos_arm.runtime.oci import (
    ACTIVITY_CONTAINER_NAME,
    CONNECTOR_ENDPOINT_ENV,
    INPUT_BRIDGE_CONTAINER_NAME,
    OUTPUT_BRIDGE_CONTAINER_NAME,
    SIDECAR_CONTAINER_NAME,
    DuplicateMountError,
    build_activity_job,
    job_name,
)
from custos_arm.runtime.oci.job import MissingBridgeMountError

_DIGEST = "sha256:" + "a" * 64


def _step(*, run_id: str = "run-1", step_id: str = "step-a", attempt: int = 1) -> StepRef:
    return StepRef.model_validate({"runId": run_id, "stepId": step_id, "attempt": attempt})


def _resources(
    *,
    tier: IsolationTier = IsolationTier.PROCESS,
    runtime_class: str = "",
    timeout: timedelta = timedelta(seconds=120),
) -> EffectiveResources:
    return EffectiveResources(
        cpu_request="250m",
        cpu_limit="500m",
        memory_request="128Mi",
        memory_limit="256Mi",
        ephemeral_storage_limit="512Mi",
        timeout=timeout,
        tier=tier,
        runtime_class=runtime_class,
    )


def _plan(
    *,
    step: StepRef | None = None,
    namespace: str = "custos-sandboxes",
    image: ImageRef | None = None,
    resources: EffectiveResources | None = None,
    tmpfs_mounts: tuple[TmpfsMount, ...] | None = None,
    sidecar: SidecarSpec | None = None,
    io_bridge_image: str = "registry.example/io-bridge:1",
) -> SandboxPlan:
    return SandboxPlan(
        step=step or _step(),
        namespace=namespace,
        image=image or ImageRef(ref="registry.example/act", digest=_DIGEST),
        resources=resources or _resources(),
        tmpfs_mounts=tmpfs_mounts
        or (
            TmpfsMount(mount_path="/custos/in", read_only=True, size_limit="64Mi"),
            TmpfsMount(mount_path="/custos/out"),
        ),
        sidecar=sidecar
        or SidecarSpec(image="registry.example/sidecar:1", endpoint="http://c:8080"),
        io_bridge_image=io_bridge_image,
        deadline=datetime(2025, 1, 1, 0, 0, 0),
    )


def _containers(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    containers = manifest["spec"]["template"]["spec"]["containers"]
    return {c["name"]: c for c in containers}


def _init_containers(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    containers = manifest["spec"]["template"]["spec"]["initContainers"]
    return {c["name"]: c for c in containers}


def _pod_spec(manifest: dict[str, Any]) -> dict[str, Any]:
    spec: dict[str, Any] = manifest["spec"]["template"]["spec"]
    return spec


# --------------------------------------------------------------------------- #
# Kubernetes API schema conformance
# --------------------------------------------------------------------------- #


class _FakeResponse:
    def __init__(self, body: dict[str, Any]) -> None:
        self.data = json.dumps(body)


def test_manifest_conforms_to_kubernetes_job_schema() -> None:
    # Deserializing into V1Job drops any field the API schema does not know,
    # so a clean re-serialization round-trip proves every key/shape is valid.
    manifest = build_activity_job(_plan())
    api = ApiClient()
    job = api.deserialize(_FakeResponse(manifest), "V1Job")
    roundtrip = api.sanitize_for_serialization(job)
    assert roundtrip == manifest


def test_manifest_is_a_batch_v1_job() -> None:
    manifest = build_activity_job(_plan())
    assert manifest["apiVersion"] == "batch/v1"
    assert manifest["kind"] == "Job"
    assert manifest["spec"]["backoffLimit"] == 0


# --------------------------------------------------------------------------- #
# Naming and metadata
# --------------------------------------------------------------------------- #


def test_job_name_is_deterministic_for_the_triple() -> None:
    step = _step()
    assert job_name(step) == job_name(step)
    assert build_activity_job(_plan(step=step))["metadata"]["name"] == job_name(step)


def test_job_name_changes_with_attempt() -> None:
    assert job_name(_step(attempt=1)) != job_name(_step(attempt=2))


def test_job_name_is_valid_dns_1123() -> None:
    name = job_name(_step(run_id="RUN_With/Bad:Chars", step_id="Step.A", attempt=3))
    assert len(name) <= 63
    assert name == name.lower()
    assert name[0].isalnum() and name[-1].isalnum()
    assert all(c.isalnum() or c == "-" for c in name)


def test_labels_carry_identity_and_managed_by() -> None:
    manifest = build_activity_job(_plan(step=_step(run_id="r1", step_id="s1", attempt=2)))
    labels = manifest["metadata"]["labels"]
    assert labels["app.kubernetes.io/managed-by"] == "custos-activity-runtime-manager"
    assert labels["custos.dev/run-id"] == "r1"
    assert labels["custos.dev/step-id"] == "s1"
    assert labels["custos.dev/attempt"] == "2"
    assert manifest["spec"]["template"]["metadata"]["labels"] == labels


def test_namespace_is_taken_from_the_plan() -> None:
    manifest = build_activity_job(_plan(namespace="ns-42"))
    assert manifest["metadata"]["namespace"] == "ns-42"


# --------------------------------------------------------------------------- #
# tmpfs mounts realize the contract filesystem
# --------------------------------------------------------------------------- #


def test_tmpfs_mounts_are_memory_backed_empty_dirs() -> None:
    manifest = build_activity_job(_plan())
    volumes = {v["name"]: v for v in _pod_spec(manifest)["volumes"]}
    assert len(volumes) == 2
    for volume in volumes.values():
        assert volume["emptyDir"]["medium"] == "Memory"


def test_input_mount_is_read_only_with_size_limit() -> None:
    manifest = build_activity_job(_plan())
    activity = _containers(manifest)[ACTIVITY_CONTAINER_NAME]
    mounts = {m["mountPath"]: m for m in activity["volumeMounts"]}
    assert mounts["/custos/in"]["readOnly"] is True
    assert "readOnly" not in mounts["/custos/out"]
    volumes = {v["name"]: v for v in _pod_spec(manifest)["volumes"]}
    in_volume = next(v for v in volumes.values() if v["name"].startswith("custos-in"))
    assert in_volume["emptyDir"]["sizeLimit"] == "64Mi"


def test_only_contract_paths_are_mounted() -> None:
    manifest = build_activity_job(_plan())
    activity = _containers(manifest)[ACTIVITY_CONTAINER_NAME]
    paths = {m["mountPath"] for m in activity["volumeMounts"]}
    assert paths == {"/custos/in", "/custos/out"}


def test_colliding_mount_names_fail_fast() -> None:
    # Two distinct paths that sanitize to the same volume name.
    mounts = (
        TmpfsMount(mount_path="/custos/in"),
        TmpfsMount(mount_path="/custos:in"),
    )
    with pytest.raises(DuplicateMountError) as excinfo:
        build_activity_job(_plan(tmpfs_mounts=mounts))
    assert excinfo.value.volume_name == "custos-in"


def test_per_container_objects_are_independent() -> None:
    manifest = build_activity_job(_plan())
    containers = _containers(manifest)
    activity = containers[ACTIVITY_CONTAINER_NAME]
    sidecar = containers[SIDECAR_CONTAINER_NAME]
    # Equal in value but not shared references, so post-processing one
    # container never mutates the other.
    assert activity["securityContext"] is not sidecar["securityContext"]
    activity_caps = activity["securityContext"]["capabilities"]
    assert activity_caps is not sidecar["securityContext"]["capabilities"]
    assert activity["volumeMounts"] is not sidecar["volumeMounts"]
    activity["securityContext"]["privileged"] = True
    activity["volumeMounts"].append({"name": "x", "mountPath": "/x"})
    assert sidecar["securityContext"]["privileged"] is False
    assert len(sidecar["volumeMounts"]) == 2


# --------------------------------------------------------------------------- #
# Hardened SecurityContext + RuntimeClass
# --------------------------------------------------------------------------- #


def test_activity_container_runs_hardened_security_context() -> None:
    manifest = build_activity_job(_plan())
    sc = _containers(manifest)[ACTIVITY_CONTAINER_NAME]["securityContext"]
    assert sc["runAsNonRoot"] is True
    assert sc["allowPrivilegeEscalation"] is False
    assert sc["privileged"] is False
    assert sc["readOnlyRootFilesystem"] is True
    assert sc["capabilities"]["drop"] == ["ALL"]
    assert "add" not in sc["capabilities"]
    assert sc["seccompProfile"]["type"] == "RuntimeDefault"


def test_both_containers_share_the_hardened_context() -> None:
    manifest = build_activity_job(_plan())
    containers = _containers(manifest)
    assert (
        containers[ACTIVITY_CONTAINER_NAME]["securityContext"]
        == containers[SIDECAR_CONTAINER_NAME]["securityContext"]
    )


def test_runtime_class_omitted_for_default_runtime() -> None:
    manifest = build_activity_job(_plan(resources=_resources(runtime_class="")))
    assert "runtimeClassName" not in _pod_spec(manifest)


def test_runtime_class_stamped_when_selected() -> None:
    manifest = build_activity_job(
        _plan(resources=_resources(tier=IsolationTier.MICROVM, runtime_class="kata-fc"))
    )
    assert _pod_spec(manifest)["runtimeClassName"] == "kata-fc"


def test_custom_security_context_is_applied() -> None:
    custom = SecurityContext(
        run_as_non_root=True,
        allow_privilege_escalation=False,
        privileged=False,
        read_only_root_filesystem=True,
        capabilities=Capabilities(drop=("ALL",), add=("NET_BIND_SERVICE",)),
        seccomp_profile=HARDENED_SECURITY_CONTEXT.seccomp_profile,
    )
    manifest = build_activity_job(_plan(), security_context=custom)
    sc = _containers(manifest)[ACTIVITY_CONTAINER_NAME]["securityContext"]
    assert sc["readOnlyRootFilesystem"] is True
    assert sc["capabilities"]["add"] == ["NET_BIND_SERVICE"]


# --------------------------------------------------------------------------- #
# Pod-level hardening
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("field", "expected"),
    [
        ("restartPolicy", "Never"),
        ("automountServiceAccountToken", False),
        ("enableServiceLinks", False),
        ("hostNetwork", False),
        ("hostPID", False),
        ("hostIPC", False),
    ],
)
def test_pod_level_hardening(field: str, expected: object) -> None:
    manifest = build_activity_job(_plan())
    assert _pod_spec(manifest)[field] == expected


# --------------------------------------------------------------------------- #
# Resources, image, sidecar, deadline
# --------------------------------------------------------------------------- #


def test_resources_reflect_the_effective_envelope() -> None:
    manifest = build_activity_job(_plan())
    resources = _containers(manifest)[ACTIVITY_CONTAINER_NAME]["resources"]
    assert resources["requests"] == {"cpu": "250m", "memory": "128Mi"}
    assert resources["limits"] == {
        "cpu": "500m",
        "memory": "256Mi",
        "ephemeral-storage": "512Mi",
    }


def test_image_pins_the_digest() -> None:
    manifest = build_activity_job(_plan())
    image = _containers(manifest)[ACTIVITY_CONTAINER_NAME]["image"]
    assert image == f"registry.example/act@{_DIGEST}"


def test_image_left_unchanged_when_ref_already_pinned() -> None:
    pinned = ImageRef(ref=f"registry.example/act@{_DIGEST}", digest=_DIGEST)
    manifest = build_activity_job(_plan(image=pinned))
    image = _containers(manifest)[ACTIVITY_CONTAINER_NAME]["image"]
    assert image == f"registry.example/act@{_DIGEST}"


def test_image_without_digest_uses_ref_verbatim() -> None:
    manifest = build_activity_job(_plan(image=ImageRef(ref="registry.example/act:1")))
    assert _containers(manifest)[ACTIVITY_CONTAINER_NAME]["image"] == "registry.example/act:1"


def test_sidecar_is_injected_with_endpoint_env() -> None:
    manifest = build_activity_job(_plan())
    sidecar = _containers(manifest)[SIDECAR_CONTAINER_NAME]
    assert sidecar["image"] == "registry.example/sidecar:1"
    env = {e["name"]: e["value"] for e in sidecar["env"]}
    assert env[CONNECTOR_ENDPOINT_ENV] == "http://c:8080"


def test_sidecar_shares_contract_mounts() -> None:
    manifest = build_activity_job(_plan())
    containers = _containers(manifest)
    activity_mounts = containers[ACTIVITY_CONTAINER_NAME]["volumeMounts"]
    sidecar_mounts = containers[SIDECAR_CONTAINER_NAME]["volumeMounts"]
    assert sidecar_mounts == activity_mounts


def test_active_deadline_seconds_from_timeout() -> None:
    manifest = build_activity_job(_plan(resources=_resources(timeout=timedelta(minutes=5))))
    assert manifest["spec"]["activeDeadlineSeconds"] == 300


def test_active_deadline_seconds_floored_at_one() -> None:
    manifest = build_activity_job(_plan(resources=_resources(timeout=timedelta(0))))
    assert manifest["spec"]["activeDeadlineSeconds"] == 1


# --------------------------------------------------------------------------- #
# io-bridge helper containers (ARM-IMPL-023)
# --------------------------------------------------------------------------- #


def test_both_bridge_helpers_are_init_containers() -> None:
    manifest = build_activity_job(_plan())
    init = _init_containers(manifest)
    assert set(init) == {INPUT_BRIDGE_CONTAINER_NAME, OUTPUT_BRIDGE_CONTAINER_NAME}
    # The activity + connector sidecar remain the only regular containers.
    assert set(_containers(manifest)) == {ACTIVITY_CONTAINER_NAME, SIDECAR_CONTAINER_NAME}


def test_bridge_helpers_use_the_io_bridge_image() -> None:
    manifest = build_activity_job(_plan(io_bridge_image="registry.example/io-bridge@sha256:dead"))
    init = _init_containers(manifest)
    for name in (INPUT_BRIDGE_CONTAINER_NAME, OUTPUT_BRIDGE_CONTAINER_NAME):
        assert init[name]["image"] == "registry.example/io-bridge@sha256:dead"
        assert init[name]["imagePullPolicy"] == "IfNotPresent"
        assert init[name]["command"][0] == "sh"


def test_output_collector_is_a_native_sidecar() -> None:
    manifest = build_activity_job(_plan())
    collector = _init_containers(manifest)[OUTPUT_BRIDGE_CONTAINER_NAME]
    # restartPolicy: Always on an initContainer is the native-sidecar marker.
    assert collector["restartPolicy"] == "Always"


def test_input_injector_is_not_a_native_sidecar() -> None:
    manifest = build_activity_job(_plan())
    injector = _init_containers(manifest)[INPUT_BRIDGE_CONTAINER_NAME]
    # The injector runs to completion to gate the activity; it is not persistent.
    assert "restartPolicy" not in injector


def test_input_injector_mounts_only_in_writable() -> None:
    manifest = build_activity_job(_plan())
    injector = _init_containers(manifest)[INPUT_BRIDGE_CONTAINER_NAME]
    mounts = injector["volumeMounts"]
    assert [m["mountPath"] for m in mounts] == ["/custos/in"]
    # Writable so the streamed input tree can land — readOnly must be absent.
    assert "readOnly" not in mounts[0]


def test_output_collector_mounts_only_out_writable() -> None:
    manifest = build_activity_job(_plan())
    collector = _init_containers(manifest)[OUTPUT_BRIDGE_CONTAINER_NAME]
    mounts = collector["volumeMounts"]
    assert [m["mountPath"] for m in mounts] == ["/custos/out"]
    assert "readOnly" not in mounts[0]


def test_bridge_helpers_share_the_activity_contract_volumes() -> None:
    manifest = build_activity_job(_plan())
    volume_names = {v["name"] for v in _pod_spec(manifest)["volumes"]}
    init = _init_containers(manifest)
    injector_volume = init[INPUT_BRIDGE_CONTAINER_NAME]["volumeMounts"][0]["name"]
    collector_volume = init[OUTPUT_BRIDGE_CONTAINER_NAME]["volumeMounts"][0]["name"]
    # Both attach to the same emptyDir volumes the activity uses — no new volume.
    assert injector_volume in volume_names
    assert collector_volume in volume_names
    assert injector_volume != collector_volume


def test_bridge_helpers_run_the_hardened_security_context() -> None:
    manifest = build_activity_job(_plan())
    activity_sc = _containers(manifest)[ACTIVITY_CONTAINER_NAME]["securityContext"]
    for name, container in _init_containers(manifest).items():
        assert container["securityContext"] == activity_sc, name


def test_no_container_uses_host_path() -> None:
    manifest = build_activity_job(_plan())
    for volume in _pod_spec(manifest)["volumes"]:
        assert "hostPath" not in volume
        assert "emptyDir" in volume


def test_activity_container_spec_is_unchanged_by_the_bridge() -> None:
    # The bridge is additive: the activity container still mounts /custos/in
    # read-only and is otherwise identical to the pre-bridge spec.
    manifest = build_activity_job(_plan())
    activity = _containers(manifest)[ACTIVITY_CONTAINER_NAME]
    mounts = {m["mountPath"]: m for m in activity["volumeMounts"]}
    assert mounts["/custos/in"]["readOnly"] is True
    assert "readOnly" not in mounts["/custos/out"]
    assert activity["image"] == f"registry.example/act@{_DIGEST}"


def test_missing_contract_volume_fails_fast() -> None:
    # A plan whose contract mounts omit /custos/out leaves the output collector
    # with no volume to attach to — a malformed plan surfaced at build time.
    mounts = (TmpfsMount(mount_path="/custos/in", read_only=True),)
    with pytest.raises(MissingBridgeMountError) as excinfo:
        build_activity_job(_plan(tmpfs_mounts=mounts))
    assert excinfo.value.mount_path == "/custos/out"

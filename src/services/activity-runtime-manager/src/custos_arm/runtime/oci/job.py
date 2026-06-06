"""OCI Container Driver — Job builder (ARM-IMPL-015).

Translate a fully-resolved :class:`~custos_arm.runtime.models.SandboxPlan` into
a Kubernetes ``Job`` manifest. This module is *pure*: it performs no cluster
access and no I/O — it only computes the spec the lifecycle monitor
(ARM-IMPL-016) submits with ``create_namespaced_job``.

The generated manifest realizes the design § Sandbox and Isolation Model:

* the contract filesystem (``/custos/in/*``, ``/custos/out``) is backed by
  ``tmpfs`` (``emptyDir`` with ``medium: Memory``) volumes derived from the
  plan's :class:`~custos_arm.runtime.models.TmpfsMount` list — the only
  writable mounts;
* every container runs with the hardened baseline
  :data:`~custos_arm.runtime.isolation.HARDENED_SECURITY_CONTEXT` (runAsNonRoot,
  no privilege escalation, all capabilities dropped, ``RuntimeDefault``
  seccomp, read-only root filesystem);
* the selected ``RuntimeClass`` from the effective envelope is stamped onto the
  Pod (empty string → the cluster-default runtime, so ``runtimeClassName`` is
  omitted);
* Pod-level hardening disables host network / PID / IPC and the
  service-account token automount;
* the connector ``sidecar`` is injected alongside the activity container and
  shares the contract mounts.

Two **io-bridge helper containers** (ARM-IMPL-023) realize the pod-side plumbing
the driver later streams the contract filesystem over (the design locks the file
*layout* but is silent on the ARM↔pod *transport*):

* an **input-injector init container** mounts ``/custos/in`` *writable* — the
  endpoint the lifecycle monitor streams the input tree into. It completes
  immediately in this milestone; ``ARM-IMPL-025`` turns it into a
  block-until-ready staging gate once ``start()`` streams ``in/`` and drops a
  readiness sentinel;
* an **output-collector native sidecar** (an ``initContainers`` entry with
  ``restartPolicy: Always``, requiring Kubernetes >= 1.28) mounts ``/custos/out``
  and idles for the pod lifetime so the monitor can stream the outputs back out
  after the activity terminates.

Both helpers run with the same hardened :data:`HARDENED_SECURITY_CONTEXT` and use
only the shared ``emptyDir`` contract volumes — no ``hostPath`` — so the activity
container's spec is unchanged.

The manifest is a plain ``dict`` (the JSON body the Kubernetes API accepts)
rather than a typed client object, keeping this layer dependency-free; the
spec's conformance to the Kubernetes API schema is asserted at unit level by
round-tripping it through the official client's ``V1Job`` model.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any, Final

from custos_arm.contract import ImageRef, StepRef
from custos_arm.limit import EffectiveResources
from custos_arm.runtime.isolation import HARDENED_SECURITY_CONTEXT, SecurityContext
from custos_arm.runtime.models import SandboxPlan, SidecarSpec, TmpfsMount

__all__ = [
    "ACTIVITY_CONTAINER_NAME",
    "CONNECTOR_ENDPOINT_ENV",
    "INPUT_BRIDGE_CONTAINER_NAME",
    "JOB_NAME_PREFIX",
    "MANAGED_BY",
    "OUTPUT_BRIDGE_CONTAINER_NAME",
    "SIDECAR_CONTAINER_NAME",
    "DuplicateMountError",
    "MissingBridgeMountError",
    "UnpinnedImageError",
    "build_activity_job",
    "job_name",
]


class DuplicateMountError(ValueError):
    """Two contract mounts sanitize to the same Kubernetes volume name.

    The Scheduler is expected to hand the builder distinct mount paths; this
    fail-fast guard turns an otherwise schema-valid-but-cluster-rejected
    manifest (duplicate ``volumes[].name``) into a clear build-time error.
    """

    def __init__(self, volume_name: str) -> None:
        super().__init__(f"duplicate sandbox volume name: {volume_name!r}")
        self.volume_name = volume_name


class MissingBridgeMountError(ValueError):
    """An io-bridge helper has no matching contract volume to attach to.

    The Scheduler always includes the ``/custos/in`` and ``/custos/out`` contract
    mounts; this fail-fast guard surfaces a malformed plan (a bridge helper with
    no backing ``emptyDir`` volume) as a clear build-time error rather than an
    API-rejected manifest.
    """

    def __init__(self, mount_path: str) -> None:
        super().__init__(f"no contract volume for io-bridge mount path: {mount_path!r}")
        self.mount_path = mount_path


class UnpinnedImageError(ValueError):
    """The activity image has no digest and unpinned images are not allowed.

    Production renders every activity as ``image@digest`` so the running bits are
    content-addressed. A digest-less image is only permitted when the operator
    opts in via ``ARM_ALLOW_UNPINNED_IMAGES`` (test/dev); otherwise this
    fail-fast guard rejects the plan at build time.
    """

    def __init__(self, ref: str) -> None:
        super().__init__(
            f"activity image {ref!r} has no digest and ARM_ALLOW_UNPINNED_IMAGES is off"
        )
        self.ref = ref


#: The container that runs the activity image.
ACTIVITY_CONTAINER_NAME: Final[str] = "activity"
#: The connector sidecar injected alongside the activity.
SIDECAR_CONTAINER_NAME: Final[str] = "connector-sidecar"
#: Init container that receives the streamed input tree and gates the activity.
INPUT_BRIDGE_CONTAINER_NAME: Final[str] = "io-bridge-input"
#: Native sidecar that holds ``/custos/out`` open for output streaming.
OUTPUT_BRIDGE_CONTAINER_NAME: Final[str] = "io-bridge-output"
#: Value of the ``app.kubernetes.io/managed-by`` label on every Job.
MANAGED_BY: Final[str] = "custos-activity-runtime-manager"
#: DNS-1123 prefix for generated Job names.
JOB_NAME_PREFIX: Final[str] = "arm"
#: Env var carrying the Connector Service endpoint into the sidecar.
CONNECTOR_ENDPOINT_ENV: Final[str] = "CUSTOS_CONNECTOR_ENDPOINT"

_KUBERNETES_API_VERSION: Final[str] = "batch/v1"
_KUBERNETES_KIND: Final[str] = "Job"
_RUN_ID_LABEL: Final[str] = "custos.dev/run-id"
_STEP_ID_LABEL: Final[str] = "custos.dev/step-id"
_ATTEMPT_LABEL: Final[str] = "custos.dev/attempt"
_MANAGED_BY_LABEL: Final[str] = "app.kubernetes.io/managed-by"

_TMPFS_MEDIUM: Final[str] = "Memory"
_MAX_NAME_LEN: Final[int] = 63
_NAME_HASH_LEN: Final[int] = 8
_INVALID_NAME_CHARS = re.compile(r"[^a-z0-9]+")
_INVALID_LABEL_CHARS = re.compile(r"[^A-Za-z0-9_.-]+")

#: Contract paths the io-bridge helper containers attach to.
_INPUT_MOUNT_PATH: Final[str] = "/custos/in"
_OUTPUT_MOUNT_PATH: Final[str] = "/custos/out"

#: ``restartPolicy: Always`` on an ``initContainers`` entry marks a native
#: sidecar (Kubernetes >= 1.28): it starts before the activity container, stays
#: running for the pod lifetime, and is terminated once the activity exits.
_NATIVE_SIDECAR_RESTART_POLICY: Final[str] = "Always"

#: The input injector exists to own the *writable* ``/custos/in`` mount the
#: lifecycle monitor streams the input tree into. In this milestone it completes
#: immediately (the contract volume is the plumbing the bridge needs);
#: ``ARM-IMPL-025`` turns it into a block-until-sentinel input-staging gate when
#: ``start()`` learns to stream ``in/`` in and drop the readiness sentinel.
_INPUT_BRIDGE_COMMAND: Final[tuple[str, ...]] = ("sh", "-c", "true")
#: The output collector idles for the pod lifetime so the monitor can stream the
#: outputs back out after the activity terminates; it exits cleanly on SIGTERM.
_OUTPUT_BRIDGE_COMMAND: Final[tuple[str, ...]] = (
    "sh",
    "-c",
    'trap "exit 0" TERM; while true; do sleep 3600 & wait $!; done',
)


def job_name(step: StepRef) -> str:
    """Deterministic DNS-1123 ``Job`` name for an attempt.

    The name is derived solely from the ``(runId, stepId, attempt)`` triple, so
    an idempotent replay of the same attempt always targets the same ``Job``
    (design § Idempotent replay). A readable, sanitized prefix is suffixed with
    a short hash of the triple to keep the name unique and within 63 chars.
    """
    triple = f"{step.run_id}|{step.step_id}|{step.attempt}"
    digest = hashlib.sha1(triple.encode("utf-8")).hexdigest()[:_NAME_HASH_LEN]
    slug = _INVALID_NAME_CHARS.sub("-", f"{step.run_id}-{step.step_id}-{step.attempt}".lower())
    budget = _MAX_NAME_LEN - len(JOB_NAME_PREFIX) - len(digest) - 2
    prefix = f"{JOB_NAME_PREFIX}-{slug.strip('-')[:budget].strip('-')}"
    return f"{prefix}-{digest}"


def build_activity_job(
    plan: SandboxPlan,
    *,
    security_context: SecurityContext = HARDENED_SECURITY_CONTEXT,
) -> dict[str, Any]:
    """Build the Kubernetes ``Job`` manifest for one activity sandbox.

    Args:
        plan: the fully-resolved sandbox request (pinned image, effective
            resources + ``RuntimeClass``, contract ``tmpfs`` mounts, connector
            sidecar, namespace, deadline).
        security_context: the hardened container ``SecurityContext`` to stamp on
            both containers; defaults to the guaranteed baseline.

    Returns:
        The ``Job`` manifest as the plain ``dict`` body accepted by the
        Kubernetes API.
    """
    name = job_name(plan.step)
    labels = _labels(plan.step)
    named_mounts = _named_mounts(plan.tmpfs_mounts)
    deadline_seconds = max(1, int(plan.resources.timeout.total_seconds()))

    pod_spec: dict[str, Any] = {
        "restartPolicy": "Never",
        "automountServiceAccountToken": False,
        "enableServiceLinks": False,
        "hostNetwork": False,
        "hostPID": False,
        "hostIPC": False,
        "initContainers": [
            _output_bridge_container(plan.io_bridge_image, security_context, named_mounts),
            _input_bridge_container(plan.io_bridge_image, security_context, named_mounts),
        ],
        "containers": [
            _activity_container(
                plan.image,
                plan.resources,
                security_context,
                named_mounts,
                allow_unpinned=plan.allow_unpinned_images,
            ),
            _sidecar_container(plan.sidecar, security_context, named_mounts),
        ],
        "volumes": _volumes(named_mounts),
    }
    runtime_class = plan.resources.runtime_class
    if runtime_class:
        pod_spec["runtimeClassName"] = runtime_class

    return {
        "apiVersion": _KUBERNETES_API_VERSION,
        "kind": _KUBERNETES_KIND,
        "metadata": {
            "name": name,
            "namespace": plan.namespace,
            "labels": labels,
        },
        "spec": {
            "backoffLimit": 0,
            "completions": 1,
            "parallelism": 1,
            "activeDeadlineSeconds": deadline_seconds,
            "template": {
                "metadata": {"labels": labels},
                "spec": pod_spec,
            },
        },
    }


def _labels(step: StepRef) -> dict[str, str]:
    return {
        _MANAGED_BY_LABEL: MANAGED_BY,
        _RUN_ID_LABEL: _label_value(step.run_id),
        _STEP_ID_LABEL: _label_value(step.step_id),
        _ATTEMPT_LABEL: str(step.attempt),
    }


def _label_value(value: str) -> str:
    sanitized = _INVALID_LABEL_CHARS.sub("_", value).strip("_.-")[:_MAX_NAME_LEN]
    return sanitized.strip("_.-")


def _named_mounts(
    tmpfs_mounts: tuple[TmpfsMount, ...],
) -> tuple[tuple[str, TmpfsMount], ...]:
    """Pair each contract mount with its Kubernetes volume name, fail-fast on
    collisions so the manifest is always API-valid."""
    named: list[tuple[str, TmpfsMount]] = []
    seen: set[str] = set()
    for mount in tmpfs_mounts:
        volume_name = _volume_name(mount.mount_path)
        if volume_name in seen:
            raise DuplicateMountError(volume_name)
        seen.add(volume_name)
        named.append((volume_name, mount))
    return tuple(named)


def _volumes(named_mounts: tuple[tuple[str, TmpfsMount], ...]) -> list[dict[str, Any]]:
    volumes: list[dict[str, Any]] = []
    for volume_name, mount in named_mounts:
        empty_dir: dict[str, Any] = {"medium": _TMPFS_MEDIUM}
        if mount.size_limit is not None:
            empty_dir["sizeLimit"] = mount.size_limit
        volumes.append({"name": volume_name, "emptyDir": empty_dir})
    return volumes


def _volume_mounts(named_mounts: tuple[tuple[str, TmpfsMount], ...]) -> list[dict[str, Any]]:
    """Build a fresh list of volume-mount dicts (independent per container)."""
    mounts: list[dict[str, Any]] = []
    for volume_name, mount in named_mounts:
        volume_mount: dict[str, Any] = {"name": volume_name, "mountPath": mount.mount_path}
        if mount.read_only:
            volume_mount["readOnly"] = True
        mounts.append(volume_mount)
    return mounts


def _volume_name(mount_path: str) -> str:
    slug = _INVALID_NAME_CHARS.sub("-", mount_path.lower()).strip("-")
    return slug[:_MAX_NAME_LEN].strip("-") or "vol"


def _security_context(security_context: SecurityContext) -> dict[str, Any]:
    capabilities: dict[str, Any] = {"drop": list(security_context.capabilities.drop)}
    if security_context.capabilities.add:
        capabilities["add"] = list(security_context.capabilities.add)
    return {
        "runAsNonRoot": security_context.run_as_non_root,
        "allowPrivilegeEscalation": security_context.allow_privilege_escalation,
        "privileged": security_context.privileged,
        "readOnlyRootFilesystem": security_context.read_only_root_filesystem,
        "capabilities": capabilities,
        "seccompProfile": {"type": security_context.seccomp_profile.type},
    }


def _activity_container(
    image: ImageRef,
    resources: EffectiveResources,
    security_context: SecurityContext,
    named_mounts: tuple[tuple[str, TmpfsMount], ...],
    *,
    allow_unpinned: bool,
) -> dict[str, Any]:
    return {
        "name": ACTIVITY_CONTAINER_NAME,
        "image": _image_reference(image, allow_unpinned=allow_unpinned),
        "imagePullPolicy": "IfNotPresent",
        "securityContext": _security_context(security_context),
        "resources": _resources(resources),
        "volumeMounts": _volume_mounts(named_mounts),
    }


def _sidecar_container(
    sidecar: SidecarSpec,
    security_context: SecurityContext,
    named_mounts: tuple[tuple[str, TmpfsMount], ...],
) -> dict[str, Any]:
    return {
        "name": SIDECAR_CONTAINER_NAME,
        "image": sidecar.image,
        "imagePullPolicy": "IfNotPresent",
        "securityContext": _security_context(security_context),
        "env": [{"name": CONNECTOR_ENDPOINT_ENV, "value": sidecar.endpoint}],
        "volumeMounts": _volume_mounts(named_mounts),
    }


def _writable_mount(
    named_mounts: tuple[tuple[str, TmpfsMount], ...],
    mount_path: str,
) -> dict[str, Any]:
    """Mount the single contract volume at ``mount_path`` *writable*.

    The io-bridge helpers attach to exactly one contract volume each and always
    need write access (the injector receives the streamed input tree; the
    collector is the live endpoint the outputs are streamed out of), so the
    plan's ``read_only`` flag on the activity-side mount is deliberately ignored
    here.
    """
    volume_name = _volume_name(mount_path)
    for name, _mount in named_mounts:
        if name == volume_name:
            return {"name": volume_name, "mountPath": mount_path}
    raise MissingBridgeMountError(mount_path)


def _input_bridge_container(
    io_bridge_image: str,
    security_context: SecurityContext,
    named_mounts: tuple[tuple[str, TmpfsMount], ...],
) -> dict[str, Any]:
    return {
        "name": INPUT_BRIDGE_CONTAINER_NAME,
        "image": io_bridge_image,
        "imagePullPolicy": "IfNotPresent",
        "command": list(_INPUT_BRIDGE_COMMAND),
        "securityContext": _security_context(security_context),
        "volumeMounts": [_writable_mount(named_mounts, _INPUT_MOUNT_PATH)],
    }


def _output_bridge_container(
    io_bridge_image: str,
    security_context: SecurityContext,
    named_mounts: tuple[tuple[str, TmpfsMount], ...],
) -> dict[str, Any]:
    return {
        "name": OUTPUT_BRIDGE_CONTAINER_NAME,
        "image": io_bridge_image,
        "imagePullPolicy": "IfNotPresent",
        "restartPolicy": _NATIVE_SIDECAR_RESTART_POLICY,
        "command": list(_OUTPUT_BRIDGE_COMMAND),
        "securityContext": _security_context(security_context),
        "volumeMounts": [_writable_mount(named_mounts, _OUTPUT_MOUNT_PATH)],
    }


def _resources(resources: EffectiveResources) -> dict[str, Any]:
    return {
        "requests": {
            "cpu": resources.cpu_request,
            "memory": resources.memory_request,
        },
        "limits": {
            "cpu": resources.cpu_limit,
            "memory": resources.memory_limit,
            "ephemeral-storage": resources.ephemeral_storage_limit,
        },
    }


def _image_reference(image: ImageRef, *, allow_unpinned: bool) -> str:
    """Render the activity image reference.

    A digest pins the image to immutable content and always wins: the result is
    ``ref@digest`` (unless ``ref`` already carries an ``@digest``). A digest-less
    image is only rendered tag-only when ``allow_unpinned`` is set (the
    ``ARM_ALLOW_UNPINNED_IMAGES`` test/dev escape hatch); otherwise it is
    rejected so production never runs unpinned bits.
    """
    if image.digest and "@" not in image.ref:
        return f"{image.ref}@{image.digest}"
    if image.digest or allow_unpinned:
        return image.ref
    raise UnpinnedImageError(image.ref)

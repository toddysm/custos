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
    "JOB_NAME_PREFIX",
    "MANAGED_BY",
    "SIDECAR_CONTAINER_NAME",
    "build_activity_job",
    "job_name",
]

#: The container that runs the activity image.
ACTIVITY_CONTAINER_NAME: Final[str] = "activity"
#: The connector sidecar injected alongside the activity.
SIDECAR_CONTAINER_NAME: Final[str] = "connector-sidecar"
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
    volumes, mounts = _volumes_and_mounts(plan.tmpfs_mounts)
    pod_security_context = _security_context(security_context)
    deadline_seconds = max(1, int(plan.resources.timeout.total_seconds()))

    pod_spec: dict[str, Any] = {
        "restartPolicy": "Never",
        "automountServiceAccountToken": False,
        "enableServiceLinks": False,
        "hostNetwork": False,
        "hostPID": False,
        "hostIPC": False,
        "containers": [
            _activity_container(plan.image, plan.resources, pod_security_context, mounts),
            _sidecar_container(plan.sidecar, pod_security_context, mounts),
        ],
        "volumes": volumes,
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


def _volumes_and_mounts(
    tmpfs_mounts: tuple[TmpfsMount, ...],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    volumes: list[dict[str, Any]] = []
    mounts: list[dict[str, Any]] = []
    for mount in tmpfs_mounts:
        volume_name = _volume_name(mount.mount_path)
        empty_dir: dict[str, Any] = {"medium": _TMPFS_MEDIUM}
        if mount.size_limit is not None:
            empty_dir["sizeLimit"] = mount.size_limit
        volumes.append({"name": volume_name, "emptyDir": empty_dir})
        volume_mount: dict[str, Any] = {
            "name": volume_name,
            "mountPath": mount.mount_path,
        }
        if mount.read_only:
            volume_mount["readOnly"] = True
        mounts.append(volume_mount)
    return volumes, mounts


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
    security_context: dict[str, Any],
    mounts: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "name": ACTIVITY_CONTAINER_NAME,
        "image": _image_reference(image),
        "imagePullPolicy": "IfNotPresent",
        "securityContext": security_context,
        "resources": _resources(resources),
        "volumeMounts": mounts,
    }


def _sidecar_container(
    sidecar: SidecarSpec,
    security_context: dict[str, Any],
    mounts: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "name": SIDECAR_CONTAINER_NAME,
        "image": sidecar.image,
        "imagePullPolicy": "IfNotPresent",
        "securityContext": security_context,
        "env": [{"name": CONNECTOR_ENDPOINT_ENV, "value": sidecar.endpoint}],
        "volumeMounts": mounts,
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


def _image_reference(image: ImageRef) -> str:
    if image.digest and "@" not in image.ref:
        return f"{image.ref}@{image.digest}"
    return image.ref

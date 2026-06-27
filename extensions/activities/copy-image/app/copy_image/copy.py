"""skopeo-based single-image copy engine (COPY-IMPL-004).

Resolves the bound ``source``/``dest`` connector contexts plus the activity
inputs into a ``skopeo copy`` invocation. ``skopeo`` (baked into the image)
performs the manifest + blob copy and the registry token exchange itself;
this module only shapes the command, materializes the credential authfile
(via :mod:`copy_image.credentials`), runs the process, and reads back the
copied digest.

Detailed ``skopeo`` failure classification into the manifest's declared
error codes lands in COPY-IMPL-005; here a non-zero exit raises
:class:`SkopeoError`.
"""

from __future__ import annotations

import re
import subprocess
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from copy_image.contract import ActivityError, Context, InputsEnvelope

#: The copy binary baked into the activity image.
SKOPEO = "skopeo"

#: ``subprocess.run``-compatible callable, injectable for tests.
Runner = Callable[..., "subprocess.CompletedProcess[str]"]


@dataclass(frozen=True)
class CopyPlan:
    """A resolved, ready-to-run copy."""

    source_transport: str  # docker://<host>/<repo>:<tag|@digest>
    dest_transport: str  # docker://<host>/<repo>:<tag>
    source_ref: str  # bare <host>/<repo>:<tag|@digest> (for oras)
    destination_ref: str  # <host>/<repo>:<tag>
    source_host: str
    dest_host: str
    all_platforms: bool
    platform: str | None
    copy_referrers: bool


@dataclass(frozen=True)
class CopyOutcome:
    """The result of a successful copy."""

    destination_ref: str
    digest: str
    manifests_copied: int


class SkopeoError(Exception):
    """Raised when ``skopeo`` exits non-zero. Classified in COPY-IMPL-005."""

    def __init__(self, returncode: int, stderr: str) -> None:
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(f"skopeo exited {returncode}")


def host_of(value: str) -> str:
    """Extract the registry host from a connector endpoint URL or bare ref."""
    candidate = value if "//" in value else f"//{value}"
    return urlsplit(candidate).hostname or ""


def _ref_with_digest(ref: str, digest: str) -> str:
    """Return ``ref`` pinned to ``digest`` (dropping any trailing ``:tag``)."""
    base = ref.split("@", 1)[0]
    if "/" in base:
        head, last = base.rsplit("/", 1)
        if ":" in last:
            last = last.split(":", 1)[0]
        base = f"{head}/{last}"
    elif ":" in base:
        base = base.split(":", 1)[0]
    return f"{base}@{digest}"


def _require_object(value: Any, *, where: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ActivityError(
            "activity.contract_violation", "permanent", f"{where} must be an object"
        )
    return value


def _require_str(value: Any, *, where: str) -> str:
    if not isinstance(value, str) or not value:
        raise ActivityError(
            "activity.contract_violation", "permanent", f"{where} is required and must be a string"
        )
    return value


def _bool_flag(value: Any, *, where: str) -> bool:
    """Parse an optional boolean input flag, rejecting non-bool values.

    ``bool("false")`` is truthy, so a permissive cast would silently flip
    behavior; an explicit non-bool (e.g. the string ``"false"``) is a
    permanent contract violation.
    """
    if value is None:
        return False
    if not isinstance(value, bool):
        raise ActivityError(
            "activity.contract_violation", "permanent", f"{where} must be a boolean"
        )
    return value


def resolve_copy_plan(inputs: InputsEnvelope, ctx: Context) -> CopyPlan:
    """Turn the activity inputs + connector contexts into a :class:`CopyPlan`."""
    source = _require_object(inputs.inputs.get("source"), where="inputs.source")
    ref = _require_str(source.get("ref"), where="inputs.source.ref")
    digest = source.get("digest")

    destination = _require_object(inputs.inputs.get("destination"), where="inputs.destination")
    repository = _require_str(destination.get("repository"), where="inputs.destination.repository")
    raw_tag = destination.get("tag")
    tag = raw_tag if isinstance(raw_tag, str) and raw_tag else "latest"

    source_conn = ctx.connectors.get("source", {})
    dest_conn = ctx.connectors.get("dest", {})
    source_endpoint = source_conn.get("endpoint", "") if isinstance(source_conn, dict) else ""
    dest_endpoint = dest_conn.get("endpoint", "") if isinstance(dest_conn, dict) else ""

    source_host = host_of(source_endpoint) or host_of(ref)
    dest_host = host_of(dest_endpoint)
    if not source_host:
        raise ActivityError(
            "activity.contract_violation",
            "permanent",
            "could not derive the source registry host from "
            "ctx.connectors.source.endpoint or inputs.source.ref",
        )
    if not dest_host:
        raise ActivityError(
            "activity.contract_violation",
            "permanent",
            "ctx.connectors.dest.endpoint is required to derive the destination host",
        )

    source_ref = _ref_with_digest(ref, digest) if isinstance(digest, str) and digest else ref

    destination_ref = f"{dest_host}/{repository}:{tag}"
    raw_platform = inputs.inputs.get("platform")
    platform = raw_platform if isinstance(raw_platform, str) and raw_platform else None
    return CopyPlan(
        source_transport="docker://" + source_ref,
        dest_transport="docker://" + destination_ref,
        source_ref=source_ref,
        destination_ref=destination_ref,
        source_host=source_host,
        dest_host=dest_host,
        all_platforms=_bool_flag(inputs.inputs.get("allPlatforms"), where="inputs.allPlatforms"),
        platform=platform,
        copy_referrers=_bool_flag(inputs.inputs.get("copyReferrers"), where="inputs.copyReferrers"),
    )


def _platform_overrides(platform: str | None) -> list[str]:
    """Translate ``os/arch[/variant]`` into skopeo ``--override-*`` globals."""
    if not platform:
        return []
    parts = platform.split("/")
    args: list[str] = []
    if parts and parts[0]:
        args += ["--override-os", parts[0]]
    if len(parts) > 1 and parts[1]:
        args += ["--override-arch", parts[1]]
    if len(parts) > 2 and parts[2]:
        args += ["--override-variant", parts[2]]
    return args


def build_argv(plan: CopyPlan, authfile: Path, digestfile: Path) -> list[str]:
    """Assemble the ``skopeo copy`` argv for ``plan``."""
    argv = [SKOPEO]
    # ``--override-*`` are skopeo *global* options (before the subcommand);
    # only meaningful when selecting a single platform from an index.
    if not plan.all_platforms and plan.platform:
        argv += _platform_overrides(plan.platform)
    argv += ["copy", "--authfile", str(authfile), "--digestfile", str(digestfile)]
    if plan.all_platforms:
        argv.append("--all")
    argv += [plan.source_transport, plan.dest_transport]
    return argv


def run_skopeo_copy(
    plan: CopyPlan,
    authfile: Path,
    *,
    runner: Runner | None = None,
    work_dir: Path | None = None,
) -> CopyOutcome:
    """Run ``skopeo copy`` and return the copied digest.

    ``skopeo copy`` is inherently idempotent — copying content that already
    exists at the destination re-verifies and skips existing blobs/manifest,
    so a repeated copy of the same digest is a fast no-op success. Raises
    :class:`SkopeoError` on a non-zero exit.
    """
    # Resolve the runner at call time (not as a default) so tests can
    # monkeypatch ``copy_image.copy.subprocess.run``.
    run = runner if runner is not None else subprocess.run
    work = work_dir if work_dir is not None else Path(tempfile.mkdtemp(prefix="copy-image-"))
    work.mkdir(parents=True, exist_ok=True)
    digestfile = work / "digest"
    argv = build_argv(plan, authfile, digestfile)
    proc = run(argv, capture_output=True, text=True)
    if proc.returncode != 0:
        raise SkopeoError(proc.returncode, proc.stderr or "")
    try:
        digest = digestfile.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        digest = ""
    if not digest:
        raise ActivityError(
            "activity.unexpected_error",
            "retryable",
            "skopeo exited 0 but wrote no digest",
        )
    return CopyOutcome(
        destination_ref=plan.destination_ref,
        digest=digest,
        manifests_copied=1,
    )


# ---------------------------------------------------------------------------
# Referrers copy (oras)
# ---------------------------------------------------------------------------

#: The artifact-aware copy binary baked into the image. ``skopeo copy`` does
#: not copy OCI referrers; ``oras cp --recursive`` copies the image manifest
#: plus its referrers (signatures / SBOM / attestations) in one pass.
ORAS = "oras"

_DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
_LABELED_DIGEST_RE = re.compile(r"Digest:\s*(sha256:[0-9a-f]{64})")


class OrasError(Exception):
    """Raised when ``oras cp`` exits non-zero. Classified like SkopeoError."""

    def __init__(self, returncode: int, stderr: str) -> None:
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(f"oras cp exited {returncode}")


def build_oras_argv(plan: CopyPlan, authfile: Path) -> list[str]:
    """Assemble the ``oras cp --recursive`` argv for ``plan``."""
    argv = [
        ORAS,
        "cp",
        "--recursive",
        "--registry-config",
        str(authfile),
    ]
    # Honor single-platform selection, mirroring the skopeo path.
    if not plan.all_platforms and plan.platform:
        argv += ["--platform", plan.platform]
    argv += [plan.source_ref, plan.destination_ref]
    return argv


def run_oras_copy(
    plan: CopyPlan,
    authfile: Path,
    *,
    runner: Runner | None = None,
) -> CopyOutcome:
    """Copy the image **and its referrers** with ``oras cp --recursive``.

    Raises :class:`OrasError` on a non-zero exit. The destination digest and
    the count of copied manifests (image + referrers) are parsed from oras's
    output.
    """
    run = runner if runner is not None else subprocess.run
    argv = build_oras_argv(plan, authfile)
    proc = run(argv, capture_output=True, text=True)
    if proc.returncode != 0:
        raise OrasError(proc.returncode, proc.stderr or "")
    output = (proc.stdout or "") + "\n" + (proc.stderr or "")
    all_digests = _DIGEST_RE.findall(output)
    labeled = _LABELED_DIGEST_RE.findall(output)
    # Only trust the labeled ``Digest:`` line for the destination digest; an
    # unlabeled bare token may be a referrer digest, so fail (retryable)
    # rather than misreport it.
    digest = labeled[-1] if labeled else ""
    if not digest:
        raise ActivityError(
            "activity.unexpected_error",
            "retryable",
            "oras cp exited 0 but reported no labeled destination digest",
        )
    return CopyOutcome(
        destination_ref=plan.destination_ref,
        digest=digest,
        manifests_copied=len(set(all_digests)) or 1,
    )

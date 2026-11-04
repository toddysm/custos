"""Entry point for the copy-image activity (``python -m copy_image`` /
the ``custos-copy-image`` console script).

Reads the activity contract envelope (COPY-IMPL-002), resolves the copy
plan, materializes per-slot credentials (COPY-IMPL-003), runs the
``skopeo`` copy engine (COPY-IMPL-004), and maps any failure onto the
manifest's declared error codes (COPY-IMPL-005), then writes the result
envelope + copy-report.
"""

from __future__ import annotations

import json
import logging
import tempfile
from pathlib import Path

from copy_image.contract import ActivityError, Sandbox, exit_code_for
from copy_image.copy import (
    CopyOutcome,
    OrasError,
    SkopeoError,
    resolve_copy_plan,
    run_oras_copy,
    run_skopeo_copy,
)
from copy_image.credentials import (
    SlotCredentials,
    read_slot_credentials,
    redact,
    release_slot_credentials,
    write_authfile,
)
from copy_image.errors import classify_skopeo_error


def main(argv: list[str] | None = None) -> int:
    """Run the copy with private temporary auth data and release acquired leases on every exit.

    Cleanup failures are reported without changing a completed copy into a retry.
    """
    sandbox = Sandbox.from_env()
    secrets: list[str] = []
    acquired: dict[str, SlotCredentials] = {}
    try:
        inputs = sandbox.read_inputs()
        ctx = sandbox.read_context()
        plan = resolve_copy_plan(inputs, ctx)

        source_creds = read_slot_credentials(sandbox, "source")
        acquired["source"] = source_creds
        secrets.extend([source_creds.secret, source_creds.docker_auth()])
        dest_creds = read_slot_credentials(sandbox, "dest")
        acquired["dest"] = dest_creds
        secrets.extend([dest_creds.secret, dest_creds.docker_auth()])
        run_root = sandbox.base / "run"
        run_root.mkdir(parents=True, exist_ok=True)
        directory = tempfile.TemporaryDirectory(prefix="copy-image-auth-", dir=run_root)
        try:
            authfile = write_authfile(
                Path(directory.name),
                {plan.source_host: source_creds, plan.dest_host: dest_creds},
            )
            outcome = (
                run_oras_copy(plan, authfile)
                if plan.copy_referrers
                else run_skopeo_copy(plan, authfile)
            )
        finally:
            try:
                directory.cleanup()
            except OSError:
                logging.getLogger(__name__).warning("Private auth cleanup failed")
        _write_result(sandbox, outcome)
        return 0
    except (SkopeoError, OrasError) as exc:
        mapped = classify_skopeo_error(exc.stderr, redactions=secrets)
        sandbox.write_failure(mapped.code, mapped.error_class, mapped.message)
        return exit_code_for(mapped.error_class)
    except ActivityError as exc:
        sandbox.write_failure(exc.code, exc.error_class, redact(exc.message, secrets))
        return exit_code_for(exc.error_class)
    except Exception as exc:  # pragma: no cover - last-resort guard
        sandbox.write_failure("activity.unexpected_error", "retryable", redact(str(exc), secrets))
        return 1
    finally:
        for slot, credentials in acquired.items():
            try:
                release_slot_credentials(sandbox, slot, credentials)
            except ActivityError:
                logging.getLogger(__name__).warning("Credential lease cleanup failed for %s", slot)


def _write_result(sandbox: Sandbox, outcome: CopyOutcome) -> None:
    report = {
        "destinationRef": outcome.destination_ref,
        "digest": outcome.digest,
        "manifestsCopied": outcome.manifests_copied,
    }
    sandbox.write_artifact("copy-report", json.dumps(report, indent=2))
    sandbox.write_success(
        {
            "destinationRef": outcome.destination_ref,
            "digest": outcome.digest,
            "manifestsCopied": outcome.manifests_copied,
            "reportRef": Sandbox.artifact_ref("copy-report"),
        }
    )


if __name__ == "__main__":
    raise SystemExit(main())

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
import tempfile
from pathlib import Path

from copy_image.contract import ActivityError, Sandbox, exit_code_for
from copy_image.copy import CopyOutcome, SkopeoError, resolve_copy_plan, run_skopeo_copy
from copy_image.credentials import read_slot_credentials, redact, write_authfile
from copy_image.errors import classify_skopeo_error


def main(argv: list[str] | None = None) -> int:
    sandbox = Sandbox.from_env()
    secrets: list[str] = []
    try:
        inputs = sandbox.read_inputs()
        ctx = sandbox.read_context()
        plan = resolve_copy_plan(inputs, ctx)

        source_creds = read_slot_credentials(sandbox, "source")
        dest_creds = read_slot_credentials(sandbox, "dest")
        secrets = [source_creds.secret, dest_creds.secret]
        authfile = write_authfile(
            Path(tempfile.mkdtemp(prefix="copy-image-auth-")),
            {plan.source_host: source_creds, plan.dest_host: dest_creds},
        )

        outcome = run_skopeo_copy(plan, authfile)
        _write_result(sandbox, outcome)
        return 0
    except SkopeoError as exc:
        mapped = classify_skopeo_error(exc.stderr, redactions=secrets)
        sandbox.write_failure(mapped.code, mapped.error_class, mapped.message)
        return exit_code_for(mapped.error_class)
    except ActivityError as exc:
        sandbox.write_failure(exc.code, exc.error_class, redact(exc.message, secrets))
        return exit_code_for(exc.error_class)
    except Exception as exc:  # pragma: no cover - last-resort guard
        sandbox.write_failure("activity.unexpected_error", "retryable", redact(str(exc), secrets))
        return 1


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

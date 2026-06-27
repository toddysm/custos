"""Entry point for the copy-image activity (``python -m copy_image`` /
the ``custos-copy-image`` console script).

ARM runs the image's entry point with the sandbox already populated:
inputs at ``/custos/in`` and an empty ``/custos/out`` to write into. This
module reads the activity contract envelope (COPY-IMPL-002) and writes the
result envelope. The actual copy engine lands in COPY-IMPL-004; until then
the activity reads its inputs and writes a *permanent* failure envelope so
an accidentally-run image is never mistaken for a successful copy.
"""

from __future__ import annotations

from copy_image.contract import ActivityError, Sandbox, exit_code_for


def main(argv: list[str] | None = None) -> int:
    sandbox = Sandbox.from_env()
    try:
        # Reading the envelope exercises the full input contract; the copy
        # engine (COPY-IMPL-004) consumes ``inputs`` + ``context`` next.
        sandbox.read_inputs()
        sandbox.read_context()
        raise ActivityError(
            "activity.not_implemented",
            "permanent",
            "copy engine not yet implemented (COPY-IMPL-004)",
        )
    except ActivityError as exc:
        sandbox.write_failure(exc.code, exc.error_class, exc.message)
        return exit_code_for(exc.error_class)
    except Exception as exc:  # pragma: no cover - last-resort guard
        sandbox.write_failure("activity.unexpected_error", "retryable", str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

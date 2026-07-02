# TODOs: Local Dev & Test CLI (`custosctl`)

Last Updated: 2026-07-01

## Open

_None._

## Closed

- [x] TODO-001: Chose the extension-folder input form — `register` always reads
the manifest from a local extension folder (or manifest file) PATH and derives a
digest-pinned image from `CUSTOS_IMAGE_PREFIX`; the optional `--image-ref` flag
overrides only that derived image reference. Implemented in #956/#957, documented
in the custosctl reference (#962).

- [x] TODO-003: `e2e` is a user-run smoke (unit-tested with mocked building blocks); whether it runs against a real kind cluster in CI is decided in the CI task #961. Resolved in #960.

- [x] TODO-002: Sample workflow fixture defined — tools/custosctl/src/custosctl/fixtures/sample-workflow.yaml (copy-image); resolved in #959.


# custos-activity-runtime-manager

Custos Activity Runtime Manager (COMP-006).

The Activity Runtime Manager (ARM) is the execution plane for workflow
activities. It schedules activities requested by the Workflow Service over
Dapr, resolves and verifies activity manifests, isolates each run in an OCI
container under an operator-configured sandbox tier, brokers the activity's
typed inputs/outputs and artifacts through the Storage Provider Layer, injects
short-lived secret leases from the Connector Service, and maps the container's
outcome back to the locked `ActivityResultEnvelope` contract the Workflow
Service consumes.

## Status

**In progress** — the `ARM-IMPL-000-ACTIVITY-RUNTIME-MANAGER` milestone
([#591](https://github.com/toddysm/custos/issues/591)) is under active
implementation. This package currently provides the ARM-IMPL-001 scaffold:
the FastAPI application factory and the `/healthz` / `/readyz` probes, wired to
the ruff + mypy (strict) + pytest (≥90 % coverage) quality-gate toolchain.

Tracking issue: [#591](https://github.com/toddysm/custos/issues/591)
(ARM-IMPL-000).

Design reference:
[`design/components/activity-runtime-manager/design.md`](../../../design/components/activity-runtime-manager/design.md).
Implementation plan:
[`design/components/activity-runtime-manager/implementation-plan.md`](../../../design/components/activity-runtime-manager/implementation-plan.md).

## Layout

```
src/custos_arm/
  __init__.py     # re-exports create_app
  app.py          # FastAPI application factory + readiness lifespan
  __main__.py     # uvicorn entry point (HOST/PORT)
  healthz.py      # /healthz + /readyz routes
  _version.py     # package version string
tests/
  test_healthz.py # probe behaviour
  test_smoke.py   # import + factory smoke
```

## Configuration

ARM-IMPL-001 reads only the ASGI server binding:

| Variable | Default     | Purpose                          |
| -------- | ----------- | -------------------------------- |
| `HOST`   | `0.0.0.0`   | uvicorn bind host.               |
| `PORT`   | `8080`      | uvicorn bind port.               |

The `ARM_*` configuration surface (AuthZ dev-shim, resolver, runtime driver,
sandbox tiers) is introduced from ARM-IMPL-002 onward.

## Development

Install the path dependencies and this package, then run the quality gates from
this directory:

```bash
pip install -e ../../libs/custos-callctx[dev]
pip install -e ../../libs/storage-provider-layer[dev]
pip install -e ".[dev]"

ruff format . && ruff check . && mypy src tests && pytest -q
```

Run the service locally:

```bash
python -m custos_arm        # or: custos-activity-runtime-manager
```

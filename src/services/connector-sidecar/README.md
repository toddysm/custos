# Custos Connector Sidecar (COMP-005 sidecar, Phase H)

Per-pod secret-bridge that serves activity-token leases to an activity
container over a Unix Domain Socket.

The sidecar is co-deployed by the Activity Runtime Manager (ARM) into
the same pod as the activity container. It exposes three endpoints on
a UDS at `/custos/run/connector.sock` (mode `0660`, trusted Sidecar
UID 1000 and the activity's supplemental group 1000):

| Method | Path | Purpose |
|---|---|---|
| `GET`  | `/v1/token`         | Mint a lease + upstream credential for a slot/purpose. |
| `POST` | `/v1/token/refresh` | Re-mint while keeping the lease id stable. |
| `POST` | `/v1/token/release` | Authoritative scoped release. |

Every request must carry `Custos-Sidecar-Token: <bootstrap>` — an
HMAC-SHA256-signed envelope ARM minted at sidecar start, bound to the
specific `(runId, stepId, attempt)` triple.

ARM and the sidecar now share the version-one codec from `custos-callctx`.
Malformed/duplicate claims, future issuance, expiry and scope mismatch are
rejected. ARM issues independent keys per workspace/attempt and never extends
an accepted token on replay. Private key delivery and full runtime activation
remain tracked in [#1038](https://github.com/toddysm/custos/issues/1038);
the activity must not receive the verification key alongside its bearer token.

Production loads an owner-only private bundle from
`CUSTOS_SIDECAR_RUNTIME_CONFIG` (default `/custos/sidecar/runtime.json`) and the
adjacent bootstrap key. Missing/unsafe configuration fails startup; it never
selects `StubCredentialMinter`. The bundle carries only attempt addressing and a
private attempt proof, not a service-account credential or general signed context.

The Sidecar forwards to the runtime's private HTTPS credential proxy. The runtime
obtains fresh scoped Auth authorization; Connector Service checks durable accepted
scope, current instance health/capabilities and lease revocation, then calls its
real identity resolvers. Production responses use `schemaVersion: "2"` with
separate `lease` and `credential` objects. `credential.material` is opaque JSON;
`credential.expiresAt: null` means upstream expiry is unknown, not that it equals
`lease.expiresAt`. Consumers must handle the material required by their provider.

TLS verification and redirect rejection are mandatory. Private CA deployments
can provide `CUSTOS_SIDECAR_CA_BUNDLE`; otherwise the image's default trust store
is used. The production broker checks revocation on every operation rather than
serving cached credentials from the legacy control-channel registry.
The explicit legacy factories and stub minter remain test utilities only.

See [design/components/connector-service/design.md § Secret and Token
Flow to
Activities](../../../design/components/connector-service/design.md#secret-and-token-flow-to-activities)
for the normative contract.

## Phase H scope split

| Issue | Title | Ships |
|---|---|---|
| #302 (CONN-IMPL-019) | Sidecar UDS server | This package + integration harness |
| #303 (CONN-IMPL-020) | Sidecar mTLS revoke control-channel | Adds `/sidecar-admin/v1/revoke` |
| #304 (CONN-IMPL-021) | Container image + standalone integration | Dockerfile + e2e wiring |

## Container image (CONN-IMPL-021)

The sidecar ships as the `ghcr.io/toddysm/custos/connector-sidecar`
OCI image. The image runs the `custos-connector-sidecar` console
script (which calls `custos_sidecar.__main__:main`) and reads its
private configuration seeded by ARM (see
[`src/custos_sidecar/broker.py`](src/custos_sidecar/broker.py)).
The [legacy settings](src/custos_sidecar/settings.py) remain for explicit
legacy UDS/mTLS integration harnesses, not production credential issuance.

Build from the repository root:

```sh
docker build -f src/services/connector-sidecar/Dockerfile -t custos-connector-sidecar:dev .
```

The image is multi-stage (`python:3.11-slim` builder → `python:3.11-slim`
runtime), runs as a non-root `sidecar` user (UID 1000), and exposes
the control-channel port `9443/tcp`. The UDS path
`/custos/run/connector.sock` and the bootstrap-state directory
`/custos/in/` are pre-created (owned by `sidecar:sidecar`) so ARM can
mount tmpfs volumes over them without `chown` gymnastics.

CI builds the image as part of the `python-services` workflow, which
fires on every `push` to `main` and on every pull request that
touches anything under `src/services/**` (the workflow shares the
filter across all per-service jobs). The image job runs after the
sidecar's lint + types + tests and integration jobs succeed. On
pull requests the image is built but not pushed (forked-PR
`GITHUB_TOKEN`s cannot write to `ghcr.io`). On `main` the build is
pushed with two tags:

| Tag | When |
|---|---|
| `:dev`        | `push` to `main` (overwrites the previous build) |
| `:sha-<sha>`  | `push` to `main`, immutable |

### Standalone integration harness

[`tests/test_e2e.py`](tests/test_e2e.py) spins up the production
entrypoint wiring (both the UDS server and the mTLS control server,
sharing one `RevocationRegistry` and one `LeaseGateway`) plus a fake
Connector Service stub on a free TCP port and exercises the full
lease lifecycle in one test run:

1. `GET /v1/token` → 200, lease envelope.
2. `POST /v1/token/refresh` → 200, same `leaseId`.
3. `GET /v1/token` again → 200, second lease.
4. mTLS `POST /sidecar-admin/v1/revoke` for lease #1 → 200.
5. `POST /v1/token/refresh` for lease #1 → 410 `lease-revoked` (no
   CS round-trip — registry hit).
6. `POST /v1/token/release` for lease #1 → 410 `lease-revoked`.
7. `POST /v1/token/release` for lease #2 → 204.

Marked `@pytest.mark.integration` so it stays out of the default
unit-test gate; run with `pytest -m integration`.

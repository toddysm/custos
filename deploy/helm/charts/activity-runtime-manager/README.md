# activity-runtime-manager

Custos activity-runtime-manager component chart. See the
[component design](../../../../design/components/activity-runtime-manager/design.md).

The umbrella's default-off `global.receiverRuntime.enabled` mode configures the
real runtime with PostgreSQL ownership, shared CSI artifacts, private staging,
namespaced sandbox control and an HTTPS credential endpoint. See the
[activation and rollback contract](../trigger-service/README.md#production-receiver-activation).

The application serves TLS using an operator-provided Secret. Its public `ca.crt`
is certificate-only, validated at startup and each attempt, bounded to 8 KiB,
and staged only under the trusted Sidecar's
private mount. `tls.key` never enters activity inputs or the Sidecar bundle.
Startup/readiness probes use HTTPS; staging lives in a newly created owner-only
child directory of the writable staging volume, not its group-readable root.
The activity never mounts controller TLS keys or Kubernetes credentials.

Managed mode exposes a separate plaintext, metrics-only listener on port 8090.
The application remains private HTTPS on 8080. Both the static Prometheus job
and ServiceMonitor select the `metrics` port, not the `https` application port;
no certificate verification bypass or TLS-key sharing is required. Network
policy permits metrics from the configured monitoring namespace only. Arbitrary
activity error codes are excluded from exported metric labels; detailed errors
remain in their existing result/audit surfaces. Use one runtime process per Pod
and scale replicas instead of starting multiple uvicorn workers.

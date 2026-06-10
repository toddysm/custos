#!/usr/bin/env bash
# Install the out-of-band prerequisites the Custos umbrella chart depends on.
#
# Background (#851/#852): the umbrella chart intentionally does NOT bundle the
# heavy upstream operators/CRD bundles (Dapr, Envoy Gateway, cert-manager,
# Prometheus, Loki, the OpenTelemetry Collector, Redis, and the optional
# Keycloak / Sealed Secrets backends). Bundling them pushed the packaged chart
# past Helm's 1 MB release-Secret limit, so `helm install custos` always failed
# with `Secret "sh.helm.release.v1.custos.v1" is invalid: data: Too long`.
#
# The umbrella still ships its OWN resources that target these operators' CRDs
# (Dapr Components/Subscriptions, the Gateway/GatewayClass/Certificate, the
# Grafana dashboards), so those CRDs must already be registered before
# `helm install custos`. This script installs each prerequisite idempotently
# (`helm upgrade --install`) at the same versions the chart and the offline
# image bundle (deploy/offline/images.txt) are validated against.
#
# Usage:
#   scripts/install-prereqs.sh [options]
#
# Options (all default to the connected-eval profile's needs):
#   --keycloak         also install Keycloak           (air-gapped auth backend)
#   --sealed-secrets   also install Sealed Secrets      (air-gapped secrets backend)
#   --registry <host>  mirror prefix for air-gapped installs (e.g. registry.internal)
#   --wait             pass `--wait` to every helm install (slower; CI-style)
#   -h, --help         show this help and exit
#
# Toggles honoured from the environment (set to "false" to skip a component):
#   INSTALL_DAPR, INSTALL_ENVOY_GATEWAY, INSTALL_CERT_MANAGER,
#   INSTALL_PROMETHEUS, INSTALL_LOKI, INSTALL_OTEL_COLLECTOR, INSTALL_REDIS
#
# Requires: helm >= 3.14, kubectl, and a reachable cluster (current context).

set -euo pipefail

# --- Pinned versions (keep in sync with deploy/helm/custos/Chart.yaml and
#     deploy/offline/images.txt) ---------------------------------------------
DAPR_VERSION="1.14.0"
ENVOY_GATEWAY_VERSION="1.8.1"
CERT_MANAGER_VERSION="v1.20.2"
PROMETHEUS_VERSION="29.10.0"
LOKI_VERSION="6.55.0"
OTEL_COLLECTOR_VERSION="0.158.1"
REDIS_VERSION="27.0.4"
KEYCLOAK_VERSION="25.2.0"
SEALED_SECRETS_VERSION="2.18.6"

# --- Defaults ---------------------------------------------------------------
INSTALL_KEYCLOAK="false"
INSTALL_SEALED_SECRETS="false"
REGISTRY=""
WAIT_FLAG=""

INSTALL_DAPR="${INSTALL_DAPR:-true}"
INSTALL_ENVOY_GATEWAY="${INSTALL_ENVOY_GATEWAY:-true}"
INSTALL_CERT_MANAGER="${INSTALL_CERT_MANAGER:-true}"
INSTALL_PROMETHEUS="${INSTALL_PROMETHEUS:-true}"
INSTALL_LOKI="${INSTALL_LOKI:-true}"
INSTALL_OTEL_COLLECTOR="${INSTALL_OTEL_COLLECTOR:-true}"
INSTALL_REDIS="${INSTALL_REDIS:-true}"

usage() {
  sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --keycloak) INSTALL_KEYCLOAK="true"; shift ;;
    --sealed-secrets) INSTALL_SEALED_SECRETS="true"; shift ;;
    --registry) REGISTRY="${2:?--registry requires a value}"; shift 2 ;;
    --wait) WAIT_FLAG="--wait"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

require() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: required command not found: $1" >&2
    exit 1
  }
}
require helm
require kubectl

# Optional air-gapped mirror prefix. When set, every helm chart that honours a
# global registry knob is pointed at the mirror.
img() {
  # img <upstream-repo> -> mirror-prefixed repo when --registry is set.
  local repo="$1"
  if [[ -n "$REGISTRY" ]]; then
    echo "${REGISTRY}/${repo}"
  else
    echo "$repo"
  fi
}

log() { echo "==> $*"; }

helm_repo_add() {
  # Idempotent: `helm repo add` re-adds without error unless the URL changes.
  helm repo add "$1" "$2" >/dev/null 2>&1 || true
}

# ---------------------------------------------------------------------------
# Dapr control plane — required by the umbrella's Component/Subscription CRs.
# ---------------------------------------------------------------------------
if [[ "$INSTALL_DAPR" == "true" ]]; then
  log "Dapr ($DAPR_VERSION)"
  helm_repo_add dapr https://dapr.github.io/helm-charts/
  helm upgrade --install dapr dapr/dapr \
    --version "$DAPR_VERSION" \
    --namespace dapr-system --create-namespace \
    ${REGISTRY:+--set global.registry="$(img dapr)"} \
    $WAIT_FLAG
fi

# ---------------------------------------------------------------------------
# Envoy Gateway — backs the GatewayClass the umbrella renders. The gateway-helm
# chart bundles the Gateway API CRDs (in its Helm `crds/` directory) alongside
# its own gateway.envoyproxy.io CRDs, so it is the single owner of those CRDs —
# we deliberately do NOT pre-apply a standalone Gateway API CRD bundle, which
# would conflict on field ownership (server-side apply) and also fail in an
# air-gapped cluster (it fetches from github.com).
# Published as an OCI chart on Docker Hub.
# ---------------------------------------------------------------------------
if [[ "$INSTALL_ENVOY_GATEWAY" == "true" ]]; then
  log "Envoy Gateway ($ENVOY_GATEWAY_VERSION)"
  helm upgrade --install envoy-gateway oci://docker.io/envoyproxy/gateway-helm \
    --version "$ENVOY_GATEWAY_VERSION" \
    --namespace envoy-gateway-system --create-namespace \
    $WAIT_FLAG
fi

# ---------------------------------------------------------------------------
# cert-manager — issues the Gateway's serving TLS (umbrella Issuer/Certificate).
# crds.enabled=true registers the cert-manager.io CRDs the umbrella references.
# ---------------------------------------------------------------------------
if [[ "$INSTALL_CERT_MANAGER" == "true" ]]; then
  log "cert-manager ($CERT_MANAGER_VERSION)"
  helm_repo_add jetstack https://charts.jetstack.io
  helm upgrade --install cert-manager jetstack/cert-manager \
    --version "$CERT_MANAGER_VERSION" \
    --namespace cert-manager --create-namespace \
    --set crds.enabled=true \
    ${REGISTRY:+--set image.repository="$(img quay.io/jetstack/cert-manager-controller)"} \
    $WAIT_FLAG
fi

# ---------------------------------------------------------------------------
# Prometheus — scrapes the Custos services' /metrics endpoints.
# ---------------------------------------------------------------------------
if [[ "$INSTALL_PROMETHEUS" == "true" ]]; then
  log "Prometheus ($PROMETHEUS_VERSION)"
  helm_repo_add prometheus-community https://prometheus-community.github.io/helm-charts
  helm upgrade --install custos-prometheus prometheus-community/prometheus \
    --version "$PROMETHEUS_VERSION" \
    --namespace custos-system --create-namespace \
    $WAIT_FLAG
fi

# ---------------------------------------------------------------------------
# Loki — log aggregation backend. The upstream chart defaults to the
# SimpleScalable deployment, which mandates object storage and fails on a bare
# eval cluster ("Please define loki.storage.bucketNames.chunks"). Pin it to the
# single-binary / filesystem profile the umbrella's bundled subchart used (see
# deploy/helm/custos/values.yaml), with caches / gateway / canary off so the
# eval footprint stays to one pod.
# ---------------------------------------------------------------------------
if [[ "$INSTALL_LOKI" == "true" ]]; then
  log "Loki ($LOKI_VERSION)"
  helm_repo_add grafana https://grafana.github.io/helm-charts
  helm upgrade --install custos-loki grafana/loki \
    --version "$LOKI_VERSION" \
    --namespace custos-system --create-namespace \
    ${REGISTRY:+--set global.image.registry="$REGISTRY"} \
    $WAIT_FLAG \
    -f - <<'EOF'
deploymentMode: SingleBinary
loki:
  auth_enabled: false
  commonConfig:
    replication_factor: 1
  schemaConfig:
    configs:
      - from: "2024-04-01"
        store: tsdb
        object_store: filesystem
        schema: v13
        index:
          prefix: loki_index_
          period: 24h
  storage:
    type: filesystem
  limits_config:
    retention_period: 744h
singleBinary:
  replicas: 1
read:
  replicas: 0
write:
  replicas: 0
backend:
  replicas: 0
chunksCache:
  enabled: false
resultsCache:
  enabled: false
gateway:
  enabled: false
lokiCanary:
  enabled: false
test:
  enabled: false
monitoring:
  selfMonitoring:
    enabled: false
    grafanaAgent:
      installOperator: false
EOF
fi

# ---------------------------------------------------------------------------
# OpenTelemetry Collector — trace/metric pipeline. The contrib image's binary
# is `otelcol-contrib`, so `command.name` must be set or the pod crash-loops;
# the chart also ships no usable default pipeline. Mirror the umbrella subchart's
# OTLP-in / debug-out config (deploy/helm/custos/values.yaml). The literal
# ${env:MY_POD_IP} is an OTel config reference resolved at runtime (the chart
# injects MY_POD_IP into the Pod), NOT a shell variable — the quoted heredoc
# keeps it intact.
# ---------------------------------------------------------------------------
if [[ "$INSTALL_OTEL_COLLECTOR" == "true" ]]; then
  log "OpenTelemetry Collector ($OTEL_COLLECTOR_VERSION)"
  helm_repo_add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts
  helm upgrade --install custos-otel-collector open-telemetry/opentelemetry-collector \
    --version "$OTEL_COLLECTOR_VERSION" \
    --namespace custos-system --create-namespace \
    --set image.repository="$(img otel/opentelemetry-collector-contrib)" \
    $WAIT_FLAG \
    -f - <<'EOF'
mode: deployment
replicaCount: 1
command:
  name: otelcol-contrib
presets:
  kubernetesAttributes:
    enabled: false
config:
  receivers:
    otlp:
      protocols:
        grpc:
          endpoint: ${env:MY_POD_IP}:4317
        http:
          endpoint: ${env:MY_POD_IP}:4318
  exporters:
    debug: {}
  service:
    pipelines:
      traces:
        receivers: [otlp]
        exporters: [debug]
      metrics:
        receivers: [otlp]
        exporters: [debug]
      logs:
        receivers: [otlp]
        exporters: [debug]
EOF
fi

# ---------------------------------------------------------------------------
# Redis — Dapr pub/sub broker. The umbrella's custos-pubsub Component targets
# the `custos-redis-master:6379` Service and the `custos-redis` Secret, so the
# release name MUST be `custos-redis` for those names to resolve.
# ---------------------------------------------------------------------------
if [[ "$INSTALL_REDIS" == "true" ]]; then
  log "Redis ($REDIS_VERSION) — release name custos-redis"
  helm_repo_add bitnami https://charts.bitnami.com/bitnami
  helm upgrade --install custos-redis bitnami/redis \
    --version "$REDIS_VERSION" \
    --namespace custos-system --create-namespace \
    --set architecture=standalone \
    ${REGISTRY:+--set global.imageRegistry="$REGISTRY"} \
    $WAIT_FLAG
fi

# ---------------------------------------------------------------------------
# Optional: Keycloak (air-gapped OIDC backend).
# ---------------------------------------------------------------------------
if [[ "$INSTALL_KEYCLOAK" == "true" ]]; then
  log "Keycloak ($KEYCLOAK_VERSION) — release name custos-keycloak"
  helm_repo_add bitnami https://charts.bitnami.com/bitnami
  helm upgrade --install custos-keycloak bitnami/keycloak \
    --version "$KEYCLOAK_VERSION" \
    --namespace custos-system --create-namespace \
    ${REGISTRY:+--set global.imageRegistry="$REGISTRY"} \
    $WAIT_FLAG
fi

# ---------------------------------------------------------------------------
# Optional: Sealed Secrets (air-gapped secrets backend).
# ---------------------------------------------------------------------------
if [[ "$INSTALL_SEALED_SECRETS" == "true" ]]; then
  log "Sealed Secrets ($SEALED_SECRETS_VERSION) — release name custos-sealed-secrets"
  helm_repo_add sealed-secrets https://bitnami-labs.github.io/sealed-secrets
  helm upgrade --install custos-sealed-secrets sealed-secrets/sealed-secrets \
    --version "$SEALED_SECRETS_VERSION" \
    --namespace custos-system --create-namespace \
    ${REGISTRY:+--set image.registry="$REGISTRY"} \
    $WAIT_FLAG
fi

log "Prerequisites installed. You can now run 'helm install custos deploy/helm/custos ...'."

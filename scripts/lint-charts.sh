#!/usr/bin/env bash
set -euo pipefail

# Lint the umbrella chart against every profile values file.

CHART_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../deploy/helm/custos" && pwd)"

cd "$CHART_DIR"

echo "==> helm dependency update"
helm dependency update

for values in values-connected-eval.yaml values-connected-ha.yaml values-airgapped-eval.yaml values-airgapped-ha.yaml; do
  echo "==> helm lint with $values"
  helm lint . -f "$values"
done

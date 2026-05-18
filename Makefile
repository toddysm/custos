# Top-level Custos Makefile.
# Wraps Helm lint/template and the offline bundle.

CHART_DIR := deploy/helm/custos
PROFILES  := connected-eval connected-ha airgapped-eval airgapped-ha
BUILD_DIR := build

# Fail on the first error in any recipe shell line. Without this, a multi-line
# `for` loop continues past a failed `helm template` and Make can exit 0.
SHELL    := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

TEMPLATE_TARGETS := $(addprefix template-,$(PROFILES))

.PHONY: lint template $(TEMPLATE_TARGETS) deps clean bundle help

help:
	@echo "Targets:"
	@echo "  lint              - helm lint umbrella chart against all 4 profiles"
	@echo "  template          - render manifests to $(BUILD_DIR)/ for all 4 profiles"
	@echo "  template-<profile>- render a single profile (connected-eval | connected-ha | airgapped-eval | airgapped-ha)"
	@echo "  bundle            - build air-gapped offline tarball (delegates to deploy/offline)"
	@echo "  clean             - remove build artifacts"

lint:
	./scripts/lint-charts.sh

deps:
	cd $(CHART_DIR) && helm dependency update

template: deps $(TEMPLATE_TARGETS)

# Per-profile render. Each target runs in its own recipe shell so a failure
# of any one profile aborts the parent `template` target via prerequisite
# failure (no silent continuation across profiles).
$(TEMPLATE_TARGETS): template-%: deps
	@mkdir -p $(BUILD_DIR)
	@echo "==> render $*"
	helm template custos $(CHART_DIR) -f $(CHART_DIR)/values-$*.yaml \
	  > $(BUILD_DIR)/manifests-$*.yaml

bundle:
	$(MAKE) -C deploy/offline bundle

clean:
	rm -rf $(BUILD_DIR)
	rm -rf $(CHART_DIR)/charts
	rm -f $(CHART_DIR)/Chart.lock

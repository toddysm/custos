# Top-level Custos Makefile.
# Wraps Helm lint/template and the offline bundle.

CHART_DIR := deploy/helm/custos
PROFILES  := connected-eval connected-ha airgapped-eval airgapped-ha
BUILD_DIR := build

.PHONY: lint template clean bundle help

help:
	@echo "Targets:"
	@echo "  lint       - helm lint umbrella chart against all 4 profiles"
	@echo "  template   - render manifests to $(BUILD_DIR)/ for all 4 profiles"
	@echo "  bundle     - build air-gapped offline tarball (delegates to deploy/offline)"
	@echo "  clean      - remove build artifacts"

lint:
	./scripts/lint-charts.sh

template:
	@mkdir -p $(BUILD_DIR)
	@cd $(CHART_DIR) && helm dependency update
	@for profile in $(PROFILES); do \
	  echo "==> render $$profile"; \
	  helm template custos $(CHART_DIR) -f $(CHART_DIR)/values-$$profile.yaml \
	    > $(BUILD_DIR)/manifests-$$profile.yaml; \
	done

bundle:
	$(MAKE) -C deploy/offline bundle

clean:
	rm -rf $(BUILD_DIR)
	rm -rf $(CHART_DIR)/charts
	rm -f $(CHART_DIR)/Chart.lock

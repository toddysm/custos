# Top-level Custos Makefile.
# Wraps Helm lint/template and the offline bundle.

CHART_DIR := deploy/helm/custos
PROFILES  := connected-eval connected-ha airgapped-eval airgapped-ha
BUILD_DIR := build

# Image build settings. Override on the command line, e.g.
#   make docker-build-api-gateway IMAGE_TAG=v1.2.3
# The build context is always the repository root so service images can install
# their in-repo `custos-*` path libraries (see src/services/<svc>/Dockerfile).
IMAGE_REGISTRY ?= ghcr.io/toddysm/custos
IMAGE_TAG      ?= dev
SERVICES       := api-gateway auth-service workflow-service trigger-service \
                  connector-service activity-runtime-manager catalog-service \
                  observability-audit-service
# Run-to-completion job images. These live under src/jobs/<name> (not
# src/services), so they get their own build recipe.
JOBS           := migrate
DOCKER_BUILD_TARGETS     := $(addprefix docker-build-,$(SERVICES))
DOCKER_BUILD_JOB_TARGETS := $(addprefix docker-build-job-,$(JOBS))

# Fail on the first error in any recipe shell line. Without this, a multi-line
# `for` loop continues past a failed `helm template` and Make can exit 0.
SHELL    := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

TEMPLATE_TARGETS := $(addprefix template-,$(PROFILES))

.PHONY: lint template $(TEMPLATE_TARGETS) deps clean bundle help helm-test docker-build $(DOCKER_BUILD_TARGETS) $(DOCKER_BUILD_JOB_TARGETS)

help:
	@echo "Targets:"
	@echo "  lint              - helm lint umbrella chart against all 4 profiles"
	@echo "  template          - render manifests to $(BUILD_DIR)/ for all 4 profiles"
	@echo "  template-<profile>- render a single profile (connected-eval | connected-ha | airgapped-eval | airgapped-ha)"
	@echo "  helm-test         - pytest-based render assertions in tests/helm (requires helm + python)"
	@echo "  docker-build      - build all service + job images (context = repo root)"
	@echo "  docker-build-<svc>- build a single service image (e.g. docker-build-api-gateway)"
	@echo "  docker-build-job-<job>- build a single job image (e.g. docker-build-job-migrate)"
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

helm-test: deps
	cd tests/helm && pip install -e . >/dev/null && pytest -q

# Build all service + job images. The build context is the repository root so
# each Dockerfile can copy the in-repo `custos-*` path libraries it depends on.
docker-build: $(DOCKER_BUILD_TARGETS) $(DOCKER_BUILD_JOB_TARGETS)

# Per-service image build. `docker-build-<svc>` builds src/services/<svc> with
# the repo root as the context, tagging `$(IMAGE_REGISTRY)/<svc>:$(IMAGE_TAG)`.
# Provenance OCI manifest annotations are applied by the publish workflow
# (DEPLOY-IMPL-006), not here.
$(DOCKER_BUILD_TARGETS): docker-build-%:
	docker build -f src/services/$*/Dockerfile -t $(IMAGE_REGISTRY)/$*:$(IMAGE_TAG) .

# Per-job image build. `docker-build-job-<job>` builds src/jobs/<job> with the
# repo root as the context, tagging `$(IMAGE_REGISTRY)/custos-<job>:$(IMAGE_TAG)`
# to match the Helm hook image references (e.g. custos-migrate).
$(DOCKER_BUILD_JOB_TARGETS): docker-build-job-%:
	docker build -f src/jobs/$*/Dockerfile -t $(IMAGE_REGISTRY)/custos-$*:$(IMAGE_TAG) .

clean:
	rm -rf $(BUILD_DIR)
	rm -rf $(CHART_DIR)/charts
	rm -f $(CHART_DIR)/Chart.lock

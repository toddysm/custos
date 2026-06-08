# Air-gapped offline install bundle.
#
# Assembles a single self-contained tarball for clusters with no internet
# access:
#
#   custos-${VERSION}-airgapped.tar.gz
#     chart/custos/         vendored umbrella chart (helm dependency update)
#     images/*.tar          `docker save` archive per row in images.txt
#     connectors/           connectors-core OCI artifact (when available)
#     checksums.sha256      sha256 of every bundled file
#     INSTALL-AIRGAPPED.md  operator runbook
#
# See design/architecture/reference-deployment.md
# (§ Air-Gapped Offline-Install Bundle).

SHELL       := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

VERSION     ?= 0.1.0
ROOT        := ../..
CHART_SRC   := $(ROOT)/deploy/helm/custos
IMAGES_FILE := images.txt

# When set, the in-repo Custos images (rows under $(CUSTOS_IMAGE_PREFIX)) are
# retagged to this value as they are saved, so a tagged release bundles
# `:vX.Y.Z` even though images.txt tracks the `:dev` default. Empty keeps the
# tags written in images.txt.
CUSTOS_IMAGE_TAG    ?=
CUSTOS_IMAGE_PREFIX ?= ghcr.io/toddysm/custos/

# Optional connectors-core OCI artifact. Saved into connectors/ when it can be
# pulled; otherwise a placeholder note is written (the artifact is published and
# mirrored separately — see the runbook). Override to bundle a specific build.
CONNECTORS_CORE_IMAGE ?= ghcr.io/toddysm/custos/connectors-core:$(VERSION)

STAGE  := custos-$(VERSION)-airgapped
OUTPUT := $(STAGE).tar.gz

.PHONY: bundle clean

bundle: clean
	@echo "==> staging $(STAGE)/"
	mkdir -p "$(STAGE)/images" "$(STAGE)/connectors"

	@echo "==> vendoring chart (helm dependency update)"
	helm dependency update "$(CHART_SRC)" >/dev/null
	mkdir -p "$(STAGE)/chart"
	cp -R "$(CHART_SRC)" "$(STAGE)/chart/custos"
	# Drop local scratch + lock files; charts/ is reconstructed on the target.
	rm -rf "$(STAGE)/chart/custos/tmpcharts-"* "$(STAGE)/chart/custos/Chart.lock"

	@echo "==> saving images from $(IMAGES_FILE)"
	grep -vE '^[[:space:]]*(#|$$)' "$(IMAGES_FILE)" | while read -r ref; do \
	  [ -z "$$ref" ] && continue; \
	  save="$$ref"; \
	  if [ -n "$(CUSTOS_IMAGE_TAG)" ] && [[ "$$ref" == $(CUSTOS_IMAGE_PREFIX)* ]]; then \
	    save="$${ref%:*}:$(CUSTOS_IMAGE_TAG)"; \
	  fi; \
	  file="$$(echo "$$save" | sed -E 's#[/:@]#_#g').tar"; \
	  echo "    $$save -> images/$$file"; \
	  docker pull -q "$$save" >/dev/null; \
	  docker save -o "$(STAGE)/images/$$file" "$$save"; \
	done

	@echo "==> connectors-core artifact"
	if docker pull -q "$(CONNECTORS_CORE_IMAGE)" >/dev/null 2>&1; then \
	  file="$$(echo "$(CONNECTORS_CORE_IMAGE)" | sed -E 's#[/:@]#_#g').tar"; \
	  docker save -o "$(STAGE)/connectors/$$file" "$(CONNECTORS_CORE_IMAGE)"; \
	  echo "    saved $(CONNECTORS_CORE_IMAGE)"; \
	else \
	  { echo "$(CONNECTORS_CORE_IMAGE) was not available at bundle time."; \
	    echo "Mirror the connectors-core OCI artifact into your internal registry separately."; \
	  } > "$(STAGE)/connectors/README.txt"; \
	  echo "    connectors-core not pullable; wrote placeholder note"; \
	fi

	@echo "==> runbook"
	sed 's/__VERSION__/$(VERSION)/g' INSTALL-AIRGAPPED.md.tmpl > "$(STAGE)/INSTALL-AIRGAPPED.md"

	@echo "==> checksums"
	cd "$(STAGE)" && find chart images connectors INSTALL-AIRGAPPED.md -type f | sort | { \
	  if command -v sha256sum >/dev/null 2>&1; then xargs sha256sum; \
	  else xargs shasum -a 256; fi; } > checksums.sha256

	@echo "==> packaging $(OUTPUT)"
	tar -czf "$(OUTPUT)" "$(STAGE)"
	@echo "==> done: $(OUTPUT)"

clean:
	rm -rf custos-*-airgapped custos-*-airgapped.tar.gz custos-offline-*.tar.gz

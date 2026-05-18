# Air-gapped offline install bundle.
# STUB — full implementation tracked in reference-deployment.md TODO-002.
# `bundle` is intentionally a no-op that exits 0 so the top-level
# `make bundle` does not break CI before the implementation lands.

VERSION ?= 0.1.0
OUTPUT  := custos-offline-$(VERSION).tar.gz

.PHONY: bundle clean

bundle:
	@echo "[stub] offline bundle is not yet implemented."
	@echo "       TODO: vendor charts, archive images, render manifests, build $(OUTPUT)"
	@echo "       Tracked in design/architecture/reference-deployment.md (TODO-002)."

clean:
	rm -f custos-offline-*.tar.gz

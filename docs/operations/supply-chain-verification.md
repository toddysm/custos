# Supply-chain verification policy

Every Custos image published to `ghcr.io/toddysm/custos/<name>` carries one
keyless [cosign](https://docs.sigstore.dev/) **signature** plus three
keyless **attestations** (two SBOMs + SLSA provenance), all signed with the
build workflow's short-lived [Fulcio](https://docs.sigstore.dev/certificate_authority/overview/)
identity and recorded in the [Rekor](https://docs.sigstore.dev/logging/overview/)
transparency log:

| Artifact | Kind | Produced by | cosign predicate type |
|---|---|---|---|
| Image signature | signature | `cosign sign` | — |
| SBOM (SPDX) | attestation | Syft | `spdxjson` |
| SBOM (CycloneDX) | attestation | Syft | `cyclonedx` |
| SLSA provenance | attestation | build workflow | `slsaprovenance` |

The signing identity (the OIDC "subject") is the build workflow itself:

- **Issuer:** `https://token.actions.githubusercontent.com`
- **Identity:** `https://github.com/toddysm/custos/.github/workflows/build-images.yml@refs/heads/main`
  (or `@refs/tags/vX.Y.Z` for release builds)

## Online verification

Verify against the digest (recommended) or a tag. Export the policy once:

```bash
export COSIGN_EXPERIMENTAL=1
IMAGE=ghcr.io/toddysm/custos/api-gateway@sha256:<digest>
IDENTITY='https://github.com/toddysm/custos/.github/workflows/build-images.yml@refs/heads/main'
ISSUER='https://token.actions.githubusercontent.com'
```

Verify the image signature:

```bash
cosign verify \
  --certificate-identity "$IDENTITY" \
  --certificate-oidc-issuer "$ISSUER" \
  "$IMAGE"
```

Verify the SBOM and SLSA provenance attestations:

```bash
cosign verify-attestation --type spdxjson \
  --certificate-identity "$IDENTITY" \
  --certificate-oidc-issuer "$ISSUER" \
  "$IMAGE"

cosign verify-attestation --type cyclonedx \
  --certificate-identity "$IDENTITY" \
  --certificate-oidc-issuer "$ISSUER" \
  "$IMAGE"

cosign verify-attestation --type slsaprovenance \
  --certificate-identity "$IDENTITY" \
  --certificate-oidc-issuer "$ISSUER" \
  "$IMAGE"
```

For release builds substitute the tag ref, or match either with a regexp:

```bash
cosign verify \
  --certificate-identity-regexp '^https://github.com/toddysm/custos/\.github/workflows/build-images\.yml@refs/(heads/main|tags/v.*)$' \
  --certificate-oidc-issuer "$ISSUER" \
  "$IMAGE"
```

## Offline / air-gapped verification

Air-gapped operators cannot reach the public Fulcio root, the Rekor log, or the
GHCR signatures at verification time, so the trust material and signatures must
be mirrored alongside the images.

### 1. On a connected host — bundle the trust root and signatures

```bash
# Sigstore trust root (Fulcio + Rekor + CT public keys), pinned to a point in time.
cosign initialize
tar -czf sigstore-root.tgz -C "$HOME/.sigstore" root

# Copy each image's signatures + attestations into the offline mirror. The
# signature/attestation objects are themselves OCI artifacts in the same repo.
for img in $(cat images.txt); do
  cosign save "$img" --dir "offline/$(basename "$img")"
done
```

`cosign save` writes the image **and** its associated signatures/attestations to
a local OCI layout that can be `cosign load`ed into the internal registry.

### 2. In the air-gapped environment — load and verify offline

```bash
# Restore the pinned Sigstore trust root.
tar -xzf sigstore-root.tgz -C "$HOME/.sigstore"

# Push images + signatures into the internal registry.
for dir in offline/*; do
  cosign load --dir "$dir" registry.internal/custos/$(basename "$dir")
done

# Verify using the bundled Rekor entry (no network), against the internal copy.
cosign verify \
  --insecure-ignore-tlog=false \
  --offline=true \
  --certificate-identity-regexp '^https://github.com/toddysm/custos/\.github/workflows/build-images\.yml@refs/(heads/main|tags/v.*)$' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  registry.internal/custos/api-gateway@sha256:<digest>
```

`--offline=true` verifies the Rekor inclusion proof from the signature bundle
that `cosign save`/`load` carried along, so no transparency-log lookup is needed
at verification time. Pin the Sigstore root (step 1) to a known-good snapshot so
verification is deterministic in the disconnected environment.

### Cluster admission (optional)

To enforce the policy at admission time, configure
[sigstore policy-controller](https://docs.sigstore.dev/policy-controller/overview/)
(or Kyverno) with a `ClusterImagePolicy` that requires the same identity +
issuer for every `ghcr.io/toddysm/custos/*` (or mirrored `registry.internal/custos/*`)
image, so unsigned or mis-signed images are rejected.

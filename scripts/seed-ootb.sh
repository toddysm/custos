#!/usr/bin/env bash
#
# Onboard the out-of-the-box (OOTB) connector-types and the copy-image
# activity-type into a running Custos catalog. Idempotent and re-runnable.
#
# This is the standalone onboarding step run after the platform is deployed and
# the OOTB images are published. It registers, via the public Catalog API:
#   * connector-types  dockerhub, ghcr   (POST /v1/catalog/connector-types)
#   * activity-type    custos.builtin/copy-image
#                      (POST /v1/workspaces/custos.builtin/activity-types)
#
# After onboarding, create connector *instances* using the usage guides under
# docs/users/connectors/ (dockerhub.md, ghcr.md); the OOTB catalog is indexed in
# extensions/README.md.
#
# It resolves each published image's digest and registers against it; it refuses
# to register against the manifest placeholder digest. Re-registering the same
# (type, version) with the same digest is idempotent; a different digest is a
# conflict (bump the version, or pass --allow-existing to treat it as non-fatal).
#
# No connector credentials are handled here — those are supplied later when an
# operator creates connector *instances* (see docs/users/connectors/).
#
# Usage:
#   GATEWAY=https://custos.local TOKEN=cst_... scripts/seed-ootb.sh [--allow-existing]
#
# Environment:
#   GATEWAY        (required) Custos API gateway base URL, e.g. https://custos.local
#   TOKEN          (required) platform-admin service token (cst_...). Needed
#                  because custos.builtin activity registration and platform-
#                  scoped connector-type registration require platform admin.
#   IMAGE_PREFIX   image repository prefix (default: ghcr.io/toddysm/custos)
#   INSECURE       set to 1 to pass `curl -k` (eval self-signed gateway cert)
#
# Flags:
#   --allow-existing   treat a 409 digest-conflict as a non-fatal warning
#   -h, --help         show this help
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

GATEWAY="${GATEWAY:-}"
TOKEN="${TOKEN:-}"
IMAGE_PREFIX="${IMAGE_PREFIX:-ghcr.io/toddysm/custos}"
INSECURE="${INSECURE:-}"
BUILTIN_NS="custos.builtin"
PLACEHOLDER_DIGEST="sha256:0000000000000000000000000000000000000000000000000000000000000000"
ALLOW_EXISTING=0

usage() {
  sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed 's/^#\{0,1\} \{0,1\}//; $d'
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --allow-existing) ALLOW_EXISTING=1 ;;
    *) echo "error: unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

[ -n "$GATEWAY" ] || { echo "error: GATEWAY is required" >&2; exit 2; }
[ -n "$TOKEN" ] || { echo "error: TOKEN is required (platform-admin service token)" >&2; exit 2; }
for tool in curl jq python3; do
  command -v "$tool" >/dev/null 2>&1 || { echo "error: ${tool} is required" >&2; exit 2; }
done

CURL_OPTS=(-sS)
[ -n "$INSECURE" ] && CURL_OPTS+=(-k)

# Resolve the sha256 digest of an image reference using whichever registry tool
# is available (docker buildx, skopeo, or crane). Prints "sha256:<hex>".
resolve_digest() {
  local ref="$1" d=""
  if command -v docker >/dev/null 2>&1; then
    d="$(docker buildx imagetools inspect "$ref" --format '{{.Manifest.Digest}}' 2>/dev/null || true)"
  fi
  if [ -z "$d" ] && command -v skopeo >/dev/null 2>&1; then
    d="$(skopeo inspect --format '{{.Digest}}' "docker://$ref" 2>/dev/null || true)"
  fi
  if [ -z "$d" ] && command -v crane >/dev/null 2>&1; then
    d="$(crane digest "$ref" 2>/dev/null || true)"
  fi
  if [ -z "$d" ]; then
    echo "error: could not resolve a digest for ${ref}" >&2
    echo "       need one of: docker buildx, skopeo, or crane (with pull access)" >&2
    return 1
  fi
  if [ "$d" = "$PLACEHOLDER_DIGEST" ]; then
    echo "error: ${ref} resolved to the placeholder digest; publish the image first" >&2
    return 1
  fi
  printf '%s' "$d"
}

# POST a JSON body and classify the response. Idempotent-aware.
api_post() {
  local path="$1" body_file="$2" label="$3"
  local resp code out
  resp="$(curl "${CURL_OPTS[@]}" -w $'\n%{http_code}' -X POST "${GATEWAY}${path}" \
    -H "authorization: Bearer ${TOKEN}" \
    -H 'content-type: application/json' \
    --data-binary @"$body_file")"
  code="${resp##*$'\n'}"
  out="${resp%$'\n'*}"
  case "$code" in
    200|201)
      echo "  ok (${code}): ${label}"
      ;;
    409)
      if printf '%s' "$out" | grep -q "digest_conflict" && [ "$ALLOW_EXISTING" -eq 1 ]; then
        echo "  warn (409 digest_conflict, --allow-existing): ${label} is registered with a different digest — bump the version to re-publish" >&2
      else
        echo "  error (409): ${label} — already registered with a different digest; bump the version or pass --allow-existing" >&2
        printf '%s\n' "$out" >&2
        return 1
      fi
      ;;
    *)
      echo "  error (${code}): ${label}" >&2
      printf '%s\n' "$out" >&2
      return 1
      ;;
  esac
}

register_connector() {
  local name="$1"
  local mf="${REPO_ROOT}/extensions/connectors/${name}/connector-manifest.json"
  [ -f "$mf" ] || { echo "error: ${mf} not found" >&2; return 1; }
  local version image digest ref body
  version="$(jq -r '.metadata.version' "$mf")"
  image="${IMAGE_PREFIX}/${name}:v${version}"
  echo "connector-type ${name}@${version} (${image})"
  digest="$(resolve_digest "$image")" || return 1
  ref="${image}@${digest}"
  echo "  ref ${ref}"
  body="$(mktemp)"
  jq -n --slurpfile m "$mf" --arg ref "$ref" '{manifest: $m[0], referrerRef: $ref}' > "$body"
  if ! api_post "/v1/catalog/connector-types" "$body" "connector-type ${name}@${version}"; then
    rm -f "$body"; return 1
  fi
  rm -f "$body"
}

register_activity_copy_image() {
  local dir="${REPO_ROOT}/extensions/activities/copy-image"
  local mf="${dir}/activity-manifest.yaml"
  [ -f "$mf" ] || { echo "error: ${mf} not found" >&2; return 1; }
  local version image digest ref mjson body
  version="$(grep -E '^  version:' "$mf" | head -1 | awk '{print $2}')"
  image="${IMAGE_PREFIX}/copy-image:v${version}"
  echo "activity-type ${BUILTIN_NS}/copy-image@${version} (${image})"
  digest="$(resolve_digest "$image")" || return 1
  ref="${image}@${digest}"
  echo "  ref ${ref}"
  # YAML -> JSON, then inject the resolved published image + digest into the
  # runtime block (the on-disk manifest carries a placeholder digest).
  mjson="$(python3 - "$mf" <<'PY'
import json, sys
try:
    import yaml
except ImportError:
    sys.stderr.write("error: PyYAML is required to read the activity manifest (pip install pyyaml)\n")
    sys.exit(3)
with open(sys.argv[1]) as fh:
    print(json.dumps(yaml.safe_load(fh)))
PY
)" || return 1
  body="$(mktemp)"
  printf '%s' "$mjson" | jq --arg img "$image" --arg dg "$digest" --arg ref "$ref" \
    '.spec.runtime.image = $img | .spec.runtime.digest = $dg | {manifest: ., referrerRef: $ref}' > "$body"
  if ! api_post "/v1/workspaces/${BUILTIN_NS}/activity-types" "$body" "activity-type ${BUILTIN_NS}/copy-image@${version}"; then
    rm -f "$body"; return 1
  fi
  rm -f "$body"
}

echo "Onboarding OOTB extensions into ${GATEWAY} (image prefix ${IMAGE_PREFIX})"
register_connector dockerhub
register_connector ghcr
register_activity_copy_image
echo "Done."

# Bootstrap the first platform administrator

Custos creates no default bearer credential. A Kubernetes administrator starts
an explicit one-time ceremony that creates the `custos-bootstrap-admin` service
account, grants it the global `platform.admin` role, and stores only a SHA-256
token hash in PostgreSQL. The plaintext exists only on the operator machine and
in a temporary Kubernetes Secret.

## Before you begin

- Install Custos and wait for the API Gateway to become reachable.
- Select the release namespace and kube-context.
- Set `CUSTOS_GATEWAY` to the externally reachable Gateway URL.
- Use Python 3.11+, `kubectl`, Helm, and `custosctl` from this checkout.

The generated token starts with `custos_` and expires after 90 days by default.
Store it in a password manager or external secret manager immediately. Custos
cannot retrieve plaintext; losing it requires recovery.

## custosctl ceremony

For local/evaluation installs:

```bash
export CUSTOS_GATEWAY=https://custos.local
export CUSTOS_TOKEN="$(custosctl bootstrap-admin init --show-token \
  | sed -n 's/^CUSTOS_TOKEN=//p')"
test -n "$CUSTOS_TOKEN"
```

`custosctl` generates 256 random bits locally, creates a temporary Secret using
kubectl stdin, runs the Helm bootstrap hook, verifies the token through the
Gateway's protected service-account API, resets bootstrap mode to `disabled`,
and deletes the Secret. `--show-token` is explicit because normal output and
errors redact it. Use `--keep-secret` instead when an external secret manager
must consume the Secret; delete it after consumption.

For a remote connected or HA release, select its profile and context first:

```bash
export CUSTOS_TARGET=remote
export CUSTOS_KUBE_CONTEXT=production
export CUSTOS_PROFILE=connected-ha
export CUSTOS_GATEWAY=https://custos.example.com
export CUSTOS_TOKEN="$(custosctl bootstrap-admin init --show-token \
  | sed -n 's/^CUSTOS_TOKEN=//p')"
```

## Direct Helm ceremony

Use this path for GitOps, connected/HA, or air-gapped operation. Generate the
token on the administrative machine; Python's standard library needs no network
access.

```bash
export RELEASE=custos NS=custos-system
export VALUES=deploy/helm/custos/values-connected-ha.yaml
export SECRET=custos-bootstrap-admin-init
umask 077
TOKEN_FILE="$(mktemp)"
python3 -c 'import secrets; print("custos_" + secrets.token_urlsafe(32))' \
  >"$TOKEN_FILE"

kubectl -n "$NS" create secret generic "$SECRET" \
  --from-file=token="$TOKEN_FILE"
helm upgrade --install "$RELEASE" deploy/helm/custos -n "$NS" \
  -f "$VALUES" --set postgres.embedded=false \
  --set bootstrap.adminToken.mode=init \
  --set bootstrap.adminToken.secretName="$SECRET" \
  --set bootstrap.adminToken.secretKey=token --wait --timeout 15m

export CUSTOS_TOKEN="$(tr -d '\n' <"$TOKEN_FILE")"
curl --fail-with-body -H "Authorization: Bearer $CUSTOS_TOKEN" \
  "$CUSTOS_GATEWAY/v1/service-accounts/custos-bootstrap-admin/tokens"

helm upgrade --install "$RELEASE" deploy/helm/custos -n "$NS" \
  -f "$VALUES" --set postgres.embedded=false \
  --set bootstrap.adminToken.mode=disabled --wait --timeout 15m
kubectl -n "$NS" delete secret "$SECRET"
rm -f "$TOKEN_FILE"
```

Use `values-connected-eval.yaml`, `values-connected-ha.yaml`,
`values-airgapped-eval.yaml`, or `values-airgapped-ha.yaml` to match the release.
In an air-gapped environment, create the Secret after transfer on the isolated
side, or synchronize it from an approved internal secret manager. Helm values
accept only a Secret name/key; the chart schema rejects a plaintext `token`
field.

If Helm or verification fails, retain the Secret and token file while you
inspect the bootstrap Job. Never paste the token into logs, issue bodies, Helm
values, or command-line `--set` arguments. Remove both after successful
verification.

## Recovery

Recovery is the only supported response to loss, expiry, or suspected
compromise. It revokes every live token for the dedicated bootstrap account
before installing one replacement.

```bash
export CUSTOS_TOKEN="$(custosctl --yes bootstrap-admin recover --show-token \
  | sed -n 's/^CUSTOS_TOKEN=//p')"
```

Without global `--yes`, custosctl requires interactive confirmation. For direct
Helm recovery, repeat the direct ceremony with a new Secret and
`bootstrap.adminToken.mode=recover`, verify the replacement, reset mode to
`disabled`, and delete the Secret. An `init` replay is rejected and recovery
fails if the dedicated service account does not already exist.

## Security lifecycle

1. Plaintext is generated locally and enters Kubernetes only as Secret data.
2. The bootstrap Job validates it and persists only its hash and expiry.
3. Verification uses the normal Gateway/Auth bearer path.
4. Successful automation deletes the temporary Secret by default.
5. Custos never exposes token retrieval; expiry or loss requires recovery.

# Kubernetes connector hooks

The Connector Service owns polling and invocation fences. Its Kubernetes
executor only runs the existing `bind`, `listen` and `health` hooks; it does not
add a poller or an automatic retry authority. Native webhook signature
verification remains Connector-owned and does not launch a plugin.

## Configuration and readiness

Production provider construction rejects the Docker development backend.
Set the following environment variables before enabling Kubernetes execution:

| Variable | Requirement |
|---|---|
| `CONN_HOOK_BACKEND` | `kubernetes`; `docker` is the development default |
| `CONN_HOOK_NAMESPACE` | Dedicated sandbox namespace |
| `CONN_HOOK_HELPER_IMAGE` | Digest-pinned helper image for the selected architecture |
| `CONN_HOOK_SERVICE_ACCOUNT` | Sandbox account, default `connector-hook`; no token is mounted |
| `CONN_CONNECTOR_REGISTRY_URL` | Registry origin, without a path prefix |
| `CONN_CONNECTOR_REGISTRY_TOKEN` | Optional registry bearer; inject as a secret |
| `CONN_HOOK_ARCHITECTURE` | `amd64` (default) or `arm64`; matching Linux nodes must exist |
| `CONN_HOOK_RUNTIME_CLASS` | Optional installed sandbox runtime class |
| `CONN_HOOK_IMAGE_PULL_SECRETS` | Optional comma-separated names in the sandbox namespace |

Controller credentials come only from the in-cluster service account, not local
kubeconfig. Startup checks the namespace, sandbox account and required access:
`create/get/list/delete/patch` Pods, `get/create` Pod exec, and `get` on the named
sandbox service account. Self-subject access reviews use Kubernetes' standard
authenticated-user authorization. Failed readiness prevents admission.
Configuration and authorization checks do not certify image availability or
upstream reachability; validate those with a deployed smoke test.
Namespaces use Kubernetes' 63-character DNS-label rules. Service-account,
image-pull Secret and RuntimeClass references use DNS-subdomain rules, including
dots and names up to 253 characters.

The image workflow publishes `ghcr.io/toddysm/custos/connector-hook-bridge-amd64`
and `ghcr.io/toddysm/custos/connector-hook-bridge-arm64`, each as a single-platform
manifest with the existing annotations, software bill of materials, signature
and provenance. Both are included in the offline inventory. Select the matching
image and pin its digest; an AMD64 Connector Service image is not an ARM64 helper.
A same-architecture Connector Service image also contains the bridge, but the
minimal helper images avoid pulling the service's Python runtime into init.

The Deployment hashes its ConfigMap into the Pod template, so configuration-only
Helm upgrades roll the controller Pods instead of retaining stale environment
values. After rotating credentials in an existing Secret, explicitly restart
the Deployment once the Secret has synchronized; a ConfigMap checksum does not
observe external Secret contents.

The Connector Helm subchart exposes these settings through `hookRuntime`.
Set `enabled`, `namespace`, `helperImage` and `registryUrl` explicitly. The
namespace must differ from the controller namespace. Supply `sandboxEgress`
NetworkPolicy rules for DNS and the required upstreams, and `controllerEgress`
rules for the Kubernetes API and registry when controller network isolation
is enabled. Empty sandbox rules deny all traffic; no broad internet allowance
is added automatically. Use a cluster network implementation that enforces
NetworkPolicy. Image pulls use node networking and the referenced pull secrets.

The chart retains the sandbox namespace and its isolation policy on disable
or uninstall so an uncertain invocation does not lose its retained evidence
or network restrictions. After verified executor termination and audited
recovery, operators can explicitly remove those retained resources.

Registration references are registry-relative `repository@sha256:digest`.
The executor verifies manifest and configuration bytes against their digests,
selects exactly one Linux platform, and pins the resulting platform manifest.
It preserves the original entrypoint, image environment and working directory;
the hook replaces the image's default command. Metadata redirects and compressed
metadata responses are rejected, so registry credentials cannot be redirected.
Use a registry endpoint that serves these documents directly.

## Isolation and confidential input

Each invocation creates one non-restarting Pod. A trusted init container copies
the static bridge into a read-only mount. The plugin initially runs an inert
bridge; private exec streams admit the actual hook only after ownership and
admission checks. Payloads never enter Pod specifications, environment overrides,
ConfigMaps or application-log streams.

The sandbox uses user/group 1000, dropped capabilities, runtime-default seccomp,
a read-only root filesystem, no privilege escalation, no host namespaces, no
service-account token, and two 16-MiB memory-backed volumes. Main-container limits
are one CPU and 256 MiB; init-container limits are 250 millicores and 64 MiB.
Requests and combined output are capped at 4 MiB, framed messages at 6 MiB, and
execution at 300 seconds. These are per-invocation limits, not fleet capacity
certification. Plugin-authored code must not deliberately bypass its standard
streams to log credentials or send them to unrelated upstream services.

The controller retains numeric exit status and closed failure categories,
not plugin diagnostics. Structured error `detail` and `data` are discarded.
Backend exception causes are suppressed at the invocation boundary so standard
logging and trace exception events cannot reintroduce those private diagnostics.
Successful business results continue through their existing private consumers.

## Ownership and recovery

Creation collisions and ambiguous creation responses are not adopted. Resource
operations are bound to a Pod UID; a read-only creation nonce also rejects stale
exec requests aimed at a reused name. A one-shot marker prevents a second
accepted invocation in the same sandbox.

After exec might have been accepted, cleanup requires positive termination of
the owned container. A lost-node status, missing API object, deletion response,
expired lease or successful local stream closure is not termination proof.
The owned finalizer retains evidence until cleanup can be verified. Finalizer
removal preserves unrelated finalizers and uses a resource-version precondition.

Unconfirmed cleanup raises `PluginCleanupFailed`. Durable polling remains fenced
for audited operator recovery; do not remove finalizers or clear fences merely
because a Pod is absent. Confirm the old invocation and executor cannot resume,
including on an unreachable node, before authorizing recovery.

An unknown exec outcome is a separate reason to retain the fence: losing a
response after dispatch does not become safe to retry merely because the Pod
was subsequently stopped. Malformed/lost responses and cancellation or timeout
while awaiting an outcome retain the stopped Pod's finalizer as recovery
evidence. Inspect possible upstream effects before authorizing audited recovery;
remove retained Kubernetes evidence only after that recovery decision. A new
replica must not automatically repeat the invocation.
Bridge-level timeout, output-limit or execution failures are not completed
plugin responses: they can follow upstream effects without preserving the
plugin result, so they retain the same fence and evidence.
The existing `plugin-cleanup-unconfirmed` audit reason is retained for
compatibility and covers both termination uncertainty and an unknown outcome.
Unusable fenced `listen` responses and completed results discarded during final
cleanup also retain recovery rather than enabling another poll. If termination
was positively verified before that failure, the pause audit records
`terminationConfirmed: true`; that observation does not authorize an automatic
retry or resolve possible upstream effects. Evidence may already have been
removed after confirmed completion, so absence still must not be used as proof.

Cancellation waits for owned cleanup rather than detaching a hook. The cleanup
budget is 30 seconds; an unresponsive backend produces an explicit uncertain
failure instead of a success-shaped response.

## Upgrade and rollback boundary

Deploy the helper image, namespace permissions, pull credentials and explicit
network allowances before activating receiver workers. Keep the helper and
service images digest-pinned and compatible. Do not inject sidecars or mutate
the sandbox execution/security contract: admission verification rejects changes.

Stop new worker admission and settle or fence in-flight work before rollback.
Also drain in-flight work before changing the sandbox namespace, architecture
or execution configuration. An automatic controller rollout is not an unfencing
operation and does not establish termination of an old executor.
An older Docker-only image cannot safely continue Kubernetes invocation recovery.
Never use a backend switch as an unfencing operation. Final receiver/profile
activation and deployed capacity certification are separate rollout gates.

See the [plugin author guide](connector-plugin-author.md) for the unchanged hook
wire contract and [receiver requirements](../../design/requirements/trigger-m2-receivers.md)
for durable recovery policy.

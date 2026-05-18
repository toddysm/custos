# Capability Registry

Last Updated: 2026-05-17
Status: Living document

This is the curated registry of Tier 1 capability tokens that connector type manifests may declare. See `design/components/connector-service/design.md` § Capabilities and Events → Namespace governance for the governance rules.

## Two-Tier Namespace

| Tier | Pattern | Validation |
|---|---|---|
| 1 (Reserved core) | `<prefix>.<verb>[.<sub-verb>]*`, prefix from the table below | Platform validates token against this file at plugin registration |
| 2 (Vendor extension) | `x-<vendor>.<verb>[.<sub>]*` matching `^x-[a-z][a-z0-9-]*\.[a-z][a-z0-9.-]*$` | Syntax-only check; no platform-side semantics |

Plugin registration rejects:

- Tokens that start with a Tier 1 prefix but are not in this registry (`unknown-core-capability`).
- Tokens that match neither Tier 1 nor the `x-*` syntax (`invalid-capability-syntax`).
- Version bumps that drop a previously advertised capability within the same major (`capability-regression`).

## Reserved Core Prefixes

| Prefix | Domain |
|---|---|
| `oci.*` | OCI registries — image and artifact data-plane |
| `s3.*` | S3-compatible object storage |
| `blob.*` | Azure Blob and GCS-style object storage |
| `http.*` | Generic HTTP fetch/post/put |
| `sql.*` | Relational database access |
| `event.*` | Reserved for trigger-stream concerns; MUST NOT appear in `capabilities` (lives under `events` instead) |
| `notification.*` | Outbound notification sinks (Slack, Teams, email, webhook posters) |

Adding a new core prefix is an architecture change and requires a PR against this file plus `design/architecture/changes/`.

## Registered Tier 1 Capabilities

### `oci.*`

| Token | Meaning |
|---|---|
| `oci.pull` | Pull image or artifact from registry |
| `oci.push` | Push image or artifact to registry |
| `oci.copy` | Copy from one repository to another (may be implemented as pull+push internally) |
| `oci.list-tags` | Enumerate tags for a repository |
| `oci.list-referrers` | Query the OCI Referrers API for related artifacts |
| `oci.delete` | Delete a tag, manifest, or blob |
| `oci.sign` | Produce a Sigstore-style signature artifact |
| `oci.verify` | Verify a Sigstore-style signature artifact |

### `s3.*`

| Token | Meaning |
|---|---|
| `s3.read` | Read object(s) |
| `s3.write` | Write object(s) |
| `s3.list` | List bucket contents |
| `s3.delete` | Delete object(s) |
| `s3.copy` | Server-side copy within S3 |

### `blob.*`

| Token | Meaning |
|---|---|
| `blob.read` | Read blob(s) |
| `blob.write` | Write blob(s) |
| `blob.list` | List container contents |
| `blob.delete` | Delete blob(s) |

### `http.*`

| Token | Meaning |
|---|---|
| `http.get` | HTTP GET |
| `http.post` | HTTP POST |
| `http.put` | HTTP PUT |
| `http.delete` | HTTP DELETE |

### `sql.*`

| Token | Meaning |
|---|---|
| `sql.read` | Execute read-only queries |
| `sql.write` | Execute INSERT / UPDATE / DELETE |
| `sql.exec` | Execute arbitrary statements (DDL, stored procedures) |

### `notification.*`

| Token | Meaning |
|---|---|
| `notification.send` | Send a single notification |
| `notification.broadcast` | Send to multiple recipients in one call |
| `slack.post` | Vendor-shaped Slack post (kept under `notification.*` parent for v1) |
| `teams.post` | Vendor-shaped Teams post |
| `email.send` | Send email |

(Tokens like `slack.post` are accepted as Tier 1 in v1 since they appear in the existing manifest examples; future tightening may move vendor-shaped tokens to `x-<vendor>.*`.)

### `event.*`

`event.*` is reserved and MUST NOT appear in `capabilities`. Event-stream behavior is declared in the `events` block (see Connector Service design § Capabilities and Events).

## Adding a New Tier 1 Token

1. Open a PR adding the token to the appropriate sub-table above.
2. Include the meaning in the same row.
3. Link to the connector type(s) that need it, or to the activity manifest(s) requiring it.
4. The Custos architecture review confirms it fits an existing prefix (vs. justifying a new prefix).
5. On merge, the next Connector Service release picks up the updated registry; previously-rejected registrations succeed.

## Deprecation and Removal

A capability can be deprecated within a major version (manifest declares `deprecated: true`); removal is only allowed on the next major bump. Deprecated tokens stay in this registry with a `(deprecated since vX.Y.Z, removed in vN.0.0)` annotation until they are actually removed.

## Change History

| Date | Change |
|---|---|
| 2026-05-17 | Initial registry; reserves `oci.*`, `s3.*`, `blob.*`, `http.*`, `sql.*`, `event.*`, `notification.*` prefixes; seeds initial token list from existing connector examples |

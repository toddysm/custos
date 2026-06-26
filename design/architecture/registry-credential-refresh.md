# Registry Credential Refresh for Activity Consumers

Last Updated: 2026-06-26
Status: Design note (supports #889 copy-image activity and future registry connectors)

This note specifies how a Custos **activity** authenticates to an OCI registry
and **keeps that authentication valid for the full duration of a long
operation** (e.g. copying a multi-GB image), in a way that does **not** depend on
the registry implementing the OCI distribution auth spec correctly. It is a
reusable consumer-side pattern shared by the Docker Hub → GHCR copy activity
(#889) and any future registry connector whose consumers move blobs (ECR, NVCR,
Docker DHI, …).

## Problem

OCI registries authenticate the data plane with a short-lived **bearer JWT**
obtained by exchanging a durable credential at a token endpoint, discovered via
the `WWW-Authenticate: Bearer realm=…` challenge on a `401`. Well-behaved
clients (for spec-compliant registries like Docker Hub and GHCR) re-run that
exchange automatically when the bearer expires mid-operation.

**Several registries break this.** ECR, NVIDIA NVCR/NGC, and Docker DHI do not
implement the challenge correctly (or issue tokens that can't be re-exchanged
through it). Tools such as ORAS and skopeo therefore **fail to refresh** when the
bearer expires part-way through a long copy, aborting the operation.

## Principle

**Custos is the credential authority; refresh is proactive and Custos-driven —
never dependent on the registry returning a spec-compliant challenge.**

The connector (server side) already encapsulates each registry's *real* auth
mechanism and exposes it uniformly through the connector **sidecar**
(`GET /v1/token`, `POST /v1/token/refresh` — see
`design/components/connector-service/design.md` § Secret and Token Flow). The
activity (consumer side) must use that uniform interface instead of the tool's
built-in registry auth, and refresh on a timer.

## Reusable component: sidecar-backed Authenticator + credential helper

A small, reusable consumer-side component that activities embed:

1. **Credential helper** — a thin adapter the copy engine calls for credentials.
   It fetches fresh material from the sidecar (`GET /v1/token?slot=&purpose=`)
   and returns it in the form the engine expects (Basic for spec-compliant
   registries; a pre-minted bearer for connector-minted ones). Uniform,
   registry-spec-independent credential acquisition.

2. **Proactive `Authenticator`** — wraps the registry HTTP transport. It tracks
   the lease `expiresAt` and **re-mints before expiry** (e.g. at ~80 % of TTL)
   via `POST /v1/token/refresh` (stable `leaseId`), updating the transport's
   credential in place. Because refresh is timer-driven, it works even when the
   registry never issues a usable `401` challenge.

3. **Pluggable-auth copy engine** — the engine must let (1)+(2) drive auth.
   Prefer **go-containerregistry / `crane`** (programmable `Authenticator`
   transport) or a custom copy loop. An opaque single-shot `skopeo copy` is
   acceptable **only** for spec-compliant registries, because Custos cannot
   inject refresh into it.

4. **Long-single-layer fallback** — if one layer is larger than the token
   lifetime, copy it with OCI **chunked blob upload** (`PATCH` ranges) and
   re-auth between chunks, bounding the maximum un-refreshable window to a single
   chunk.

## Just-in-time + max-lifetime minting

Mint the credential **right before** the copy and request the **longest lifetime
the registry grants** (ECR ~12 h, NVCR ~hours). For account-scoped registries
this alone makes mid-copy expiry vanishingly rare; the proactive Authenticator
is the backstop for the remainder and for short-lived per-repo bearers.

## Division of responsibility

| Concern | Owner |
|---|---|
| Knowing each registry's real auth mechanism + minting tokens | the **connector** (plugin + sidecar) |
| Uniform `GET /v1/token` + `POST /v1/token/refresh` | the **sidecar** (platform) |
| Credential helper + proactive Authenticator + engine choice + chunked fallback | the **activity** (consumer) |

The connector never has to mint registry bearers per-repo at bind time (it
can't — the repo scope isn't known then); it exposes the durable credential and
a uniform refresh, and the consumer-side Authenticator turns that into
continuous data-plane auth.

## Consumers

- #889 — copy-image activity (Docker Hub → GHCR). First implementation of this
  component; default copy engine `crane`/go-containerregistry.
- Future ECR / NVCR / Docker DHI connectors — reuse the same consumer component
  unchanged; only the connector's token-minting differs.

## References

- `design/components/connector-service/design.md` § Secret and Token Flow to Activities
- #887 (Docker Hub connector) § Registries that break tool-driven refresh
- #889 (copy-image activity)

# User Guide: Custos

Last Updated: 2026-06-08

## Overview

This guide is for **users and evaluators** who want to deploy and operate the Custos
workflow platform — not extend it. If you are writing connectors, activities, or
plugins, see the [Developer Guide](../developers/README.md) instead.

The current release (**M1 — Core engine**) ships a Kubernetes-native control plane
you can stand up on any conformant cluster for evaluation. These pages walk you
through deploying the platform with the **evaluation (`eval`) profile**, verifying
it is healthy, and running your first workflow.

> **M1 limitations.** The evaluation build is intended for trying out the core
> engine, not for production. In M1:
>
> - Authentication uses **operator-bootstrapped service tokens**; the interactive
>    OIDC device-code flow is disabled.
> - There is **no Web UI** — you interact with the platform through its HTTP APIs.
> - Infrastructure runs **single-replica and PersistentVolume-backed** (no HA).
>
> See the [evaluation overview](evaluation/overview.md) for the full list.

## Quickstart (local cluster)

Want to try Custos on your laptop first? The
[local-cluster quickstart](evaluation/local-cluster.md) stands up the platform on
**kind** or **Docker Desktop Kubernetes** with locally built images, then points
you at the first-workflow walkthrough.

## Evaluation deployment

The evaluation guides are organized as a sequence — read them in order the first
time, then use them as a reference.

| Step | Guide | What it covers |
|---|---|---|
| 1 | [Overview](evaluation/overview.md) | What "evaluation" is, what gets deployed, connected vs air-gapped, and M1 limitations |
| 2 | [Prerequisites](evaluation/prerequisites.md) | Tooling versions, cluster requirements, and the operators you install before Custos |
| 3a | [Install — connected](evaluation/install-connected.md) | Deploy the `connected-eval` profile pulling images from the public registry |
| 3b | [Install — air-gapped](evaluation/install-airgapped.md) | Deploy the `airgapped-eval` profile from an offline bundle and a private registry |
| 4 | [Verify](evaluation/verify.md) | Confirm the platform is healthy and find the API gateway endpoint |
| 5 | [Bootstrap administrator](../operations/bootstrap-admin.md) | Create, verify, retain, recover, and clean up the first platform-admin credential |
| 6 | [First workflow](evaluation/first-workflow.md) | Authenticate and run a sample workflow end to end |
| 5b | [Copy-image walkthrough](evaluation/copy-image-walkthrough.md) | Onboard the OOTB connectors + copy-image activity and run a Docker Hub -> GHCR copy end to end |
| 6 | [Troubleshooting](evaluation/troubleshooting.md) | Common failure modes, debug commands, and known issues |
| 7 | [Uninstall](evaluation/uninstall.md) | Tear down an evaluation deployment and clean up |

Choose **one** install path at step 3 based on whether your cluster has outbound
internet access to the public container registry (connected) or is isolated
behind an air gap (air-gapped).

## Connectors

Once the platform is running, connectors let your workflows talk to external
systems. The out-of-the-box **registry connectors** ship ready to use:

| Guide | What it covers |
|---|---|
| [Using the OOTB registry connectors](connectors/README.md) | Shared concepts: the `x-dapr-secret` credential model, auth, and the five-step lifecycle |
| [Docker Hub connector](connectors/dockerhub.md) | Pull from / push to Docker Hub (`custos-dockerhub`) |
| [GHCR connector](connectors/ghcr.md) | Pull from / push to GitHub Container Registry (`custos-ghcr`) |

## Related documentation

| Document | Description |
|---|---|
| [Documentation index](../README.md) | Documentation across all audiences |
| [Developer Guide](../developers/README.md) | Writing connectors, activities, and plugins |
| [Reference deployment](../../design/architecture/reference-deployment.md) | Topology and profile matrix, component inventory, supply chain |
| [Architecture overview](../../design/architecture/overview.md) | System architecture, domain model, and contracts |

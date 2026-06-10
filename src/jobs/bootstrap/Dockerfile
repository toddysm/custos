# syntax=docker/dockerfile:1.7
#
# Custos bootstrap Job (custos-bootstrap) — image (DEPLOY-IMPL-005).
#
# The idempotent Helm post-install hook that seeds a usable platform (canonical
# permissions + roles, the default tenant/workspace, and the admin binding). It
# is a DEDICATED image (D4): it reuses the auth *domain code* (the canonical
# permission registry + built-in role table) but is built and shipped
# independently of the auth-service runtime image.
#
# Follows the canonical multi-stage pattern established by the api-gateway
# reference image (DEPLOY-IMPL-001): the build context is the **repository
# root** so the builder can install the in-repo path libs the job declares —
# ``custos-spl`` (the ``AuthStoreProvider`` interface), ``custos-postgres`` (the
# Postgres auth adapter), and ``custos-auth-service`` (the canonical permission
# registry + role seeders) — before building the job wheel. The runtime stage
# installs everything offline into a venv and runs as a non-root user.
#
# Provenance OCI manifest annotations are stamped by the publish workflow
# (DEPLOY-IMPL-006), not as mutable LABELs here.

ARG PYTHON_VERSION=3.14

# --------------------------------------------------------------------------- #
# Stage 1: builder
# --------------------------------------------------------------------------- #
FROM python:${PYTHON_VERSION}-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

RUN pip install --upgrade pip==24.2 build==1.2.2

# In-repo path libs (per pyproject.toml: custos-spl + custos-postgres +
# custos-auth-service) and the job sources. The in-repo distributions are
# wheeled with ``--no-deps``; the final ``pip wheel`` of the job (without
# ``--no-deps``) resolves the auth-service third-party deps (fastapi, pydantic,
# pyjwt[crypto], httpx, pyyaml, opentelemetry, ...) into the wheel dir so the
# runtime install can be fully offline.
COPY src/libs/storage-provider-layer/ ./libs/storage-provider-layer/
COPY src/libs/custos-postgres/ ./libs/custos-postgres/
COPY src/services/auth-service/ ./services/auth-service/
COPY src/jobs/bootstrap/ ./jobs/bootstrap/

RUN pip wheel --no-deps --wheel-dir /wheels \
        ./libs/storage-provider-layer \
        ./libs/custos-postgres \
        ./services/auth-service

RUN pip wheel --wheel-dir /wheels --find-links /wheels \
        ./jobs/bootstrap

# --------------------------------------------------------------------------- #
# Stage 2: runtime
# --------------------------------------------------------------------------- #
FROM python:${PYTHON_VERSION}-slim AS runtime

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/custos/venv/bin:${PATH}"

RUN groupadd --system --gid 1000 custos && \
    useradd --system --uid 1000 --gid custos --home-dir /home/custos \
            --create-home --shell /usr/sbin/nologin custos

RUN python -m venv /opt/custos/venv && \
    /opt/custos/venv/bin/pip install --upgrade pip==24.2

COPY --from=builder /wheels /tmp/wheels
RUN /opt/custos/venv/bin/pip install --no-index --find-links=/tmp/wheels \
        custos-bootstrap-job && \
    rm -rf /tmp/wheels

# Per-component permission registries (#867 / AS-IMPL-032). The bootstrap seeder
# aggregates every component's permissions.yaml when CUSTOS_AUTH_PERMISSIONS_PATHS
# points at these files (mirroring the auth-service image), so the post-install
# seed and the running auth-service pod load the identical registry surface. An
# empty CUSTOS_AUTH_PERMISSIONS_PATHS falls back to the bundled aggregate embedded
# in the custos_auth wheel; the permission-drift guard test keeps the two paths in
# lockstep.
COPY src/services/auth-service/permissions.yaml /opt/custos/permissions/auth-service.yaml
COPY src/services/catalog-service/permissions.yaml /opt/custos/permissions/catalog-service.yaml
COPY src/services/workflow-service/permissions.yaml /opt/custos/permissions/workflow-service.yaml
COPY src/services/trigger-service/permissions.yaml /opt/custos/permissions/trigger-service.yaml
COPY src/services/connector-service/permissions.yaml /opt/custos/permissions/connector-service.yaml
COPY src/services/observability-audit-service/permissions.yaml /opt/custos/permissions/observability-audit-service.yaml

USER custos
WORKDIR /home/custos

# Run-to-completion Job: no long-lived listener, so no EXPOSE / HEALTHCHECK.
# The container exits 0 once the platform is seeded and non-zero on a seeding
# error, which the Helm post-install hook surfaces as an install failure.
ENTRYPOINT ["custos-bootstrap"]

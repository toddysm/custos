# syntax=docker/dockerfile:1.7
#
# Custos migration Job (custos-migrate) — image (DEPLOY-IMPL-004).
#
# The Helm pre-install/pre-upgrade hook that runs SPL's strict forward-only
# `migrate up`. Follows the canonical multi-stage pattern established by the
# api-gateway reference image (DEPLOY-IMPL-001): the build context is the
# **repository root** so the builder can install the in-repo path libs the job
# declares — ``custos-spl`` (= ``storage-provider-layer``, the migration runner
# + strict policy) and ``custos-postgres`` (which registers the Postgres
# adapter entry points the CLI discovers) — before building the job wheel. The
# runtime stage installs everything offline into a venv and runs as a non-root
# user.
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

# In-repo path libs (per pyproject.toml: custos-spl + custos-postgres) and the
# job sources. ``custos-postgres`` depends on ``custos-spl``; both are built as
# wheels below.
COPY src/libs/storage-provider-layer/ ./libs/storage-provider-layer/
COPY src/libs/custos-postgres/ ./libs/custos-postgres/
COPY src/jobs/migrate/ ./jobs/migrate/

RUN pip wheel --no-deps --wheel-dir /wheels \
        ./libs/storage-provider-layer \
        ./libs/custos-postgres

RUN pip wheel --wheel-dir /wheels --find-links /wheels \
        ./jobs/migrate

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
        custos-migrate-job && \
    rm -rf /tmp/wheels

USER custos
WORKDIR /home/custos

# Run-to-completion Job: no long-lived listener, so no EXPOSE / HEALTHCHECK.
# The container exits 0 on success and non-zero on a remaining revision gap,
# which the Helm hook uses to gate the install/upgrade.
ENTRYPOINT ["custos-migrate-job"]

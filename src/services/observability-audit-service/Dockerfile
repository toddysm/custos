# syntax=docker/dockerfile:1.7
#
# Custos Observability & Audit Service (COMP-008) — service image
# (DEPLOY-IMPL-003).
#
# Follows the canonical multi-stage service pattern established by the
# api-gateway reference image (DEPLOY-IMPL-001): the build context is the
# **repository root** so the builder can install the in-repo ``custos-*`` path
# libraries the service declares (``custos-spl`` = ``storage-provider-layer``;
# ``custos-postgres``; ``custos-loki``; ``custos-prometheus``;
# ``custos-callctx``) before building the service wheel, then the runtime stage
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
# custos-loki + custos-prometheus + custos-callctx) and the service sources.
# ``custos-postgres``, ``custos-loki`` and ``custos-prometheus`` each depend on
# ``custos-spl``; all are built as wheels below.
COPY src/libs/storage-provider-layer/ ./libs/storage-provider-layer/
COPY src/libs/custos-postgres/ ./libs/custos-postgres/
COPY src/libs/custos-loki/ ./libs/custos-loki/
COPY src/libs/custos-prometheus/ ./libs/custos-prometheus/
COPY src/libs/custos-callctx/ ./libs/custos-callctx/
COPY src/services/observability-audit-service/ ./services/observability-audit-service/

RUN pip wheel --no-deps --wheel-dir /wheels \
        ./libs/storage-provider-layer \
        ./libs/custos-postgres \
        ./libs/custos-loki \
        ./libs/custos-prometheus \
        ./libs/custos-callctx

RUN pip wheel --wheel-dir /wheels --find-links /wheels \
        ./services/observability-audit-service

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
        custos-observability-audit-service && \
    rm -rf /tmp/wheels

USER custos
WORKDIR /home/custos

# Application port (plain HTTP; TLS is terminated upstream at the Envoy Gateway)
# and metrics port.
EXPOSE 8080 8090

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "import os,urllib.request,sys; \
url='http://127.0.0.1:%s/healthz' % os.environ.get('PORT','8080'); \
sys.exit(0 if urllib.request.urlopen(url, timeout=2).status == 200 else 1)" \
    || exit 1

ENTRYPOINT ["custos-observability-audit-service"]

# syntax=docker/dockerfile:1.7
#
# Custos API Gateway (COMP-001) — reference service image (DEPLOY-IMPL-001).
#
# This is the canonical multi-stage pattern every Custos *service* image
# follows. Unlike the self-contained ``connector-sidecar`` image, services
# depend on in-repo ``custos-*`` path libraries, so the build context is the
# **repository root** (``docker build -f src/services/api-gateway/Dockerfile .``)
# and the builder copies the exact libraries the service declares in its
# ``pyproject.toml`` before building the service wheel.
#
#   * ``builder`` — python:3.14-slim. Builds wheels for the in-repo path libs
#     (``custos-spl`` = ``src/libs/storage-provider-layer``; ``custos-callctx``)
#     with ``--no-deps``, then ``pip wheel``s the service so its external
#     dependencies resolve against PyPI while the ``custos-*`` deps resolve
#     against the locally built wheels in ``/wheels``.
#   * ``runtime`` — python:3.14-slim. Installs the service wheel offline
#     (``--no-index --find-links``) into a dedicated virtualenv at
#     ``/opt/custos/venv`` and runs as a non-root ``custos`` user (UID/GID 1000).
#
# Provenance is NOT embedded here as mutable Docker ``LABEL``s. The build/publish
# workflow (DEPLOY-IMPL-006) stamps immutable OCI *manifest* annotations
# (``org.opencontainers.image.*`` + ``vnd.custos.build.*``) at push time via
# ``docker buildx --annotation`` so they bind to the content-addressed manifest.
#
# Default entrypoint runs the ``custos-api-gateway`` console script.

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

# Copy the in-repo path libraries this service depends on (per its
# ``pyproject.toml``: ``custos-spl`` + ``custos-callctx``) and the service
# sources. ``.dockerignore`` keeps caches, tests, and virtualenvs out of the
# build context.
COPY src/libs/storage-provider-layer/ ./libs/storage-provider-layer/
COPY src/libs/custos-callctx/ ./libs/custos-callctx/
COPY src/services/api-gateway/ ./services/api-gateway/

# Build wheels for the in-repo libraries first (no deps — their external
# requirements are resolved transitively when the service is wheeled below).
RUN pip wheel --no-deps --wheel-dir /wheels \
        ./libs/storage-provider-layer \
        ./libs/custos-callctx

# Build the service wheel plus all remaining (external) dependency wheels.
# ``--find-links /wheels`` lets the ``custos-spl`` / ``custos-callctx``
# requirements resolve against the locally built wheels.
RUN pip wheel --wheel-dir /wheels --find-links /wheels \
        ./services/api-gateway

# --------------------------------------------------------------------------- #
# Stage 2: runtime
# --------------------------------------------------------------------------- #
FROM python:${PYTHON_VERSION}-slim AS runtime

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/custos/venv/bin:${PATH}"

# Non-root runtime user. UID/GID 1000 matches the platform-wide convention
# used by the connector-sidecar and the hardened pod security contexts.
RUN groupadd --system --gid 1000 custos && \
    useradd --system --uid 1000 --gid custos --home-dir /home/custos \
            --create-home --shell /usr/sbin/nologin custos

# Dedicated virtualenv keeps the runtime python clean.
RUN python -m venv /opt/custos/venv && \
    /opt/custos/venv/bin/pip install --upgrade pip==24.2

# Install the service offline from the wheels built in the builder stage.
COPY --from=builder /wheels /tmp/wheels
RUN /opt/custos/venv/bin/pip install --no-index --find-links=/tmp/wheels \
        custos-api-gateway && \
    rm -rf /tmp/wheels

USER custos
WORKDIR /home/custos

# Application port (HTTPS REST surface) and metrics port.
EXPOSE 8080 8090

# Liveness/readiness during ``docker run`` smoke tests; Kubernetes uses the
# chart's probes in production.
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "import os,urllib.request,sys; \
url='http://127.0.0.1:%s/healthz' % os.environ.get('PORT','8080'); \
sys.exit(0 if urllib.request.urlopen(url, timeout=2).status == 200 else 1)" \
    || exit 1

ENTRYPOINT ["custos-api-gateway"]

# syntax=docker/dockerfile:1.7
#
# Custos Connector Sidecar (Phase H, CONN-IMPL-021).
#
# Multi-stage build:
#
#   * ``builder`` — python:3.11-slim. Installs build prerequisites and
#     produces a wheel for ``custos-connector-sidecar`` plus pre-fetched
#     wheels for its runtime dependencies (fastapi, uvicorn, pydantic,
#     httpx, opentelemetry-api) in ``/wheels``.
#   * ``runtime`` — python:3.11-slim. Copies ``/wheels`` from the
#     builder into ``/tmp/wheels`` and ``pip install``s the sidecar
#     wheel offline (``--no-index --find-links``) into a dedicated
#     virtualenv at ``/opt/custos/venv``. Runs as a non-root
#     ``sidecar`` user.
#
# The sidecar has zero dependencies on other in-repo Python packages
# (SPL / custos-postgres / etc.); it talks to Connector Service over
# HTTP. The build context therefore is the connector-sidecar
# directory itself — there is no monorepo copy step.
#
# Default entrypoint runs ``custos-connector-sidecar`` (which calls
# :func:`custos_sidecar.__main__.main`). Configuration arrives via
# ``CUSTOS_SIDECAR_*`` environment variables seeded by ARM.

ARG PYTHON_VERSION=3.11

# --------------------------------------------------------------------------- #
# Stage 1: builder
# --------------------------------------------------------------------------- #
FROM python:${PYTHON_VERSION}-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /build

# Install build prerequisites. ``build`` produces the wheel; ``pip``
# (already present) resolves the runtime deps. No native compilers are
# needed for the sidecar's pure-python dependency tree.
RUN pip install --upgrade pip==24.2 build==1.2.2

# Copy the package sources. ``.dockerignore`` keeps caches and test
# artefacts out of the build context.
COPY pyproject.toml README.md ./
COPY src/ ./src/

# Build the wheel.
RUN python -m build --wheel --outdir /wheels .

# Resolve and download the runtime dependency wheels into ``/wheels``
# so the runtime stage can ``pip install`` from a local index with no
# network access.
RUN pip wheel --wheel-dir /wheels \
        "fastapi>=0.110,<1" \
        "uvicorn[standard]>=0.27,<1" \
        "pydantic>=2.6,<3" \
        "httpx>=0.27,<1" \
        "opentelemetry-api>=1.20,<2"

# --------------------------------------------------------------------------- #
# Stage 2: runtime
# --------------------------------------------------------------------------- #
FROM python:${PYTHON_VERSION}-slim AS runtime

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/custos/venv/bin:${PATH}"

# Create the non-root user the sidecar runs as. UID 1000 matches the
# default ARM-injected sidecar UID; the ``activity_gid`` env var is
# used by the sidecar itself (not by Docker) to chown the UDS file
# after bind.
RUN groupadd --system --gid 1000 sidecar && \
    useradd --system --uid 1000 --gid sidecar --home-dir /home/sidecar \
            --create-home --shell /usr/sbin/nologin sidecar

# Create a dedicated virtualenv so the runtime python stays clean and
# system tooling can be upgraded independently.
RUN python -m venv /opt/custos/venv && \
    /opt/custos/venv/bin/pip install --upgrade pip==24.2

# Copy wheels from the builder and install offline.
COPY --from=builder /wheels /tmp/wheels
RUN /opt/custos/venv/bin/pip install --no-index --find-links=/tmp/wheels \
        custos-connector-sidecar && \
    rm -rf /tmp/wheels

# The sidecar binds a UDS under ``/custos/run`` and reads bootstrap
# state from ``/custos/in``. In production ARM mounts both as tmpfs
# volumes; the image creates the mount points (writable for the
# sidecar user) so the container also runs with the upstream
# defaults during local ``docker run`` smoke tests.
RUN mkdir -p /custos/run /custos/in && \
    chown -R sidecar:sidecar /custos

USER sidecar
WORKDIR /home/sidecar

# Document the control-channel port. The UDS surface does not expose
# a TCP port; ARM mounts the socket directory into the activity
# container.
EXPOSE 9443

ENTRYPOINT ["custos-connector-sidecar"]

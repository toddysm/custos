# syntax=docker/dockerfile:1
#
# Out-of-the-box Custos Docker Hub connector plugin.
#
# The image is invoked by the connector-service runtime as
# ``docker run --rm -i <image> <hook>`` with a single JSON request
# envelope on stdin. The ENTRYPOINT routes the hook argument through
# ``dockerhub_plugin.__main__``, which writes a single JSON response to
# stdout.

FROM python:3.12-slim AS runtime

# Hardened defaults: no pip cache, no Python pyc files written, stdout/
# stderr unbuffered so the runtime's wait-for-output loop sees the
# response promptly.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /opt/plugin

COPY pyproject.toml README.md ./
COPY src/ ./src/

# Installing the project wires up the console-script
# ``custos-dockerhub-plugin`` and pulls the single runtime dependency
# (``httpx``) used by the ``health`` reachability probe.
RUN pip install --no-cache-dir .

# Run as an unprivileged user. The plugin only needs stdin/stdout plus
# outbound HTTPS for the health probe; no filesystem writes outside /tmp
# are expected.
RUN useradd --create-home --uid 65532 --shell /usr/sbin/nologin plugin
USER plugin

ENTRYPOINT ["custos-dockerhub-plugin"]

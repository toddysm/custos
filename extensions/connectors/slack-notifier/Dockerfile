# syntax=docker/dockerfile:1
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /opt/plugin

COPY pyproject.toml README.md ./
COPY src/ ./src/

RUN pip install --no-cache-dir .

RUN useradd --create-home --uid 65532 --shell /usr/sbin/nologin plugin
USER plugin

ENTRYPOINT ["custos-slack-notifier-plugin"]

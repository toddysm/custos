FROM python:3.13-alpine

LABEL com.azure.containerizationassist.createdby="containerization-assist"
WORKDIR /app
COPY plugin.py /app/plugin.py
RUN adduser -D -u 10001 appuser
USER appuser
# One-shot hook process; it does not expose a long-running health endpoint.
HEALTHCHECK NONE
ENTRYPOINT ["python", "/app/plugin.py"]
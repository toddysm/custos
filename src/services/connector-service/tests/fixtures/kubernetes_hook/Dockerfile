FROM python:3.13-alpine
WORKDIR /app
COPY plugin.py /app/plugin.py
ENV PYTHONDONTWRITEBYTECODE=1
USER 1000:1000
HEALTHCHECK NONE
ENTRYPOINT ["python", "/app/plugin.py"]

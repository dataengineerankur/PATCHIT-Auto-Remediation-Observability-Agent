ARG BASE_REGISTRY=docker.io/library
FROM ${BASE_REGISTRY}/docker:27.1.1-cli AS dockercli

ARG BASE_REGISTRY=docker.io/library
FROM ${BASE_REGISTRY}/python:3.11-slim

WORKDIR /opt/patchit
COPY pyproject.toml /opt/patchit/pyproject.toml

RUN apt-get update && apt-get install -y --no-install-recommends \
  bash \
  patch \
  git \
  && rm -rf /var/lib/apt/lists/*

# Docker CLI for local validation (uses host daemon via mounted /var/run/docker.sock)
COPY --from=dockercli /usr/local/bin/docker /usr/local/bin/docker

RUN pip install -U pip && pip install \
  fastapi==0.115.6 \
  uvicorn[standard]==0.34.0 \
  httpx==0.28.1 \
  pydantic==2.10.4 \
  pydantic-settings==2.7.0 \
  jsonschema==4.23.0 \
  python-json-logger==3.2.1 \
  PyYAML==6.0.2 \
  pytest==8.3.4 \
  dbt-core==1.9.0 \
  dbt-duckdb==1.9.1 \
  snowflake-connector-python==3.12.4

# Copy source last so dependency layers remain cached during rapid iteration.
COPY patchit /opt/patchit/patchit

EXPOSE 8088
CMD ["uvicorn", "patchit.service.app:app", "--host", "0.0.0.0", "--port", "8088"]




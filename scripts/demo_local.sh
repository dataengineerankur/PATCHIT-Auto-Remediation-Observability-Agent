#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/ankurchopra/repo_projects/PATCHIT-Auto-Remediation-Observability-Agent"

echo "==> Starting docker compose stack"
cd "$ROOT/airflow"
docker compose up --build




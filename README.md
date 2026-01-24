## PATCHIT (Auto-Remediation Data Observability Agent) — Phase-1 MVP

PATCHIT detects pipeline failures (Airflow-first; dbt optional), normalizes errors, collects code/log context, proposes a minimal safe patch, and opens a PR (mock or real) with guardrails + audit logs.

### Quickstart (local, no SaaS)

#### 1) Create a venv + install

```bash
cd /Users/ankurchopra/repo_projects/PATCHIT-Auto-Remediation-Observability-Agent
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"
```

#### 2) Run PATCHIT service

```bash
export PATCHIT_AUDIT_LOG_PATH="$(pwd)/var/audit/patchit_audit.jsonl"
export PATCHIT_GITHUB_MODE="mock"
export PATCHIT_GITHUB_REPO="local/mock"
uvicorn patchit.service.app:app --host 0.0.0.0 --port 8088 --reload
```

#### 3) Run tests

```bash
pytest
```

### Airflow + Mock API (docker-compose)

See `airflow/README.md` for exact commands and the demo runbook.

### Environment variables

- **PATCHIT_GITHUB_MODE**: `mock` (default) or `real`
- **PATCHIT_GITHUB_TOKEN**: required if `real`
- **PATCHIT_GITHUB_REPO**: `owner/repo` if `real`
- **PATCHIT_AUDIT_LOG_PATH**: JSONL audit log output path



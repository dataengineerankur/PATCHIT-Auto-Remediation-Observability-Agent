## Airflow demo stack (PATCHIT complex test pipeline)

This folder runs:
- Airflow (LocalExecutor) with an intentionally failure-prone DAG
- Mock API service
- PATCHIT service

### Prereqs

- Docker Desktop
- `docker compose` available

### Start everything

From repo root:

```bash
cd /Users/ankurchopra/repo_projects/PATCHIT-Auto-Remediation-Observability-Agent/airflow
# IMPORTANT: Use a unique project name so Docker doesn't merge this stack with other "airflow" stacks on your machine.
export GROQ_API_KEY="YOUR_GROQ_KEY_HERE"
# Optional (legacy):
# export BYTEZ_API_KEY="YOUR_BYTEZ_KEY_HERE"
docker compose -p patchit_airflow -f docker-compose.yml up --build
```

Airflow UI: `http://localhost:8080` (user/pass: `airflow` / `airflow`)

PATCHIT service: `http://localhost:8088`

Mock API: `http://localhost:8099`

### Trigger deterministic failure scenarios

In Airflow UI, trigger DAG `patchit_complex_test_dag` with JSON config:

```json
{"scenario": "partial_json"}
```

Supported scenarios:
- `good`
- `partial_json`
- `schema_drift`
- `temporal_error`
- `race_condition`
- `dbt_missing_column`

### Chaos DAG (separate folder)

This repo also mounts `../airflow_extra_dags` into Airflow at `/opt/airflow/dags/extra_dags`.

Trigger DAG `patchit_chaos_dag` with config:

```json
{"scenario": "random"}
```

Other scenarios:
- `key_error`
- `file_not_found`
- `type_error`
- `attribute_error`
- `json_decode`
- `division_by_zero`

### Where to see PRs + audit logs

- Mock PRs: repo root `/.mock_github/prs/*.json` and `*.diff`
- Audit log JSONL: `var/audit/patchit_audit.jsonl`




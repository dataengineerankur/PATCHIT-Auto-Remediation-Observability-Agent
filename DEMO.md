## End-to-end demo script (Phase-1 MVP)

This demo shows: Airflow failure → PATCHIT ingests failure event → mock PR created → quarantine path (optional) → audit logs.

### 0) Start the stack

```bash
cd /Users/ankurchopra/repo_projects/PATCHIT-Auto-Remediation-Observability-Agent/airflow
docker compose up --build
```

### 1) Trigger each failure mode (Airflow UI)

Open Airflow UI at `http://localhost:8080` (airflow/airflow), trigger DAG `patchit_complex_test_dag` with run config.

#### A) Malformed/partial JSON

```json
{"scenario": "partial_json"}
```

Expected: `validate_contract` fails with `DataContractError` wrapping `JSONDecodeError`.

#### B) Schema drift

```json
{"scenario": "schema_drift"}
```

Expected: `transform_payload` fails with `DataContractError: Missing required field userId...`

#### C) Temporal upstream failure + retries

```json
{"scenario": "temporal_error"}
```

Expected: `fetch_payload` retries and then fails with `UpstreamApiError status_code=500`.

#### D) Race condition (read before write completes)

```json
{"scenario": "race_condition"}
```

Expected: downstream reads partial file; JSON parse/contract task fails.

#### E) dbt missing column failure

```json
{"scenario": "dbt_missing_column"}
```

Expected: `dbt_build` fails with a DuckDB binder error about missing `amount`.

### 2) Inspect PATCHIT output

#### Mock PRs

- `/.mock_github/prs/*.json` (metadata)
- `/.mock_github/prs/*.diff` (proposed diff)

#### Audit logs

- `var/audit/patchit_audit.jsonl`

Search for the latest correlation id:

```bash
tail -n 50 /Users/ankurchopra/repo_projects/PATCHIT-Auto-Remediation-Observability-Agent/var/audit/patchit_audit.jsonl
```

### 3) (Optional) Apply patch manually + rerun

Phase-1 is safety-first: **PATCHIT proposes**, humans apply.

- Open the `.diff` from the mock PR
- Apply changes in your editor
- Re-trigger the same scenario run in Airflow




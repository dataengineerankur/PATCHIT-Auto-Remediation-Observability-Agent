## Using PATCHIT with your own Airflow DAGs (local + prod-style)

### What PATCHIT needs

- **A UniversalFailureEvent** posted to PATCHIT (Phase-1 uses Airflow `on_failure_callback`).
- A **log URI PATCHIT can read** (in docker-compose this is `file://...` inside the PATCHIT container).
- (Optional) artifact URIs for upstream/downstream correlation and better root cause.

### Minimal integration pattern (copy/paste)

In your DAG file:

1) Import notifier:
- If your DAG is in the same Airflow environment as this repo, reuse:
  - `from task_lib.notify_patchit import notify_failure_to_patchit`

2) Add an `on_failure_callback`:

```python
def _airflow_log_path(*, dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
    return (
        f"/opt/airflow/logs/"
        f"dag_id={dag_id}/run_id={run_id}/task_id={task_id}/attempt={try_number}.log"
    )

def _on_failure_callback(context):
    ti = context["task_instance"]
    dag_id = context["dag"].dag_id
    run_id = context["dag_run"].run_id
    task_id = ti.task_id
    try_number = ti.try_number

    notify_failure_to_patchit(
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        try_number=try_number,
        log_path=_airflow_log_path(dag_id=dag_id, run_id=run_id, task_id=task_id, try_number=try_number),
        artifact_uris=[
            # Optional but recommended:
            # f"file:///opt/airflow/data/your_artifact.parquet"
        ],
        metadata={"exception": str(context.get("exception"))},
    )
```

3) Attach it via `default_args`:

```python
default_args = {
  "retries": 1,
  "retry_delay": timedelta(seconds=5),
  "on_failure_callback": _on_failure_callback,
}
```

### Docker-compose requirements

In `airflow/docker-compose.yml` (already done for this repo):
- **Airflow containers** export:
  - `PATCHIT_ENDPOINT=http://patchit:8088/events/ingest`
- **PATCHIT container** must be able to read:
  - the Airflow log files (we translate `/opt/airflow/logs` → `/opt/patchit/repo/airflow/logs`)
  - the DAG code paths for context collection

### “Verified means it actually runs” (runtime-faithful validation)

PATCHIT can always do **static verification** (apply diff, `compileall`, unit tests, dbt parse/build).
But for **orchestration changes** (patches touching `airflow/dags/**` or `airflow_extra_dags/**`), static checks are not enough:
you need **runtime fidelity** (Airflow semantics, Jinja templating, TriggerDagRun propagation, run_id/conf).

**Default behavior (new):** if a patch touches orchestration code and PATCHIT is not configured for runtime-faithful validation,
PATCHIT will emit `verify.finished summary=local_validation_required_missing` and **will not open a PR**.

To enable runtime-faithful validation, configure local worktree sync + a validation command:

- `PATCHIT_LOCAL_SYNC_ENABLED=true`
- `PATCHIT_LOCAL_VALIDATE_CMD="..."`

This makes PATCHIT apply the verified patch to a **host-visible git worktree** and run your command there
(for example: `docker compose ... airflow tasks test ...`). If the command fails, PATCHIT blocks PR creation.

### Where outputs show up

- **Event console**: `http://localhost:8088/ui`
- **Mock PR viewer**: `http://localhost:8088/mock/pr/<id>`
- **Mock PR files**: `/.mock_github/prs/*.json` and `*.diff`
- **Audit log**: `var/audit/patchit_audit.jsonl`

### LLM provider options (agent_mode)

PATCHIT can use multiple agent providers. Set via env (`PATCHIT_AGENT_MODE`) or switch at runtime in the UI.

- **OpenRouter**: `openrouter`
- **Groq**: `groq`
- **Cursor Cloud Agents**: `cursor`

#### Cursor Cloud Agents provider (`cursor`)

Cursor Cloud Agents run remotely against a **GitHub repo** and push changes to a branch. PATCHIT then:
- fetches the resulting diff via GitHub compare
- runs PATCHIT’s existing **sandbox verification** on that diff (verify-before-PR)
- creates a PR only after verification gates pass

Requirements:
- `PATCHIT_AGENT_MODE=cursor` (or UI runtime override)
- `PATCHIT_CURSOR_API_KEY` (Cursor API key)
- `PATCHIT_GITHUB_MODE=real`
- `PATCHIT_GITHUB_TOKEN` + `PATCHIT_GITHUB_REPO` (needed to fetch the diff via compare)

Optional:
- `PATCHIT_CURSOR_REPOSITORY`: explicit repo URL (`https://github.com/<owner>/<repo>`) if it differs from `PATCHIT_GITHUB_REPO`
- `PATCHIT_CURSOR_REF`: base ref for Cursor agent (defaults to `PATCHIT_GITHUB_BASE_BRANCH`)
- `PATCHIT_CURSOR_BRANCH_PREFIX`: default `patchit/cursor`

Notes:
- This provider sends failure context to Cursor’s cloud environment; evaluate privacy requirements before enabling.
- Cursor agents can optionally create PRs themselves, but PATCHIT defaults to creating PRs only after its own verification gates.



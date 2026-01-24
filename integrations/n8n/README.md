## n8n setup (local) — Airflow → n8n → PATCHIT (with Spark enrichment)

You have two Spark enrichment modes:
- **Mode A (future / external)**: Airflow (or any system) provides `spark_run_url` → n8n fetches logs via HTTP → sends enriched payload to PATCHIT.
- **Mode B (local today)**: Airflow provides `spark_local_cmd` (a docker `spark-submit` command) → n8n runs it → captures stdout/stderr → sends enriched payload to PATCHIT.

### 1) Import the workflow JSON
- Open n8n UI → **Workflows** → **Import from File**
- Select: `integrations/n8n/airflow_to_patchit_with_spark_enrichment.json`

### 2) Configure PATCHIT ingest URL (n8n Cloud vs self-hosted)

#### If you are using **n8n Cloud**
n8n Cloud blocks `$env.*` access inside expressions, so the workflow expects the PATCHIT ingest URL to be provided in the incoming payload as:
- `patchit_ingest_url`, or
- `metadata.patchit_ingest_url`

Example:
- `https://<YOUR-PATCHIT-PUBLIC-HOST>/events/ingest`

If PATCHIT is only running locally on your Mac, you must expose it via a tunnel for n8n Cloud:
- Option: Cloudflare Tunnel (recommended):
  - `cloudflared tunnel --url http://localhost:8088`
  - Use the HTTPS URL it prints as `metadata.patchit_ingest_url`
- Option: ngrok:
  - `ngrok http 8088`
  - Use the HTTPS URL it prints as `metadata.patchit_ingest_url`

#### If you are **self-hosting n8n**
You can hardcode the PATCHIT URL directly in the HTTP Request node, or pass it in payload the same way as above.

### 3) Configure the Airflow inbound webhook
In the workflow:
- Node: **Webhook (Airflow failure payload)**
- Path: `airflow-failure`
- Method: `POST`
- Click **Listen for test event**

Your webhook URL will look like:
- `http://<n8n-host>:5678/webhook/airflow-failure`

### 4) Spark enrichment (choose one)

#### Mode A: external Spark run URL (Databricks/EMR later)
- Provide `spark_run_url` in the incoming JSON body.
- Configure the node:
  - **Fetch Spark run output (HTTP)**
  - Set credential: **HTTP Header Auth** (if needed)

#### Mode B: local spark-submit executed by n8n (works now)
You must run n8n where it can execute Docker:
- n8n container must have the docker CLI installed, AND
- the docker socket must be mounted (`/var/run/docker.sock`), OR run n8n directly on your host.

Provide `spark_local_cmd` in the incoming JSON body (example below).

### 5) Activate workflow
Click **Active** (top-right) once you’ve tested successfully.

---

## Test payloads

### A) Test (external fetch)
POST to `/webhook/airflow-failure`:

```json
{
  "event_id": "airflow:test:1",
  "pipeline_id": "enterprise_spark_submit_dag",
  "run_id": "manual__test",
  "task_id": "spark_submit_job",
  "artifact_uris": [],
  "metadata": {
    "patchit_ingest_url": "https://<YOUR-PATCHIT-PUBLIC-HOST>/events/ingest",
    "spark_run_url": "http://example.local/spark/run/123/logs"
  }
}
```

### B) Test (local spark-submit run)
POST to `/webhook/airflow-failure`:

```json
{
  "event_id": "airflow:test:local_spark:1",
  "pipeline_id": "enterprise_spark_submit_dag",
  "run_id": "manual__test",
  "task_id": "spark_submit_job",
  "artifact_uris": [],
  "metadata": {
    "patchit_ingest_url": "https://<YOUR-PATCHIT-PUBLIC-HOST>/events/ingest",
    "spark_local_cmd": "docker run --rm apache/spark:3.5.1 bash -lc '/opt/spark/bin/spark-submit --version | head -n 5'"
  }
}
```

### Recommended “real” local command (matches your repos)
This is the same pattern we use for PATCHIT’s local Spark validation, but run by n8n:

```bash
docker run --rm \
  -v /Users/ankurchopra/repo_projects/patchit-enterprise-spark-jobs:/opt/airflow/ext/patchit-enterprise-spark-jobs \
  -v /Users/ankurchopra/repo_projects/patchit-enterprise-shared-libs:/opt/airflow/ext/patchit-enterprise-shared-libs \
  -v /tmp:/work/out \
  apache/spark:3.5.1 bash -c \
  'python3 -m pip install -q --no-cache-dir --target /tmp/pylibs pydantic==2.10.4 && \
   export PYTHONPATH="/tmp/pylibs:/opt/airflow/ext/patchit-enterprise-spark-jobs/src:/opt/airflow/ext/patchit-enterprise-shared-libs/src" && \
   /opt/spark/bin/spark-submit --master local[2] \
     /opt/airflow/ext/patchit-enterprise-spark-jobs/src/enterprise_spark/jobs/spark_submit_entrypoint.py \
     --scenario spark_missing_column \
     --out /work/out/n8n_spark_out.json'
```

Put the above as the value of `metadata.spark_local_cmd` (single line) in your webhook payload.



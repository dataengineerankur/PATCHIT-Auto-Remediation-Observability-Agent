# PATCHIT Multi-Repo Drill Plan (Airflow + dbt + Spark + SWE-bench)

You are a principal reliability engineer and test harness builder. This plan builds a **reproducible, multi-repo** drill program that stress-tests PATCHIT across **Airflow**, **Spark**, **dbt**, and **SWE-bench**. It emphasizes **producer-side failures**, safe fixes, and verified patches.

---

## 0) Quick Summary

- **Repos**:  
  - Airflow: `/Users/ankurchopra/repo_projects/2-10-example-dags`  
  - dbt (DuckDB): `/Users/ankurchopra/repo_projects/jaffle_shop_duckdb`  
  - Spark: `/Users/ankurchopra/repo_projects/pyspark-example-project`  
  - SWE-bench: `/Users/ankurchopra/repo_projects/SWE-bench`
- **Goal**: Create deterministic failures, capture evidence, and see PATCHIT produce safe PRs with verification.
- **Outcome**: Evidence packs + registry + scorecard + summary metrics.

---

## 1) Global Rules (Safety + Quality)

1. **Never commit to `main`**. Branch format:  
   `drill/<repo>/<scenario>/<timestamp>`
2. **No destructive changes**. No deletions, no disabling tests.
3. **Prefer safe guardrails**: validation, quarantine, idempotency, contract tests.
4. **Stop conditions**:  
   - Patch touches > 5 files  
   - Patch touches secrets/auth/credentials  
   - Patch deletes data or disables tests  
   → **Refuse** and generate report only.

---

## 2) Output Directory + Registry

Create an output root for drill evidence.

**OUTPUT_DIR**:  
`/Users/ankurchopra/repo_projects/patchit_drills`

Required subfolders:
```
/Users/ankurchopra/repo_projects/patchit_drills/
  airflow/
  dbt/
  spark/
  swebench/
  drills_registry.jsonl
```

**Registry format** (one JSON per line):
```
{"scenario_id":"A1_partial_write","repo":"airflow","status":"failed","branch":"drill/airflow/A1_partial_write/20260118-101500","patchit_outcome":"pr_opened","timestamp":"2026-01-18T10:15:00Z"}
```

---

## 3) Evidence Pack JSON (per drill)

Each drill writes a `evidence.json` to:
```
OUTPUT_DIR/<repo>/<scenario>/evidence.json
```

**Schema:**
```json
{
  "repo_name": "airflow",
  "scenario_id": "A1_partial_write",
  "injected_change_diff": "git diff --stat or unified diff",
  "failure_logs": [
    { "path": "logs.txt", "snippet": "stack trace snippet" }
  ],
  "suspected_root_cause": "earliest producer failure",
  "patchit_action": "pr_opened | report_only | refuse",
  "patch_summary": "atomic write + checksum validation",
  "files_changed": ["path/a.py", "path/b.py"],
  "patch_size": "+34/-6",
  "validation": {
    "before": "failed",
    "after": "passed",
    "command": "pytest -k drill"
  }
}
```

---

## 4) Combined Multi-Repo Drill Tracks (New + Complex)

These are **composed drills** that intentionally cross repositories. Use these to push PATCHIT’s repo selection, RCA, and fix safety.

### Track T1 — Airflow → Spark → dbt Chain (Producer-first failure)
**Goal:** Force a failure in the producer (Airflow task) that surfaces later in Spark and dbt.

- **Airflow**: `t2_write_raw` writes malformed JSON to a shared artifact path.
- **Spark**: Drill reads JSON and fails fast with schema validation.
- **dbt**: Models depend on Spark output, fail with missing columns or null spikes.

**Success criteria:**  
PATCHIT identifies producer failure in Airflow, and fixes **only** producer logic (atomic write or schema validation), not Spark/dbt.

---

### Track T2 — Shared Library Blast Radius
**Goal:** Break a shared helper in Airflow and Spark, trigger multiple failures.

- **Airflow**: `include/drill_lib/common.py` used by multiple DAGs.
- **Spark**: shared utility function used across jobs.

**Success criteria:**  
PATCHIT suggests **one fix** in shared lib, not multiple repo edits.

---

### Track T3 — Schema Drift Propagation
**Goal:** Introduce a schema drift in Spark output that breaks dbt downstream.

- **Spark**: output adds a new field + type change.
- **dbt**: model references missing/changed type columns.

**Success criteria:**  
PATCHIT adds safe handling in dbt (fallback/cast), and suggests **schema contract** update.

---

### Track T4 — Idempotency + Duplicate Effects
**Goal:** Rerun a pipeline and observe duplicated outputs.

- **Airflow**: no run_id isolation in output path.
- **Spark**: appends duplicates.
- **dbt**: counts double.

**Success criteria:**  
PATCHIT fixes idempotency at the **earliest producer stage**.

---

## 5) Airflow Drills (6 Scenarios)

Create DAGs in:  
`/Users/ankurchopra/repo_projects/2-10-example-dags/dags/patchit_drill_<scenario>.py`

Each DAG has 5 tasks:  
t1_fetch_api → t2_write_raw → t3_parse → t4_validate → t5_load

### A1_partial_write
Producer writes JSON in chunks, parser fails with `JSONDecodeError`.

**Expected fix:** atomic write (temp + fsync + rename) + checksum/JSON validation.

### A2_wrong_shape_200_ok
API returns error payload with HTTP 200. Parser expects list of records.

**Expected fix:** detect error payload + quarantine; add contract check.

### A3_multi_producer_collision
Two tasks write to the same output path.

**Expected fix:** unique output path + lock/idempotency guard.

### A4_silent_corruption
Parser drops 50% rows due to wrong join key.

**Expected behavior:** propose canary tests (row count delta + uniqueness), not logic rewrites.

### A5_wrong_partition
Producer writes `dt=today-1` instead of `dt=today`.

**Expected fix:** partition derived from DAG run context + assert partition exists.

### A6_shared_lib_break
Shared helper breaks multiple DAGs.

**Expected behavior:** single shared lib fix, not per DAG.

---

## 6) dbt Drills (6 Scenarios)

Use repo:  
`/Users/ankurchopra/repo_projects/jaffle_shop_duckdb`

Models: `models/drills/<scenario>/`  
Tests: `tests/drills/<scenario>/`

### D1_missing_column
Reference a removed column.

**Expected fix:** safe fallback + add dbt test.

### D2_type_change
String-to-int cast mismatch.

**Expected fix:** explicit casting policy + type tests.

### D3_null_spike
Introduce null spike.

**Expected behavior:** add `not_null` tests + anomaly thresholds.

### D4_new_enum_value
Add unknown enum value.

**Expected fix:** safe `OTHER` bucket + update accepted_values test.

### D5_contract_break
Break schema contract.

**Expected fix:** versioned contract update + rollback note.

### D6_silent_bad_join
Row count shifts without failure.

**Expected behavior:** canary tests (row count delta + uniqueness).

---

## 7) Spark Drills (6 Scenarios)

Repo:  
`/Users/ankurchopra/repo_projects/pyspark-example-project`

Jobs: `jobs/drills/<scenario>/`  
Data: `data/drills/<scenario>/`

### S1_schema_drift_parquet
Parquet adds or changes column types.

**Expected fix:** explicit schema + safe cast + validation.

### S2_partial_write
Write output without atomic commit.

**Expected fix:** temp write + rename + success marker.

### S3_bad_json_payload
JSON includes error objects.

**Expected fix:** detect errors + quarantine.

### S4_duplicate_events
Duplicates cause double counts.

**Expected fix:** deterministic dedup + unit test.

### S5_skew_performance
Heavy key skew causes timeouts.

**Expected behavior:** suggest safe salting strategy, no random cluster tuning.

### S6_idempotency_break
Rerun appends duplicate outputs.

**Expected fix:** overwrite/merge strategy + run_id isolation.

---

## 8) SWE-bench Drills (Mini Benchmark)

Goal: Use SWE-bench as a patch correctness benchmark.

1. Install per SWE-bench README.  
2. Pick 10 verified instances.  
3. For each instance:
   - materialize repo
   - run tests, capture logs
   - feed issue + logs into PATCHIT
   - re-run tests, record pass/fail
4. Scorecard at:
   `OUTPUT_DIR/swebench/scorecard.csv`

**CSV format:**
```
instance_id,repo,patchit_pass,tests_before,tests_after,files_changed,patch_size
```

---

## 9) How to Test Now (Step-by-Step)

### Step 1 — Create Output Directory
```
mkdir -p /Users/ankurchopra/repo_projects/patchit_drills/{airflow,dbt,spark,swebench}
touch /Users/ankurchopra/repo_projects/patchit_drills/drills_registry.jsonl
```

### Step 2 — Run One Airflow Drill (Example: A1_partial_write)
```
cd /Users/ankurchopra/repo_projects/2-10-example-dags
git checkout -b drill/airflow/A1_partial_write/$(date +%Y%m%d-%H%M%S)

# Add DAG file: dags/patchit_drill_A1_partial_write.py
# Trigger DAG or run import checks
pytest -k patchit_drill_A1_partial_write || true

# Capture logs
cp /tmp/airflow_failure.log /Users/ankurchopra/repo_projects/patchit_drills/airflow/A1_partial_write/logs.txt
```

### Step 3 — Invoke PATCHIT
```
PATCHIT_CLI \
  --repo /Users/ankurchopra/repo_projects/2-10-example-dags \
  --scenario A1_partial_write \
  --logs /Users/ankurchopra/repo_projects/patchit_drills/airflow/A1_partial_write/logs.txt \
  --mode pr_only
```

### Step 4 — Validate After Patch
```
pytest -k patchit_drill_A1_partial_write
```

### Step 5 — Write Evidence Pack
```
cat > /Users/ankurchopra/repo_projects/patchit_drills/airflow/A1_partial_write/evidence.json << 'EOF'
{ "repo_name": "airflow", "scenario_id": "A1_partial_write", "injected_change_diff": "", "failure_logs": [{ "path": "logs.txt", "snippet": "" }], "suspected_root_cause": "", "patchit_action": "pr_opened", "patch_summary": "", "files_changed": [], "patch_size": "", "validation": { "before": "failed", "after": "passed", "command": "pytest -k patchit_drill_A1_partial_write" } }
EOF
```

### Step 6 — Append Registry Line
```
echo "{\"scenario_id\":\"A1_partial_write\",\"repo\":\"airflow\",\"status\":\"completed\",\"branch\":\"$(git rev-parse --abbrev-ref HEAD)\",\"patchit_outcome\":\"pr_opened\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" \
  >> /Users/ankurchopra/repo_projects/patchit_drills/drills_registry.jsonl
```

---

## 10) Recommended Initial Drill Set (12 to start fast)

- **Airflow**: A1_partial_write, A2_wrong_shape_200_ok, A3_multi_producer_collision  
- **dbt**: D1_missing_column, D2_type_change, D4_new_enum_value  
- **Spark**: S1_schema_drift_parquet, S2_partial_write, S6_idempotency_break  
- **SWE-bench**: 3 verified instances

---

## 11) Done Criteria

- 24+ drills executed  
- Evidence packs for all  
- Summary metrics report: pass rate, refusal rate, false-fix rate, average patch size  
- At least 3 composed multi-repo tracks completed

---

## 12) Next Enhancements (Optional)

- Add a `drill_runner.py` to automate evidence pack creation  
- Add `pre-commit` checks to enforce stop conditions  
- Add local dashboard to visualize drill outcomes over time  


# Closed-Loop Self-Healing Design (Task-Agnostic, Pipeline-Agnostic)

This document extends the Phase-1 PATCHIT design to eliminate human-in-the-loop trial-and-error. The system must **own the full debug → patch → validate → iterate → stop** loop, and only produce PRs when it has **high-confidence evidence** that the change truly resolves the failure in a runtime-faithful sandbox.

## Goals

- **No manual merge/pull/retry loop**: humans can inspect changes locally *before* PR creation; PRs are created only after verification.
- **High sandbox fidelity**: verification must mirror runtime (Airflow task runner / docker-compose / CI) rather than only “python compile”.
- **Iteration memory**: persist all attempts, reasons for failure, and detect equivalent/repeated patches across runs.
- **Intelligent stop condition**: stop patching when it’s likely upstream/infra/data or repeated safe attempts fail.
- **Audit-ready Root Cause Report**: when auto-fix is not possible, produce a structured, human-readable report.

---

## Architecture: Agents + Execution Harness

### Core loop (closed-loop)

1. **Failure intake**: ingest `UniversalFailureEvent`.
2. **Evidence gathering**: logs + artifact summaries + code context + codebase index.
3. **Patch attempt generation**: LLM proposes a candidate change.
4. **Pre-checks**:
   - Safety policy (deny/allow)
   - Attempt memory (reject repeated diffs / repeated strategies)
5. **Runtime-faithful verification** (non-negotiable):
   - Apply patch into isolated sandbox workspace
   - Re-run the exact failed task in a harness that matches runtime
   - Only proceed if the failure is resolved
6. **Local sync**:
   - Apply the verified patch into a **local worktree branch** so the user can run it locally without merge/pull
7. **PR creation**:
   - Open PR only after verification passes (and optionally after local validation passes)
8. **Outcome tracking**:
   - Record whether the PR led to green runs; if not, store regression/no-effect outcome
9. **Stop / escalate**:
   - After repeated safe failures or upstream/infra/data classification, output RootCauseReport (no PR).

### “Execution Harness” abstraction (key to fidelity)

Verification must be performed by a pluggable harness that matches runtime. Examples:

- **AirflowHarness** (docker-compose):
  - `docker compose exec airflow-scheduler airflow tasks test ...`
  - or use Airflow API to run a single task instance
- **DbtHarness**:
  - `dbt build` / `dbt test` with correct profiles and warehouse
- **PythonHarness** (library tasks):
  - direct invocation for pure-python modules
- **CIHarness**:
  - run the same command set used in CI pipelines

The harness chosen is inferred from:
- platform (airflow/dbt/other)
- changed files (e.g., `airflow/dags/` ⇒ AirflowHarness)
- presence of docker-compose in repo

---

## Strong Sandbox Fidelity (How to actually do it)

### Sandbox types

1. **File sandbox (fast)**: copy repo to temp directory, apply diff, run lightweight checks.
2. **Runtime sandbox (authoritative)**: run the real runtime (docker-compose / k8s job / Airflow task runner) against the patched workspace.

To prevent “agent regressions” (like the `ensure_dirs(paths.out_dir)` bug), Airflow changes must pass **Airflow parsing + task execution** checks in the runtime sandbox.

### Recommended implementation for local dev stacks

- Use `git worktree` to create an isolated workspace:
  - `git worktree add /tmp/patchit_ws_<id> -b patchit/<id> <base>`
- Bind-mount that worktree into docker-compose services for Airflow/worker
- Run:
  - `airflow dags list` (parsing check)
  - `airflow tasks test <dag_id> <task_id> <logical_date>` (repro the exact failure)

This allows “true runtime fidelity” without requiring the user to merge anything.

---

## Local + Sandbox Sync (No manual merge/pull)

### Workflow

- After sandbox verification passes, PATCHIT:
  - creates a local branch (or worktree) like `patchit/local/<patch_id>`
  - applies the verified diff
  - prints the exact local test command(s) to run
- Only after optional local confirmation (configurable), PATCHIT creates the PR.

### Why `git worktree` is the right primitive

- isolates changes without touching user’s current branch
- allows multiple concurrent candidate fixes
- trivial reset: remove worktree directory
- supports “compare” across attempts

---

## Iteration Awareness & Memory (Persistence across runs)

### What must be stored

For each attempt:
- error fingerprint
- patch diff hash (to detect repeats)
- verification outcome + logs
- PR link + merge status (optional)
- categorized outcome: `fixed | no_effect | regression | upstream_issue | unknown`

### How to avoid repeating equivalent approaches

Use “equivalence buckets”:
- diff hash (exact repeat)
- semantic strategy tag (LLM-provided): e.g., `add_retry`, `add_guard`, `schema_tolerant`, `path_fix`
- touched files + error fingerprint

If the same strategy bucket fails multiple times, escalate earlier.

---

## Intelligent Stop Condition

Stop auto-fixing when any of these is true:

- **max attempts** reached for a fingerprint
- repeated diff/strategy detected
- verification cannot be run with sufficient fidelity (env mismatch)
- evidence indicates upstream dependency/infra/data:
  - upstream API 5xx, auth failures, missing secrets
  - missing upstream artifact due to upstream task crash
  - external DB down / network errors

When stopping, emit **RootCauseReport** and do **not** open a PR.

---

## Standard Root Cause Report (Audit-ready)

Required fields:

- **Failure summary**: pipeline, run, task, fingerprint, normalized error
- **Timeline of attempts**: patch ids, files touched, verify outputs, PR links, outcome
- **Why each attempt failed**: extracted from verify logs + harness results
- **Final classification**:
  - `code_bug | env_mismatch | dependency_issue | infra_issue | data_issue | unknown`
- **Next actions**:
  - concrete runbooks / commands / logs to inspect
  - whether human intervention is required

---

## What’s implemented now (Phase-1 extension)

- Persistent attempt store (SQLite): `var/memory/attempts.sqlite3`
- Error fingerprinting + repeated-diff rejection
- Stop condition: max attempts per fingerprint triggers escalation event (audit) rather than PR

Next steps to reach full closed-loop:
- Runtime harness (Airflow task-runner inside docker-compose)
- Local worktree sync
- Post-PR outcome tracker (re-run and mark fixed/no-effect/regression)







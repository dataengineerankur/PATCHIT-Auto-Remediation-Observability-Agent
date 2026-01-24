from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PATCHIT_", extra="ignore")

    github_mode: str = "mock"  # mock|real
    github_token: str | None = None
    github_repo: str | None = None
    github_base_branch: str = "main"

    audit_log_path: str = "var/audit/patchit_audit.jsonl"
    mock_github_dir: str = ".mock_github"
    public_base_url: str = "http://localhost:8088"
    patch_store_dir: str = "var/patches"
    report_store_dir: str = "var/reports"
    evidence_store_dir: str = "var/evidence"
    drill_store_dir: str = "var/drills"

    # Agentic fixer (Bytez)
    agent_mode: str = "openrouter"  # openrouter|groq|cursor|bytez|off
    agent_url: str | None = None  # used for bytez agent service URL

    # Groq OpenAI-compatible API
    groq_api_key: str | None = None
    groq_base_url: str = "https://api.groq.com/openai/v1"
    groq_model: str = "openai/gpt-oss-120b"

    # OpenRouter OpenAI-compatible API
    openrouter_api_key: str | None = None
    openrouter_base_url: str = "https://openrouter.ai/api/v1"
    openrouter_model: str = "openai/gpt-5.2"
    # Optional headers for OpenRouter rankings
    openrouter_site_url: str | None = None
    openrouter_site_name: str | None = None
    openrouter_max_tokens: int = 2048

    # If true and OpenRouter fails (quota/credits), fall back to Groq (still agentic).
    agent_fallback_to_groq: bool = True
    groq_max_tokens: int = 2048

    # Cursor Cloud Agents (optional)
    # Requires a GitHub-backed repo (PATCHIT github_mode=real) because PATCHIT fetches the agent's diff via GitHub compare.
    cursor_api_key: str | None = None
    cursor_base_url: str = "https://api.cursor.com"
    # If not set, PATCHIT will derive this from github_repo (https://github.com/<owner>/<repo>).
    cursor_repository: str | None = None
    # If not set, defaults to github_base_branch.
    cursor_ref: str | None = None
    cursor_branch_prefix: str = "patchit/cursor"
    cursor_poll_interval_s: float = 3.0
    cursor_max_wait_s: float = 600.0
    # Keep false: PATCHIT will run its own verification gates and then create the PR.
    cursor_auto_create_pr: bool = False

    # Inhouse agent (external) - user-provided API/service
    inhouse_agent_url: str | None = None
    inhouse_api_key: str | None = None
    inhouse_headers_json: str | None = None
    inhouse_timeout_s: float = 60.0

    # Path translation for stack frames so PATCHIT can collect context when running in containers.
    # Example (airflow docker-compose):
    #   from: /opt/airflow/dags
    #   to:   /opt/patchit/repo/airflow/dags
    code_translate_from: str | None = None
    code_translate_to: str | None = None

    # Path translation for file:// URIs (logs/artifacts) when Airflow and PATCHIT run in different containers.
    # Example:
    #   from: /opt/airflow/logs
    #   to:   /opt/patchit/repo/airflow/logs
    file_translate_from: str | None = None
    file_translate_to: str | None = None

    # Repo root (mounted in docker-compose at /opt/patchit/repo)
    repo_root: str = "repo"

    # -------- Multi-repo support --------
    # JSON string describing multiple target repos PATCHIT can remediate.
    # This enables one PATCHIT instance to demo/operate across multiple Airflow repos.
    #
    # Format (v1):
    # [
    #   {
    #     "key": "retail",
    #     "pipeline_id_regex": "^validation_.*|^retail_.*",
    #     "repo_root": "repos/retail",            # path inside container or absolute
    #     "github_repo": "org/repo",              # owner/name (for real PRs and Cursor compare)
    #     "github_base_branch": "main",
    #     "cursor_repository": "https://github.com/org/repo",
    #     "code_translate_from": "/opt/airflow/dags",
    #     "code_translate_to": "/opt/patchit/repos/retail/airflow/dags"
    #   }
    # ]
    repo_registry_json: str | None = None

    # -------- Integrations (via n8n or direct) --------
    # If set, PATCHIT will POST integration events (JSON) to each URL (fan-out).
    # Example:
    #   PATCHIT_INTEGRATION_WEBHOOK_URLS_JSON='["https://n8n.example/webhook/patchit","https://..."]'
    integration_webhook_urls_json: str | None = None
    # Optional: restrict which audit event_types are emitted to webhooks (defaults to terminal events + pr.created).
    integration_emit_event_types_json: str | None = None

    # Patch verification (sandbox) - PATCHIT will only create PRs for patches that pass verification.
    verify_enabled: bool = True
    verify_max_attempts: int = 8
    verify_timeout_s: float = 180.0
    verify_replay_enabled: bool = True
    converge_to_green: bool = False
    # If false, PATCHIT will NOT use deterministic fallback patching when agentic mode is enabled.
    # Recommended for production-like behavior: require agent-generated patches only.
    allow_deterministic_fallback: bool = False

    # Airflow (optional poller usage)
    airflow_base_url: str | None = None  # e.g. http://localhost:8080
    airflow_username: str | None = None
    airflow_password: str | None = None
    airflow_poller_enabled: bool = False
    airflow_poller_interval_s: float = 8.0
    airflow_poller_lookback_minutes: int = 180  # initial scan window (dedup prevents reprocessing)
    airflow_poller_max_dags: int = 200
    # HTTP timeout for Airflow API calls made by the poller (seconds).
    airflow_poller_timeout_s: float = 20.0
    # Safety valve: cap how many new events the poller will submit per cycle (prevents “reset storms”).
    airflow_poller_max_new_events_per_cycle: int = 5
    # Timeout for the poller's POST to the PATCHIT ingest endpoint (seconds).
    airflow_poller_ingest_timeout_s: float = 15.0
    airflow_logs_base: str = "/opt/airflow/logs"
    airflow_data_base: str = "/opt/airflow/data"

    # Runtime overrides (mutable, persisted under var/ so UI toggles survive container restarts)
    runtime_config_path: str = "var/config/runtime.json"

    # Persistent attempt memory (SQLite). Mount `var/` to persist across restarts.
    memory_db_path: str = "var/memory/attempts.sqlite3"

    # Codebase intelligence depth (used for LLM grounding).
    # Increase when you want the agent to reason across many DAGs/libs.
    index_max_files: int = 1200

    # Agent call guardrails:
    # - heartbeat keeps UI lively even when LLM is slow
    # - attempt_timeout prevents a single hung network call from stalling remediation forever
    agent_heartbeat_interval_s: float = 10.0
    agent_attempt_timeout_s: float = 90.0
    # Hard cap on agent attempts per single ingest (keeps UI snappy, prevents “attempt 6+” spirals).
    agent_max_attempts_per_event: int = 3
    # Patch generation strategy:
    # - structured: one LLM call returns JSON with full new file content; PATCHIT synthesizes a valid diff (fast + reliable)
    # - diff: LLM returns unified diff directly (more brittle)
    agent_patch_format: str = "structured"

    # Stop conditions (agent should stop + report rather than spam PRs)
    max_total_attempts_per_fingerprint: int = 6
    max_repeat_patch_attempts_per_fingerprint: int = 1
    # If a PR was already created recently for the same error fingerprint, do not open another one.
    # This is a first-pass clustering guard to prevent PR spam during bursts.
    fingerprint_pr_cooldown_s: float = 6 * 3600.0

    # Runtime-faithful verification (optional but recommended):
    # If set, runs this command inside the sandbox AFTER patch apply + basic checks.
    # Supported placeholders:
    #   {sandbox} -> sandbox temp directory
    #   {repo}    -> sandbox repo directory (usually {sandbox}/repo)
    # Example:
    #   PATCHIT_RUNTIME_VERIFY_CMD="bash -lc 'cd {repo} && python -m compileall -q airflow'"
    runtime_verify_cmd: str | None = None
    runtime_verify_required: bool = False

    # If true, require runtime verification for orchestration changes (Airflow DAGs / triggers).
    # Prevents "diff applies + compileall" from being treated as sufficient for Airflow semantics fixes.
    runtime_verify_required_for_orchestration_changes: bool = True

    # If true, emit per-command verifier events (verify.cmd_started/verify.cmd_finished) for richer UI.
    verifier_emit_command_events: bool = True

    # Verification harness extensions (dbt is built-in; Spark/API are best-effort and config-driven).
    # JSON rules to add extra verification commands when certain failures happen.
    #
    # Format (v1):
    # [
    #   {
    #     "name": "spark_smoke",
    #     "when": {"touched_path_regex": "(?i)spark|pyspark|\\.scala$"},
    #     "cmds": ["pytest -q repo/tests/spark"]
    #   },
    #   {
    #     "name": "api_smoke",
    #     "when": {"error_regex": "(?i)HTTPError|requests\\.|timeout"},
    #     "cmds": ["pytest -q repo/tests/integration"]
    #   }
    # ]
    verify_cmd_rules_json: str | None = None

    # Extra python paths for sandbox verification (JSON list of strings).
    # Useful for multi-repo setups where repo-under-test imports shared libraries from another mounted repo.
    # Example:
    #   PATCHIT_VERIFY_EXTRA_PYTHONPATHS_JSON='["/opt/patchit/repos/shared-libs/src"]'
    verify_extra_pythonpaths_json: str | None = None

    # Local sync (optional): apply verified patches to a local git worktree for immediate inspection/testing.
    local_sync_enabled: bool = False
    # Absolute path to the repo to create worktrees from (defaults to CWD when unset).
    local_sync_repo_path: str | None = None
    # Where to create worktrees (relative or absolute). Default keeps them in var/ so docker volumes persist.
    local_sync_worktree_root: str = "var/worktrees"
    # Optional: run an additional command in the local worktree before creating PR (stronger guarantee).
    # Same placeholders as runtime_verify_cmd, but {repo} points to the worktree path.
    local_validate_cmd: str | None = None
    # Hard cap for Option A runtime validation (prevents hangs leaving patchit_val_* stacks running forever).
    local_validate_timeout_s: float = 420.0

    # If true, require local worktree validation for orchestration changes (Airflow DAG edits).
    # This is the only practical way to run a true Airflow runtime check, because a temp sandbox
    # directory inside the PATCHIT container is not visible to the host Docker daemon.
    local_validate_required_for_orchestration_changes: bool = True

    # Spark runtime validation (Approach 1):
    # If set, PATCHIT will run this command in the local worktree for Spark repo patches (even when not DAG/orchestration).
    # This should run a real `spark-submit` (typically via docker) against the patched worktree.
    spark_local_validate_cmd: str | None = None
    spark_local_validate_timeout_s: float = 900.0



from __future__ import annotations

import concurrent.futures
import hashlib
import json
from datetime import datetime, timezone
import os
import re
import threading
import time
import uuid
from typing import Any, Dict

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from starlette.responses import StreamingResponse

from patchit.classifier.rules import RuleBasedClassifier
from patchit.code_engine.engine import CodeEngine
from patchit.code_engine.agentic import AgenticCodeEngine
from patchit.adapters.airflow_adapter import AirflowAdapter
from patchit.context.artifacts import summarize_artifacts
from patchit.context.collector import CodeContextCollector
from patchit.models import (
    AttemptSummary,
    NormalizedErrorObject,
    PatchProposal,
    RemediationPolicy,
    RemediationResult,
    RootCauseCategory,
    RootCauseReport,
    UniversalFailureEvent,
)
from patchit.parsers.log_fetcher import LogFetcher
from patchit.parsers.airflow_log_parser import extract_error_excerpt
from patchit.parsers.stacktrace import parse_first_exception, parse_python_traceback
from patchit.policy.engine import PolicyEngine
from patchit.policy.safety import detect_prompt_injection
from patchit.settings import Settings
from patchit.telemetry.audit import AuditLogger
from patchit.telemetry.evidence import build_evidence_payload, persist_evidence_pack
from patchit.prompting.pipeline import PromptPipelineConfig, parse_typed_outputs, run_prompt_pipeline
from patchit.gitops.pr_creator import PullRequestCreator
from patchit.local_sync.worktree import apply_patch_to_worktree
from patchit.service.ui import render_ui_html, stream_jsonl_sse, tail_jsonl
from patchit.service.mock_pr_view import load_mock_pr, render_mock_pr_html
from patchit.indexer.indexer import build_codebase_index
from patchit.verify.sandbox import SandboxVerifier
from patchit.memory.store import RemediationMemoryStore
from patchit.artifacts.contracts import infer_missing_artifact_context
from patchit.critic.patch_critic import should_reject_patch
from patchit.reports.render import render_root_cause_report_md
from patchit.reports.agent import ReportAgent
from patchit.poller.airflow_poller import AirflowFailurePoller, AirflowPollerConfig
from patchit.repos.registry import RepoContext, select_repo_context
from patchit.integrations.emitter import IntegrationEmitter


def _persist_patch_diff(*, patch_id: str, diff_text: str, store_dir: str) -> str:
    os.makedirs(store_dir, exist_ok=True)
    path = os.path.join(store_dir, f"{patch_id}.diff")
    with open(path, "w", encoding="utf-8") as f:
        f.write(diff_text or "")
        if diff_text and not diff_text.endswith("\n"):
            f.write("\n")
    return path


def _persist_report_md(*, report_id: str, md_text: str, store_dir: str) -> str:
    os.makedirs(store_dir, exist_ok=True)
    path = os.path.join(store_dir, f"{report_id}.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(md_text or "")
        if md_text and not md_text.endswith("\n"):
            f.write("\n")
    return path


def _persist_report_text(*, report_id: str, text: str, store_dir: str, ext: str = "log") -> str:
    os.makedirs(store_dir, exist_ok=True)
    path = os.path.join(store_dir, f"{report_id}.{ext}")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text or "")
        if text and not text.endswith("\n"):
            f.write("\n")
    return path


def create_app(settings: Settings | None = None) -> FastAPI:
    """
    App factory used by uvicorn and tests.

    Important: routes are registered on the module-level `app` instance via decorators.
    Tests call `create_app(custom_settings)` and expect the returned app to include routes.
    """
    s = settings or Settings()
    existing = globals().get("app")
    if isinstance(existing, FastAPI):
        existing.state.settings = s
        # Keep emitter in sync with settings if app already exists.
        if not hasattr(existing.state, "integration_emitter"):
            em = IntegrationEmitter(
                webhook_urls_json=getattr(s, "integration_webhook_urls_json", None),
                emit_event_types_json=getattr(s, "integration_emit_event_types_json", None),
            )
            em.start()
            existing.state.integration_emitter = em
        return existing

    new_app = FastAPI(title="PATCHIT", version="0.1.0")
    new_app.state.settings = s
    emit_types = getattr(s, "integration_emit_event_types_json", None)
    if not emit_types:
        # Default: only high-signal / terminal events.
        emit_types = json.dumps(
            ["pr.created", "policy.decided", "report.saved", "evidence.saved", "remediation.escalated", "agent.failed"]
        )
    em = IntegrationEmitter(webhook_urls_json=getattr(s, "integration_webhook_urls_json", None), emit_event_types_json=emit_types)
    em.start()
    new_app.state.integration_emitter = em
    # Ensure persistence dirs exist (audit + patches + memory)
    try:
        os.makedirs(os.path.dirname(s.audit_log_path) or ".", exist_ok=True)
    except Exception:
        pass
    try:
        os.makedirs(s.patch_store_dir, exist_ok=True)
    except Exception:
        pass
    try:
        os.makedirs(s.report_store_dir, exist_ok=True)
    except Exception:
        pass
    try:
        os.makedirs(os.path.dirname(s.memory_db_path) or ".", exist_ok=True)
    except Exception:
        pass
    try:
        os.makedirs(os.path.dirname(s.runtime_config_path) or ".", exist_ok=True)
    except Exception:
        pass
    return new_app


app = create_app()

# ---------- Runtime config (mutable overrides) ----------
_runtime_lock = threading.Lock()
_runtime_cache: dict[str, Any] | None = None


def _sanitize_runtime_config(cfg: dict[str, Any]) -> dict[str, Any]:
    """
    Never echo secrets back to the browser UI.
    We still allow storing them (for local demo convenience), but they should not be retrievable via API.
    """
    c = dict(cfg or {})
    for k in (
        "airflow_password",
        "inhouse_api_key",
        "inhouse_headers_json",
        "github_token",
        "cursor_api_key",
        "openrouter_api_key",
        "groq_api_key",
        "databricks_token",
    ):
        if k in c:
            c[k] = "***"
    return c


def _load_runtime_config(settings: Settings) -> dict[str, Any]:
    global _runtime_cache
    with _runtime_lock:
        if _runtime_cache is not None:
            return dict(_runtime_cache)
        try:
            if os.path.exists(settings.runtime_config_path):
                with open(settings.runtime_config_path, "r", encoding="utf-8") as f:
                    _runtime_cache = json.load(f) or {}
            else:
                _runtime_cache = {}
        except Exception:
            _runtime_cache = {}
        return dict(_runtime_cache)


def _save_runtime_config(settings: Settings, cfg: dict[str, Any]) -> None:
    global _runtime_cache
    with _runtime_lock:
        _runtime_cache = dict(cfg or {})
        try:
            os.makedirs(os.path.dirname(settings.runtime_config_path) or ".", exist_ok=True)
            with open(settings.runtime_config_path, "w", encoding="utf-8") as f:
                json.dump(_runtime_cache, f)
        except Exception:
            pass


def _effective_agent_mode(settings: Settings) -> str:
    cfg = _load_runtime_config(settings)
    mode = cfg.get("agent_mode")
    if isinstance(mode, str) and mode in ("openrouter", "groq", "cursor", "bytez", "inhouse", "off"):
        # If runtime override points to a provider without credentials, fall back to env default.
        # This avoids “stuck in groq mode” when GROQ_API_KEY is removed but runtime_config persists.
        if mode == "groq" and not settings.groq_api_key:
            return settings.agent_mode
        if mode == "openrouter" and not settings.openrouter_api_key:
            return settings.agent_mode
        if mode == "cursor" and (not getattr(settings, "cursor_api_key", None) or settings.github_mode != "real" or not settings.github_token or not settings.github_repo):
            return settings.agent_mode
        if mode == "inhouse" and not (cfg.get("inhouse_agent_url") or getattr(settings, "inhouse_agent_url", None)):
            return settings.agent_mode
        return mode
    return settings.agent_mode


def _explain_mode_override(settings: Settings) -> dict[str, Any] | None:
    """
    Best-effort explanation when the persisted runtime_config requests a mode that cannot be honored.
    Used for UI warnings and per-incident audit events.
    """
    cfg = _load_runtime_config(settings)
    requested = cfg.get("agent_mode")
    if not isinstance(requested, str) or requested not in ("openrouter", "groq", "cursor", "bytez", "inhouse", "off"):
        return None
    effective = _effective_agent_mode(settings)
    if requested == effective:
        return None

    reasons: list[str] = []
    if requested == "cursor":
        if not getattr(settings, "cursor_api_key", None):
            reasons.append("cursor_api_key_missing")
        if settings.github_mode != "real":
            reasons.append("github_mode_not_real")
        if not getattr(settings, "github_token", None):
            reasons.append("github_token_missing")
        if not getattr(settings, "github_repo", None):
            reasons.append("github_repo_missing")
    if requested == "groq" and not getattr(settings, "groq_api_key", None):
        reasons.append("groq_api_key_missing")
    if requested == "openrouter" and not getattr(settings, "openrouter_api_key", None):
        reasons.append("openrouter_api_key_missing")
    if requested == "inhouse" and not (cfg.get("inhouse_agent_url") or getattr(settings, "inhouse_agent_url", None)):
        reasons.append("inhouse_agent_url_missing")

    if not reasons:
        reasons = ["provider_unavailable"]
    return {"requested": requested, "effective": effective, "reasons": reasons}


def _effective_report_agent_mode(settings: Settings) -> str:
    """
    ReportAgent is LLM-backed but does NOT support Cursor Cloud Agents.
    Even if patching uses Cursor, report generation should fall back to OpenRouter/Groq.
    """
    if getattr(settings, "openrouter_api_key", None):
        return "openrouter"
    if getattr(settings, "groq_api_key", None):
        return "groq"
    # No credentials: keep deterministic report rendering (will raise if called).
    return "openrouter"


# ---------- Poller startup ----------
_poller: AirflowFailurePoller | None = None
_poller_lock = threading.Lock()


@app.on_event("startup")
def _startup() -> None:
    global _poller
    s: Settings = app.state.settings
    runtime_cfg = _load_runtime_config(s)
    poller_enabled = bool(runtime_cfg.get("airflow_poller_enabled", getattr(s, "airflow_poller_enabled", False)))
    airflow_base_url = runtime_cfg.get("airflow_base_url") or getattr(s, "airflow_base_url", None)
    airflow_username = runtime_cfg.get("airflow_username") or getattr(s, "airflow_username", None)
    airflow_password = runtime_cfg.get("airflow_password") or getattr(s, "airflow_password", None)
    # Start Airflow poller if configured (keeps DAGs fully PATCHIT-agnostic).
    if (
        poller_enabled
        and airflow_base_url
        and airflow_username
        and airflow_password
    ):
        cfg = AirflowPollerConfig(
            base_url=airflow_base_url,
            username=airflow_username,
            password=airflow_password,
            timeout_s=float(getattr(s, "airflow_poller_timeout_s", 20.0)),
            interval_s=float(getattr(s, "airflow_poller_interval_s", 8.0)),
            lookback_minutes=int(getattr(s, "airflow_poller_lookback_minutes", 180)),
            max_dags=int(getattr(s, "airflow_poller_max_dags", 200)),
            max_new_events_per_cycle=int(getattr(s, "airflow_poller_max_new_events_per_cycle", 5)),
            logs_base=str(getattr(s, "airflow_logs_base", "/opt/airflow/logs")),
            data_base=str(getattr(s, "airflow_data_base", "/opt/airflow/data")),
            runtime_config_path=s.runtime_config_path,
            audit_log_path=s.audit_log_path,
            ingest_timeout_s=float(getattr(s, "airflow_poller_ingest_timeout_s", 15.0)),
        )
        with _poller_lock:
            _poller = AirflowFailurePoller(cfg)
            _poller.start()


def _poller_state_path(settings: Settings) -> str:
    # Keep in sync with AirflowFailurePoller._state_path derivation
    base_dir = os.path.dirname(settings.runtime_config_path) or "var/config"
    return os.path.join(base_dir, "airflow_poller_state.json")


@app.get("/api/poller/status")
def poller_status(request: Request) -> JSONResponse:
    settings: Settings = request.app.state.settings
    runtime_cfg = _load_runtime_config(settings)
    path = _poller_state_path(settings)
    st: dict[str, Any] = {}
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                st = json.load(f) or {}
    except Exception:
        st = {}

    with _poller_lock:
        alive = bool(_poller and getattr(_poller, "_thread", None) and _poller._thread.is_alive())  # type: ignore[attr-defined]

    seen = st.get("seen") or []
    return JSONResponse(
        {
            "ok": True,
            "enabled": bool(runtime_cfg.get("airflow_poller_enabled", getattr(settings, "airflow_poller_enabled", False))),
            "alive": alive,
            "base_url": runtime_cfg.get("airflow_base_url") or settings.airflow_base_url,
            "interval_s": float(getattr(settings, "airflow_poller_interval_s", 8.0)),
            "state_path": path,
            "seen_count": len(seen) if isinstance(seen, list) else None,
            "last_poll_ts": st.get("last_poll_ts"),
        }
    )


@app.post("/api/poller/reset")
def poller_reset(request: Request, full_backfill: bool = False) -> JSONResponse:
    """
    “Clear queue” for the poller:
    - clears the persisted seen-set so old failures can be re-discovered
    - restarts the poller thread to re-load state immediately
    """
    global _poller
    settings: Settings = request.app.state.settings
    runtime_cfg = _load_runtime_config(settings)
    poller_enabled = bool(runtime_cfg.get("airflow_poller_enabled", getattr(settings, "airflow_poller_enabled", False)))
    airflow_base_url = runtime_cfg.get("airflow_base_url") or getattr(settings, "airflow_base_url", None)
    airflow_username = runtime_cfg.get("airflow_username") or getattr(settings, "airflow_username", None)
    airflow_password = runtime_cfg.get("airflow_password") or getattr(settings, "airflow_password", None)
    if not poller_enabled:
        return JSONResponse({"ok": False, "error": "poller_disabled"}, status_code=400)

    # Clear state file.
    # By default we do NOT rewind far back (avoids “spam”); we reset the seen-set but keep last_poll_ts near now.
    # If you explicitly want to backfill older failures, call with `?full_backfill=true`.
    path = _poller_state_path(settings)
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            last_poll_ts = None if full_backfill else datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            json.dump({"seen": [], "last_poll_ts": last_poll_ts}, f)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"state_write_failed: {e}"}, status_code=500)

    # Restart poller so it re-reads cleared state
    try:
        cfg = AirflowPollerConfig(
            base_url=airflow_base_url,
            username=airflow_username,
            password=airflow_password,
            timeout_s=float(getattr(settings, "airflow_poller_timeout_s", 20.0)),
            interval_s=float(getattr(settings, "airflow_poller_interval_s", 8.0)),
            lookback_minutes=int(getattr(settings, "airflow_poller_lookback_minutes", 180)),
            max_dags=int(getattr(settings, "airflow_poller_max_dags", 200)),
            max_new_events_per_cycle=int(getattr(settings, "airflow_poller_max_new_events_per_cycle", 5)),
            logs_base=str(getattr(settings, "airflow_logs_base", "/opt/airflow/logs")),
            data_base=str(getattr(settings, "airflow_data_base", "/opt/airflow/data")),
            runtime_config_path=settings.runtime_config_path,
            audit_log_path=settings.audit_log_path,
            ingest_timeout_s=float(getattr(settings, "airflow_poller_ingest_timeout_s", 15.0)),
        )
        with _poller_lock:
            if _poller:
                try:
                    _poller.stop()
                except Exception:
                    pass
            _poller = AirflowFailurePoller(cfg)
            _poller.start()
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"poller_restart_failed: {e}"}, status_code=500)

    # Emit an audit record so the UI has a visible “you reset it” breadcrumb
    try:
        AuditLogger(settings.audit_log_path).write("poller", "poller.reset", {"state_path": path})
    except Exception:
        pass

    return JSONResponse({"ok": True, "state_path": path})


@app.get("/health")
def health(request: Request) -> Dict[str, Any]:
    return {"ok": True, "version": "0.1.0"}


@app.get("/ui", response_class=HTMLResponse)
def ui(request: Request) -> HTMLResponse:
    # Avoid browser caching stale UI JS/CSS embedded in the HTML string.
    return HTMLResponse(
        content=render_ui_html(),
        headers={
            "Cache-Control": "no-store, max-age=0",
            "Pragma": "no-cache",
        },
    )


@app.get("/api/audit/recent")
def audit_recent(request: Request, n: int = 200) -> JSONResponse:
    settings: Settings = request.app.state.settings
    records = [r.__dict__ for r in tail_jsonl(settings.audit_log_path, max_lines=max(1, min(n, 2000)))]
    return JSONResponse({"records": records})


@app.post("/api/airflow/test")
async def airflow_test(request: Request) -> JSONResponse:
    try:
        body = await request.json()
    except Exception:
        body = {}
    body = body if isinstance(body, dict) else {}
    base_url = str(body.get("airflow_base_url") or "").strip()
    username = str(body.get("airflow_username") or "").strip()
    password = str(body.get("airflow_password") or "").strip()
    if not base_url or not username or not password:
        return JSONResponse({"ok": False, "error": "missing_airflow_credentials"}, status_code=400)
    health_url = base_url.rstrip("/") + "/api/v1/health"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(health_url, auth=(username, password))
        if r.status_code != 200:
            return JSONResponse({"ok": False, "error": f"http_{r.status_code}", "body": r.text[:500]}, status_code=400)
        data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        return JSONResponse({"ok": True, "health": data})
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


@app.post("/api/repo/test")
async def repo_test(request: Request) -> JSONResponse:
    try:
        body = await request.json()
    except Exception:
        body = {}
    body = body if isinstance(body, dict) else {}
    repo_path = str(body.get("repo_path") or "").strip()
    if not repo_path:
        return JSONResponse({"ok": False, "error": "repo_path_missing"}, status_code=400)
    if not os.path.exists(repo_path):
        return JSONResponse({"ok": False, "error": "repo_path_not_found"}, status_code=400)
    if not os.path.isdir(repo_path):
        return JSONResponse({"ok": False, "error": "repo_path_not_directory"}, status_code=400)
    file_count = 0
    for root, dirs, files in os.walk(repo_path):
        dirs[:] = [d for d in dirs if d not in {".git", "__pycache__", ".venv"}]
        file_count += len(files)
        if file_count > 4000:
            break
    git_ok = os.path.isdir(os.path.join(repo_path, ".git"))
    return JSONResponse({"ok": True, "file_count": file_count, "git_repo": git_ok})


@app.post("/api/inhouse/test")
async def inhouse_test(request: Request) -> JSONResponse:
    try:
        body = await request.json()
    except Exception:
        body = {}
    body = body if isinstance(body, dict) else {}
    url = str(body.get("inhouse_agent_url") or "").strip()
    if not url:
        return JSONResponse({"ok": False, "error": "inhouse_agent_url_missing"}, status_code=400)
    headers = {"Content-Type": "application/json"}
    headers_json = str(body.get("inhouse_headers_json") or "").strip()
    if headers_json:
        try:
            extra = json.loads(headers_json) or {}
            if isinstance(extra, dict):
                headers.update({str(k): str(v) for k, v in extra.items()})
        except json.JSONDecodeError:
            pass
    api_key = str(body.get("inhouse_api_key") or "").strip()
    if api_key and "Authorization" not in headers:
        headers["Authorization"] = f"Bearer {api_key}"
    payload = {"event": {"event_id": "patchit:test", "platform": "airflow", "pipeline_id": "test", "run_id": "test"}, "normalized_error": {"message": "test"}, "code_context": {}, "artifact_summaries": [], "codebase_index": {}}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(url, json=payload, headers=headers)
        if r.status_code != 200:
            return JSONResponse({"ok": False, "error": f"http_{r.status_code}", "body": r.text[:500]}, status_code=400)
        data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        has_patch = bool((data or {}).get("diff") or (data or {}).get("patch"))
        return JSONResponse({"ok": True, "has_patch": has_patch})
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


@app.post("/api/databricks/test")
async def databricks_test(request: Request) -> JSONResponse:
    """
    Databricks connector sanity check.
    We only validate basic HTTP reachability + token auth against Jobs API.
    """
    try:
        body = await request.json()
    except Exception:
        body = {}
    body = body if isinstance(body, dict) else {}
    host = str(body.get("databricks_host") or "").strip().rstrip("/")
    token = str(body.get("databricks_token") or "").strip()
    if not host or not token:
        return JSONResponse({"ok": False, "error": "missing_databricks_host_or_token"}, status_code=400)
    url = host + "/api/2.1/jobs/list?limit=1"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, headers={"Authorization": f"Bearer {token}"})
        if r.status_code != 200:
            return JSONResponse({"ok": False, "error": f"http_{r.status_code}", "body": r.text[:500]}, status_code=400)
        data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        jobs = (data or {}).get("jobs") if isinstance(data, dict) else None
        return JSONResponse({"ok": True, "jobs_visible": isinstance(jobs, list), "sample": data})
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


@app.get("/api/audit/stream")
async def audit_stream(request: Request) -> StreamingResponse:
    settings: Settings = request.app.state.settings
    return StreamingResponse(stream_jsonl_sse(settings.audit_log_path), media_type="text/event-stream")


@app.get("/api/runtime_config")
def runtime_config_get(request: Request) -> JSONResponse:
    settings: Settings = request.app.state.settings
    cfg = _load_runtime_config(settings)
    effective = _effective_agent_mode(settings)
    override = _explain_mode_override(settings)
    warnings: list[str] = []
    if override:
        warnings.append(
            f"Requested agent_mode={override.get('requested')} is unavailable ({', '.join(override.get('reasons') or [])}); "
            f"using {override.get('effective')}."
        )
    return JSONResponse(
        {
            "config": _sanitize_runtime_config(cfg),
            "effective": {
                "agent_mode": effective,
                "openrouter_model": settings.openrouter_model,
                "groq_model": settings.groq_model,
                "cursor_model": "cursor_cloud_agent",
            },
            # Safe to expose (booleans only) so the UI can disable impossible selections.
            "available": {
                "openrouter": bool(getattr(settings, "openrouter_api_key", None)),
                "groq": bool(getattr(settings, "groq_api_key", None)),
                "cursor": bool(getattr(settings, "cursor_api_key", None))
                and settings.github_mode == "real"
                and bool(getattr(settings, "github_token", None))
                and bool(getattr(settings, "github_repo", None)),
                "inhouse": bool(cfg.get("inhouse_agent_url") or getattr(settings, "inhouse_agent_url", None)),
            },
            "warnings": warnings,
            "override": override,
        }
    )


@app.post("/api/runtime_config")
async def runtime_config_set(request: Request) -> JSONResponse:
    settings: Settings = request.app.state.settings
    try:
        body = await request.json()
    except Exception:
        body = {}
    body = body if isinstance(body, dict) else {}
    cfg = _load_runtime_config(settings)
    # Allow only safe keys
    if "agent_mode" in body:
        v = body.get("agent_mode")
        if v == "groq" and not settings.groq_api_key:
            return JSONResponse({"ok": False, "error": "groq_api_key_missing"}, status_code=400)
        if v == "openrouter" and not settings.openrouter_api_key:
            return JSONResponse({"ok": False, "error": "openrouter_api_key_missing"}, status_code=400)
        if v == "cursor" and (
            not getattr(settings, "cursor_api_key", None)
            or settings.github_mode != "real"
            or not settings.github_token
            or not settings.github_repo
        ):
            return JSONResponse({"ok": False, "error": "cursor_config_missing"}, status_code=400)
        if v == "inhouse" and not (body.get("inhouse_agent_url") or cfg.get("inhouse_agent_url") or getattr(settings, "inhouse_agent_url", None)):
            return JSONResponse({"ok": False, "error": "inhouse_agent_url_missing"}, status_code=400)
        if v in ("openrouter", "groq", "cursor", "inhouse"):
            cfg["agent_mode"] = v
        elif v in ("", None):
            cfg.pop("agent_mode", None)

    # Cursor-specific UI toggle: allow Cursor to auto-create PR (PATCHIT will adopt/link it post-verify).
    if "cursor_auto_create_pr" in body:
        v = body.get("cursor_auto_create_pr")
        if isinstance(v, bool):
            cfg["cursor_auto_create_pr"] = v
    if "inhouse_agent_url" in body:
        v = body.get("inhouse_agent_url")
        if isinstance(v, str):
            cfg["inhouse_agent_url"] = v.strip()
    if "inhouse_api_key" in body:
        v = body.get("inhouse_api_key")
        if isinstance(v, str):
            cfg["inhouse_api_key"] = v.strip()
    if "inhouse_headers_json" in body:
        v = body.get("inhouse_headers_json")
        if isinstance(v, str):
            cfg["inhouse_headers_json"] = v.strip()
    if "airflow_base_url" in body:
        v = body.get("airflow_base_url")
        if isinstance(v, str):
            v = v.strip()
            if v:
                cfg["airflow_base_url"] = v
            else:
                cfg.pop("airflow_base_url", None)
    if "airflow_username" in body:
        v = body.get("airflow_username")
        if isinstance(v, str):
            v = v.strip()
            if v:
                cfg["airflow_username"] = v
            else:
                cfg.pop("airflow_username", None)
    if "airflow_password" in body:
        v = body.get("airflow_password")
        if isinstance(v, str):
            v = v.strip()
            if v:
                cfg["airflow_password"] = v
            else:
                cfg.pop("airflow_password", None)
    if "airflow_poller_enabled" in body:
        v = body.get("airflow_poller_enabled")
        if isinstance(v, bool):
            cfg["airflow_poller_enabled"] = v
    if "repo_root_override" in body:
        v = body.get("repo_root_override")
        if isinstance(v, str):
            v = v.strip()
            if v:
                cfg["repo_root_override"] = v
            else:
                cfg.pop("repo_root_override", None)
    if "repo_registry_json" in body:
        v = body.get("repo_registry_json")
        if isinstance(v, str):
            v = v.strip()
            if v:
                cfg["repo_registry_json"] = v
            else:
                cfg.pop("repo_registry_json", None)
    if "local_sync_enabled" in body:
        v = body.get("local_sync_enabled")
        if isinstance(v, bool):
            cfg["local_sync_enabled"] = v
    if "local_sync_repo_path" in body:
        v = body.get("local_sync_repo_path")
        if isinstance(v, str):
            v = v.strip()
            if v:
                cfg["local_sync_repo_path"] = v
            else:
                cfg.pop("local_sync_repo_path", None)
    if "local_sync_worktree_root" in body:
        v = body.get("local_sync_worktree_root")
        if isinstance(v, str):
            v = v.strip()
            if v:
                cfg["local_sync_worktree_root"] = v
            else:
                cfg.pop("local_sync_worktree_root", None)
    if "local_validate_cmd" in body:
        v = body.get("local_validate_cmd")
        if isinstance(v, str):
            v = v.strip()
            if v:
                cfg["local_validate_cmd"] = v
            else:
                cfg.pop("local_validate_cmd", None)
    if "pr_target" in body:
        v = body.get("pr_target")
        if isinstance(v, str) and v in ("github", "local"):
            cfg["pr_target"] = v
    if "databricks_host" in body:
        v = body.get("databricks_host")
        if isinstance(v, str):
            v = v.strip()
            if v:
                cfg["databricks_host"] = v
            else:
                cfg.pop("databricks_host", None)
    if "databricks_token" in body:
        v = body.get("databricks_token")
        if isinstance(v, str):
            v = v.strip()
            if v:
                cfg["databricks_token"] = v
            else:
                cfg.pop("databricks_token", None)
    _save_runtime_config(settings, cfg)
    return JSONResponse({"ok": True, "config": cfg, "effective_agent_mode": _effective_agent_mode(settings)})


@app.get("/mock/pr/{pr_number}", response_class=HTMLResponse)
def mock_pr_page(request: Request, pr_number: int) -> str:
    settings: Settings = request.app.state.settings
    pr = load_mock_pr(settings.mock_github_dir, pr_number)
    return render_mock_pr_html(pr)


@app.get("/api/mock/pr/{pr_number}")
def mock_pr_json(request: Request, pr_number: int) -> JSONResponse:
    settings: Settings = request.app.state.settings
    pr = load_mock_pr(settings.mock_github_dir, pr_number)
    return JSONResponse(
        {
            "pr_number": pr.pr_number,
            "title": pr.title,
            "repo": pr.repo,
            "branch": pr.branch,
            "body": pr.body,
            "diff": pr.diff_text,
        }
    )


@app.get("/api/patch/{patch_id}.diff")
def patch_diff(request: Request, patch_id: str) -> PlainTextResponse:
    settings: Settings = request.app.state.settings
    path = os.path.join(settings.patch_store_dir, f"{patch_id}.diff")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="patch not found")
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        diff = f.read()
    return PlainTextResponse(diff, media_type="text/x-diff")


@app.get("/api/report/{report_id}.md")
def report_md(request: Request, report_id: str) -> PlainTextResponse:
    settings: Settings = request.app.state.settings
    path = os.path.join(settings.report_store_dir, f"{report_id}.md")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="report not found")
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        text = f.read()
    return PlainTextResponse(text, media_type="text/markdown")


@app.get("/api/report/{report_id}.log")
def report_log(request: Request, report_id: str) -> PlainTextResponse:
    settings: Settings = request.app.state.settings
    path = os.path.join(settings.report_store_dir, f"{report_id}.log")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="report log not found")
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        text = f.read()
    return PlainTextResponse(text, media_type="text/plain")


@app.get("/api/evidence/{correlation_id}.json")
def evidence_json(request: Request, correlation_id: str) -> JSONResponse:
    settings: Settings = request.app.state.settings
    path = os.path.join(settings.evidence_store_dir, f"{correlation_id}.evidence.json")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="evidence not found")
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            payload = json.load(f)
    except Exception:
        # Fall back to raw text if JSON parse fails (should not happen, but keeps integrations robust).
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return JSONResponse({"schema": "patchit.evidence_pack.unknown", "raw": f.read()})
    return JSONResponse(payload)


@app.post("/events/ingest", response_model=RemediationResult)
def ingest(event: UniversalFailureEvent, request: Request) -> RemediationResult:
    settings: Settings = request.app.state.settings
    agent_mode = _effective_agent_mode(settings)
    runtime_cfg = _load_runtime_config(settings)
    audit = AuditLogger(settings.audit_log_path)
    correlation_id = audit.new_correlation_id()
    # If runtime_config requested a mode that can't be honored, make it explicit (no silent fallbacks).
    try:
        ov = _explain_mode_override(settings)
        if ov:
            audit.write(correlation_id, "agent.mode_override", ov)
    except Exception:  # noqa: BLE001
        pass
    # Integration fan-out: mirror selected audit events to n8n (or any webhook receiver).
    try:
        emitter: IntegrationEmitter | None = getattr(request.app.state, "integration_emitter", None)
    except Exception:
        emitter = None

    if emitter is not None:
        _raw_write = audit.write

        def _emit_safe(et: str, pl: dict[str, Any]) -> None:
            try:
                links = {
                    "ui": str(getattr(settings, "public_base_url", "http://localhost:8088")),
                    "evidence": f"{getattr(settings, 'public_base_url', 'http://localhost:8088')}/api/evidence/{correlation_id}.json",
                }
                # Avoid huge payloads; keep the integration event small and stable.
                payload_small: dict[str, Any] = {}
                if isinstance(pl, dict):
                    # Include only shallow keys; truncate large string fields.
                    for k, v in list(pl.items())[:25]:
                        if isinstance(v, str) and len(v) > 1500:
                            payload_small[k] = v[:1500] + "…"
                        else:
                            payload_small[k] = v
                emitter.emit(
                    event_type=et,
                    correlation_id=correlation_id,
                    payload={
                        "schema": "patchit.integration.payload.v1",
                        "event": {
                            "event_id": event.event_id,
                            "pipeline_id": event.pipeline_id,
                            "run_id": event.run_id,
                            "task_id": event.task_id,
                        },
                        "links": links,
                        "audit": {"event_type": et, "payload": payload_small},
                    },
                )
            except Exception:
                return

        def _write(cid: str, et: str, pl: dict[str, Any]) -> None:
            _raw_write(cid, et, pl)
            _emit_safe(et, pl or {})

        audit.write = _write  # type: ignore[method-assign]
    # Include the configured agent provider/model in the first audit record for easy debugging.
    agent_cfg: Dict[str, Any] = {"mode": agent_mode}
    if agent_mode == "openrouter":
        agent_cfg["model"] = settings.openrouter_model
    elif agent_mode == "groq":
        agent_cfg["model"] = settings.groq_model
    elif agent_mode == "cursor":
        agent_cfg["model"] = "cursor_cloud_agent"
    elif agent_mode == "bytez":
        agent_cfg["model"] = "bytez"
    audit.write(correlation_id, "event.received", {"event": event.model_dump(mode="json"), "agent": agent_cfg})
    mem = RemediationMemoryStore(db_path=settings.memory_db_path)
    report: RootCauseReport | None = None

    # Fail fast on missing provider credentials (otherwise the UI looks like "no_patch_returned").
    if agent_mode == "groq" and not getattr(settings, "groq_api_key", None):
        audit.write(correlation_id, "agent.failed", {"error": "groq_api_key_missing", "attempt": 0, "provider": "groq"})
        report = RootCauseReport(
            event_id=event.event_id,
            pipeline_id=event.pipeline_id,
            run_id=event.run_id,
            task_id=event.task_id,
            error_fingerprint="(config_missing)",
            failure_summary="PATCHIT is configured to use Groq, but GROQ_API_KEY is missing. No agent call can be made, so no patch/PR can be generated.",
            category="env_mismatch",
            recommended_next_steps=[
                "Set GROQ_API_KEY in `postgres-etl-pipeline/airflow/.env` (or export it before `docker compose up`).",
                "Restart PATCHIT: `cd postgres-etl-pipeline/airflow && docker compose up -d --force-recreate patchit`.",
                "Re-run the test DAG (or click 'Reset poller' then re-trigger) so PATCHIT re-ingests the failure with Groq enabled.",
            ],
            timeline=[],
            notes="This is a configuration gate, not a remediation failure. Once GROQ_API_KEY is present, PATCHIT will proceed with agentic patching + verification gates.",
        )
        audit.write(correlation_id, "remediation.escalated", {"report": report.model_dump(mode="json")})
        try:
            report_id = f"{correlation_id}.config_missing"
            saved = _persist_report_md(
                report_id=report_id,
                md_text=render_root_cause_report_md(report=report, title="PATCHIT Report: Configuration required"),
                store_dir=settings.report_store_dir,
            )
            audit.write(correlation_id, "report.saved", {"report_id": report_id, "path": saved})
        except Exception as e:  # noqa: BLE001
            audit.write(correlation_id, "report.save_failed", {"error": str(e)})

        decision = PolicyEngine(RemediationPolicy(require_human_approval=True)).evaluate_patch(
            PatchProposal(
                patch_id="none",
                title="blocked: groq_api_key_missing",
                rationale="blocked: groq_api_key_missing",
                diff_unified="",
                files=[],
                confidence=0.0,
                requires_human_approval=True,
            )
        )
        decision.allowed = False
        decision.reasons = ["groq_api_key_missing"]
        audit.write(correlation_id, "policy.decided", {"decision": decision.model_dump(mode="json"), "phase": "final"})
        return RemediationResult(
            event=event,
            error=NormalizedErrorObject(
                error_id=uuid.uuid4().hex,
                error_type="ConfigurationError",
                message="groq_api_key_missing",
                stack=[],
                raw_excerpt="",
            ),
            patch=None,
            policy=decision,
            pr=None,
            report=report,
            audit_correlation_id=correlation_id,
        )

    if agent_mode == "openrouter" and not getattr(settings, "openrouter_api_key", None):
        audit.write(correlation_id, "agent.failed", {"error": "openrouter_api_key_missing", "attempt": 0, "provider": "openrouter"})
        report = RootCauseReport(
            event_id=event.event_id,
            pipeline_id=event.pipeline_id,
            run_id=event.run_id,
            task_id=event.task_id,
            error_fingerprint="(config_missing)",
            failure_summary="PATCHIT is configured to use OpenRouter, but OPENROUTER_API_KEY is missing. No agent call can be made, so no patch/PR can be generated.",
            category="env_mismatch",
            recommended_next_steps=[
                "Set OPENROUTER_API_KEY in your PATCHIT environment (e.g. `postgres-etl-pipeline/airflow/.env`).",
                "Restart PATCHIT, then re-run the test DAG (or reset the poller) to re-ingest the failure.",
            ],
            timeline=[],
            notes="This is a configuration gate, not a remediation failure.",
        )
        audit.write(correlation_id, "remediation.escalated", {"report": report.model_dump(mode="json")})
        try:
            report_id = f"{correlation_id}.config_missing"
            saved = _persist_report_md(
                report_id=report_id,
                md_text=render_root_cause_report_md(report=report, title="PATCHIT Report: Configuration required"),
                store_dir=settings.report_store_dir,
            )
            audit.write(correlation_id, "report.saved", {"report_id": report_id, "path": saved})
        except Exception as e:  # noqa: BLE001
            audit.write(correlation_id, "report.save_failed", {"error": str(e)})
        decision = PolicyEngine(RemediationPolicy(require_human_approval=True)).evaluate_patch(
            PatchProposal(
                patch_id="none",
                title="blocked: openrouter_api_key_missing",
                rationale="blocked: openrouter_api_key_missing",
                diff_unified="",
                files=[],
                confidence=0.0,
                requires_human_approval=True,
            )
        )
        decision.allowed = False
        decision.reasons = ["openrouter_api_key_missing"]
        audit.write(correlation_id, "policy.decided", {"decision": decision.model_dump(mode="json"), "phase": "final"})
        return RemediationResult(
            event=event,
            error=NormalizedErrorObject(
                error_id=uuid.uuid4().hex,
                error_type="ConfigurationError",
                message="openrouter_api_key_missing",
                stack=[],
                raw_excerpt="",
            ),
            patch=None,
            policy=decision,
            pr=None,
            report=report,
            audit_correlation_id=correlation_id,
        )

    if agent_mode == "cursor":
        missing: list[str] = []
        if not getattr(settings, "cursor_api_key", None):
            missing.append("CURSOR_API_KEY")
        if settings.github_mode != "real":
            missing.append("github_mode=real")
        if not getattr(settings, "github_token", None):
            missing.append("PATCHIT_GITHUB_TOKEN")
        if not getattr(settings, "github_repo", None):
            missing.append("PATCHIT_GITHUB_REPO")
        if missing:
            audit.write(
                correlation_id,
                "agent.failed",
                {"error": "cursor_config_missing", "attempt": 0, "provider": "cursor", "missing": missing},
            )
            report = RootCauseReport(
                event_id=event.event_id,
                pipeline_id=event.pipeline_id,
                run_id=event.run_id,
                task_id=event.task_id,
                error_fingerprint="(config_missing)",
                failure_summary=(
                    "PATCHIT is configured to use Cursor Cloud Agents, but required configuration is missing. "
                    "No Cursor agent can be launched, so no patch/PR can be generated."
                ),
                category="env_mismatch",
                recommended_next_steps=[
                    "Set CURSOR_API_KEY in your PATCHIT environment (e.g. `postgres-etl-pipeline/airflow/.env`).",
                    "Ensure PATCHIT is in `github_mode=real` and PATCHIT_GITHUB_TOKEN + PATCHIT_GITHUB_REPO are set (Cursor diffs are fetched via GitHub compare).",
                    "Restart PATCHIT, then re-run the test DAG (or reset the poller) to re-ingest the failure.",
                ],
                timeline=[],
                notes=f"Missing/invalid: {', '.join(missing)}",
            )
            audit.write(correlation_id, "remediation.escalated", {"report": report.model_dump(mode="json")})
            try:
                report_id = f"{correlation_id}.config_missing"
                saved = _persist_report_md(
                    report_id=report_id,
                    md_text=render_root_cause_report_md(report=report, title="PATCHIT Report: Configuration required"),
                    store_dir=settings.report_store_dir,
                )
                audit.write(correlation_id, "report.saved", {"report_id": report_id, "path": saved})
            except Exception as e:  # noqa: BLE001
                audit.write(correlation_id, "report.save_failed", {"error": str(e)})

            decision = PolicyEngine(RemediationPolicy(require_human_approval=True)).evaluate_patch(
                PatchProposal(
                    patch_id="none",
                    title="blocked: cursor_config_missing",
                    rationale="blocked: cursor_config_missing",
                    diff_unified="",
                    files=[],
                    confidence=0.0,
                    requires_human_approval=True,
                )
            )
            decision.allowed = False
            decision.reasons = ["cursor_config_missing"]
            audit.write(correlation_id, "policy.decided", {"decision": decision.model_dump(mode="json"), "phase": "final"})
            return RemediationResult(
                event=event,
                error=NormalizedErrorObject(
                    error_id=uuid.uuid4().hex,
                    error_type="ConfigurationError",
                    message="cursor_config_missing",
                    stack=[],
                    raw_excerpt="",
                ),
                patch=None,
                policy=decision,
                pr=None,
                report=report,
                audit_correlation_id=correlation_id,
            )

    if not event.log_uri:
        raise HTTPException(status_code=400, detail="log_uri is required for Phase-1 MVP")

    # Fetch logs
    fetcher = LogFetcher(
        timeout_s=15.0,
        translate_from=settings.file_translate_from,
        translate_to=settings.file_translate_to,
        attempt_fallback=True,
    )
    try:
        log_text = fetcher.fetch(event.log_uri)
    except Exception as e:  # noqa: BLE001 (service boundary)
        audit.write(correlation_id, "log.fetch_failed", {"log_uri": event.log_uri, "error": str(e)})
        raise HTTPException(status_code=400, detail=f"Could not fetch logs: {e}") from e

    audit.write(correlation_id, "log.fetched", {"bytes": len(log_text.encode('utf-8'))})

    # Reduce noisy Airflow logs to just the error region, then parse stack trace
    reduced = extract_error_excerpt(log_text)
    audit.write(correlation_id, "log.excerpted", {"has_traceback": reduced.has_traceback, "lines": len(reduced.excerpt.splitlines())})

    parsed = parse_python_traceback(reduced.excerpt)
    if parsed:
        err_type = parsed.error_type
        msg = parsed.message
        frames = parsed.frames
        excerpt = parsed.excerpt
    else:
        err_type, msg = parse_first_exception(reduced.excerpt)
        frames = []
        excerpt = "\n".join(reduced.excerpt.splitlines()[-50:])

    err = NormalizedErrorObject(
        error_id=uuid.uuid4().hex,
        error_type=err_type,
        message=msg,
        stack=frames,
        raw_excerpt=excerpt,
    )
    audit.write(correlation_id, "error.parsed", {"error": err.model_dump(mode="json")})

    # --- Safety wall: prompt injection in logs/artifacts should trigger immediate stop + report. ---
    inj = detect_prompt_injection(err.raw_excerpt)
    if not inj.ok:
        audit.write(correlation_id, "safety.prompt_injection_detected", {"matches": inj.matches})
        report = RootCauseReport(
            event_id=event.event_id,
            pipeline_id=event.pipeline_id,
            run_id=event.run_id,
            task_id=event.task_id,
            error_fingerprint=RemediationMemoryStore.error_fingerprint(error=err.model_dump(mode="json")),
            failure_summary="PATCHIT refused to auto-remediate because prompt-injection-like strings were detected in logs/artifacts.",
            category=RootCauseCategory.unknown,
            recommended_next_steps=[
                "Treat this incident input as untrusted. Review upstream logs/artifacts for injected instructions.",
                "Sanitize/escape untrusted payloads before logging them.",
                "If this is a false positive, tune detector patterns in `patchit/policy/safety.py`.",
            ],
            notes=f"Detected patterns: {inj.matches}",
        )
        audit.write(correlation_id, "remediation.escalated", {"report": report.model_dump(mode="json")})
        try:
            report_id = f"{correlation_id}.prompt_injection"
            saved = _persist_report_md(
                report_id=report_id,
                md_text=render_root_cause_report_md(report=report, title="PATCHIT Report: Safety refusal (prompt injection)"),
                store_dir=settings.report_store_dir,
            )
            audit.write(correlation_id, "report.saved", {"report_id": report_id, "path": saved})
        except Exception as e:  # noqa: BLE001
            audit.write(correlation_id, "report.save_failed", {"error": str(e)})

        decision = PolicyEngine(RemediationPolicy(require_human_approval=True)).evaluate_patch(
            PatchProposal(
                patch_id="none",
                title="refused: prompt_injection_detected",
                rationale="refused: prompt_injection_detected",
                diff_unified="",
                files=[],
                confidence=0.0,
                requires_human_approval=True,
            )
        )
        decision.allowed = False
        decision.reasons = ["prompt_injection_detected"]
        audit.write(correlation_id, "policy.decided", {"decision": decision.model_dump(mode="json"), "phase": "final"})

        # Evidence pack (refusal): persist proof bundle even when we refuse to patch.
        try:
            payload = build_evidence_payload(
                correlation_id=correlation_id,
                event=event,
                error=err,
                policy=decision,
                patch=None,
                pr=None,
                report=report,
                agent_mode=str(agent_mode),
                agent_model=agent_cfg.get("model") if isinstance(agent_cfg, dict) else None,
                artifact_summaries=[],
                code_context={},
                agent_pipeline=last_agent_pipeline,
            )
            ev = persist_evidence_pack(store_dir=settings.evidence_store_dir, correlation_id=correlation_id, payload=payload)
            audit.write(correlation_id, "evidence.saved", {"path": ev.path})
        except Exception as e:  # noqa: BLE001
            audit.write(correlation_id, "evidence.save_failed", {"error": str(e)})

        return RemediationResult(event=event, error=err, patch=None, policy=decision, pr=None, report=report, audit_correlation_id=correlation_id)

    # --- Orchestration pivot: TriggerDagRunOperator failures are pointers, not root causes. ---
    # If a TriggerDagRunOperator fails because the *triggered* DAG failed, PATCHIT should remediate
    # the downstream DAG's failed task (dbt_run) rather than the trigger task itself.
    try:
        is_airflow = bool(getattr(event, "platform", None)) and getattr(event.platform, "value", None) == "airflow"
        is_trigger_task = (event.task_id or "").startswith("trigger_")
        is_airflow_exception = err.error_type in ("AirflowException", "Exception")
        has_airflow_creds = bool(getattr(settings, "airflow_base_url", None) and getattr(settings, "airflow_username", None) and getattr(settings, "airflow_password", None))

        # Parse target DAG id directly from the exception message (more robust than substring checks).
        msg = (err.message or "").strip()
        # NOTE: raw strings use a single backslash for regex escapes (use \s, not \\s).
        m = re.search(r"(?P<dag>[A-Za-z0-9_.\\-]+)\s+failed\s+with\s+failed\s+states", msg, flags=re.IGNORECASE)
        target_dag = m.group("dag") if m else None

        # Try to parse the triggered run logical timestamp from the log excerpt.
        m2 = re.search(
            r"Waiting\s+for\s+(?P<dag>[A-Za-z0-9_.\\-]+)\s+on\s+(?P<ts>[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9:.]+(?:\+00:00|Z)?)",
            err.raw_excerpt or "",
        )
        wait_ts_s = m2.group("ts") if m2 else None
        if m2 and not target_dag:
            target_dag = m2.group("dag")

        # Emit a lightweight breadcrumb whenever we see a likely orchestration-pointer failure.
        if is_airflow and is_trigger_task and is_airflow_exception and has_airflow_creds:
            audit.write(
                correlation_id,
                "orchestration.pointer_seen",
                {"target_dag": target_dag, "wait_ts": wait_ts_s, "message": msg[:180]},
            )

        if is_airflow and is_trigger_task and is_airflow_exception and has_airflow_creds and target_dag:
            # Try to parse the triggered run logical timestamp from the log excerpt.
            audit.write(
                correlation_id,
                "orchestration.detected",
                {"kind": "trigger_dagrun_pointer", "from": {"dag_id": event.pipeline_id, "task_id": event.task_id}, "to": {"dag_id": target_dag}},
            )

            adapter = AirflowAdapter(settings.airflow_base_url, settings.airflow_username, settings.airflow_password, timeout_s=10.0)
            # Find the most likely triggered dag_run_id.
            # IMPORTANT: Airflow API run ordering isn't guaranteed; always sort by most recent timestamp.
            target_run_id: str | None = None
            try:
                from datetime import datetime, timezone

                def _parse_dt(s: str | None) -> datetime | None:
                    if not isinstance(s, str) or not s:
                        return None
                    try:
                        dt0 = datetime.fromisoformat(s.replace("Z", "+00:00").replace(" ", "T"))
                        if dt0.tzinfo is None:
                            dt0 = dt0.replace(tzinfo=timezone.utc)
                        return dt0
                    except Exception:
                        return None

                def _run_dt(dr: dict) -> datetime | None:
                    ts0 = dr.get("logical_date") or dr.get("execution_date") or dr.get("start_date")
                    return _parse_dt(ts0) if isinstance(ts0, str) else None

                runs = adapter.list_dag_runs(target_dag, limit=200)
                runs_sorted = sorted(runs, key=lambda dr: _run_dt(dr) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)

                if wait_ts_s:
                    dt_target = _parse_dt(wait_ts_s)
                    if dt_target:
                        best = None
                        best_abs = None
                        for dr in runs_sorted:
                            rid0 = dr.get("dag_run_id") or dr.get("run_id")
                            if not isinstance(rid0, str) or not rid0:
                                continue
                            dt0 = _run_dt(dr)
                            if not dt0:
                                continue
                            dabs = abs((dt0 - dt_target).total_seconds())
                            if best_abs is None or dabs < best_abs:
                                best_abs = dabs
                                best = rid0
                        if best:
                            target_run_id = best
                            audit.write(
                                correlation_id,
                                "orchestration.run_selected",
                                {"dag_id": target_dag, "run_id": target_run_id, "by": "wait_ts", "abs_s": best_abs},
                            )

                if not target_run_id:
                    # Fallback: pick the most recent FAILED run (by timestamp).
                    for dr in runs_sorted:
                        rid0 = dr.get("dag_run_id") or dr.get("run_id")
                        if not isinstance(rid0, str) or not rid0:
                            continue
                        st0 = str(dr.get("state") or "").lower()
                        if st0 == "failed":
                            target_run_id = rid0
                            audit.write(
                                correlation_id,
                                "orchestration.run_selected",
                                {"dag_id": target_dag, "run_id": target_run_id, "by": "most_recent_failed"},
                            )
                            break
            except Exception as e:  # noqa: BLE001
                audit.write(correlation_id, "orchestration.derive_failed", {"error": f"list_dag_runs_failed: {e}"})

            if target_run_id:
                # Pick the first truly failed task (avoid upstream_failed noise).
                tis = adapter.list_failed_task_instances(target_dag, target_run_id)
                failed_first = [ti for ti in tis if str(ti.get("state") or "").lower() == "failed"] or tis
                if failed_first:
                    ti0 = failed_first[0]
                    target_task_id = ti0.get("task_id")
                    try_no = ti0.get("try_number") or 1
                    try:
                        try_no = int(try_no)
                    except Exception:
                        try_no = 1
                    try_no = max(1, int(try_no))
                    if isinstance(target_task_id, str) and target_task_id:
                        logs_base = str(getattr(settings, "airflow_logs_base", "/opt/airflow/logs")).rstrip("/")
                        log_uri = f"file://{logs_base}/dag_id={target_dag}/run_id={target_run_id}/task_id={target_task_id}/attempt={try_no}.log"
                        derived = adapter.build_failure_event(
                            dag_id=target_dag,
                            dag_run_id=target_run_id,
                            task_id=target_task_id,
                            try_number=try_no,
                            log_uri=log_uri,
                            artifact_uris=[],
                            metadata={"derived_from": event.event_id, "orchestration_pointer": True},
                        )
                        audit.write(correlation_id, "event.derived", {"event": derived.model_dump(mode="json")})

                        # Re-run the fetch/parse loop for the derived event so we get the real dbt/root error.
                        try:
                            log_text = fetcher.fetch(derived.log_uri)
                            audit.write(correlation_id, "log.fetched", {"bytes": len(log_text.encode("utf-8")), "derived": True})
                            reduced = extract_error_excerpt(log_text)
                            audit.write(
                                correlation_id,
                                "log.excerpted",
                                {"has_traceback": reduced.has_traceback, "lines": len(reduced.excerpt.splitlines()), "derived": True},
                            )
                            parsed = parse_python_traceback(reduced.excerpt)
                            if parsed:
                                err_type = parsed.error_type
                                msg = parsed.message
                                frames = parsed.frames
                                excerpt = parsed.excerpt
                            else:
                                err_type, msg = parse_first_exception(reduced.excerpt)
                                frames = []
                                excerpt = "\n".join(reduced.excerpt.splitlines()[-50:])
                            err = NormalizedErrorObject(
                                error_id=uuid.uuid4().hex,
                                error_type=err_type,
                                message=msg,
                                stack=frames,
                                raw_excerpt=excerpt,
                            )
                            audit.write(correlation_id, "error.parsed", {"error": err.model_dump(mode="json"), "derived": True})
                            event = derived
                        except Exception as e:  # noqa: BLE001
                            audit.write(correlation_id, "orchestration.derive_failed", {"error": str(e)})
    except Exception as e:  # noqa: BLE001
        audit.write(correlation_id, "orchestration.derive_failed", {"error": str(e)})

    error_fingerprint = mem.error_fingerprint(error=err.model_dump(mode="json"))
    audit.write(correlation_id, "error.fingerprinted", {"fingerprint": error_fingerprint})

    # --- Clustering guard (PR spam control) ---
    # If we already created a PR recently for this exact failure fingerprint, refuse to open another one.
    try:
        existing_pr = mem.recent_pr_for_fingerprint(
            error_fingerprint=error_fingerprint,
            within_s=float(getattr(settings, "fingerprint_pr_cooldown_s", 21600.0)),
        )
    except Exception:
        existing_pr = None
    if existing_pr:
        audit.write(correlation_id, "cluster.hit", {"fingerprint": error_fingerprint, "existing_pr": existing_pr})
        report = RootCauseReport(
            event_id=event.event_id,
            pipeline_id=event.pipeline_id,
            run_id=event.run_id,
            task_id=event.task_id,
            error_fingerprint=error_fingerprint,
            failure_summary="PATCHIT refused to create a new PR because this failure matches a recent cluster that already has a PR.",
            category=RootCauseCategory.unknown,
            recommended_next_steps=[
                f"Review/merge the existing PR: {existing_pr}",
                "If the PR is merged but failures continue, rebuild/restart the runtime so the fix is deployed.",
                "If you believe this is a different root cause, improve fingerprinting (normalize volatile tokens less aggressively, or add asset/lineage keys).",
            ],
            notes=f"cluster_guard window_s={getattr(settings, 'fingerprint_pr_cooldown_s', 21600.0)}",
        )
        audit.write(correlation_id, "remediation.escalated", {"report": report.model_dump(mode="json")})
        try:
            report_id = f"{correlation_id}.cluster_guard"
            saved = _persist_report_md(
                report_id=report_id,
                md_text=render_root_cause_report_md(report=report, title="PATCHIT Report: Cluster guard (PR already exists)"),
                store_dir=settings.report_store_dir,
            )
            audit.write(correlation_id, "report.saved", {"report_id": report_id, "path": saved})
        except Exception as e:  # noqa: BLE001
            audit.write(correlation_id, "report.save_failed", {"error": str(e)})

        decision = PolicyEngine(RemediationPolicy(require_human_approval=True)).evaluate_patch(
            PatchProposal(
                patch_id="none",
                title="refused: cluster_guard_existing_pr",
                rationale=f"refused: cluster_guard_existing_pr ({existing_pr})",
                diff_unified="",
                files=[],
                confidence=0.0,
                requires_human_approval=True,
            )
        )
        decision.allowed = False
        decision.reasons = ["cluster_guard_existing_pr", f"fingerprint={error_fingerprint}", f"pr={existing_pr}"]
        audit.write(correlation_id, "policy.decided", {"decision": decision.model_dump(mode="json"), "phase": "final"})

        try:
            payload = build_evidence_payload(
                correlation_id=correlation_id,
                event=event,
                error=err,
                policy=decision,
                patch=None,
                pr=None,
                report=report,
                agent_mode=str(agent_mode),
                agent_model=agent_cfg.get("model") if isinstance(agent_cfg, dict) else None,
                artifact_summaries=[],
                code_context={},
                agent_pipeline=last_agent_pipeline,
            )
            ev = persist_evidence_pack(store_dir=settings.evidence_store_dir, correlation_id=correlation_id, payload=payload)
            audit.write(correlation_id, "evidence.saved", {"path": ev.path})
        except Exception as e:  # noqa: BLE001
            audit.write(correlation_id, "evidence.save_failed", {"error": str(e)})

        return RemediationResult(event=event, error=err, patch=None, policy=decision, pr=None, report=report, audit_correlation_id=correlation_id)

    # Stop condition: too many failed attempts for same fingerprint.
    prior = mem.recent_attempts_for_fingerprint(error_fingerprint=error_fingerprint, limit=50)
    if len(prior) >= int(settings.max_total_attempts_per_fingerprint or 0):
        report = RootCauseReport(
            event_id=event.event_id,
            pipeline_id=event.pipeline_id,
            run_id=event.run_id,
            task_id=event.task_id,
            error_fingerprint=error_fingerprint,
            failure_summary=f"Exceeded max attempts ({settings.max_total_attempts_per_fingerprint}) for same failure fingerprint.",
            category=RootCauseCategory.unknown,
            recommended_next_steps=[
                "Inspect upstream task logs and external dependencies (source API, DB, secrets).",
                "Review PATCHIT attempt timeline for repeated patch patterns.",
                "Consider adding/strengthening a sandbox execution harness that mirrors runtime (Airflow task runner / docker-compose).",
            ],
            timeline=[
                AttemptSummary(
                    attempt_id=a.attempt_id,
                    patch_id=a.patch_id,
                    patch_title=a.patch_title,
                    verify_ok=a.verify_ok,
                    verify_summary=a.verify_summary,
                    pr_url=a.pr_url,
                    notes=a.notes,
                )
                for a in prior[:20]
            ],
            notes="PATCHIT stopped to avoid repeated ineffective PRs. This is an escalation report, not a patch.",
        )
        audit.write(correlation_id, "remediation.escalated", {"report": report.model_dump(mode="json")})
        # Persist a downloadable markdown report for the UI.
        try:
            os.makedirs(settings.report_store_dir, exist_ok=True)
            report_id = f"{correlation_id}.root_cause"
            rp = os.path.join(settings.report_store_dir, f"{report_id}.md")
            with open(rp, "w", encoding="utf-8") as f:
                f.write(render_root_cause_report_md(report=report))
            audit.write(correlation_id, "report.saved", {"report_id": report_id, "path": rp})
        except Exception as e:  # noqa: BLE001
            audit.write(correlation_id, "report.save_failed", {"error": str(e)})
        # Return a RemediationResult with no patch; policy will be allowed=false below.
        decision = PolicyEngine(RemediationPolicy(require_human_approval=True)).evaluate_patch(
            PatchProposal(
                patch_id="none",
                title="no patch",
                rationale="escalated: too many attempts",
                diff_unified="",
                files=[],
                confidence=0.0,
                requires_human_approval=True,
            )
        )
        decision.allowed = False
        decision.reasons = ["escalated: max_attempts_reached", f"fingerprint={error_fingerprint}"]
        audit.write(correlation_id, "policy.decided", {"decision": decision.model_dump(mode="json"), "phase": "final"})
        return RemediationResult(event=event, error=err, patch=None, policy=decision, pr=None, report=report, audit_correlation_id=correlation_id)

    # Classify
    err = RuleBasedClassifier().classify(err)
    audit.write(correlation_id, "error.classified", {"classification": err.classification, "confidence": err.confidence})

    # Fast-path escalation: failures dominated by runtime environment misconfiguration.
    # Example: subprocess cannot execute a command (missing binary / wrong PATH / non-executable file).
    # In these cases, LLM patching is usually wasteful and can even fail due to prompt size.
    try:
        msg = (err.message or "").strip()
        stack = err.stack or []
        is_subprocess_exec = any(
            (sf.file_path or "").endswith("/subprocess.py") and sf.function in ("_execute_child", "__init__", "run")
            for sf in stack
        )
        if is_subprocess_exec and (("Permission denied" in msg) or ("No such file or directory" in msg)):
            m = re.search(r":\s*'([^']+)'\s*$", msg)
            exe = m.group(1) if m else None
            report = RootCauseReport(
                event_id=event.event_id,
                pipeline_id=event.pipeline_id,
                run_id=event.run_id,
                task_id=event.task_id,
                error_fingerprint=error_fingerprint,
                failure_summary=(
                    f"Task failed while attempting to execute an external command{(' ' + exe) if exe else ''}: {msg}\n"
                    "This indicates a runtime environment issue (missing binary, wrong PATH, or non-executable file), not a code logic bug."
                ),
                category=RootCauseCategory.env_mismatch,
                recommended_next_steps=[
                    "Enter the Airflow container and verify the command is available and executable (e.g. `command -v <cmd>` and `ls -l $(command -v <cmd>)`).",
                    "Ensure the dependency is installed in the Airflow image/venv used by the task (e.g. add to `airflow/requirements.txt` and rebuild).",
                    "Re-run the DAG after the runtime environment is fixed.",
                ],
                timeline=[],
                notes="PATCHIT skipped agentic patch generation because this failure is dominated by runtime environment configuration.",
            )
            audit.write(correlation_id, "remediation.escalated", {"report": report.model_dump(mode="json")})
            try:
                report_id = f"{correlation_id}.env_mismatch"
                saved = _persist_report_md(
                    report_id=report_id,
                    md_text=render_root_cause_report_md(report=report, title="PATCHIT Report: Runtime environment required"),
                    store_dir=settings.report_store_dir,
                )
                audit.write(correlation_id, "report.saved", {"report_id": report_id, "path": saved})
            except Exception as e:  # noqa: BLE001
                audit.write(correlation_id, "report.save_failed", {"error": str(e)})

            decision = PolicyEngine(RemediationPolicy(require_human_approval=True)).evaluate_patch(
                PatchProposal(
                    patch_id="none",
                    title="escalated: runtime env mismatch",
                    rationale="runtime env mismatch: external command execution failed",
                    diff_unified="",
                    files=[],
                    confidence=0.0,
                    requires_human_approval=True,
                )
            )
            decision.allowed = False
            decision.reasons = ["escalated: env_mismatch_external_command", f"fingerprint={error_fingerprint}"]
            audit.write(correlation_id, "policy.decided", {"decision": decision.model_dump(mode="json"), "phase": "final"})
            return RemediationResult(
                event=event,
                error=err,
                patch=None,
                policy=decision,
                pr=None,
                report=report,
                audit_correlation_id=correlation_id,
            )
    except Exception:  # noqa: BLE001
        pass

    # Inspect artifacts to strengthen root-cause (and infer upstream task when downstream fails)
    artifact_summaries = summarize_artifacts(event.artifact_uris)
    audit.write(
        correlation_id,
        "artifacts.summarized",
        {"artifacts": [a.__dict__ for a in artifact_summaries]},
    )

    # Early escalation: if this fingerprint has already produced a verified PR (or multiple attempts),
    # and we still see the same downstream "missing artifact" symptom with *all* expected artifacts missing,
    # this often indicates an upstream orchestration / runtime semantics issue (ex: DAG triggered directly,
    # trigger conf not propagated, or artifacts written under a different run_id). In this case, produce a
    # human decision report rather than thrashing on more patches that cannot be validated in our sandbox.
    try:
        has_prior_verified_pr = any((a.verify_ok is True) and bool(a.pr_url) for a in prior)
        all_missing = bool(artifact_summaries) and all((not a.ok) for a in artifact_summaries[: min(6, len(artifact_summaries))])
        if has_prior_verified_pr and err.classification == "python_file_not_found" and all_missing:
            report = RootCauseReport(
                event_id=event.event_id,
                pipeline_id=event.pipeline_id,
                run_id=event.run_id,
                task_id=event.task_id,
                error_fingerprint=error_fingerprint,
                failure_summary=(
                    "Downstream task failed due to missing artifacts, but this fingerprint has already had a verified PR.\n"
                    "This strongly suggests an upstream orchestration/runtime issue (run_id/trigger conf mismatch, or triggering the downstream DAG directly)."
                ),
                category=RootCauseCategory.env_mismatch,
                recommended_next_steps=[
                    "Confirm you triggered the pipeline from the *root/ingest* DAG (not directly from a downstream DAG).",
                    "In Airflow UI, check that validate/enrich DAG runs exist for the same logical data run id (dag_run.conf['run_id']).",
                    "Inspect `/opt/airflow/data/grocery_runs/` and verify which run_id directory contains `raw/`, `staged/`, and `out/enriched.json`.",
                    "If run_id propagation is still inconsistent, update TriggerDagRunOperator templates to use only `dag_run.conf.get('run_id', dag_run.run_id)` and consider setting `trigger_run_id` explicitly.",
                ],
                timeline=[
                    AttemptSummary(
                        attempt_id=a.attempt_id,
                        patch_id=a.patch_id,
                        patch_title=a.patch_title,
                        verify_ok=a.verify_ok,
                        verify_summary=a.verify_summary,
                        pr_url=a.pr_url,
                        notes=a.notes,
                    )
                    for a in prior[:20]
                ],
                notes="PATCHIT cannot conclusively validate Airflow cross-DAG trigger semantics in the current sandbox; escalating for human decision.",
            )
            audit.write(correlation_id, "remediation.escalated", {"report": report.model_dump(mode="json")})
            try:
                report_id = f"{correlation_id}.upstream_orchestration"
                saved = _persist_report_md(
                    report_id=report_id,
                    md_text=render_root_cause_report_md(report=report, title="PATCHIT Report: Upstream orchestration required"),
                    store_dir=settings.report_store_dir,
                )
                audit.write(correlation_id, "report.saved", {"report_id": report_id, "path": saved})
            except Exception as e:  # noqa: BLE001
                audit.write(correlation_id, "report.save_failed", {"error": str(e)})
            decision = PolicyEngine(RemediationPolicy(require_human_approval=True)).evaluate_patch(
                PatchProposal(
                    patch_id="none",
                    title="escalated: upstream orchestration",
                    rationale="escalated: likely upstream/orchestration issue",
                    diff_unified="",
                    files=[],
                    confidence=0.0,
                    requires_human_approval=True,
                )
            )
            decision.allowed = False
            decision.reasons = ["escalated: upstream_orchestration", f"fingerprint={error_fingerprint}"]
            audit.write(correlation_id, "policy.decided", {"decision": decision.model_dump(mode="json"), "phase": "final"})
            return RemediationResult(
                event=event,
                error=err,
                patch=None,
                policy=decision,
                pr=None,
                report=report,
                audit_correlation_id=correlation_id,
            )
    except Exception:  # noqa: BLE001
        pass
    if err.classification == "partial_or_malformed_json" and event.task_id:
        # In our demo DAG, downstream JSON parse failures are usually caused by the writer task.
        err.likely_upstream_task_id = "write_raw_payload"
        err.likely_root_cause = (err.likely_root_cause or "") + " Likely upstream task: write_raw_payload."

    # -------- Multi-repo selection (per incident) --------
    # PATCHIT can be configured with multiple target repos via PATCHIT_REPO_REGISTRY_JSON.
    # Choose repo based on pipeline_id patterns, then ensure *all* context collection,
    # verification, local sync, and PR creation use the selected repo.
    stack_paths: list[str] = []
    try:
        stack_paths = [str(fr.file_path or "") for fr in (err.stack or []) if getattr(fr, "file_path", None)]
    except Exception:
        stack_paths = []
    repo_registry_json = runtime_cfg.get("repo_registry_json") or getattr(settings, "repo_registry_json", None)
    try:
        base_dict = settings.model_dump()
    except Exception:  # noqa: BLE001
        base_dict = {}
    from types import SimpleNamespace
    settings_proxy = SimpleNamespace(**base_dict)
    setattr(settings_proxy, "repo_registry_json", repo_registry_json)
    repo_ctx: RepoContext = select_repo_context(settings=settings_proxy, pipeline_id=event.pipeline_id, error_stack_paths=stack_paths)
    repo_root_override = runtime_cfg.get("repo_root_override")
    if repo_root_override and repo_ctx.key == "default":
        rr = str(repo_root_override)
        repo_root_dir = rr if os.path.isabs(rr) else os.path.join(os.getcwd(), rr)
        repo_ctx = RepoContext(
            key=repo_ctx.key,
            repo_root_dir=repo_root_dir,
            github_repo=repo_ctx.github_repo,
            github_base_branch=repo_ctx.github_base_branch,
            cursor_repository=repo_ctx.cursor_repository,
            cursor_ref=repo_ctx.cursor_ref,
            local_sync_repo_path=runtime_cfg.get("local_sync_repo_path") or repo_ctx.local_sync_repo_path,
            code_translate_from=repo_ctx.code_translate_from,
            code_translate_to=repo_ctx.code_translate_to,
            file_translate_from=repo_ctx.file_translate_from,
            file_translate_to=repo_ctx.file_translate_to,
        )
    repo_root_dir = repo_ctx.repo_root_dir
    try:
        audit.write(
            correlation_id,
            "repo.selected",
            {
                "repo_key": repo_ctx.key,
                "repo_root_dir": repo_root_dir,
                "github_repo": repo_ctx.github_repo,
                "github_base_branch": repo_ctx.github_base_branch,
            },
        )
    except Exception:  # noqa: BLE001
        pass

    # dbt special-case context enrichment (pipeline-agnostic):
    # dbt logs often mention failing model files (models/...sql) or compiled paths (target/run/<project>/models/...sql).
    # Those files are not stack frames, so we inject lightweight synthetic frames so CodeContextCollector will include
    # them in `context.collected`, enabling the agent to do surgical model fixes.
    try:
        # Use the full fetched log for dbt model path extraction; the reduced excerpt may omit
        # `compiled code at target/run/...` lines that we need for repo-relative file resolution.
        raw = (log_text or "") if "log_text" in locals() else (err.raw_excerpt or "")

        # Strip ANSI color codes to make string parsing reliable.
        raw_clean = re.sub(r"\x1b\[[0-9;]*m", "", raw or "")

        # Extract model paths from the log text (avoid regex brittleness on real Airflow logs).
        model_paths: list[str] = []
        _pos = 0
        while True:
            i = raw_clean.find("models/", _pos)
            if i == -1:
                break
            j = raw_clean.find(".sql", i)
            if j == -1:
                break
            p = raw_clean[i : j + 4].strip()
            if p.startswith("models/"):
                model_paths.append(p)
            _pos = j + 4

        # Extract compiled-code hints:
        # Example line: "compiled code at target/run/<dbt_project>/models/staging/foo.sql"
        compiled_refs: list[tuple[str, str]] = []
        _needle = "compiled code at "
        _pos = 0
        while True:
            i = raw_clean.find(_needle, _pos)
            if i == -1:
                break
            j = i + len(_needle)
            k = raw_clean.find("\n", j)
            token = (raw_clean[j:] if k == -1 else raw_clean[j:k]).strip()
            path = (token.split() or [""])[0]
            # Normalize to the "target/<run|compiled>/<proj>/models/..." shape.
            if path.startswith("target/"):
                parts = path.split("/")
                # parts[0]=target, parts[1]=run|compiled, parts[2]=proj, parts[3:]=rest
                if len(parts) >= 4 and parts[1] in ("run", "compiled"):
                    proj = parts[2]
                    rest = "/".join(parts[3:])
                    if proj and rest.startswith("models/"):
                        compiled_refs.append((proj, rest))
            _pos = j

        # Map dbt project name -> repo-relative directory by reading dbt_project.yml (agnostic to folder names).
        dbt_name_to_dir: dict[str, str] = {}
        try:
            import yaml  # type: ignore

            for root, dirs, files in os.walk(repo_root_dir):
                dirs[:] = [
                    d
                    for d in dirs
                    if d not in {".git", ".venv", "venv", "__pycache__", "node_modules"} and not d.startswith(".")
                ]
                if "dbt_project.yml" in files:
                    yml_path = os.path.join(root, "dbt_project.yml")
                    try:
                        data = yaml.safe_load(open(yml_path, "r", encoding="utf-8")) or {}
                        name = (data.get("name") or "").strip() if isinstance(data, dict) else ""
                        rel = os.path.relpath(root, repo_root_dir)
                        if name and rel and not rel.startswith(".."):
                            dbt_name_to_dir[name] = rel if rel != "." else ""
                    except Exception:
                        pass
                if os.path.relpath(root, repo_root_dir).count(os.sep) >= 3:
                    dirs[:] = []
        except Exception:
            dbt_name_to_dir = {}

        candidates: list[str] = []
        candidates.extend(model_paths)
        for mpath in model_paths:
            for d in dbt_name_to_dir.values():
                if d:
                    candidates.append(os.path.join(d, mpath))
        for proj_name, mpath in compiled_refs:
            if proj_name in dbt_name_to_dir and dbt_name_to_dir[proj_name]:
                candidates.append(os.path.join(dbt_name_to_dir[proj_name], mpath))
            candidates.append(os.path.join(proj_name, mpath))

        seen = set()
        existing: list[str] = []
        for p in candidates:
            if not p or not isinstance(p, str) or p in seen:
                continue
            seen.add(p)
            if os.path.exists(os.path.join(repo_root_dir, p)):
                existing.append(p)

        try:
            audit.write(
                correlation_id,
                "dbt.context_injection",
                {
                    "raw_len": len(raw or ""),
                    "raw_head": (raw or "")[:160],
                    "contains_target_run": ("target/run" in (raw or "")) or ("compiled code at" in (raw or "")),
                    "contains_models_sql": ("models/" in (raw or "")) and (".sql" in (raw or "")),
                    "model_paths": model_paths[:5],
                    "compiled_refs": compiled_refs[:5],
                    "dbt_projects": list(dbt_name_to_dir.items())[:5],
                    "existing": existing[:5],
                },
            )
        except Exception:  # noqa: BLE001
            pass

        if existing:
            from patchit.models import StackFrame

            for rp in existing[:5]:
                err.stack.append(StackFrame(file_path=rp, line_number=1, function="(dbt_model)"))
    except Exception as e:  # noqa: BLE001
        try:
            audit.write(correlation_id, "dbt.context_injection_failed", {"error": repr(e)})
        except Exception:  # noqa: BLE001
            pass

    # Collect code context (prefer repo root for clean repo-relative paths in prompts)
    collector = CodeContextCollector(
        project_root=repo_root_dir,
        translate_from=repo_ctx.code_translate_from,
        translate_to=repo_ctx.code_translate_to,
    )
    ctx = collector.collect(err.stack)

    # Add extra context files referenced by logs (not present in stack frames).
    # For dbt failures, logs often mention:
    # - source model paths like `models/staging/foo.sql`
    # - compiled paths like `target/run/<dbt_project>/models/staging/foo.sql`
    try:
        raw = err.raw_excerpt or ""

        # Extract model paths mentioned by dbt logs.
        model_paths: list[str] = []
        for m in re.findall(r"models/[A-Za-z0-9_\\-\\/\\.]+\\.sql", raw, flags=re.IGNORECASE):
            p = (m or "").strip()
            if p.startswith("models/"):
                model_paths.append(p)

        # Extract compiled-code hints: target/run/<dbt_project_name>/models/...
        compiled_refs: list[tuple[str, str]] = []
        for proj, mpath in re.findall(
            r"(?:target/(?:run|compiled))/([A-Za-z0-9_\\-]+)/((?:models)/[A-Za-z0-9_\\-\\/\\.]+\\.sql)",
            raw,
            flags=re.IGNORECASE,
        ):
            if isinstance(proj, str) and isinstance(mpath, str) and proj and mpath.startswith("models/"):
                compiled_refs.append((proj, mpath))

        # Discover dbt project directories in a repo-agnostic way and map dbt "project name" -> directory.
        # This avoids hardcoding folder names like dbt_retail/.
        dbt_name_to_dir: dict[str, str] = {}
        try:
            import yaml  # type: ignore

            for root, dirs, files in os.walk(repo_root_dir):
                dirs[:] = [d for d in dirs if d not in {".git", ".venv", "venv", "__pycache__", "node_modules"} and not d.startswith(".")]
                if "dbt_project.yml" in files:
                    yml_path = os.path.join(root, "dbt_project.yml")
                    try:
                        data = yaml.safe_load(open(yml_path, "r", encoding="utf-8")) or {}
                        name = (data.get("name") or "").strip() if isinstance(data, dict) else ""
                        rel = os.path.relpath(root, repo_root_dir)
                        if name and rel and not rel.startswith(".."):
                            dbt_name_to_dir[name] = rel if rel != "." else ""
                    except Exception:
                        pass
                if os.path.relpath(root, repo_root_dir).count(os.sep) >= 3:
                    dirs[:] = []
        except Exception:
            dbt_name_to_dir = {}

        # Build candidate repo-relative file paths and keep only ones that exist.
        candidates: list[str] = []
        # 1) If repo has a dbt project at root, models/... may exist directly.
        candidates.extend(model_paths)
        # 2) For each discovered dbt project dir, try <dir>/models/...
        for mpath in model_paths:
            for d in dbt_name_to_dir.values():
                if not d:
                    continue
                candidates.append(os.path.join(d, mpath))
        # 3) Use compiled project name mapping: <mapped_dir>/models/... (or fallback: <proj>/models/...)
        for proj_name, mpath in compiled_refs:
            if proj_name in dbt_name_to_dir and dbt_name_to_dir[proj_name]:
                candidates.append(os.path.join(dbt_name_to_dir[proj_name], mpath))
            candidates.append(os.path.join(proj_name, mpath))

        # De-dupe while preserving order + filter to existing files only.
        seen = set()
        existing: list[str] = []
        for p in candidates:
            if not p or not isinstance(p, str):
                continue
            if p in seen:
                continue
            seen.add(p)
            if os.path.exists(os.path.join(repo_root_dir, p)):
                existing.append(p)

        if existing:
            # Attach the actual dbt model file(s) so the agent can do a surgical fix.
            try:
                ctx.update(collector.collect_files(existing))
            except Exception:  # noqa: BLE001
                for rp in existing[:5]:
                    try:
                        ap = os.path.join(repo_root_dir, rp)
                        with open(ap, "r", encoding="utf-8", errors="replace") as f:
                            lines = f.read().splitlines()
                        head_n = min(len(lines), 80)
                        snippet = "\n".join(f"{i+1}: {lines[i]}" for i in range(0, head_n))
                        ctx[rp] = {"line": 1, "function": "(file)", "snippet": snippet}
                    except Exception:
                        continue
    except Exception:  # noqa: BLE001
        pass
    audit.write(correlation_id, "context.collected", {"files": list(ctx.keys())})

    # Index codebase (agent prerequisite). This is the foundation for upstream reasoning:
    # - intra-DAG edges (a >> b)
    # - cross-DAG triggers (TriggerDagRunOperator) including conf/run_id propagation
    max_files = int(getattr(settings, "index_max_files", 1200))
    if agent_mode == "groq":
        # Groq rejects overly-large prompts; cap index size aggressively.
        max_files = min(max_files, 350)
    codebase_index = build_codebase_index(repo_root_dir, max_files=max_files)
    if settings.converge_to_green:
        codebase_index["repair_goal"] = "converge_to_green"
    # Provide the agent with iteration memory: what we tried before for this fingerprint.
    # This helps it avoid repeating ineffective patches and enables "human decision" escalation.
    try:
        codebase_index["prior_attempts"] = [
            {
                "attempt_id": a.attempt_id,
                "ts_unix": a.ts_unix,
                "patch_id": a.patch_id,
                "patch_diff_sha": a.patch_diff_sha,
                "patch_title": a.patch_title,
                "patch_class": a.patch_class,
                "verify_ok": a.verify_ok,
                "verify_summary": a.verify_summary,
                "pr_url": a.pr_url,
                "outcome": a.outcome,
                "notes": a.notes,
            }
            for a in prior[:20]
        ]
    except Exception:  # noqa: BLE001
        codebase_index["prior_attempts"] = []
    # Best-effort artifact contract inference to help the agent reason about producer/consumer chains.
    try:
        codebase_index["artifact_contracts"] = infer_missing_artifact_context(
            project_root=repo_root_dir,
            pipeline_id=event.pipeline_id,
            failing_task_id=event.task_id,
            artifact_uris=event.artifact_uris,
            error_message=err.message,
        )
    except Exception:  # noqa: BLE001
        codebase_index["artifact_contracts"] = {"missing_artifacts": []}
    audit.write(correlation_id, "index.built", {"file_count_indexed": codebase_index.get("file_count_indexed")})

    # Propose patch (ALWAYS agentic if enabled)
    patch = None
    decision = None
    verifier_feedback: str | None = None
    verified_ok: bool = not settings.verify_enabled
    last_verify: dict | None = None
    local_validation_ok: bool = False
    last_agent_pipeline: dict | None = None

    agent_ran = False
    if agent_mode != "off":
        cursor_auto_create_pr = bool(
            runtime_cfg.get("cursor_auto_create_pr", getattr(settings, "cursor_auto_create_pr", False))
        )

        def _make_agent(mode: str) -> AgenticCodeEngine:
            return AgenticCodeEngine(
                mode=mode,
                agent_url=settings.agent_url,
                inhouse_agent_url=runtime_cfg.get("inhouse_agent_url") or getattr(settings, "inhouse_agent_url", None),
                inhouse_api_key=runtime_cfg.get("inhouse_api_key") or getattr(settings, "inhouse_api_key", None),
                inhouse_headers_json=runtime_cfg.get("inhouse_headers_json") or getattr(settings, "inhouse_headers_json", None),
                inhouse_timeout_s=float(getattr(settings, "inhouse_timeout_s", 60.0)),
                groq_api_key=settings.groq_api_key,
                groq_base_url=settings.groq_base_url,
                groq_model=settings.groq_model,
                openrouter_api_key=settings.openrouter_api_key,
                openrouter_base_url=settings.openrouter_base_url,
                openrouter_model=settings.openrouter_model,
                openrouter_site_url=settings.openrouter_site_url,
                openrouter_site_name=settings.openrouter_site_name,
                cursor_api_key=getattr(settings, "cursor_api_key", None),
                cursor_base_url=str(getattr(settings, "cursor_base_url", "https://api.cursor.com")),
                cursor_repository=repo_ctx.cursor_repository,
                cursor_ref=str(repo_ctx.cursor_ref or repo_ctx.github_base_branch or settings.github_base_branch),
                cursor_branch_prefix=str(getattr(settings, "cursor_branch_prefix", "patchit/cursor")),
                cursor_poll_interval_s=float(getattr(settings, "cursor_poll_interval_s", 3.0)),
                cursor_max_wait_s=float(getattr(settings, "cursor_max_wait_s", 600.0)),
                cursor_auto_create_pr=cursor_auto_create_pr,
                github_token=getattr(settings, "github_token", None),
                github_repo=repo_ctx.github_repo,
                github_base_branch=str(repo_ctx.github_base_branch or getattr(settings, "github_base_branch", "main")),
                repo_root=repo_root_dir,
                patch_format=getattr(settings, "agent_patch_format", "structured"),
                use_two_pass=False,
            )

        agent = _make_agent(agent_mode)

        # Verify-and-retry loop: only PR verified patches.
        # Cursor is a heavyweight remote agent; prefer a single attempt (it can iterate internally).
        attempts = max(1, int(settings.verify_max_attempts or 1)) if settings.verify_enabled else 1
        attempts = min(attempts, int(getattr(settings, "agent_max_attempts_per_event", 3) or 3))
        if agent_mode == "cursor":
            attempts = 1
        for attempt in range(1, attempts + 1):
            agent_ran = True
            audit.write(
                correlation_id,
                "agent.attempt_started",
                {
                    "attempt": attempt,
                    "has_verifier_feedback": bool(verifier_feedback),
                    "verifier_feedback_chars": (len(verifier_feedback) if verifier_feedback else 0),
                },
            )
            try:
                # Guardrail: sometimes LLM calls can hang (network/socket). We emit heartbeats and enforce a timeout,
                # so the system retries rather than getting stuck at agent.attempt_started forever.
                stop = threading.Event()
                started = time.monotonic()

                def _heartbeat() -> None:
                    while not stop.is_set():
                        try:
                            audit.write(
                                correlation_id,
                                "agent.heartbeat",
                                {
                                    "attempt": attempt,
                                    "elapsed_s": round(time.monotonic() - started, 2),
                                    "stage": "llm_call",
                                },
                            )
                        except Exception:
                            pass
                        stop.wait(float(getattr(settings, "agent_heartbeat_interval_s", 10.0)))

                hb = threading.Thread(target=_heartbeat, name="patchit-agent-heartbeat", daemon=True)
                hb.start()
                try:
                    # IMPORTANT: do not use a `with ThreadPoolExecutor()` here: on timeout the context manager
                    # waits for the worker thread to finish, which defeats the timeout.
                    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)

                    def _run_agent() -> PatchProposal | None:
                        nonlocal last_agent_pipeline
                        # Cursor/bytez modes remain diff-returning agents; methodology pipeline is implemented for chat providers.
                        if agent_mode not in ("groq", "openrouter"):
                            return agent.propose_patch(
                                event=event,
                                error=err,
                                code_context=ctx,
                                artifact_summaries=[a.__dict__ for a in artifact_summaries],
                                codebase_index=codebase_index,
                                verifier_feedback=verifier_feedback,
                                repair_goal="converge_to_green" if settings.converge_to_green else "remediate_failure",
                            )

                        # --- Full prompt methodology pipeline (RCA -> Fix -> Validate -> Decide) ---
                        try:
                            pb = mem.get_prompt_playbook(error_fingerprint=error_fingerprint)
                        except Exception:
                            pb = {}
                        audit.write(
                            correlation_id,
                            "prompt.pipeline.started",
                            {"attempt": attempt, "provider": agent_mode, "model": agent_cfg.get("model"), "playbook": pb},
                        )
                        pp_cfg = PromptPipelineConfig(
                            provider=("groq" if agent_mode == "groq" else "openrouter"),
                            model=(settings.groq_model if agent_mode == "groq" else settings.openrouter_model),
                            timeout_s=min(75.0, float(getattr(settings, "agent_attempt_timeout_s", 90.0))),
                            groq_api_key=settings.groq_api_key,
                            groq_base_url=settings.groq_base_url,
                            openrouter_api_key=settings.openrouter_api_key,
                            openrouter_base_url=settings.openrouter_base_url,
                            openrouter_site_url=settings.openrouter_site_url,
                            openrouter_site_name=settings.openrouter_site_name,
                        )
                        pipeline_payload = {
                            "event": event.model_dump(mode="json"),
                            "normalized_error": err.model_dump(mode="json"),
                            "code_context": ctx,
                            "artifact_summaries": [a.__dict__ for a in artifact_summaries],
                            "codebase_index": codebase_index,
                            "verifier_feedback": verifier_feedback,
                            "repair_goal": ("converge_to_green" if settings.converge_to_green else "remediate_failure"),
                        }
                        trace = run_prompt_pipeline(pp_cfg, payload=pipeline_payload, playbook=pb)
                        last_agent_pipeline = trace.model_dump(mode="json")
                        rca_out, fix_out, val_out, dec_out = parse_typed_outputs(trace)

                        if rca_out:
                            audit.write(correlation_id, "prompt.rca", {"attempt": attempt, "rca": rca_out.model_dump(mode="json")})
                        if fix_out:
                            audit.write(correlation_id, "prompt.fix", {"attempt": attempt, "fix": fix_out.model_dump(mode="json")})
                        if val_out:
                            audit.write(correlation_id, "prompt.validate", {"attempt": attempt, "validate": val_out.model_dump(mode="json")})
                        if dec_out:
                            audit.write(correlation_id, "prompt.decide", {"attempt": attempt, "decide": dec_out.model_dump(mode="json")})
                        audit.write(
                            correlation_id,
                            "prompt.pipeline.completed",
                            {
                                "attempt": attempt,
                                "selected": {k: {"variant_id": v.variant_id, "modality": v.modality, "score": v.score} for k, v in trace.selected.items()},
                            },
                        )

                        if not fix_out or fix_out.decision != "propose_patch" or not fix_out.edits:
                            try:
                                pb2 = mem.get_prompt_playbook(error_fingerprint=error_fingerprint)
                                pb2["last_fix_decision"] = (fix_out.decision if fix_out else "missing")
                                pb2["last_refusal_reason"] = (fix_out.refusal_reason if fix_out else "missing_fix_output")
                                mem.upsert_prompt_playbook(error_fingerprint=error_fingerprint, playbook=pb2)
                            except Exception:
                                pass
                            return None

                        from patchit.code_engine.agentic import (  # noqa: PLC0415
                            _canonicalize_diff_paths,
                            _extract_files_from_diff,
                            _sanitize_unified_diff,
                            _unified_diff_for_update,
                        )

                        diffs: list[str] = []
                        for e in fix_out.edits[:8]:
                            pth = (e.path or "").strip()
                            if pth.startswith("repo/"):
                                pth = pth[len("repo/") :]
                            pth = pth.lstrip("/")
                            fp = os.path.join(repo_root_dir, pth)
                            if not pth or (not os.path.exists(fp)):
                                continue
                            try:
                                with open(fp, "r", encoding="utf-8", errors="replace") as f:
                                    oldc = f.read()
                                diffs.append(_unified_diff_for_update(path=pth, old=oldc, new=e.new_content))
                            except Exception:
                                continue
                        diff_text = ("\n".join([d for d in diffs if d.strip()]).strip() + "\n") if diffs else ""
                        diff_text = _canonicalize_diff_paths(_sanitize_unified_diff(diff_text))
                        files = _extract_files_from_diff(diff_text) if diff_text else []
                        if not diff_text or not files:
                            return None

                        rationale = "Generated by PATCHIT prompt methodology pipeline (RCA->Fix->Validate->Decide)."
                        try:
                            if rca_out:
                                rationale += "\nRCA_JSON:" + json.dumps(rca_out.model_dump(mode='json'), separators=(',', ':'))
                            if val_out:
                                rationale += "\nVALIDATE_JSON:" + json.dumps(val_out.model_dump(mode='json'), separators=(',', ':'))
                            if dec_out:
                                rationale += "\nDECIDE_JSON:" + json.dumps(dec_out.model_dump(mode='json'), separators=(',', ':'))
                        except Exception:
                            pass

                        return PatchProposal(
                            patch_id=uuid.uuid4().hex,
                            title=f"PATCHIT agentic fix (pipeline): {err.error_type}",
                            rationale=rationale,
                            diff_unified=diff_text,
                            files=files,
                            tests_to_run=list(fix_out.tests_to_run or []),
                            confidence=float(fix_out.confidence),
                            requires_human_approval=True,
                        )

                    fut = ex.submit(_run_agent)
                    timeout_s = float(getattr(settings, "agent_attempt_timeout_s", 90.0))
                    # Cursor Cloud Agent can take a few minutes; align attempt timeout with cursor_max_wait_s.
                    if agent_mode == "cursor":
                        timeout_s = max(timeout_s, float(getattr(settings, "cursor_max_wait_s", 600.0)) + 30.0)
                    patch = fut.result(timeout=timeout_s)
                except concurrent.futures.TimeoutError:
                    patch = None
                    verifier_feedback = (
                        (verifier_feedback or "")
                        + "\nLLM call timed out. Produce a smaller patch and ensure diff hunks are valid."
                    ).strip()
                    audit.write(
                        correlation_id,
                        "agent.failed",
                        {
                            "error": f"agent_timeout_{getattr(settings, 'agent_attempt_timeout_s', 90.0)}s",
                            "attempt": attempt,
                        },
                    )
                    # Agentic provider failover (still LLM): if OpenRouter is slow/unreliable, try Groq for subsequent attempts.
                    if (
                        getattr(settings, "agent_fallback_to_groq", True)
                        and agent_mode == "openrouter"
                        and settings.groq_api_key
                    ):
                        try:
                            agent = _make_agent("groq")
                            audit.write(
                                correlation_id,
                                "agent.fallback",
                                {"from": "openrouter", "to": "groq", "reason": "timeout", "attempt": attempt},
                            )
                            verifier_feedback = (
                                verifier_feedback
                                + "\nSwitched provider to Groq for next attempt due to OpenRouter timeout."
                            ).strip()
                        except Exception:
                            pass
                finally:
                    # Avoid blocking on a stuck LLM call thread.
                    try:
                        ex.shutdown(wait=False, cancel_futures=True)  # type: ignore[misc]
                    except Exception:
                        pass
                    stop.set()
                    try:
                        hb.join(timeout=0.2)
                    except Exception:
                        pass
                audit.write(correlation_id, "agent.patch_proposed", {"ok": bool(patch), "attempt": attempt})
            except Exception as e:  # noqa: BLE001
                audit.write(correlation_id, "agent.failed", {"error": str(e), "attempt": attempt})
                patch = None

            if not patch:
                # If the agent couldn't produce a patch, still allow retries by giving it explicit feedback.
                audit.write(
                    correlation_id,
                    "agent.failed",
                    {
                        "error": "no_patch_returned",
                        "attempt": attempt,
                        "provider": agent.mode if hasattr(agent, "mode") else agent_mode,
                    },
                )

                # Agentic provider failover (still LLM): if OpenRouter returns no patch (often formatting/JSON issues),
                # try Groq for subsequent attempts instead of immediately escalating.
                if (
                    getattr(settings, "agent_fallback_to_groq", True)
                    and agent_mode == "openrouter"
                    and settings.groq_api_key
                    and getattr(agent, "mode", agent_mode) == "openrouter"
                ):
                    try:
                        agent = _make_agent("groq")
                        audit.write(
                            correlation_id,
                            "agent.fallback",
                            {"from": "openrouter", "to": "groq", "reason": "no_patch_returned", "attempt": attempt},
                        )
                        verifier_feedback = (
                            (verifier_feedback or "")
                            + "\nOpenRouter produced no patch. Switching provider to Groq; return a structured JSON patch with correct file paths and complete content."
                        ).strip()
                    except Exception:
                        pass

                if settings.verify_enabled and attempt < attempts:
                    verifier_feedback = "Agent did not produce a valid unified diff. Return a COMPLETE unified diff with file headers (---/+++), at least one @@ hunk, and real +/− lines."
                    continue
                break

            patch_class: str | None = None

            # Memory: block exact-repeat patches for the same fingerprint.
            diff_sha = hashlib.sha256((patch.diff_unified or "").encode("utf-8", errors="replace")).hexdigest()
            if mem.seen_patch_diff_sha(error_fingerprint=error_fingerprint, patch_diff_sha=diff_sha):
                audit.write(
                    correlation_id,
                    "patch.rejected",
                    {"reason": "repeat_patch", "attempt": attempt, "patch_id": patch.patch_id},
                )
                mem.record_attempt(
                    attempt_id=f"{correlation_id}:{attempt}",
                    correlation_id=correlation_id,
                    event=event.model_dump(mode="json"),
                    error=err.model_dump(mode="json"),
                    patch=patch.model_dump(mode="json"),
                    verify=None,
                    pr=None,
                    outcome=None,
                    notes="Rejected: exact repeat diff for same fingerprint.",
                )
                patch = None
                verifier_feedback = "You proposed the same patch as a previous attempt. Propose a materially different approach, or return an InvestigationReport-style explanation if this is upstream/infra/data."
                continue

            # Coarser de-dupe: if the exact same patch diff was already proposed for this pipeline,
            # do not keep generating redundant PRs (even if the fingerprint drifted).
            if mem.seen_patch_diff_sha_for_pipeline(pipeline_id=event.pipeline_id, patch_diff_sha=diff_sha):
                existing_pr = None
                try:
                    existing_pr = mem.pr_url_for_pipeline_patch_diff(pipeline_id=event.pipeline_id, patch_diff_sha=diff_sha)
                except Exception:
                    existing_pr = None
                audit.write(
                    correlation_id,
                    "patch.rejected",
                    {"reason": "repeat_patch_pipeline", "attempt": attempt, "patch_id": patch.patch_id},
                )
                mem.record_attempt(
                    attempt_id=f"{correlation_id}:{attempt}",
                    correlation_id=correlation_id,
                    event=event.model_dump(mode="json"),
                    error=err.model_dump(mode="json"),
                    patch=patch.model_dump(mode="json"),
                    verify=None,
                    pr=({"pr_url": existing_pr} if existing_pr else None),
                    outcome=None,
                    notes="Rejected: exact repeat diff already proposed for this pipeline (merge-awareness guard).",
                    patch_class=patch_class,
                )
                if existing_pr:
                    # Do not pretend this is "no patch available" — it's a duplicate-suppression case.
                    report = RootCauseReport(
                        event_id=event.event_id,
                        pipeline_id=event.pipeline_id,
                        run_id=event.run_id,
                        task_id=event.task_id,
                        error_fingerprint=error_fingerprint,
                        failure_summary=(
                            "PATCHIT suppressed creating a duplicate PR because the exact same patch was already proposed for this pipeline."
                        ),
                        category=RootCauseCategory.unknown,
                        recommended_next_steps=[
                            f"Review/merge the existing PR: {existing_pr}",
                            "If the PR is merged but failures continue, rebuild/restart the runtime so the fix is deployed.",
                            "If this is a different root cause, improve fingerprinting inputs (include asset/lineage keys) so PATCHIT can distinguish incidents.",
                        ],
                        notes="duplicate_guard repeat_patch_pipeline",
                    )
                    audit.write(correlation_id, "remediation.escalated", {"report": report.model_dump(mode="json")})
                    try:
                        report_id = f"{correlation_id}.duplicate_pr_guard"
                        saved = _persist_report_md(
                            report_id=report_id,
                            md_text=render_root_cause_report_md(
                                report=report,
                                title="PATCHIT Report: Duplicate patch suppressed (existing PR)",
                            ),
                            store_dir=settings.report_store_dir,
                        )
                        audit.write(correlation_id, "report.saved", {"report_id": report_id, "path": saved})
                    except Exception as e:  # noqa: BLE001
                        audit.write(correlation_id, "report.save_failed", {"error": str(e)})

                    decision = PolicyEngine(RemediationPolicy(require_human_approval=True)).evaluate_patch(
                        PatchProposal(
                            patch_id="none",
                            title="refused: duplicate_pr_exists",
                            rationale=f"refused: duplicate_pr_exists ({existing_pr})",
                            diff_unified="",
                            files=[],
                            confidence=0.0,
                            requires_human_approval=True,
                        )
                    )
                    decision.allowed = False
                    decision.reasons = [
                        "duplicate_pr_exists",
                        f"pipeline_id={event.pipeline_id}",
                        f"pr={existing_pr}",
                    ]
                    audit.write(correlation_id, "policy.decided", {"decision": decision.model_dump(mode="json"), "phase": "final"})

                    try:
                        payload = build_evidence_payload(
                            correlation_id=correlation_id,
                            event=event,
                            error=err,
                            policy=decision,
                            patch=None,
                            pr={"pr_url": existing_pr},
                            report=report,
                            agent_mode=str(agent_mode),
                            agent_model=agent_cfg.get("model") if isinstance(agent_cfg, dict) else None,
                            artifact_summaries=artifact_summaries,
                            code_context=ctx,
                            agent_pipeline=last_agent_pipeline,
                        )
                        ev = persist_evidence_pack(
                            store_dir=settings.evidence_store_dir, correlation_id=correlation_id, payload=payload
                        )
                        audit.write(correlation_id, "evidence.saved", {"path": ev.path})
                    except Exception as e:  # noqa: BLE001
                        audit.write(correlation_id, "evidence.save_failed", {"error": str(e)})

                    return RemediationResult(
                        event=event,
                        error=err,
                        patch=None,
                        policy=decision,
                        pr=None,
                        report=report,
                        audit_correlation_id=correlation_id,
                    )

                patch = None
                verifier_feedback = (
                    "This exact patch was already proposed previously for this pipeline, but the PR link was not found in memory. "
                    "Propose a DIFFERENT remediation or escalate with an investigation report."
                )
                continue

            # Patch critic: reject low-value patches early (ex: logging-only for missing artifact).
            critic = should_reject_patch(error=err, patch=patch)
            patch_class = critic.patch_class
            if not critic.ok:
                audit.write(
                    correlation_id,
                    "patch.rejected",
                    {"reason": critic.reason, "attempt": attempt, "patch_id": patch.patch_id, "patch_class": critic.patch_class},
                )
                mem.record_attempt(
                    attempt_id=f"{correlation_id}:{attempt}",
                    correlation_id=correlation_id,
                    event=event.model_dump(mode="json"),
                    error=err.model_dump(mode="json"),
                    patch=patch.model_dump(mode="json"),
                    verify=None,
                    pr=None,
                    outcome=None,
                    notes=f"Rejected by critic: {critic.reason}",
                    patch_class=critic.patch_class,
                )
                patch = None
                verifier_feedback = critic.feedback_for_agent or "Patch rejected by critic. Propose a different fix."
                continue

            # Iteration memory: avoid repeating ineffective patch classes for the same fingerprint.
            if patch_class and mem.seen_patch_class(error_fingerprint=error_fingerprint, patch_class=patch_class):
                audit.write(
                    correlation_id,
                    "patch.rejected",
                    {"reason": "repeat_patch_class", "attempt": attempt, "patch_id": patch.patch_id, "patch_class": patch_class},
                )
                mem.record_attempt(
                    attempt_id=f"{correlation_id}:{attempt}",
                    correlation_id=correlation_id,
                    event=event.model_dump(mode="json"),
                    error=err.model_dump(mode="json"),
                    patch=patch.model_dump(mode="json"),
                    verify=None,
                    pr=None,
                    outcome=None,
                    notes=f"Rejected: repeat patch class for same fingerprint ({patch_class})",
                    patch_class=patch_class,
                )
                patch = None
                verifier_feedback = (
                    "You repeated a previously-attempted (ineffective) patch strategy for this failure.\n"
                    "For missing artifacts: fix the upstream producer and make the artifact write atomic (temp file + rename), "
                    "or implement explicit skip/quarantine branching.\n"
                    "Do NOT submit log-only changes."
                )
                continue

            # Enforce policy before spending cycles verifying un-allowlisted changes.
            policy = RemediationPolicy(require_human_approval=True)
            decision = PolicyEngine(
                policy,
                repo_root=repo_root_dir,
                require_tracked_files=bool(getattr(settings, "local_sync_enabled", False)),
            ).evaluate_patch(patch)
            audit.write(
                correlation_id,
                "policy.decided",
                {"decision": decision.model_dump(mode="json"), "attempt": attempt, "phase": "pre_verify"},
            )
            if not decision.allowed:
                # For some policy failures we want to stop immediately (hard guardrails),
                # but for "not tracked in git" we can retry: it's usually a poor patch choice
                # (touching untracked/generated files) rather than a safety violation.
                if any(r.startswith("not tracked in git") for r in (decision.reasons or [])):
                    verifier_feedback = (
                        "Your patch touches files that are NOT tracked in git on the base branch.\n"
                        "PATCHIT applies patches in a git worktree and cannot patch untracked files (git apply fails with 'No such file or directory').\n"
                        "Do NOT modify untracked/generated/runtime files. Propose a patch only to git-tracked source files.\n"
                        "If the missing file is actually intended source code, ask the human to `git add` + commit it first."
                    )
                    audit.write(
                        correlation_id,
                        "patch.rejected",
                        {"reason": "not_tracked_in_git", "attempt": attempt, "patch_id": patch.patch_id},
                    )
                    patch = None
                    decision = None
                    continue

                # Blocked by hard guardrails; do not attempt verification or retries.
                patch = patch
                break

            if settings.verify_enabled:
                audit.write(correlation_id, "verify.started", {"attempt": attempt})
                audit.write(
                    correlation_id,
                    "patch.candidate",
                    {
                        "attempt": attempt,
                        "patch_id": patch.patch_id,
                        "files": [f.path for f in patch.files],
                        "diff_bytes": len((patch.diff_unified or "").encode("utf-8")),
                        "diff_head": (patch.diff_unified or "")[:600],
                        "diff_tail": (patch.diff_unified or "")[-600:],
                    },
                )
                # Orchestration changes (Airflow DAGs / triggers) require runtime-faithful verification.
                # Without this, the verifier can return ok=true for patches that only "look right" but still fail in Airflow.
                touched = [f.path for f in patch.files]
                orchestration_change = any(
                    p.startswith("airflow/dags/") or p.startswith("airflow_extra_dags/") or p.startswith("repo/airflow/dags/") or p.startswith("repo/airflow_extra_dags/")
                    for p in touched
                )
                rt_required = bool(settings.runtime_verify_required)

                # Orchestration changes MUST be validated in a host-visible worktree (docker/airflow runtime fidelity).
                # Otherwise, PATCHIT will produce "plausible but wrong" PRs that pass compileall but fail at runtime.
                local_required = bool(getattr(settings, "local_validate_required_for_orchestration_changes", True)) and orchestration_change
                if local_required and (not settings.local_sync_enabled or not settings.local_validate_cmd):
                    audit.write(
                        correlation_id,
                        "verify.finished",
                        {
                            "ok": False,
                            "summary": "local_validation_required_missing",
                            "attempted_commands": [],
                            "attempt": attempt,
                            "output_tail": (
                                "This patch touches orchestration code (Airflow DAGs / triggers).\n"
                                "PATCHIT requires runtime-faithful validation in a host-visible worktree before opening a PR.\n"
                                "Configure:\n"
                                "  - PATCHIT_LOCAL_SYNC_ENABLED=true\n"
                                "  - PATCHIT_LOCAL_VALIDATE_CMD='...'\n"
                                "Then re-run the failing task."
                            ),
                        },
                    )
                    verifier_feedback = (
                        "Verifier blocked: orchestration change requires local worktree runtime validation.\n"
                        "Do NOT propose DAG run_id/XCom/TriggerDagRunOperator changes unless you can also provide a local_validate_cmd "
                        "that proves the pipeline succeeds in real runtime.\n"
                        "If runtime validation can't be configured, escalate with a RootCauseReport instead of a patch."
                    )
                    patch = None
                    decision = None
                    continue

                def _on_verify_event(name: str, payload: Dict[str, Any]) -> None:
                    # Attach attempt number for UI grouping.
                    payload = dict(payload or {})
                    payload.setdefault("attempt", attempt)
                    audit.write(correlation_id, name, payload)

                # If the upstream run provides a scenario (common in our chaos/test DAGs), pass it through
                # to dbt verification so sandbox validation matches the runtime branch.
                dbt_vars_json: str | None = None
                try:
                    sc = (event.metadata or {}).get("scenario")
                    if isinstance(sc, str) and sc.strip():
                        dbt_vars_json = json.dumps({"scenario": sc.strip()})
                except Exception:  # noqa: BLE001
                    dbt_vars_json = None

                try:
                    v = SandboxVerifier(repo_root=repo_root_dir, timeout_s=settings.verify_timeout_s).verify(
                        patch=patch,
                        artifact_uris=event.artifact_uris,
                        event=event.model_dump(mode="json"),
                        error=err.model_dump(mode="json"),
                        dbt_vars_json=dbt_vars_json,
                        replay_enabled=settings.verify_replay_enabled,
                        converge_to_green=settings.converge_to_green,
                        runtime_verify_cmd=settings.runtime_verify_cmd,
                        runtime_verify_required=rt_required,
                        orchestration_runtime_required=orchestration_change,
                        verify_cmd_rules_json=getattr(settings, "verify_cmd_rules_json", None),
                        verify_extra_pythonpaths_json=getattr(settings, "verify_extra_pythonpaths_json", None),
                        on_event=_on_verify_event if bool(getattr(settings, "verifier_emit_command_events", True)) else None,
                    )
                    audit.write(
                        correlation_id,
                        "verify.finished",
                        {
                            "ok": v.ok,
                            "summary": v.summary,
                            "attempted_commands": v.attempted_commands,
                            "attempt": attempt,
                            # Include a small tail for fast debugging in the UI/audit log.
                            "output_tail": (v.output or "")[-4000:],
                        },
                    )
                except Exception as e:  # noqa: BLE001 (service boundary)
                    # Never let verifier exceptions crash the remediation flow. Always surface them in audit/UI.
                    v = None
                    msg = f"{type(e).__name__}: {e}"
                    audit.write(
                        correlation_id,
                        "verify.finished",
                        {
                            "ok": False,
                            "summary": "verify_exception",
                            "attempted_commands": [],
                            "attempt": attempt,
                            "output_tail": msg,
                        },
                    )
                    verifier_feedback = (verifier_feedback or "") + "\nVerifier exception: " + msg
                    patch = None
                    decision = None
                    continue
                # Record attempt outcome into persistent memory.
                mem.record_attempt(
                    attempt_id=f"{correlation_id}:{attempt}",
                    correlation_id=correlation_id,
                    event=event.model_dump(mode="json"),
                    error=err.model_dump(mode="json"),
                    patch=patch.model_dump(mode="json"),
                    verify={
                        "ok": v.ok if v else False,
                        "summary": v.summary if v else "verify_exception",
                        "attempted_commands": v.attempted_commands if v else [],
                    },
                    pr=None,
                    outcome=None,
                    notes=None,
                    patch_class=patch_class,
                )
                if v and v.ok:
                    # If this patch touches orchestration, enforce Option A local runtime validation BEFORE we stop retrying.
                    # Local validation failures should be fed back into the agent so it can iterate until green.
                    if local_required and settings.local_sync_enabled and settings.local_validate_cmd:
                        repo_path = repo_ctx.local_sync_repo_path or os.path.join(os.getcwd(), settings.repo_root)
                        wt_root = settings.local_sync_worktree_root
                        try:
                            res = apply_patch_to_worktree(
                                repo_path=repo_path,
                                worktree_root=wt_root,
                                patch_id=patch.patch_id,
                                diff_text=patch.diff_unified,
                                base_ref=str(repo_ctx.github_base_branch or settings.github_base_branch or "HEAD"),
                                validate_cmd=settings.local_validate_cmd,
                                validate_timeout_s=float(getattr(settings, "local_validate_timeout_s", 420.0)),
                                pipeline_id=event.pipeline_id,
                                task_id=event.task_id,
                            )
                            audit.write(
                                correlation_id,
                                "local_sync.finished",
                                {
                                    "ok": res.ok,
                                    "worktree_path": res.worktree_path,
                                    "branch": res.branch,
                                    "output_tail": (res.output or "")[-3000:],
                                    "attempt": attempt,
                                },
                            )
                            if not res.ok:
                                # Emit a downloadable analysis report immediately (even while the agent keeps iterating),
                                # so users can see why Option A runtime validation failed + what to do next.
                                try:
                                    report_id = f"{correlation_id}.attempt{attempt}.local_validation_failed"

                                    # Persist the FULL local validation output so the report can be meaningful.
                                    log_path = _persist_report_text(
                                        report_id=report_id,
                                        text=res.output or "",
                                        store_dir=settings.report_store_dir,
                                        ext="log",
                                    )

                                    # Agent-generated report content (pipeline-agnostic).
                                    reporter = ReportAgent(
                                        mode=_effective_report_agent_mode(settings),
                                        groq_api_key=settings.groq_api_key,
                                        groq_base_url=settings.groq_base_url,
                                        groq_model=settings.groq_model,
                                        openrouter_api_key=settings.openrouter_api_key,
                                        openrouter_base_url=settings.openrouter_base_url,
                                        openrouter_model=settings.openrouter_model,
                                        openrouter_site_url=settings.openrouter_site_url,
                                        openrouter_site_name=settings.openrouter_site_name,
                                        timeout_s=min(90.0, float(settings.agent_attempt_timeout_s)),
                                    )

                                    # Provide a compact view of prior attempts to the agent.
                                    prior_for_agent: list[dict[str, Any]] = []
                                    for a in prior[:8]:
                                        prior_for_agent.append(
                                            {
                                                "attempt_id": a.attempt_id,
                                                "patch_id": a.patch_id,
                                                "patch_title": a.patch_title,
                                                "verify_ok": a.verify_ok,
                                                "verify_summary": a.verify_summary,
                                                "notes": a.notes,
                                            }
                                        )

                                    report = reporter.generate(
                                        event=event,
                                        error_fingerprint=error_fingerprint,
                                        error=err,
                                        artifact_summaries=artifact_summaries,
                                        codebase_index=codebase_index,
                                        local_validation_output=res.output or "",
                                        local_validation_output_path=log_path,
                                        patch_id=patch.patch_id,
                                        patch_title=patch.title,
                                        patch_files=[fc.path for fc in patch.files],
                                        attempt=attempt,
                                        prior_attempts=prior_for_agent,
                                    )
                                    report.timeline = [
                                        AttemptSummary(
                                            attempt_id=a.attempt_id,
                                            patch_id=a.patch_id,
                                            patch_title=a.patch_title,
                                            verify_ok=a.verify_ok,
                                            verify_summary=a.verify_summary,
                                            pr_url=a.pr_url,
                                            notes=a.notes,
                                        )
                                        for a in prior[:20]
                                    ]

                                    audit.write(
                                        correlation_id,
                                        "remediation.escalated",
                                        {
                                            "report": report.model_dump(mode="json"),
                                            "attempt": attempt,
                                            "report_id": report_id,
                                            "report_log_path": log_path,
                                        },
                                    )

                                    rp = _persist_report_md(
                                        report_id=report_id,
                                        md_text=render_root_cause_report_md(
                                            report=report,
                                            title="PATCHIT Report: Local validation failed (agent)",
                                        ),
                                        store_dir=settings.report_store_dir,
                                    )
                                    audit.write(
                                        correlation_id,
                                        "report.saved",
                                        {"report_id": report_id, "path": rp, "attempt": attempt, "log_path": log_path},
                                    )
                                except Exception as e:  # noqa: BLE001
                                    audit.write(
                                        correlation_id,
                                        "report.save_failed",
                                        {"error": str(e), "attempt": attempt},
                                    )

                                verifier_feedback = (
                                    "Local runtime validation (Option A) FAILED.\n"
                                    "You must propose a different patch and re-run validation until the DAG succeeds.\n\n"
                                    "== local_validation_output_tail ==\n"
                                    + ((res.output or "")[-3000:])
                                ).strip()
                                patch = None
                                decision = None
                                continue
                            local_validation_ok = True
                        except Exception as e:  # noqa: BLE001
                            audit.write(
                                correlation_id,
                                "local_sync.failed",
                                {"error": str(e), "attempt": attempt},
                            )
                            verifier_feedback = (
                                "Local runtime validation (Option A) crashed with an exception.\n"
                                f"{type(e).__name__}: {e}"
                            ).strip()
                            patch = None
                            decision = None
                            continue

                    verified_ok = True
                    break
                if v and v.summary == "patch_already_applied":
                    # Strong signal: fix is already in the codebase, so a repeat PR would be pure noise.
                    last_verify = {
                        "attempt": attempt,
                        "patch_id": patch.patch_id,
                        "summary": v.summary,
                        "output_tail": (v.output or "")[-1200:],
                    }
                    verifier_feedback = (
                        "Verifier indicates the proposed patch is ALREADY PRESENT in the current codebase.\n"
                        "Do NOT propose the same fix again.\n"
                        "Instead, explain why the failure still occurs (e.g., runtime not using latest code, "
                        "wrong DAG executed, upstream task never produced artifacts, env mismatch) "
                        "and propose a different remediation, or escalate with a RootCauseReport."
                    )
                    patch = None
                    decision = None
                    continue
                last_verify = {
                    "attempt": attempt,
                    "patch_id": patch.patch_id,
                    "summary": (v.summary if v else "verify_exception"),
                    "output_tail": ((v.output or "") if v else "")[-1200:],
                }
                verifier_feedback = (v.output if v else verifier_feedback)
                patch = None
                decision = None
                continue
            else:
                break

    # Deterministic fallback is optional. For "agent-only" behavior, set:
    #   PATCHIT_ALLOW_DETERMINISTIC_FALLBACK=false
    allow_fallback = bool(settings.allow_deterministic_fallback)
    if not settings.verify_enabled:
        # Never allow deterministic fallback without verification.
        allow_fallback = False

    # In converge-to-green, we strongly prefer agent-only unless explicitly allowed.
    if settings.converge_to_green and not settings.allow_deterministic_fallback:
        allow_fallback = False

    if patch is None and allow_fallback:
        engine = CodeEngine(project_root=os.getcwd())
        patch = engine.propose_patch(
            error=err,
            code_context=ctx,
            artifacts=event.artifact_uris,
            pipeline_id=event.pipeline_id,
            converge_to_green=settings.converge_to_green,
        )

    # If agent ran but could not produce a patch (timeouts/quota/etc), emit an agent-generated report
    # instead of the unhelpful "no matching template". (Only after deterministic fallback also fails.)
    if patch is None and agent_ran and not allow_fallback:
        try:
            report_id = f"{correlation_id}.agent_failed"
            reporter = ReportAgent(
                mode=_effective_report_agent_mode(settings),
                groq_api_key=settings.groq_api_key,
                groq_base_url=settings.groq_base_url,
                groq_model=settings.groq_model,
                openrouter_api_key=settings.openrouter_api_key,
                openrouter_base_url=settings.openrouter_base_url,
                openrouter_model=settings.openrouter_model,
                openrouter_site_url=settings.openrouter_site_url,
                openrouter_site_name=settings.openrouter_site_name,
                timeout_s=min(60.0, float(settings.agent_attempt_timeout_s)),
            )
            report = reporter.generate(
                event=event,
                error_fingerprint=error_fingerprint,
                error=err,
                artifact_summaries=artifact_summaries,
                codebase_index=codebase_index,
                local_validation_output=None,
                local_validation_output_path=None,
                patch_id=None,
                patch_title=None,
                patch_files=[],
                attempt=0,
                prior_attempts=[],
            )
            audit.write(correlation_id, "remediation.escalated", {"report": report.model_dump(mode="json")})
            rp = _persist_report_md(
                report_id=report_id,
                md_text=render_root_cause_report_md(report=report, title="PATCHIT Report: Agent could not produce a patch"),
                store_dir=settings.report_store_dir,
            )
            audit.write(correlation_id, "report.saved", {"report_id": report_id, "path": rp})
        except Exception as e:  # noqa: BLE001
            audit.write(correlation_id, "report.save_failed", {"error": str(e)})
    if patch:
        audit.write(correlation_id, "patch.proposed", {"patch": patch.model_dump(mode="json")})
    else:
        audit.write(correlation_id, "patch.none", {"reason": "no matching template"})

    # Enforce policy (fallback path, or if agent loop produced patch without a decision)
    if decision is None:
        policy = RemediationPolicy(require_human_approval=True)
        policy_engine = PolicyEngine(
            policy,
            repo_root=repo_root_dir,
            require_tracked_files=bool(getattr(settings, "local_sync_enabled", False)),
        )
        if patch:
            decision = policy_engine.evaluate_patch(patch)
        else:
            decision = policy_engine.evaluate_patch(
            PatchProposal(  # minimal placeholder for policy decision only; no PR will be created without a patch
                patch_id="none",
                title="no patch",
                rationale="no verified patch available",
                diff_unified="",
                files=[],
                confidence=0.0,
                requires_human_approval=True,
            )
            )
            decision.allowed = False
            if last_verify:
                decision.reasons = [
                    f"no verified patch after {int(settings.verify_max_attempts or 1)} attempts",
                    f"last_summary={last_verify.get('summary')}",
                    f"last_patch_id={last_verify.get('patch_id')}",
                ]
            else:
                decision.reasons = ["no patch available"]

        audit.write(correlation_id, "policy.decided", {"decision": decision.model_dump(mode="json"), "phase": "final"})

        # Evidence pack (non-PR outcomes): persist proof bundle even when no patch/PR is produced.
        # This is critical for drills that expect "fix_or_refuse" or "refuse" to always produce evidence.
        if (patch is None) or (decision and not decision.allowed):
            try:
                payload = build_evidence_payload(
                    correlation_id=correlation_id,
                    event=event,
                    error=err,
                    policy=decision,
                    patch=patch,
                    pr=None,
                    report=report,
                    agent_mode=str(agent_mode),
                    agent_model=agent_cfg.get("model") if isinstance(agent_cfg, dict) else None,
                    artifact_summaries=artifact_summaries,
                    code_context=ctx,
                    agent_pipeline=last_agent_pipeline,
                )
                ev = persist_evidence_pack(store_dir=settings.evidence_store_dir, correlation_id=correlation_id, payload=payload)
                audit.write(correlation_id, "evidence.saved", {"path": ev.path})
            except Exception as e:  # noqa: BLE001
                audit.write(correlation_id, "evidence.save_failed", {"error": str(e)})

    # If verification tells us the "fix" is already applied, stop spamming PRs and provide an investigation report.
    if (patch is None) and last_verify and last_verify.get("summary") == "patch_already_applied":
        report = RootCauseReport(
            event_id=event.event_id,
            pipeline_id=event.pipeline_id,
            run_id=event.run_id,
            task_id=event.task_id,
            error_fingerprint=error_fingerprint,
            failure_summary=(
                "PATCHIT detected that the proposed fix is already present in the current codebase, "
                "but the pipeline is still failing. Creating another PR would be redundant."
            ),
            category=RootCauseCategory.env_mismatch,
            recommended_next_steps=[
                "Confirm the runtime is executing the latest merged code (rebuild/restart Airflow scheduler+webserver containers).",
                "If you ran a downstream DAG manually, pass the upstream data run_id in dag_run.conf; otherwise artifacts will be missing.",
                "Check upstream DAG/task logs: verify the producer created the expected artifacts in the expected run_dir.",
                "If runtime code is correct and artifacts are still missing, investigate source/API/DB availability and data validity.",
            ],
            timeline=[
                AttemptSummary(
                    attempt_id=a.attempt_id,
                    patch_id=a.patch_id,
                    patch_title=a.patch_title,
                    verify_ok=a.verify_ok,
                    verify_summary=a.verify_summary,
                    pr_url=a.pr_url,
                    notes=a.notes,
                )
                for a in prior[:20]
            ],
            notes="Merge-awareness guard triggered: patch_already_applied. Root cause is likely upstream/orchestration/runtime-code mismatch.",
        )
        audit.write(correlation_id, "remediation.escalated", {"report": report.model_dump(mode="json")})
        try:
            os.makedirs(settings.report_store_dir, exist_ok=True)
            report_id = f"{correlation_id}.already_applied"
            rp = os.path.join(settings.report_store_dir, f"{report_id}.md")
            with open(rp, "w", encoding="utf-8") as f:
                f.write(render_root_cause_report_md(report=report, title="PATCHIT Report: Fix already merged/applied"))
            audit.write(correlation_id, "report.saved", {"report_id": report_id, "path": rp})
        except Exception as e:  # noqa: BLE001
            audit.write(correlation_id, "report.save_failed", {"error": str(e)})
        return RemediationResult(
            event=event,
            error=err,
            patch=None,
            policy=decision,
            pr=None,
            report=report,
            audit_correlation_id=correlation_id,
        )

    pr = None
    last_pr_error: str | None = None
    last_local_validation: dict | None = None
    last_local_sync: dict | None = None
    if patch and decision.allowed and not verified_ok and settings.verify_enabled:
        # Enforce verify-before-PR globally (including fallback/non-agent patches).
        audit.write(correlation_id, "verify.started", {"attempt": 0, "phase": "post_policy"})
        audit.write(
            correlation_id,
            "patch.candidate",
            {
                "attempt": 0,
                "patch_id": patch.patch_id,
                "files": [f.path for f in patch.files],
                "diff_bytes": len((patch.diff_unified or "").encode("utf-8")),
                "diff_head": (patch.diff_unified or "")[:600],
                "diff_tail": (patch.diff_unified or "")[-600:],
            },
        )

        touched = [f.path for f in patch.files]
        orchestration_change = any(
            p.startswith("airflow/dags/")
            or p.startswith("airflow_extra_dags/")
            or p.startswith("repo/airflow/dags/")
            or p.startswith("repo/airflow_extra_dags/")
            for p in touched
        )
        rt_required = bool(settings.runtime_verify_required)

        def _on_verify_event_post(name: str, payload: Dict[str, Any]) -> None:
            payload = dict(payload or {})
            payload.setdefault("attempt", 0)
            payload.setdefault("phase", "post_policy")
            audit.write(correlation_id, name, payload)

        v = SandboxVerifier(repo_root=repo_root_dir, timeout_s=settings.verify_timeout_s).verify(
            patch=patch,
            artifact_uris=event.artifact_uris,
            event=event.model_dump(mode="json"),
            error=err.model_dump(mode="json"),
            replay_enabled=settings.verify_replay_enabled,
            converge_to_green=settings.converge_to_green,
            runtime_verify_cmd=settings.runtime_verify_cmd,
            runtime_verify_required=rt_required,
            orchestration_runtime_required=orchestration_change,
            verify_cmd_rules_json=getattr(settings, "verify_cmd_rules_json", None),
            verify_extra_pythonpaths_json=getattr(settings, "verify_extra_pythonpaths_json", None),
            on_event=_on_verify_event_post if bool(getattr(settings, "verifier_emit_command_events", True)) else None,
        )
        audit.write(
            correlation_id,
            "verify.finished",
            {
                "ok": v.ok,
                "summary": v.summary,
                "attempted_commands": v.attempted_commands,
                "attempt": 0,
                "output_tail": (v.output or "")[-4000:],
            },
        )
        verified_ok = v.ok
        if not v.ok:
            # Block PR creation if not verified.
            decision.allowed = False
            decision.reasons = ["patch not verified"]
            audit.write(
                correlation_id,
                "policy.decided",
                {"decision": decision.model_dump(mode="json"), "phase": "final"},
            )

    if patch and decision.allowed and (verified_ok or not settings.verify_enabled):
        local_sync_enabled = bool(runtime_cfg.get("local_sync_enabled", settings.local_sync_enabled))
        local_sync_repo_path = runtime_cfg.get("local_sync_repo_path") or repo_ctx.local_sync_repo_path
        local_sync_worktree_root = runtime_cfg.get("local_sync_worktree_root") or settings.local_sync_worktree_root
        local_validate_cmd = runtime_cfg.get("local_validate_cmd") or settings.local_validate_cmd
        # Persist the exact diff so the user can apply/test locally before (or in parallel with) reviewing the PR.
        try:
            saved_path = _persist_patch_diff(
                patch_id=patch.patch_id,
                diff_text=patch.diff_unified,
                store_dir=settings.patch_store_dir,
            )
            audit.write(correlation_id, "patch.saved", {"path": saved_path, "patch_id": patch.patch_id})
        except Exception as e:  # noqa: BLE001
            audit.write(correlation_id, "patch.save_failed", {"error": str(e), "patch_id": patch.patch_id})

        # Local sync: apply the verified patch to a local git worktree so the user can run it immediately
        # without merging/pulling. Best-effort unless local_validate_cmd is set (then it can gate PR).
        # If we already ran local validation successfully inside the agent loop (required for orchestration changes),
        # don't re-run it here; avoid doubling runtime.
        if local_sync_enabled and not local_validation_ok:
            repo_path = local_sync_repo_path or os.path.join(os.getcwd(), settings.repo_root)
            wt_root = local_sync_worktree_root
            touched = [f.path for f in (patch.files or [])]
            orchestration_change = any(
                p.startswith("airflow/dags/")
                or p.startswith("airflow_extra_dags/")
                or p.startswith("repo/airflow/dags/")
                or p.startswith("repo/airflow_extra_dags/")
                for p in touched
            )
            # Gate PRs on local runtime validation:
            # - orchestration changes (Airflow DAG edits) via local_validate_cmd
            # - Spark repo patches via spark_local_validate_cmd (Approach 1: spark-submit in local)
            validate_cmd = None
            if bool(getattr(settings, "local_validate_required_for_orchestration_changes", True)) and orchestration_change:
                validate_cmd = local_validate_cmd
            else:
                # Spark job code changes: optionally run spark-submit locally (docker-based) before PR.
                # This keeps demo validation realistic without requiring the Airflow runtime itself to have Spark installed.
                spark_cmd = getattr(settings, "spark_local_validate_cmd", None)
                if spark_cmd:
                    spark_touched = any(
                        p.startswith("src/") or p.startswith("tests/spark/") or ("/enterprise_spark/" in p) or p.startswith("enterprise_spark/")
                        for p in touched
                    )
                    if spark_touched:
                        validate_cmd = spark_cmd
            try:
                res = apply_patch_to_worktree(
                    repo_path=repo_path,
                    worktree_root=wt_root,
                    patch_id=patch.patch_id,
                    diff_text=patch.diff_unified,
                    base_ref=str(repo_ctx.github_base_branch or settings.github_base_branch or "HEAD"),
                    validate_cmd=validate_cmd,
                    validate_timeout_s=(
                        float(getattr(settings, "local_validate_timeout_s", 420.0))
                        if validate_cmd != getattr(settings, "spark_local_validate_cmd", None)
                        else float(getattr(settings, "spark_local_validate_timeout_s", 900.0))
                    ),
                    pipeline_id=event.pipeline_id,
                    task_id=event.task_id,
                )
                audit.write(
                    correlation_id,
                    "local_sync.finished",
                    {
                        "ok": res.ok,
                        "worktree_path": res.worktree_path,
                        "branch": res.branch,
                        "stage": getattr(res, "stage", None),
                        "returncode": getattr(res, "returncode", None),
                        "output_tail": (res.output or "")[-3000:],
                    },
                )
                last_local_validation = {
                    "ok": res.ok,
                    "worktree_path": res.worktree_path,
                    "branch": res.branch,
                    "patch_id": patch.patch_id,
                    "stage": getattr(res, "stage", None),
                    "returncode": getattr(res, "returncode", None),
                    "output_tail": (res.output or "")[-3000:],
                }
                last_local_sync = {
                    "ok": res.ok,
                    "worktree_path": res.worktree_path,
                    "branch": res.branch,
                    "stage": getattr(res, "stage", None),
                    "returncode": getattr(res, "returncode", None),
                }
                if not res.ok:
                    # Only block PR creation if the failure happened during the validation command itself
                    # (Option A runtime validation). If git worktree/apply failed, that's a local-sync error
                    # and should be reported accurately (not mislabeled as "local validation").
                    if validate_cmd and getattr(res, "stage", None) in ("local_validate_cmd", "local_validate_timeout"):
                        decision.allowed = False
                        decision.reasons = ["local_validation_failed"]
                    elif validate_cmd:
                        decision.allowed = False
                        decision.reasons = ["local_sync_failed"]
            except Exception as e:  # noqa: BLE001
                audit.write(correlation_id, "local_sync.failed", {"error": str(e)})

        pr_target = runtime_cfg.get("pr_target") or "github"
        if pr_target == "local":
            audit.write(correlation_id, "pr.skipped", {"reason": "local_only"})
        # Create PR (real or mock). Never let this throw a 500: failures should be visible in audit/UI.
        if pr_target != "local":
            try:
                if not decision.allowed:
                    # Provide more accurate cause for downstream reporting.
                    if decision.reasons and "local_sync_failed" in decision.reasons:
                        raise RuntimeError("PR blocked: local sync failed")
                    raise RuntimeError("PR blocked: local validation failed")

                # If Cursor is configured to auto-create PRs, prefer adopting that PR (post-verify)
                # instead of creating a duplicate PR from PATCHIT.
                cursor_auto_create_pr = bool(
                    runtime_cfg.get("cursor_auto_create_pr", getattr(settings, "cursor_auto_create_pr", False))
                )
                if agent_mode == "cursor" and cursor_auto_create_pr and pr is None and patch and (patch.rationale or ""):
                    try:
                        import json as _json

                        m = re.search(r"CURSOR_META:(\{.*\})\s*$", patch.rationale.strip(), flags=re.DOTALL)
                        meta = _json.loads(m.group(1)) if m else {}
                        pr_url = meta.get("pr_url") if isinstance(meta, dict) else None
                        branch = meta.get("branch") if isinstance(meta, dict) else None
                        if isinstance(pr_url, str) and pr_url:
                            pr_num = 0
                            try:
                                mm = re.search(r"/pull/(\d+)", pr_url)
                                if mm:
                                    pr_num = int(mm.group(1))
                            except Exception:
                                pr_num = 0
                            pr = PullRequestResult(
                                mode="real",
                                pr_number=pr_num,
                                pr_title=patch.title,
                                pr_url=pr_url,
                                branch_name=str(branch or ""),
                            )
                            audit.write(correlation_id, "cursor.pr.adopted", {"pr": pr.model_dump(mode="json"), "meta": meta})
                            mem.record_attempt(
                                attempt_id=f"{correlation_id}:pr",
                                correlation_id=correlation_id,
                                event=event.model_dump(mode="json"),
                                error=err.model_dump(mode="json"),
                                patch=patch.model_dump(mode="json") if patch else None,
                                verify={"ok": True, "summary": "verified"} if settings.verify_enabled else None,
                                pr=pr.model_dump(mode="json"),
                                outcome="unknown",
                                notes="Cursor created PR; PATCHIT verified patch and adopted PR link.",
                                patch_class=None,
                            )
                            audit.write(correlation_id, "pr.created", {"pr": pr.model_dump(mode="json"), "source": "cursor"})
                    except Exception as e:  # noqa: BLE001
                        audit.write(correlation_id, "cursor.pr.adopt_failed", {"error": str(e)})

                if pr is None:
                    # Repo-specific PR target (multi-repo): override github_repo/base_branch for this incident.
                    try:
                        base_dict = settings.model_dump()
                    except Exception:  # noqa: BLE001
                        base_dict = {}
                    from types import SimpleNamespace

                    eff = SimpleNamespace(**base_dict)
                    setattr(eff, "github_repo", repo_ctx.github_repo or getattr(settings, "github_repo", None))
                    setattr(eff, "github_base_branch", str(repo_ctx.github_base_branch or getattr(settings, "github_base_branch", "main")))
                    setattr(eff, "repo_root", repo_root_dir)
                    pr = PullRequestCreator(eff).create(
                        title=patch.title,
                        body=_render_pr_body(event, err, patch, ctx, artifact_summaries),
                        patch=patch,
                    )
                    audit.write(correlation_id, "pr.created", {"pr": pr.model_dump(mode="json")})
                    # Update memory with PR link.
                    mem.record_attempt(
                        attempt_id=f"{correlation_id}:pr",
                        correlation_id=correlation_id,
                        event=event.model_dump(mode="json"),
                        error=err.model_dump(mode="json"),
                        patch=patch.model_dump(mode="json") if patch else None,
                        verify={"ok": True, "summary": "verified"} if settings.verify_enabled else None,
                        pr=pr.model_dump(mode="json"),
                        outcome="unknown",
                        notes="PR created after verification.",
                        patch_class=None,
                    )

                    # Evidence pack (post-PR): persist a structured proof bundle to disk for audits.
                    try:
                        payload = build_evidence_payload(
                            correlation_id=correlation_id,
                            event=event,
                            error=err,
                            policy=decision,
                            patch=patch,
                            pr=pr,
                            report=None,
                            agent_mode=str(agent_mode),
                            agent_model=agent_cfg.get("model") if isinstance(agent_cfg, dict) else None,
                            artifact_summaries=artifact_summaries,
                            code_context=ctx,
                            agent_pipeline=last_agent_pipeline,
                        )
                        ev = persist_evidence_pack(store_dir=settings.evidence_store_dir, correlation_id=correlation_id, payload=payload)
                        audit.write(correlation_id, "evidence.saved", {"path": ev.path})
                    except Exception as e:  # noqa: BLE001
                        audit.write(correlation_id, "evidence.save_failed", {"error": str(e)})
            except httpx.HTTPStatusError as e:
                last_pr_error = f"http_status_error: {e}"
                status = getattr(e.response, "status_code", None)
                body = None
                try:
                    body = e.response.text
                except Exception:
                    body = None
                audit.write(
                    correlation_id,
                    "pr.create_failed",
                    {
                        "error": last_pr_error,
                        "status": status,
                        "body": (body or "")[:2000],
                    },
                )
            except Exception as e:  # noqa: BLE001
                last_pr_error = f"{type(e).__name__}: {e}"
                audit.write(
                    correlation_id,
                    "pr.create_failed",
                    {"error": last_pr_error},
                )
        if pr is None and pr_target == "local":
            try:
                payload = build_evidence_payload(
                    correlation_id=correlation_id,
                    event=event,
                    error=err,
                    policy=decision,
                    patch=patch,
                    pr=None,
                    report=None,
                    agent_mode=str(agent_mode),
                    agent_model=agent_cfg.get("model") if isinstance(agent_cfg, dict) else None,
                    artifact_summaries=artifact_summaries,
                    code_context=ctx,
                    agent_pipeline=last_agent_pipeline,
                )
                ev = persist_evidence_pack(store_dir=settings.evidence_store_dir, correlation_id=correlation_id, payload=payload)
                audit.write(correlation_id, "evidence.saved", {"path": ev.path})
            except Exception as e:  # noqa: BLE001
                audit.write(correlation_id, "evidence.save_failed", {"error": str(e)})

    # If PR creation is blocked by Option A runtime validation or local sync, emit a downloadable report
    # with concrete next steps and a clear root cause.
    if pr is None and last_pr_error and ("local validation failed" in last_pr_error or "local sync failed" in last_pr_error):
        notes = [f"Underlying error: {last_pr_error}"]
        if last_local_validation:
            notes.append("")
            notes.append("== Local validation details ==")
            notes.append(f"- patch_id: {last_local_validation.get('patch_id')}")
            notes.append(f"- worktree_path: {last_local_validation.get('worktree_path')}")
            notes.append(f"- branch: {last_local_validation.get('branch')}")
            if last_local_validation.get("stage"):
                notes.append(f"- stage: {last_local_validation.get('stage')}")
            if last_local_validation.get("returncode") is not None:
                notes.append(f"- returncode: {last_local_validation.get('returncode')}")
            notes.append("")
            notes.append("== local_validation_output_tail ==")
            notes.append(str(last_local_validation.get("output_tail") or "").strip())

        report = RootCauseReport(
            event_id=event.event_id,
            pipeline_id=event.pipeline_id,
            run_id=event.run_id,
            task_id=event.task_id,
            error_fingerprint=error_fingerprint,
            failure_summary=(
                "PATCHIT verified a patch in the sandbox, but PR creation was blocked because local runtime validation "
                "or local sync failed. This prevents shipping an unproven (or unapplied) fix."
            ),
            category=RootCauseCategory.env_mismatch if "local sync failed" in last_pr_error else RootCauseCategory.code_bug,
            recommended_next_steps=[
                "Open the PATCHIT UI and download the patch diff (`patch.saved`) and this report (`report.saved`).",
                "Inspect the output tail in this report; it contains the concrete error.",
                "If the failure is `git apply` with 'No such file or directory', the patch likely targets files that are NOT tracked in git on the base branch (worktrees only include tracked files). Commit those files first or adjust repository structure.",
                "If the failure is an Airflow task failure during validation, the patch must address the true upstream root cause, then re-run to let PATCHIT iterate until green.",
            ],
            timeline=[
                AttemptSummary(
                    attempt_id=a.attempt_id,
                    patch_id=a.patch_id,
                    patch_title=a.patch_title,
                    verify_ok=a.verify_ok,
                    verify_summary=a.verify_summary,
                    pr_url=a.pr_url,
                    notes=a.notes,
                )
                for a in prior[:20]
            ],
            notes="\n".join([n for n in notes if n is not None]).strip(),
        )
        audit.write(correlation_id, "remediation.escalated", {"report": report.model_dump(mode="json")})
        try:
            os.makedirs(settings.report_store_dir, exist_ok=True)
            report_id = f"{correlation_id}.local_validation_failed" if "local validation failed" in last_pr_error else f"{correlation_id}.local_sync_failed"
            rp = os.path.join(settings.report_store_dir, f"{report_id}.md")
            with open(rp, "w", encoding="utf-8") as f:
                f.write(render_root_cause_report_md(report=report, title="PATCHIT Report: Local validation failed (PR blocked)"))
            audit.write(correlation_id, "report.saved", {"report_id": report_id, "path": rp})
        except Exception as e:  # noqa: BLE001
            audit.write(correlation_id, "report.save_failed", {"error": str(e)})
        return RemediationResult(
            event=event,
            error=err,
            patch=patch,
            policy=decision,
            pr=None,
            report=report,
            audit_correlation_id=correlation_id,
        )

    # If PR creation is blocked due to missing GitHub configuration, emit a downloadable report
    # (so the user can understand why PATCHIT can't proceed without human/environment changes).
    if pr is None and settings.github_mode == "real" and last_pr_error and (
        "PATCHIT_GITHUB_TOKEN" in last_pr_error or "PATCHIT_GITHUB_REPO" in last_pr_error
    ):
        report = RootCauseReport(
            event_id=event.event_id,
            pipeline_id=event.pipeline_id,
            run_id=event.run_id,
            task_id=event.task_id,
            error_fingerprint=error_fingerprint,
            failure_summary="Patch was verified, but PATCHIT could not create a real GitHub PR due to missing GitHub configuration.",
            category=RootCauseCategory.env_mismatch,
            recommended_next_steps=[
                "Set PATCHIT_GITHUB_TOKEN and PATCHIT_GITHUB_REPO in the PATCHIT container environment (compose .env), then restart PATCHIT.",
                "Re-run the failing task to re-trigger PATCHIT once PR creation is enabled.",
                "Download the verified patch diff from the UI and apply it manually while GitHub is unavailable.",
            ],
            timeline=[
                AttemptSummary(
                    attempt_id=a.attempt_id,
                    patch_id=a.patch_id,
                    patch_title=a.patch_title,
                    verify_ok=a.verify_ok,
                    verify_summary=a.verify_summary,
                    pr_url=a.pr_url,
                    notes=a.notes,
                )
                for a in prior[:20]
            ],
            notes=f"Underlying error: {last_pr_error}",
        )
        audit.write(correlation_id, "remediation.escalated", {"report": report.model_dump(mode="json")})
        try:
            os.makedirs(settings.report_store_dir, exist_ok=True)
            report_id = f"{correlation_id}.github_config"
            rp = os.path.join(settings.report_store_dir, f"{report_id}.md")
            with open(rp, "w", encoding="utf-8") as f:
                f.write(render_root_cause_report_md(report=report, title="PATCHIT Report: PR creation blocked"))
            audit.write(correlation_id, "report.saved", {"report_id": report_id, "path": rp})
        except Exception as e:  # noqa: BLE001
            audit.write(correlation_id, "report.save_failed", {"error": str(e)})
        return RemediationResult(
            event=event,
            error=err,
            patch=patch,
            policy=decision,
            pr=None,
            report=report,
            audit_correlation_id=correlation_id,
        )

    return RemediationResult(
        event=event,
        error=err,
        patch=patch,
        policy=decision,
        pr=pr,
        report=None,
        audit_correlation_id=correlation_id,
    )


def _render_pr_body(
    event: UniversalFailureEvent,
    err: NormalizedErrorObject,
    patch,
    ctx: Dict[str, Dict[str, object]],
    artifact_summaries,
) -> str:
    lines = []
    lines.append("## PATCHIT Auto-Remediation Proposal (Phase-1 MVP)")
    lines.append("")
    lines.append("### Failure event")
    lines.append(f"- platform: `{event.platform}`")
    lines.append(f"- pipeline: `{event.pipeline_id}`")
    lines.append(f"- run_id: `{event.run_id}`")
    lines.append(f"- task: `{event.task_id}`")
    lines.append(f"- log_uri: `{event.log_uri}`")
    lines.append("")
    lines.append("### Normalized error")
    lines.append(f"- type: `{err.error_type}`")
    lines.append(f"- message: `{err.message}`")
    lines.append(f"- classification: `{err.classification}` (confidence={err.confidence})")
    if err.likely_upstream_task_id:
        lines.append(f"- likely_upstream_task_id: `{err.likely_upstream_task_id}`")
    lines.append("")
    if artifact_summaries:
        lines.append("### Artifacts (summaries)")
        for a in artifact_summaries[:10]:
            lines.append(f"- `{a.uri}`: {a.kind} ({'ok' if a.ok else 'bad'}) — {a.summary}")
        lines.append("")
    if ctx:
        lines.append("### Code context (snippets)")
        for fp, meta in ctx.items():
            lines.append(f"- `{fp}` @ line {meta.get('line')}")
    lines.append("")
    lines.append("### Proposed patch rationale")
    lines.append(patch.rationale)
    lines.append("")
    lines.append("### Safety")
    lines.append(
        "- This PR was created by PATCHIT after sandbox verification. "
        "In production, keep strict path allowlists and require human approval for high-risk changes."
    )
    return "\n".join(lines)



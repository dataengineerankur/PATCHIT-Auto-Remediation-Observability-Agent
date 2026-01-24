from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx

from patchit.adapters.airflow_adapter import AirflowAdapter
from patchit.models import UniversalFailureEvent
from patchit.telemetry.audit import AuditLogger


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_mkdir(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass


def _read_json(path: str) -> dict:
    try:
        if not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _write_json(path: str, obj: dict) -> None:
    try:
        _safe_mkdir(os.path.dirname(path) or ".")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f)
    except Exception:
        pass


@dataclass(frozen=True)
class AirflowPollerConfig:
    base_url: str
    username: str
    password: str
    timeout_s: float
    interval_s: float
    lookback_minutes: int
    max_dags: int
    max_new_events_per_cycle: int
    logs_base: str
    data_base: str
    runtime_config_path: str
    audit_log_path: str
    # Where PATCHIT is listening inside the container
    ingest_url: str = "http://127.0.0.1:8088/events/ingest"
    # Timeout for POSTing to PATCHIT ingest endpoint (seconds).
    ingest_timeout_s: float = 15.0


class AirflowFailurePoller:
    """
    Polls Airflow REST API for failed task instances and submits UniversalFailureEvents to PATCHIT.

    Key goal: keep pipelines/DAGs completely PATCHIT-agnostic (no on_failure_callback required).
    """

    def __init__(self, cfg: AirflowPollerConfig) -> None:
        self.cfg = cfg
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

        # State is persisted so restarts don’t re-ingest everything.
        self._state_path = os.path.join(os.path.dirname(cfg.runtime_config_path) or "var/config", "airflow_poller_state.json")

        # Backoff / circuit breaker state for noisy network failures (Airflow webserver restart, overload, etc).
        self._backoff_s: float = max(1.0, float(cfg.interval_s))
        self._backoff_max_s: float = max(30.0, 10.0 * float(cfg.interval_s))
        self._last_error_sig: str | None = None
        self._last_error_ts: float = 0.0
        self._last_backoff_emitted_s: float = 0.0
        self._last_backoff_emit_ts: float = 0.0
        # Submission error dedupe (avoid spamming if PATCHIT ingest is temporarily unhealthy).
        self._last_submit_err_sig: str | None = None
        self._last_submit_err_ts: float = 0.0

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="patchit-airflow-poller", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _load_state(self) -> dict:
        st = _read_json(self._state_path)
        if "seen" not in st:
            st["seen"] = []
        if "last_poll_ts" not in st:
            st["last_poll_ts"] = None
        return st

    def _save_state(self, st: dict) -> None:
        # Keep state bounded
        seen = st.get("seen") or []
        if isinstance(seen, list) and len(seen) > 4000:
            st["seen"] = seen[-2500:]
        _write_json(self._state_path, st)

    def _auth_header(self) -> Dict[str, str]:
        # reuse AirflowAdapter's basic auth helper
        return AirflowAdapter(self.cfg.base_url, self.cfg.username, self.cfg.password)._auth_header()

    def _list_dags(self) -> List[str]:
        url = f"{self.cfg.base_url.rstrip('/')}/api/v1/dags?limit={int(self.cfg.max_dags)}&only_active=true"
        with httpx.Client(timeout=float(self.cfg.timeout_s)) as client:
            r = client.get(url, headers=self._auth_header())
            r.raise_for_status()
            data = r.json() or {}
        dags = data.get("dags", []) or []
        out: list[str] = []
        for d in dags:
            dag_id = (d or {}).get("dag_id")
            if isinstance(dag_id, str) and dag_id:
                out.append(dag_id)
        return out

    def _list_recent_runs(self, dag_id: str, *, since: datetime) -> List[Dict[str, Any]]:
        # Try to fetch recent runs. Airflow supports query params but may vary; keep best-effort.
        # We'll just take the latest N and then filter by logical_date/execution_date fields.
        # NOTE: some Airflow builds return dagRuns in ascending order; a small limit can exclude the newest runs.
        # Use a larger limit and sort client-side to ensure we see recent failures.
        # Airflow often enforces a maximum page size (commonly 100). Requesting more can increase load
        # and may be capped server-side anyway.
        # Prefer server-side ordering by newest first when supported; this avoids missing recent runs when the
        # server returns ascending order and applies the limit before sorting.
        base = f"{self.cfg.base_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns?limit=100"
        # Airflow's REST API restricts which fields may be used for ordering; `logical_date` is often disallowed.
        # `execution_date` (or `start_date`) is typically allowed and provides stable newest-first ordering.
        url = base + "&order_by=-execution_date"
        with httpx.Client(timeout=float(self.cfg.timeout_s)) as client:
            r = client.get(url, headers=self._auth_header())
            if r.status_code == 400:
                # Some Airflow deployments don't support order_by; fall back to best-effort list.
                r = client.get(base, headers=self._auth_header())
            r.raise_for_status()
            data = r.json() or {}
        runs = data.get("dag_runs", []) or []
        out: list[dict] = []
        for dr in runs:
            if not isinstance(dr, dict):
                continue
            # Prefer 'logical_date' or 'execution_date' if present.
            ts = dr.get("logical_date") or dr.get("execution_date") or dr.get("start_date")
            if isinstance(ts, str) and ts:
                try:
                    # ISO strings from Airflow are usually timezone-aware.
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    if dt >= since:
                        out.append(dr)
                except Exception:
                    out.append(dr)
            else:
                out.append(dr)
        # Process most recent first (helps quickly find fresh failures).
        def _dt(dr: dict) -> datetime:
            ts = dr.get("logical_date") or dr.get("execution_date") or dr.get("start_date")
            if isinstance(ts, str) and ts:
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except Exception:
                    return datetime.min.replace(tzinfo=timezone.utc)
            return datetime.min.replace(tzinfo=timezone.utc)

        out.sort(key=_dt, reverse=True)
        return out

    def _log_uri_for(self, *, dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
        base = self.cfg.logs_base.rstrip("/")
        return f"file://{base}/dag_id={dag_id}/run_id={run_id}/task_id={task_id}/attempt={try_number}.log"

    def _build_event(self, *, dag_id: str, run_id: str, task_id: str, try_number: int, metadata: Dict[str, Any]) -> UniversalFailureEvent:
        adapter = AirflowAdapter(self.cfg.base_url, self.cfg.username, self.cfg.password, timeout_s=float(self.cfg.timeout_s))
        return adapter.build_failure_event(
            dag_id=dag_id,
            dag_run_id=run_id,
            task_id=task_id,
            try_number=try_number,
            log_uri=self._log_uri_for(dag_id=dag_id, run_id=run_id, task_id=task_id, try_number=try_number),
            artifact_uris=[],
            metadata=metadata,
        )

    def _submit_event(self, *, event: UniversalFailureEvent) -> bool:
        # Fire-and-forget: don't block the poller on long agent/verification loops.
        # We use a bounded timeout; even if we time out client-side, the server may continue processing.
        # However, if ingestion is failing, we MUST surface it (otherwise the UI looks “stuck”).
        try:
            with httpx.Client(timeout=float(self.cfg.ingest_timeout_s or 15.0)) as client:
                client.post(self.cfg.ingest_url, json=event.model_dump(mode="json"))
            return True
        except Exception as e:  # noqa: BLE001
            try:
                audit = AuditLogger(self.cfg.audit_log_path)
                corr = "poller"
                err_sig = f"{type(e).__name__}: {e}"
                now_s = time.monotonic()
                if (self._last_submit_err_sig != err_sig) or (now_s - self._last_submit_err_ts > 60.0):
                    audit.write(
                        corr,
                        "poller.ingest_failed",
                        {"error": err_sig, "ingest_url": self.cfg.ingest_url, "event_id": event.event_id},
                    )
                    self._last_submit_err_sig = err_sig
                    self._last_submit_err_ts = now_s
            except Exception:
                pass
            return False

    def _run(self) -> None:
        audit = AuditLogger(self.cfg.audit_log_path)
        corr = "poller"
        st = self._load_state()

        # Start window:
        # - use last_poll_ts if present, but ALWAYS subtract a safety overlap so restarts don't miss failures
        #   that landed between the last poll and the process restart.
        # - else fall back to a configurable lookback.
        now0 = _utc_now()
        lookback = timedelta(minutes=int(self.cfg.lookback_minutes))
        since = now0 - lookback
        if isinstance(st.get("last_poll_ts"), str) and st["last_poll_ts"]:
            try:
                last = datetime.fromisoformat(st["last_poll_ts"].replace("Z", "+00:00"))
                if last.tzinfo is None:
                    last = last.replace(tzinfo=timezone.utc)
                overlap = timedelta(seconds=max(60.0, float(self.cfg.interval_s) * 4.0))
                # IMPORTANT: use a bounded lookback on restart even if last_poll_ts exists.
                # Otherwise, if PATCHIT was down for longer than `overlap`, we can permanently miss failures.
                # Choose the MORE RECENT (later) of:
                # - last poll time (minus overlap)
                # - now minus lookback window
                # so we don't accidentally rewind farther back than the configured lookback.
                since = max(last - overlap, now0 - lookback)
                # Never start in the future.
                if since > now0:
                    since = now0 - overlap
            except Exception:
                pass

        audit.write(
            corr,
            "poller.started",
            {
                "type": "airflow",
                "base_url": self.cfg.base_url,
                "since": since.isoformat(),
                "interval_s": float(self.cfg.interval_s),
            },
        )

        while not self._stop.is_set():
            try:
                dags = self._list_dags()
                now = _utc_now()
                newly_seen: int = 0
                for dag_id in dags:
                    runs = self._list_recent_runs(dag_id, since=since)
                    for dr in runs:
                        run_id = dr.get("dag_run_id") or dr.get("run_id")
                        if not isinstance(run_id, str) or not run_id:
                            continue

                        # Only care about FAILED dag runs. This keeps poller load low and prevents
                        # Airflow API timeouts under pressure (common with small demo stacks).
                        state = str(dr.get("state") or "").lower()
                        if state != "failed":
                            continue

                        # List task instances and pick the first failed/upstream_failed.
                        adapter = AirflowAdapter(self.cfg.base_url, self.cfg.username, self.cfg.password, timeout_s=float(self.cfg.timeout_s))
                        try:
                            # Enrich with dag_run.conf so downstream remediation can replay the same branch
                            # (e.g., scenario-driven dbt vars) without requiring any DAG modifications.
                            dr_full: dict = {}
                            try:
                                dr_full = adapter.get_dag_run(dag_id, run_id) or {}
                            except Exception:
                                dr_full = {}
                            tis = adapter.list_failed_task_instances(dag_id, run_id)
                        except Exception:
                            continue

                        for ti in tis:
                            # Avoid upstream_failed noise: it often has no log file.
                            if str(ti.get("state") or "").lower() != "failed":
                                continue
                            task_id = ti.get("task_id")
                            if not isinstance(task_id, str) or not task_id:
                                continue
                            try_number = ti.get("try_number") or ti.get("try_number") or ti.get("try_number")
                            if not isinstance(try_number, int):
                                # Airflow sometimes returns as str
                                try:
                                    try_number = int(try_number)
                                except Exception:
                                    try_number = 1
                            try_number = max(1, int(try_number or 1))
                            ev_id = f"airflow:{dag_id}:{run_id}:{task_id}:{try_number}"
                            seen = st.get("seen") or []
                            if ev_id in seen:
                                continue

                            meta = {
                                "poller": True,
                                "airflow_state": ti.get("state"),
                                "airflow_dag_run_state": dr.get("state"),
                                "detected_at": now.isoformat(),
                                "exception": ti.get("note") or ti.get("state") or "",
                            }
                            if isinstance(dr_full, dict) and dr_full:
                                conf = dr_full.get("conf")
                                if isinstance(conf, dict) and conf:
                                    meta["airflow_conf"] = conf
                                    # Convenience: hoist scenario to the top level if present.
                                    sc = conf.get("scenario")
                                    if isinstance(sc, str) and sc.strip():
                                        meta["scenario"] = sc.strip()
                            event = self._build_event(dag_id=dag_id, run_id=run_id, task_id=task_id, try_number=try_number, metadata=meta)
                            audit.write(corr, "poller.event_discovered", {"event_id": event.event_id, "pipeline_id": dag_id, "run_id": run_id, "task_id": task_id, "try_number": try_number})
                            ok = self._submit_event(event=event)
                            if ok:
                                # Only mark as seen if submission succeeded. If ingest fails/times out,
                                # retry on the next cycle instead of permanently dropping the event.
                                st["seen"] = (seen + [ev_id]) if isinstance(seen, list) else [ev_id]
                                newly_seen += 1
                            # Prevent “backfill storms” on reset/restart by capping how many brand-new
                            # events we ingest per cycle.
                            if newly_seen >= max(1, int(self.cfg.max_new_events_per_cycle or 1)):
                                break
                        if newly_seen >= max(1, int(self.cfg.max_new_events_per_cycle or 1)):
                            break
                    if newly_seen >= max(1, int(self.cfg.max_new_events_per_cycle or 1)):
                        break

                # Advance window conservatively; keep a small overlap.
                since = now - timedelta(seconds=max(2.0, float(self.cfg.interval_s)))
                st["last_poll_ts"] = since.isoformat().replace("+00:00", "Z")
                self._save_state(st)
                if newly_seen:
                    audit.write(corr, "poller.cycle", {"new_events": newly_seen, "dags_scanned": len(dags)})
                # Success: reset backoff to normal interval.
                self._backoff_s = max(1.0, float(self.cfg.interval_s))
            except Exception as e:  # noqa: BLE001
                # Transient network/timeout errors are common when Airflow is restarting or under load.
                # Avoid spamming audit logs every few seconds; dedupe + exponential backoff.
                err_sig = f"{type(e).__name__}: {e}"
                now_s = time.monotonic()
                if (self._last_error_sig != err_sig) or (now_s - self._last_error_ts > 60.0):
                    audit.write(corr, "poller.error", {"error": err_sig, "base_url": self.cfg.base_url, "backoff_s": round(self._backoff_s, 2)})
                    self._last_error_sig = err_sig
                    self._last_error_ts = now_s

                # Backoff on likely transient failures (ConnectError/ReadTimeout/ReadError/etc).
                self._backoff_s = min(self._backoff_max_s, max(self._backoff_s * 1.8, float(self.cfg.interval_s)))
                try:
                    # Avoid spamming poller.backoff every loop: emit only when it meaningfully changes,
                    # or once per minute as a heartbeat while degraded.
                    now_s2 = time.monotonic()
                    if (
                        abs(self._backoff_s - (self._last_backoff_emitted_s or 0.0)) >= 1.0
                        or (now_s2 - (self._last_backoff_emit_ts or 0.0)) > 60.0
                    ):
                        audit.write(
                            corr,
                            "poller.backoff",
                            {"backoff_s": round(self._backoff_s, 2), "reason": type(e).__name__},
                        )
                        self._last_backoff_emitted_s = float(self._backoff_s)
                        self._last_backoff_emit_ts = float(now_s2)
                except Exception:
                    pass

            self._stop.wait(float(self._backoff_s))



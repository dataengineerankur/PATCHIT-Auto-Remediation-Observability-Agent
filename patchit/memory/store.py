from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional


def _sha256_text(s: str) -> str:
    h = hashlib.sha256()
    h.update((s or "").encode("utf-8", errors="replace"))
    return h.hexdigest()


_RUN_ID_PATTERNS: list[re.Pattern[str]] = [
    # Airflow run ids commonly look like: manual__2025-12-18T21:37:00.421663+00:00
    re.compile(r"(manual__\d{4}-\d{2}-\d{2}T[0-9:\.\+\-]+)"),
    # ISO-ish timestamps
    re.compile(r"(\d{4}-\d{2}-\d{2}T[0-9:\.]+Z)"),
]


def _normalize_error_text(s: str) -> str:
    """
    Normalize volatile tokens (run ids, timestamps, per-run paths) so fingerprints remain stable across reruns.
    """
    t = s or ""
    for pat in _RUN_ID_PATTERNS:
        t = pat.sub("<RUN_ID>", t)
    # Normalize the common run-dir segment: .../grocery_runs/<RUN_ID>/...
    t = re.sub(r"(grocery_runs/)([^/]+)(/)", r"\1<RUN_ID>\3", t)
    return t


@dataclass(frozen=True)
class AttemptRecord:
    attempt_id: str
    ts_unix: float
    correlation_id: str
    event_id: str
    pipeline_id: str
    run_id: str
    task_id: str | None
    error_fingerprint: str
    patch_id: str | None
    patch_diff_sha: str | None
    patch_title: str | None
    patch_class: str | None
    verify_ok: bool | None
    verify_summary: str | None
    pr_url: str | None
    outcome: str | None  # fixed|no_effect|regression|unknown
    notes: str | None


class RemediationMemoryStore:
    """
    Minimal persistent memory (SQLite) for remediation attempts.

    Goals:
    - persist across container restarts
    - dedupe equivalent patches
    - enable "stop conditions" and post-PR outcome tracking later
    """

    def __init__(self, *, db_path: str) -> None:
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path, timeout=10.0)
        con.row_factory = sqlite3.Row
        return con

    def _init_db(self) -> None:
        with self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS attempts (
                    attempt_id TEXT PRIMARY KEY,
                    ts_unix REAL NOT NULL,
                    correlation_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    pipeline_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    task_id TEXT,
                    error_fingerprint TEXT NOT NULL,
                    patch_id TEXT,
                    patch_diff_sha TEXT,
                    patch_title TEXT,
                    patch_class TEXT,
                    verify_ok INTEGER,
                    verify_summary TEXT,
                    pr_url TEXT,
                    outcome TEXT,
                    notes TEXT
                )
                """
            )
            # Lightweight schema migration for older DBs.
            cols = [r[1] for r in con.execute("PRAGMA table_info(attempts)").fetchall()]
            if "patch_class" not in cols:
                con.execute("ALTER TABLE attempts ADD COLUMN patch_class TEXT")
            con.execute("CREATE INDEX IF NOT EXISTS idx_attempts_event ON attempts(event_id)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_attempts_fingerprint ON attempts(error_fingerprint)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_attempts_pipeline ON attempts(pipeline_id)")

            # Prompt methodology playbooks (per fingerprint). These are NOT pipeline-specific templates;
            # they are tightening constraints / exclusions / preferred modalities after failures.
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS prompt_playbooks (
                    error_fingerprint TEXT PRIMARY KEY,
                    updated_ts_unix REAL NOT NULL,
                    playbook_json TEXT NOT NULL
                )
                """
            )
            con.commit()

    @staticmethod
    def error_fingerprint(*, error: Dict[str, Any]) -> str:
        """
        Generate a stable-ish fingerprint from the NormalizedErrorObject-like dict.
        Keep it coarse to avoid being too brittle across line-number churn.
        """
        et = str(error.get("error_type") or "")
        msg = _normalize_error_text(str(error.get("message") or ""))
        cls = str(error.get("classification") or "")
        stack = error.get("stack") or []
        top = stack[0] if isinstance(stack, list) and stack else {}
        top_fp = f"{top.get('file_path','')}:{top.get('function','')}"
        raw = f"{et}|{cls}|{top_fp}|{msg[:180]}"
        return _sha256_text(raw)

    def record_attempt(
        self,
        *,
        attempt_id: str,
        correlation_id: str,
        event: Dict[str, Any],
        error: Dict[str, Any],
        patch: Dict[str, Any] | None,
        verify: Dict[str, Any] | None,
        pr: Dict[str, Any] | None,
        outcome: str | None = None,
        notes: str | None = None,
        patch_class: str | None = None,
    ) -> None:
        event_id = str(event.get("event_id") or "")
        pipeline_id = str(event.get("pipeline_id") or "")
        run_id = str(event.get("run_id") or "")
        task_id = event.get("task_id")
        fingerprint = self.error_fingerprint(error=error)

        patch_id = None
        patch_title = None
        diff_sha = None
        if patch:
            patch_id = str(patch.get("patch_id") or "") or None
            patch_title = str(patch.get("title") or "") or None
            diff_sha = _sha256_text(str(patch.get("diff_unified") or ""))

        verify_ok = None
        verify_summary = None
        if verify:
            verify_ok = bool(verify.get("ok"))
            verify_summary = str(verify.get("summary") or "") or None

        pr_url = None
        if pr:
            pr_url = str((pr.get("pr_url") or "")) or None

        with self._connect() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO attempts (
                    attempt_id, ts_unix, correlation_id, event_id, pipeline_id, run_id, task_id,
                    error_fingerprint, patch_id, patch_diff_sha, patch_title, patch_class,
                    verify_ok, verify_summary, pr_url, outcome, notes
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    attempt_id,
                    float(time.time()),
                    correlation_id,
                    event_id,
                    pipeline_id,
                    run_id,
                    task_id,
                    fingerprint,
                    patch_id,
                    diff_sha,
                    patch_title,
                    patch_class,
                    None if verify_ok is None else (1 if verify_ok else 0),
                    verify_summary,
                    pr_url,
                    outcome,
                    notes,
                ),
            )
            con.commit()

    def recent_attempts_for_fingerprint(self, *, error_fingerprint: str, limit: int = 20) -> List[AttemptRecord]:
        with self._connect() as con:
            rows = con.execute(
                "SELECT * FROM attempts WHERE error_fingerprint=? ORDER BY ts_unix DESC LIMIT ?",
                (error_fingerprint, int(limit)),
            ).fetchall()
        return [self._row_to_attempt(r) for r in rows]

    def seen_patch_diff_sha(self, *, error_fingerprint: str, patch_diff_sha: str) -> bool:
        with self._connect() as con:
            row = con.execute(
                "SELECT 1 FROM attempts WHERE error_fingerprint=? AND patch_diff_sha=? LIMIT 1",
                (error_fingerprint, patch_diff_sha),
            ).fetchone()
        return bool(row)

    def seen_patch_diff_sha_for_pipeline(self, *, pipeline_id: str, patch_diff_sha: str) -> bool:
        """
        Coarser de-dupe: if we've already proposed the exact same patch diff for this pipeline,
        don't keep proposing it on subsequent runs (even if the fingerprint drifts slightly).

        IMPORTANT: only treat it as "seen" if it previously resulted in a PR URL. Otherwise we'd
        permanently suppress a potentially-correct patch that never actually shipped (e.g., was rejected,
        blocked by local validation, or simply never made it to PR creation).
        """
        if not pipeline_id or not patch_diff_sha:
            return False
        with self._connect() as con:
            row = con.execute(
                "SELECT 1 FROM attempts WHERE pipeline_id=? AND patch_diff_sha=? AND pr_url IS NOT NULL LIMIT 1",
                (pipeline_id, patch_diff_sha),
            ).fetchone()
        return bool(row)

    def pr_url_for_pipeline_patch_diff(self, *, pipeline_id: str, patch_diff_sha: str) -> str | None:
        """
        If we previously created a PR for this pipeline_id with the same patch diff, return that PR URL.
        Used to surface the existing PR when we suppress duplicate PR creation.
        """
        if not pipeline_id or not patch_diff_sha:
            return None
        with self._connect() as con:
            row = con.execute(
                "SELECT pr_url FROM attempts WHERE pipeline_id=? AND patch_diff_sha=? AND pr_url IS NOT NULL ORDER BY ts_unix DESC LIMIT 1",
                (pipeline_id, patch_diff_sha),
            ).fetchone()
        if not row:
            return None
        v = row["pr_url"]
        return str(v) if v else None

    def seen_patch_class(self, *, error_fingerprint: str, patch_class: str) -> bool:
        if not patch_class:
            return False
        with self._connect() as con:
            row = con.execute(
                # Only treat a patch_class as "seen (ineffective)" if it previously failed verification
                # or was rejected (verify_ok NULL).
                "SELECT 1 FROM attempts WHERE error_fingerprint=? AND patch_class=? AND (verify_ok=0 OR verify_ok IS NULL) LIMIT 1",
                (error_fingerprint, patch_class),
            ).fetchone()
        return bool(row)

    def recent_pr_for_fingerprint(self, *, error_fingerprint: str, within_s: float) -> str | None:
        """
        Return the most recent PR URL created for this fingerprint within a time window.
        Used as a coarse clustering guard to prevent PR spam during bursts.
        """
        if not error_fingerprint:
            return None
        cutoff = float(time.time()) - float(within_s)
        with self._connect() as con:
            row = con.execute(
                "SELECT pr_url FROM attempts WHERE error_fingerprint=? AND pr_url IS NOT NULL AND ts_unix >= ? ORDER BY ts_unix DESC LIMIT 1",
                (error_fingerprint, cutoff),
            ).fetchone()
        if not row:
            return None
        v = row["pr_url"]
        return str(v) if v else None

    def get_prompt_playbook(self, *, error_fingerprint: str) -> Dict[str, Any]:
        if not error_fingerprint:
            return {}
        with self._connect() as con:
            row = con.execute(
                "SELECT playbook_json FROM prompt_playbooks WHERE error_fingerprint=? LIMIT 1",
                (error_fingerprint,),
            ).fetchone()
        if not row:
            return {}
        try:
            return json.loads(str(row["playbook_json"] or "{}")) or {}
        except Exception:
            return {}

    def upsert_prompt_playbook(self, *, error_fingerprint: str, playbook: Dict[str, Any]) -> None:
        if not error_fingerprint:
            return
        pb = json.dumps(playbook or {}, separators=(",", ":"), ensure_ascii=False)
        with self._connect() as con:
            con.execute(
                "INSERT OR REPLACE INTO prompt_playbooks (error_fingerprint, updated_ts_unix, playbook_json) VALUES (?,?,?)",
                (error_fingerprint, float(time.time()), pb),
            )
            con.commit()

    @staticmethod
    def _row_to_attempt(r: sqlite3.Row) -> AttemptRecord:
        return AttemptRecord(
            attempt_id=str(r["attempt_id"]),
            ts_unix=float(r["ts_unix"]),
            correlation_id=str(r["correlation_id"]),
            event_id=str(r["event_id"]),
            pipeline_id=str(r["pipeline_id"]),
            run_id=str(r["run_id"]),
            task_id=str(r["task_id"]) if r["task_id"] is not None else None,
            error_fingerprint=str(r["error_fingerprint"]),
            patch_id=str(r["patch_id"]) if r["patch_id"] is not None else None,
            patch_diff_sha=str(r["patch_diff_sha"]) if r["patch_diff_sha"] is not None else None,
            patch_title=str(r["patch_title"]) if r["patch_title"] is not None else None,
            patch_class=str(r["patch_class"]) if ("patch_class" in r.keys() and r["patch_class"] is not None) else None,
            verify_ok=(None if r["verify_ok"] is None else bool(int(r["verify_ok"]))),
            verify_summary=str(r["verify_summary"]) if r["verify_summary"] is not None else None,
            pr_url=str(r["pr_url"]) if r["pr_url"] is not None else None,
            outcome=str(r["outcome"]) if r["outcome"] is not None else None,
            notes=str(r["notes"]) if r["notes"] is not None else None,
        )


def to_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)



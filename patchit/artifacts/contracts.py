from __future__ import annotations

import ast
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class ArtifactHit:
    file: str
    function: str | None
    line: int | None
    snippet: str | None


def _safe_relpath(root: str, path: str) -> str:
    try:
        return os.path.relpath(path, root).replace("\\", "/")
    except Exception:
        return path.replace("\\", "/")


def _iter_python_files(project_root: str, *, max_files: int = 1200) -> List[str]:
    out: List[str] = []
    for r, dirs, files in os.walk(project_root):
        rel = _safe_relpath(project_root, r)
        if rel.startswith(("repo/airflow/logs", "repo/airflow/data", "repo/airflow/pgdata", "var", ".git", ".mock_github")):
            dirs[:] = []
            continue
        for fn in files:
            if fn.endswith(".py"):
                out.append(os.path.join(r, fn))
                if len(out) >= max_files:
                    return out
    return out


def _artifact_hits_in_file(*, abs_path: str, artifact_name: str) -> List[ArtifactHit]:
    try:
        text = open(abs_path, "r", encoding="utf-8", errors="replace").read()
    except Exception:
        return []
    if artifact_name not in text:
        return []

    hits: List[ArtifactHit] = []
    try:
        tree = ast.parse(text)
    except Exception:
        # Fallback: provide the file-level hit
        return [ArtifactHit(file=abs_path, function=None, line=None, snippet=None)]

    # Build line->function mapping (best-effort)
    func_spans: List[Tuple[int, int, str]] = []
    for n in ast.walk(tree):
        if isinstance(n, ast.FunctionDef):
            start = getattr(n, "lineno", None) or 0
            end = getattr(n, "end_lineno", None) or start
            func_spans.append((start, end, n.name))
    lines = text.splitlines()

    for n in ast.walk(tree):
        if isinstance(n, ast.Constant) and isinstance(n.value, str) and artifact_name in n.value:
            ln = getattr(n, "lineno", None)
            func = None
            if ln is not None:
                for a, b, name in func_spans:
                    if a <= ln <= b:
                        func = name
                        break
            snippet = None
            if ln is not None and 1 <= ln <= len(lines):
                snippet = lines[ln - 1].strip()
            hits.append(ArtifactHit(file=abs_path, function=func, line=ln, snippet=snippet))
    if hits:
        return hits[:8]
    # If AST didn't catch constants (constructed paths), still treat as file-level hit
    return [ArtifactHit(file=abs_path, function=None, line=None, snippet=None)]


def infer_missing_artifact_context(
    *,
    project_root: str,
    pipeline_id: str,
    failing_task_id: str | None,
    artifact_uris: List[str],
    error_message: str,
    max_hits: int = 20,
) -> Dict[str, Any]:
    """
    Best-effort inference to help the agent fix missing-artifact failures:
    - identify the missing artifact basename(s)
    - find code references to that basename (producer/consumer candidates)
    - suggest likely producer task id heuristically (keyword match)
    """
    missing: List[Dict[str, Any]] = []
    # Heuristic: pull obvious basenames from error message and artifact URIs
    candidates: List[str] = []
    for uri in artifact_uris or []:
        try:
            bn = os.path.basename(uri.split("?", 1)[0])
            if bn:
                candidates.append(bn)
        except Exception:
            continue
    # Extract *.json/csv/parquet from message
    for tok in (error_message or "").replace("'", " ").replace('"', " ").split():
        if any(tok.endswith(ext) for ext in (".json", ".csv", ".parquet", ".duckdb")):
            candidates.append(os.path.basename(tok))
    # De-dupe keep order
    seen = set()
    basenames: List[str] = []
    for c in candidates:
        if c and c not in seen:
            seen.add(c)
            basenames.append(c)
    basenames = basenames[:3]

    py_files = _iter_python_files(project_root)
    for bn in basenames:
        hits: List[ArtifactHit] = []
        for f in py_files:
            for h in _artifact_hits_in_file(abs_path=f, artifact_name=bn):
                hits.append(h)
                if len(hits) >= max_hits:
                    break
            if len(hits) >= max_hits:
                break

        # Guess a likely producer task id by keyword match against bn
        keyword = os.path.splitext(bn)[0].lower()
        keyword = keyword.replace("-", "_")
        parts = [p for p in keyword.split("_") if p and len(p) >= 4]
        scored: List[Tuple[int, str]] = []
        for p in parts[:3]:
            scored.append((0, p))

        # very cheap task-id hint: look for "enrich" style words
        hint_task = None
        for w in ("enrich", "extract", "ingest", "transform", "load", "write"):
            if w in keyword:
                hint_task = w
                break
        likely_producer = None
        if hint_task:
            likely_producer = hint_task

        missing.append(
            {
                "basename": bn,
                "pipeline_id": pipeline_id,
                "failing_task_id": failing_task_id,
                "likely_producer_task_hint": likely_producer,
                "evidence": [
                    {
                        "file": _safe_relpath(project_root, h.file),
                        "function": h.function,
                        "line": h.line,
                        "snippet": h.snippet,
                    }
                    for h in hits
                ],
            }
        )

    return {"missing_artifacts": missing}







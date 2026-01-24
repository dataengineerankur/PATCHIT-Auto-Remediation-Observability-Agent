from __future__ import annotations

import ast
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_EXCLUDES = (
    ".git",
    ".venv",
    "venv",
    "node_modules",
    "__pycache__",
    "airflow/logs",
    "airflow/pgdata",
    "airflow/data",
    "var",
    ".mock_github",
)


@dataclass(frozen=True)
class FileIndex:
    path: str
    kind: str
    symbols: List[str]
    imports: List[str] = None  # type: ignore[assignment]
    doc: str | None = None


@dataclass(frozen=True)
class DagIndex:
    path: str
    dag_ids: List[str]
    tasks: List[str]
    edges: List[Tuple[str, str]]


@dataclass(frozen=True)
class DagTriggerIndex:
    """
    Best-effort extraction of cross-DAG triggers via TriggerDagRunOperator.
    This is intentionally lightweight (no Airflow import/runtime).
    """

    path: str
    source_dag_ids: List[str]
    source_task_id: str | None
    target_dag_id: str | None
    conf_keys: List[str]
    run_id_value_expr: str | None


def _is_excluded(rel_path: str) -> bool:
    norm = rel_path.replace("\\", "/")
    return any(norm == x or norm.startswith(x + "/") for x in DEFAULT_EXCLUDES)


def _py_symbols(text: str) -> List[str]:
    out: List[str] = []
    try:
        tree = ast.parse(text)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                out.append(node.name)
    except Exception:
        return []
    return sorted(set(out))[:200]


def _py_imports_and_doc(text: str) -> Tuple[List[str], Optional[str]]:
    imports: List[str] = []
    doc: Optional[str] = None
    try:
        tree = ast.parse(text)
        doc = ast.get_docstring(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for n in node.names:
                    imports.append(n.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.append(node.module)
    except Exception:
        return ([], None)
    return (sorted(set(imports))[:200], (doc or None))


_DAG_ID_RE = re.compile(r'dag_id\s*=\s*["\']([^"\']+)["\']')
_TASK_ID_RE = re.compile(r'task_id\s*=\s*["\']([^"\']+)["\']')


def _is_trigger_dag_run_call(node: ast.AST) -> bool:
    # TriggerDagRunOperator(...) or airflow.operators.trigger_dagrun.TriggerDagRunOperator(...)
    if not isinstance(node, ast.Call):
        return False
    f = node.func
    if isinstance(f, ast.Name):
        return f.id == "TriggerDagRunOperator"
    if isinstance(f, ast.Attribute):
        return f.attr == "TriggerDagRunOperator"
    return False


def _extract_str_constant(n: ast.AST) -> str | None:
    if isinstance(n, ast.Constant) and isinstance(n.value, str):
        return n.value
    return None


def _extract_conf_keys_and_run_id_expr(*, text: str, conf_node: ast.AST | None) -> tuple[list[str], str | None]:
    if not isinstance(conf_node, ast.Dict):
        return ([], None)
    keys: list[str] = []
    run_id_expr: str | None = None
    for k, v in zip(conf_node.keys, conf_node.values):
        ks = _extract_str_constant(k) if k is not None else None
        if not ks:
            continue
        keys.append(ks)
        if ks == "run_id":
            # keep the exact template/expression as source text for grounding
            run_id_expr = ast.get_source_segment(text, v) if v is not None else None
    # de-dupe keep order
    seen = set()
    out: list[str] = []
    for k in keys:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return (out[:30], (run_id_expr.strip() if isinstance(run_id_expr, str) else None))


def _extract_trigger_dag_indices(path: str, text: str) -> list[DagTriggerIndex]:
    try:
        tree = ast.parse(text)
    except Exception:
        return []

    # Best-effort: associate the trigger with any dag_id found in the same file.
    source_dag_ids = sorted(set(_DAG_ID_RE.findall(text)))[:10]
    out: list[DagTriggerIndex] = []

    for node in ast.walk(tree):
        if not _is_trigger_dag_run_call(node):
            continue
        task_id: str | None = None
        target_dag_id: str | None = None
        conf_node: ast.AST | None = None
        for kw in getattr(node, "keywords", []) or []:
            if not isinstance(kw, ast.keyword):
                continue
            if kw.arg == "task_id":
                task_id = _extract_str_constant(kw.value)
            if kw.arg == "trigger_dag_id":
                target_dag_id = _extract_str_constant(kw.value)
            if kw.arg == "conf":
                conf_node = kw.value
        conf_keys, run_id_expr = _extract_conf_keys_and_run_id_expr(text=text, conf_node=conf_node)
        out.append(
            DagTriggerIndex(
                path=path,
                source_dag_ids=source_dag_ids,
                source_task_id=task_id,
                target_dag_id=target_dag_id,
                conf_keys=conf_keys,
                run_id_value_expr=run_id_expr,
            )
        )
    return out[:80]


def _extract_airflow_edges(text: str) -> List[Tuple[str, str]]:
    """
    Best-effort DAG edge extraction from `a >> b` patterns.
    Works well for our demo DAGs; not a full parser.
    """
    edges: List[Tuple[str, str]] = []
    for line in text.splitlines():
        if ">>" not in line:
            continue
        parts = line.split(">>")
        if len(parts) < 2:
            continue
        left = parts[0].strip().strip("()")
        right = parts[1].strip().strip().strip("()")
        if not left.isidentifier():
            continue
        if right.startswith("[") and right.endswith("]"):
            inner = right[1:-1]
            for tok in inner.split(","):
                t = tok.strip()
                if t.isidentifier():
                    edges.append((left, t))
        else:
            tok = right.split()[0].strip().strip(",")
            if tok.isidentifier():
                edges.append((left, tok))
    seen = set()
    out: List[Tuple[str, str]] = []
    for a, b in edges:
        if (a, b) not in seen:
            seen.add((a, b))
            out.append((a, b))
    return out


def _maybe_index_airflow_dag(path: str, text: str) -> Optional[DagIndex]:
    norm = path.replace("\\", "/")
    if "dags" not in norm:
        return None
    if "from airflow" not in text and "DAG(" not in text:
        return None
    dag_ids = _DAG_ID_RE.findall(text)
    tasks = sorted(set(_TASK_ID_RE.findall(text)))
    edges = _extract_airflow_edges(text)
    if not dag_ids and not tasks:
        return None
    return DagIndex(path=path, dag_ids=sorted(set(dag_ids)), tasks=tasks[:300], edges=edges[:600])


def build_codebase_index(project_root: str, *, max_files: int = 600) -> Dict[str, Any]:
    """
    Lightweight repo index for prompting.
    Returns a JSON-serializable dict.
    """
    files: List[FileIndex] = []
    dags: List[DagIndex] = []
    triggers: List[DagTriggerIndex] = []
    count = 0
    for root, dirs, filenames in os.walk(project_root):
        rel_root = os.path.relpath(root, project_root).replace("\\", "/")
        if rel_root == ".":
            rel_root = ""
        # prune excluded dirs
        dirs[:] = [d for d in dirs if not _is_excluded(f"{rel_root}/{d}".strip("/"))]

        for fn in filenames:
            rel = f"{rel_root}/{fn}".strip("/")
            if _is_excluded(rel):
                continue
            if fn.endswith((".py", ".sql", ".yml", ".yaml", ".json")):
                abs_path = os.path.join(project_root, rel)
                try:
                    with open(abs_path, "r", encoding="utf-8", errors="replace") as f:
                        text = f.read()
                except Exception:
                    continue

                kind = "python" if fn.endswith(".py") else ("sql" if fn.endswith(".sql") else "data")
                symbols: List[str] = []
                imports: List[str] = []
                doc: Optional[str] = None
                if kind == "python":
                    symbols = _py_symbols(text)
                    imports, doc = _py_imports_and_doc(text)
                    di = _maybe_index_airflow_dag(rel, text)
                    if di:
                        dags.append(di)
                    # Cross-DAG trigger map (TriggerDagRunOperator)
                    if "TriggerDagRunOperator" in text:
                        triggers.extend(_extract_trigger_dag_indices(rel, text))
                files.append(FileIndex(path=rel, kind=kind, symbols=symbols, imports=imports, doc=doc))
                count += 1
                if count >= max_files:
                    break
        if count >= max_files:
            break

    return {
        "root": project_root,
        "file_count_indexed": len(files),
        "dag_count_indexed": len(dags),
        "files": [f.__dict__ for f in files],
        "airflow_dags": [d.__dict__ for d in dags],
        "airflow_dag_triggers": [t.__dict__ for t in triggers[:300]],
    }



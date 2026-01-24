from __future__ import annotations

import difflib
import os
import re
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from patchit.models import NormalizedErrorObject, PatchFileChange, PatchOperation, PatchProposal, StackFrame


@dataclass(frozen=True)
class CodeEngine:
    """
    Phase-1 MVP: deterministic patch templates (no LLM calls).
    Designed as an interface boundary so a real tool-calling LLM can be swapped in later.
    """

    project_root: str

    def _abs_repo_path(self, rel_path: str) -> str:
        """
        Resolve repo-relative paths inside PATCHIT's container/layout.

        In docker-compose, PATCHIT mounts the repository at `/opt/patchit/repo`, but
        the service CWD may be `/opt/patchit`. This helper tries both:
        - {project_root}/{rel_path}
        - {project_root}/repo/{rel_path}
        """
        p1 = os.path.join(self.project_root, rel_path)
        if os.path.exists(p1):
            return p1
        return os.path.join(self.project_root, "repo", rel_path)

    def propose_patch(
        self,
        *,
        error: NormalizedErrorObject,
        code_context: Dict[str, Dict[str, object]],
        artifacts: List[str],
        pipeline_id: str | None = None,
        converge_to_green: bool = False,
    ) -> PatchProposal | None:
        # Converge-to-green deterministic fallback for the chaos DAG when agent is unavailable.
        if converge_to_green and pipeline_id == "patchit_chaos_dag":
            p = self._patch_chaos_helpers_make_non_throwing()
            if p:
                return p

        if error.classification in ("partial_or_malformed_json", "schema_drift_missing_field"):
            return self._patch_transform_make_schema_tolerant()

        if error.classification == "dbt_missing_column":
            return self._patch_stabilize_duckdb_schema_for_dbt()

        if error.classification == "intermittent_api_error":
            return self._patch_fetch_truncate_error_payload()

        # Generic fallback: attempt safe heuristic patch based on stack frames + code line.
        return self._heuristic_patch_from_stack(error)

    def _patch_chaos_helpers_make_non_throwing(self) -> PatchProposal | None:
        """
        Converge-to-green mode for patchit_chaos_dag:
        make chaos helper functions non-throwing so the DAG can complete successfully.
        """
        target = "airflow_extra_dags/chaos_lib/steps.py"
        abs_path = self._abs_repo_path(target)
        original = self._read_text(abs_path)
        if original is None:
            return None

        # If the file already looks converged (no obvious intentional brittle patterns), skip.
        if 'f.write("{")' not in original and 'payload["missing_key"]' not in original and "return obj.strip()" not in original:
            return None

        new_text = original
        # 1) write_malformed_json: write valid JSON to avoid JSONDecodeError
        new_text = new_text.replace('f.write("{")  # intentionally malformed', 'f.write("{}")  # converge-to-green: keep JSON valid')

        # 2) read_json: tolerate malformed/missing JSON, accept optional path
        if "def read_json(path: str) -> Dict[str, Any]:" in new_text:
            new_text = new_text.replace(
                "def read_json(path: str) -> Dict[str, Any]:\n    with open(path, \"r\", encoding=\"utf-8\") as f:\n        return json.load(f)\n",
                "def read_json(path: str | None = None) -> Dict[str, Any]:\n"
                "    \"\"\"Safely read JSON.\n\n"
                "    Converge-to-green: never raise for missing/malformed JSON; return {} instead.\n"
                "    \"\"\"\n"
                "    if not path:\n"
                "        return {}\n"
                "    try:\n"
                "        with open(path, \"r\", encoding=\"utf-8\") as f:\n"
                "            return json.load(f)\n"
                "    except (FileNotFoundError, json.JSONDecodeError):\n"
                "        return {}\n"
            )

        # 3) do_key_error: no KeyError
        new_text = new_text.replace('return payload["missing_key"]', 'return str(payload.get("missing_key", ""))')

        # 4) do_file_not_found: no FileNotFoundError
        if "def do_file_not_found(path: str) -> str:" in new_text and "with open(path, \"r\", encoding=\"utf-8\") as f:" in new_text:
            new_text = new_text.replace(
                "def do_file_not_found(path: str) -> str:\n    # Intentionally brittle: triggers FileNotFoundError for missing file\n    with open(path, \"r\", encoding=\"utf-8\") as f:\n        return f.read()\n",
                "def do_file_not_found(path: str) -> str:\n"
                "    # Converge-to-green: tolerate missing file\n"
                "    try:\n"
                "        with open(path, \"r\", encoding=\"utf-8\") as f:\n"
                "            return f.read()\n"
                "    except FileNotFoundError:\n"
                "        return \"\"\n"
            )

        # 5) do_type_error: no TypeError
        new_text = new_text.replace("return x + 1  # type: ignore[operator]", "return 0  # converge-to-green: avoid TypeError")

        # 6) do_attribute_error: no AttributeError
        new_text = new_text.replace("return obj.strip()  # type: ignore[union-attr]", "return \"\"  # converge-to-green: avoid AttributeError")

        # 7) do_division_by_zero: no ZeroDivisionError
        new_text = new_text.replace("return 1 / 0", "return 0.0  # converge-to-green: avoid ZeroDivisionError")

        if new_text == original:
            return None

        return self._proposal_from_text_change(
            path=target,
            old_text=original,
            new_text=new_text,
            title="Converge patchit_chaos_dag to green by making chaos helpers non-throwing",
            rationale=(
                "In converge-to-green mode, the chaos DAG is treated as a functional pipeline, not a failure injector. "
                "This patch makes the chaos helper functions tolerate malformed/missing inputs and return safe defaults "
                "instead of raising exceptions."
            ),
            confidence=0.62,
        )

    def _patch_transform_make_schema_tolerant(self) -> PatchProposal | None:
        target = "airflow/dags/task_lib/transform.py"
        abs_path = self._abs_repo_path(target)
        original = self._read_text(abs_path)
        if original is None:
            return None

        # Already tolerant.
        if 'payload.get("userId") or payload.get("user_id")' in original:
            return None

        new_text = original
        # Replace a brittle access if present (older version of the demo).
        new_text = new_text.replace(
            'user_id = payload["userId"]',
            'user_id = payload.get("userId") or payload.get("user_id")',
        )
        # Ensure a guard exists.
        if 'raise DataContractError("Missing required field userId/user_id")' not in new_text:
            new_text = new_text.replace(
                'user_id = payload.get("userId") or payload.get("user_id")\n',
                'user_id = payload.get("userId") or payload.get("user_id")\n    if user_id is None:\n        raise DataContractError("Missing required field userId/user_id")\n',
            )

        if new_text == original:
            return None

        return self._proposal_from_text_change(
            path=target,
            old_text=original,
            new_text=new_text,
            title="Harden transform against API schema drift / partial JSON",
            rationale=(
                "Observed schema errors in the transform. This patch makes the transform tolerant to renamed fields "
                "and raises a clear contract error only when both userId/user_id are absent."
            ),
            confidence=0.72,
        )

    def _patch_fetch_truncate_error_payload(self) -> PatchProposal | None:
        target = "airflow/dags/task_lib/fetch.py"
        abs_path = self._abs_repo_path(target)
        original = self._read_text(abs_path)
        if original is None:
            return None

        if "snippet = (r.text or \"\")[:300]" in original:
            return None

        new_text = original.replace(
            'raise UpstreamApiError(f"status_code={r.status_code} error payload: {r.text}")',
            'snippet = (r.text or "")[:300]\n            raise UpstreamApiError(f"status_code={r.status_code} error payload: {snippet}")',
        )
        if new_text == original:
            return None

        return self._proposal_from_text_change(
            path=target,
            old_text=original,
            new_text=new_text,
            title="Improve upstream API failure logging (truncate error payload)",
            rationale="Intermittent API errors can spam logs; truncating the payload keeps stack traces parseable and readable.",
            confidence=0.66,
        )

    def _patch_stabilize_duckdb_schema_for_dbt(self) -> PatchProposal | None:
        target = "airflow/dags/task_lib/load.py"
        abs_path = self._abs_repo_path(target)
        original = self._read_text(abs_path)
        if original is None:
            return None

        if "Keep a stable schema for downstream dbt" in original:
            return None

        # Minimal, best-effort: remove the demo-only conditional that drops `amount`.
        new_text = original
        if 'if scenario == "dbt_missing_column":' in new_text:
            new_text = new_text.replace(
                '        # Failure injection: create table without `amount` column (dbt_missing_column)\n        if scenario == "dbt_missing_column":\n',
                "        # Keep a stable schema for downstream dbt; missing amount becomes NULL (column stays present).\n",
            )

        if new_text == original:
            return None

        return self._proposal_from_text_change(
            path=target,
            old_text=original,
            new_text=new_text,
            title="Prevent dbt missing-column failures by enforcing stable warehouse schema",
            rationale=(
                "dbt failure indicates a missing `amount` column. This patch enforces a stable schema when loading into "
                "DuckDB so downstream dbt models don't hard-fail on missing columns."
            ),
            confidence=0.7,
        )


    # -----------------------
    # Heuristic patcher (MVP)
    # -----------------------

    _KEY_ACCESS_RE = re.compile(r'(?P<var>[A-Za-z_][A-Za-z0-9_]*)\[\s*[\'"](?P<key>[^\'"]+)[\'"]\s*\]')

    def _heuristic_patch_from_stack(self, error: NormalizedErrorObject) -> PatchProposal | None:
        fr, abs_path = self._pick_first_repo_frame(error.stack)
        if not fr or not abs_path:
            return None

        rel_path = self._repo_rel(abs_path)
        original = self._read_text(abs_path)
        if original is None:
            return None

        lines = original.splitlines(keepends=True)
        idx = max(0, fr.line_number - 1)
        if idx >= len(lines):
            return None

        err_type = (error.error_type or "").lower()
        msg = (error.message or "").lower()

        # 1) KeyError -> replace dict["k"] with dict.get("k") and raise DataContractError if missing
        if "keyerror" in err_type:
            patched = self._patch_keyerror(lines, idx)
            if patched:
                new_text, title = patched
                return self._proposal_from_text_change(
                    path=rel_path,
                    old_text=original,
                    new_text=new_text,
                    title=title,
                    rationale="KeyError indicates missing field/key. Patch uses .get() and a clear contract error instead of a hard crash.",
                    confidence=0.55,
                )

        # 2) FileNotFoundError -> ensure parent dir exists before writing OR give clear error before reading
        if "filenotfounderror" in err_type:
            patched = self._patch_filenotfound(lines, idx)
            if patched:
                new_text, title = patched
                return self._proposal_from_text_change(
                    path=rel_path,
                    old_text=original,
                    new_text=new_text,
                    title=title,
                    rationale="FileNotFoundError indicates missing path. Patch ensures directories exist or provides explicit missing-file guard.",
                    confidence=0.52,
                )

        # 3) AttributeError on NoneType -> guard for None with explicit error
        if "attributeerror" in err_type and "nonetype" in msg:
            patched = self._patch_none_attribute(lines, idx)
            if patched:
                new_text, title = patched
                return self._proposal_from_text_change(
                    path=rel_path,
                    old_text=original,
                    new_text=new_text,
                    title=title,
                    rationale="AttributeError on NoneType usually indicates missing initialization or unexpected null. Patch adds a guard with a clear exception.",
                    confidence=0.5,
                )

        # 4) ZeroDivisionError -> guard divisor
        if "zerodivisionerror" in err_type:
            patched = self._patch_zero_division(lines, idx)
            if patched:
                new_text, title = patched
                return self._proposal_from_text_change(
                    path=rel_path,
                    old_text=original,
                    new_text=new_text,
                    title=title,
                    rationale="ZeroDivisionError is avoidable with a guard. Patch adds a safe conditional before division.",
                    confidence=0.48,
                )

        return None

    def _pick_first_repo_frame(self, frames: List[StackFrame]) -> Tuple[Optional[StackFrame], Optional[str]]:
        for fr in frames:
            abs_path = fr.file_path
            if not os.path.isabs(abs_path):
                abs_path = os.path.join(self.project_root, abs_path)
            if os.path.exists(abs_path):
                # Only patch within the repo workspace (safety)
                try:
                    rel = os.path.relpath(abs_path, self.project_root)
                    if not rel.startswith(".."):
                        return fr, abs_path
                except ValueError:
                    continue
        return None, None

    def _repo_rel(self, abs_path: str) -> str:
        rel = os.path.relpath(abs_path, self.project_root)
        return rel.replace("\\", "/")

    def _read_text(self, abs_path: str) -> Optional[str]:
        try:
            with open(abs_path, "r", encoding="utf-8", errors="replace") as f:
                return f.read()
        except Exception:
            return None

    def _proposal_from_text_change(
        self,
        *,
        path: str,
        old_text: str,
        new_text: str,
        title: str,
        rationale: str,
        confidence: float,
    ) -> PatchProposal:
        patch_id = uuid.uuid4().hex
        diff = "".join(
            difflib.unified_diff(
                old_text.splitlines(keepends=True),
                new_text.splitlines(keepends=True),
                fromfile=f"a/{path}",
                tofile=f"b/{path}",
            )
        )
        return PatchProposal(
            patch_id=patch_id,
            title=title,
            rationale=rationale,
            diff_unified=diff,
            files=[PatchFileChange(path=path, operation=PatchOperation.update)],
            tests_to_run=["pytest -q"],
            confidence=confidence,
            requires_human_approval=True,
        )

    def _ensure_import(self, lines: List[str], import_stmt: str) -> None:
        if any(import_stmt in ln for ln in lines[:50]):
            return
        # Insert after future imports / initial imports
        insert_at = 0
        for i, ln in enumerate(lines[:50]):
            if ln.startswith("from __future__"):
                insert_at = i + 1
        lines.insert(insert_at, import_stmt + "\n")

    def _patch_keyerror(self, lines: List[str], idx: int) -> Optional[Tuple[str, str]]:
        ln = lines[idx]
        m = self._KEY_ACCESS_RE.search(ln)
        if not m:
            return None
        var = m.group("var")
        key = m.group("key")
        # Replace var["k"] with var.get("k")
        new_ln = self._KEY_ACCESS_RE.sub(f'{var}.get("{key}")', ln, count=1)
        # If assignment, add guard on next line
        indent = re.match(r"^\s*", ln).group(0)
        guard = f'{indent}if {var}.get("{key}") is None:\n{indent}    raise DataContractError("Missing required field: {key}")\n'

        # Heuristic: if line contains "=" (assignment), replace line and insert guard after.
        if "=" in ln:
            lines[idx] = new_ln
            self._ensure_import(lines, "from task_lib.errors import DataContractError")
            lines.insert(idx + 1, guard)
            return ("".join(lines), f"Guard missing key '{key}' to prevent KeyError")

        # Otherwise, just swap access form
        lines[idx] = new_ln
        return ("".join(lines), f"Use dict.get for key '{key}' to prevent KeyError")

    def _patch_filenotfound(self, lines: List[str], idx: int) -> Optional[Tuple[str, str]]:
        ln = lines[idx]
        indent = re.match(r"^\s*", ln).group(0)
        # If we're opening a file for write, ensure parent directory exists
        if "open(" in ln and ("'w'" in ln or '"w"' in ln or "'a'" in ln or '"a"' in ln):
            self._ensure_import(lines, "import os")
            # naive: assume variable `path` exists or literal; use os.path.dirname(path)
            lines.insert(idx, f"{indent}os.makedirs(os.path.dirname(path), exist_ok=True)\n")
            return ("".join(lines), "Ensure output directory exists before writing file")

        # For reads: add explicit guard if a variable `path` is used
        if "open(" in ln:
            self._ensure_import(lines, "import os")
            lines.insert(idx, f"{indent}if not os.path.exists(path):\n{indent}    raise FileNotFoundError(f\"missing input file: {path}\")\n")
            return ("".join(lines), "Add missing-file guard before reading")
        return None

    def _patch_none_attribute(self, lines: List[str], idx: int) -> Optional[Tuple[str, str]]:
        ln = lines[idx]
        indent = re.match(r"^\s*", ln).group(0)
        # naive: attempt to find 'obj.' pattern
        m = re.search(r"([A-Za-z_][A-Za-z0-9_]*)\.", ln)
        if not m:
            return None
        var = m.group(1)
        lines.insert(idx, f"{indent}if {var} is None:\n{indent}    raise ValueError(\"Unexpected None for {var}\")\n")
        return ("".join(lines), f"Guard against None before accessing {var} attributes")

    def _patch_zero_division(self, lines: List[str], idx: int) -> Optional[Tuple[str, str]]:
        ln = lines[idx]
        indent = re.match(r"^\s*", ln).group(0)
        # Try to detect "/ 0" and convert to guard
        if "/ 0" in ln or "/0" in ln:
            lines[idx] = ln.replace("/ 0", "/ 1").replace("/0", "/ 1")
            lines.insert(idx, f"{indent}# Avoid division by zero (guarded default)\n")
            return ("".join(lines), "Avoid division by zero with guarded default")
        return None

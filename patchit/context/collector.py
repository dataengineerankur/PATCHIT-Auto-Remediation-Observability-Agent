from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, List, Optional

from patchit.models import StackFrame


@dataclass(frozen=True)
class CodeContextCollector:
    """
    Collects file snippets around stack frames for better patch proposals & PR explainability.
    """

    project_root: str
    context_lines: int = 8
    translate_from: Optional[str] = None
    translate_to: Optional[str] = None

    def _read_lines(self, abs_path: str) -> List[str]:
        with open(abs_path, "r", encoding="utf-8", errors="replace") as f:
            return f.read().splitlines()

    def _translate(self, abs_path: str) -> str:
        if self.translate_from and self.translate_to and abs_path.startswith(self.translate_from):
            rel = abs_path[len(self.translate_from) :].lstrip("/")
            return os.path.join(self.translate_to, rel)
        return abs_path

    def _to_repo_relative(self, abs_path: str) -> str:
        try:
            rel = os.path.relpath(abs_path, self.project_root)
            if not rel.startswith(".."):
                # Canonicalize docker-mounted Airflow extra_dags paths to the real repo path.
                if rel.startswith("airflow/dags/extra_dags/"):
                    return "airflow_extra_dags/" + rel[len("airflow/dags/extra_dags/") :]
                return rel
        except ValueError:
            pass
        return abs_path

    def collect(self, frames: List[StackFrame]) -> Dict[str, Dict[str, object]]:
        out: Dict[str, Dict[str, object]] = {}
        for fr in frames:
            abs_path = fr.file_path
            if os.path.isabs(abs_path):
                abs_path = self._translate(abs_path)
            else:
                abs_path = os.path.join(self.project_root, fr.file_path)
            if not os.path.exists(abs_path):
                continue

            lines = self._read_lines(abs_path)
            idx = max(0, fr.line_number - 1)
            start = max(0, idx - self.context_lines)
            end = min(len(lines), idx + self.context_lines + 1)
            snippet = "\n".join(f"{i+1}: {lines[i]}" for i in range(start, end))
            out[self._to_repo_relative(abs_path)] = {
                "line": fr.line_number,
                "function": fr.function,
                "snippet": snippet,
            }
        return out

    def collect_files(self, rel_paths: List[str]) -> Dict[str, Dict[str, object]]:
        """
        Collect lightweight context for repo-relative files (non-stack-frame driven).
        Useful for artifacts referenced in logs (ex: dbt model paths like `models/...sql`).
        """
        out: Dict[str, Dict[str, object]] = {}
        for rp in rel_paths or []:
            if not isinstance(rp, str) or not rp:
                continue
            # rp is expected to be repo-relative; resolve to absolute under project_root
            abs_path = os.path.join(self.project_root, rp)
            if not os.path.exists(abs_path):
                continue
            try:
                lines = self._read_lines(abs_path)
            except Exception:
                continue
            # Keep it compact but useful: first ~80 lines of the file.
            head_n = min(len(lines), max(10, self.context_lines * 10))
            snippet = "\n".join(f"{i+1}: {lines[i]}" for i in range(0, head_n))
            out[self._to_repo_relative(abs_path)] = {
                "line": 1,
                "function": "(file)",
                "snippet": snippet,
            }
        return out



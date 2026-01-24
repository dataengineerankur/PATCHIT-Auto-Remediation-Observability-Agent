from __future__ import annotations

import os
import re
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx


_ATTEMPT_RE = re.compile(r"(?:^|/)(attempt=)(?P<n>[0-9]+)(?P<suffix>\.log)$")


@dataclass(frozen=True)
class LogFetcher:
    """
    Fetch logs from file:// paths or http(s):// endpoints.
    For Phase-1 MVP, AirflowAdapter will often pass file:// URIs (mounted logs).
    """

    timeout_s: float = 15.0
    translate_from: str | None = None
    translate_to: str | None = None
    attempt_fallback: bool = True

    def fetch(self, uri: str) -> str:
        parsed = urlparse(uri)
        if parsed.scheme in ("", "file"):
            path = parsed.path if parsed.scheme == "file" else uri
            resolved = self._resolve_local_path(path)
            if not resolved:
                raise FileNotFoundError(f"log path not found: {path}")
            with open(resolved, "r", encoding="utf-8", errors="replace") as f:
                return f.read()

        if parsed.scheme in ("http", "https"):
            with httpx.Client(timeout=self.timeout_s, follow_redirects=True) as client:
                r = client.get(uri)
                r.raise_for_status()
                return r.text

        raise ValueError(f"unsupported log uri scheme: {parsed.scheme}")

    def _resolve_local_path(self, path: str) -> str | None:
        """
        Resolve a local file path that may not exist due to:
        - container mount differences (translate_from/translate_to)
        - Airflow retry bookkeeping (attempt=N.log doesn't exist, but attempt=N-1.log does)
        """
        candidates: list[str] = []

        # 1) As-is
        candidates.append(path)

        # 2) Optional translation
        if self.translate_from and self.translate_to and path.startswith(self.translate_from):
            candidates.append(self.translate_to + path[len(self.translate_from) :])

        # 3) Attempt fallback (for both as-is and translated)
        if self.attempt_fallback:
            more: list[str] = []
            for p in list(candidates):
                more.extend(self._attempt_fallback_candidates(p))
            candidates.extend(more)

        for p in candidates:
            if os.path.exists(p):
                return p
        return None

    def _attempt_fallback_candidates(self, path: str) -> list[str]:
        m = _ATTEMPT_RE.search(path)
        if not m:
            return []
        n = int(m.group("n"))
        if n <= 1:
            return []
        prefix = path[: m.start("n")]
        suffix = path[m.end("n") :]
        return [f"{prefix}{k}{suffix}" for k in range(n - 1, 0, -1)]




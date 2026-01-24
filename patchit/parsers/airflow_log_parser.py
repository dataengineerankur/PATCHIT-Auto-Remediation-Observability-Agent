from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


_TRACEBACK_START_RE = re.compile(r"Traceback \(most recent call last\):")
_DBT_ERROR_HINT_RE = re.compile(
    r"("
    r"database error|compilation error|runtime error|"
    r"error creating|"
    r"column .* (does not exist|not found)|"
    r"relation .* (does not exist|not found)|"
    r"permission denied|"
    r"bind(er)? error"
    r")",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class AirflowLogParseResult:
    """
    Reduced log content focused on the actual exception, so stacktrace parsing is more reliable.
    """

    excerpt: str
    has_traceback: bool


def extract_error_excerpt(log_text: str, *, max_lines: int = 250) -> AirflowLogParseResult:
    """
    Airflow task logs can be huge/noisy. This extracts the last traceback block if present,
    else returns the last N lines.
    """
    lines = log_text.splitlines()
    last_tb = None
    for i, ln in enumerate(lines):
        if _TRACEBACK_START_RE.search(ln):
            last_tb = i

    if last_tb is None:
        excerpt = "\n".join(lines[-max_lines:])
        return AirflowLogParseResult(excerpt=excerpt, has_traceback=False)

    # By default we only return the *last* traceback block. This keeps excerpts tight and avoids
    # accidentally including earlier, unrelated tracebacks (a common pattern in long task logs).
    #
    # However, for subprocess/CLI-wrapped failures (dbt, spark-submit, etc) the *real* root-cause
    # is often printed to stdout/stderr BEFORE Python raises the wrapper exception. For those cases
    # we intentionally include a pre-context window.
    lookback = 500
    around_tb = lines[max(0, last_tb - lookback) : min(len(lines), last_tb + 40)]
    around_text = "\n".join(around_tb).lower()
    looks_like_cli = ("dbt" in around_text) or ("subprocess" in around_text) or any(
        _DBT_ERROR_HINT_RE.search(ln) for ln in around_tb
    )
    pre = 140 if looks_like_cli else 0
    start = max(0, last_tb - pre)
    end = min(len(lines), last_tb + max_lines)
    window = lines[start:end]

    # If this looks like a dbt/CLI-style failure, prefer a compact excerpt that includes:
    # - the most relevant "dbt adapter/database error" block
    # - plus the traceback block (so stack parsing still works)
    #
    # This keeps prompts smaller while preserving the actual root-cause text (often printed BEFORE the traceback).
    hint_idxs: list[int] = [i for i, ln in enumerate(window) if _DBT_ERROR_HINT_RE.search(ln)]
    if hint_idxs:
        last_hint = hint_idxs[-1]
        # Keep a small neighborhood around the best hint.
        hint_pre = 25
        hint_post = 25
        a0 = max(0, last_hint - hint_pre)
        a1 = min(len(window), last_hint + hint_post + 1)
        hint_block = window[a0:a1]

        # Keep the traceback block starting at the traceback line (relative to `window`)
        tb_rel = last_tb - start
        tb_block = window[max(0, tb_rel) :]

        excerpt_lines: list[str] = []
        # Preserve order and avoid duplicates (some logs overlap).
        for ln in hint_block + ["", "----- traceback -----"] + tb_block:
            if excerpt_lines and excerpt_lines[-1] == ln:
                continue
            excerpt_lines.append(ln)
        excerpt = "\n".join(excerpt_lines[-max(120, max_lines) :])
        return AirflowLogParseResult(excerpt=excerpt, has_traceback=True)

    excerpt = "\n".join(window)
    return AirflowLogParseResult(excerpt=excerpt, has_traceback=True)




from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional, Tuple

from patchit.models import StackFrame


_PY_FRAME_RE = re.compile(r'^\s*File\s+"(?P<file>[^"]+)",\s+line\s+(?P<line>\d+),\s+in\s+(?P<fn>.+?)\s*$')
_EXC_LINE_RE = re.compile(r"^\s*(?P<exc>[A-Za-z_][A-Za-z0-9_\.]*):\s*(?P<msg>.*)\s*$")


@dataclass(frozen=True)
class ParsedException:
    error_type: str
    message: str
    frames: List[StackFrame]
    excerpt: str


def parse_python_traceback(text: str, *, max_frames: int = 30) -> Optional[ParsedException]:
    """
    Best-effort parser for common Python tracebacks as found in Airflow task logs.
    """
    lines = text.splitlines()
    frames: List[StackFrame] = []
    error_type = "UnknownError"
    message = ""

    # Scan for "Traceback (most recent call last):" then collect frames
    tb_idx = None
    for i, ln in enumerate(lines):
        if "Traceback (most recent call last):" in ln:
            tb_idx = i
            break
    if tb_idx is None:
        return None

    # Parse frames + code lines
    i = tb_idx + 1
    while i < len(lines) and len(frames) < max_frames:
        m = _PY_FRAME_RE.match(lines[i])
        if m:
            file_path = m.group("file")
            line_no = int(m.group("line"))
            fn = m.group("fn").strip()
            code_line = None
            if i + 1 < len(lines) and lines[i + 1].startswith("    "):
                code_line = lines[i + 1].strip()
                i += 1
            frames.append(StackFrame(file_path=file_path, line_number=line_no, function=fn, code_line=code_line))
        else:
            em = _EXC_LINE_RE.match(lines[i])
            if em:
                error_type = em.group("exc").split(".")[-1]
                message = em.group("msg")
                break
        i += 1

    # IMPORTANT:
    # - `text` is usually already "reduced" by `extract_error_excerpt()` (Airflow log parser) and includes
    #   critical pre-context for subprocess/CLI-wrapped failures (dbt, spark-submit, etc).
    # - Do NOT re-shrink the excerpt to only the traceback window; doing so frequently drops the *real*
    #   underlying error message that was printed BEFORE Python raised (ex: dbt adapter "Database Error").
    excerpt = text
    return ParsedException(error_type=error_type, message=message, frames=frames, excerpt=excerpt)


def parse_first_exception(text: str) -> Tuple[str, str]:
    """
    Fallback: try to find a last-line ExceptionType: message pattern.
    """
    for ln in reversed(text.splitlines()[-200:]):
        m = _EXC_LINE_RE.match(ln)
        if m:
            return (m.group("exc").split(".")[-1], m.group("msg"))
    return ("UnknownError", "Could not parse exception from logs")




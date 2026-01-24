from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class WriteResult:
    final_path: str
    tmp_path: Optional[str]
    mode: str


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def atomic_write_json(path: str, payload: Dict[str, Any]) -> WriteResult:
    """
    Safe write: write to temp then atomic rename.
    """
    p = Path(path)
    ensure_dir(str(p.parent))
    tmp = str(p.with_suffix(p.suffix + ".tmp"))
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    return WriteResult(final_path=path, tmp_path=tmp, mode="atomic_tmp_rename")


def write_partial_json(path: str, payload: Dict[str, Any], *, truncate_bytes: int = 40) -> WriteResult:
    """
    Intentional failure injection: write malformed/truncated JSON and rename to final.
    """
    p = Path(path)
    ensure_dir(str(p.parent))
    tmp = str(p.with_suffix(p.suffix + ".tmp"))
    raw = json.dumps(payload)
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(raw[:truncate_bytes])
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    return WriteResult(final_path=path, tmp_path=tmp, mode="partial_truncated_then_rename")


def write_racy_incomplete_then_finish_later(
    path: str,
    payload: Dict[str, Any],
    *,
    first_bytes: int = 25,
    delay_s: float = 5.0,
) -> WriteResult:
    """
    Intentional failure injection: write to final path directly and return early,
    then complete write after delay in a background thread. Downstream may read
    partial file and fail JSON parsing.
    """
    p = Path(path)
    ensure_dir(str(p.parent))
    raw = json.dumps(payload)

    with open(path, "w", encoding="utf-8") as f:
        f.write(raw[:first_bytes])
        f.flush()
        os.fsync(f.fileno())

    def _finish() -> None:
        time.sleep(delay_s)
        with open(path, "a", encoding="utf-8") as f2:
            f2.write(raw[first_bytes:])
            f2.flush()
            os.fsync(f2.fileno())

    t = threading.Thread(target=_finish, daemon=True)
    t.start()

    return WriteResult(final_path=path, tmp_path=None, mode="race_write_final_then_finish_later")




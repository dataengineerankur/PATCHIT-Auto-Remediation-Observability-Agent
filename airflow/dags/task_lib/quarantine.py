from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class QuarantineResult:
    ok: bool
    quarantine_dir: str
    metadata_path: str


def quarantine_run(
    *,
    run_dir: str,
    quarantine_root: str,
    reason: str,
    failing_task_id: Optional[str],
    extra: Optional[Dict[str, Any]] = None,
) -> QuarantineResult:
    os.makedirs(quarantine_root, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    qdir = os.path.join(quarantine_root, f"{ts}_{os.path.basename(run_dir)}")
    shutil.copytree(run_dir, qdir, dirs_exist_ok=True)

    meta = {
        "timestamp": ts,
        "run_dir": run_dir,
        "failing_task_id": failing_task_id,
        "reason": reason,
        "extra": extra or {},
    }
    meta_path = os.path.join(qdir, "quarantine_metadata.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    return QuarantineResult(ok=True, quarantine_dir=qdir, metadata_path=meta_path)




from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Dict

import duckdb


@dataclass(frozen=True)
class CanaryResult:
    ok: bool
    details: str


def _sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def canary_compare(*, duckdb_path: str, out_path: str) -> CanaryResult:
    """
    Compares current output checksum with last-known-good (LKG).
    If LKG missing, creates it (first run becomes baseline).
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    con = duckdb.connect(duckdb_path)
    try:
        rows = con.execute("select * from analytics.stg_api_payload order by run_id").fetchall()
    finally:
        con.close()

    checksum = _sha(json.dumps(rows, default=str))
    if not os.path.exists(out_path):
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump({"checksum": checksum, "rows": len(rows)}, f)
        return CanaryResult(ok=True, details="initialized_lkg")

    with open(out_path, "r", encoding="utf-8") as f:
        prev: Dict[str, str] = json.load(f)
    if prev.get("checksum") != checksum:
        raise RuntimeError(f"Canary failed: output checksum changed vs LKG (prev={prev.get('checksum')} new={checksum})")
    return CanaryResult(ok=True, details="matches_lkg")




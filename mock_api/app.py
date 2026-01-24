from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Query


app = FastAPI(title="PATCHIT Mock API", version="0.1.0")


def _stable_int(seed: str) -> int:
    h = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return int(h[:8], 16)


@dataclass(frozen=True)
class Scenario:
    name: str


@app.get("/payload")
def payload(
    run_id: str = Query(...),
    scenario: str = Query("auto"),
    seed: Optional[str] = Query(None),
) -> Dict[str, Any]:
    """
    A deterministic mock API. Behavior depends on `scenario` and `run_id`.
    Scenarios:
      - good
      - partial_json (downstream file writer will create malformed/partial file)
      - schema_drift (rename userId -> user_id; sometimes remove amount)
      - temporal_error (returns 500 with error payload)
      - auto (deterministically pick based on run_id)
    """
    seed_str = seed or os.getenv("MOCK_API_SEED", "seed")
    if scenario == "auto":
        n = _stable_int(f"{seed_str}:{run_id}") % 5
        scenario = ["good", "partial_json", "schema_drift", "temporal_error", "good"][n]

    if scenario == "temporal_error":
        raise HTTPException(status_code=500, detail={"error": "upstream_unavailable", "run_id": run_id})

    base = {
        "eventTime": "2025-12-14T00:00:00Z",
        "userId": f"user_{_stable_int(run_id) % 1000}",
        "amount": 12.34,
        "items": [{"sku": "A1", "qty": 1}],
        "runId": run_id,
    }

    if scenario == "schema_drift":
        drift = dict(base)
        drift["user_id"] = drift.pop("userId")
        # Occasionally remove/rename a field deterministically
        if (_stable_int(f"drift:{seed_str}:{run_id}") % 2) == 0:
            drift.pop("amount", None)
        return drift

    # good / partial_json both return valid json; partialness injected at file-write stage
    return base


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True}




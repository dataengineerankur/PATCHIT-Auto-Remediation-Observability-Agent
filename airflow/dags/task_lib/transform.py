from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from typing import Any, Dict

from task_lib.errors import DataContractError


@dataclass(frozen=True)
class TransformResult:
    out_csv_path: str
    rows: int


def transform_payload_to_rows(*, raw_path: str, out_csv_path: str, scenario: str) -> TransformResult:
    """
    Intentionally brittle in Phase-1 test DAG:
      - schema_drift: downstream expects `userId` and fails if API renamed to `user_id`
    """
    with open(raw_path, "r", encoding="utf-8") as f:
        payload: Dict[str, Any] = json.load(f)

    # Schema drift injection: if API renamed field, this raises KeyError and fails task
    user_id = payload.get("userId") or payload.get("user_id")
    if user_id is None:
        raise DataContractError("Missing required field userId/user_id")

    amount = payload.get("amount")  # may be missing -> downstream/dbt may fail later
    event_time = payload.get("eventTime")
    run_id = payload.get("runId")

    os.makedirs(os.path.dirname(out_csv_path), exist_ok=True)
    with open(out_csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["run_id", "event_time", "user_id", "amount"])
        w.writeheader()
        w.writerow({"run_id": run_id, "event_time": event_time, "user_id": user_id, "amount": amount})

    return TransformResult(out_csv_path=out_csv_path, rows=1)




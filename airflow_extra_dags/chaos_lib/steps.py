from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class ChaosArtifacts:
    run_dir: str
    raw_path: str
    out_path: str


def make_paths(run_id: str) -> ChaosArtifacts:
    run_dir = f"/opt/airflow/data/chaos_runs/{run_id}"
    raw_path = f"{run_dir}/raw/input.json"
    out_path = f"{run_dir}/out/result.json"
    return ChaosArtifacts(run_dir=run_dir, raw_path=raw_path, out_path=out_path)


def write_input_json(*, path: str, payload: Dict[str, Any], make_dirs: bool = True) -> str:
    if make_dirs:
        os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    return path


def write_malformed_json(*, path: str) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("{")  # intentionally malformed
    return path


def read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def do_key_error(payload: Dict[str, Any]) -> str:
    # Intentionally brittle: triggers KeyError for missing key
    return payload["missing_key"]


def do_file_not_found(path: str) -> str:
    # Intentionally brittle: triggers FileNotFoundError for missing file
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def do_type_error() -> int:
    # Intentionally brittle: TypeError (None + int)
    x = None
    return x + 1  # type: ignore[operator]


def do_attribute_error() -> str:
    # Intentionally brittle: AttributeError on None
    obj = None
    return obj.strip()  # type: ignore[union-attr]


def do_division_by_zero() -> float:
    return 1 / 0


def write_result(path: str, result: Dict[str, Any]) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(result, f)
    return path



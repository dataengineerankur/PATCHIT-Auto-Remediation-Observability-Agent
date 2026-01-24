from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict

from jsonschema import Draft202012Validator

from task_lib.errors import DataContractError


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    details: str


def validate_json_contract(*, raw_path: str, schema_path: str) -> ValidationResult:
    try:
        with open(raw_path, "r", encoding="utf-8") as f:
            payload: Dict[str, Any] = json.load(f)
    except json.JSONDecodeError as e:
        raise DataContractError(f"JSONDecodeError reading raw payload: {e}") from e

    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)

    v = Draft202012Validator(schema)
    errors = sorted(v.iter_errors(payload), key=lambda e: e.path)
    if errors:
        msg = "; ".join([f"{'/'.join([str(p) for p in e.path])}: {e.message}" for e in errors[:5]])
        raise DataContractError(f"JSON schema contract validation failed: {msg}")

    return ValidationResult(ok=True, details="contract_ok")




from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class FailureScenario(str, Enum):
    good = "good"
    partial_json = "partial_json"
    schema_drift = "schema_drift"
    temporal_error = "temporal_error"
    race_condition = "race_condition"
    dbt_missing_column = "dbt_missing_column"

    auto = "auto"


def _stable_int(seed: str) -> int:
    h = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return int(h[:8], 16)


@dataclass(frozen=True)
class ScenarioSelector:
    seed: str = "patchit"

    def choose(self, *, run_id: str, requested: Optional[str]) -> FailureScenario:
        if requested and requested != FailureScenario.auto:
            return FailureScenario(requested)

        n = _stable_int(f"{self.seed}:{run_id}") % 6
        return [
            FailureScenario.good,
            FailureScenario.partial_json,
            FailureScenario.schema_drift,
            FailureScenario.temporal_error,
            FailureScenario.race_condition,
            FailureScenario.dbt_missing_column,
        ][n]




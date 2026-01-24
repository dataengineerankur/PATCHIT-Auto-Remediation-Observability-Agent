from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class ChaosScenario(str, Enum):
    random = "random"
    key_error = "key_error"
    file_not_found = "file_not_found"
    type_error = "type_error"
    attribute_error = "attribute_error"
    json_decode = "json_decode"
    division_by_zero = "division_by_zero"


def _stable_int(seed: str) -> int:
    return int(hashlib.sha256(seed.encode("utf-8")).hexdigest()[:8], 16)


@dataclass(frozen=True)
class ChaosSelector:
    seed: str = "chaos"

    def choose(self, run_id: str, requested: Optional[str]) -> ChaosScenario:
        if requested and requested != ChaosScenario.random:
            return ChaosScenario(requested)
        i = _stable_int(f"{self.seed}:{run_id}") % 6
        return [
            ChaosScenario.key_error,
            ChaosScenario.file_not_found,
            ChaosScenario.type_error,
            ChaosScenario.attribute_error,
            ChaosScenario.json_decode,
            ChaosScenario.division_by_zero,
        ][i]



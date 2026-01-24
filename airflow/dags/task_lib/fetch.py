from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict

import httpx

from task_lib.errors import UpstreamApiError


@dataclass(frozen=True)
class FetchConfig:
    base_url: str
    timeout_s: float = 10.0


def fetch_payload(*, run_id: str, scenario: str) -> Dict[str, Any]:
    base_url = os.environ.get("MOCK_API_BASE_URL", "http://mock_api:8099")
    cfg = FetchConfig(base_url=base_url)
    url = f"{cfg.base_url}/payload"

    with httpx.Client(timeout=cfg.timeout_s) as client:
        r = client.get(url, params={"run_id": run_id, "scenario": scenario})
        # temporal failure: treat 5xx as retryable typed error
        if r.status_code >= 500:
            raise UpstreamApiError(f"status_code={r.status_code} error payload: {r.text}")
        r.raise_for_status()
        return r.json()




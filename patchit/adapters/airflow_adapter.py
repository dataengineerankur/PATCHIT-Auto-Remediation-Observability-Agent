from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx

from patchit.models import Platform, UniversalFailureEvent


@dataclass(frozen=True)
class AirflowAdapter:
    """
    MVP adapter interface:
    - Can generate UniversalFailureEvents from Airflow REST API (polling).
    - In docker-compose demo, we also support file:// log URIs passed by the DAG.
    """

    base_url: str
    username: str
    password: str
    timeout_s: float = 10.0

    def _auth_header(self) -> Dict[str, str]:
        token = base64.b64encode(f"{self.username}:{self.password}".encode("utf-8")).decode("ascii")
        return {"Authorization": f"Basic {token}"}

    def list_failed_task_instances(self, dag_id: str, dag_run_id: str) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
        with httpx.Client(timeout=self.timeout_s) as client:
            r = client.get(url, headers=self._auth_header())
            r.raise_for_status()
            items = r.json().get("task_instances", [])
        return [ti for ti in items if ti.get("state") in ("failed", "upstream_failed")]

    def list_dag_runs(self, dag_id: str, *, limit: int = 200) -> List[Dict[str, Any]]:
        """
        Best-effort helper for orchestration scenarios (TriggerDagRunOperator):
        given a target dag_id, list recent dag runs so PATCHIT can locate the triggered run.
        """
        # Airflow API ordering can vary by version/config; ensure we fetch *most recent* runs first.
        # Prefer logical_date when supported, fallback to execution_date.
        base = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns?limit={int(limit)}"
        urls = [
            base + "&order_by=-logical_date",
            base + "&order_by=-execution_date",
            base,
        ]
        items: list[Any] = []
        with httpx.Client(timeout=self.timeout_s) as client:
            last_err: Exception | None = None
            for url in urls:
                try:
                    r = client.get(url, headers=self._auth_header())
                    r.raise_for_status()
                    items = (r.json() or {}).get("dag_runs", []) or []
                    last_err = None
                    break
                except Exception as e:  # noqa: BLE001
                    last_err = e
                    continue
            if last_err is not None and not items:
                raise last_err
        return [dr for dr in items if isinstance(dr, dict)]

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """
        Fetch a single DAG run payload from Airflow.
        Useful for enriching poller events with dag_run.conf (e.g., scenario flags).
        """
        url = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"
        with httpx.Client(timeout=self.timeout_s) as client:
            r = client.get(url, headers=self._auth_header())
            r.raise_for_status()
            data = r.json() or {}
        return data if isinstance(data, dict) else {}

    def build_failure_event(
        self,
        *,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        try_number: Optional[int] = None,
        log_uri: Optional[str] = None,
        artifact_uris: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> UniversalFailureEvent:
        return UniversalFailureEvent(
            event_id=f"airflow:{dag_id}:{dag_run_id}:{task_id}:{try_number or 0}",
            platform=Platform.airflow,
            pipeline_id=dag_id,
            run_id=dag_run_id,
            task_id=task_id,
            try_number=try_number,
            log_uri=log_uri,
            artifact_uris=artifact_uris or [],
            metadata=metadata or {},
        )




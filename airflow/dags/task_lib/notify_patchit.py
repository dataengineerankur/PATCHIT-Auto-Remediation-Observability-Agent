from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx


@dataclass(frozen=True)
class NotifyConfig:
    endpoint: str
    # Converge-to-green can take longer (agent + sandbox verification loops), so we allow a higher timeout.
    timeout_s: float = 60.0


def _translate_log_path_for_patchit(airflow_log_path: str) -> str:
    """
    Airflow containers write logs to /opt/airflow/logs (mounted to host path airflow/logs).
    PATCHIT container sees the repo mounted at /opt/patchit/repo, so we translate to that mount.
    """
    src = os.environ.get("PATCHIT_LOGS_TRANSLATE_FROM", "/opt/airflow/logs")
    dst = os.environ.get("PATCHIT_LOGS_TRANSLATE_TO", "/opt/patchit/repo/airflow/logs")
    if airflow_log_path.startswith(src):
        rel = airflow_log_path[len(src) :].lstrip("/")
        return f"{dst}/{rel}"
    return airflow_log_path


def _translate_artifact_uri_for_patchit(uri: str) -> str:
    """
    Translate file:// URIs that point to /opt/airflow/data so PATCHIT can read them from its repo mount.
    In this repo's docker-compose, airflow's /opt/airflow/data is host-mounted at ./airflow/data,
    which PATCHIT sees at /opt/patchit/repo/airflow/data.
    """
    if uri.startswith("file://"):
        path = uri[len("file://") :]
        src = os.environ.get("PATCHIT_DATA_TRANSLATE_FROM", "/opt/airflow/data")
        dst = os.environ.get("PATCHIT_DATA_TRANSLATE_TO", "/opt/patchit/repo/airflow/data")
        if path.startswith(src):
            rel = path[len(src) :].lstrip("/")
            return f"file://{dst}/{rel}"
    return uri


def notify_failure_to_patchit(
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    try_number: int,
    log_path: str,
    artifact_uris: Optional[List[str]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    endpoint = os.environ.get("PATCHIT_ENDPOINT")
    if not endpoint:
        return

    cfg = NotifyConfig(endpoint=endpoint)
    translated = _translate_log_path_for_patchit(log_path)
    translated_artifacts = [_translate_artifact_uri_for_patchit(u) for u in (artifact_uris or [])]
    payload = {
        "event_id": f"airflow:{dag_id}:{run_id}:{task_id}:{try_number}",
        "platform": "airflow",
        "pipeline_id": dag_id,
        "run_id": run_id,
        "task_id": task_id,
        "try_number": try_number,
        "status": "failed",
        "log_uri": f"file://{translated}",
        "artifact_uris": translated_artifacts,
        "metadata": metadata or {},
    }

    with httpx.Client(timeout=cfg.timeout_s) as client:
        client.post(cfg.endpoint, json=payload)




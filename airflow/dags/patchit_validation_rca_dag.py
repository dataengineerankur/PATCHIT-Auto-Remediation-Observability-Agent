from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

from task_lib.notify_patchit import notify_failure_to_patchit


def _run_dir(run_id: str) -> str:
    return f"/opt/airflow/data/validation_runs/{run_id}"


def _artifact_path(run_id: str) -> str:
    return f"{_run_dir(run_id)}/producer/artifact.json"


def _atomic_write_json(path: str, obj: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f)
    os.replace(tmp, path)


def _partial_write_json(path: str, obj: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Non-atomic/corrupt write: write only the first bytes of a JSON string.
    s = json.dumps(obj)
    with open(path, "w", encoding="utf-8") as f:
        f.write(s[: max(1, len(s) // 3)])


def _airflow_log_path(*, dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
    return f"/opt/airflow/logs/dag_id={dag_id}/run_id={run_id}/task_id={task_id}/attempt={try_number}.log"


def _on_failure_callback(context: Dict[str, Any]) -> None:
    try:
        ti = context["task_instance"]
        dag_id = context["dag"].dag_id
        run_id = context["dag_run"].run_id
        task_id = ti.task_id
        try_number = ti.try_number

        # Always attach the producer artifact so PATCHIT can reason across tasks.
        art = _artifact_path(run_id)
        artifact_uris = [f"file://{art}"]

        log_path = _airflow_log_path(dag_id=dag_id, run_id=run_id, task_id=task_id, try_number=try_number)
        notify_failure_to_patchit(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            try_number=try_number,
            log_path=log_path,
            artifact_uris=artifact_uris,
            metadata={
                "scenario": (context.get("dag_run").conf or {}).get("scenario", "unknown"),
                "purpose": "root_cause_across_tasks",
                "producer_task_id": "producer_write",
                "consumer_task_id": "consumer_read",
            },
        )
    except Exception as e:  # noqa: BLE001
        print(f"notify_failure_to_patchit failed: {e}")


def choose_scenario(**context: Any) -> str:
    return str((context["dag_run"].conf or {}).get("scenario", "non_atomic_write"))


def producer_write(**context: Any) -> str:
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    path = _artifact_path(run_id)

    payload = {"run_id": run_id, "kind": "artifact", "schema_version": 1, "rows": [{"id": 1, "value": 123}]}

    if scenario == "non_atomic_write":
        _partial_write_json(path, payload)
    elif scenario == "wrong_shape_200":
        # Valid JSON but wrong shape (looks like API error envelope).
        _atomic_write_json(path, {"error": {"code": "bad_request", "message": "synthetic"}, "ok": True})
    else:
        _atomic_write_json(path, payload)

    return path


def consumer_read(**context: Any) -> str:
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    path = _artifact_path(run_id)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Consumer intentionally fails "downstream" even though root cause is producer quality.
    if scenario == "wrong_shape_200":
        # This should have been caught at the producer boundary (schema validation).
        if "rows" not in data:
            raise ValueError("Data contract failure: expected key `rows` in producer artifact")
    # For non_atomic_write, json.load will raise JSONDecodeError.
    return "ok"


default_args = {
    "owner": "patchit",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=1),
    "on_failure_callback": _on_failure_callback,
}


with DAG(
    dag_id="patchit_validation_rca_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="Synthetic RCA drill: producer writes bad artifact, consumer fails; PATCHIT should fix producer boundary.",
) as dag:
    choose = PythonOperator(task_id="choose_scenario", python_callable=choose_scenario)
    prod = PythonOperator(task_id="producer_write", python_callable=producer_write)
    cons = PythonOperator(task_id="consumer_read", python_callable=consumer_read)

    choose >> prod >> cons




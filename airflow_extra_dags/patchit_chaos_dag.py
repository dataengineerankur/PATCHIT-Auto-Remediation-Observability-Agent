from __future__ import annotations

from datetime import datetime, timedelta
import os
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

from extra_dags.chaos_lib.scenarios import ChaosSelector, ChaosScenario
from extra_dags.chaos_lib.steps import (
    do_attribute_error,
    do_division_by_zero,
    do_file_not_found,
    do_key_error,
    do_type_error,
    make_paths,
    read_json,
    write_input_json,
    write_malformed_json,
    write_result,
)

# Reuse PATCHIT notifier from the main demo DAG (already mounted in /opt/airflow/dags/task_lib)
from task_lib.notify_patchit import notify_failure_to_patchit


def _airflow_log_path(*, dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
    return (
        f"/opt/airflow/logs/"
        f"dag_id={dag_id}/run_id={run_id}/task_id={task_id}/attempt={try_number}.log"
    )


def _resolve_existing_log_path(*, dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
    # Same logic as the main demo DAG: try reported try_number then walk backwards.
    for n in range(int(try_number), 0, -1):
        p = _airflow_log_path(dag_id=dag_id, run_id=run_id, task_id=task_id, try_number=n)
        if os.path.exists(p):
            return p
    return _airflow_log_path(dag_id=dag_id, run_id=run_id, task_id=task_id, try_number=try_number)


def _on_failure_callback(context: Dict[str, Any]) -> None:
    try:
        ti = context["task_instance"]
        dag_id = context["dag"].dag_id
        run_id = context["dag_run"].run_id
        task_id = ti.task_id
        try_number = ti.try_number
        log_path = _resolve_existing_log_path(dag_id=dag_id, run_id=run_id, task_id=task_id, try_number=try_number)

        paths = make_paths(run_id)
        artifact_uris = [
            f"file://{paths.raw_path}",
            f"file://{paths.out_path}",
        ]
        notify_failure_to_patchit(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            try_number=try_number,
            log_path=log_path,
            artifact_uris=artifact_uris,
            metadata={"exception": str(context.get("exception"))},
        )
    except Exception as e:  # noqa: BLE001
        print(f"PATCHIT chaos DAG notify callback failed: {e}")


def choose_scenario(**context: Any) -> str:
    run_id = context["dag_run"].run_id
    requested = (context["dag_run"].conf or {}).get("scenario", "random")
    seed = (context["dag_run"].conf or {}).get("seed", "chaos")
    return ChaosSelector(seed=seed).choose(run_id, requested).value


def prepare_inputs(**context: Any) -> Dict[str, str]:
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    paths = make_paths(run_id)
    # Ensure run directory exists even if we fail early
    os.makedirs(os.path.dirname(paths.raw_path), exist_ok=True)
    os.makedirs(os.path.dirname(paths.out_path), exist_ok=True)

    if scenario == ChaosScenario.json_decode.value:
        write_malformed_json(path=paths.raw_path)
    else:
        write_input_json(path=paths.raw_path, payload={"run_id": run_id, "ok": True})

    return {"raw_path": paths.raw_path, "out_path": paths.out_path, "run_dir": paths.run_dir}


def chaos_step(**context: Any) -> str:
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    meta = context["ti"].xcom_pull(task_ids="prepare_inputs")
    raw = meta["raw_path"]

    payload = read_json(raw)

    if scenario == ChaosScenario.key_error.value:
        return do_key_error(payload)
    if scenario == ChaosScenario.file_not_found.value:
        return do_file_not_found("/opt/airflow/data/chaos_runs/definitely_missing.txt")
    if scenario == ChaosScenario.type_error.value:
        return str(do_type_error())
    if scenario == ChaosScenario.attribute_error.value:
        return do_attribute_error()
    if scenario == ChaosScenario.division_by_zero.value:
        return str(do_division_by_zero())

    return "ok"


def finalize(**context: Any) -> str:
    meta = context["ti"].xcom_pull(task_ids="prepare_inputs")
    result = {"ok": True, "value": context["ti"].xcom_pull(task_ids="chaos_step")}
    return write_result(meta["out_path"], result)


default_args = {
    "owner": "patchit",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    "on_failure_callback": _on_failure_callback,
}


with DAG(
    dag_id="patchit_chaos_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="Chaos DAG in a separate folder to generate diverse failures for PATCHIT heuristic remediation.",
) as dag:
    choose = PythonOperator(task_id="choose_scenario", python_callable=choose_scenario)
    prep = PythonOperator(task_id="prepare_inputs", python_callable=prepare_inputs)
    chaos = PythonOperator(task_id="chaos_step", python_callable=chaos_step)
    fin = PythonOperator(task_id="finalize", python_callable=finalize)

    choose >> prep >> chaos >> fin



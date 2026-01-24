from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from task_lib.failure_injector import FailureScenario, ScenarioSelector
from task_lib.fetch import fetch_payload
from task_lib.io_utils import (
    atomic_write_json,
    write_partial_json,
    write_racy_incomplete_then_finish_later,
)
from task_lib.validate import validate_json_contract
from task_lib.transform import transform_payload_to_rows
from task_lib.load import load_csv_to_duckdb
from task_lib.dbt_runner import run_dbt_build
from task_lib.canary import canary_compare
from task_lib.quarantine import quarantine_run
from task_lib.notify_patchit import notify_failure_to_patchit


def _base_run_dir(run_id: str) -> str:
    return f"/opt/airflow/data/runs/{run_id}"


def _schema_path() -> str:
    return "/opt/airflow/dags/schemas/api_payload.schema.json"


def _dbt_project_dir() -> str:
    return "/opt/airflow/dbt"


def _airflow_log_path(*, dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
    """
    Airflow file task logs (default file task handler) in this docker stack land under:
      /opt/airflow/logs/dag_id=<dag_id>/run_id=<run_id>/task_id=<task_id>/attempt=<try>.log
    """
    return (
        f"/opt/airflow/logs/"
        f"dag_id={dag_id}/run_id={run_id}/task_id={task_id}/attempt={try_number}.log"
    )


def _resolve_existing_log_path(*, dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
    """
    Some Airflow states can yield a try_number that exceeds the number of attempt log files present
    (e.g., after clears/manual retries). We pick the highest existing attempt <= try_number.
    """
    # Try the reported try_number first, then walk backwards.
    for n in range(int(try_number), 0, -1):
        p = _airflow_log_path(dag_id=dag_id, run_id=run_id, task_id=task_id, try_number=n)
        if os.path.exists(p):
            return p
    # Fallback to the reported value even if missing (PATCHIT will record fetch failure).
    return _airflow_log_path(dag_id=dag_id, run_id=run_id, task_id=task_id, try_number=try_number)


def _on_failure_callback(context: Dict[str, Any]) -> None:
    try:
        ti = context["task_instance"]
        dag_id = context["dag"].dag_id
        run_id = context["dag_run"].run_id
        task_id = ti.task_id
        try_number = ti.try_number

        # Prefer reporting the *primary* failing task instead of recovery tasks like quarantine_bad_run.
        dag_run = context["dag_run"]
        primary_candidates = [
            "fetch_payload",
            "write_raw_payload",
            "validate_contract",
            "transform_payload",
            "load_to_duckdb",
            "dbt_build",
            "canary_compare",
        ]
        primary_failed = None
        for tid in primary_candidates:
            ti2 = dag_run.get_task_instance(tid)
            if ti2 and ti2.state in ("failed", "upstream_failed"):
                primary_failed = tid
                break

        report_task_id = primary_failed or task_id
        report_try = try_number
        if report_task_id != task_id:
            # If we're in quarantine_bad_run etc, use the primary task's latest try number for log path resolution.
            ti_primary = dag_run.get_task_instance(report_task_id)
            if ti_primary and ti_primary.try_number:
                report_try = ti_primary.try_number

        log_path = _resolve_existing_log_path(dag_id=dag_id, run_id=run_id, task_id=report_task_id, try_number=report_try)

        run_dir = _base_run_dir(run_id)
        artifact_uris = [
            f"file://{run_dir}/raw/payload.json",
            f"file://{run_dir}/staged/payload.csv",
            f"file://{run_dir}/warehouse/warehouse.duckdb",
        ]
        notify_failure_to_patchit(
            dag_id=dag_id,
            run_id=run_id,
            task_id=report_task_id,
            try_number=report_try,
            log_path=log_path,
            artifact_uris=artifact_uris,
            metadata={
                "exception": str(context.get("exception")),
                "original_failing_task_id": task_id,
                "primary_failed_task_id": primary_failed,
            },
        )
    except Exception as e:  # noqa: BLE001
        # Never let observability callbacks break the task runner; log to task logs for visibility.
        print(f"PATCHIT notify callback failed: {e}")


def choose_scenario(**context: Any) -> str:
    run_id = context["dag_run"].run_id
    requested = (context["dag_run"].conf or {}).get("scenario", "auto")
    seed = (context["dag_run"].conf or {}).get("seed", "patchit")
    scenario = ScenarioSelector(seed=seed).choose(run_id=run_id, requested=requested)
    return scenario.value


def fetch_task(**context: Any) -> Dict[str, Any]:
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")

    # For race_condition: API can still be "good"; race is in file writer
    api_scenario = scenario if scenario in ("good", "schema_drift", "temporal_error") else "good"
    return fetch_payload(run_id=run_id, scenario=api_scenario)


def write_raw(**context: Any) -> Dict[str, Any]:
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    payload = context["ti"].xcom_pull(task_ids="fetch_payload")

    run_dir = _base_run_dir(run_id)
    raw_path = f"{run_dir}/raw/payload.json"

    if scenario == FailureScenario.partial_json.value:
        res = write_partial_json(raw_path, payload, truncate_bytes=50)
    elif scenario == FailureScenario.race_condition.value:
        res = write_racy_incomplete_then_finish_later(raw_path, payload, first_bytes=25, delay_s=6.0)
    else:
        res = atomic_write_json(raw_path, payload)

    return {"raw_path": raw_path, "write_mode": res.mode}


def validate_contract(**context: Any) -> str:
    meta = context["ti"].xcom_pull(task_ids="write_raw_payload")
    return validate_json_contract(raw_path=meta["raw_path"], schema_path=_schema_path()).details


def transform(**context: Any) -> Dict[str, Any]:
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    meta = context["ti"].xcom_pull(task_ids="write_raw_payload")
    out_csv = f"{_base_run_dir(run_id)}/staged/payload.csv"
    res = transform_payload_to_rows(raw_path=meta["raw_path"], out_csv_path=out_csv, scenario=scenario)
    return {"csv_path": res.out_csv_path, "rows": res.rows}


def load(**context: Any) -> Dict[str, Any]:
    run_id = context["dag_run"].run_id
    scenario = context["ti"].xcom_pull(task_ids="choose_scenario")
    tmeta = context["ti"].xcom_pull(task_ids="transform_payload")
    duckdb_path = f"{_base_run_dir(run_id)}/warehouse/warehouse.duckdb"
    res = load_csv_to_duckdb(csv_path=tmeta["csv_path"], duckdb_path=duckdb_path, scenario=scenario)
    return {"duckdb_path": res.duckdb_path, "table": res.table}


def dbt_build(**context: Any) -> str:
    run_id = context["dag_run"].run_id
    lmeta = context["ti"].xcom_pull(task_ids="load_to_duckdb")
    profiles_dir = f"{_base_run_dir(run_id)}/dbt_profiles"
    res = run_dbt_build(project_dir=_dbt_project_dir(), profiles_dir=profiles_dir, duckdb_path=lmeta["duckdb_path"])
    return res.stdout[-2000:]


def canary(**context: Any) -> str:
    run_id = context["dag_run"].run_id
    lmeta = context["ti"].xcom_pull(task_ids="load_to_duckdb")
    lkg = "/opt/airflow/data/lkg/analytics_checksum.json"
    res = canary_compare(duckdb_path=lmeta["duckdb_path"], out_path=lkg)
    return res.details


def decide_branch(**context: Any) -> str:
    """
    Branch into success vs quarantine based on upstream failures.
    """
    dag_run = context["dag_run"]
    # Any of these tasks failing triggers quarantine branch (task trigger rules set accordingly)
    failed = []
    for tid in ["validate_contract", "transform_payload", "load_to_duckdb", "dbt_build", "canary_compare"]:
        ti = dag_run.get_task_instance(tid)
        if ti and ti.state in ("failed", "upstream_failed"):
            failed.append(tid)
    return "quarantine_bad_run" if failed else "success_end"


def quarantine(**context: Any) -> str:
    dag_run = context["dag_run"]
    run_id = dag_run.run_id
    run_dir = _base_run_dir(run_id)
    failing: Optional[str] = None
    for tid in ["validate_contract", "transform_payload", "load_to_duckdb", "dbt_build", "canary_compare"]:
        ti = dag_run.get_task_instance(tid)
        if ti and ti.state in ("failed", "upstream_failed"):
            failing = tid
            break
    res = quarantine_run(
        run_dir=run_dir,
        quarantine_root="/opt/airflow/data/quarantine",
        reason="pipeline_failure",
        failing_task_id=failing,
    )
    return res.metadata_path


default_args = {
    "owner": "patchit",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    "on_failure_callback": _on_failure_callback,
}


with DAG(
    dag_id="patchit_complex_test_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="Complex failure-injection DAG for PATCHIT MVP (8+ tasks, branching, artifacts, dbt, canary, quarantine).",
) as dag:
    start = EmptyOperator(task_id="start")

    def ensure_run_dir(**context: Any) -> str:
        run_id = context["dag_run"].run_id
        run_dir = _base_run_dir(run_id)
        os.makedirs(f"{run_dir}/raw", exist_ok=True)
        os.makedirs(f"{run_dir}/staged", exist_ok=True)
        os.makedirs(f"{run_dir}/warehouse", exist_ok=True)
        return run_dir

    init_run_dir = PythonOperator(
        task_id="init_run_dir",
        python_callable=ensure_run_dir,
    )

    choose = PythonOperator(
        task_id="choose_scenario",
        python_callable=choose_scenario,
    )

    fetch = PythonOperator(
        task_id="fetch_payload",
        python_callable=fetch_task,
        retries=3,  # allow temporal_error to show retry behavior
        retry_delay=timedelta(seconds=5),
    )

    write_raw_payload = PythonOperator(
        task_id="write_raw_payload",
        python_callable=write_raw,
    )

    validate_contract_task = PythonOperator(
        task_id="validate_contract",
        python_callable=validate_contract,
    )

    transform_task = PythonOperator(
        task_id="transform_payload",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=load,
    )

    dbt_task = PythonOperator(
        task_id="dbt_build",
        python_callable=dbt_build,
    )

    canary_task = PythonOperator(
        task_id="canary_compare",
        python_callable=canary,
    )

    branch = BranchPythonOperator(task_id="branch_success_vs_quarantine", python_callable=decide_branch)

    quarantine_task = PythonOperator(
        task_id="quarantine_bad_run",
        python_callable=quarantine,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    success_end = EmptyOperator(task_id="success_end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    start >> init_run_dir >> choose >> fetch >> write_raw_payload >> validate_contract_task >> transform_task >> load_task >> dbt_task >> canary_task
    canary_task >> branch
    branch >> [quarantine_task, success_end]
    quarantine_task >> end
    success_end >> end




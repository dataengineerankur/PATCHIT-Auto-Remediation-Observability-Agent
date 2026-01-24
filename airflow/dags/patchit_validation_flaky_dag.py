from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator


def flaky_api_call(**context: Any) -> str:
    """
    Synthetic flaky scenario:
    - attempt 1: raise a 429-like transient error
    - attempt 2: succeed

    Expected PATCHIT behavior: no PR (the DAG should succeed after retry, so poller shouldn't ingest).
    """
    ti = context["ti"]
    if int(getattr(ti, "try_number", 1)) <= 1:
        raise RuntimeError("HTTP 429 Rate limit exceeded (synthetic); retry should succeed")
    return "ok"


default_args = {
    "owner": "patchit",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=2),
}


with DAG(
    dag_id="patchit_validation_flaky_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="Synthetic flaky drill: transient 429 on first try, succeeds on retry; PATCHIT should do nothing.",
) as dag:
    t = PythonOperator(task_id="flaky_api_call", python_callable=flaky_api_call)




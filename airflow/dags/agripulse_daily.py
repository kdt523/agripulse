"""Airflow DAG: trigger the Databricks `agripulse_daily` workflow, then verify freshness in ops.freshness_status.

Databricks Workflows is the primary scheduler (06:30 IST). This DAG shows cross-tool orchestration: Airflow owns the
trigger plus an independent post-run freshness gate with an SLA-miss callback.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sensors.python import PythonSensor

JOB_ID = int(os.environ.get("AGRIPULSE_JOB_ID", "0"))
WAREHOUSE_ID = os.environ.get("AGRIPULSE_SQL_WAREHOUSE_ID", "")


def _sql(statement: str) -> list[list]:
    """Run a statement through the Databricks SQL Statement API using the Airflow connection."""
    import requests

    hook = DatabricksHook("databricks_default")
    host = hook.host if hook.host.startswith("http") else f"https://{hook.host}"
    token = hook.databricks_conn.password
    r = requests.post(
        f"{host}/api/2.0/sql/statements",
        headers={"Authorization": f"Bearer {token}"},
        json={"warehouse_id": WAREHOUSE_ID, "statement": statement, "wait_timeout": "50s"},
        timeout=90,
    )
    r.raise_for_status()
    return r.json().get("result", {}).get("data_array", [])


def freshness_ok() -> bool:
    """True when the newest freshness check (< 2h old) has no breached source."""
    rows = _sql(
        "SELECT count_if(breached), count(*) FROM agripulse.ops.freshness_status "
        "WHERE checked_at > current_timestamp() - INTERVAL 2 HOURS"
    )
    breached, total = (int(rows[0][0]), int(rows[0][1])) if rows else (0, 0)
    return total > 0 and breached == 0


def sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"SLA missed for {task_list}: pipeline did not finish in time (see the Databricks run page).")


with DAG(
    "agripulse_daily",
    start_date=datetime(2026, 11, 1),
    schedule="0 2 * * *",  # 02:00 UTC = 07:30 IST: an independent trigger/safety net after the 06:30 IST Databricks run
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10), "sla": timedelta(hours=4)},
    sla_miss_callback=sla_miss,
    tags=["agripulse"],
) as dag:
    run_workflow = DatabricksRunNowOperator(task_id="run_agripulse_daily", databricks_conn_id="databricks_default", job_id=JOB_ID)
    check = PythonSensor(task_id="freshness_gate", python_callable=freshness_ok, poke_interval=120, timeout=1800, mode="reschedule")
    done = PythonOperator(task_id="done", python_callable=lambda: print("AgriPulse daily run verified"))
    run_workflow >> check >> done

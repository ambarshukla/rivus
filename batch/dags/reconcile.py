"""Airflow DAGs for the batch side.

reconcile_hourly   at ten past every hour, checks the hour that has just ended
reconcile_daily    at half past midnight, summarises the day before

Airflow only decides when and in what order; the work is done by
`python -m rivus_batch <step>`, run in its own Python environment so that dbt's
dependencies and Airflow's never have to agree.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

BATCH = "cd /opt/rivus && /home/airflow/batch-venv/bin/python -m rivus_batch"

# The run at 14:10 handles 13:00-14:00: take the run's time, drop the minutes,
# go back one hour. Re-running a past run redoes the same hour.
HOUR = "{{ (logical_date.replace(minute=0, second=0, microsecond=0) - macros.timedelta(hours=1)).strftime('%Y-%m-%dT%H:00:00') }}"
YESTERDAY = "{{ (logical_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"

DEFAULTS = {"retries": 2, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="reconcile_hourly",
    description="Rebuild the last hour's bars from raw trades and compare with the stream",
    schedule="10 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=DEFAULTS,
    tags=["rivus"],
):
    export_raw = BashOperator(task_id="export_raw", bash_command=f"{BATCH} export-raw --hour {HOUR}")
    export_stream = BashOperator(task_id="export_stream", bash_command=f"{BATCH} export-stream --hour {HOUR}")
    # dbt build = run the models, then the tests; a failing test fails the task.
    dbt_build = BashOperator(task_id="dbt_build", bash_command=f"{BATCH} dbt --hour {HOUR}")
    load = BashOperator(task_id="load_results", bash_command=f"{BATCH} load --hour {HOUR}")

    [export_raw, export_stream] >> dbt_build >> load

with DAG(
    dag_id="reconcile_daily",
    description="Summarise yesterday's hourly reconciliations",
    schedule="30 0 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    is_paused_upon_creation=False,
    default_args=DEFAULTS,
    tags=["rivus"],
):
    BashOperator(task_id="rollup", bash_command=f"{BATCH} rollup --day {YESTERDAY}")

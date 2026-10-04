"""Monitoring DAG: alerts when the data goes stale.

Why a separate DAG: the main pipeline cannot report on its own absence. If it is paused, mis-scheduled or keeps
being skipped, no task fails and no email is sent. This DAG runs every 6 hours and runs `dbt source freshness`,
which compares now() with the newest `ingested_at` of each raw table against the thresholds in
dbt_project/models/staging/sources.yml. A source past its `error_after` threshold fails the task, which sends the
failure email.

It checks the data itself, not Airflow's bookkeeping, so it also catches runs that "succeeded" while loading nothing.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator

from alerts import notify_failure
from dbt_cmd import dbt_command


@dag(
    dag_id="pipeline_monitor",
    description="Alerts when raw data is older than the freshness thresholds",
    schedule="0 */6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 0,  # staleness is not transient; alert immediately
        "execution_timeout": timedelta(minutes=5),
        "on_failure_callback": notify_failure,
    },
    tags=["ns", "monitoring"],
)
def pipeline_monitor():
    """Run the dbt source freshness check."""
    BashOperator(task_id="dbt_source_freshness", bash_command=dbt_command("source freshness"))


pipeline_monitor()

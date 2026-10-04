"""Daily ELT DAG: NS API -> raw (Postgres) -> dbt staging -> dbt marts.

Task graph:

    extract_stations ──┐
                       ├──> dbt_build_staging ──> dbt_build_marts
    extract_disruptions┘

* The extract tasks run in parallel and land unchanged API data in `raw` (scripts/scraping/raw_loader.py).
* `dbt build` runs models AND tests in dependency order. A failing test fails the task and skips everything
  downstream, so bad data never reaches the marts.

Reliability settings:
    * Extract tasks: 3 retries with exponential backoff. API and network failures are usually transient.
    * dbt tasks: NO retries. A failing data-quality test or a SQL error is deterministic; retrying only delays the
      alert by minutes.
    * Task timeout 10 min, run timeout 1 h, `max_active_runs=1` (two runs never write to the same tables).
    * `catchup=False`: the API only knows "now", so replaying past schedule dates would store today's data
      under old dates. Loads are labelled with the real fetch date instead (see raw_loader.py).
    * Email on final failure of any task; summary email and healthcheck ping on success (scripts/alerts.py).

Staleness (the pipeline silently not running) is watched by the separate `pipeline_monitor` DAG.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from alerts import notify_failure, notify_success
from dbt_cmd import dbt_command

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="elt_pipeline",
    description="ELT pipeline for Dutch train data",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    default_args=default_args,
    on_success_callback=notify_success,
    tags=["ns", "elt"],
)
def elt_pipeline():
    """Define the tasks and their dependencies for the daily train-data pipeline."""

    @task
    def extract_stations(**context) -> int:
        """Load today's NL station snapshot into `raw.stations`; returns the row count.

        The extract module is imported inside the task so that parsing this file (which the scheduler does
        constantly) does not need database or API credentials.
        """
        from scraping.extract_ns import extract_stations as run

        return run(context["dag"].dag_id, context["run_id"], context["logical_date"])

    @task
    def extract_disruptions(**context) -> int:
        """Load today's active disruptions into `raw.disruptions` (may be 0); returns the row count."""
        from scraping.extract_ns import extract_disruptions as run

        return run(context["dag"].dag_id, context["run_id"], context["logical_date"])

    dbt_staging = BashOperator(
        task_id="dbt_build_staging",
        bash_command=dbt_command("build --select staging"),
        retries=0,
    )
    dbt_marts = BashOperator(
        task_id="dbt_build_marts",
        bash_command=dbt_command("build --select marts"),
        retries=0,
    )

    [extract_stations(), extract_disruptions()] >> dbt_staging >> dbt_marts


elt_pipeline()

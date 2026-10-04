"""DAG integrity tests: the DAGs must import cleanly and keep their intended shape."""

from datetime import timedelta

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag(dags_folder):
    """Parse the DAG folder once for all tests in this module (parsing is slow)."""
    return DagBag(dag_folder=dags_folder, include_examples=False)


@pytest.fixture(scope="module")
def dag(dagbag):
    """The `elt_pipeline` DAG, read from memory (get_dag() would query Airflow's metadata database)."""
    return dagbag.dags["elt_pipeline"]


def test_no_import_errors(dagbag):
    """All DAG files import without errors (a broken DAG silently disappears from the UI)."""
    assert dagbag.import_errors == {}


def test_expected_tasks(dag):
    """The pipeline contains exactly the four intended tasks."""
    assert set(dag.task_ids) == {"extract_stations", "extract_disruptions", "dbt_build_staging", "dbt_build_marts"}


def test_dependencies(dag):
    """Both extracts feed staging, and staging feeds the marts (so bad data never reaches them)."""
    assert set(dag.get_task("dbt_build_staging").upstream_task_ids) == {"extract_stations", "extract_disruptions"}
    assert dag.get_task("dbt_build_marts").upstream_task_ids == {"dbt_build_staging"}


def test_extract_tasks_retry_with_backoff(dag):
    """API and network failures are transient: extract tasks retry with exponential backoff."""
    for task_id in ("extract_stations", "extract_disruptions"):
        task = dag.get_task(task_id)
        assert task.retries >= 3
        assert task.retry_exponential_backoff is True


def test_dbt_tasks_do_not_retry(dag):
    """Failing data tests are deterministic; retrying only delays the alert."""
    for task_id in ("dbt_build_staging", "dbt_build_marts"):
        assert dag.get_task(task_id).retries == 0


def test_every_task_has_timeout_and_failure_alert(dag):
    """Nothing can hang forever, and every task alerts on final failure."""
    for task in dag.tasks:
        assert task.execution_timeout is not None
        assert task.on_failure_callback is not None


def test_run_level_safety_settings(dag):
    """One run at a time, a run timeout, no catch-up (the API only knows the present), success summary."""
    assert dag.max_active_runs == 1
    assert dag.dagrun_timeout == timedelta(hours=1)
    assert dag.catchup is False
    assert dag.on_success_callback is not None


def test_monitor_dag_checks_freshness(dagbag):
    """The monitoring DAG exists, runs the freshness check, and alerts immediately on failure."""
    monitor = dagbag.dags["pipeline_monitor"]
    task = monitor.get_task("dbt_source_freshness")
    assert "source freshness" in task.bash_command
    assert task.retries == 0 and task.on_failure_callback is not None

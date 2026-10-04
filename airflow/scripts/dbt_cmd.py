"""Helpers to build dbt command lines for Airflow BashOperators.

dbt lives in its own virtualenv inside the Airflow image (see airflow/Dockerfile) so its dependencies cannot
clash with Airflow's. Connection settings come from the DBT_USER/DBT_PASSWORD/DB_* environment variables read by
dbt_project/profiles.yml.
"""

DBT_BIN = "/opt/airflow/dbt_venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"


def dbt_command(args: str) -> str:
    """Return a full shell command, e.g. `dbt_command("build --select staging")`."""
    return f"{DBT_BIN} {args} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"

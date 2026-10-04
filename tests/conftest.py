"""Test setup: make the Airflow scripts importable and point database tests at the separate test database."""

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent

# Repo layout: <root>/airflow/{scripts,dags}. Container layout: /opt/airflow/{scripts,dags}.
AIRFLOW_DIR = next(base for base in (ROOT / "airflow", ROOT) if (base / "dags").is_dir())

for sub in ("scripts", "dags"):
    path = str(AIRFLOW_DIR / sub)
    if path not in sys.path:
        sys.path.insert(0, path)


@pytest.fixture(scope="session", autouse=True)
def use_test_database():
    """Run every test against DB_NAME_TEST (set in docker-compose) so tests can never touch real data.

    In CI there is only one throwaway database, so DB_NAME_TEST is unset and DB_NAME is used as is.
    """
    test_db = os.getenv("DB_NAME_TEST")
    if not test_db:
        yield
        return
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("DB_NAME", test_db)
        yield


@pytest.fixture(scope="session")
def dags_folder() -> str:
    """Path of the folder containing the DAG files."""
    return str(AIRFLOW_DIR / "dags")

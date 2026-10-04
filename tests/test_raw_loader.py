"""Tests for raw_loader.

Pure helpers and validation need no database. The load tests use the real PostgreSQL *test* database (see
conftest.py) with the real migrated tables, and are skipped when it is not reachable. They write to `raw.stations`
only inside that test database and clean up before and after each test.
"""

from datetime import datetime, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from db_connection import DatabaseManager
from scraping import raw_loader

ENDPOINT = "stations"  # a real migrated table; the test database is isolated from real data
DAY_1 = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
DAY_2 = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)


def test_sha256_is_deterministic():
    """Same input gives the same 64-char hash; different input gives a different one."""
    assert raw_loader._sha256("a|b") == raw_loader._sha256("a|b")
    assert raw_loader._sha256("a|b") != raw_loader._sha256("a|c")
    assert len(raw_loader._sha256("x")) == 64


@pytest.mark.parametrize("name", ["Robert; DROP TABLE x", "1abc", "a-b", "UPPER", ""])
def test_invalid_endpoint_is_rejected(name):
    """Names that could inject SQL or are not plain lowercase identifiers are rejected before any DB access."""
    with pytest.raises(ValueError, match="Invalid endpoint"):
        raw_loader.load_snapshot(name, lambda: [], "id", "dag", "run", DAY_1)


@pytest.fixture
def db():
    """A DatabaseManager on a clean test database; skips the test when no database is reachable."""
    try:
        manager = DatabaseManager()
        reachable = manager.test_connection()
    except ValueError:
        reachable = False
    if not reachable:
        pytest.skip("database not available")

    def cleanup():
        manager.execute_statement(f"DELETE FROM raw.{ENDPOINT}")
        manager.execute_statement("DELETE FROM raw.ingestion_log WHERE dag_id = 'pytest'")

    cleanup()
    yield manager
    cleanup()
    manager.close()


def _load(records, snapshot_at=DAY_1, run_id="run", **kwargs):
    """Run load_snapshot for the test endpoint with a fake `fetch` returning `records`."""
    return raw_loader.load_snapshot(
        ENDPOINT, lambda: records, "id", "pytest", run_id, snapshot_at, snapshot_at=snapshot_at, **kwargs
    )


def _count(db, where="TRUE"):
    """Count rows in the test table, optionally filtered by a trusted SQL condition."""
    return db.execute_query(f"SELECT count(*) FROM raw.{ENDPOINT} WHERE {where}")[0][0]


def _log_rows(db, run_id=None):
    """Return (status, rows_loaded, error) of the test's ingestion_log rows, optionally for one run."""
    where = "dag_id = 'pytest'" + (f" AND run_id = '{run_id}'" if run_id else "")
    return db.execute_query(f"SELECT status, rows_loaded, error FROM raw.ingestion_log WHERE {where}")


def test_load_inserts_rows_and_logs_success(db):
    """A normal load inserts every record and writes a success row to ingestion_log."""
    assert _load([{"id": "a"}, {"id": "b"}]) == 2
    assert _count(db) == 2
    assert _log_rows(db) == [("success", 2, None)]


def test_reload_same_day_replaces_instead_of_duplicating(db):
    """Idempotency: loading the same day twice leaves the same rows, and record_key stays stable."""
    _load([{"id": "a"}, {"id": "b"}], run_id="first")
    first_key = db.execute_query(f"SELECT record_key FROM raw.{ENDPOINT} WHERE business_key = 'a'")[0][0]

    _load([{"id": "a"}, {"id": "b"}], run_id="second")

    assert _count(db) == 2
    assert _count(db, "run_id = 'second'") == 2
    assert db.execute_query(f"SELECT record_key FROM raw.{ENDPOINT} WHERE business_key = 'a'")[0][0] == first_key


def test_other_days_are_untouched(db):
    """Reloading one day only replaces that day's partition; other days stay as they were."""
    _load([{"id": "a"}], DAY_1)
    _load([{"id": "a"}], DAY_2)
    _load([{"id": "a"}, {"id": "b"}], DAY_1)
    assert _count(db, "snapshot_date = '2026-01-02'") == 1
    assert _count(db, "snapshot_date = '2026-01-01'") == 2


def test_snapshot_date_is_the_fetch_day_in_amsterdam(db):
    """23:30 UTC on 1 Jan is already 2 Jan in the Netherlands (UTC+1), so that is the snapshot day."""
    _load([{"id": "a"}], datetime(2026, 1, 1, 23, 30, tzinfo=timezone.utc))
    assert db.execute_query(f"SELECT snapshot_date::text FROM raw.{ENDPOINT}") == [("2026-01-02",)]


def test_snapshot_date_ignores_the_airflow_logical_date(db):
    """Replaying an old logical date must not label today's data with an old date."""
    old_logical_date = datetime(2020, 5, 5, tzinfo=timezone.utc)
    raw_loader.load_snapshot(
        ENDPOINT, lambda: [{"id": "a"}], "id", "pytest", "run", old_logical_date, snapshot_at=DAY_1
    )
    assert db.execute_query(f"SELECT snapshot_date::text FROM raw.{ENDPOINT}") == [("2026-01-01",)]


def test_duplicate_business_key_rolls_back_and_keeps_old_data(db):
    """A duplicate key fails the load, rolls back the DELETE too, and logs a short sanitised error."""
    _load([{"id": "a"}, {"id": "b"}], run_id="good")

    with pytest.raises(IntegrityError):
        _load([{"id": "a"}, {"id": "a"}], run_id="bad")

    assert _count(db) == 2
    assert _count(db, "run_id = 'good'") == 2
    ((status, rows, error),) = _log_rows(db, "bad")
    assert status == "failed" and rows is None
    assert "duplicate key" in error
    assert "\n" not in error and "[SQL" not in error and "parameters" not in error


def test_empty_snapshot_fails_by_default_and_is_logged(db):
    """An empty API result raises (so no day is wiped with nothing) and is logged as failed."""
    with pytest.raises(ValueError, match="0 records"):
        _load([])
    ((status, _, error),) = _log_rows(db)
    assert status == "failed" and "0 records" in error


def test_allow_empty_clears_the_day_and_logs_success(db):
    """For endpoints where empty is legitimate, an empty result clears that day and logs 0 rows."""
    _load([{"id": "a"}], run_id="before")
    assert _load([], run_id="empty", allow_empty=True) == 0
    assert _count(db) == 0
    assert ("success", 0, None) in _log_rows(db, "empty")


def test_logging_failure_does_not_mask_the_original_error(db, monkeypatch):
    """If writing the failure log also fails (for example the database is down), the real error still surfaces."""
    real_log = raw_loader._log

    def broken_log(conn, dag_id, run_id, endpoint, logical_date, snapshot_date, status, *args, **kwargs):
        if status == "failed":
            raise RuntimeError("log table unavailable")
        return real_log(conn, dag_id, run_id, endpoint, logical_date, snapshot_date, status, *args, **kwargs)

    monkeypatch.setattr(raw_loader, "_log", broken_log)

    def failing_fetch():
        raise ValueError("api down")

    with pytest.raises(ValueError, match="api down"):
        raw_loader.load_snapshot(ENDPOINT, failing_fetch, "id", "pytest", "run", DAY_1, snapshot_at=DAY_1)


def test_payload_is_stored_unchanged(db):
    """Raw keeps the API record exactly as received, including nested data and non-ASCII text."""
    record = {"id": "a", "nested": {"x": [1, 2]}, "text": "é"}
    _load([record])
    with db.engine.connect() as conn:
        stored = conn.execute(text(f"SELECT payload FROM raw.{ENDPOINT}")).scalar()
    assert stored == record

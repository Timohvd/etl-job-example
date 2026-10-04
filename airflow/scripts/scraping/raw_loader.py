"""Generic loader that lands API snapshots in the `raw` schema (the "L" of ELT).

It stores already-fetched API records unchanged (as JSONB) in `raw.<endpoint>` plus bookkeeping columns. Parsing,
renaming and typing happen later in dbt, so raw always holds exactly what the API returned and every downstream
model can be rebuilt without calling the API again. Table layouts are defined in sql/migrations/; this module
never creates tables.

Snapshot semantics. The NS endpoints return "the situation right now", not history. A load is therefore labelled
with the day it was FETCHED (`snapshot_date`, in Europe/Amsterdam), not with Airflow's logical date. That keeps the
history truthful: re-running or backfilling an old Airflow date cannot overwrite an old day with today's data.
Re-running within the same day replaces that day's snapshot, which makes loads idempotent.

Duplicates are prevented twice:
    * a load deletes its day's partition and inserts the new rows in ONE transaction, and
    * `record_key = sha256(endpoint|snapshot_date|business_key)` has a UNIQUE index, so a duplicate (for example
      the API returning the same station twice) fails the insert and rolls back the whole load.

Every load attempt, successful or failed, is recorded in `raw.ingestion_log` with a sanitised error message.
"""

import hashlib
import json
import logging
import re
import time
from datetime import datetime
from typing import Callable, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

from db_connection import DatabaseManager
from safe_errors import safe_error

log = logging.getLogger(__name__)

SNAPSHOT_TZ = ZoneInfo("Europe/Amsterdam")

# Table names are spliced into SQL text (identifiers cannot be bound as parameters): plain lowercase only.
_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")


def _sha256(value: str) -> str:
    """Hex SHA-256 digest. One-way: the same input always gives the same output, but it cannot be reversed
    (a hash, not encryption). Used for deterministic row ids and change detection."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _log(conn, dag_id, run_id, endpoint, logical_date, snapshot_date, status, rows, duration, error=None):
    """Insert one row into raw.ingestion_log using an existing connection (joins the caller's transaction)."""
    conn.execute(
        text(
            "INSERT INTO raw.ingestion_log "
            "(dag_id, run_id, endpoint, logical_date, snapshot_date, status, rows_loaded, duration_s, error) "
            "VALUES (:dag_id, :run_id, :endpoint, :logical_date, :snapshot_date, :status, :rows, :duration, :error)"
        ),
        {
            "dag_id": dag_id,
            "run_id": run_id,
            "endpoint": endpoint,
            "logical_date": logical_date,
            "snapshot_date": snapshot_date,
            "status": status,
            "rows": rows,
            "duration": round(duration, 2),
            "error": error,
        },
    )


def _log_failure(db, dag_id, run_id, endpoint, logical_date, snapshot_date, duration, exc) -> None:
    """Record a failed load in its own transaction, without ever raising.

    If the database itself is the problem, writing the log row fails too. That secondary error is only logged so
    the original exception, the real cause, is the one that reaches Airflow.
    """
    try:
        with db.begin() as conn:
            _log(conn, dag_id, run_id, endpoint, logical_date, snapshot_date, "failed", None, duration, safe_error(exc))
    except Exception:
        log.exception("Could not write the failure to raw.ingestion_log")


def load_snapshot(
    endpoint: str,
    fetch: Callable[[], list],
    key_field: str,
    dag_id: str,
    run_id: str,
    logical_date: datetime,
    *,
    allow_empty: bool = False,
    snapshot_at: Optional[datetime] = None,
) -> int:
    """Fetch an API snapshot and replace today's partition of `raw.<endpoint>` with it.

    Steps:
        1. Validate the table name.
        2. Call `fetch()`. An empty result raises unless `allow_empty`, because for most endpoints zero records
           means an upstream problem, and loading it would wipe the day's data.
        3. Build one row per record: business key, deterministic `record_key`, `payload_hash`, full payload.
        4. In ONE transaction: delete the day's partition, insert the rows, write a "success" log row.
        5. On any error: the transaction rolls back (earlier data stays intact), a "failed" log row is written
           separately, and the original exception is re-raised so Airflow retries and alerts.

    Args:
        endpoint: Endpoint and table name; must exist as `raw.<endpoint>` (create it with a migration).
        fetch: Zero-argument callable returning the records, e.g. `NSClient().get_stations`.
        key_field: Field that uniquely identifies a record (for example "code" or "id").
        dag_id: Airflow DAG id, stored in the log.
        run_id: Airflow run id, stored on every row and in the log.
        logical_date: Airflow's logical date; stored in the log for tracing only, it does NOT decide the partition.
        allow_empty: True when an empty snapshot is legitimate (for example "no active disruptions today").
            The day's partition is then cleared and a success row with 0 rows is logged.
        snapshot_at: The fetch moment. Defaults to now; pass a value in tests.

    Returns:
        The number of rows loaded.

    Raises:
        ValueError: Invalid table name, or an empty result while `allow_empty` is False.
        KeyError: A record has no `key_field`.
        sqlalchemy.exc.IntegrityError: Duplicate business keys within one snapshot.
        Exception: Anything `fetch()` raises (network or HTTP errors).
    """
    if not _IDENT.match(endpoint):
        raise ValueError(f"Invalid endpoint/table name: {endpoint}")

    snapshot_at = (snapshot_at or datetime.now(SNAPSHOT_TZ)).astimezone(SNAPSHOT_TZ)
    snapshot_date = snapshot_at.date()

    db = DatabaseManager()
    start = time.monotonic()
    try:
        records = fetch()
        if not records and not allow_empty:
            raise ValueError(f"NS API returned 0 records for {endpoint}")

        rows = []
        for record in records:
            business_key = str(record[key_field])
            # sort_keys makes the JSON text, and therefore payload_hash, stable.
            payload = json.dumps(record, sort_keys=True)
            rows.append(
                {
                    "d": snapshot_date,
                    "fetched_at": snapshot_at,
                    "run_id": run_id,
                    "business_key": business_key,
                    "record_key": _sha256(f"{endpoint}|{snapshot_date.isoformat()}|{business_key}"),
                    "payload_hash": _sha256(payload),
                    "payload": payload,
                }
            )

        with db.begin() as conn:
            conn.execute(text(f"DELETE FROM raw.{endpoint} WHERE snapshot_date = :d"), {"d": snapshot_date})
            if rows:
                conn.execute(
                    text(
                        f"INSERT INTO raw.{endpoint} "
                        "(snapshot_date, fetched_at, run_id, business_key, record_key, payload_hash, payload) "
                        "VALUES (:d, :fetched_at, :run_id, :business_key, :record_key, :payload_hash, "
                        "CAST(:payload AS JSONB))"
                    ),
                    rows,
                )
            _log(
                conn, dag_id, run_id, endpoint, logical_date, snapshot_date, "success", len(rows),
                time.monotonic() - start,
            )  # fmt: skip
        return len(rows)
    except Exception as exc:
        _log_failure(db, dag_id, run_id, endpoint, logical_date, snapshot_date, time.monotonic() - start, exc)
        raise
    finally:
        db.close()

"""Per-endpoint extract functions called by the Airflow DAG.

Each function connects one NS API call (from `NSClient`) to the generic loader (`load_snapshot`) by naming the
target table and the field that identifies a record. A new endpoint needs: a method on `NSClient`, a migration
that creates `raw.<endpoint>`, a function here, and a task in the DAG. The loading logic is never copied.

No Airflow imports here, so these can be run and tested from a plain Python shell.
"""

from datetime import datetime

from scraping.ns_client import NSClient
from scraping.raw_loader import load_snapshot


def extract_stations(dag_id: str, run_id: str, logical_date: datetime) -> int:
    """Load a snapshot of all NL stations into `raw.stations` (business key: station `code`, e.g. "UT").

    An empty result is an error: the Netherlands always has stations, so zero rows means the API or our call is
    broken.

    Returns:
        The number of rows loaded.
    """
    return load_snapshot("stations", NSClient().get_stations, "code", dag_id, run_id, logical_date)


def extract_disruptions(dag_id: str, run_id: str, logical_date: datetime) -> int:
    """Load a snapshot of active disruptions into `raw.disruptions` (business key: disruption `id`, a UUID).

    An empty result is legitimate (a quiet day with no active disruptions), so it is allowed: the day's partition
    is cleared and a success row with 0 rows is logged. The same disruption appears in the snapshot of every day
    it is active; the marts use that to compute first-seen and last-seen dates.

    Returns:
        The number of rows loaded.
    """
    return load_snapshot(
        "disruptions", NSClient().get_disruptions, "id", dag_id, run_id, logical_date, allow_empty=True
    )

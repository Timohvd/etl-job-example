"""Email alerts and the optional dead man's switch for the ELT DAGs.

Callbacks:
    * `notify_failure`: attached to every task via `default_args`; fires once a task has failed after its retries.
    * `notify_success`: attached to the main DAG; emails a per-endpoint summary and pings an optional
      healthcheck URL (see below).

Mail goes through Airflow's `send_email` (SMTP settings come from AIRFLOW__SMTP__* in docker-compose.yml; MailHog
locally). The recipient is ALERT_EMAIL; when empty, alerts are skipped with a log warning.

Dead man's switch. Failure emails only help while Airflow itself is running. If the scheduler or the mail server
dies, nothing is sent and nothing looks wrong. Set HEALTHCHECK_URL to a monitor such as healthchecks.io: the DAG
pings it after every successful run, and the monitor alerts you when the pings STOP arriving.

Error text is reduced with `safe_error` so SQL and API payloads never end up in an inbox.
"""

import html
import logging
import os

import requests
from airflow.utils.email import send_email

from db_connection import DatabaseManager
from safe_errors import safe_error

log = logging.getLogger(__name__)


def _send(subject: str, body_html: str) -> None:
    """Send one HTML email to ALERT_EMAIL without ever raising.

    A callback that raises would hide the failure it reports, so sending problems are logged and swallowed.
    Callers must `html.escape` any text that did not originate in this module.
    """
    recipient = os.getenv("ALERT_EMAIL")
    if not recipient:
        log.warning("ALERT_EMAIL is not set; skipping alert: %s", subject)
        return
    try:
        send_email(to=[recipient], subject=subject, html_content=body_html)
    except Exception:
        log.exception("Failed to send alert email: %s", subject)


def _ping_healthcheck() -> None:
    """GET HEALTHCHECK_URL if configured (dead man's switch); never raises."""
    url = os.getenv("HEALTHCHECK_URL")
    if not url:
        return
    try:
        requests.get(url, timeout=10)
    except Exception:
        log.exception("Healthcheck ping failed")


def notify_failure(context: dict) -> None:
    """Email an alert when a task has failed after all retries (`on_failure_callback`).

    Airflow calls this once, after the final attempt, so there is one email per failed task and not one per retry.

    Args:
        context: Airflow task context. Used: `task_instance`, `exception`, `run_id`, `logical_date`.
    """
    ti = context["task_instance"]
    error = html.escape(safe_error(context.get("exception") or Exception("unknown error")))
    _send(
        subject=f"[ELT FAILED] {ti.dag_id}.{ti.task_id}",
        body_html=(
            f"<p>Task <b>{html.escape(ti.task_id)}</b> in DAG <b>{html.escape(ti.dag_id)}</b> failed "
            f"after {ti.try_number} attempt(s).</p>"
            f"<p>Run: {html.escape(context['run_id'])}<br>Logical date: {context['logical_date']}</p>"
            f"<p>Error:</p><pre>{error}</pre>"
            f'<p><a href="{html.escape(ti.log_url, quote=True)}">Task log</a></p>'
        ),
    )


def notify_success(context: dict) -> None:
    """On DAG success: email the rows loaded per endpoint and ping the healthcheck (`on_success_callback`).

    The summary comes from `raw.ingestion_log`, so it shows what was actually loaded, not what was intended.
    """
    run_id = context["run_id"]
    db = DatabaseManager()
    try:
        rows = db.execute_query(
            "SELECT endpoint, status, rows_loaded FROM raw.ingestion_log WHERE run_id = :run_id ORDER BY endpoint",
            {"run_id": run_id},
        )
    finally:
        db.close()

    lines = "".join(f"<li>{html.escape(e)}: {s}, {n} rows</li>" for e, s, n in rows) or "<li>no ingestion rows</li>"
    _send(
        subject=f"[ELT OK] {context['dag'].dag_id}",
        body_html=f"<p>Run {html.escape(run_id)} finished successfully.</p><ul>{lines}</ul>",
    )
    _ping_healthcheck()

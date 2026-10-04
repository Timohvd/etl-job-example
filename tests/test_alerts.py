"""Tests for the alert callbacks; email sending and HTTP are patched out."""

from datetime import datetime
from types import SimpleNamespace

import pytest

import alerts


@pytest.fixture
def sent(monkeypatch):
    """Capture (to, subject, body) of every email instead of sending it."""
    captured = []
    monkeypatch.setattr(
        alerts,
        "send_email",
        lambda to, subject, html_content: captured.append((to, subject, html_content)),
    )
    return captured


def _failure_context(error="boom"):
    """Build a minimal fake Airflow task context, like the one `on_failure_callback` receives."""
    ti = SimpleNamespace(dag_id="elt_pipeline", task_id="extract_stations", try_number=4, log_url="http://x/log")
    return {"task_instance": ti, "exception": ValueError(error), "run_id": "run1", "logical_date": datetime(2026, 1, 1)}


def test_failure_alert_contains_task_and_error(monkeypatch, sent):
    """The failure email goes to ALERT_EMAIL and names the task, the error and the attempt count."""
    monkeypatch.setenv("ALERT_EMAIL", "me@example.com")
    alerts.notify_failure(_failure_context("API down"))
    ((to, subject, body),) = sent
    assert to == ["me@example.com"]
    assert subject == "[ELT FAILED] elt_pipeline.extract_stations"
    assert "API down" in body and "4 attempt" in body


def test_failure_alert_escapes_html(monkeypatch, sent):
    """Error text is HTML-escaped so an error message cannot inject markup into the email."""
    monkeypatch.setenv("ALERT_EMAIL", "me@example.com")
    alerts.notify_failure(_failure_context("<script>alert(1)</script>"))
    body = sent[0][2]
    assert "<script>" not in body and "&lt;script&gt;" in body


def test_failure_alert_only_contains_the_first_line_of_the_error(monkeypatch, sent):
    """Multi-line errors (SQL and parameters in database errors) are cut so payloads never reach an inbox."""
    monkeypatch.setenv("ALERT_EMAIL", "me@example.com")
    alerts.notify_failure(_failure_context("duplicate key\n[SQL: INSERT ...]\n[parameters: {'payload': 'secret'}]"))
    body = sent[0][2]
    assert "duplicate key" in body and "secret" not in body and "[SQL" not in body


def test_no_recipient_means_no_email(monkeypatch, sent):
    """Without ALERT_EMAIL nothing is sent (and nothing crashes)."""
    monkeypatch.delenv("ALERT_EMAIL", raising=False)
    alerts.notify_failure(_failure_context())
    assert sent == []


def test_send_failure_never_raises(monkeypatch):
    """A broken mail server must not raise, otherwise it would hide the original task failure."""
    monkeypatch.setenv("ALERT_EMAIL", "me@example.com")

    def broken(**kwargs):
        raise OSError("smtp down")

    monkeypatch.setattr(alerts, "send_email", broken)
    alerts.notify_failure(_failure_context())  # must not raise


def test_healthcheck_is_pinged_when_configured(monkeypatch):
    """The dead man's switch pings HEALTHCHECK_URL, and does nothing when it is not set."""
    calls = []
    monkeypatch.setattr(alerts.requests, "get", lambda url, timeout: calls.append(url))

    monkeypatch.delenv("HEALTHCHECK_URL", raising=False)
    alerts._ping_healthcheck()
    assert calls == []

    monkeypatch.setenv("HEALTHCHECK_URL", "https://hc.example/ping/abc")
    alerts._ping_healthcheck()
    assert calls == ["https://hc.example/ping/abc"]


def test_healthcheck_failure_never_raises(monkeypatch):
    """An unreachable monitor must not fail the pipeline."""
    monkeypatch.setenv("HEALTHCHECK_URL", "https://hc.example/ping/abc")

    def broken(url, timeout):
        raise OSError("no network")

    monkeypatch.setattr(alerts.requests, "get", broken)
    alerts._ping_healthcheck()  # must not raise

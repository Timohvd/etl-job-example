"""Tests for safe_error: only the type and first line of an error may leave the process."""

from sqlalchemy.exc import IntegrityError

from safe_errors import safe_error


def test_only_first_line_is_kept():
    """Everything after the first line (SQL, parameters) is dropped."""
    assert safe_error(ValueError("first\nsecond\nthird")) == "ValueError: first"


def test_truncates_long_messages():
    """The result never exceeds the limit."""
    assert len(safe_error(ValueError("x" * 1000), limit=50)) == 50


def test_uses_the_driver_error_for_sqlalchemy_exceptions():
    """SQLAlchemy wraps the driver error; the message must come from the driver, without SQL and parameters."""

    class DriverError(Exception):
        pass

    wrapped = IntegrityError("INSERT ...", {"payload": "secret"}, DriverError("duplicate key\nDETAIL: Key (x)=(y)"))
    result = safe_error(wrapped)
    assert result == "DriverError: duplicate key"
    assert "secret" not in result


def test_empty_message():
    """An exception without a message still gives a readable result."""
    assert safe_error(RuntimeError()) == "RuntimeError: "

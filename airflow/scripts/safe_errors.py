"""Reduce raw error text to something safe to store and email.

Database errors from SQLAlchemy/psycopg2 append the failing SQL and its parameters (which here would be API
payloads) after the first line, and HTTP errors can embed URLs. Only the exception type and the first line of
its message are kept.
"""


def safe_error(exc: BaseException, limit: int = 300) -> str:
    """Return "<ExceptionType>: <first line of the message>", truncated to `limit` characters.

    For SQLAlchemy wrapper exceptions the underlying driver error (`exc.orig`) is used, since its first line is
    the actual database message (for example "duplicate key value violates unique constraint ...").
    """
    inner = getattr(exc, "orig", None) or exc
    lines = str(inner).strip().splitlines()
    first_line = lines[0] if lines else ""
    return f"{type(inner).__name__}: {first_line}"[:limit]

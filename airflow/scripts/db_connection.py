"""Shared SQLAlchemy database access for the Airflow scripts.

One small class, `DatabaseManager`, used by every script that talks to the project database, so connection
settings and transaction behaviour live in exactly one place.

Configuration comes only from environment variables (DB_USER, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT). In Docker
the scheduler receives the credentials of the least-privileged `loader` role (see docker-compose.yml and
sql/init/00_bootstrap.sh), so this code can write to the `raw` schema and nothing else. There are deliberately no
fallback defaults: a missing variable raises immediately instead of silently connecting somewhere unexpected.
"""

import os
from contextlib import AbstractContextManager
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine


class DatabaseManager:
    """Thin wrapper around a SQLAlchemy engine for the project database.

    Create one instance per task and call `close()` when finished (use `try/finally`). The engine keeps a small
    connection pool; `close()` releases it instead of leaving connections open until the process exits.

    Use `begin()` when several statements must succeed or fail together; use `execute_query` and
    `execute_statement` for single statements.
    """

    def __init__(self):
        """Build the engine from the DB_* environment variables.

        Raises:
            ValueError: If any variable is missing or empty. The message lists names only, never values.
        """
        required = ("DB_USER", "DB_PASSWORD", "DB_NAME", "DB_HOST", "DB_PORT")
        missing = [name for name in required if not os.getenv(name)]
        if missing:
            raise ValueError(f"Missing required DB environment variables: {', '.join(missing)}")

        url = (
            f"postgresql+psycopg2://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
            f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
        )
        self.engine: Engine = create_engine(url)

    def test_connection(self) -> bool:
        """Return True if `SELECT 1` succeeds, False on any error (usable as a health probe)."""
        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return True
        except Exception as e:
            print(f"Connection failed: {type(e).__name__}")
            return False

    def begin(self) -> AbstractContextManager[Connection]:
        """Open a transaction: `with db.begin() as conn:`. Commits on success, rolls back on any exception."""
        return self.engine.begin()

    def execute_query(self, query: str, params: Optional[dict] = None) -> list:
        """Run a SELECT and return all rows.

        Args:
            query: SQL with named placeholders (`:name`). Never format data into the string; placeholders are
                what protect against SQL injection.
            params: Values for the placeholders.
        """
        with self.engine.connect() as connection:
            return connection.execute(text(query), params or {}).fetchall()

    def execute_statement(self, statement: str, params: Optional[dict] = None) -> None:
        """Run one write statement in its own transaction (commit on success, rollback on error)."""
        with self.begin() as connection:
            connection.execute(text(statement), params or {})

    def close(self) -> None:
        """Dispose of the connection pool."""
        self.engine.dispose()

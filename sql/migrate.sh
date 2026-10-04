#!/usr/bin/env bash
# Minimal migration runner: applies sql/migrations/*.sql in filename order to each given database,
# exactly once, and remembers what ran in public.schema_migrations.
#
# Usage:  PGHOST=... PGUSER=... PGPASSWORD=... bash sql/migrate.sh <database> [<database>...]
#
# Rules (the same ones Flyway and Alembic enforce):
#   * A migration that has been applied is NEVER edited. To change the schema, add a new file.
#   * Each migration runs in one transaction: it fully applies or not at all.
#   * If an applied file's checksum changed, the runner stops, because that means history was rewritten.
set -euo pipefail

DIR="$(cd "$(dirname "$0")/migrations" && pwd)"

for db in "$@"; do
  echo "== $db"
  psql -v ON_ERROR_STOP=1 -d "$db" -q -c "
    CREATE TABLE IF NOT EXISTS public.schema_migrations (
      version    TEXT PRIMARY KEY,
      checksum   TEXT NOT NULL,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )"

  for file in "$DIR"/*.sql; do
    version="$(basename "$file" .sql)"
    checksum="$(sha256sum "$file" | cut -d' ' -f1)"
    applied="$(psql -d "$db" -At -c "SELECT checksum FROM public.schema_migrations WHERE version = '$version'")"

    if [ -z "$applied" ]; then
      echo "   applying $version"
      psql -v ON_ERROR_STOP=1 -1 -d "$db" -q -f "$file" \
        -c "INSERT INTO public.schema_migrations (version, checksum) VALUES ('$version', '$checksum')"
    elif [ "$applied" != "$checksum" ]; then
      echo "ERROR: $version was modified after it was applied to $db. Add a new migration instead." >&2
      exit 1
    else
      echo "   up to date $version"
    fi
  done
done

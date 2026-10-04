#!/usr/bin/env bash
# Bootstrap: roles, databases, schemas and privileges. Runs ONCE, automatically, the first time the
# Postgres container starts with an empty data volume (mounted into /docker-entrypoint-initdb.d/).
# It runs as POSTGRES_USER, the admin. Tables are NOT created here; see sql/migrations/.
#
# PRINCIPLE: least privilege. Every component gets its own role that can do only what it needs:
#   loader   Airflow extract tasks   INSERT/DELETE/SELECT on raw
#   dbt      dbt transformations     SELECT on raw, full control of staging and production
#   reader   APIs, BI tools          SELECT on production
#   airflow  Airflow's own metadata  owner of the separate `airflow` database
# The admin account is only for bootstrap, migrations and manual maintenance.
#
# Passwords arrive as environment variables (see docker-compose.yml) and are passed to psql as
# variables, so they are quoted safely and never written into a file.
set -euo pipefail

: "${POSTGRES_USER:?}" "${POSTGRES_DB:?}" "${LOADER_PASSWORD:?}" "${DBT_PASSWORD:?}" "${READER_PASSWORD:?}" "${AIRFLOW_DB_PASSWORD:?}"
TEST_DB="${POSTGRES_DB}_test"

# --- Pass 1: cluster-wide objects (roles and databases) ------------------------------------------
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -v loader_pw="$LOADER_PASSWORD" -v dbt_pw="$DBT_PASSWORD" \
  -v reader_pw="$READER_PASSWORD" -v airflow_pw="$AIRFLOW_DB_PASSWORD" \
  -v test_db="$TEST_DB" <<'SQL'
CREATE ROLE loader  LOGIN PASSWORD :'loader_pw';
CREATE ROLE dbt     LOGIN PASSWORD :'dbt_pw';
CREATE ROLE reader  LOGIN PASSWORD :'reader_pw';
CREATE ROLE airflow LOGIN PASSWORD :'airflow_pw';

-- Airflow keeps DAG runs, task states and users here, separate from our data.
CREATE DATABASE airflow OWNER airflow;
-- Tests run against their own copy so they can never touch real data.
CREATE DATABASE :"test_db";
SQL

# --- Pass 2: schemas and privileges, in the data database and in the test database ---------------
for db in "$POSTGRES_DB" "$TEST_DB"; do
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" -v db="$db" <<'SQL'
-- Nobody gets in by default; connecting is granted explicitly.
REVOKE ALL ON DATABASE :"db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db" TO loader, dbt, reader;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- The three layers of the pipeline.
CREATE SCHEMA raw;         -- unchanged API data
CREATE SCHEMA staging;     -- cleaned, typed (dbt)
CREATE SCHEMA production;  -- analysis-ready marts (dbt)

-- raw: tables are created by migrations (run as admin), so privileges are granted as DEFAULTS that
-- apply to every table the admin creates there later.
GRANT USAGE ON SCHEMA raw TO loader, dbt;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT SELECT, INSERT, DELETE ON TABLES TO loader;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT USAGE, SELECT ON SEQUENCES TO loader;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT SELECT ON TABLES TO dbt;

-- staging and production: dbt builds and owns the tables.
GRANT USAGE, CREATE ON SCHEMA staging, production TO dbt;

-- reader may only read the marts, including tables dbt creates in the future.
GRANT USAGE ON SCHEMA production TO reader;
ALTER DEFAULT PRIVILEGES FOR ROLE dbt IN SCHEMA production GRANT SELECT ON TABLES TO reader;
SQL
done

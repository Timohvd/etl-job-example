-- 0001: raw layer.
--
-- Every raw table holds one API record per row, unchanged, as JSONB. All parsing and typing happens later
-- in dbt, so raw can always be re-transformed without calling the API again.
--
-- Shared columns of a raw snapshot table:
--   snapshot_date  the day the data was FETCHED (Europe/Amsterdam). The API returns "the situation now",
--                  so this is the real date of the data, not the Airflow schedule date.
--   fetched_at     the exact moment of the fetch
--   run_id         the Airflow run that loaded the row, to trace problems
--   ingested_at    when the row was written (dbt source freshness uses this)
--   business_key   the record's natural ID in the API
--   record_key     sha256(endpoint|snapshot_date|business_key); UNIQUE, so a duplicate load fails and rolls back
--   payload_hash   sha256 of the JSON; differs between days when the record's content changed
--   payload        the full record

-- One row per load attempt, successful or failed.
CREATE TABLE raw.ingestion_log (
    id            BIGSERIAL PRIMARY KEY,
    dag_id        TEXT        NOT NULL,
    run_id        TEXT        NOT NULL,
    endpoint      TEXT        NOT NULL,
    logical_date  TIMESTAMPTZ NOT NULL,   -- Airflow's schedule date for the run (NOT the data date)
    snapshot_date DATE        NOT NULL,   -- the data date the load targeted
    status        TEXT        NOT NULL CHECK (status IN ('success', 'failed')),
    rows_loaded   INTEGER,                -- NULL when failed
    duration_s    NUMERIC(10, 2),
    error         TEXT,                   -- short, sanitised message when failed
    logged_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ingestion_log_run_idx ON raw.ingestion_log (run_id);

CREATE TABLE raw.stations (
    id             BIGSERIAL PRIMARY KEY,
    snapshot_date  DATE        NOT NULL,
    fetched_at     TIMESTAMPTZ NOT NULL,
    run_id         TEXT        NOT NULL,
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    business_key   TEXT        NOT NULL,
    record_key     TEXT        NOT NULL,
    payload_hash   TEXT        NOT NULL,
    payload        JSONB       NOT NULL
);
CREATE INDEX stations_snapshot_date_idx ON raw.stations (snapshot_date);
CREATE UNIQUE INDEX stations_record_key_uq ON raw.stations (record_key);

CREATE TABLE raw.disruptions (
    id             BIGSERIAL PRIMARY KEY,
    snapshot_date  DATE        NOT NULL,
    fetched_at     TIMESTAMPTZ NOT NULL,
    run_id         TEXT        NOT NULL,
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    business_key   TEXT        NOT NULL,
    record_key     TEXT        NOT NULL,
    payload_hash   TEXT        NOT NULL,
    payload        JSONB       NOT NULL
);
CREATE INDEX disruptions_snapshot_date_idx ON raw.disruptions (snapshot_date);
CREATE UNIQUE INDEX disruptions_record_key_uq ON raw.disruptions (record_key);

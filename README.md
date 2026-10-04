# End-to-End ELT Pipeline: Dutch Train Data

A reference project showing how a production-minded batch pipeline is designed. It extracts train data from the NS API, lands it unchanged in PostgreSQL, transforms it with dbt (staging, then marts), and is orchestrated by Airflow with retries, alerts and monitoring.

**Stack:** NS API, Python (`requests`), PostgreSQL 15, dbt 1.8, Airflow 2.8, Docker Compose.

To learn *why* it is built this way, read [WORKFLOW.txt](WORKFLOW.txt) (Dutch): a walkthrough of one run and the design principles behind each decision.

## Data flow

```
NS API -> raw.*  (JSONB, unchanged) -> staging.* (dbt views: typed) -> production.* (dbt tables: marts)
```

| Layer | Schema | Contents |
|---|---|---|
| Raw | `raw` | `stations`, `disruptions` (one JSONB record per row), `ingestion_log` |
| Staging | `staging` | `stg_stations`, `stg_disruptions` |
| Marts | `production` | `dim_station`, `fct_disruptions`, `mart_disruptions_daily` |

**DAG `elt_pipeline`** (daily): `extract_stations` and `extract_disruptions` in parallel, then `dbt_build_staging`, then `dbt_build_marts`. `dbt build` runs models and tests, so a failing test stops the pipeline before the marts.
**DAG `pipeline_monitor`** (every 6 h): `dbt source freshness`; alerts when raw data is stale.

## Design decisions

| Concern | Decision |
|---|---|
| Idempotency | A load replaces its `snapshot_date` partition in one transaction; a unique `record_key` (SHA-256 of endpoint, date, business key) blocks duplicates. |
| Snapshot dates | The API only knows "now", so data is labelled with the day it was *fetched* (Europe/Amsterdam), not Airflow's logical date. `catchup=False`. |
| Schema management | Versioned SQL migrations in `sql/migrations/` (checksummed, applied once). The loader never creates tables. |
| Security | One Postgres role per component (loader, dbt, reader, airflow); the admin is used only for bootstrap and migrations. Ports bind to `127.0.0.1`. Secrets only in `.env`; the NS key only reaches the scheduler. Errors are reduced to type + first line before being stored or emailed. |
| Reliability | Extract: 3 retries with exponential backoff. dbt: no retries (failures are deterministic). Task and run timeouts. `max_active_runs=1`. |
| Data quality | Generic tests plus singular tests (volume, coordinates, consistency), source freshness, and a loader that fails on unexpected empty data. |
| Observability | `raw.ingestion_log` for every attempt, email on failure and success, optional dead man's switch (`HEALTHCHECK_URL`). |
| Isolation | Tests run against a separate `<db>_test` database. |

## Quick start

Prerequisites: Docker with Compose, [uv](https://docs.astral.sh/uv/) (for linting).

1. `cp .env.example .env` and fill it in. Generate every secret (commands are in `.env.example`). Get an NS key at the [NS API portal](https://apiportal.ns.nl/). Never commit `.env`.
2. `make up`. Postgres starts, runs the bootstrap script, migrations are applied, then Airflow starts.
3. Open http://localhost:8080 (user `admin`, password `AIRFLOW_ADMIN_PASSWORD`), unpause `elt_pipeline` and trigger it.

| Service | Address |
|---|---|
| Airflow UI | http://localhost:8080 |
| MailHog (caught alert emails) | http://localhost:8025 |
| PostgreSQL | `127.0.0.1:5432`; log in with `PG_ADMIN_USER` for inspection |

## Commands

| Command | What it does |
|---|---|
| `make up` / `down` / `logs` | Start, stop, follow logs |
| `make migrate` | Apply new migrations |
| `make test` | pytest inside the Airflow container (separate test database) |
| `make lint` | ruff check + format check |
| `make backup` / `make restore FILE=...` | `pg_dump` / `pg_restore` |
| `make dbt-run` / `dbt-test` / `dbt-freshness` / `dbt-debug` | dbt via the `dbt-runner` container |

## Project structure

```
.
├── airflow/
│   ├── dags/                     # elt_pipeline.py, pipeline_monitor.py
│   ├── scripts/
│   │   ├── db_connection.py      # DatabaseManager (shared DB access)
│   │   ├── alerts.py             # email callbacks, healthcheck ping
│   │   ├── safe_errors.py        # reduces error text before it is stored or sent
│   │   ├── dbt_cmd.py            # builds dbt command lines
│   │   ├── scraping/             # ns_client, raw_loader, extract_ns
│   │   └── ml/                   # placeholder for future ML work
│   ├── Dockerfile
│   └── requirements.txt
├── dbt_project/                  # models (staging, marts), sources, tests/ (singular tests)
├── sql/
│   ├── init/00_bootstrap.sh      # roles, databases, schemas, privileges (first start only)
│   ├── migrations/               # versioned DDL
│   └── migrate.sh                # migration runner
├── tests/                        # pytest suite
├── .github/                      # CI workflow, dependabot
├── docker-compose.yml
├── Makefile
├── WORKFLOW.txt                  # learning guide
└── REVIEW.txt                    # design review: what was fixed, what is still open
```

## Adding data

**A new NS endpoint (same key):**
1. Add a method to `NSClient`.
2. Add `sql/migrations/000N_<name>.sql` creating `raw.<name>` (copy the shape of `raw.stations`), then `make migrate`.
3. Add a function in `extract_ns.py` (key field, `allow_empty` if zero rows is legitimate).
4. Add a task to the DAG before `dbt_build_staging`.
5. Add the table to `sources.yml`, a `stg_` model with tests, then any marts.

**A different API with its own key:**
1. Add `WEATHER_API_KEY` to `.env` and `.env.example`; pass it in `docker-compose.yml` to the **scheduler only** (`x-pipeline-env`).
2. Write a separate client (own auth, timeouts, rate limits, pagination), modelled on `NSClient`. Do not share one client between APIs.
3. Reuse `load_snapshot()`; then follow steps 2-5 above.
4. In CI, mock the API; never put a real key in the workflow.

Later, move keys to Airflow Connections backed by a secrets manager, and give each API its own Airflow pool to respect rate limits.

## Continuous integration

[.github/workflows/ci.yml](.github/workflows/ci.yml) runs on every push and pull request: `lint` (ruff), `secrets` (gitleaks over the full history), `test` (pytest against a temporary Postgres with the real migrations) and `dbt` (`dbt parse`). The file is heavily commented and explains how GitHub Actions works.

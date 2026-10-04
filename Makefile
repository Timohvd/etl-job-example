# Shortcuts: run `make <name>`, e.g. `make up`. (Recipe lines must start with a TAB.)
.PHONY: up down logs migrate test lint backup restore dbt-run dbt-test dbt-docs dbt-seed dbt-debug dbt-freshness

## Stack ------------------------------------------------------------------------------------
# Start all services in the background.
up:
	docker compose up -d

# Stop and remove the containers (database data is kept in its volume).
down:
	docker compose down

# Follow the logs of all services (Ctrl+C to stop following).
logs:
	docker compose logs -f

# Apply new migrations from sql/migrations/ (already applied ones are skipped).
migrate:
	docker compose run --rm db-migrate

## Quality -----------------------------------------------------------------------------------
# Run pytest inside the Airflow container (Airflow, database and env vars exist there). Tests use the separate
# *_test database. pytest/responses are installed on the fly to keep the production image free of test tools.
test:
	docker compose exec -T airflow-scheduler bash -c "pip install -q pytest responses && cd /opt/airflow && python -m pytest tests -v"

# Same as the CI "lint" job. Fix formatting with `uvx ruff format .`
lint:
	uvx ruff check . && uvx ruff format --check .

## Backup ------------------------------------------------------------------------------------
# Dump the data database to backups/<timestamp>.dump (custom format, compressed).
backup:
	@mkdir -p backups
	docker compose exec -T postgres sh -c 'pg_dump -U "$$POSTGRES_USER" -Fc "$$POSTGRES_DB"' > backups/$$(date +%Y%m%d_%H%M%S).dump
	@ls -lh backups | tail -3

# Restore a dump into the data database: make restore FILE=backups/<name>.dump  (replaces existing objects).
restore:
	@test -n "$(FILE)" || (echo "usage: make restore FILE=backups/<name>.dump" && exit 1)
	docker compose exec -T postgres sh -c 'pg_restore -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" --clean --if-exists' < $(FILE)

## dbt (manual runs via the dbt-runner container; the DAG runs dbt itself on schedule) --------
dbt-run:
	docker compose run --rm dbt-runner dbt run

dbt-test:
	docker compose run --rm dbt-runner dbt test

dbt-freshness:
	docker compose run --rm dbt-runner dbt source freshness

dbt-docs:
	docker compose run --rm dbt-runner dbt docs generate

dbt-seed:
	docker compose run --rm dbt-runner dbt seed

dbt-debug:
	docker compose run --rm dbt-runner dbt debug

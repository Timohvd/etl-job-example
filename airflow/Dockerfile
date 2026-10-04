# Image used by the airflow-init, airflow-webserver and airflow-scheduler services (see docker-compose.yml).
# Docker builds each instruction as a cached layer, so rarely-changing steps come first.

# Official Airflow image (Python 3.11) as the base.
FROM apache/airflow:3.3.2-python3.11

# The base image runs as the unprivileged "airflow" user. Installing OS packages needs root.
USER root
# gcc is needed to compile some Python packages. Deleting the apt cache keeps the image small.
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

# Switch back: Airflow refuses to run as root.
USER airflow

# dbt gets its own virtual environment so its dependencies cannot clash with Airflow's.
# The DAG calls /opt/airflow/dbt_venv/bin/dbt directly.
# - PIP_USER=false: the base image forces `pip --user`, which does not work inside a venv.
# - dbt-core is pinned too; without it pip picks a newer major version without Postgres support.
RUN python -m venv /opt/airflow/dbt_venv \
    && PIP_USER=false /opt/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core==1.8.3 dbt-postgres==1.8.2

# Our own Python dependencies (API client, database access) go into Airflow's normal environment.
# Copying requirements.txt alone first means this layer is only rebuilt when the requirements change.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

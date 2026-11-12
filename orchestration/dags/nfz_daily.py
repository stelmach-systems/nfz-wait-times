"""Daily NFZ snapshot: dlt ingestion -> dbt build -> source freshness.

Runs inside the local Astro (Airflow) environment against its own DuckDB file
(data-airflow/nfz.duckdb). The production schedule is GitHub Actions (session 3);
until then the host launchd job fills data/nfz.duckdb.
"""

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

NFZ_HOME = "/usr/local/airflow/nfz"  # repo code mounted read-only, see docker-compose.override.yml
VENV = "/usr/local/airflow/nfz_venv/bin"  # pipeline venv built in the Dockerfile

# dbt must not write into the read-only project mount.
DBT_ENV = {
    "DBT_TARGET_PATH": "/tmp/dbt/target",
    "DBT_LOG_PATH": "/tmp/dbt/logs",
    "DBT_PROFILES_DIR": f"{NFZ_HOME}/transform",
    # Pin the file dlt writes (parents[1]/data), so a stray DUCKDB_PATH in the container env cannot split them.
    "DUCKDB_PATH": f"{NFZ_HOME}/data/nfz.duckdb",
    "DO_NOT_TRACK": "1",
}

with DAG(
    dag_id="nfz_daily",
    description="NFZ queues API -> DuckDB (dlt) -> stg models and tests (dbt)",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2026, 11, 12, tz="Europe/Warsaw"),
    # The script snapshots *today* (the API has no history), so a backfill would only rewrite today.
    catchup=False,
    max_active_runs=1,
    tags=["nfz", "dlt", "dbt"],
) as dag:
    ingest = BashOperator(
        task_id="ingest_nfz_queues",
        # A manual re-trigger on the same day keeps the day already loaded.
        bash_command=f"cd {NFZ_HOME} && {VENV}/python ingestion/nfz_queues.py --skip-if-loaded",
        append_env=True,
        env={"DO_NOT_TRACK": "1"},
        execution_timeout=pendulum.duration(minutes=30),
        # Only the network step is worth retrying; a failing dbt test or stale source is deterministic.
        retries=1,
        retry_delay=pendulum.duration(minutes=10),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {NFZ_HOME}/transform && {VENV}/dbt build",
        append_env=True,
        env=DBT_ENV,
    )

    freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {NFZ_HOME}/transform && {VENV}/dbt source freshness",
        append_env=True,
        env=DBT_ENV,
    )

    ingest >> dbt_build >> freshness
